import { getSpreadsheet } from "./sheets-client";
import type { GoogleSpreadsheet } from "google-spreadsheet";
import type {
  DashboardStats,
  TrendRowData,
  PacingData,
  ProjectionData,
  DropInData,
  FirstVisitSegment,
  FirstVisitData,
  ReturningNonMemberData,
  TrendsData,
} from "@/types/dashboard";

// In-memory cache with 5-minute TTL
let cached: { data: DashboardStats; fetchedAt: number } | null = null;
// eslint-disable-next-line prefer-const -- assigned in readTrendsData section below
let cachedTrends: { data: TrendsData | null; fetchedAt: number } | null = null;
const CACHE_TTL_MS = 5 * 60 * 1000;

/** Clear all dashboard caches so next read fetches fresh data from sheets */
export function clearDashboardCache(): void {
  cached = null;
  cachedTrends = null;
}

export async function readDashboardStats(
  spreadsheetId: string
): Promise<DashboardStats> {
  if (cached && Date.now() - cached.fetchedAt < CACHE_TTL_MS) {
    return cached.data;
  }

  const doc = await getSpreadsheet(spreadsheetId);
  const sheet = doc.sheetsByTitle["Dashboard"];

  if (!sheet) {
    throw new Error("Dashboard tab not found in spreadsheet. Run the pipeline first.");
  }

  const rows = await sheet.getRows();

  let lastUpdated: string | null = null;
  let dateRange: string | null = null;
  const metricData: Record<string, Record<string, number>> = {};

  for (const row of rows) {
    const metric = String(row.get("Metric") || "").trim();

    if (metric.startsWith("Run:")) {
      lastUpdated = metric.replace("Run:", "").trim();
    } else if (metric.startsWith("Date Range:")) {
      dateRange = metric.replace("Date Range:", "").trim();
    } else if (
      metric === "MRR" ||
      metric === "Active Subscribers" ||
      metric === "ARPU"
    ) {
      metricData[metric] = {
        member: parseFloat(row.get("MEMBER")) || 0,
        sky3: parseFloat(row.get("SKY3")) || 0,
        skyTingTv: parseFloat(row.get("SKY TING TV")) || 0,
        unknown: parseFloat(row.get("UNKNOWN")) || 0,
        overall: parseFloat(row.get("Overall")) || 0,
      };
    } else if (metric === "Current Month Revenue") {
      metricData[metric] = { overall: parseFloat(row.get("Overall")) || 0 };
    } else if (metric === "Previous Month Revenue") {
      metricData[metric] = { overall: parseFloat(row.get("Overall")) || 0 };
    }
  }

  const mrrRow = metricData["MRR"] || {};
  const activeRow = metricData["Active Subscribers"] || {};
  const arpuRow = metricData["ARPU"] || {};

  const data: DashboardStats = {
    lastUpdated,
    dateRange,
    mrr: {
      member: mrrRow.member ?? 0,
      sky3: mrrRow.sky3 ?? 0,
      skyTingTv: mrrRow.skyTingTv ?? 0,
      unknown: mrrRow.unknown ?? 0,
      total: mrrRow.overall ?? 0,
    },
    activeSubscribers: {
      member: activeRow.member ?? 0,
      sky3: activeRow.sky3 ?? 0,
      skyTingTv: activeRow.skyTingTv ?? 0,
      unknown: activeRow.unknown ?? 0,
      total: activeRow.overall ?? 0,
    },
    arpu: {
      member: arpuRow.member ?? 0,
      sky3: arpuRow.sky3 ?? 0,
      skyTingTv: arpuRow.skyTingTv ?? 0,
      overall: arpuRow.overall ?? 0,
    },
    currentMonthRevenue: metricData["Current Month Revenue"]?.overall ?? 0,
    previousMonthRevenue: metricData["Previous Month Revenue"]?.overall ?? 0,
  };

  cached = { data, fetchedAt: Date.now() };
  console.log(
    `[read-dashboard] Loaded stats: ${data.activeSubscribers.total} total subscribers, ${data.mrr.total} MRR`
  );
  return data;
}

// ─── Trends Data Reader ─────────────────────────────────────
// All types imported from @/types/dashboard (single source of truth)

function parseNum(val: string | undefined | null): number {
  if (!val) return 0;
  const cleaned = String(val).replace(/[$,%→]/g, "").trim();
  const n = parseFloat(cleaned);
  return isNaN(n) ? 0 : n;
}

function parseDeltaNum(val: string | undefined | null): number | null {
  if (!val || String(val).trim() === "") return null;
  const cleaned = String(val).replace(/[$,%]/g, "").trim();
  const n = parseFloat(cleaned);
  return isNaN(n) ? null : n;
}

function parsePacingField(val: string | undefined | null): { actual: number; paced: number } {
  if (!val) return { actual: 0, paced: 0 };
  const str = String(val);
  const parts = str.split("→").map((s) => s.replace(/[$,]/g, "").trim());
  if (parts.length === 2) {
    return { actual: parseFloat(parts[0]) || 0, paced: parseFloat(parts[1]) || 0 };
  }
  const n = parseFloat(str.replace(/[$,]/g, "")) || 0;
  return { actual: n, paced: n };
}

export async function readTrendsData(spreadsheetId: string): Promise<TrendsData | null> {
  if (cachedTrends && Date.now() - cachedTrends.fetchedAt < CACHE_TTL_MS) {
    return cachedTrends.data;
  }

  const doc = await getSpreadsheet(spreadsheetId);
  const sheet = doc.sheetsByTitle["Trends"];

  if (!sheet) {
    console.log("[read-dashboard] Trends tab not found — run pipeline first");
    return null;
  }

  const rows = await sheet.getRows();

  const weekly: TrendRowData[] = [];
  const monthly: TrendRowData[] = [];
  let pacing: PacingData | null = null;
  let projection: ProjectionData | null = null;

  // Temp vars — MRR/PriorYear rows may appear before or after the Projection row
  let pendingCurrentMRR: number | null = null;
  let pendingYearEndMRR: number | null = null;
  let pendingPriorYearRevenue: number | null = null;

  // Drop-in tracking
  let dropInMtd = 0;
  let dropInMtdPaced = 0;
  let dropInMtdDaysElapsed = 0;
  let dropInMtdDaysInMonth = 0;
  let dropInLastMonth = 0;
  let dropInWeeklyAvg = 0;
  const dropInWeeklyBreakdown: { week: string; count: number }[] = [];
  let fvCurrentWeekTotal = 0;
  let fvCurrentWeekSegments: Record<FirstVisitSegment, number> = { introWeek: 0, dropIn: 0, guest: 0, other: 0 };
  const fvCompletedWeeks: { week: string; uniqueVisitors: number; segments: Record<FirstVisitSegment, number> }[] = [];
  let fvAggregateSegments: Record<FirstVisitSegment, number> = { introWeek: 0, dropIn: 0, guest: 0, other: 0 };
  let rnmCurrentWeekTotal = 0;
  let rnmCurrentWeekSegments: Record<FirstVisitSegment, number> = { introWeek: 0, dropIn: 0, guest: 0, other: 0 };
  const rnmCompletedWeeks: { week: string; uniqueVisitors: number; segments: Record<FirstVisitSegment, number> }[] = [];
  let rnmAggregateSegments: Record<FirstVisitSegment, number> = { introWeek: 0, dropIn: 0, guest: 0, other: 0 };

  for (const row of rows) {
    const period = String(row.get("Period") || "").trim();
    const type = String(row.get("Type") || "").trim();

    // Capture MRR rows regardless of row order (projection may not exist yet)
    if (period === "Current MRR") {
      pendingCurrentMRR = parseNum(row.get("Revenue Added"));
      continue;
    }
    if (period === "Projected Year-End MRR") {
      pendingYearEndMRR = parseNum(row.get("Revenue Added"));
      continue;
    }
    if (period.includes("Est. Revenue")) {
      pendingPriorYearRevenue = parseNum(row.get("Revenue Added"));
      continue;
    }

    // Skip section headers and spacers
    if (!period || period.startsWith("—") || !type) continue;

    if (type === "Weekly" || type === "Monthly") {
      const trendRow: TrendRowData = {
        period,
        type,
        newMembers: parseNum(row.get("New Members")),
        newSky3: parseNum(row.get("New SKY3")),
        newSkyTingTv: parseNum(row.get("New SKY TING TV")),
        memberChurn: parseNum(row.get("Member Churn")),
        sky3Churn: parseNum(row.get("SKY3 Churn")),
        skyTingTvChurn: parseNum(row.get("SKY TING TV Churn")),
        netMemberGrowth: parseNum(row.get("Net Member Growth")),
        netSky3Growth: parseNum(row.get("Net SKY3 Growth")),
        revenueAdded: parseNum(row.get("Revenue Added")),
        revenueLost: parseNum(row.get("Revenue Lost")),
        deltaNewMembers: parseDeltaNum(row.get("Δ New Members")),
        deltaNewSky3: parseDeltaNum(row.get("Δ New SKY3")),
        deltaRevenue: parseDeltaNum(row.get("Δ Revenue")),
        deltaPctNewMembers: parseDeltaNum(row.get("Δ% New Members")),
        deltaPctNewSky3: parseDeltaNum(row.get("Δ% New SKY3")),
        deltaPctRevenue: parseDeltaNum(row.get("Δ% Revenue")),
      };
      if (type === "Weekly") weekly.push(trendRow);
      else monthly.push(trendRow);
    } else if (type === "Pacing") {
      // Parse pacing row: "Pacing: 2026-02 (14/28 days)"
      const pacingMatch = period.match(/Pacing:\s*(\S+)\s*\((\d+)\/(\d+)/);
      const newMembersP = parsePacingField(row.get("New Members"));
      const newSky3P = parsePacingField(row.get("New SKY3"));
      const memberChurnP = parsePacingField(row.get("Member Churn"));
      const sky3ChurnP = parsePacingField(row.get("SKY3 Churn"));
      const revenueP = parsePacingField(row.get("Revenue Added"));

      pacing = {
        month: pacingMatch ? pacingMatch[1] : "",
        daysElapsed: pacingMatch ? parseInt(pacingMatch[2]) : 0,
        daysInMonth: pacingMatch ? parseInt(pacingMatch[3]) : 0,
        newMembersActual: newMembersP.actual,
        newMembersPaced: newMembersP.paced,
        newSky3Actual: newSky3P.actual,
        newSky3Paced: newSky3P.paced,
        revenueActual: revenueP.actual,
        revenuePaced: revenueP.paced,
        memberCancellationsActual: memberChurnP.actual,
        memberCancellationsPaced: memberChurnP.paced,
        sky3CancellationsActual: sky3ChurnP.actual,
        sky3CancellationsPaced: sky3ChurnP.paced,
      };
    } else if (type === "Projection") {
      // Parse projection row
      const yearMatch = period.match(/(\d{4})/);
      const growthMatch = String(row.get("Δ Revenue") || "").match(/([\d.]+)%/);
      const revStr = String(row.get("Revenue Added") || "").replace(/[$,]/g, "");

      projection = {
        year: yearMatch ? parseInt(yearMatch[1]) : new Date().getFullYear(),
        projectedAnnualRevenue: parseFloat(revStr) || 0,
        currentMRR: 0,
        projectedYearEndMRR: 0,
        monthlyGrowthRate: growthMatch ? parseFloat(growthMatch[1]) : 0,
        priorYearRevenue: 0,
        priorYearActualRevenue: null,
      };
    } else if (type === "DropIn") {
      // Parse aggregated drop-in rows
      if (period.startsWith("Drop-Ins MTD")) {
        const daysMatch = period.match(/\((\d+)\/(\d+)/);
        dropInMtd = parseNum(row.get("New Members"));
        dropInMtdPaced = parseNum(row.get("New SKY3"));
        dropInMtdDaysElapsed = daysMatch ? parseInt(daysMatch[1]) : 0;
        dropInMtdDaysInMonth = daysMatch ? parseInt(daysMatch[2]) : 0;
      } else if (period === "Drop-Ins Last Month") {
        dropInLastMonth = parseNum(row.get("New Members"));
      } else if (period.startsWith("Drop-Ins Weekly Avg")) {
        dropInWeeklyAvg = parseNum(row.get("New Members"));
      }
    } else if (type === "DropInWeek") {
      dropInWeeklyBreakdown.push({
        week: period,
        count: parseNum(row.get("New Members")),
      });
    } else if (type === "FirstVisitCurrent") {
      fvCurrentWeekTotal = parseNum(row.get("New Members"));
      fvCurrentWeekSegments = {
        introWeek: parseNum(row.get("New SKY3")),
        dropIn: parseNum(row.get("New SKY TING TV")),
        guest: parseNum(row.get("Member Churn")),
        other: parseNum(row.get("SKY3 Churn")),
      };
    } else if (type === "FirstVisitWeek") {
      fvCompletedWeeks.push({
        week: period,
        uniqueVisitors: parseNum(row.get("New Members")),
        segments: {
          introWeek: parseNum(row.get("New SKY3")),
          dropIn: parseNum(row.get("New SKY TING TV")),
          guest: parseNum(row.get("Member Churn")),
          other: parseNum(row.get("SKY3 Churn")),
        },
      });
    } else if (type === "ReturningNonMemberCurrent") {
      rnmCurrentWeekTotal = parseNum(row.get("New Members"));
      rnmCurrentWeekSegments = {
        introWeek: parseNum(row.get("New SKY3")),
        dropIn: parseNum(row.get("New SKY TING TV")),
        guest: parseNum(row.get("Member Churn")),
        other: parseNum(row.get("SKY3 Churn")),
      };
    } else if (type === "ReturningNonMemberWeek") {
      rnmCompletedWeeks.push({
        week: period,
        uniqueVisitors: parseNum(row.get("New Members")),
        segments: {
          introWeek: parseNum(row.get("New SKY3")),
          dropIn: parseNum(row.get("New SKY TING TV")),
          guest: parseNum(row.get("Member Churn")),
          other: parseNum(row.get("SKY3 Churn")),
        },
      });
    } else if (type === "FirstVisitAggregate") {
      fvAggregateSegments = {
        introWeek: parseNum(row.get("New SKY3")),
        dropIn: parseNum(row.get("New SKY TING TV")),
        guest: parseNum(row.get("Member Churn")),
        other: parseNum(row.get("SKY3 Churn")),
      };
    } else if (type === "ReturningNonMemberAggregate") {
      rnmAggregateSegments = {
        introWeek: parseNum(row.get("New SKY3")),
        dropIn: parseNum(row.get("New SKY TING TV")),
        guest: parseNum(row.get("Member Churn")),
        other: parseNum(row.get("SKY3 Churn")),
      };
    }
  }

  // Assign MRR / prior-year values after loop (handles any row order)
  if (projection) {
    if (pendingCurrentMRR !== null) projection.currentMRR = pendingCurrentMRR;
    if (pendingYearEndMRR !== null) projection.projectedYearEndMRR = pendingYearEndMRR;
    if (pendingPriorYearRevenue !== null) projection.priorYearRevenue = pendingPriorYearRevenue;
  }

  // Build drop-in data (null if no drop-in rows were found)
  const dropIns: DropInData | null = dropInMtd > 0 || dropInLastMonth > 0 || dropInWeeklyBreakdown.length > 0
    ? {
        currentMonthTotal: dropInMtd,
        currentMonthDaysElapsed: dropInMtdDaysElapsed,
        currentMonthDaysInMonth: dropInMtdDaysInMonth,
        currentMonthPaced: dropInMtdPaced,
        previousMonthTotal: dropInLastMonth,
        weeklyAvg6w: dropInWeeklyAvg,
        weeklyBreakdown: dropInWeeklyBreakdown,
      }
    : null;

  // Build first visit data (null if no first visit rows were found)
  const firstVisits: FirstVisitData | null = fvCurrentWeekTotal > 0 || fvCompletedWeeks.length > 0
    ? { currentWeekTotal: fvCurrentWeekTotal, currentWeekSegments: fvCurrentWeekSegments, completedWeeks: fvCompletedWeeks, aggregateSegments: fvAggregateSegments, otherBreakdownTop5: [] }
    : null;

  // Build returning non-members data (null if no rows were found)
  const returningNonMembers: ReturningNonMemberData | null = rnmCurrentWeekTotal > 0 || rnmCompletedWeeks.length > 0
    ? { currentWeekTotal: rnmCurrentWeekTotal, currentWeekSegments: rnmCurrentWeekSegments, completedWeeks: rnmCompletedWeeks, aggregateSegments: rnmAggregateSegments, otherBreakdownTop5: [] }
    : null;

  // Legacy sheets path: only weekly/monthly/pacing/projection are fully typed.
  // Module-level data (dropIns, firstVisits, etc.) would need conversion to match
  // the DB-path types (DropInModuleData, etc.), so we null them out here.
  const data: TrendsData = { weekly, monthly, pacing, projection, dropIns: null, introWeek: null, firstVisits: null, returningNonMembers: null, churnRates: null, newCustomerVolume: null, newCustomerCohorts: null, conversionPool: null, usage: null };
  cachedTrends = { data, fetchedAt: Date.now() };

  console.log(
    `[read-dashboard] Loaded trends: ${weekly.length} weekly, ${monthly.length} monthly periods` +
    (dropIns ? `, drop-ins: MTD=${dropIns.currentMonthTotal}` : "")
  );
  return data;
}
