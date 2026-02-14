import { getSpreadsheet } from "./sheets-client";
import type { GoogleSpreadsheet } from "google-spreadsheet";

export interface DashboardStats {
  lastUpdated: string | null;
  dateRange: string | null;
  mrr: {
    member: number;
    sky3: number;
    skyTingTv: number;
    unknown: number;
    total: number;
  };
  activeSubscribers: {
    member: number;
    sky3: number;
    skyTingTv: number;
    unknown: number;
    total: number;
  };
  arpu: {
    member: number;
    sky3: number;
    skyTingTv: number;
    overall: number;
  };
}

// In-memory cache with 5-minute TTL
let cached: { data: DashboardStats; fetchedAt: number } | null = null;
const CACHE_TTL_MS = 5 * 60 * 1000;

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
  };

  cached = { data, fetchedAt: Date.now() };
  console.log(
    `[read-dashboard] Loaded stats: ${data.activeSubscribers.total} total subscribers, ${data.mrr.total} MRR`
  );
  return data;
}

// ─── Trends Data Reader ─────────────────────────────────────

export interface TrendRowData {
  period: string;
  type: string;
  newMembers: number;
  newSky3: number;
  newSkyTingTv: number;
  memberChurn: number;
  sky3Churn: number;
  skyTingTvChurn: number;
  netMemberGrowth: number;
  netSky3Growth: number;
  revenueAdded: number;
  revenueLost: number;
  deltaNewMembers: number | null;
  deltaNewSky3: number | null;
  deltaRevenue: number | null;
  deltaPctNewMembers: number | null;
  deltaPctNewSky3: number | null;
  deltaPctRevenue: number | null;
}

export interface PacingData {
  month: string;
  daysElapsed: number;
  daysInMonth: number;
  newMembersActual: number;
  newMembersPaced: number;
  newSky3Actual: number;
  newSky3Paced: number;
  revenueActual: number;
  revenuePaced: number;
  memberCancellationsActual: number;
  memberCancellationsPaced: number;
  sky3CancellationsActual: number;
  sky3CancellationsPaced: number;
}

export interface ProjectionData {
  year: number;
  projectedAnnualRevenue: number;
  currentMRR: number;
  projectedYearEndMRR: number;
  monthlyGrowthRate: number;
}

export interface TrendsData {
  weekly: TrendRowData[];
  monthly: TrendRowData[];
  pacing: PacingData | null;
  projection: ProjectionData | null;
}

let cachedTrends: { data: TrendsData; fetchedAt: number } | null = null;

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

  for (const row of rows) {
    const period = String(row.get("Period") || "").trim();
    const type = String(row.get("Type") || "").trim();

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
      };
    } else if (period === "Current MRR") {
      if (projection) {
        projection.currentMRR = parseNum(row.get("Revenue Added"));
      }
    } else if (period === "Projected Year-End MRR") {
      if (projection) {
        projection.projectedYearEndMRR = parseNum(row.get("Revenue Added"));
      }
    }
  }

  const data: TrendsData = { weekly, monthly, pacing, projection };
  cachedTrends = { data, fetchedAt: Date.now() };

  console.log(
    `[read-dashboard] Loaded trends: ${weekly.length} weekly, ${monthly.length} monthly periods`
  );
  return data;
}
