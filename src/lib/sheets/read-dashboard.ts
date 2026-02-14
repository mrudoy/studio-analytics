import { getSpreadsheet } from "./sheets-client";

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
