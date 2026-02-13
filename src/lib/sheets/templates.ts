import type { GoogleSpreadsheet } from "google-spreadsheet";
import type { FunnelResults, FunnelRow } from "../analytics/funnel";
import type { FunnelOverviewResults } from "../analytics/funnel-overview";
import type { ChurnResults, ChurnRow } from "../analytics/churn";
import type { VolumeResults, WeeklyVolumeRow } from "../analytics/volume";
import type { SummaryKPIs } from "../analytics/summary";
import { getOrCreateSheet, writeRows } from "./sheets-client";
import { applyStandardFormatting, formatPercentColumns, formatCurrencyColumns } from "./formatters";

export async function writeDashboardTab(
  doc: GoogleSpreadsheet,
  summary: SummaryKPIs,
  dateRange: string,
  recordCounts: Record<string, number>
): Promise<void> {
  const headers = ["Metric", "MEMBER", "SKY3", "SKY TING TV", "UNKNOWN", "Overall"];
  const sheet = await getOrCreateSheet(doc, "Dashboard", headers);

  const r = (metric: string, member: string | number, sky3: string | number, tv: string | number, unknown: string | number, overall: string | number) =>
    ({ Metric: metric, MEMBER: member, SKY3: sky3, "SKY TING TV": tv, UNKNOWN: unknown, Overall: overall });

  const rows: Record<string, string | number>[] = [
    r(`Run: ${new Date().toISOString()}`, "", "", "", "", ""),
    r(`Date Range: ${dateRange}`, "", "", "", "", ""),
    r("", "", "", "", "", ""),
    r("MRR", summary.mrrMember, summary.mrrSky3, summary.mrrSkyTingTv, summary.mrrUnknown, summary.mrrTotal),
    r("Active Subscribers", summary.activeMembers, summary.activeSky3, summary.activeSkyTingTv, summary.activeUnknown, summary.activeTotal),
    r("ARPU", summary.arpuMember, summary.arpuSky3, summary.arpuSkyTingTv, "", summary.arpuOverall),
    r("", "", "", "", "", ""),
    r("Records Processed", "", "", "", "", ""),
    ...Object.entries(recordCounts).map(([key, count]) =>
      r(`  ${key}`, count, "", "", "", "")
    ),
  ];

  // Add unrecognized plan names section if any exist
  const unknownEntries = Object.entries(summary.unknownPlanNames).sort((a, b) => b[1] - a[1]);
  if (unknownEntries.length > 0) {
    rows.push(r("", "", "", "", "", ""));
    rows.push(r("Unrecognized Plan Names", "Count", "", "", "", ""));
    for (const [planName, count] of unknownEntries) {
      rows.push(r(`  ${planName}`, count, "", "", "", ""));
    }
  }

  await writeRows(sheet, rows);
  await applyStandardFormatting(sheet, rows.length);
}

/**
 * Funnel Overview tab — mirrors the visual funnel:
 *
 *   New Customers          2,801
 *   First Visit (5-wk)       907   41.5% conversion
 *   Auto Renew (10-wk)        76    8.2% conversion
 *
 * Laid out with one "funnel block" per period (All Time, each month, rolling windows).
 */
export async function writeFunnelOverviewTab(
  doc: GoogleSpreadsheet,
  overview: FunnelOverviewResults
): Promise<void> {
  const headers = ["Period", "Stage", "Count", "Conversion %"];
  const sheet = await getOrCreateSheet(doc, "Funnel Overview", headers);

  const rows: Record<string, string | number>[] = [];

  function addFunnelBlock(row: typeof overview.allTime) {
    rows.push({ Period: row.period, Stage: "New Customers", Count: row.newCustomers, "Conversion %": "" });
    rows.push({ Period: "", Stage: "First Visit (5-wk)", Count: row.firstVisits, "Conversion %": row.firstVisitConversion });
    rows.push({ Period: "", Stage: "Auto Renew (10-wk)", Count: row.autoRenews, "Conversion %": row.autoRenewConversion });
    rows.push({ Period: "", Stage: "", Count: "", "Conversion %" : "" }); // spacer
  }

  // All time first
  addFunnelBlock(overview.allTime);

  // Rolling windows
  addFunnelBlock(overview.rolling30);
  addFunnelBlock(overview.rolling60);
  addFunnelBlock(overview.rolling90);

  // Monthly — separator row, then each month
  rows.push({ Period: "— Monthly Breakdown —", Stage: "", Count: "", "Conversion %": "" });
  rows.push({ Period: "", Stage: "", Count: "", "Conversion %": "" });

  for (const mo of overview.monthly) {
    addFunnelBlock(mo);
  }

  await writeRows(sheet, rows);
  await applyStandardFormatting(sheet, rows.length);
  await formatPercentColumns(sheet, [3], rows.length);
}

export async function writeFunnelTab(
  doc: GoogleSpreadsheet,
  funnel: FunnelResults
): Promise<void> {
  const headers = [
    "Period", "Type",
    "Intro→SKY3 Count", "Intro→SKY3 Rate %", "Intro→SKY3 Avg Days",
    "Intro→Member Count", "Intro→Member Rate %", "Intro→Member Avg Days",
    "SKY3→Member Count", "SKY3→Member Rate %", "SKY3→Member Avg Days",
  ];
  const sheet = await getOrCreateSheet(doc, "Funnel Analysis", headers);

  function funnelToRow(r: FunnelRow) {
    return {
      Period: r.period,
      Type: r.periodType,
      "Intro→SKY3 Count": r.introToSky3Count,
      "Intro→SKY3 Rate %": r.introToSky3Rate,
      "Intro→SKY3 Avg Days": r.introToSky3AvgDays,
      "Intro→Member Count": r.introToMemberCount,
      "Intro→Member Rate %": r.introToMemberRate,
      "Intro→Member Avg Days": r.introToMemberAvgDays,
      "SKY3→Member Count": r.sky3ToMemberCount,
      "SKY3→Member Rate %": r.sky3ToMemberRate,
      "SKY3→Member Avg Days": r.sky3ToMemberAvgDays,
    };
  }

  const rows = [
    ...funnel.weekly.map(funnelToRow),
    { Period: "---", Type: "---", "Intro→SKY3 Count": "", "Intro→SKY3 Rate %": "", "Intro→SKY3 Avg Days": "", "Intro→Member Count": "", "Intro→Member Rate %": "", "Intro→Member Avg Days": "", "SKY3→Member Count": "", "SKY3→Member Rate %": "", "SKY3→Member Avg Days": "" },
    ...funnel.monthly.map(funnelToRow),
    { Period: "---", Type: "---", "Intro→SKY3 Count": "", "Intro→SKY3 Rate %": "", "Intro→SKY3 Avg Days": "", "Intro→Member Count": "", "Intro→Member Rate %": "", "Intro→Member Avg Days": "", "SKY3→Member Count": "", "SKY3→Member Rate %": "", "SKY3→Member Avg Days": "" },
    funnelToRow(funnel.rolling30),
    funnelToRow(funnel.rolling60),
    funnelToRow(funnel.rolling90),
  ];

  await writeRows(sheet, rows as Record<string, string | number>[]);
  await applyStandardFormatting(sheet, rows.length);
  // Format percent columns (indices 3, 6, 9 are rate columns)
  await formatPercentColumns(sheet, [3, 6, 9], rows.length);
}

export async function writeWeeklyVolumeTab(
  doc: GoogleSpreadsheet,
  volume: VolumeResults
): Promise<void> {
  const headers = [
    "Week", "New SKY3", "New Member", "Total New Auto-Renews",
    "Net New Customers", "SKY3 Cancellations", "Member Cancellations",
    "Net SKY3 Growth", "Net Member Growth",
  ];
  const sheet = await getOrCreateSheet(doc, "Weekly Volume", headers);

  function volumeToRow(r: WeeklyVolumeRow) {
    return {
      Week: r.week,
      "New SKY3": r.newSky3,
      "New Member": r.newMember,
      "Total New Auto-Renews": r.totalNewAutoRenews,
      "Net New Customers": r.netNewCustomers,
      "SKY3 Cancellations": r.sky3Cancellations,
      "Member Cancellations": r.memberCancellations,
      "Net SKY3 Growth": r.netSky3Growth,
      "Net Member Growth": r.netMemberGrowth,
    };
  }

  const rows = [
    ...volume.weekly.map(volumeToRow),
    { Week: "---", "New SKY3": "", "New Member": "", "Total New Auto-Renews": "", "Net New Customers": "", "SKY3 Cancellations": "", "Member Cancellations": "", "Net SKY3 Growth": "", "Net Member Growth": "" },
    volumeToRow(volume.rolling30),
    volumeToRow(volume.rolling60),
    volumeToRow(volume.rolling90),
  ];

  await writeRows(sheet, rows as Record<string, string | number>[]);
  await applyStandardFormatting(sheet, rows.length);
}

export async function writeChurnTab(
  doc: GoogleSpreadsheet,
  churn: ChurnResults
): Promise<void> {
  const headers = [
    "Period", "Type",
    "SKY3 Churn Rate %", "Member Churn Rate %",
    "SKY3 Avg Duration (days)", "Member Avg Duration (days)",
    "SKY3 Revenue Lost", "Member Revenue Lost",
    "SKY3 Cancellations", "Member Cancellations",
  ];
  const sheet = await getOrCreateSheet(doc, "Churn", headers);

  function churnToRow(r: ChurnRow) {
    return {
      Period: r.period,
      Type: r.periodType,
      "SKY3 Churn Rate %": r.sky3ChurnRate,
      "Member Churn Rate %": r.memberChurnRate,
      "SKY3 Avg Duration (days)": r.sky3AvgDuration,
      "Member Avg Duration (days)": r.memberAvgDuration,
      "SKY3 Revenue Lost": r.sky3RevenueLost,
      "Member Revenue Lost": r.memberRevenueLost,
      "SKY3 Cancellations": r.sky3Cancellations,
      "Member Cancellations": r.memberCancellations,
    };
  }

  const rows = [
    ...churn.weekly.map(churnToRow),
    { Period: "---", Type: "---", "SKY3 Churn Rate %": "", "Member Churn Rate %": "", "SKY3 Avg Duration (days)": "", "Member Avg Duration (days)": "", "SKY3 Revenue Lost": "", "Member Revenue Lost": "", "SKY3 Cancellations": "", "Member Cancellations": "" },
    ...churn.monthly.map(churnToRow),
    { Period: "---", Type: "---", "SKY3 Churn Rate %": "", "Member Churn Rate %": "", "SKY3 Avg Duration (days)": "", "Member Avg Duration (days)": "", "SKY3 Revenue Lost": "", "Member Revenue Lost": "", "SKY3 Cancellations": "", "Member Cancellations": "" },
    churnToRow(churn.rolling30),
    churnToRow(churn.rolling60),
    churnToRow(churn.rolling90),
  ];

  await writeRows(sheet, rows as Record<string, string | number>[]);
  await applyStandardFormatting(sheet, rows.length);
  await formatPercentColumns(sheet, [2, 3], rows.length);
  await formatCurrencyColumns(sheet, [6, 7], rows.length);
}

export async function writeRunLogEntry(
  doc: GoogleSpreadsheet,
  entry: {
    timestamp: string;
    duration: number;
    recordCounts: Record<string, number>;
    warnings: string[];
  }
): Promise<void> {
  const headers = ["Timestamp", "Duration (s)", "Records", "Warnings"];
  let sheet = doc.sheetsByTitle["Run Log"];

  if (!sheet) {
    sheet = await doc.addSheet({ title: "Run Log", headerValues: headers });
    await applyStandardFormatting(sheet, 0);
  }

  await sheet.addRow({
    Timestamp: entry.timestamp,
    "Duration (s)": Math.round(entry.duration / 1000),
    Records: Object.entries(entry.recordCounts).map(([k, v]) => `${k}: ${v}`).join(", "),
    Warnings: entry.warnings.length > 0 ? entry.warnings.join("; ").slice(0, 500) : "None",
  });
}

export async function writeRawDataSheets(
  doc: GoogleSpreadsheet,
  data: {
    newCustomers: Record<string, string | number>[];
    orders: Record<string, string | number>[];
    registrations: Record<string, string | number>[];
    autoRenews: Record<string, string | number>[];
  }
): Promise<void> {
  // Members tab (from active auto-renews / customers)
  if (data.autoRenews.length > 0) {
    const headers = Object.keys(data.autoRenews[0]);
    const sheet = await getOrCreateSheet(doc, "Members", headers);
    await writeRows(sheet, data.autoRenews);
    await applyStandardFormatting(sheet, data.autoRenews.length);
  }

  // Sales/Transactions
  if (data.orders.length > 0) {
    const headers = Object.keys(data.orders[0]);
    const sheet = await getOrCreateSheet(doc, "Sales/Transactions", headers);
    await writeRows(sheet, data.orders);
    await applyStandardFormatting(sheet, data.orders.length);
  }

  // Class Roster
  if (data.registrations.length > 0) {
    const headers = Object.keys(data.registrations[0]);
    const sheet = await getOrCreateSheet(doc, "Class Roster", headers);
    await writeRows(sheet, data.registrations);
    await applyStandardFormatting(sheet, data.registrations.length);
  }

  // Sign-ups
  if (data.newCustomers.length > 0) {
    const headers = Object.keys(data.newCustomers[0]);
    const sheet = await getOrCreateSheet(doc, "Sign-ups", headers);
    await writeRows(sheet, data.newCustomers);
    await applyStandardFormatting(sheet, data.newCustomers.length);
  }
}
