import type { GoogleSpreadsheet } from "google-spreadsheet";
import type { TrendsResults, TrendDelta } from "../analytics/trends";
import { getOrCreateSheet, writeRows } from "./sheets-client";
import { applyStandardFormatting, formatCurrencyColumns } from "./formatters";

export async function writeTrendsTab(
  doc: GoogleSpreadsheet,
  trends: TrendsResults
): Promise<void> {
  const headers = [
    "Period",
    "Type",
    "New Members",
    "New SKY3",
    "New SKY TING TV",
    "Member Churn",
    "SKY3 Churn",
    "SKY TING TV Churn",
    "Net Member Growth",
    "Net SKY3 Growth",
    "Revenue Added",
    "Revenue Lost",
    "Δ New Members",
    "Δ New SKY3",
    "Δ Revenue",
    "Δ% New Members",
    "Δ% New SKY3",
    "Δ% Revenue",
  ];
  const sheet = await getOrCreateSheet(doc, "Trends", headers);

  function deltaToRow(d: TrendDelta, type: string): Record<string, string | number> {
    return {
      Period: d.current.period,
      Type: type,
      "New Members": d.current.newMembers,
      "New SKY3": d.current.newSky3,
      "New SKY TING TV": d.current.newSkyTingTv,
      "Member Churn": d.current.memberCancellations,
      "SKY3 Churn": d.current.sky3Cancellations,
      "SKY TING TV Churn": d.current.skyTingTvCancellations,
      "Net Member Growth": d.current.netMemberGrowth,
      "Net SKY3 Growth": d.current.netSky3Growth,
      "Revenue Added": d.current.revenue,
      "Revenue Lost": d.current.revenueLost,
      "Δ New Members": d.delta?.newMembers ?? "",
      "Δ New SKY3": d.delta?.newSky3 ?? "",
      "Δ Revenue": d.delta?.revenue ?? "",
      "Δ% New Members": d.deltaPercent?.newMembers != null ? `${d.deltaPercent.newMembers}%` : "",
      "Δ% New SKY3": d.deltaPercent?.newSky3 != null ? `${d.deltaPercent.newSky3}%` : "",
      "Δ% Revenue": d.deltaPercent?.revenue != null ? `${d.deltaPercent.revenue}%` : "",
    };
  }

  const spacer: Record<string, string | number> = {
    Period: "", Type: "", "New Members": "", "New SKY3": "", "New SKY TING TV": "",
    "Member Churn": "", "SKY3 Churn": "", "SKY TING TV Churn": "",
    "Net Member Growth": "", "Net SKY3 Growth": "",
    "Revenue Added": "", "Revenue Lost": "",
    "Δ New Members": "", "Δ New SKY3": "", "Δ Revenue": "",
    "Δ% New Members": "", "Δ% New SKY3": "", "Δ% Revenue": "",
  };

  const rows: Record<string, string | number>[] = [];

  // Weekly section header
  rows.push({ ...spacer, Period: "— Weekly (WoW) —" });
  for (const w of trends.weekly) {
    rows.push(deltaToRow(w, "Weekly"));
  }

  // Spacer
  rows.push(spacer);

  // Monthly section header
  rows.push({ ...spacer, Period: "— Monthly (MoM) —" });
  for (const m of trends.monthly) {
    rows.push(deltaToRow(m, "Monthly"));
  }

  // Spacer
  rows.push(spacer);

  // Pacing
  const p = trends.currentMonthPacing;
  rows.push({
    Period: `Pacing: ${p.month} (${p.daysElapsed}/${p.daysInMonth} days)`,
    Type: "Pacing",
    "New Members": `${p.newMembersActual} → ${p.newMembersPaced}`,
    "New SKY3": `${p.newSky3Actual} → ${p.newSky3Paced}`,
    "New SKY TING TV": "",
    "Member Churn": `${p.memberCancellationsActual} → ${p.memberCancellationsPaced}`,
    "SKY3 Churn": `${p.sky3CancellationsActual} → ${p.sky3CancellationsPaced}`,
    "SKY TING TV Churn": "",
    "Net Member Growth": "",
    "Net SKY3 Growth": "",
    "Revenue Added": `$${p.revenueActual} → $${p.revenuePaced}`,
    "Revenue Lost": "",
    "Δ New Members": "", "Δ New SKY3": "", "Δ Revenue": "",
    "Δ% New Members": "", "Δ% New SKY3": "", "Δ% Revenue": "",
  });

  // Spacer
  rows.push(spacer);

  // Annual projection
  const a = trends.annualProjection;
  rows.push({
    Period: `${a.year} Annual Projection`,
    Type: "Projection",
    "New Members": "",
    "New SKY3": "",
    "New SKY TING TV": "",
    "Member Churn": "",
    "SKY3 Churn": "",
    "SKY TING TV Churn": "",
    "Net Member Growth": "",
    "Net SKY3 Growth": "",
    "Revenue Added": `$${a.projectedAnnualRevenue.toLocaleString()}`,
    "Revenue Lost": "",
    "Δ New Members": "",
    "Δ New SKY3": "",
    "Δ Revenue": `Growth: ${a.monthlyGrowthRate}%/mo`,
    "Δ% New Members": "",
    "Δ% New SKY3": "",
    "Δ% Revenue": "",
  });
  rows.push({
    Period: "Current MRR",
    Type: "",
    "New Members": "",
    "New SKY3": "",
    "New SKY TING TV": "",
    "Member Churn": "",
    "SKY3 Churn": "",
    "SKY TING TV Churn": "",
    "Net Member Growth": "",
    "Net SKY3 Growth": "",
    "Revenue Added": a.currentMRR,
    "Revenue Lost": "",
    "Δ New Members": "",
    "Δ New SKY3": "",
    "Δ Revenue": "",
    "Δ% New Members": "",
    "Δ% New SKY3": "",
    "Δ% Revenue": "",
  });
  rows.push({
    Period: "Projected Year-End MRR",
    Type: "",
    "New Members": "",
    "New SKY3": "",
    "New SKY TING TV": "",
    "Member Churn": "",
    "SKY3 Churn": "",
    "SKY TING TV Churn": "",
    "Net Member Growth": "",
    "Net SKY3 Growth": "",
    "Revenue Added": a.projectedYearEndMRR,
    "Revenue Lost": "",
    "Δ New Members": "",
    "Δ New SKY3": "",
    "Δ Revenue": "",
    "Δ% New Members": "",
    "Δ% New SKY3": "",
    "Δ% Revenue": "",
  });

  await writeRows(sheet, rows);
  await applyStandardFormatting(sheet, rows.length);
  // Format revenue columns (10 = Revenue Added, 11 = Revenue Lost)
  await formatCurrencyColumns(sheet, [10, 11], rows.length);
}
