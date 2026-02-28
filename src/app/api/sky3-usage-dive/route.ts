import { NextResponse } from "next/server";
import { getSky3UsageDeepDive } from "@/lib/db/registration-store";

export const dynamic = "force-dynamic";

export async function GET() {
  try {
    const rows = await getSky3UsageDeepDive();
    const total = rows.length;

    // Monthly usage distribution (visits in last 30 days)
    const last30dBuckets = {
      "0 classes": 0,
      "1 class": 0,
      "2 classes": 0,
      "3 classes (maxing out)": 0,
      "4+ classes (over plan)": 0,
    };
    for (const r of rows) {
      const v = r.visits_last_30d;
      if (v === 0) last30dBuckets["0 classes"]++;
      else if (v === 1) last30dBuckets["1 class"]++;
      else if (v === 2) last30dBuckets["2 classes"]++;
      else if (v === 3) last30dBuckets["3 classes (maxing out)"]++;
      else last30dBuckets["4+ classes (over plan)"]++;
    }

    // Average per month distribution
    const avgBuckets = {
      "0 (dormant)": 0,
      "0.1-1.0": 0,
      "1.1-2.0": 0,
      "2.1-3.0": 0,
      "3.1-4.0 (at/near cap)": 0,
      "4.0+ (exceeding plan)": 0,
    };
    for (const r of rows) {
      const a = parseFloat(r.avg_per_month);
      if (a === 0) avgBuckets["0 (dormant)"]++;
      else if (a <= 1) avgBuckets["0.1-1.0"]++;
      else if (a <= 2) avgBuckets["1.1-2.0"]++;
      else if (a <= 3) avgBuckets["2.1-3.0"]++;
      else if (a <= 4) avgBuckets["3.1-4.0 (at/near cap)"]++;
      else avgBuckets["4.0+ (exceeding plan)"]++;
    }

    // Tenure distribution
    const tenureBuckets = {
      "Under 1 month": 0,
      "1-3 months": 0,
      "3-6 months": 0,
      "6-12 months": 0,
      "12+ months": 0,
    };
    for (const r of rows) {
      const t = parseFloat(r.tenure_months);
      if (t < 1) tenureBuckets["Under 1 month"]++;
      else if (t < 3) tenureBuckets["1-3 months"]++;
      else if (t < 6) tenureBuckets["3-6 months"]++;
      else if (t < 12) tenureBuckets["6-12 months"]++;
      else tenureBuckets["12+ months"]++;
    }

    // Cross-tab: usage by tenure
    const usageByTenure: Record<string, { total: number; using0: number; using1: number; using2: number; using3: number; using4plus: number; avgVisits: number }> = {};
    for (const tenureLabel of Object.keys(tenureBuckets)) {
      usageByTenure[tenureLabel] = { total: 0, using0: 0, using1: 0, using2: 0, using3: 0, using4plus: 0, avgVisits: 0 };
    }
    for (const r of rows) {
      const t = parseFloat(r.tenure_months);
      const v = r.visits_last_30d;
      let label: string;
      if (t < 1) label = "Under 1 month";
      else if (t < 3) label = "1-3 months";
      else if (t < 6) label = "3-6 months";
      else if (t < 12) label = "6-12 months";
      else label = "12+ months";

      usageByTenure[label].total++;
      usageByTenure[label].avgVisits += v;
      if (v === 0) usageByTenure[label].using0++;
      else if (v === 1) usageByTenure[label].using1++;
      else if (v === 2) usageByTenure[label].using2++;
      else if (v === 3) usageByTenure[label].using3++;
      else usageByTenure[label].using4plus++;
    }
    // compute averages
    for (const label of Object.keys(usageByTenure)) {
      const b = usageByTenure[label];
      b.avgVisits = b.total > 0 ? Math.round((b.avgVisits / b.total) * 10) / 10 : 0;
    }

    // Per-class cost analysis
    const costAnalysis = {
      using0: { count: last30dBuckets["0 classes"], effectiveCostPerClass: "∞ (paying $95 for 0 classes)" },
      using1: { count: last30dBuckets["1 class"], effectiveCostPerClass: "$95/class" },
      using2: { count: last30dBuckets["2 classes"], effectiveCostPerClass: "$47.50/class" },
      using3: { count: last30dBuckets["3 classes (maxing out)"], effectiveCostPerClass: "$31.67/class" },
      using4plus: { count: last30dBuckets["4+ classes (over plan)"], effectiveCostPerClass: "$23.75/class or less" },
    };

    return NextResponse.json({
      total,
      last30dDistribution: Object.entries(last30dBuckets).map(([k, v]) => ({
        bucket: k, count: v, pct: ((v / total) * 100).toFixed(1)
      })),
      avgMonthlyDistribution: Object.entries(avgBuckets).map(([k, v]) => ({
        bucket: k, count: v, pct: ((v / total) * 100).toFixed(1)
      })),
      tenureDistribution: Object.entries(tenureBuckets).map(([k, v]) => ({
        bucket: k, count: v, pct: ((v / total) * 100).toFixed(1)
      })),
      usageByTenure,
      costAnalysis,
      // Top 20 most active
      topUsers: rows.slice(0, 20).map((r) => ({
        name: r.name, email: r.email, plan: r.plan_name,
        tenureMonths: r.tenure_months,
        last30d: r.visits_last_30d, month2: r.visits_month2, month3: r.visits_month3,
        avgPerMonth: r.avg_per_month,
      })),
      // Bottom 20 (least active, excluding 0)
      bottomUsers: rows.filter((r: { visits_90d: number }) => r.visits_90d > 0).slice(-20).map((r) => ({
        name: r.name, email: r.email, plan: r.plan_name,
        tenureMonths: r.tenure_months,
        last30d: r.visits_last_30d, month2: r.visits_month2, month3: r.visits_month3,
        avgPerMonth: r.avg_per_month,
      })),
    });
  } catch (err: unknown) {
    return NextResponse.json({ error: String(err) }, { status: 500 });
  }
}
