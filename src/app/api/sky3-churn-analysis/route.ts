/**
 * TEMPORARY endpoint — Sky3 Churn Profile Analysis
 * DELETE after reviewing data
 */
import { NextResponse } from "next/server";
import { getSky3ChurnProfile } from "@/lib/db/registration-store";

export const dynamic = "force-dynamic";

export async function GET() {
  try {
    const data = await getSky3ChurnProfile();

    // Summary for quick reading
    const summary = {
      totalCanceledSky3Last6Mo: data.total,
      alreadyActiveBeforeSubscribing: `${data.alreadyActivePercent}% (${data.alreadyActive} of ${data.total})`,
      brandNew: `${data.brandNewPercent}% (${data.byType["brand-new"] || 0} of ${data.total})`,
      priorCustomerBreakdown: data.byType,
      medianTenureMonths: data.medianTenure,
      avgTenureMonths: data.avgTenure,
    };

    return NextResponse.json({ summary, rows: data.rows });
  } catch (err) {
    console.error("Sky3 churn analysis error:", err);
    return NextResponse.json({ error: String(err) }, { status: 500 });
  }
}
