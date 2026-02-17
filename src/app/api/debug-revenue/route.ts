import { NextResponse } from "next/server";
import { getAllPeriods } from "@/lib/db/revenue-store";

export async function GET() {
  try {
    const allPeriods = await getAllPeriods();
    const currentYear = new Date().getFullYear();
    const priorYear = currentYear - 1;

    const priorYearPeriods = allPeriods.filter(
      (p) => p.periodStart.startsWith(String(priorYear)) && p.totalNetRevenue > 0
    );

    let diagnosis = "unknown";
    let priorYearActualRevenue: number | null = null;

    if (priorYearPeriods.length === 0) {
      diagnosis = `No periods found for ${priorYear}`;
    } else {
      const totalNet = priorYearPeriods.reduce((sum, p) => sum + p.totalNetRevenue, 0);
      const spansFullYear = priorYearPeriods.some((p) => {
        const start = new Date(p.periodStart + "T00:00:00");
        const end = new Date(p.periodEnd + "T00:00:00");
        const months = (end.getFullYear() - start.getFullYear()) * 12 + (end.getMonth() - start.getMonth());
        return months >= 11;
      });

      if (spansFullYear) {
        priorYearActualRevenue = Math.round(totalNet);
        diagnosis = `Full year detected. Total: $${priorYearActualRevenue}`;
      } else {
        const coveredMonths = new Set(priorYearPeriods.map((p) => p.periodStart.slice(0, 7))).size;
        priorYearActualRevenue = Math.round(totalNet / coveredMonths * 12);
        diagnosis = `Partial year: ${coveredMonths} months. Annualized: $${priorYearActualRevenue}`;
      }
    }

    return NextResponse.json({
      currentYear,
      priorYear,
      allPeriodsCount: allPeriods.length,
      allPeriods: allPeriods.map(p => ({
        start: p.periodStart,
        end: p.periodEnd,
        totalNetRevenue: p.totalNetRevenue,
        categoryCount: p.categoryCount,
      })),
      priorYearPeriodsCount: priorYearPeriods.length,
      priorYearActualRevenue,
      diagnosis,
    });
  } catch (err) {
    return NextResponse.json({ error: err instanceof Error ? err.message : String(err) }, { status: 500 });
  }
}
