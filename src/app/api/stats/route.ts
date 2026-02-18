import { NextResponse } from "next/server";
import { getLatestPeriod, getRevenueForPeriod, getMonthlyRevenue, getAllMonthlyRevenue } from "@/lib/db/revenue-store";
import { analyzeRevenueCategories } from "@/lib/analytics/revenue-categories";
import { computeStatsFromDB } from "@/lib/analytics/db-stats";
import { computeTrendsFromDB } from "@/lib/analytics/db-trends";
import type { RevenueCategory } from "@/types/union-data";
import type { DashboardStats } from "@/types/dashboard";
import type { TrendsData } from "@/types/dashboard";

export async function GET() {
  try {
    // ── 1. Load from database ─────────────────────────────────
    let stats: DashboardStats | null = null;
    let trends: TrendsData | null = null;

    try {
      stats = await computeStatsFromDB();
      if (stats) {
        console.log("[api/stats] Loaded stats from database");
      }
    } catch (err) {
      console.warn("[api/stats] Database stats failed:", err);
    }

    try {
      trends = await computeTrendsFromDB();
      if (trends) {
        console.log("[api/stats] Loaded trends from database");
      }
    } catch (err) {
      console.warn("[api/stats] Database trends failed:", err);
    }

    if (!stats) {
      return NextResponse.json(
        { error: "No data available — database is empty. Upload data or run the pipeline." },
        { status: 503 }
      );
    }

    // ── 2. Revenue categories from database ───────────────────
    let revenueCategories = null;
    try {
      const latestPeriod = await getLatestPeriod();
      if (latestPeriod) {
        const rows = await getRevenueForPeriod(latestPeriod.periodStart, latestPeriod.periodEnd);
        if (rows.length > 0) {
          const asRevenueCategory: RevenueCategory[] = rows.map((r) => ({
            revenueCategory: r.category,
            revenue: r.revenue,
            unionFees: r.unionFees,
            stripeFees: r.stripeFees,
            otherFees: r.otherFees,
            transfers: r.transfers,
            refunded: r.refunded,
            unionFeesRefunded: r.unionFeesRefunded,
            netRevenue: r.netRevenue,
          }));
          revenueCategories = {
            periodStart: latestPeriod.periodStart,
            periodEnd: latestPeriod.periodEnd,
            ...analyzeRevenueCategories(asRevenueCategory),
          };
        }
      }
    } catch (dbErr) {
      console.warn("[api/stats] Failed to load revenue categories from database:", dbErr);
    }

    // ── 3. Patch prior year revenue if missing from projection ──
    if (trends?.projection && !trends.projection.priorYearActualRevenue && revenueCategories) {
      const priorYear = trends.projection.year - 1;
      const start = revenueCategories.periodStart || "";
      const end = revenueCategories.periodEnd || "";
      if (start.startsWith(String(priorYear)) && revenueCategories.totalNetRevenue > 0) {
        const s = new Date(start + "T00:00:00");
        const e = new Date(end + "T00:00:00");
        const months = (e.getFullYear() - s.getFullYear()) * 12 + (e.getMonth() - s.getMonth());
        const actualRev = months >= 11
          ? Math.round(revenueCategories.totalNetRevenue)
          : Math.round(revenueCategories.totalNetRevenue);
        trends.projection.priorYearActualRevenue = actualRev;
        if (!trends.projection.priorYearRevenue || trends.projection.priorYearRevenue < actualRev) {
          trends.projection.priorYearRevenue = actualRev;
        }
        console.log(`[api/stats] Patched priorYearActualRevenue=$${actualRev} from revenueCategories`);
      }
    }

    // ── 4. Month-over-month YoY comparison ──────────────────
    let monthOverMonth = null;
    try {
      const now = new Date();
      let lastMonth = now.getMonth(); // 0-indexed current month
      let lastMonthYear = now.getFullYear();
      if (lastMonth === 0) {
        lastMonth = 12;
        lastMonthYear -= 1;
      }

      const current = await getMonthlyRevenue(lastMonthYear, lastMonth);
      const priorYear = await getMonthlyRevenue(lastMonthYear - 1, lastMonth);

      if (current || priorYear) {
        const monthNames = ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
        monthOverMonth = {
          month: lastMonth,
          monthName: monthNames[lastMonth],
          current: current ? {
            year: lastMonthYear,
            periodStart: current.periodStart,
            periodEnd: current.periodEnd,
            gross: Math.round(current.totalRevenue * 100) / 100,
            net: Math.round(current.totalNetRevenue * 100) / 100,
            categoryCount: current.categoryCount,
          } : null,
          priorYear: priorYear ? {
            year: lastMonthYear - 1,
            periodStart: priorYear.periodStart,
            periodEnd: priorYear.periodEnd,
            gross: Math.round(priorYear.totalRevenue * 100) / 100,
            net: Math.round(priorYear.totalNetRevenue * 100) / 100,
            categoryCount: priorYear.categoryCount,
          } : null,
          yoyGrossChange: current && priorYear
            ? Math.round((current.totalRevenue - priorYear.totalRevenue) * 100) / 100
            : null,
          yoyNetChange: current && priorYear
            ? Math.round((current.totalNetRevenue - priorYear.totalNetRevenue) * 100) / 100
            : null,
          yoyGrossPct: current && priorYear && priorYear.totalRevenue > 0
            ? Math.round((current.totalRevenue - priorYear.totalRevenue) / priorYear.totalRevenue * 1000) / 10
            : null,
          yoyNetPct: current && priorYear && priorYear.totalNetRevenue > 0
            ? Math.round((current.totalNetRevenue - priorYear.totalNetRevenue) / priorYear.totalNetRevenue * 1000) / 10
            : null,
        };
        console.log(`[api/stats] MonthOverMonth: ${monthNames[lastMonth]} ${lastMonthYear} vs ${lastMonthYear - 1}`);
      }
    } catch (err) {
      console.warn("[api/stats] Failed to compute month-over-month:", err);
    }

    // ── 5. Monthly revenue timeline ──────────────────────────
    let monthlyRevenue: { month: string; gross: number; net: number }[] = [];
    try {
      const allMonthly = await getAllMonthlyRevenue();
      monthlyRevenue = allMonthly.map((m) => ({
        month: m.periodStart.slice(0, 7),
        gross: Math.round(m.totalRevenue * 100) / 100,
        net: Math.round(m.totalNetRevenue * 100) / 100,
      }));

      // Override currentMonthRevenue with actual monthly data if available
      if (stats && monthlyRevenue.length > 0) {
        const now = new Date();
        const currentMonthKey = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, "0")}`;
        const prevMonthDate = new Date(now.getFullYear(), now.getMonth() - 1, 1);
        const prevMonthKey = `${prevMonthDate.getFullYear()}-${String(prevMonthDate.getMonth() + 1).padStart(2, "0")}`;

        const currentEntry = monthlyRevenue.find((m) => m.month === currentMonthKey);
        const prevEntry = monthlyRevenue.find((m) => m.month === prevMonthKey);

        if (currentEntry) {
          stats.currentMonthRevenue = currentEntry.net;
        }
        if (prevEntry) {
          stats.previousMonthRevenue = prevEntry.net;
        }
      }

      console.log(`[api/stats] Monthly revenue timeline: ${monthlyRevenue.length} months`);
    } catch (err) {
      console.warn("[api/stats] Failed to load monthly revenue timeline:", err);
    }

    // ── 6. Return response ──────────────────────────────────
    return NextResponse.json({
      ...(stats || {}),
      trends,
      revenueCategories,
      monthOverMonth,
      monthlyRevenue,
      dataSource: "database",
    });
  } catch (error) {
    const message =
      error instanceof Error ? error.message : "Failed to load stats";
    console.error("[api/stats] Error:", message);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
