import { NextResponse } from "next/server";
import { getLatestPeriod, getRevenueForPeriod, getMonthlyRevenue, getAllMonthlyRevenue, getMonthlyMerchRevenue } from "@/lib/db/revenue-store";
import { analyzeRevenueCategories } from "@/lib/analytics/revenue-categories";
import { computeStatsFromDB } from "@/lib/analytics/db-stats";
import { computeTrendsFromDB } from "@/lib/analytics/db-trends";
import {
  getShopifyStats,
  getShopifyMTDRevenue,
  getShopifyRevenueSummary,
  getShopifyTopProducts,
  getShopifyRepeatCustomerRate,
  getShopifyCustomerBreakdown,
} from "@/lib/db/shopify-store";
import { getSpaStats } from "@/lib/db/spa-store";
import type { RevenueCategory } from "@/types/union-data";
import type { DashboardStats, ShopifyStats, ShopifyMerchData, SpaData } from "@/types/dashboard";
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

    // ── 6. Shopify stats + merch data ────────────────────────
    let shopify: ShopifyStats | null = null;
    let shopifyMerch: ShopifyMerchData | null = null;
    try {
      const [shopifyStats, mtd, revSummary, topProducts, repeatData, customerBreakdown] = await Promise.all([
        getShopifyStats(),
        getShopifyMTDRevenue(),
        getShopifyRevenueSummary(),
        getShopifyTopProducts(3),
        getShopifyRepeatCustomerRate(),
        getShopifyCustomerBreakdown(),
      ]);

      shopify = shopifyStats;

      if (shopifyStats && shopifyStats.totalOrders > 0) {
        console.log(`[api/stats] Shopify: ${shopifyStats.totalOrders} orders, $${shopifyStats.totalRevenue} revenue`);

        // Compute average monthly revenue from completed months only
        const now = new Date();
        const currentMonthKey = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, "0")}`;
        const completedMonths = revSummary.filter((m) => m.month < currentMonthKey);
        const avgMonthlyRevenue = completedMonths.length > 0
          ? Math.round(completedMonths.reduce((sum, m) => sum + m.gross, 0) / completedMonths.length)
          : 0;

        shopifyMerch = {
          mtdRevenue: mtd,
          monthlyRevenue: revSummary,
          avgMonthlyRevenue,
          topProducts,
          repeatCustomerRate: repeatData.repeatRate,
          repeatCustomerCount: repeatData.repeatCount,
          totalCustomersWithOrders: repeatData.totalWithOrders,
          customerBreakdown: customerBreakdown.total.orders > 0 ? customerBreakdown : null,
        };

        // ── Revenue deduplication ──────────────────────────────
        // Union.fit has "Merch" + "Products" categories that overlap with Shopify.
        // For months where Shopify data exists, replace Union.fit merch with Shopify totals.
        // Formula: adjusted = unionTotal - unionMerch + shopifyGross
        if (monthlyRevenue.length > 0) {
          try {
            const unionMerch = await getMonthlyMerchRevenue();
            const shopifyByMonth = new Map(revSummary.map((m) => [m.month, m]));

            for (let i = 0; i < monthlyRevenue.length; i++) {
              const m = monthlyRevenue[i];
              const shopMonth = shopifyByMonth.get(m.month);
              if (shopMonth) {
                const deduction = unionMerch.get(m.month);
                const deductGross = deduction?.gross ?? 0;
                const deductNet = deduction?.net ?? 0;
                monthlyRevenue[i] = {
                  month: m.month,
                  gross: Math.round((m.gross - deductGross + shopMonth.gross) * 100) / 100,
                  net: Math.round((m.net - deductNet + shopMonth.net) * 100) / 100,
                };
              }
            }

            // Update current/previous month revenue on stats
            if (stats) {
              const currentEntry = monthlyRevenue.find((m) => m.month === currentMonthKey);
              const prevMonthDate = new Date(now.getFullYear(), now.getMonth() - 1, 1);
              const prevMonthKey = `${prevMonthDate.getFullYear()}-${String(prevMonthDate.getMonth() + 1).padStart(2, "0")}`;
              const prevEntry = monthlyRevenue.find((m) => m.month === prevMonthKey);
              if (currentEntry) stats.currentMonthRevenue = currentEntry.net;
              if (prevEntry) stats.previousMonthRevenue = prevEntry.net;
            }

            console.log(`[api/stats] Revenue deduplication applied for ${shopifyByMonth.size} Shopify months`);
          } catch (err) {
            console.warn("[api/stats] Revenue deduplication failed:", err);
          }
        }
      }
    } catch {
      // Shopify tables might not exist yet (migration hasn't run)
    }

    // ── 7. Spa stats ────────────────────────────────────────
    let spa: SpaData | null = null;
    try {
      const spaStats = await getSpaStats();
      if (spaStats.totalRevenue > 0) {
        spa = {
          mtdRevenue: spaStats.mtdRevenue,
          avgMonthlyRevenue: spaStats.avgMonthlyRevenue,
          totalRevenue: spaStats.totalRevenue,
          monthlyRevenue: spaStats.monthlyRevenue.map((m) => ({ month: m.month, gross: m.gross, net: m.net })),
          serviceBreakdown: spaStats.serviceBreakdown.map((s) => ({
            category: s.category,
            totalRevenue: s.totalRevenue,
            totalNetRevenue: s.totalNetRevenue,
          })),
        };
      }
    } catch {
      // Spa data may not exist yet
    }

    // ── 8. Return response ──────────────────────────────────
    return NextResponse.json({
      ...(stats || {}),
      trends,
      revenueCategories,
      monthOverMonth,
      monthlyRevenue,
      shopify,
      shopifyMerch,
      spa,
      dataSource: "database",
    });
  } catch (error) {
    const message =
      error instanceof Error ? error.message : "Failed to load stats";
    console.error("[api/stats] Error:", message);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
