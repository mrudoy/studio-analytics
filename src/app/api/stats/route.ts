import { NextResponse } from "next/server";
import { getLatestPeriod, getRevenueForPeriod, getAllMonthlyRevenue, getMonthlyMerchRevenue, getAnnualRevenueBreakdown } from "@/lib/db/revenue-store";
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
  getShopifyAnnualRevenue,
  getShopifyCategoryBreakdown,
} from "@/lib/db/shopify-store";
import { getSpaStats } from "@/lib/db/spa-store";
import { getPool } from "@/lib/db/database";
import type { RevenueCategory } from "@/types/union-data";
import type { DashboardStats, DataFreshness, ShopifyStats, ShopifyMerchData, SpaData } from "@/types/dashboard";
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
    // NOTE: Built after monthlyRevenue + deduplication (section 5/6) so both
    // the month breakdown card and YoY use the same adjusted numbers.
    // Placeholder — computed below after deduplication.
    let monthOverMonth = null;

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
      const [shopifyStats, mtd, revSummary, topProducts, repeatData, customerBreakdown, annualRevenue, categoryBreakdown] = await Promise.all([
        getShopifyStats(),
        getShopifyMTDRevenue(),
        getShopifyRevenueSummary(),
        getShopifyTopProducts(10),
        getShopifyRepeatCustomerRate(),
        getShopifyCustomerBreakdown(),
        getShopifyAnnualRevenue(),
        getShopifyCategoryBreakdown(),
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
          annualRevenue,
          categoryBreakdown,
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

    // ── 7. Month-over-month YoY (uses deduplicated monthlyRevenue) ──
    try {
      const now2 = new Date();
      let lastMonth = now2.getMonth(); // 0-indexed current month
      let lastMonthYear = now2.getFullYear();
      if (lastMonth === 0) {
        lastMonth = 12;
        lastMonthYear -= 1;
      }

      const currentKey = `${lastMonthYear}-${String(lastMonth).padStart(2, "0")}`;
      const priorKey = `${lastMonthYear - 1}-${String(lastMonth).padStart(2, "0")}`;
      const currentEntry = monthlyRevenue.find((m) => m.month === currentKey);
      const priorEntry = monthlyRevenue.find((m) => m.month === priorKey);

      if (currentEntry || priorEntry) {
        const monthNames = ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
        const cGross = currentEntry?.gross ?? 0;
        const cNet = currentEntry?.net ?? 0;
        const pGross = priorEntry?.gross ?? 0;
        const pNet = priorEntry?.net ?? 0;

        monthOverMonth = {
          month: lastMonth,
          monthName: monthNames[lastMonth],
          current: currentEntry ? { year: lastMonthYear, gross: cGross, net: cNet } : null,
          priorYear: priorEntry ? { year: lastMonthYear - 1, gross: pGross, net: pNet } : null,
          yoyGrossChange: currentEntry && priorEntry ? Math.round((cGross - pGross) * 100) / 100 : null,
          yoyNetChange: currentEntry && priorEntry ? Math.round((cNet - pNet) * 100) / 100 : null,
          yoyGrossPct: currentEntry && priorEntry && pGross > 0
            ? Math.round((cGross - pGross) / pGross * 1000) / 10 : null,
          yoyNetPct: currentEntry && priorEntry && pNet > 0
            ? Math.round((cNet - pNet) / pNet * 1000) / 10 : null,
        };
        console.log(`[api/stats] MonthOverMonth: ${monthNames[lastMonth]} ${lastMonthYear} vs ${lastMonthYear - 1} (using deduplicated data)`);
      }
    } catch (err) {
      console.warn("[api/stats] Failed to compute month-over-month:", err);
    }

    // ── 8. Spa stats ────────────────────────────────────────
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

    // ── 8. Annual revenue breakdown by segment ─────────────
    let annualBreakdown: import("@/types/dashboard").AnnualRevenueBreakdown[] | null = null;
    try {
      const raw = await getAnnualRevenueBreakdown();

      // Replace Union.fit "Merch" with Shopify totals for deduplication
      if (shopifyMerch?.annualRevenue) {
        const shopifyByYear = new Map(shopifyMerch.annualRevenue.map((a) => [a.year, a]));
        for (const yearData of raw) {
          const shopYear = shopifyByYear.get(yearData.year);
          if (shopYear) {
            const merchIdx = yearData.segments.findIndex((s) => s.segment === "Merch");
            const unionMerchGross = merchIdx >= 0 ? yearData.segments[merchIdx].gross : 0;
            const unionMerchNet = merchIdx >= 0 ? yearData.segments[merchIdx].net : 0;
            if (merchIdx >= 0) {
              yearData.segments[merchIdx].gross = shopYear.gross;
              yearData.segments[merchIdx].net = shopYear.net;
            } else {
              yearData.segments.push({ segment: "Merch", gross: shopYear.gross, net: shopYear.net });
            }
            yearData.totalGross = yearData.totalGross - unionMerchGross + shopYear.gross;
            yearData.totalNet = yearData.totalNet - unionMerchNet + shopYear.net;
          }
        }
      }

      annualBreakdown = raw;
      console.log(`[api/stats] Annual breakdown: ${raw.length} years`);
    } catch (err) {
      console.warn("[api/stats] Annual breakdown failed:", err);
    }

    // ── 9. Data freshness ──────────────────────────────────
    let dataFreshness: DataFreshness | null = null;
    try {
      const pool = getPool();
      const [arRes, regRes, shopRes, pipeRes] = await Promise.all([
        pool.query("SELECT MAX(imported_at) AS ts FROM auto_renews").catch(() => ({ rows: [{ ts: null }] })),
        pool.query("SELECT MAX(imported_at) AS ts FROM registrations").catch(() => ({ rows: [{ ts: null }] })),
        pool.query("SELECT MAX(synced_at) AS ts FROM shopify_orders").catch(() => ({ rows: [{ ts: null }] })),
        pool.query("SELECT MAX(ran_at) AS ts FROM pipeline_runs").catch(() => ({ rows: [{ ts: null }] })),
      ]);

      const timestamps = [arRes.rows[0].ts, regRes.rows[0].ts, shopRes.rows[0].ts, pipeRes.rows[0].ts]
        .filter(Boolean)
        .map((t: string) => new Date(t).getTime());
      const overall = timestamps.length > 0 ? new Date(Math.max(...timestamps)).toISOString() : null;

      // Partial = union and shopify updated at different times (>1h apart)
      const unionTs = [arRes.rows[0].ts, regRes.rows[0].ts].filter(Boolean).map((t: string) => new Date(t).getTime());
      const shopifyTs = shopRes.rows[0].ts ? new Date(shopRes.rows[0].ts).getTime() : null;
      const latestUnion = unionTs.length > 0 ? Math.max(...unionTs) : null;
      const isPartial = !!(latestUnion && shopifyTs && Math.abs(latestUnion - shopifyTs) > 60 * 60 * 1000);

      dataFreshness = {
        unionAutoRenews: arRes.rows[0].ts ? new Date(arRes.rows[0].ts).toISOString() : null,
        unionRegistrations: regRes.rows[0].ts ? new Date(regRes.rows[0].ts).toISOString() : null,
        shopifySync: shopRes.rows[0].ts ? new Date(shopRes.rows[0].ts).toISOString() : null,
        lastPipelineRun: pipeRes.rows[0].ts ? new Date(pipeRes.rows[0].ts).toISOString() : null,
        overall,
        isPartial,
      };
    } catch (err) {
      console.warn("[api/stats] Data freshness query failed:", err);
    }

    // ── 10. Return response ──────────────────────────────────
    if (stats) {
      stats.dataFreshness = dataFreshness;
      stats.annualBreakdown = annualBreakdown;
    }
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
