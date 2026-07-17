import { NextResponse } from "next/server";
import { getLatestPeriod, getRevenueForPeriod, getAllMonthlyRevenue, getAnnualRevenueBreakdown, getMonthlyRentalRevenue, getAnnualRentalRevenue, getMonthlyRetreatRevenue, getMonthlySubscriptionBilling } from "@/lib/db/revenue-store";
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
  getShopifyDailySales,
} from "@/lib/db/shopify-store";
import { getSpaStats } from "@/lib/db/spa-store";
import { getDataFreshness } from "@/lib/union-api/fetch-export";
import { computeAvgChurnRates } from "@/lib/analytics/metrics/subscriber-movement";
import { getPool } from "@/lib/db/database";
import { getStatsCache, setStatsCache, getStatsCacheAge } from "@/lib/cache/stats-cache";
import type { RevenueCategory } from "@/types/union-data";
import type { DashboardStats, DataFreshness, ShopifyStats, ShopifyMerchData, SpaData } from "@/types/dashboard";
import type { TrendsData } from "@/types/dashboard";

/** Safely await a promise, returning null on failure */
async function safe<T>(p: Promise<T>): Promise<T | null> {
  try { return await p; } catch { return null; }
}

export async function GET(request: Request) {
  try {
    // ── 0. Check in-memory cache ──────────────────────────────
    const url = new URL(request.url);
    const noCache = url.searchParams.get("nocache") === "1";

    if (!noCache) {
      const cached = await getStatsCache();
      if (cached) {
        const age = getStatsCacheAge();
        console.log(`[api/stats] Serving from cache (age: ${age}s)`);
        return NextResponse.json(cached, {
          headers: { "X-Cache": "HIT", "X-Cache-Age": String(age ?? 0) },
        });
      }
    }

    const t0 = Date.now();

    // ══════════════════════════════════════════════════════════════
    // PHASE 1: Fire ALL independent DB queries in parallel
    // ══════════════════════════════════════════════════════════════
    const pool = getPool();

    const [
      statsResult,
      trendsResult,
      latestPeriodResult,
      monthlyRevenueResult,
      shopifyResult,
      spaResult,
      rentalResult,
      annualBreakdownResult,
      freshnessResult,
      insightsResult,
      overviewResult,
      unionRentalResult,
      movementResult,
      exportFreshnessResult,
    ] = await Promise.all([
      // 1. Core stats
      safe(computeStatsFromDB()),
      // 2. Trends (the heaviest — many sub-queries, but internally parallelized)
      safe(computeTrendsFromDB()),
      // 3. Latest revenue period
      safe(getLatestPeriod()),
      // 4. Monthly revenue + retreat + subscription billing
      safe(Promise.all([getAllMonthlyRevenue(), getMonthlyRetreatRevenue(), getMonthlySubscriptionBilling()])),
      // 5. All 9 Shopify queries
      safe(Promise.all([
        getShopifyStats(),
        getShopifyMTDRevenue(),
        getShopifyRevenueSummary(),
        getShopifyTopProducts(10),
        getShopifyRepeatCustomerRate(),
        getShopifyCustomerBreakdown(),
        getShopifyAnnualRevenue(),
        getShopifyCategoryBreakdown(),
        getShopifyDailySales(30),
      ])),
      // 6. Spa
      safe(getSpaStats()),
      // 7. Rentals
      safe(Promise.all([getMonthlyRentalRevenue(), getAnnualRentalRevenue()])),
      // 8. Annual breakdown
      safe(getAnnualRevenueBreakdown()),
      // 9. Data freshness (4 MAX queries)
      safe(Promise.all([
        pool.query("SELECT MAX(imported_at) AS ts FROM auto_renews").catch(() => ({ rows: [{ ts: null }] })),
        pool.query("SELECT MAX(imported_at) AS ts FROM registrations").catch(() => ({ rows: [{ ts: null }] })),
        pool.query("SELECT MAX(synced_at) AS ts FROM shopify_orders").catch(() => ({ rows: [{ ts: null }] })),
        pool.query("SELECT MAX(ran_at) AS ts FROM pipeline_runs").catch(() => ({ rows: [{ ts: null }] })),
      ])),
      // 10. Insights
      safe(import("@/lib/db/insights-store").then((m) => m.getRecentInsights(20))),
      // 11. Overview
      safe(import("@/lib/db/overview-store").then((m) => m.getOverviewData())),
      // 12. Union rental dedup query (needed for rental merge)
      safe(pool.query(`
        SELECT TO_CHAR(period_start, 'YYYY-MM') AS month,
               SUM(revenue) AS gross, SUM(net_revenue) AS net
        FROM revenue_categories
        WHERE (category ~* 'rental|teacher\\s*rental|studio\\s*rental')
          AND DATE_TRUNC('month', period_start) = DATE_TRUNC('month', period_end)
        GROUP BY TO_CHAR(period_start, 'YYYY-MM')
      `)),
      // 13. Subscriber movement (canonical source for new + canceled counts per window/period)
      safe(import("@/lib/analytics/metrics/subscriber-movement").then((m) => m.getSubscriberMovement())),
      // 14. Export-log freshness (same check as digest email)
      safe(getDataFreshness()),
    ]);

    const t1 = Date.now();
    console.log(`[api/stats] Phase 1 (parallel queries): ${t1 - t0}ms`);

    // ══════════════════════════════════════════════════════════════
    // PHASE 2: Assemble results (in-memory only, no DB calls)
    // ══════════════════════════════════════════════════════════════

    const stats: DashboardStats | null = statsResult;
    const trends: TrendsData | null = trendsResult;

    if (!stats) {
      return NextResponse.json(
        { error: "No data available — database is empty. Upload data or run the pipeline." },
        { status: 503 }
      );
    }

    // ── Revenue categories ──
    let revenueCategories = null;
    if (latestPeriodResult) {
      try {
        const rows = await getRevenueForPeriod(latestPeriodResult.periodStart, latestPeriodResult.periodEnd);
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
            periodStart: latestPeriodResult.periodStart,
            periodEnd: latestPeriodResult.periodEnd,
            ...analyzeRevenueCategories(asRevenueCategory),
          };
        }
      } catch (dbErr) {
        console.warn("[api/stats] Failed to load revenue categories:", dbErr);
      }
    }

    // ── Patch prior year revenue if missing from projection ──
    if (trends?.projection && !trends.projection.priorYearActualRevenue && revenueCategories) {
      const priorYear = trends.projection.year - 1;
      const start = revenueCategories.periodStart || "";
      if (start.startsWith(String(priorYear)) && revenueCategories.totalNetRevenue > 0) {
        const retreatCat = revenueCategories.categories?.find(
          (c: { category: string }) => c.category === "Retreats"
        );
        const retreatNetRev = retreatCat?.netRevenue ?? 0;
        const actualRev = Math.round(revenueCategories.totalNetRevenue - retreatNetRev);
        trends.projection.priorYearActualRevenue = actualRev;
        if (!trends.projection.priorYearRevenue || trends.projection.priorYearRevenue < actualRev) {
          trends.projection.priorYearRevenue = actualRev;
        }
      }
    }

    // ── Build monthly revenue timeline ──
    let monthlyRevenue: { month: string; gross: number; net: number; retreatGross?: number; retreatNet?: number }[] = [];
    if (monthlyRevenueResult) {
      const [allMonthly, retreatByMonth, subBillingByMonth] = monthlyRevenueResult;
      monthlyRevenue = allMonthly.map((m) => {
        const mKey = m.periodStart.slice(0, 7);
        const retreat = retreatByMonth.get(mKey);
        const retreatGross = retreat ? Math.round(retreat.gross * 100) / 100 : 0;
        const retreatNet = retreat ? Math.round(retreat.net * 100) / 100 : 0;
        return {
          month: mKey,
          gross: Math.round(m.totalRevenue * 100) / 100 - retreatGross,
          net: Math.round(m.totalNetRevenue * 100) / 100 - retreatNet,
          retreatGross,
          retreatNet,
        };
      });

      // Patch current/previous month on stats
      const now = new Date();
      const currentMonthKey = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, "0")}`;
      const prevMonthDate = new Date(now.getFullYear(), now.getMonth() - 1, 1);
      const prevMonthKey = `${prevMonthDate.getFullYear()}-${String(prevMonthDate.getMonth() + 1).padStart(2, "0")}`;

      const currentEntry = monthlyRevenue.find((m) => m.month === currentMonthKey);
      const prevEntry = monthlyRevenue.find((m) => m.month === prevMonthKey);
      if (currentEntry) stats.currentMonthRevenue = currentEntry.net;
      if (prevEntry) stats.previousMonthRevenue = prevEntry.net;

      // Subscription run-rate: current and previous month from auto_renews (not orders).
      // Both values are full-month run-rates, not partial cash — no pacing applied.
      const currentSubBilling = subBillingByMonth.get(currentMonthKey);
      const lastSubBilling = subBillingByMonth.get(prevMonthKey);
      const currentActual = Math.round((currentSubBilling?.gross ?? 0) * 100) / 100;
      stats.subscriptionBilling = {
        currentMonth: currentMonthKey,
        currentMonthActual: currentActual,
        lastMonth: prevMonthKey,
        lastMonthTotal: Math.round((lastSubBilling?.gross ?? 0) * 100) / 100,
      };
    }

    // ── Shopify stats + merch ──
    let shopify: ShopifyStats | null = null;
    let shopifyMerch: ShopifyMerchData | null = null;
    if (shopifyResult) {
      const [shopifyStats, mtd, revSummary, topProducts, repeatData, customerBreakdown, annualRevenue, categoryBreakdown, dailySales] = shopifyResult;
      shopify = shopifyStats;

      if (shopifyStats && shopifyStats.totalOrders > 0) {
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
          dailySales,
        };

        // Merge Shopify into monthlyRevenue
        if (monthlyRevenue.length > 0) {
          const shopifyByMonth = new Map(revSummary.map((m) => [m.month, m]));
          for (let i = 0; i < monthlyRevenue.length; i++) {
            const m = monthlyRevenue[i];
            const shopMonth = shopifyByMonth.get(m.month);
            if (shopMonth) {
              monthlyRevenue[i] = {
                ...m,
                gross: Math.round((m.gross + shopMonth.gross) * 100) / 100,
                net: Math.round((m.net + shopMonth.net) * 100) / 100,
              };
            }
          }

          // Re-update current/previous month stats
          const prevMonthDate = new Date(now.getFullYear(), now.getMonth() - 1, 1);
          const prevMonthKey = `${prevMonthDate.getFullYear()}-${String(prevMonthDate.getMonth() + 1).padStart(2, "0")}`;
          const currentEntry = monthlyRevenue.find((m) => m.month === currentMonthKey);
          const prevEntry = monthlyRevenue.find((m) => m.month === prevMonthKey);
          if (currentEntry) stats.currentMonthRevenue = currentEntry.net;
          if (prevEntry) stats.previousMonthRevenue = prevEntry.net;
        }
      }
    }

    // ── Spa ──
    let spa: SpaData | null = null;
    if (spaResult && spaResult.totalRevenue > 0) {
      spa = {
        mtdRevenue: spaResult.mtdRevenue,
        avgMonthlyRevenue: spaResult.avgMonthlyRevenue,
        totalRevenue: spaResult.totalRevenue,
        monthlyRevenue: spaResult.monthlyRevenue.map((m) => ({ month: m.month, gross: m.gross, net: m.net })),
        serviceBreakdown: spaResult.serviceBreakdown.map((s) => ({
          category: s.category,
          totalRevenue: s.totalRevenue,
          totalNetRevenue: s.totalNetRevenue,
        })),
        customerBehavior: spaResult.customerBehavior ?? null,
      };
    }

    // ── Rentals + dedup ──
    let rentalRevenue: import("@/types/dashboard").RentalRevenueData | null = null;
    if (rentalResult) {
      const [rentalMonthly, rentalAnnual] = rentalResult;
      rentalRevenue = { monthly: rentalMonthly, annual: rentalAnnual };

      // Dedup: replace Union.fit rental with spreadsheet totals in monthlyRevenue
      if (monthlyRevenue.length > 0 && unionRentalResult) {
        const unionRentalByMonth = new Map(
          unionRentalResult.rows.map((r: Record<string, unknown>) => [r.month as string, { gross: Number(r.gross) || 0, net: Number(r.net) || 0 }])
        );
        const ssRentalByMonth = new Map(rentalMonthly.map((r) => [r.month, r.total]));

        for (let i = 0; i < monthlyRevenue.length; i++) {
          const m = monthlyRevenue[i];
          const unionR = unionRentalByMonth.get(m.month);
          const ssR = ssRentalByMonth.get(m.month);
          if (unionR || ssR) {
            const deductGross = unionR?.gross ?? 0;
            const deductNet = unionR?.net ?? 0;
            const addAmount = ssR ?? 0;
            monthlyRevenue[i] = {
              ...m,
              gross: Math.round((m.gross - deductGross + addAmount) * 100) / 100,
              net: Math.round((m.net - deductNet + addAmount) * 100) / 100,
            };
          }
        }

        // Re-update current/previous month stats after rental dedup
        const now3 = new Date();
        const curKey = `${now3.getFullYear()}-${String(now3.getMonth() + 1).padStart(2, "0")}`;
        const prevD = new Date(now3.getFullYear(), now3.getMonth() - 1, 1);
        const prevKey = `${prevD.getFullYear()}-${String(prevD.getMonth() + 1).padStart(2, "0")}`;
        const curE = monthlyRevenue.find((m) => m.month === curKey);
        const prevE = monthlyRevenue.find((m) => m.month === prevKey);
        if (curE) stats.currentMonthRevenue = curE.net;
        if (prevE) stats.previousMonthRevenue = prevE.net;
      }
    }

    // ── Month-over-month YoY ──
    let monthOverMonth = null;
    if (monthlyRevenue.length > 0) {
      const now2 = new Date();
      let lastMonth = now2.getMonth();
      let lastMonthYear = now2.getFullYear();
      if (lastMonth === 0) { lastMonth = 12; lastMonthYear -= 1; }

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
      }
    }

    // ── Annual breakdown + Shopify/rental adjustments ──
    let annualBreakdown: import("@/types/dashboard").AnnualRevenueBreakdown[] | null = null;
    if (annualBreakdownResult) {
      const raw = annualBreakdownResult;

      // Add Shopify merch
      if (shopifyMerch?.annualRevenue) {
        const shopifyByYear = new Map(shopifyMerch.annualRevenue.map((a) => [a.year, a]));
        for (const yearData of raw) {
          const shopYear = shopifyByYear.get(yearData.year);
          if (shopYear) {
            const merchIdx = yearData.segments.findIndex((s) => s.segment === "Merch");
            if (merchIdx >= 0) {
              yearData.segments[merchIdx].gross += shopYear.gross;
              yearData.segments[merchIdx].net += shopYear.net;
            } else {
              yearData.segments.push({ segment: "Merch", gross: shopYear.gross, net: shopYear.net });
            }
            yearData.totalGross += shopYear.gross;
            yearData.totalNet += shopYear.net;
          }
        }
      }

      // Dedup rentals
      if (rentalRevenue?.annual) {
        const ssRentalByYear = new Map(rentalRevenue.annual.map((a) => [a.year, a.total]));
        for (const yearData of raw) {
          const ssTotal = ssRentalByYear.get(yearData.year);
          if (ssTotal !== undefined) {
            const rentalIdx = yearData.segments.findIndex((s) => s.segment === "Rentals");
            const unionRentalGross = rentalIdx >= 0 ? yearData.segments[rentalIdx].gross : 0;
            const unionRentalNet = rentalIdx >= 0 ? yearData.segments[rentalIdx].net : 0;
            if (rentalIdx >= 0) {
              yearData.segments[rentalIdx].gross = ssTotal;
              yearData.segments[rentalIdx].net = ssTotal;
            } else {
              yearData.segments.push({ segment: "Rentals", gross: ssTotal, net: ssTotal });
            }
            yearData.totalGross = yearData.totalGross - unionRentalGross + ssTotal;
            yearData.totalNet = yearData.totalNet - unionRentalNet + ssTotal;
          }
        }
      }

      // Exclude retreat revenue from annual totals
      for (const yearData of raw) {
        const retreatSeg = yearData.segments.find((s) => s.segment === "Retreats");
        if (retreatSeg) {
          yearData.totalGross -= retreatSeg.gross;
          yearData.totalNet -= retreatSeg.net;
        }
      }

      annualBreakdown = raw;
    }

    // ── Data freshness ──
    let dataFreshness: DataFreshness | null = null;
    if (freshnessResult) {
      const [arRes, regRes, shopRes, pipeRes] = freshnessResult;
      const timestamps = [arRes.rows[0].ts, regRes.rows[0].ts, shopRes.rows[0].ts, pipeRes.rows[0].ts]
        .filter(Boolean)
        .map((t: string) => new Date(t).getTime());
      const overall = timestamps.length > 0 ? new Date(Math.max(...timestamps)).toISOString() : null;

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
        ...(exportFreshnessResult && {
          isFresh: exportFreshnessResult.isFresh,
          daysStale: exportFreshnessResult.daysStale,
          latestDataDate: exportFreshnessResult.latestDataDate,
        }),
      };
    }

    const t2 = Date.now();
    console.log(`[api/stats] Phase 2 (assembly): ${t2 - t1}ms | Total: ${t2 - t0}ms`);

    // ── Overwrite churn/new counts in trends.weekly and trends.monthly from the
    //    canonical movement source. This ensures every card reading from trends.*
    //    sees the same numbers as the Overview table (single source of truth).
    if (trends && movementResult) {
      const movementWeeklyByPeriod = new Map(movementResult.weekly.map((w) => [w.period, w]));
      for (const tw of trends.weekly) {
        const mv = movementWeeklyByPeriod.get(tw.period);
        if (!mv) continue;
        // Member weekly churn cards are labeled "Monthly-billed member churn rate",
        // so use the monthly-billed-only subset from canonical movement. Annual
        // members are not represented on the Members page weekly chart.
        const memMonthlyCanceled = mv.member.monthlyCanceled ?? mv.member.canceled;
        const memMonthlyActive = mv.member.monthlyActiveAtStart ?? mv.member.activeAtStart;
        tw.memberChurn = memMonthlyCanceled;
        tw.sky3Churn = mv.sky3.canceled;
        tw.skyTingTvChurn = mv.skyTingTv.canceled;
        tw.newMembers = mv.member.new;
        tw.newSky3 = mv.sky3.new;
        tw.newSkyTingTv = mv.skyTingTv.new;
        // Override weekly rate + active-at-start fields with canonical values
        tw.activeMembersAtWeekStart = memMonthlyActive;
        tw.activeSky3AtWeekStart = mv.sky3.activeAtStart;
        tw.activeSkyTingTvAtWeekStart = mv.skyTingTv.activeAtStart;
        tw.memberChurnPct = memMonthlyActive > 0
          ? Math.round((memMonthlyCanceled / memMonthlyActive) * 1000) / 10 : 0;
        tw.sky3ChurnPct = mv.sky3.activeAtStart > 0
          ? Math.round((mv.sky3.canceled / mv.sky3.activeAtStart) * 1000) / 10 : 0;
        tw.skyTingTvChurnPct = mv.skyTingTv.activeAtStart > 0
          ? Math.round((mv.skyTingTv.canceled / mv.skyTingTv.activeAtStart) * 1000) / 10 : 0;
      }
      const movementMonthlyByPeriod = new Map(movementResult.monthly.map((m) => [m.period, m]));
      for (const tm of trends.monthly) {
        const mv = movementMonthlyByPeriod.get(tm.period);
        if (!mv) continue;
        tm.memberChurn = mv.member.canceled;
        tm.sky3Churn = mv.sky3.canceled;
        tm.skyTingTvChurn = mv.skyTingTv.canceled;
        tm.newMembers = mv.member.new;
        tm.newSky3 = mv.sky3.new;
        tm.newSkyTingTv = mv.skyTingTv.new;
      }

      // Also override churnRates.byCategory[*].monthly[*] — that's what the
      // monthly churn cards read. We override canceledCount and activeAtStart
      // and recompute userChurnRate + mrrChurnRate accordingly.
      if (trends.churnRates?.byCategory) {
        const catKeys = ["member", "sky3", "skyTingTv"] as const;
        for (const key of catKeys) {
          const catData = trends.churnRates.byCategory[key];
          if (!catData) continue;
          for (const m of catData.monthly) {
            const mv = movementMonthlyByPeriod.get(m.month);
            if (!mv) continue;
            const mvCat = mv[key];
            m.canceledCount = mvCat.canceled;
            m.activeAtStart = mvCat.activeAtStart;
            m.canceledMrr = mvCat.canceledMrr;
            m.activeMrrAtStart = mvCat.activeMrrAtStart;
            m.userChurnRate = mvCat.activeAtStart > 0
              ? Math.round((mvCat.canceled / mvCat.activeAtStart) * 1000) / 10 : 0;
            m.mrrChurnRate = mvCat.activeMrrAtStart > 0
              ? Math.round((mvCat.canceledMrr / mvCat.activeMrrAtStart) * 1000) / 10 : 0;

            // MEMBER-only: also override the monthly-billed subset that drives
            // the "Monthly-billed member churn rate" card. Without this, the
            // displayed rate (eligibleChurnRate) was the stale legacy value
            // from db-trends.ts while the count above was the canonical one —
            // they didn't agree.
            if (key === "member") {
              const memMonthlyCanceled = mvCat.monthlyCanceled ?? mvCat.canceled;
              const memMonthlyActive = mvCat.monthlyActiveAtStart ?? mvCat.activeAtStart;
              const memMonthlyCanceledMrr = mvCat.monthlyCanceledMrr ?? mvCat.canceledMrr;
              const memMonthlyActiveMrr = mvCat.monthlyActiveMrrAtStart ?? mvCat.activeMrrAtStart;
              m.monthlyCanceledCount = memMonthlyCanceled;
              m.monthlyActiveAtStart = memMonthlyActive;
              m.monthlyCanceledMrr = memMonthlyCanceledMrr;
              m.monthlyActiveMrrAtStart = memMonthlyActiveMrr;
              m.eligibleChurnRate = memMonthlyActive > 0
                ? Math.round((memMonthlyCanceled / memMonthlyActive) * 1000) / 10 : 0;
              m.monthlyMrrChurnRate = memMonthlyActiveMrr > 0
                ? Math.round((memMonthlyCanceledMrr / memMonthlyActiveMrr) * 1000) / 10 : 0;
            }
          }
          // Recompute the 6-mo averages via canonical helper
          const avgs = computeAvgChurnRates(catData.monthly);
          catData.avgUserChurnRate = avgs.avgUserChurnRate;
          catData.avgMrrChurnRate = avgs.avgMrrChurnRate;

          // MEMBER-only: recompute avgEligibleChurnRate from canonical values.
          // Excludes current partial month and 2025-10 (bulk admin cleanup),
          // matching computeAvgChurnRates' exclusions.
          if (key === "member") {
            const completed = catData.monthly
              .slice(0, -1)
              .filter((mm) => mm.month !== "2025-10");
            catData.avgEligibleChurnRate = completed.length > 0
              ? Math.round(
                  (completed.reduce((s, mm) => s + (mm.eligibleChurnRate ?? 0), 0) /
                    completed.length) * 10,
                ) / 10
              : 0;
            catData.avgMonthlyMrrChurnRate = completed.length > 0
              ? Math.round(
                  (completed.reduce((s, mm) => s + (mm.monthlyMrrChurnRate ?? 0), 0) /
                    completed.length) * 10,
                ) / 10
              : 0;
          }
        }
        // Update legacy flat averages
        trends.churnRates.avgMemberRate = trends.churnRates.byCategory.member?.avgUserChurnRate ?? 0;
        trends.churnRates.avgSky3Rate = trends.churnRates.byCategory.sky3?.avgUserChurnRate ?? 0;
      }
    }

    // ── Return response ──
    stats.dataFreshness = dataFreshness;
    stats.annualBreakdown = annualBreakdown;
    stats.rentalRevenue = rentalRevenue;

    const responseBody = {
      ...(stats || {}),
      trends,
      revenueCategories,
      monthOverMonth,
      monthlyRevenue,
      shopify,
      shopifyMerch,
      spa,
      insights: insightsResult,
      overviewData: overviewResult,
      movement: movementResult,
      dataSource: "database",
    };

    await setStatsCache(responseBody);

    return NextResponse.json(responseBody, {
      headers: { "X-Cache": "MISS" },
    });
  } catch (error) {
    const message =
      error instanceof Error ? error.message : "Failed to load stats";
    console.error("[api/stats] Error:", message);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
