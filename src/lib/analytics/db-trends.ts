/**
 * Compute TrendsData from PostgreSQL data.
 *
 * Returns the same shape as readTrendsData() from Sheets so the
 * dashboard components need zero changes. Returns null if the database doesn't
 * have enough data (caller falls back to Sheets).
 *
 * Data sources:
 *   - auto_renews table    → weekly/monthly trends, pacing, projection
 *   - registrations table  → drop-in analytics
 *   - first_visits table   → first visit stats, returning non-members
 */

import { getCategory, isAnnualPlan } from "./categories";
import { parseDate, getWeekKey, getMonthKey } from "./date-utils";
import {
  getNewAutoRenews,
  getCanceledAutoRenews,
  getAutoRenewStats,
  hasAutoRenewData,
} from "../db/auto-renew-store";
import {
  hasRegistrationData,
  hasFirstVisitData,
  getFirstTimeUniqueVisitorsByWeek,
  getFirstTimeSourceBreakdown,
  getReturningUniqueVisitorsByWeek,
  getReturningSourceBreakdown,
  getNewCustomerVolumeByWeek,
  getNewCustomerCohorts,
  getDropInWeeklyDetail,
  getDropInWTD,
  getDropInLastWeekWTD,
  getDropInFrequencyDistribution,
  getIntroWeekCustomersByWeek,
  getConversionPoolWeekly,
  getConversionPoolWTD,
  getConversionPoolLagStats,
  getUsageFrequencyByCategory,
  type PoolSliceKey,
} from "../db/registration-store";
import type {
  TrendsData,
  TrendRowData,
  PacingData,
  ProjectionData,
  DropInModuleData,
  FirstVisitData,
  ReturningNonMemberData,
  ChurnRateData,
  CategoryChurnData,
  CategoryMonthlyChurn,
  NewCustomerVolumeData,
  NewCustomerCohortData,
  ConversionPoolModuleData,
  ConversionPoolSliceData,
  ConversionPoolSlice,
  UsageData,
  TenureMetrics,
  RenewalAlertMember,
  TenureMilestoneMember,
  MemberAlerts,
} from "@/types/dashboard";
import { getPool } from "../db/database";
import { getAllPeriods } from "../db/revenue-store";

// ── Helpers ─────────────────────────────────────────────────

interface PeriodBucket {
  newMembers: number;
  newSky3: number;
  newSkyTingTv: number;
  memberChurn: number;
  sky3Churn: number;
  skyTingTvChurn: number;
  revenueAdded: number;
  revenueLost: number;
}

function emptyBucket(): PeriodBucket {
  return {
    newMembers: 0, newSky3: 0, newSkyTingTv: 0,
    memberChurn: 0, sky3Churn: 0, skyTingTvChurn: 0,
    revenueAdded: 0, revenueLost: 0,
  };
}

function getOrCreate(map: Map<string, PeriodBucket>, key: string): PeriodBucket {
  if (!map.has(key)) map.set(key, emptyBucket());
  return map.get(key)!;
}

function bucketToTrendRow(period: string, type: string, b: PeriodBucket, prev: PeriodBucket | null): TrendRowData {
  const netMemberGrowth = b.newMembers - b.memberChurn;
  const netSky3Growth = b.newSky3 - b.sky3Churn;
  const revenueAdded = Math.round(b.revenueAdded * 100) / 100;
  const revenueLost = Math.round(b.revenueLost * 100) / 100;

  return {
    period,
    type,
    newMembers: b.newMembers,
    newSky3: b.newSky3,
    newSkyTingTv: b.newSkyTingTv,
    memberChurn: b.memberChurn,
    sky3Churn: b.sky3Churn,
    skyTingTvChurn: b.skyTingTvChurn,
    netMemberGrowth,
    netSky3Growth,
    revenueAdded,
    revenueLost,
    deltaNewMembers: prev ? b.newMembers - prev.newMembers : null,
    deltaNewSky3: prev ? b.newSky3 - prev.newSky3 : null,
    deltaRevenue: prev ? Math.round((b.revenueAdded - prev.revenueAdded) * 100) / 100 : null,
    deltaPctNewMembers: prev && prev.newMembers > 0
      ? Math.round(((b.newMembers - prev.newMembers) / prev.newMembers) * 1000) / 10
      : null,
    deltaPctNewSky3: prev && prev.newSky3 > 0
      ? Math.round(((b.newSky3 - prev.newSky3) / prev.newSky3) * 1000) / 10
      : null,
    deltaPctRevenue: prev && prev.revenueAdded > 0
      ? Math.round(((b.revenueAdded - prev.revenueAdded) / prev.revenueAdded) * 1000) / 10
      : null,
  };
}

// ── Main ────────────────────────────────────────────────────

export async function computeTrendsFromDB(): Promise<TrendsData | null> {
  if (!(await hasAutoRenewData())) {
    console.log("[db-trends] No auto-renew data — skipping");
    return null;
  }

  // ── 1. Build weekly/monthly buckets from auto-renew data ──
  const now = new Date();
  const currentMonthKey = getMonthKey(now);
  const currentWeekKey = getWeekKey(now);

  // Look back ~6 months for trend data
  const sixMonthsAgo = new Date(now);
  sixMonthsAgo.setMonth(sixMonthsAgo.getMonth() - 6);
  const startDate = `${sixMonthsAgo.getFullYear()}-${String(sixMonthsAgo.getMonth() + 1).padStart(2, "0")}-01`;
  const endDate = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, "0")}-${String(now.getDate() + 1).padStart(2, "0")}`;

  const newSubs = await getNewAutoRenews(startDate, endDate);
  const canceledSubs = await getCanceledAutoRenews(startDate, endDate);

  const weeklyBuckets = new Map<string, PeriodBucket>();
  const monthlyBuckets = new Map<string, PeriodBucket>();

  // Bucket new subscriptions
  for (const sub of newSubs) {
    const date = parseDate(sub.createdAt);
    if (!date) continue;
    const cat = sub.category;
    const wk = getWeekKey(date);
    const mo = getMonthKey(date);
    const wB = getOrCreate(weeklyBuckets, wk);
    const mB = getOrCreate(monthlyBuckets, mo);

    if (cat === "MEMBER") { wB.newMembers++; mB.newMembers++; }
    else if (cat === "SKY3") { wB.newSky3++; mB.newSky3++; }
    else if (cat === "SKY_TING_TV") { wB.newSkyTingTv++; mB.newSkyTingTv++; }

    // Revenue added (monthly rate for MRR tracking)
    wB.revenueAdded += sub.monthlyRate;
    mB.revenueAdded += sub.monthlyRate;
  }

  // Bucket canceled subscriptions
  for (const sub of canceledSubs) {
    const date = sub.canceledAt ? parseDate(sub.canceledAt) : null;
    if (!date) continue;
    const cat = sub.category;
    const wk = getWeekKey(date);
    const mo = getMonthKey(date);
    const wB = getOrCreate(weeklyBuckets, wk);
    const mB = getOrCreate(monthlyBuckets, mo);

    if (cat === "MEMBER") { wB.memberChurn++; mB.memberChurn++; }
    else if (cat === "SKY3") { wB.sky3Churn++; mB.sky3Churn++; }
    else if (cat === "SKY_TING_TV") { wB.skyTingTvChurn++; mB.skyTingTvChurn++; }

    wB.revenueLost += sub.monthlyRate;
    mB.revenueLost += sub.monthlyRate;
  }

  // ── 2. Convert to sorted arrays ──────────────────────────
  const allWeekKeys = Array.from(weeklyBuckets.keys()).sort();
  const allMonthKeys = Array.from(monthlyBuckets.keys()).sort();
  const recentWeekKeys = allWeekKeys.slice(-8);
  const recentMonthKeys = allMonthKeys.slice(-6);

  const weekly: TrendRowData[] = recentWeekKeys.map((wk, i) => {
    const bucket = weeklyBuckets.get(wk)!;
    const prevKey = i > 0 ? recentWeekKeys[i - 1] : null;
    const prevBucket = prevKey ? weeklyBuckets.get(prevKey)! : null;
    return bucketToTrendRow(wk, "Weekly", bucket, prevBucket);
  });

  const monthly: TrendRowData[] = recentMonthKeys.map((mo, i) => {
    const bucket = monthlyBuckets.get(mo)!;
    const prevKey = i > 0 ? recentMonthKeys[i - 1] : null;
    const prevBucket = prevKey ? monthlyBuckets.get(prevKey)! : null;
    return bucketToTrendRow(mo, "Monthly", bucket, prevBucket);
  });

  // ── 3. Current month pacing ──────────────────────────────
  const currentBucket = monthlyBuckets.get(currentMonthKey) || emptyBucket();
  const daysElapsed = now.getDate();
  const daysInMonth = new Date(now.getFullYear(), now.getMonth() + 1, 0).getDate();
  const pacingMultiplier = daysElapsed > 0 ? daysInMonth / daysElapsed : 1;

  const pacing: PacingData = {
    month: currentMonthKey,
    daysElapsed,
    daysInMonth,
    newMembersActual: currentBucket.newMembers,
    newMembersPaced: Math.round(currentBucket.newMembers * pacingMultiplier),
    newSky3Actual: currentBucket.newSky3,
    newSky3Paced: Math.round(currentBucket.newSky3 * pacingMultiplier),
    revenueActual: Math.round(currentBucket.revenueAdded * 100) / 100,
    revenuePaced: Math.round(currentBucket.revenueAdded * pacingMultiplier * 100) / 100,
    memberCancellationsActual: currentBucket.memberChurn,
    memberCancellationsPaced: Math.round(currentBucket.memberChurn * pacingMultiplier),
    sky3CancellationsActual: currentBucket.sky3Churn,
    sky3CancellationsPaced: Math.round(currentBucket.sky3Churn * pacingMultiplier),
  };

  // ── 4. Annual projection ─────────────────────────────────
  let projection: ProjectionData | null = null;
  const subStats = await getAutoRenewStats();

  if (subStats) {
    const currentMRR = subStats.mrr.total;
    const currentYear = now.getFullYear();
    const currentMonth = now.getMonth(); // 0-indexed
    const completedMonthKeys = allMonthKeys.filter((k) => k < currentMonthKey);

    // Compute per-month MRR changes
    const monthlyMrrGained = new Map<string, number>();
    const monthlyMrrLost = new Map<string, number>();

    for (const sub of newSubs) {
      const date = parseDate(sub.createdAt);
      if (!date) continue;
      const mo = getMonthKey(date);
      monthlyMrrGained.set(mo, (monthlyMrrGained.get(mo) || 0) + sub.monthlyRate);
    }

    for (const sub of canceledSubs) {
      const date = sub.canceledAt ? parseDate(sub.canceledAt) : null;
      if (!date) continue;
      const mo = getMonthKey(date);
      monthlyMrrLost.set(mo, (monthlyMrrLost.get(mo) || 0) + sub.monthlyRate);
    }

    // Reconstruct MRR series
    const mrrSeries: { month: string; mrr: number }[] = [];
    let backtrackMrr = currentMRR;
    for (let i = completedMonthKeys.length - 1; i >= 0; i--) {
      const mo = completedMonthKeys[i];
      mrrSeries.unshift({ month: mo, mrr: backtrackMrr });
      const gained = monthlyMrrGained.get(mo) || 0;
      const lost = monthlyMrrLost.get(mo) || 0;
      backtrackMrr = backtrackMrr - gained + lost;
    }

    // Growth rate from last 6 months
    const recentMrr = mrrSeries.slice(-6);
    let monthlyGrowthRate = 0;
    if (recentMrr.length >= 2) {
      const rates: number[] = [];
      for (let i = 1; i < recentMrr.length; i++) {
        const prev = recentMrr[i - 1].mrr;
        if (prev > 0) {
          rates.push((recentMrr[i].mrr - prev) / prev);
        }
      }
      if (rates.length > 0) {
        monthlyGrowthRate = rates.reduce((a, b) => a + b, 0) / rates.length;
      }
    }

    // Prior year revenue: prefer actual from revenue_categories, fall back to MRR estimate
    const priorYear = currentYear - 1;
    let priorYearRevenue = 0;
    let priorYearActualRevenue: number | null = null;

    // Try to find actual revenue from revenue_categories table
    // Sum all periods that fall within the prior year
    try {
      const allPeriods = await getAllPeriods();
      console.log(`[db-trends] getAllPeriods: ${allPeriods.length} periods. Starts: [${allPeriods.slice(0, 5).map(p => p.periodStart).join(", ")}]`);
      const priorYearPeriods = allPeriods.filter(
        (p) => p.periodStart.startsWith(String(priorYear)) && p.totalNetRevenue > 0
      );
      console.log(`[db-trends] Prior year ${priorYear} filter: ${priorYearPeriods.length} matching periods`);
      if (priorYearPeriods.length > 0) {
        // Check if any single period covers the full year (e.g. 2025-01-01 to 2025-12-31)
        const fullYearPeriod = priorYearPeriods.find((p) => {
          const start = new Date(p.periodStart + "T00:00:00");
          const end = new Date(p.periodEnd + "T00:00:00");
          const months = (end.getFullYear() - start.getFullYear()) * 12 + (end.getMonth() - start.getMonth());
          console.log(`[db-trends] Period span check: ${p.periodStart} to ${p.periodEnd}, months=${months}`);
          return months >= 11; // covers 11+ months = full year
        });

        if (fullYearPeriod) {
          // Use the full-year period ONLY — do NOT sum with monthly periods (would double-count)
          priorYearActualRevenue = Math.round(fullYearPeriod.totalNetRevenue);
          console.log(`[db-trends] Prior year ${priorYear}: full-year period ${fullYearPeriod.periodStart}→${fullYearPeriod.periodEnd}, using $${priorYearActualRevenue.toLocaleString()} (NOT summing ${priorYearPeriods.length} periods to avoid double-count)`);
        } else {
          // No full-year period — sum monthly periods (no overlap risk)
          const totalNet = priorYearPeriods.reduce((sum, p) => sum + p.totalNetRevenue, 0);
          // Calculate covered months from unique period-start months
          const coveredMonths = new Set(
            priorYearPeriods.map((p) => p.periodStart.slice(0, 7))
          ).size;
          if (coveredMonths >= 10) {
            priorYearActualRevenue = Math.round(totalNet / coveredMonths * 12);
          } else if (coveredMonths > 0) {
            priorYearActualRevenue = Math.round(totalNet / coveredMonths * 12);
          }
          console.log(`[db-trends] Prior year ${priorYear}: ${coveredMonths} months, $${Math.round(totalNet).toLocaleString()} actual, $${priorYearActualRevenue?.toLocaleString()} annualized`);
        }
      }
    } catch (err) {
      console.warn(`[db-trends] Failed to compute prior year actual revenue:`, err instanceof Error ? err.message : err);
    }

    // MRR-based estimate as fallback
    const priorMonths = mrrSeries.filter((e) => e.month.startsWith(String(priorYear)));
    console.log(`[db-trends] MRR series: ${mrrSeries.length} entries. Prior year (${priorYear}) months: ${priorMonths.length}. Keys: [${priorMonths.map(m => m.month).join(", ")}]`);
    if (priorMonths.length > 0) {
      const total = priorMonths.reduce((sum, e) => sum + e.mrr, 0);
      priorYearRevenue = priorMonths.length < 12
        ? (total / priorMonths.length) * 12
        : total;
      console.log(`[db-trends] MRR-based prior year estimate: $${Math.round(priorYearRevenue)}`);
    }

    // Use actual if available
    if (priorYearActualRevenue && priorYearActualRevenue > priorYearRevenue) {
      priorYearRevenue = priorYearActualRevenue;
    }
    console.log(`[db-trends] Final priorYearRevenue=$${Math.round(priorYearRevenue)}, priorYearActualRevenue=${priorYearActualRevenue ? "$" + Math.round(priorYearActualRevenue) : "null"}`);

    // Compute non-MRR revenue ratio from actual data
    // (drop-ins, workshops, retail, teacher training, etc.)
    // If we have actual prior year revenue > MRR-based estimate, the ratio tells us
    // how much total revenue exceeds subscription-only revenue
    const mrrBasedPriorTotal = priorMonths.length > 0
      ? (priorMonths.length < 12
        ? (priorMonths.reduce((s, e) => s + e.mrr, 0) / priorMonths.length) * 12
        : priorMonths.reduce((s, e) => s + e.mrr, 0))
      : 0;
    // Non-MRR multiplier: how much total revenue exceeds MRR-only revenue.
    // Capped at 2.0 to prevent absurd projections when MRR data is sparse
    // (e.g., auto_renews table empty → MRR backtrack produces tiny numbers
    // while revenue_categories has real $2M+ actuals).
    const rawMultiplier = (priorYearActualRevenue && mrrBasedPriorTotal > 0)
      ? priorYearActualRevenue / mrrBasedPriorTotal
      : 1;
    const nonMrrMultiplier = Math.min(rawMultiplier, 2.0);
    if (rawMultiplier > 2.0) {
      console.warn(`[db-trends] nonMrrMultiplier capped: raw=${rawMultiplier.toFixed(2)}, using 2.0. MRR data may be sparse.`);
    }

    // Project annual revenue (MRR-based, then scaled by non-MRR multiplier)
    let projectedMrrRevenue = 0;
    let projectedMRR = currentMRR;

    // Completed months this year
    for (const mo of completedMonthKeys) {
      if (mo.startsWith(String(currentYear))) {
        const entry = mrrSeries.find((s) => s.month === mo);
        projectedMrrRevenue += entry ? entry.mrr : currentMRR;
      }
    }

    // Paced current month
    projectedMrrRevenue += pacing.revenuePaced > 0 ? pacing.revenuePaced : currentMRR;

    // Remaining months with growth
    const remainingMonths = 11 - currentMonth;
    for (let i = 0; i < remainingMonths; i++) {
      projectedMRR = projectedMRR * (1 + monthlyGrowthRate);
      projectedMrrRevenue += projectedMRR;
    }

    // Scale by non-MRR multiplier to account for drop-ins, workshops, retail, etc.
    let projectedAnnualRevenue = Math.round(projectedMrrRevenue * nonMrrMultiplier);

    // Sanity check: projection shouldn't be more than 3x prior year
    if (priorYearRevenue > 0 && projectedAnnualRevenue > priorYearRevenue * 3) {
      console.warn(`[db-trends] Projection sanity check failed: $${projectedAnnualRevenue.toLocaleString()} > 3x prior year $${Math.round(priorYearRevenue).toLocaleString()}. Capping.`);
      projectedAnnualRevenue = Math.round(priorYearRevenue * 1.3); // assume 30% growth max
    }

    projection = {
      year: currentYear,
      projectedAnnualRevenue,
      currentMRR: Math.round(currentMRR * 100) / 100,
      projectedYearEndMRR: Math.round(projectedMRR * 100) / 100,
      monthlyGrowthRate: Math.round(monthlyGrowthRate * 10000) / 100,
      priorYearRevenue: Math.round(priorYearRevenue),
      priorYearActualRevenue,
    };
  }

  // If no auto-renew data but we have revenue_categories, build minimal projection
  if (!projection) {
    const currentYear = now.getFullYear();
    const priorYear = currentYear - 1;
    let priorYearActual: number | null = null;

    try {
      const allPeriods = await getAllPeriods();
      const priorYearPeriods = allPeriods.filter(
        (p) => p.periodStart.startsWith(String(priorYear)) && p.totalNetRevenue > 0
      );
      if (priorYearPeriods.length > 0) {
        // Check for a single full-year period — use it exclusively to avoid double-counting
        const fullYearPeriod = priorYearPeriods.find((p) => {
          const start = new Date(p.periodStart + "T00:00:00");
          const end = new Date(p.periodEnd + "T00:00:00");
          const months = (end.getFullYear() - start.getFullYear()) * 12 + (end.getMonth() - start.getMonth());
          return months >= 11;
        });
        if (fullYearPeriod) {
          priorYearActual = Math.round(fullYearPeriod.totalNetRevenue);
        } else {
          const totalNet = priorYearPeriods.reduce((sum, p) => sum + p.totalNetRevenue, 0);
          const coveredMonths = new Set(priorYearPeriods.map((p) => p.periodStart.slice(0, 7))).size;
          if (coveredMonths > 0) {
            priorYearActual = Math.round(totalNet / coveredMonths * 12);
          }
        }
      }
    } catch (err) {
      console.warn(`[db-trends] Fallback revenue lookup failed:`, err instanceof Error ? err.message : err);
    }

    if (priorYearActual) {
      console.log(`[db-trends] No auto-renew data, but found ${priorYear} revenue: $${priorYearActual.toLocaleString()}`);
      projection = {
        year: currentYear,
        projectedAnnualRevenue: 0, // No MRR data to project
        currentMRR: 0,
        projectedYearEndMRR: 0,
        monthlyGrowthRate: 0,
        priorYearRevenue: priorYearActual,
        priorYearActualRevenue: priorYearActual,
      };
    }
  }

  // ── 5. Drop-in data (weekly-first module) ─────────────────
  let dropIns: DropInModuleData | null = null;

  if (await hasRegistrationData()) {
    try {
      const [weeklyDetail, wtdRaw, frequencyRaw, lastWeekWTDVisits] = await Promise.all([
        getDropInWeeklyDetail(16),
        getDropInWTD(),
        getDropInFrequencyDistribution(),
        getDropInLastWeekWTD(),
      ]);

      if (weeklyDetail.length > 0 || wtdRaw) {
        const completeWeeks = weeklyDetail.map((w) => ({
          weekStart: w.weekStart,
          weekEnd: w.weekEnd,
          visits: w.visits,
          uniqueCustomers: w.uniqueCustomers,
          firstTime: w.firstTime,
          repeatCustomers: w.repeatCustomers,
        }));

        const lastCompleteWeek = completeWeeks.length > 0
          ? completeWeeks[completeWeeks.length - 1]
          : null;

        // Typical week = average of last 8 complete weeks
        const last8 = completeWeeks.slice(-8);
        const typicalWeekVisits = last8.length > 0
          ? Math.round(last8.reduce((s, w) => s + w.visits, 0) / last8.length)
          : 0;

        // Trend: avg of last 4 vs prior 4 complete weeks, ±5% threshold
        const last4 = completeWeeks.slice(-4);
        const prior4 = completeWeeks.slice(-8, -4);
        const avgLast4 = last4.length > 0
          ? last4.reduce((s, w) => s + w.visits, 0) / last4.length
          : 0;
        const avgPrior4 = prior4.length > 0
          ? prior4.reduce((s, w) => s + w.visits, 0) / prior4.length
          : 0;
        const trendDeltaPercent = avgPrior4 > 0
          ? Math.round(((avgLast4 - avgPrior4) / avgPrior4) * 1000) / 10
          : 0;
        const trend: "up" | "flat" | "down" =
          trendDeltaPercent > 5 ? "up" : trendDeltaPercent < -5 ? "down" : "flat";

        // WTD delta: compare WTD visits to last week through the same weekday
        const wtdVisits = wtdRaw?.visits ?? 0;
        const wtdDelta = wtdVisits - lastWeekWTDVisits;
        const wtdDeltaPercent = lastWeekWTDVisits > 0
          ? Math.round((wtdDelta / lastWeekWTDVisits) * 1000) / 10
          : 0;

        const wtd = wtdRaw ? {
          weekStart: wtdRaw.weekStart,
          weekEnd: wtdRaw.weekEnd,
          visits: wtdRaw.visits,
          uniqueCustomers: wtdRaw.uniqueCustomers,
          firstTime: wtdRaw.firstTime,
          repeatCustomers: wtdRaw.repeatCustomers,
          daysLeft: wtdRaw.daysLeft,
        } : null;

        const frequency = frequencyRaw.totalCustomers > 0 ? {
          bucket1: frequencyRaw.bucket1,
          bucket2to4: frequencyRaw.bucket2to4,
          bucket5to10: frequencyRaw.bucket5to10,
          bucket11plus: frequencyRaw.bucket11plus,
          totalCustomers: frequencyRaw.totalCustomers,
        } : null;

        // Day label: "As of Mon", "As of Tue", etc.
        const dayNames = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
        const wtdDayLabel = `As of ${dayNames[new Date().getDay()]}`;

        dropIns = {
          completeWeeks,
          wtd,
          lastCompleteWeek,
          typicalWeekVisits,
          trend,
          trendDeltaPercent,
          wtdDelta,
          wtdDeltaPercent,
          wtdDayLabel,
          frequency,
        };
      }
    } catch (err) {
      console.warn("[db-trends] Failed to compute drop-in module data:", err);
    }
  }

  // ── 6. First visit data (unique visitors) ──────────────
  let firstVisits: FirstVisitData | null = null;

  if (await hasFirstVisitData()) {
    const fiveWeeksAgo = new Date(now);
    fiveWeeksAgo.setDate(fiveWeeksAgo.getDate() - 35);
    const startStr = fiveWeeksAgo.toISOString().split("T")[0];

    const uvWeeks = await getFirstTimeUniqueVisitorsByWeek(startStr);
    const sourceBreakdown = await getFirstTimeSourceBreakdown(startStr);

    if (uvWeeks.length > 0) {
      const sortedWeeks = [...uvWeeks].sort((a, b) =>
        a.weekStart.localeCompare(b.weekStart)
      );
      const lastWeek = sortedWeeks[sortedWeeks.length - 1];
      const completedWeeks = sortedWeeks.slice(0, -1).slice(-4);
      const zeroSeg = { introWeek: 0, dropIn: 0, guest: 0, other: 0 };

      firstVisits = {
        currentWeekTotal: lastWeek.uniqueVisitors,
        currentWeekSegments: { ...zeroSeg },
        completedWeeks: completedWeeks.map((w) => ({
          week: w.weekStart,
          uniqueVisitors: w.uniqueVisitors,
          segments: { ...zeroSeg },
        })),
        aggregateSegments: {
          introWeek: sourceBreakdown.introWeek,
          dropIn: sourceBreakdown.dropIn,
          guest: sourceBreakdown.guest,
          other: sourceBreakdown.other,
        },
        otherBreakdownTop5: sourceBreakdown.otherBreakdownTop5,
      };
    }
  }

  // ── 6b. Intro Week data ──────────────────────────────────
  let introWeekData: import("../../types/dashboard").IntroWeekData | null = null;

  if (await hasFirstVisitData()) {
    try {
      const sixWeeksAgo = new Date(now);
      sixWeeksAgo.setDate(sixWeeksAgo.getDate() - 42);
      const startStr = sixWeeksAgo.toISOString().split("T")[0];

      const iwWeeks = await getIntroWeekCustomersByWeek(startStr);
      if (iwWeeks.length > 0) {
        const sorted = [...iwWeeks].sort((a, b) => a.weekStart.localeCompare(b.weekStart));
        const lastWeek = sorted[sorted.length - 1];
        const completed = sorted.slice(0, -1).slice(-4);
        const avg = completed.length > 0
          ? Math.round(completed.reduce((s, w) => s + w.customers, 0) / completed.length)
          : 0;

        introWeekData = {
          lastWeek: { weekStart: lastWeek.weekStart, customers: lastWeek.customers },
          last4Weeks: completed,
          last4WeekAvg: avg,
        };
      }
    } catch (err) {
      console.warn("[db-trends] Failed to compute intro week data:", err);
    }
  }

  // ── 7. Returning non-members (unique visitors) ─────────
  let returningNonMembers: ReturningNonMemberData | null = null;

  if (await hasRegistrationData()) {
    try {
      const fiveWeeksAgo = new Date(now);
      fiveWeeksAgo.setDate(fiveWeeksAgo.getDate() - 35);
      const startStr = fiveWeeksAgo.toISOString().split("T")[0];

      const uvWeeks = await getReturningUniqueVisitorsByWeek(startStr);
      const sourceBreakdown = await getReturningSourceBreakdown(startStr);

      if (uvWeeks.length > 0) {
        const sortedWeeks = [...uvWeeks].sort((a, b) =>
          a.weekStart.localeCompare(b.weekStart)
        );
        const lastWeek = sortedWeeks[sortedWeeks.length - 1];
        const completedWeeks = sortedWeeks.slice(0, -1).slice(-4);
        const zeroSeg = { introWeek: 0, dropIn: 0, guest: 0, other: 0 };

        returningNonMembers = {
          currentWeekTotal: lastWeek.uniqueVisitors,
          currentWeekSegments: { ...zeroSeg },
          completedWeeks: completedWeeks.map((w) => ({
            week: w.weekStart,
            uniqueVisitors: w.uniqueVisitors,
            segments: { ...zeroSeg },
          })),
          aggregateSegments: {
            introWeek: sourceBreakdown.introWeek,
            dropIn: sourceBreakdown.dropIn,
            guest: sourceBreakdown.guest,
            other: sourceBreakdown.other,
          },
          otherBreakdownTop5: sourceBreakdown.otherBreakdownTop5,
        };
      }
    } catch (err) {
      console.warn("[db-trends] Failed to compute returning non-members:", err);
    }
  }

  // ── 8. Churn rates ──────────────────────────────────────────
  let churnRates: ChurnRateData | null = null;
  try {
    churnRates = await computeChurnRates();
  } catch (err) {
    console.warn("[db-trends] Failed to compute churn rates:", err);
  }

  // ── 9. New customer volume & cohort conversion ────────────
  let newCustomerVolume: NewCustomerVolumeData | null = null;
  let newCustomerCohorts: NewCustomerCohortData | null = null;

  if (await hasFirstVisitData()) {
    try {
      // Volume: weekly new customer counts
      const volumeWeeks = await getNewCustomerVolumeByWeek();
      if (volumeWeeks.length > 0) {
        const sorted = [...volumeWeeks].sort((a, b) => a.weekStart.localeCompare(b.weekStart));
        // Last week is current (partial); rest are completed
        const currentWeek = sorted[sorted.length - 1];
        const completed = sorted.slice(0, -1).slice(-4); // last 4 completed weeks

        newCustomerVolume = {
          currentWeekCount: currentWeek.count,
          completedWeeks: completed.map((w) => ({
            weekStart: w.weekStart,
            weekEnd: w.weekEnd,
            count: w.count,
          })),
        };
      }

      // Cohorts: 3-week conversion tracking
      const cohortRows = await getNewCustomerCohorts();
      if (cohortRows.length > 0) {
        const today = new Date();
        const todayStr = today.toISOString().split("T")[0];

        // A cohort is "complete" if cohort_start + 20 days < today
        const completeCohorts = cohortRows.filter((c) => {
          const start = new Date(c.cohortStart + "T00:00:00");
          const cutoff = new Date(start);
          cutoff.setDate(cutoff.getDate() + 20);
          const cutoffStr = cutoff.toISOString().split("T")[0];
          return cutoffStr < todayStr;
        });

        // Avg conversion rate from last 3-5 complete cohorts (null if < 3)
        let avgConversionRate: number | null = null;
        const recentComplete = completeCohorts.slice(-5);
        if (recentComplete.length >= 3) {
          const totalNew = recentComplete.reduce((s, c) => s + c.newCustomers, 0);
          const totalConverted = recentComplete.reduce((s, c) => s + c.total3Week, 0);
          avgConversionRate = totalNew > 0
            ? Math.round((totalConverted / totalNew) * 1000) / 10
            : 0;
        }

        newCustomerCohorts = {
          cohorts: cohortRows.map((c) => ({
            cohortStart: c.cohortStart,
            cohortEnd: c.cohortEnd,
            newCustomers: c.newCustomers,
            week1: c.week1,
            week2: c.week2,
            week3: c.week3,
            total3Week: c.total3Week,
          })),
          avgConversionRate,
        };
      }
    } catch (err) {
      console.warn("[db-trends] Failed to compute new customer data:", err);
    }
  }

  // ── 10. Conversion Pool: non-auto visitors → in-studio auto-renew ──
  let conversionPool: ConversionPoolModuleData | null = null;

  // Helper to assemble one slice's data
  async function buildPoolSlice(slice: PoolSliceKey): Promise<ConversionPoolSliceData | null> {
    const [weeklyRows, wtdRow, lagRow] = await Promise.all([
      getConversionPoolWeekly(16, slice),
      getConversionPoolWTD(slice),
      getConversionPoolLagStats(slice),
    ]);

    if (weeklyRows.length === 0 && !wtdRow) return null;

    const completeWeeks = weeklyRows.map((w) => ({
      weekStart: w.weekStart,
      weekEnd: w.weekEnd,
      activePool7d: w.activePool7d,
      converts: w.converts,
      conversionRate: w.activePool7d > 0
        ? Math.round((w.converts / w.activePool7d) * 10000) / 100
        : 0,
      yieldPer100: w.activePool7d > 0
        ? Math.round((w.converts / w.activePool7d) * 10000) / 100
        : 0,
    }));

    const lastCompleteWeek = completeWeeks[completeWeeks.length - 1] ?? null;
    const last8 = completeWeeks.slice(-8);
    const avgPool7d = last8.length > 0
      ? Math.round(last8.reduce((s, w) => s + w.activePool7d, 0) / last8.length) : 0;
    const avgRate = last8.length > 0
      ? Math.round((last8.reduce((s, w) => s + w.conversionRate, 0) / last8.length) * 100) / 100 : 0;

    const wtd = wtdRow ? {
      weekStart: wtdRow.weekStart,
      weekEnd: wtdRow.weekEnd,
      activePool7d: wtdRow.activePool7d,
      activePool30d: wtdRow.activePool30d,
      converts: wtdRow.converts,
      conversionRate: wtdRow.activePool7d > 0
        ? Math.round((wtdRow.converts / wtdRow.activePool7d) * 10000) / 100 : 0,
      daysLeft: wtdRow.daysLeft,
    } : null;

    const lagStats = lagRow ? {
      medianTimeToConvert: lagRow.medianTimeToConvert != null
        ? Math.round(lagRow.medianTimeToConvert) : null,
      avgVisitsBeforeConvert: lagRow.avgVisitsBeforeConvert != null
        ? Math.round(lagRow.avgVisitsBeforeConvert * 10) / 10 : null,
      timeBucket0to30: lagRow.timeBucket0to30,
      timeBucket31to90: lagRow.timeBucket31to90,
      timeBucket91to180: lagRow.timeBucket91to180,
      timeBucket180plus: lagRow.timeBucket180plus,
      visitBucket1to2: lagRow.visitBucket1to2,
      visitBucket3to5: lagRow.visitBucket3to5,
      visitBucket6to10: lagRow.visitBucket6to10,
      visitBucket11plus: lagRow.visitBucket11plus,
      totalConvertersInBuckets: lagRow.totalConvertersInBuckets,
      historicalMedianTimeToConvert: lagRow.historicalMedianTimeToConvert != null
        ? Math.round(lagRow.historicalMedianTimeToConvert) : null,
      historicalAvgVisitsBeforeConvert: lagRow.historicalAvgVisitsBeforeConvert != null
        ? Math.round(lagRow.historicalAvgVisitsBeforeConvert * 10) / 10 : null,
    } : null;

    return { completeWeeks, wtd, lagStats, lastCompleteWeek, avgPool7d, avgRate };
  }

  if (await hasRegistrationData()) {
    try {
      const sliceKeys: ConversionPoolSlice[] = ["all", "drop-ins", "intro-week", "class-packs", "high-intent"];
      const sliceResults = await Promise.all(
        sliceKeys.map((k) => buildPoolSlice(k as PoolSliceKey))
      );

      const slices: Partial<Record<ConversionPoolSlice, ConversionPoolSliceData>> = {};
      sliceKeys.forEach((k, i) => {
        if (sliceResults[i]) slices[k] = sliceResults[i]!;
      });

      if (Object.keys(slices).length > 0) {
        conversionPool = { slices };
      }
    } catch (err) {
      console.warn("[db-trends] Failed to compute conversion pool data:", err);
    }
  }

  // ── 11. Usage frequency segments by plan category ──────────
  let usage: UsageData | null = null;

  try {
    usage = await getUsageFrequencyByCategory();
    if (usage.categories.length === 0) usage = null;
  } catch (err) {
    console.warn("[db-trends] Failed to compute usage frequency data:", err);
  }

  console.log(
    `[db-trends] Computed: ${weekly.length} weekly, ${monthly.length} monthly periods` +
    (dropIns ? `, drop-ins typical=${dropIns.typicalWeekVisits}/wk, trend=${dropIns.trend}` : "") +
    (firstVisits ? `, first visits this week=${firstVisits.currentWeekTotal}` : "") +
    (newCustomerVolume ? `, new customers this week=${newCustomerVolume.currentWeekCount}` : "") +
    (newCustomerCohorts ? `, cohort avg conversion=${newCustomerCohorts.avgConversionRate?.toFixed(1) ?? "N/A"}%` : "") +
    (churnRates ? `, member churn=${churnRates.avgMemberRate.toFixed(1)}%, sky3 churn=${churnRates.avgSky3Rate.toFixed(1)}%` : "") +
    (conversionPool?.slices?.all ? `, conversion pool=${conversionPool.slices.all.avgPool7d} avg pool, ${conversionPool.slices.all.avgRate.toFixed(2)}% avg rate` : "") +
    (usage ? `, usage: ${usage.categories.map(c => `${c.label}=${c.totalActive}`).join(", ")}` : "")
  );

  return {
    weekly,
    monthly,
    pacing,
    projection,
    dropIns,
    introWeek: introWeekData,
    firstVisits,
    returningNonMembers,
    churnRates,
    newCustomerVolume,
    newCustomerCohorts,
    conversionPool,
    usage,
  };
}

// ── Member Alerts: Renewal Approaching + Tenure Milestones ──

type CategorizedRow = {
  plan_name: string;
  plan_state: string;
  plan_price: number;
  canceled_at: string | null;
  created_at: string | null;
  category: string;
  isAnnual: boolean;
  monthlyRate: number;
  customer_name: string;
  customer_email: string;
};

/**
 * Compute next renewal date for a subscriber.
 * Monthly: same day-of-month each month.
 * Annual: same month+day each year.
 */
function getNextRenewalDate(createdAt: string, isAnnual: boolean): Date {
  const created = new Date(createdAt);
  const now = new Date();

  if (isAnnual) {
    // Next anniversary of the created_at date
    const thisYear = new Date(now.getFullYear(), created.getMonth(), created.getDate());
    if (thisYear > now) return thisYear;
    return new Date(now.getFullYear() + 1, created.getMonth(), created.getDate());
  } else {
    // Next occurrence of the created_at day-of-month
    const dayOfMonth = created.getDate();
    // Try this month first
    const thisMonth = new Date(now.getFullYear(), now.getMonth(), dayOfMonth);
    if (thisMonth > now) return thisMonth;
    // Next month
    return new Date(now.getFullYear(), now.getMonth() + 1, dayOfMonth);
  }
}

/**
 * Compute member alerts: who's renewing soon + who's at a tenure milestone.
 */
function computeMemberAlerts(
  allRows: CategorizedRow[],
  activeStates: string[]
): MemberAlerts {
  const now = new Date();
  const nowMs = now.getTime();
  const MS_PER_MONTH = 30.44 * 24 * 60 * 60 * 1000;
  const SEVEN_DAYS_MS = 7 * 24 * 60 * 60 * 1000;

  // Only active MEMBER subscribers with created_at
  const activeMembers = allRows.filter(
    (r) => r.category === "MEMBER" && r.created_at && activeStates.includes(r.plan_state)
  );

  // ── Renewal approaching (within 7 days) ──
  const renewalApproaching: RenewalAlertMember[] = [];
  for (const r of activeMembers) {
    const renewal = getNextRenewalDate(r.created_at!, r.isAnnual);
    const daysUntil = Math.ceil((renewal.getTime() - nowMs) / (24 * 60 * 60 * 1000));
    if (daysUntil >= 0 && daysUntil <= 7) {
      const tenure = (nowMs - new Date(r.created_at!).getTime()) / MS_PER_MONTH;
      renewalApproaching.push({
        name: r.customer_name,
        email: r.customer_email,
        planName: r.plan_name,
        isAnnual: r.isAnnual,
        createdAt: r.created_at!,
        renewalDate: `${renewal.getFullYear()}-${String(renewal.getMonth() + 1).padStart(2, "0")}-${String(renewal.getDate()).padStart(2, "0")}`,
        daysUntilRenewal: daysUntil,
        tenureMonths: Math.round(tenure * 10) / 10,
      });
    }
  }
  renewalApproaching.sort((a, b) => a.daysUntilRenewal - b.daysUntilRenewal);

  // ── Tenure milestones (within ±1 week of key months) ──
  const MILESTONES = [
    { months: 3, label: "3-month cliff" },
    { months: 7, label: "7-month mark" },
  ];
  const WEEK_IN_MONTHS = 7 / 30.44; // ~0.23 months

  const tenureMilestones: TenureMilestoneMember[] = [];
  for (const r of activeMembers) {
    const tenure = (nowMs - new Date(r.created_at!).getTime()) / MS_PER_MONTH;
    for (const ms of MILESTONES) {
      if (tenure >= ms.months - WEEK_IN_MONTHS && tenure <= ms.months + WEEK_IN_MONTHS) {
        tenureMilestones.push({
          name: r.customer_name,
          email: r.customer_email,
          planName: r.plan_name,
          isAnnual: r.isAnnual,
          createdAt: r.created_at!,
          tenureMonths: Math.round(tenure * 10) / 10,
          milestone: ms.label,
        });
        break; // only closest milestone per person
      }
    }
  }
  tenureMilestones.sort((a, b) => a.tenureMonths - b.tenureMonths);

  console.log(`[member-alerts] ${renewalApproaching.length} renewals within 7 days, ${tenureMilestones.length} at tenure milestones`);

  return { renewalApproaching, tenureMilestones };
}

// ── Churn Rate Computation ─────────────────────────────────

/**
 * Compute per-category monthly churn rates.
 *
 * For each of MEMBER, SKY3, SKY_TING_TV:
 *   - User churn: canceledCount / activeAtStart
 *   - MRR churn:  canceledMRR / activeMRR (uses monthlyRate = price/12 for annual)
 *   - MEMBER-only: annual vs monthly billing breakdown
 *
 * "Active at start of month M" = created before M AND
 *   (still currently active OR canceled on/after month M start).
 */
async function computeChurnRates(): Promise<ChurnRateData | null> {
  const pool = getPool();

  const { rows: allRows } = await pool.query(
    `SELECT plan_name, plan_state, plan_price, canceled_at, created_at, customer_name, customer_email FROM auto_renews`
  );

  if (allRows.length === 0) return null;

  /**
   * Normalize a raw date string from the database to YYYY-MM-DD.
   * The CSV may store dates in various formats:
   *   "2024-01-28 22:46:51 -0500"  (direct CSV)
   *   "1/28/2024"                   (HTML-scraped)
   *   "Jan 28, 2024"               (other)
   * We use parseDate() from date-utils to handle all of these.
   */
  function toDateStr(raw: string | null | undefined): string | null {
    if (!raw || raw.trim() === "") return null;
    const d = parseDate(raw);
    if (!d) return null;
    return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}-${String(d.getDate()).padStart(2, "0")}`;
  }

  const categorized = allRows.map((r: Record<string, unknown>) => {
    const name = r.plan_name as string;
    const annual = isAnnualPlan(name);
    const price = (r.plan_price as number) || 0;
    return {
      plan_name: name,
      plan_state: r.plan_state as string,
      plan_price: price,
      canceled_at: toDateStr(r.canceled_at as string | null),
      created_at: toDateStr(r.created_at as string | null),
      category: getCategory(name),
      isAnnual: annual,
      monthlyRate: annual ? Math.round((price / 12) * 100) / 100 : price,
      customer_name: (r.customer_name as string) || "",
      customer_email: (r.customer_email as string) || "",
    };
  });

  // Log date parsing diagnostics
  const withCreated = categorized.filter((r) => r.created_at !== null);
  const withCanceled = categorized.filter((r) => r.canceled_at !== null);
  console.log(`[churn] Parsed dates: ${withCreated.length}/${categorized.length} have created_at, ${withCanceled.length}/${categorized.length} have canceled_at`);
  if (withCreated.length > 0) {
    console.log(`[churn] Sample created_at: "${allRows[0].created_at}" -> "${categorized[0].created_at}"`);
  }

  const ACTIVE_STATES = ["Valid Now", "Pending Cancel", "Paused", "Past Due", "In Trial"];
  const AT_RISK_STATES = ["Past Due", "Invalid", "Pending Cancel"];
  const CATEGORIES = ["MEMBER", "SKY3", "SKY_TING_TV"] as const;

  // Generate last 6 completed months + current month
  const now = new Date();
  const months: string[] = [];
  for (let i = 6; i >= 0; i--) {
    const d = new Date(now.getFullYear(), now.getMonth() - i, 1);
    months.push(d.getFullYear() + "-" + String(d.getMonth() + 1).padStart(2, "0"));
  }

  // Build per-category churn data
  const catResults: Record<string, CategoryChurnData> = {};

  for (const cat of CATEGORIES) {
    const catRows = categorized.filter((r) => r.category === cat);
    const monthlyChurn: CategoryMonthlyChurn[] = [];

    for (const month of months) {
      const monthStart = month + "-01";
      const [yearStr, moStr] = month.split("-");
      const nextMonth = new Date(parseInt(yearStr), parseInt(moStr), 1);
      const monthEnd = nextMonth.getFullYear() + "-" +
        String(nextMonth.getMonth() + 1).padStart(2, "0") + "-01";

      // Active at start of month: created before month start AND
      // (still active OR canceled on/after month start)
      const activeAtStart = catRows.filter((r) => {
        if (!r.created_at || r.created_at >= monthStart) return false;
        if (ACTIVE_STATES.includes(r.plan_state)) return true;
        if (r.canceled_at && r.canceled_at >= monthStart) return true;
        return false;
      });

      const activeMrrAtStart = activeAtStart.reduce((s, r) => s + r.monthlyRate, 0);

      // Canceled during this month
      const canceledInMonth = catRows.filter((r) =>
        r.canceled_at && r.canceled_at >= monthStart && r.canceled_at < monthEnd
      );

      const canceledMrr = canceledInMonth.reduce((s, r) => s + r.monthlyRate, 0);

      const entry: CategoryMonthlyChurn = {
        month,
        userChurnRate: activeAtStart.length > 0
          ? Math.round((canceledInMonth.length / activeAtStart.length) * 1000) / 10
          : 0,
        mrrChurnRate: activeMrrAtStart > 0
          ? Math.round((canceledMrr / activeMrrAtStart) * 1000) / 10
          : 0,
        activeAtStart: activeAtStart.length,
        activeMrrAtStart: Math.round(activeMrrAtStart * 100) / 100,
        canceledCount: canceledInMonth.length,
        canceledMrr: Math.round(canceledMrr * 100) / 100,
      };

      // MEMBER-only: annual vs monthly breakdown + eligible churn rate
      if (cat === "MEMBER") {
        const annualActive = activeAtStart.filter((r) => r.isAnnual);
        const monthlyActive = activeAtStart.filter((r) => !r.isAnnual);
        const annualCanceled = canceledInMonth.filter((r) => r.isAnnual);
        const monthlyCanceled = canceledInMonth.filter((r) => !r.isAnnual);
        entry.annualActiveAtStart = annualActive.length;
        entry.annualCanceledCount = annualCanceled.length;
        entry.monthlyActiveAtStart = monthlyActive.length;
        entry.monthlyCanceledCount = monthlyCanceled.length;
        // Eligible churn: only monthly subscribers can churn month-to-month
        entry.eligibleChurnRate = monthlyActive.length > 0
          ? Math.round((monthlyCanceled.length / monthlyActive.length) * 1000) / 10
          : 0;
        // Annual user churn rate
        entry.annualUserChurnRate = annualActive.length > 0
          ? Math.round((annualCanceled.length / annualActive.length) * 1000) / 10
          : 0;
        // Per-billing MRR split
        const annualActiveMrr = annualActive.reduce((s, r) => s + r.monthlyRate, 0);
        const annualCanceledMrr = annualCanceled.reduce((s, r) => s + r.monthlyRate, 0);
        const monthlyActiveMrr = monthlyActive.reduce((s, r) => s + r.monthlyRate, 0);
        const monthlyCanceledMrr = monthlyCanceled.reduce((s, r) => s + r.monthlyRate, 0);
        entry.annualActiveMrrAtStart = Math.round(annualActiveMrr * 100) / 100;
        entry.annualCanceledMrr = Math.round(annualCanceledMrr * 100) / 100;
        entry.annualMrrChurnRate = annualActiveMrr > 0
          ? Math.round((annualCanceledMrr / annualActiveMrr) * 1000) / 10 : 0;
        entry.monthlyActiveMrrAtStart = Math.round(monthlyActiveMrr * 100) / 100;
        entry.monthlyCanceledMrr = Math.round(monthlyCanceledMrr * 100) / 100;
        entry.monthlyMrrChurnRate = monthlyActiveMrr > 0
          ? Math.round((monthlyCanceledMrr / monthlyActiveMrr) * 1000) / 10 : 0;
      }

      monthlyChurn.push(entry);
    }

    // Averages (exclude current partial month)
    const completed = monthlyChurn.slice(0, -1);
    const avgUser = completed.length > 0
      ? Math.round((completed.reduce((s, r) => s + r.userChurnRate, 0) / completed.length) * 10) / 10
      : 0;
    const avgMrr = completed.length > 0
      ? Math.round((completed.reduce((s, r) => s + r.mrrChurnRate, 0) / completed.length) * 10) / 10
      : 0;

    // At-risk per category (in-memory)
    const atRiskCount = catRows.filter((r) => AT_RISK_STATES.includes(r.plan_state)).length;

    // MEMBER-only: average eligible churn rate (monthly subscribers only)
    const avgEligible = cat === "MEMBER" && completed.length > 0
      ? Math.round((completed.reduce((s, r) => s + (r.eligibleChurnRate ?? 0), 0) / completed.length) * 10) / 10
      : undefined;

    const result: typeof catResults[typeof cat] = {
      category: cat,
      monthly: monthlyChurn,
      avgUserChurnRate: avgUser,
      avgMrrChurnRate: avgMrr,
      atRiskCount,
      ...(avgEligible !== undefined && { avgEligibleChurnRate: avgEligible }),
    };

    // MEMBER-only: split averages + at-risk by billing type
    if (cat === "MEMBER" && completed.length > 0) {
      const avg = (key: keyof CategoryMonthlyChurn) =>
        Math.round((completed.reduce((s, r) => s + ((r[key] as number) ?? 0), 0) / completed.length) * 10) / 10;
      result.avgAnnualUserChurnRate = avg("annualUserChurnRate");
      result.avgAnnualMrrChurnRate = avg("annualMrrChurnRate");
      result.avgMonthlyMrrChurnRate = avg("monthlyMrrChurnRate");
      result.annualAtRiskCount = catRows.filter((r) => r.isAnnual && AT_RISK_STATES.includes(r.plan_state)).length;
      result.monthlyAtRiskCount = catRows.filter((r) => !r.isAnnual && AT_RISK_STATES.includes(r.plan_state)).length;

      // ── Tenure / retention metrics (MEMBER only) ────────────
      const tenureRows = catRows.filter((r) => r.created_at);
      if (tenureRows.length > 0) {
        const nowMs = Date.now();
        const CLIFF_MONTHS = 3; // 3-month minimum commitment
        const MAX_CURVE_MONTHS = 24;

        // Compute tenure in months for each subscriber
        const tenures = tenureRows.map((r) => {
          const createdMs = new Date(r.created_at!).getTime();
          const endMs = r.canceled_at ? new Date(r.canceled_at).getTime() : nowMs;
          const months = Math.max(0, (endMs - createdMs) / (30.44 * 24 * 60 * 60 * 1000));
          return {
            tenure: months,
            isCensored: !r.canceled_at, // still active = censored
          };
        });

        // ── Median Tenure (Kaplan-Meier estimate) ──
        // Sort by tenure. For uncensored (canceled), that's an event.
        // For censored (still active), they're "withdrawn" at their tenure point.
        const sorted = [...tenures].sort((a, b) => a.tenure - b.tenure);
        let medianTenure = MAX_CURVE_MONTHS; // default if never drops below 0.5
        let medianFound = false;

        // Build survival curve via Kaplan-Meier
        const survivalCurve: { month: number; retained: number }[] = [{ month: 0, retained: 100 }];
        // We need survival probability at each integer month
        // Process events in order and track survival
        const events: { time: number; isEvent: boolean }[] = sorted.map((t) => ({
          time: t.tenure,
          isEvent: !t.isCensored,
        }));

        // Group events by unique time points for proper KM calculation
        let currentN = events.length;
        let currentSurv = 1.0;
        let eventIdx = 0;

        for (let m = 1; m <= MAX_CURVE_MONTHS; m++) {
          // Process all events that happened before month m
          while (eventIdx < events.length && events[eventIdx].time < m) {
            if (events[eventIdx].isEvent) {
              // Churn event: decrease survival
              currentSurv *= (currentN - 1) / currentN;
            }
            currentN--;
            eventIdx++;
          }
          survivalCurve.push({ month: m, retained: Math.round(currentSurv * 1000) / 10 });

          // Check for median
          if (!medianFound && currentSurv < 0.5) {
            medianTenure = m;
            medianFound = true;
          }
        }

        // If median not found from KM, use simple median of all tenures
        if (!medianFound) {
          const allTenures = tenures.map((t) => t.tenure).sort((a, b) => a - b);
          medianTenure = Math.round(allTenures[Math.floor(allTenures.length / 2)] * 10) / 10;
        } else {
          medianTenure = Math.round(medianTenure * 10) / 10;
        }

        // ── Month-4 Renewal Rate ──
        // Members who reached month 3 and chose to continue to month 4
        const reachedMonth3 = tenures.filter((t) => t.tenure >= CLIFF_MONTHS);
        const reachedMonth4 = tenures.filter((t) => t.tenure >= CLIFF_MONTHS + 1);
        const month4RenewalRate = reachedMonth3.length > 0
          ? Math.round((reachedMonth4.length / reachedMonth3.length) * 1000) / 10
          : 0;

        // ── Avg Post-Cliff Tenure ──
        // For members surviving past the 3-month cliff, average total tenure
        const postCliffMembers = tenures.filter((t) => t.tenure > CLIFF_MONTHS);
        const avgPostCliffTenure = postCliffMembers.length > 0
          ? Math.round((postCliffMembers.reduce((s, t) => s + t.tenure, 0) / postCliffMembers.length) * 10) / 10
          : 0;

        result.tenureMetrics = {
          medianTenure,
          month4RenewalRate,
          avgPostCliffTenure,
          survivalCurve,
        };
      }
    }

    catResults[cat] = result;
  }

  const totalAtRisk = CATEGORIES.reduce((s, c) => s + catResults[c].atRiskCount, 0);

  // ── Member alerts: renewals approaching + tenure milestones ──
  const memberAlerts = computeMemberAlerts(categorized, ACTIVE_STATES);

  // Legacy backward-compat monthly array
  const legacyMonthly = months.map((month) => {
    const mem = catResults.MEMBER.monthly.find((m) => m.month === month)!;
    const sky3 = catResults.SKY3.monthly.find((m) => m.month === month)!;
    return {
      month,
      memberRate: mem.userChurnRate,
      sky3Rate: sky3.userChurnRate,
      memberActiveStart: mem.activeAtStart,
      sky3ActiveStart: sky3.activeAtStart,
      memberCanceled: mem.canceledCount,
      sky3Canceled: sky3.canceledCount,
    };
  });

  return {
    byCategory: {
      member: catResults.MEMBER,
      sky3: catResults.SKY3,
      skyTingTv: catResults.SKY_TING_TV,
    },
    totalAtRisk,
    memberAlerts,
    // Legacy flat fields
    monthly: legacyMonthly,
    avgMemberRate: catResults.MEMBER.avgUserChurnRate,
    avgSky3Rate: catResults.SKY3.avgUserChurnRate,
    atRisk: totalAtRisk,
  };
}
