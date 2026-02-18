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
  getDropInsByWeek,
  getDropInStats,
  hasRegistrationData,
  hasFirstVisitData,
  getFirstTimeUniqueVisitorsByWeek,
  getFirstTimeSourceBreakdown,
  getReturningUniqueVisitorsByWeek,
  getReturningSourceBreakdown,
} from "../db/registration-store";
import type {
  TrendsData,
  TrendRowData,
  PacingData,
  ProjectionData,
  DropInData,
  FirstVisitData,
  ReturningNonMemberData,
  ChurnRateData,
  CategoryChurnData,
  CategoryMonthlyChurn,
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
        const totalNet = priorYearPeriods.reduce((sum, p) => sum + p.totalNetRevenue, 0);

        // Determine how many months the data actually spans
        // Check if any period covers a full year (e.g. 2025-01-01 to 2025-12-31)
        const spansFullYear = priorYearPeriods.some((p) => {
          const start = new Date(p.periodStart + "T00:00:00");
          const end = new Date(p.periodEnd + "T00:00:00");
          const months = (end.getFullYear() - start.getFullYear()) * 12 + (end.getMonth() - start.getMonth());
          console.log(`[db-trends] Period span check: ${p.periodStart} to ${p.periodEnd}, start=${start.toISOString()}, end=${end.toISOString()}, months=${months}`);
          return months >= 11; // covers 11+ months = full year
        });

        if (spansFullYear) {
          // Data covers full year — use actual total, no extrapolation
          priorYearActualRevenue = Math.round(totalNet);
          console.log(`[db-trends] Prior year ${priorYear}: full-year period detected, using actual total $${priorYearActualRevenue.toLocaleString()}`);
        } else {
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
        const totalNet = priorYearPeriods.reduce((sum, p) => sum + p.totalNetRevenue, 0);
        const spansFullYear = priorYearPeriods.some((p) => {
          const start = new Date(p.periodStart + "T00:00:00");
          const end = new Date(p.periodEnd + "T00:00:00");
          const months = (end.getFullYear() - start.getFullYear()) * 12 + (end.getMonth() - start.getMonth());
          return months >= 11;
        });
        if (spansFullYear) {
          priorYearActual = Math.round(totalNet);
        } else {
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

  // ── 5. Drop-in data ──────────────────────────────────────
  let dropIns: DropInData | null = null;

  if (await hasRegistrationData()) {
    const dropInStats = await getDropInStats();
    if (dropInStats) {
      // Get weekly breakdown from registrations
      const eightWeeksAgo = new Date(now);
      eightWeeksAgo.setDate(eightWeeksAgo.getDate() - 56);
      const dropInWeeks = await getDropInsByWeek(
        eightWeeksAgo.toISOString().split("T")[0]
      );

      dropIns = {
        currentMonthTotal: dropInStats.currentMonthTotal,
        currentMonthDaysElapsed: dropInStats.currentMonthDaysElapsed,
        currentMonthDaysInMonth: dropInStats.currentMonthDaysInMonth,
        currentMonthPaced: dropInStats.currentMonthPaced,
        previousMonthTotal: dropInStats.previousMonthTotal,
        weeklyAvg6w: dropInStats.weeklyAvg6w,
        weeklyBreakdown: dropInWeeks.map((w) => ({ week: w.week, count: w.count })),
      };
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

  console.log(
    `[db-trends] Computed: ${weekly.length} weekly, ${monthly.length} monthly periods` +
    (dropIns ? `, drop-ins MTD=${dropIns.currentMonthTotal}` : "") +
    (firstVisits ? `, first visits this week=${firstVisits.currentWeekTotal}` : "") +
    (churnRates ? `, member churn=${churnRates.avgMemberRate.toFixed(1)}%, sky3 churn=${churnRates.avgSky3Rate.toFixed(1)}%` : "")
  );

  return {
    weekly,
    monthly,
    pacing,
    projection,
    dropIns,
    firstVisits,
    returningNonMembers,
    churnRates,
  };
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
    `SELECT plan_name, plan_state, plan_price, canceled_at, created_at FROM auto_renews`
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

      // MEMBER-only: annual vs monthly breakdown
      if (cat === "MEMBER") {
        const annualActive = activeAtStart.filter((r) => r.isAnnual);
        const monthlyActive = activeAtStart.filter((r) => !r.isAnnual);
        const annualCanceled = canceledInMonth.filter((r) => r.isAnnual);
        const monthlyCanceled = canceledInMonth.filter((r) => !r.isAnnual);
        entry.annualActiveAtStart = annualActive.length;
        entry.annualCanceledCount = annualCanceled.length;
        entry.monthlyActiveAtStart = monthlyActive.length;
        entry.monthlyCanceledCount = monthlyCanceled.length;
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

    catResults[cat] = {
      category: cat,
      monthly: monthlyChurn,
      avgUserChurnRate: avgUser,
      avgMrrChurnRate: avgMrr,
      atRiskCount,
    };
  }

  const totalAtRisk = CATEGORIES.reduce((s, c) => s + catResults[c].atRiskCount, 0);

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
    // Legacy flat fields
    monthly: legacyMonthly,
    avgMemberRate: catResults.MEMBER.avgUserChurnRate,
    avgSky3Rate: catResults.SKY3.avgUserChurnRate,
    atRisk: totalAtRisk,
  };
}
