/**
 * Compute TrendsData from SQLite data.
 *
 * Returns the same shape as readTrendsData() from Sheets so the
 * dashboard components need zero changes. Returns null if SQLite doesn't
 * have enough data (caller falls back to Sheets).
 *
 * Data sources:
 *   - subscriptions table  → weekly/monthly trends, pacing, projection
 *   - registrations table  → drop-in analytics
 *   - first_visits table   → first visit stats, returning non-members
 */

import { getCategory, isAnnualPlan } from "./categories";
import { parseDate, getWeekKey, getMonthKey } from "./date-utils";
import {
  getNewSubscriptions,
  getCanceledSubscriptions,
  getSubscriptionStats,
  hasSubscriptionData,
} from "../db/subscription-store";
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
} from "../sheets/read-dashboard";

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

export function computeTrendsFromSQLite(): TrendsData | null {
  if (!hasSubscriptionData()) {
    console.log("[sqlite-trends] No subscription data in SQLite — skipping");
    return null;
  }

  // ── 1. Build weekly/monthly buckets from subscription data ──
  const now = new Date();
  const currentMonthKey = getMonthKey(now);
  const currentWeekKey = getWeekKey(now);

  // Look back ~6 months for trend data
  const sixMonthsAgo = new Date(now);
  sixMonthsAgo.setMonth(sixMonthsAgo.getMonth() - 6);
  const startDate = `${sixMonthsAgo.getFullYear()}-${String(sixMonthsAgo.getMonth() + 1).padStart(2, "0")}-01`;
  const endDate = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, "0")}-${String(now.getDate() + 1).padStart(2, "0")}`;

  const newSubs = getNewSubscriptions(startDate, endDate);
  const canceledSubs = getCanceledSubscriptions(startDate, endDate);

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
  const subStats = getSubscriptionStats();

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

    // Prior year revenue estimate
    const priorYear = currentYear - 1;
    let priorYearRevenue = 0;
    const priorMonths = mrrSeries.filter((e) => e.month.startsWith(String(priorYear)));
    if (priorMonths.length > 0) {
      const total = priorMonths.reduce((sum, e) => sum + e.mrr, 0);
      priorYearRevenue = priorMonths.length < 12
        ? (total / priorMonths.length) * 12
        : total;
    }

    // Project annual revenue
    let projectedAnnualRevenue = 0;
    let projectedMRR = currentMRR;

    // Completed months this year
    for (const mo of completedMonthKeys) {
      if (mo.startsWith(String(currentYear))) {
        const entry = mrrSeries.find((s) => s.month === mo);
        projectedAnnualRevenue += entry ? entry.mrr : currentMRR;
      }
    }

    // Paced current month
    projectedAnnualRevenue += pacing.revenuePaced > 0 ? pacing.revenuePaced : currentMRR;

    // Remaining months with growth
    const remainingMonths = 11 - currentMonth;
    for (let i = 0; i < remainingMonths; i++) {
      projectedMRR = projectedMRR * (1 + monthlyGrowthRate);
      projectedAnnualRevenue += projectedMRR;
    }

    projection = {
      year: currentYear,
      projectedAnnualRevenue: Math.round(projectedAnnualRevenue),
      currentMRR: Math.round(currentMRR * 100) / 100,
      projectedYearEndMRR: Math.round(projectedMRR * 100) / 100,
      monthlyGrowthRate: Math.round(monthlyGrowthRate * 10000) / 100,
      priorYearRevenue: Math.round(priorYearRevenue),
    };
  }

  // ── 5. Drop-in data ──────────────────────────────────────
  let dropIns: DropInData | null = null;

  if (hasRegistrationData()) {
    const dropInStats = getDropInStats();
    if (dropInStats) {
      // Get weekly breakdown from registrations
      const eightWeeksAgo = new Date(now);
      eightWeeksAgo.setDate(eightWeeksAgo.getDate() - 56);
      const dropInWeeks = getDropInsByWeek(
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

  if (hasFirstVisitData()) {
    const fiveWeeksAgo = new Date(now);
    fiveWeeksAgo.setDate(fiveWeeksAgo.getDate() - 35);
    const startStr = fiveWeeksAgo.toISOString().split("T")[0];

    const uvWeeks = getFirstTimeUniqueVisitorsByWeek(startStr);
    const sourceBreakdown = getFirstTimeSourceBreakdown(startStr);

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

  if (hasRegistrationData()) {
    try {
      const fiveWeeksAgo = new Date(now);
      fiveWeeksAgo.setDate(fiveWeeksAgo.getDate() - 35);
      const startStr = fiveWeeksAgo.toISOString().split("T")[0];

      const uvWeeks = getReturningUniqueVisitorsByWeek(startStr);
      const sourceBreakdown = getReturningSourceBreakdown(startStr);

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
      console.warn("[sqlite-trends] Failed to compute returning non-members:", err);
    }
  }

  console.log(
    `[sqlite-trends] Computed from SQLite: ${weekly.length} weekly, ${monthly.length} monthly periods` +
    (dropIns ? `, drop-ins MTD=${dropIns.currentMonthTotal}` : "") +
    (firstVisits ? `, first visits this week=${firstVisits.currentWeekTotal}` : "")
  );

  return {
    weekly,
    monthly,
    pacing,
    projection,
    dropIns,
    firstVisits,
    returningNonMembers,
  };
}
