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
} from "../sheets/read-dashboard";
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
    try {
      const allPeriods = await getAllPeriods();
      // Find the longest period that starts in the prior year
      for (const p of allPeriods) {
        if (p.periodStart.startsWith(String(priorYear)) && p.totalNetRevenue > 0) {
          // Normalize to 12 months if the period is longer or shorter
          const pStart = new Date(p.periodStart);
          const pEnd = new Date(p.periodEnd);
          const periodDays = (pEnd.getTime() - pStart.getTime()) / (1000 * 60 * 60 * 24);
          if (periodDays > 0) {
            const annualized = p.totalNetRevenue / periodDays * 365;
            priorYearActualRevenue = Math.round(annualized);
          } else {
            priorYearActualRevenue = Math.round(p.totalNetRevenue);
          }
          break;
        }
      }
    } catch {
      // revenue_categories may not exist yet
    }

    // MRR-based estimate as fallback
    const priorMonths = mrrSeries.filter((e) => e.month.startsWith(String(priorYear)));
    if (priorMonths.length > 0) {
      const total = priorMonths.reduce((sum, e) => sum + e.mrr, 0);
      priorYearRevenue = priorMonths.length < 12
        ? (total / priorMonths.length) * 12
        : total;
    }

    // Use actual if available
    if (priorYearActualRevenue && priorYearActualRevenue > priorYearRevenue) {
      priorYearRevenue = priorYearActualRevenue;
    }

    // Compute non-MRR revenue ratio from actual data
    // (drop-ins, workshops, retail, teacher training, etc.)
    // If we have actual prior year revenue > MRR-based estimate, the ratio tells us
    // how much total revenue exceeds subscription-only revenue
    const mrrBasedPriorTotal = priorMonths.length > 0
      ? (priorMonths.length < 12
        ? (priorMonths.reduce((s, e) => s + e.mrr, 0) / priorMonths.length) * 12
        : priorMonths.reduce((s, e) => s + e.mrr, 0))
      : 0;
    const nonMrrMultiplier = (priorYearActualRevenue && mrrBasedPriorTotal > 0)
      ? priorYearActualRevenue / mrrBasedPriorTotal
      : 1;

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
    const projectedAnnualRevenue = Math.round(projectedMrrRevenue * nonMrrMultiplier);

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
 * Compute proper monthly churn rates: cancellations / active-at-start-of-month.
 *
 * "Active at start of month M" = created before M AND
 *   (still currently active OR canceled on/after month M start).
 *
 * This reconstructs historical active counts from the snapshot data.
 */
async function computeChurnRates(): Promise<ChurnRateData | null> {
  const pool = getPool();

  const { rows: allRows } = await pool.query(
    `SELECT plan_name, plan_state, plan_price, canceled_at, created_at FROM auto_renews`
  );

  if (allRows.length === 0) return null;

  const categorized = allRows.map((r: Record<string, unknown>) => ({
    plan_name: r.plan_name as string,
    plan_state: r.plan_state as string,
    plan_price: r.plan_price as number,
    canceled_at: r.canceled_at as string | null,
    created_at: r.created_at as string,
    category: getCategory(r.plan_name as string),
  }));

  const ACTIVE_STATES = ["Valid Now", "Pending Cancel", "Paused", "Past Due", "In Trial"];

  // Generate last 6 completed months + current month
  const now = new Date();
  const months: string[] = [];
  for (let i = 6; i >= 0; i--) {
    const d = new Date(now.getFullYear(), now.getMonth() - i, 1);
    months.push(
      d.getFullYear() + "-" + String(d.getMonth() + 1).padStart(2, "0")
    );
  }

  const results: ChurnRateData["monthly"] = [];

  for (const month of months) {
    const monthStart = month + "-01";
    const [yearStr, moStr] = month.split("-");
    const nextMonth = new Date(parseInt(yearStr), parseInt(moStr), 1);
    const monthEnd =
      nextMonth.getFullYear() +
      "-" +
      String(nextMonth.getMonth() + 1).padStart(2, "0") +
      "-01";

    for (const cat of ["MEMBER", "SKY3"] as const) {
      const catRows = categorized.filter((r) => r.category === cat);

      // Active at start of month: created before monthStart AND
      //   (still in an active state OR canceled on/after monthStart)
      const activeAtStart = catRows.filter((r) => {
        if (r.created_at >= monthStart) return false;
        if (ACTIVE_STATES.includes(r.plan_state)) return true;
        if (r.canceled_at && r.canceled_at >= monthStart) return true;
        return false;
      });

      // Canceled during this month
      const canceledInMonth = catRows.filter(
        (r) =>
          r.canceled_at &&
          r.canceled_at >= monthStart &&
          r.canceled_at < monthEnd
      );

      const existing = results.find((r) => r.month === month);
      if (existing) {
        if (cat === "SKY3") {
          existing.sky3Rate =
            activeAtStart.length > 0
              ? Math.round((canceledInMonth.length / activeAtStart.length) * 1000) / 10
              : 0;
          existing.sky3ActiveStart = activeAtStart.length;
          existing.sky3Canceled = canceledInMonth.length;
        } else {
          existing.memberRate =
            activeAtStart.length > 0
              ? Math.round((canceledInMonth.length / activeAtStart.length) * 1000) / 10
              : 0;
          existing.memberActiveStart = activeAtStart.length;
          existing.memberCanceled = canceledInMonth.length;
        }
      } else {
        results.push({
          month,
          memberRate:
            cat === "MEMBER" && activeAtStart.length > 0
              ? Math.round((canceledInMonth.length / activeAtStart.length) * 1000) / 10
              : 0,
          sky3Rate:
            cat === "SKY3" && activeAtStart.length > 0
              ? Math.round((canceledInMonth.length / activeAtStart.length) * 1000) / 10
              : 0,
          memberActiveStart: cat === "MEMBER" ? activeAtStart.length : 0,
          sky3ActiveStart: cat === "SKY3" ? activeAtStart.length : 0,
          memberCanceled: cat === "MEMBER" ? canceledInMonth.length : 0,
          sky3Canceled: cat === "SKY3" ? canceledInMonth.length : 0,
        });
      }
    }
  }

  // Averages (exclude current partial month)
  const completedMonths = results.slice(0, -1);
  const avgMemberRate =
    completedMonths.length > 0
      ? Math.round(
          (completedMonths.reduce((s, r) => s + r.memberRate, 0) /
            completedMonths.length) *
            10
        ) / 10
      : 0;
  const avgSky3Rate =
    completedMonths.length > 0
      ? Math.round(
          (completedMonths.reduce((s, r) => s + r.sky3Rate, 0) /
            completedMonths.length) *
            10
        ) / 10
      : 0;

  // At-risk count
  const atRiskResult = await pool.query(
    `SELECT COUNT(*) as cnt FROM auto_renews WHERE plan_state IN ('Past Due', 'Invalid', 'Pending Cancel')`
  );
  const atRisk = Number(atRiskResult.rows[0].cnt);

  return {
    monthly: results,
    avgMemberRate,
    avgSky3Rate,
    atRisk,
  };
}
