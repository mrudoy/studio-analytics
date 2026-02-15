import { getDaysInMonth } from "date-fns";
import type { AutoRenew, Order } from "@/types/union-data";
import type { SummaryKPIs } from "./summary";
import { getCategory } from "./categories";
import { parseDate, getWeekKey, getMonthKey } from "./date-utils";

// ─── Types ───────────────────────────────────────────────────

export interface TrendPeriod {
  period: string;
  newMembers: number;
  newSky3: number;
  newSkyTingTv: number;
  memberCancellations: number;
  sky3Cancellations: number;
  skyTingTvCancellations: number;
  netMemberGrowth: number;
  netSky3Growth: number;
  revenue: number;
  revenueLost: number;
}

export interface TrendDelta {
  current: TrendPeriod;
  previous: TrendPeriod | null;
  delta: {
    newMembers: number | null;
    newSky3: number | null;
    revenue: number | null;
    memberCancellations: number | null;
    sky3Cancellations: number | null;
  } | null;
  deltaPercent: {
    newMembers: number | null;
    newSky3: number | null;
    revenue: number | null;
  } | null;
}

export interface CurrentMonthPacing {
  month: string;
  daysElapsed: number;
  daysInMonth: number;
  newMembersActual: number;
  newSky3Actual: number;
  revenueActual: number;
  newMembersPaced: number;
  newSky3Paced: number;
  revenuePaced: number;
  memberCancellationsActual: number;
  sky3CancellationsActual: number;
  memberCancellationsPaced: number;
  sky3CancellationsPaced: number;
}

export interface AnnualProjection {
  year: number;
  currentMRR: number;
  monthlyGrowthRate: number;
  projectedAnnualRevenue: number;
  projectedYearEndMRR: number;
}

export interface TrendsResults {
  weekly: TrendDelta[];
  monthly: TrendDelta[];
  currentMonthPacing: CurrentMonthPacing;
  annualProjection: AnnualProjection;
}

// ─── Helpers ─────────────────────────────────────────────────

interface PeriodBucket {
  newMembers: number;
  newSky3: number;
  newSkyTingTv: number;
  memberCancellations: number;
  sky3Cancellations: number;
  skyTingTvCancellations: number;
  revenue: number;
  revenueLost: number;
}

function emptyBucket(): PeriodBucket {
  return {
    newMembers: 0,
    newSky3: 0,
    newSkyTingTv: 0,
    memberCancellations: 0,
    sky3Cancellations: 0,
    skyTingTvCancellations: 0,
    revenue: 0,
    revenueLost: 0,
  };
}

function getOrCreate(map: Map<string, PeriodBucket>, key: string): PeriodBucket {
  if (!map.has(key)) map.set(key, emptyBucket());
  return map.get(key)!;
}

function bucketToTrendPeriod(period: string, b: PeriodBucket): TrendPeriod {
  return {
    period,
    newMembers: b.newMembers,
    newSky3: b.newSky3,
    newSkyTingTv: b.newSkyTingTv,
    memberCancellations: b.memberCancellations,
    sky3Cancellations: b.sky3Cancellations,
    skyTingTvCancellations: b.skyTingTvCancellations,
    netMemberGrowth: b.newMembers - b.memberCancellations,
    netSky3Growth: b.newSky3 - b.sky3Cancellations,
    revenue: Math.round(b.revenue * 100) / 100,
    revenueLost: Math.round(b.revenueLost * 100) / 100,
  };
}

function pctChange(current: number, previous: number): number | null {
  if (previous === 0) return current > 0 ? 100 : null;
  return Math.round(((current - previous) / previous) * 1000) / 10;
}

function buildDeltas(periods: TrendPeriod[]): TrendDelta[] {
  return periods.map((current, i) => {
    const previous = i > 0 ? periods[i - 1] : null;
    if (!previous) {
      return { current, previous: null, delta: null, deltaPercent: null };
    }
    return {
      current,
      previous,
      delta: {
        newMembers: current.newMembers - previous.newMembers,
        newSky3: current.newSky3 - previous.newSky3,
        revenue: Math.round((current.revenue - previous.revenue) * 100) / 100,
        memberCancellations: current.memberCancellations - previous.memberCancellations,
        sky3Cancellations: current.sky3Cancellations - previous.sky3Cancellations,
      },
      deltaPercent: {
        newMembers: pctChange(current.newMembers, previous.newMembers),
        newSky3: pctChange(current.newSky3, previous.newSky3),
        revenue: pctChange(current.revenue, previous.revenue),
      },
    };
  });
}

// ─── Main ────────────────────────────────────────────────────

export function analyzeTrends(
  newAutoRenews: AutoRenew[],
  canceledAutoRenews: AutoRenew[],
  activeAutoRenews: AutoRenew[],
  summary: SummaryKPIs,
  orders: Order[] = []
): TrendsResults {
  const weeklyBuckets = new Map<string, PeriodBucket>();
  const monthlyBuckets = new Map<string, PeriodBucket>();

  // --- Bucket new auto-renews ---
  for (const ar of newAutoRenews) {
    const date = parseDate(ar.created || "");
    if (!date) continue;
    const cat = getCategory(ar.name);
    const wk = getWeekKey(date);
    const mo = getMonthKey(date);
    const wBucket = getOrCreate(weeklyBuckets, wk);
    const mBucket = getOrCreate(monthlyBuckets, mo);

    if (cat === "MEMBER") {
      wBucket.newMembers++;
      mBucket.newMembers++;
    } else if (cat === "SKY3") {
      wBucket.newSky3++;
      mBucket.newSky3++;
    } else if (cat === "SKY_TING_TV") {
      wBucket.newSkyTingTv++;
      mBucket.newSkyTingTv++;
    }
    // NOTE: revenue now comes from orders (below), not subscription prices
  }

  // --- Bucket canceled auto-renews ---
  for (const ar of canceledAutoRenews) {
    const date = parseDate(ar.canceledAt || "");
    if (!date) continue;
    const cat = getCategory(ar.name);
    const wk = getWeekKey(date);
    const mo = getMonthKey(date);
    const wBucket = getOrCreate(weeklyBuckets, wk);
    const mBucket = getOrCreate(monthlyBuckets, mo);

    if (cat === "MEMBER") {
      wBucket.memberCancellations++;
      mBucket.memberCancellations++;
    } else if (cat === "SKY3") {
      wBucket.sky3Cancellations++;
      mBucket.sky3Cancellations++;
    } else if (cat === "SKY_TING_TV") {
      wBucket.skyTingTvCancellations++;
      mBucket.skyTingTvCancellations++;
    }
    wBucket.revenueLost += ar.price;
    mBucket.revenueLost += ar.price;
  }

  // --- Bucket actual revenue from orders ---
  for (const order of orders) {
    const date = parseDate(order.created || "");
    if (!date || order.total <= 0) continue;
    const wk = getWeekKey(date);
    const mo = getMonthKey(date);
    const wBucket = getOrCreate(weeklyBuckets, wk);
    const mBucket = getOrCreate(monthlyBuckets, mo);
    wBucket.revenue += order.total;
    mBucket.revenue += order.total;
  }

  // --- Sort and convert to TrendPeriod arrays ---
  const allWeekKeys = Array.from(weeklyBuckets.keys()).sort();
  const allMonthKeys = Array.from(monthlyBuckets.keys()).sort();

  // Take last 8 weeks
  const recentWeekKeys = allWeekKeys.slice(-8);
  const weeklyPeriods = recentWeekKeys.map((wk) =>
    bucketToTrendPeriod(wk, weeklyBuckets.get(wk)!)
  );

  // Take last 6 months
  const recentMonthKeys = allMonthKeys.slice(-6);
  const monthlyPeriods = recentMonthKeys.map((mo) =>
    bucketToTrendPeriod(mo, monthlyBuckets.get(mo)!)
  );

  // --- Build deltas ---
  const weekly = buildDeltas(weeklyPeriods);
  const monthly = buildDeltas(monthlyPeriods);

  // --- Current month pacing ---
  const now = new Date();
  const currentMonthKey = getMonthKey(now);
  const currentBucket = monthlyBuckets.get(currentMonthKey) || emptyBucket();
  const daysElapsed = now.getDate();
  const daysTotal = getDaysInMonth(now);
  const pacingMultiplier = daysElapsed > 0 ? daysTotal / daysElapsed : 1;

  const currentMonthPacing: CurrentMonthPacing = {
    month: currentMonthKey,
    daysElapsed,
    daysInMonth: daysTotal,
    newMembersActual: currentBucket.newMembers,
    newSky3Actual: currentBucket.newSky3,
    revenueActual: Math.round(currentBucket.revenue * 100) / 100,
    newMembersPaced: Math.round(currentBucket.newMembers * pacingMultiplier),
    newSky3Paced: Math.round(currentBucket.newSky3 * pacingMultiplier),
    revenuePaced: Math.round(currentBucket.revenue * pacingMultiplier * 100) / 100,
    memberCancellationsActual: currentBucket.memberCancellations,
    sky3CancellationsActual: currentBucket.sky3Cancellations,
    memberCancellationsPaced: Math.round(currentBucket.memberCancellations * pacingMultiplier),
    sky3CancellationsPaced: Math.round(currentBucket.sky3Cancellations * pacingMultiplier),
  };

  // --- Annual projection ---
  // Use last 3 completed months (exclude current partial month) to compute avg MoM net revenue growth
  const completedMonthKeys = allMonthKeys.filter((k) => k < currentMonthKey);
  const last3Completed = completedMonthKeys.slice(-3);

  let monthlyGrowthRate = 0;
  if (last3Completed.length >= 2) {
    const growthRates: number[] = [];
    for (let i = 1; i < last3Completed.length; i++) {
      const prev = monthlyBuckets.get(last3Completed[i - 1])!;
      const curr = monthlyBuckets.get(last3Completed[i])!;
      const prevNet = prev.revenue - prev.revenueLost;
      const currNet = curr.revenue - curr.revenueLost;
      if (prevNet > 0) {
        growthRates.push((currNet - prevNet) / prevNet);
      }
    }
    if (growthRates.length > 0) {
      monthlyGrowthRate = growthRates.reduce((a, b) => a + b, 0) / growthRates.length;
    }
  }

  const currentMRR = summary.mrrTotal;
  const currentYear = now.getFullYear();
  const currentMonth = now.getMonth(); // 0-indexed

  // Project remaining months of the year
  let projectedAnnualRevenue = 0;
  let projectedMRR = currentMRR;

  // Add actual revenue for completed months this year
  for (const monthKey of completedMonthKeys) {
    if (monthKey.startsWith(String(currentYear))) {
      const bucket = monthlyBuckets.get(monthKey)!;
      projectedAnnualRevenue += bucket.revenue - bucket.revenueLost;
    }
  }

  // Add paced current month
  projectedAnnualRevenue += currentMonthPacing.revenuePaced;

  // Project forward for remaining months
  const remainingMonths = 11 - currentMonth; // months after current
  for (let i = 0; i < remainingMonths; i++) {
    projectedMRR = projectedMRR * (1 + monthlyGrowthRate);
    projectedAnnualRevenue += projectedMRR;
  }

  const annualProjection: AnnualProjection = {
    year: currentYear,
    currentMRR: Math.round(currentMRR * 100) / 100,
    monthlyGrowthRate: Math.round(monthlyGrowthRate * 10000) / 100, // as percentage
    projectedAnnualRevenue: Math.round(projectedAnnualRevenue),
    projectedYearEndMRR: Math.round(projectedMRR * 100) / 100,
  };

  console.log(
    `[trends] Weekly: ${weekly.length} periods, Monthly: ${monthly.length} periods, ` +
    `Growth rate: ${annualProjection.monthlyGrowthRate}%, ` +
    `Projected annual: $${annualProjection.projectedAnnualRevenue}`
  );

  return {
    weekly,
    monthly,
    currentMonthPacing,
    annualProjection,
  };
}
