import { getDaysInMonth } from "date-fns";
import type { AutoRenew, Order, FirstVisit } from "@/types/union-data";
import type { SummaryKPIs } from "./summary";
import { getCategory, isAnnualPlan, isDropInOrIntro } from "./categories";
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
  priorYearRevenue: number; // estimated from reconstructed MRR
}

export interface DropInStats {
  currentMonthTotal: number;
  currentMonthDaysElapsed: number;
  currentMonthDaysInMonth: number;
  currentMonthPaced: number;
  previousMonthTotal: number;
  weeklyAvg6w: number; // rolling 6-week average
  weeklyBreakdown: { week: string; count: number }[]; // last 8 weeks
}

export type FirstVisitSegment = "introWeek" | "dropIn" | "guest" | "other";

export interface FirstVisitWeek {
  week: string;
  count: number;
  segments: Record<FirstVisitSegment, number>;
}

export interface FirstVisitStats {
  currentWeekTotal: number;       // cumulative this week (in progress)
  currentWeekSegments: Record<FirstVisitSegment, number>;
  completedWeeks: FirstVisitWeek[];  // last 4 completed weeks
  aggregateSegments: Record<FirstVisitSegment, number>; // unique people by source across full window
}

export interface TrendsResults {
  weekly: TrendDelta[];
  monthly: TrendDelta[];
  currentMonthPacing: CurrentMonthPacing;
  annualProjection: AnnualProjection;
  dropIns: DropInStats;
  firstVisitStats: FirstVisitStats;
  returningNonMembers: import("./returning-non-members").ReturningNonMemberStats | null;
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
  /** Sum of prices from new auto-renews — fallback when order data is unavailable */
  subscriptionRevenueAdded: number;
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
    subscriptionRevenueAdded: 0,
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
  orders: Order[] = [],
  firstVisits: FirstVisit[] = []
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
    // Track subscription revenue for fallback when orders data is limited
    wBucket.subscriptionRevenueAdded += ar.price;
    mBucket.subscriptionRevenueAdded += ar.price;
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

  // --- Fill gaps: if a period has $0 order revenue but has subscription
  //     activity, use the subscription revenue as a fallback estimate.
  //     Union.fit's Orders export only covers ~1 month of history, so older
  //     months need this fallback to avoid showing $0 in the chart. ---
  for (const bucket of weeklyBuckets.values()) {
    if (bucket.revenue === 0 && bucket.subscriptionRevenueAdded > 0) {
      bucket.revenue = bucket.subscriptionRevenueAdded;
    }
  }
  for (const bucket of monthlyBuckets.values()) {
    if (bucket.revenue === 0 && bucket.subscriptionRevenueAdded > 0) {
      bucket.revenue = bucket.subscriptionRevenueAdded;
    }
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

  // --- Annual projection using subscription-based MRR growth ---
  // Proper SaaS accounting: compute MRR growth from subscription gains/losses,
  // not from order revenue (which may be incomplete or only cover recent months).
  //
  // Method: For each month, compute net MRR change = MRR gained - MRR lost.
  // Then reconstruct historical MRR estimates by working backwards from current MRR.
  // Growth rate = average MoM % change in reconstructed MRR.

  const completedMonthKeys = allMonthKeys.filter((k) => k < currentMonthKey);
  const currentMRR = summary.mrrTotal;
  const currentYear = now.getFullYear();
  const currentMonth = now.getMonth(); // 0-indexed

  // Compute per-month net MRR change from subscription data
  // (new subscriptions added × their monthly price) - (canceled × their monthly price)
  const monthlyMrrGained = new Map<string, number>();
  const monthlyMrrLost = new Map<string, number>();

  for (const ar of newAutoRenews) {
    const date = parseDate(ar.created || "");
    if (!date) continue;
    const mo = getMonthKey(date);
    const monthlyPrice = isAnnualPlan(ar.name) ? ar.price / 12 : ar.price;
    monthlyMrrGained.set(mo, (monthlyMrrGained.get(mo) || 0) + monthlyPrice);
  }

  for (const ar of canceledAutoRenews) {
    const date = parseDate(ar.canceledAt || "");
    if (!date) continue;
    const mo = getMonthKey(date);
    const monthlyPrice = isAnnualPlan(ar.name) ? ar.price / 12 : ar.price;
    monthlyMrrLost.set(mo, (monthlyMrrLost.get(mo) || 0) + monthlyPrice);
  }

  // Reconstruct MRR series by working backwards from current MRR
  // currentMRR = MRR at end of last completed month
  // previous month MRR = currentMRR - netChange(last completed month)
  // Go back ALL completed months (not just 6) so we can estimate prior year revenue
  const mrrSeries: { month: string; mrr: number }[] = [];
  let backtrackMrr = currentMRR;

  // Build series from most recent backwards
  for (let i = completedMonthKeys.length - 1; i >= 0; i--) {
    const mo = completedMonthKeys[i];
    mrrSeries.unshift({ month: mo, mrr: backtrackMrr });
    const gained = monthlyMrrGained.get(mo) || 0;
    const lost = monthlyMrrLost.get(mo) || 0;
    backtrackMrr = backtrackMrr - gained + lost; // undo this month's changes to get start-of-month
  }

  // Compute MoM growth rates from the last 6 months of reconstructed MRR
  const recentMrrSeries = mrrSeries.slice(-6);
  let monthlyGrowthRate = 0;
  if (recentMrrSeries.length >= 2) {
    const growthRates: number[] = [];
    for (let i = 1; i < recentMrrSeries.length; i++) {
      const prevMrr = recentMrrSeries[i - 1].mrr;
      const currMrr = recentMrrSeries[i].mrr;
      if (prevMrr > 0) {
        growthRates.push((currMrr - prevMrr) / prevMrr);
      }
    }
    if (growthRates.length > 0) {
      monthlyGrowthRate = growthRates.reduce((a, b) => a + b, 0) / growthRates.length;
    }
  }

  // Log full MRR history
  console.log(`[trends] MRR reconstruction (subscription-based, ${mrrSeries.length} months):`);
  for (const s of mrrSeries) {
    const gained = monthlyMrrGained.get(s.month) || 0;
    const lost = monthlyMrrLost.get(s.month) || 0;
    console.log(`  ${s.month}: MRR=$${s.mrr.toFixed(0)}, +$${gained.toFixed(0)} gained, -$${lost.toFixed(0)} lost`);
  }
  console.log(`  Avg monthly growth (last ${recentMrrSeries.length} months): ${(monthlyGrowthRate * 100).toFixed(2)}%`);

  // Estimate prior year (e.g. 2025) revenue from reconstructed MRR
  const priorYear = currentYear - 1;
  let priorYearRevenue = 0;
  for (const entry of mrrSeries) {
    if (entry.month.startsWith(String(priorYear))) {
      priorYearRevenue += entry.mrr;
    }
  }
  // If we don't have full 12 months for prior year, extrapolate from available months
  const priorYearMonths = mrrSeries.filter(e => e.month.startsWith(String(priorYear))).length;
  if (priorYearMonths > 0 && priorYearMonths < 12) {
    priorYearRevenue = (priorYearRevenue / priorYearMonths) * 12;
    console.log(`[trends] Prior year ${priorYear}: ${priorYearMonths} months of MRR data, extrapolated to 12 months: $${Math.round(priorYearRevenue).toLocaleString()}`);
  } else {
    console.log(`[trends] Prior year ${priorYear} est. revenue (from MRR): $${Math.round(priorYearRevenue).toLocaleString()}`);
  }

  // Project remaining months of the year
  let projectedAnnualRevenue = 0;
  let projectedMRR = currentMRR;

  // For completed months this year: use order revenue if available, otherwise use MRR estimate
  for (const monthKey of completedMonthKeys) {
    if (monthKey.startsWith(String(currentYear))) {
      const bucket = monthlyBuckets.get(monthKey)!;
      // Use order revenue if we have it; otherwise use reconstructed MRR as monthly revenue estimate
      if (bucket.revenue > 0) {
        projectedAnnualRevenue += bucket.revenue;
      } else {
        const mrrEntry = mrrSeries.find(s => s.month === monthKey);
        projectedAnnualRevenue += mrrEntry ? mrrEntry.mrr : currentMRR;
      }
    }
  }

  // Add paced current month (use order revenue pacing if available, else MRR)
  if (currentMonthPacing.revenuePaced > 0) {
    projectedAnnualRevenue += currentMonthPacing.revenuePaced;
  } else {
    projectedAnnualRevenue += currentMRR; // full month MRR estimate
  }

  // Project forward for remaining months with growth
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
    priorYearRevenue: Math.round(priorYearRevenue),
  };

  // --- Drop-in analytics ---
  const dropInWeekly = new Map<string, number>();
  const dropInMonthly = new Map<string, number>();

  for (const fv of firstVisits) {
    if (!isDropInOrIntro(fv.pass)) continue;
    const date = parseDate(fv.redeemedAt || "");
    if (!date) continue;
    const wk = getWeekKey(date);
    const mo = getMonthKey(date);
    dropInWeekly.set(wk, (dropInWeekly.get(wk) || 0) + 1);
    dropInMonthly.set(mo, (dropInMonthly.get(mo) || 0) + 1);
  }

  const currentMonthDropIns = dropInMonthly.get(currentMonthKey) || 0;
  const prevMonthKey = (() => {
    const d = new Date(now.getFullYear(), now.getMonth() - 1, 1);
    return getMonthKey(d);
  })();
  const previousMonthDropIns = dropInMonthly.get(prevMonthKey) || 0;

  // Rolling 6-week average (use completed weeks only)
  const allDropInWeekKeys = Array.from(dropInWeekly.keys()).sort();
  // Current week is in progress — exclude it
  const completedDropInWeeks = allDropInWeekKeys.filter((w) => w < getWeekKey(now));
  const last6Weeks = completedDropInWeeks.slice(-6);
  const dropInSum6w = last6Weeks.reduce((sum, wk) => sum + (dropInWeekly.get(wk) || 0), 0);
  const weeklyAvg6w = last6Weeks.length > 0 ? Math.round((dropInSum6w / last6Weeks.length) * 10) / 10 : 0;

  // Weekly breakdown (last 8 weeks including current)
  const recentDropInWeeks = allDropInWeekKeys.slice(-8);
  const weeklyBreakdown = recentDropInWeeks.map((wk) => ({
    week: wk,
    count: dropInWeekly.get(wk) || 0,
  }));

  const dropIns: DropInStats = {
    currentMonthTotal: currentMonthDropIns,
    currentMonthDaysElapsed: daysElapsed,
    currentMonthDaysInMonth: daysTotal,
    currentMonthPaced: Math.round(currentMonthDropIns * pacingMultiplier),
    previousMonthTotal: previousMonthDropIns,
    weeklyAvg6w,
    weeklyBreakdown,
  };

  // --- First Visit weekly stats (unique people, with segment breakdown) ---
  const emptySegments = (): Record<FirstVisitSegment, number> =>
    ({ introWeek: 0, dropIn: 0, guest: 0, other: 0 });

  const classifyPass = (pass: string): FirstVisitSegment => {
    const upper = pass.trim().toUpperCase();
    if (upper.includes("INTRO")) return "introWeek";
    if (upper.includes("GUEST")) return "guest";
    if (
      upper.includes("DROP-IN") || upper.includes("DROP IN") || upper.includes("DROPIN") ||
      upper.includes("SINGLE CLASS") || upper.includes("WELLHUB") ||
      upper.includes("5 PACK") || upper.includes("5-PACK") ||
      upper.includes("COMMUNITY DAY") || upper.includes("POKER CHIP") ||
      upper.includes("ON RUNNING")
    ) return "dropIn";
    return "other";
  };

  /** Normalize attendee name for dedup ("Last, First" → "first last") */
  const normalizeFvName = (name: string): string => {
    let n = name.trim().toLowerCase();
    if (n.includes(",")) {
      const parts = n.split(",").map((p) => p.trim());
      if (parts.length === 2 && parts[0] && parts[1]) n = parts[1] + " " + parts[0];
    }
    return n.replace(/\s+/g, " ");
  };

  // Per-week: Set of normalized names (unique people) + segment counts
  const fvWeekNames = new Map<string, Set<string>>();
  const fvWeekSegments = new Map<string, Record<FirstVisitSegment, number>>();
  // Cross-window: person → source (for aggregate breakdown, each person counted once)
  const fvPersonSource = new Map<string, FirstVisitSegment>();

  for (const fv of firstVisits) {
    const date = parseDate(fv.redeemedAt || "");
    if (!date) continue;
    const wk = getWeekKey(date);
    const seg = classifyPass(fv.pass);
    const name = normalizeFvName(fv.attendee || "");
    if (!name) continue;

    // Per-week dedup
    if (!fvWeekNames.has(wk)) fvWeekNames.set(wk, new Set());
    if (!fvWeekSegments.has(wk)) fvWeekSegments.set(wk, emptySegments());
    const nameSet = fvWeekNames.get(wk)!;
    if (!nameSet.has(name)) {
      nameSet.add(name);
      fvWeekSegments.get(wk)![seg]++;
    }

    // Cross-window: first occurrence sets the source (each person counted once)
    if (!fvPersonSource.has(name)) {
      fvPersonSource.set(name, seg);
    }
  }

  const currentWeekKey = getWeekKey(now);
  const allFvWeeks = Array.from(fvWeekNames.keys()).sort();
  const completedFvWeeks = allFvWeeks.filter((w) => w < currentWeekKey);
  const last4Completed = completedFvWeeks.slice(-4);

  // Aggregate: only count people whose weeks fall in the display window
  const displayWeeks = new Set([...last4Completed, currentWeekKey]);
  const fvAggregateSegments = emptySegments();
  const fvAggregateNames = new Set<string>();
  for (const wk of displayWeeks) {
    const names = fvWeekNames.get(wk);
    if (!names) continue;
    for (const name of names) {
      if (!fvAggregateNames.has(name)) {
        fvAggregateNames.add(name);
        const seg = fvPersonSource.get(name) || "other";
        fvAggregateSegments[seg]++;
      }
    }
  }

  const firstVisitStats: FirstVisitStats = {
    currentWeekTotal: fvWeekNames.get(currentWeekKey)?.size || 0,
    currentWeekSegments: fvWeekSegments.get(currentWeekKey) || emptySegments(),
    completedWeeks: last4Completed.map((wk) => ({
      week: wk,
      count: fvWeekNames.get(wk)?.size || 0,
      segments: fvWeekSegments.get(wk) || emptySegments(),
    })),
    aggregateSegments: fvAggregateSegments,
  };

  console.log(
    `[trends] Weekly: ${weekly.length} periods, Monthly: ${monthly.length} periods, ` +
    `Growth rate: ${annualProjection.monthlyGrowthRate}%, ` +
    `Projected annual: $${annualProjection.projectedAnnualRevenue}`
  );
  console.log(
    `[trends] Drop-ins: MTD=${dropIns.currentMonthTotal}, last month=${dropIns.previousMonthTotal}, ` +
    `6w avg=${dropIns.weeklyAvg6w}/wk, paced=${dropIns.currentMonthPaced}/mo`
  );
  console.log(
    `[trends] First visits: this week=${firstVisitStats.currentWeekTotal}, ` +
    `last 4 weeks=[${firstVisitStats.completedWeeks.map((w) => w.count).join(", ")}]`
  );

  return {
    weekly,
    monthly,
    currentMonthPacing,
    annualProjection,
    dropIns,
    firstVisitStats,
    returningNonMembers: null, // Set by pipeline worker after separate analysis
  };
}
