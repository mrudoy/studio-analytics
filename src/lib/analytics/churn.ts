import { differenceInDays, subDays } from "date-fns";
import type { AutoRenew } from "@/types/union-data";
import { getCategory } from "./categories";
import { parseDate, getWeekKey, getMonthKey } from "./date-utils";

export interface ChurnRow {
  period: string;
  periodType: "weekly" | "monthly";
  sky3ChurnRate: number;
  memberChurnRate: number;
  sky3AvgDuration: number;
  memberAvgDuration: number;
  sky3RevenueLost: number;
  memberRevenueLost: number;
  sky3Cancellations: number;
  memberCancellations: number;
  sky3ActiveStart: number;
  memberActiveStart: number;
}

export interface ChurnResults {
  weekly: ChurnRow[];
  monthly: ChurnRow[];
  rolling30: ChurnRow;
  rolling60: ChurnRow;
  rolling90: ChurnRow;
}

interface PeriodBucket {
  sky3Cancellations: number;
  memberCancellations: number;
  sky3RevenueLost: number;
  memberRevenueLost: number;
  sky3Durations: number[];
  memberDurations: number[];
}

export function analyzeChurn(
  canceledAutoRenews: AutoRenew[],
  activeAutoRenews: AutoRenew[],
  newAutoRenews: AutoRenew[]
): ChurnResults {
  // Count active members at a rough level (for rate calculation)
  let sky3ActiveTotal = 0;
  let memberActiveTotal = 0;
  for (const ar of activeAutoRenews) {
    const cat = getCategory(ar.name);
    if (cat === "SKY3") sky3ActiveTotal++;
    if (cat === "MEMBER") memberActiveTotal++;
  }

  // Group cancellations by period
  const weeklyBuckets = new Map<string, PeriodBucket>();
  const monthlyBuckets = new Map<string, PeriodBucket>();

  function getOrCreate(map: Map<string, PeriodBucket>, key: string): PeriodBucket {
    if (!map.has(key))
      map.set(key, {
        sky3Cancellations: 0,
        memberCancellations: 0,
        sky3RevenueLost: 0,
        memberRevenueLost: 0,
        sky3Durations: [],
        memberDurations: [],
      });
    return map.get(key)!;
  }

  for (const ar of canceledAutoRenews) {
    const cancelDate = parseDate(ar.canceledAt || "");
    if (!cancelDate) continue;

    const category = getCategory(ar.name);
    if (category !== "SKY3" && category !== "MEMBER") continue;

    const wk = getWeekKey(cancelDate);
    const mo = getMonthKey(cancelDate);
    const wBucket = getOrCreate(weeklyBuckets, wk);
    const mBucket = getOrCreate(monthlyBuckets, mo);

    // Estimate duration: if we have a createdAt, compute days
    const createdDate = parseDate(ar.created || "");
    const duration = createdDate ? differenceInDays(cancelDate, createdDate) : 0;

    if (category === "SKY3") {
      wBucket.sky3Cancellations++;
      mBucket.sky3Cancellations++;
      wBucket.sky3RevenueLost += ar.price;
      mBucket.sky3RevenueLost += ar.price;
      if (duration > 0) {
        wBucket.sky3Durations.push(duration);
        mBucket.sky3Durations.push(duration);
      }
    } else {
      wBucket.memberCancellations++;
      mBucket.memberCancellations++;
      wBucket.memberRevenueLost += ar.price;
      mBucket.memberRevenueLost += ar.price;
      if (duration > 0) {
        wBucket.memberDurations.push(duration);
        mBucket.memberDurations.push(duration);
      }
    }
  }

  function buildRows(
    buckets: Map<string, PeriodBucket>,
    periodType: "weekly" | "monthly"
  ): ChurnRow[] {
    const avg = (arr: number[]) =>
      arr.length > 0 ? Math.round(arr.reduce((a, b) => a + b, 0) / arr.length) : 0;

    // For weekly churn rate, divide by total active (rough approximation)
    // A more accurate approach would track active count per period
    const periodCount = buckets.size || 1;

    return Array.from(buckets.entries())
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([period, b]) => ({
        period,
        periodType,
        sky3ChurnRate:
          sky3ActiveTotal > 0 ? (b.sky3Cancellations / sky3ActiveTotal) * 100 : 0,
        memberChurnRate:
          memberActiveTotal > 0 ? (b.memberCancellations / memberActiveTotal) * 100 : 0,
        sky3AvgDuration: avg(b.sky3Durations),
        memberAvgDuration: avg(b.memberDurations),
        sky3RevenueLost: b.sky3RevenueLost,
        memberRevenueLost: b.memberRevenueLost,
        sky3Cancellations: b.sky3Cancellations,
        memberCancellations: b.memberCancellations,
        sky3ActiveStart: sky3ActiveTotal,
        memberActiveStart: memberActiveTotal,
      }));
  }

  const weekly = buildRows(weeklyBuckets, "weekly");
  const monthly = buildRows(monthlyBuckets, "monthly");

  function computeRolling(days: number): ChurnRow {
    const cutoff = subDays(new Date(), days);
    let sky3Cancel = 0, memberCancel = 0;
    let sky3RevLost = 0, memberRevLost = 0;
    const sky3Durs: number[] = [], memberDurs: number[] = [];

    for (const ar of canceledAutoRenews) {
      const cancelDate = parseDate(ar.canceledAt || "");
      if (!cancelDate || cancelDate < cutoff) continue;
      const category = getCategory(ar.name);
      const createdDate = parseDate(ar.created || "");
      const duration = createdDate ? differenceInDays(cancelDate, createdDate) : 0;

      if (category === "SKY3") {
        sky3Cancel++;
        sky3RevLost += ar.price;
        if (duration > 0) sky3Durs.push(duration);
      } else if (category === "MEMBER") {
        memberCancel++;
        memberRevLost += ar.price;
        if (duration > 0) memberDurs.push(duration);
      }
    }

    const avg = (arr: number[]) =>
      arr.length > 0 ? Math.round(arr.reduce((a, b) => a + b, 0) / arr.length) : 0;

    return {
      period: `Rolling ${days}d`,
      periodType: "weekly",
      sky3ChurnRate: sky3ActiveTotal > 0 ? (sky3Cancel / sky3ActiveTotal) * 100 : 0,
      memberChurnRate: memberActiveTotal > 0 ? (memberCancel / memberActiveTotal) * 100 : 0,
      sky3AvgDuration: avg(sky3Durs),
      memberAvgDuration: avg(memberDurs),
      sky3RevenueLost: sky3RevLost,
      memberRevenueLost: memberRevLost,
      sky3Cancellations: sky3Cancel,
      memberCancellations: memberCancel,
      sky3ActiveStart: sky3ActiveTotal,
      memberActiveStart: memberActiveTotal,
    };
  }

  return {
    weekly,
    monthly,
    rolling30: computeRolling(30),
    rolling60: computeRolling(60),
    rolling90: computeRolling(90),
  };
}
