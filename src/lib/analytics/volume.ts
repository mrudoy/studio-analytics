import { subDays } from "date-fns";
import type { AutoRenew, NewCustomer } from "@/types/union-data";
import { getCategory } from "./categories";
import { parseDate, getWeekKey } from "./date-utils";

export interface WeeklyVolumeRow {
  week: string;
  newSky3: number;
  newMember: number;
  totalNewAutoRenews: number;
  netNewCustomers: number;
  sky3Cancellations: number;
  memberCancellations: number;
  netSky3Growth: number;
  netMemberGrowth: number;
}

export interface VolumeResults {
  weekly: WeeklyVolumeRow[];
  rolling30: WeeklyVolumeRow;
  rolling60: WeeklyVolumeRow;
  rolling90: WeeklyVolumeRow;
}

interface WeekBucket {
  newSky3: number;
  newMember: number;
  sky3Cancellations: number;
  memberCancellations: number;
  netNewCustomers: number;
}

export function analyzeVolume(
  newAutoRenews: AutoRenew[],
  canceledAutoRenews: AutoRenew[],
  newCustomers: NewCustomer[]
): VolumeResults {
  const buckets = new Map<string, WeekBucket>();

  function getOrCreate(key: string): WeekBucket {
    if (!buckets.has(key))
      buckets.set(key, {
        newSky3: 0,
        newMember: 0,
        sky3Cancellations: 0,
        memberCancellations: 0,
        netNewCustomers: 0,
      });
    return buckets.get(key)!;
  }

  // New auto-renews
  for (const ar of newAutoRenews) {
    const date = parseDate(ar.created || "");
    if (!date) continue;
    const wk = getWeekKey(date);
    const cat = getCategory(ar.name);
    const bucket = getOrCreate(wk);
    if (cat === "SKY3") bucket.newSky3++;
    if (cat === "MEMBER") bucket.newMember++;
  }

  // Cancellations
  for (const ar of canceledAutoRenews) {
    const date = parseDate(ar.canceledAt || "");
    if (!date) continue;
    const wk = getWeekKey(date);
    const cat = getCategory(ar.name);
    const bucket = getOrCreate(wk);
    if (cat === "SKY3") bucket.sky3Cancellations++;
    if (cat === "MEMBER") bucket.memberCancellations++;
  }

  // Net new customers
  for (const nc of newCustomers) {
    const date = parseDate(nc.created);
    if (!date) continue;
    const wk = getWeekKey(date);
    getOrCreate(wk).netNewCustomers++;
  }

  const weekly: WeeklyVolumeRow[] = Array.from(buckets.entries())
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([week, b]) => ({
      week,
      newSky3: b.newSky3,
      newMember: b.newMember,
      totalNewAutoRenews: b.newSky3 + b.newMember,
      netNewCustomers: b.netNewCustomers,
      sky3Cancellations: b.sky3Cancellations,
      memberCancellations: b.memberCancellations,
      netSky3Growth: b.newSky3 - b.sky3Cancellations,
      netMemberGrowth: b.newMember - b.memberCancellations,
    }));

  function computeRolling(days: number): WeeklyVolumeRow {
    const cutoff = subDays(new Date(), days);
    let newSky3 = 0, newMember = 0, sky3Cancel = 0, memberCancel = 0, netNew = 0;

    for (const ar of newAutoRenews) {
      const date = parseDate(ar.created || "");
      if (!date || date < cutoff) continue;
      const cat = getCategory(ar.name);
      if (cat === "SKY3") newSky3++;
      if (cat === "MEMBER") newMember++;
    }

    for (const ar of canceledAutoRenews) {
      const date = parseDate(ar.canceledAt || "");
      if (!date || date < cutoff) continue;
      const cat = getCategory(ar.name);
      if (cat === "SKY3") sky3Cancel++;
      if (cat === "MEMBER") memberCancel++;
    }

    for (const nc of newCustomers) {
      const date = parseDate(nc.created);
      if (!date || date < cutoff) continue;
      netNew++;
    }

    // Normalize to weekly average
    const weeks = days / 7;
    return {
      week: `Rolling ${days}d (avg/wk)`,
      newSky3: Math.round(newSky3 / weeks),
      newMember: Math.round(newMember / weeks),
      totalNewAutoRenews: Math.round((newSky3 + newMember) / weeks),
      netNewCustomers: Math.round(netNew / weeks),
      sky3Cancellations: Math.round(sky3Cancel / weeks),
      memberCancellations: Math.round(memberCancel / weeks),
      netSky3Growth: Math.round((newSky3 - sky3Cancel) / weeks),
      netMemberGrowth: Math.round((newMember - memberCancel) / weeks),
    };
  }

  return {
    weekly,
    rolling30: computeRolling(30),
    rolling60: computeRolling(60),
    rolling90: computeRolling(90),
  };
}
