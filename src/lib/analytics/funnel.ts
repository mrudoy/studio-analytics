import {
  differenceInDays,
  subDays,
} from "date-fns";
import type { FirstVisit, AutoRenew, NewCustomer } from "@/types/union-data";
import { getCategory, isDropInOrIntro } from "./categories";
import { parseDate, getWeekKey, getMonthKey } from "./date-utils";

export interface FunnelRow {
  period: string; // "2026-W03" or "2026-01"
  periodType: "weekly" | "monthly";
  // Drop-in/Intro → SKY3
  introToSky3Count: number;
  introToSky3Rate: number;
  introToSky3AvgDays: number;
  // Drop-in/Intro → Member
  introToMemberCount: number;
  introToMemberRate: number;
  introToMemberAvgDays: number;
  // SKY3 → Member
  sky3ToMemberCount: number;
  sky3ToMemberRate: number;
  sky3ToMemberAvgDays: number;
}

export interface FunnelResults {
  weekly: FunnelRow[];
  monthly: FunnelRow[];
  rolling30: FunnelRow;
  rolling60: FunnelRow;
  rolling90: FunnelRow;
}

/**
 * Build a map of customer name -> earliest known dates for different stages.
 */
function buildCustomerTimeline(
  firstVisits: FirstVisit[],
  newAutoRenews: AutoRenew[],
  newCustomers: NewCustomer[]
) {
  const timeline = new Map<
    string,
    {
      firstVisitDate?: Date;
      firstVisitPass?: string;
      sky3Date?: Date;
      memberDate?: Date;
      signUpDate?: Date;
    }
  >();

  /**
   * Normalize a name to a consistent key for matching across reports.
   * First Visits use "Last, First" format; other reports use "First Last".
   * This converts both to "first last" lowercase.
   */
  function normalizeName(name: string): string {
    let n = name.trim().toLowerCase();
    // If the name contains a comma, assume "Last, First" format
    if (n.includes(",")) {
      const parts = n.split(",").map((p) => p.trim());
      if (parts.length === 2 && parts[0] && parts[1]) {
        n = parts[1] + " " + parts[0]; // "first last"
      }
    }
    return n.replace(/\s+/g, " ");
  }

  function getOrCreate(name: string) {
    const key = normalizeName(name);
    if (!timeline.has(key)) timeline.set(key, {});
    return timeline.get(key)!;
  }

  // Map first visits
  for (const fv of firstVisits) {
    if (!fv.attendee || !isDropInOrIntro(fv.pass)) continue;
    const date = parseDate(fv.redeemedAt);
    if (!date) continue;
    const entry = getOrCreate(fv.attendee);
    if (!entry.firstVisitDate || date < entry.firstVisitDate) {
      entry.firstVisitDate = date;
      entry.firstVisitPass = fv.pass;
    }
  }

  // Map new auto-renews to SKY3 or MEMBER start dates
  for (const ar of newAutoRenews) {
    if (!ar.customer) continue;
    const category = getCategory(ar.name);
    const date = parseDate(ar.created || "");
    if (!date) continue;
    const entry = getOrCreate(ar.customer);

    if (category === "SKY3" && (!entry.sky3Date || date < entry.sky3Date)) {
      entry.sky3Date = date;
    }
    if (category === "MEMBER" && (!entry.memberDate || date < entry.memberDate)) {
      entry.memberDate = date;
    }
  }

  // Map sign-up dates
  for (const nc of newCustomers) {
    if (!nc.name) continue;
    const date = parseDate(nc.created);
    if (!date) continue;
    const entry = getOrCreate(nc.name);
    if (!entry.signUpDate || date < entry.signUpDate) {
      entry.signUpDate = date;
    }
  }

  return timeline;
}

/**
 * Analyze conversion funnels.
 */
export function analyzeFunnel(
  firstVisits: FirstVisit[],
  newAutoRenews: AutoRenew[],
  newCustomers: NewCustomer[]
): FunnelResults {
  const timeline = buildCustomerTimeline(firstVisits, newAutoRenews, newCustomers);

  // Group conversions by period
  const weeklyData = new Map<string, { introToSky3: number[]; introToMember: number[]; sky3ToMember: number[]; introTotal: number; sky3Total: number }>();
  const monthlyData = new Map<string, { introToSky3: number[]; introToMember: number[]; sky3ToMember: number[]; introTotal: number; sky3Total: number }>();

  function getOrCreatePeriod(map: Map<string, { introToSky3: number[]; introToMember: number[]; sky3ToMember: number[]; introTotal: number; sky3Total: number }>, key: string) {
    if (!map.has(key)) map.set(key, { introToSky3: [], introToMember: [], sky3ToMember: [], introTotal: 0, sky3Total: 0 });
    return map.get(key)!;
  }

  for (const [, entry] of timeline) {
    // Intro → SKY3
    if (entry.firstVisitDate && entry.sky3Date && entry.sky3Date >= entry.firstVisitDate) {
      const days = differenceInDays(entry.sky3Date, entry.firstVisitDate);
      const wk = getWeekKey(entry.firstVisitDate);
      const mo = getMonthKey(entry.firstVisitDate);
      getOrCreatePeriod(weeklyData, wk).introToSky3.push(days);
      getOrCreatePeriod(monthlyData, mo).introToSky3.push(days);
    }

    // Intro → Member
    if (entry.firstVisitDate && entry.memberDate && entry.memberDate >= entry.firstVisitDate) {
      const days = differenceInDays(entry.memberDate, entry.firstVisitDate);
      const wk = getWeekKey(entry.firstVisitDate);
      const mo = getMonthKey(entry.firstVisitDate);
      getOrCreatePeriod(weeklyData, wk).introToMember.push(days);
      getOrCreatePeriod(monthlyData, mo).introToMember.push(days);
    }

    // SKY3 → Member
    if (entry.sky3Date && entry.memberDate && entry.memberDate > entry.sky3Date) {
      const days = differenceInDays(entry.memberDate, entry.sky3Date);
      const wk = getWeekKey(entry.sky3Date);
      const mo = getMonthKey(entry.sky3Date);
      getOrCreatePeriod(weeklyData, wk).sky3ToMember.push(days);
      getOrCreatePeriod(monthlyData, mo).sky3ToMember.push(days);
    }

    // Count totals for rate calculation
    if (entry.firstVisitDate) {
      const wk = getWeekKey(entry.firstVisitDate);
      const mo = getMonthKey(entry.firstVisitDate);
      getOrCreatePeriod(weeklyData, wk).introTotal++;
      getOrCreatePeriod(monthlyData, mo).introTotal++;
    }

    if (entry.sky3Date) {
      const wk = getWeekKey(entry.sky3Date);
      const mo = getMonthKey(entry.sky3Date);
      getOrCreatePeriod(weeklyData, wk).sky3Total++;
      getOrCreatePeriod(monthlyData, mo).sky3Total++;
    }
  }

  function buildRows(data: Map<string, { introToSky3: number[]; introToMember: number[]; sky3ToMember: number[]; introTotal: number; sky3Total: number }>, periodType: "weekly" | "monthly"): FunnelRow[] {
    const avg = (arr: number[]) => (arr.length > 0 ? arr.reduce((a, b) => a + b, 0) / arr.length : 0);

    return Array.from(data.entries())
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([period, d]) => ({
        period,
        periodType,
        introToSky3Count: d.introToSky3.length,
        introToSky3Rate: d.introTotal > 0 ? (d.introToSky3.length / d.introTotal) * 100 : 0,
        introToSky3AvgDays: Math.round(avg(d.introToSky3)),
        introToMemberCount: d.introToMember.length,
        introToMemberRate: d.introTotal > 0 ? (d.introToMember.length / d.introTotal) * 100 : 0,
        introToMemberAvgDays: Math.round(avg(d.introToMember)),
        sky3ToMemberCount: d.sky3ToMember.length,
        sky3ToMemberRate: d.sky3Total > 0 ? (d.sky3ToMember.length / d.sky3Total) * 100 : 0,
        sky3ToMemberAvgDays: Math.round(avg(d.sky3ToMember)),
      }));
  }

  const weekly = buildRows(weeklyData, "weekly");
  const monthly = buildRows(monthlyData, "monthly");

  // Rolling averages
  function computeRolling(days: number): FunnelRow {
    const cutoff = subDays(new Date(), days);
    const relevantEntries = Array.from(timeline.values());

    let introTotal = 0;
    const introToSky3Days: number[] = [];
    const introToMemberDays: number[] = [];
    let sky3Total = 0;
    const sky3ToMemberDays: number[] = [];

    for (const entry of relevantEntries) {
      if (entry.firstVisitDate && entry.firstVisitDate >= cutoff) {
        introTotal++;
        if (entry.sky3Date && entry.sky3Date >= entry.firstVisitDate) {
          introToSky3Days.push(differenceInDays(entry.sky3Date, entry.firstVisitDate));
        }
        if (entry.memberDate && entry.memberDate >= entry.firstVisitDate) {
          introToMemberDays.push(differenceInDays(entry.memberDate, entry.firstVisitDate));
        }
      }
      if (entry.sky3Date && entry.sky3Date >= cutoff) {
        sky3Total++;
        if (entry.memberDate && entry.memberDate > entry.sky3Date) {
          sky3ToMemberDays.push(differenceInDays(entry.memberDate, entry.sky3Date));
        }
      }
    }

    const avg = (arr: number[]) => (arr.length > 0 ? arr.reduce((a, b) => a + b, 0) / arr.length : 0);

    return {
      period: `Rolling ${days}d`,
      periodType: "weekly",
      introToSky3Count: introToSky3Days.length,
      introToSky3Rate: introTotal > 0 ? (introToSky3Days.length / introTotal) * 100 : 0,
      introToSky3AvgDays: Math.round(avg(introToSky3Days)),
      introToMemberCount: introToMemberDays.length,
      introToMemberRate: introTotal > 0 ? (introToMemberDays.length / introTotal) * 100 : 0,
      introToMemberAvgDays: Math.round(avg(introToMemberDays)),
      sky3ToMemberCount: sky3ToMemberDays.length,
      sky3ToMemberRate: sky3Total > 0 ? (sky3ToMemberDays.length / sky3Total) * 100 : 0,
      sky3ToMemberAvgDays: Math.round(avg(sky3ToMemberDays)),
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
