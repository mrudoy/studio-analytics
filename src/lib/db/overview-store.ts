/**
 * Overview store — computes time-window metrics for the Overview page.
 *
 * Returns data for 5 windows: Yesterday, This Week, Last Week, This Month, Last Month.
 * Each window has subscription changes, activity counts (including guests), and merch revenue.
 * Also returns current active subscriber counts by tier.
 */

import { getNewAutoRenews, getCanceledAutoRenews, getAutoRenewStats } from "./auto-renew-store";
import { getDropInCountForRange, getIntroWeekCountForRange, getGuestCountForRange } from "./registration-store";
import { getShopifyRevenueForRange } from "./shopify-store";
import type { OverviewData, TimeWindowMetrics, PlanChangeDetail } from "@/types/dashboard";
import type { StoredAutoRenew } from "./auto-renew-store";
import type { AutoRenewCategory } from "@/types/union-data";

// ── Date helpers (all dates computed in ET) ─────────────

const ET = "America/New_York";

/** Get today's date parts in ET timezone */
function nowInET(): { year: number; month: number; day: number; dayOfWeek: number } {
  const now = new Date();
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone: ET,
    year: "numeric",
    month: "numeric",
    day: "numeric",
    weekday: "short",
  }).formatToParts(now);
  const get = (type: string) => parts.find((p) => p.type === type)?.value ?? "";
  const dayMap: Record<string, number> = { Sun: 0, Mon: 1, Tue: 2, Wed: 3, Thu: 4, Fri: 5, Sat: 6 };
  return {
    year: Number(get("year")),
    month: Number(get("month")),
    day: Number(get("day")),
    dayOfWeek: dayMap[get("weekday")] ?? 0,
  };
}

function toISO(d: Date): string {
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}-${String(d.getDate()).padStart(2, "0")}`;
}

function getYesterday() {
  const { year, month, day } = nowInET();
  const today = new Date(year, month - 1, day);
  const yesterday = new Date(year, month - 1, day - 1);
  const sublabel = yesterday.toLocaleDateString("en-US", {
    timeZone: ET,
    weekday: "short",
    month: "short",
    day: "numeric",
  });
  return { start: toISO(yesterday), end: toISO(today), label: "Yesterday", sublabel };
}

function getThisWeek() {
  const { year, month, day, dayOfWeek } = nowInET();
  const isoDay = dayOfWeek === 0 ? 7 : dayOfWeek; // ISO: Mon=1, Sun=7
  const today = new Date(year, month - 1, day);
  const thisMonday = new Date(year, month - 1, day - isoDay + 1);
  const tomorrow = new Date(year, month - 1, day + 1);

  const fmt = (d: Date) => d.toLocaleDateString("en-US", { timeZone: ET, month: "short", day: "numeric" });
  return {
    start: toISO(thisMonday),
    end: toISO(tomorrow),
    label: "This Week",
    sublabel: `${fmt(thisMonday)} – ${fmt(today)}`,
  };
}

function getLastWeek() {
  const { year, month, day, dayOfWeek } = nowInET();
  const isoDay = dayOfWeek === 0 ? 7 : dayOfWeek; // ISO: Mon=1, Sun=7
  const thisMonday = new Date(year, month - 1, day - isoDay + 1);
  const lastMonday = new Date(thisMonday.getFullYear(), thisMonday.getMonth(), thisMonday.getDate() - 7);
  const lastSunday = new Date(lastMonday.getFullYear(), lastMonday.getMonth(), lastMonday.getDate() + 6);
  const endExclusive = new Date(lastSunday.getFullYear(), lastSunday.getMonth(), lastSunday.getDate() + 1);

  const fmt = (d: Date) => d.toLocaleDateString("en-US", { timeZone: ET, month: "short", day: "numeric" });
  return {
    start: toISO(lastMonday),
    end: toISO(endExclusive),
    label: "Last Week",
    sublabel: `${fmt(lastMonday)} – ${fmt(lastSunday)}`,
  };
}

function getThisMonth() {
  const { year, month, day } = nowInET();
  const start = new Date(year, month - 1, 1);
  const tomorrow = new Date(year, month - 1, day + 1);
  const monthName = start.toLocaleDateString("en-US", { timeZone: ET, month: "long" });
  return {
    start: toISO(start),
    end: toISO(tomorrow),
    label: "This Month",
    sublabel: `${monthName} 1 – ${day}`,
  };
}

function getLastMonth() {
  const { year, month } = nowInET();
  const start = new Date(year, month - 2, 1);
  const end = new Date(year, month - 1, 1);
  const lastDay = new Date(end.getFullYear(), end.getMonth(), end.getDate() - 1);
  const monthName = start.toLocaleDateString("en-US", { timeZone: ET, month: "long" });
  return {
    start: toISO(start),
    end: toISO(end),
    label: "Last Month",
    sublabel: `${monthName} 1 – ${lastDay.getDate()}`,
  };
}

// ── Tier ranking for upgrade / downgrade classification ──

const TIER_RANK: Record<string, number> = {
  SKY_TING_TV: 1,
  SKY3: 2,
  MEMBER: 3,
};

/**
 * Detect plan changes: emails that appear in BOTH the new and canceled arrays
 * with different categories in the same time window.
 *
 * When someone upgrades (e.g. Sky3 → Member), Union.fit cancels the old plan
 * and creates a new one. Without this filter those show as +1 new Member
 * AND -1 churned Sky3 — inflating both growth and churn.
 *
 * Returns the counts and the filtered arrays with plan-changers removed.
 */
type KnownCategory = "MEMBER" | "SKY3" | "SKY_TING_TV";

function detectPlanChanges(
  newAR: StoredAutoRenew[],
  canceledAR: StoredAutoRenew[],
): {
  filteredNew: StoredAutoRenew[];
  filteredCanceled: StoredAutoRenew[];
  planChanges: PlanChangeDetail[];
} {
  // Build email → category map from each list (dedup by email, take first match)
  const newByEmail = new Map<string, KnownCategory>();
  for (const r of newAR) {
    const email = r.customerEmail?.toLowerCase() || "";
    if (!email || r.category === "UNKNOWN") continue;
    if (!newByEmail.has(email)) newByEmail.set(email, r.category as KnownCategory);
  }

  const canceledByEmail = new Map<string, KnownCategory>();
  for (const r of canceledAR) {
    const email = r.customerEmail?.toLowerCase() || "";
    if (!email || r.category === "UNKNOWN") continue;
    if (!canceledByEmail.has(email)) canceledByEmail.set(email, r.category as KnownCategory);
  }

  // Find emails in BOTH lists with DIFFERENT categories.
  // Group by (from → to) pair so we can report "3 people: Sky3 → Member".
  const planChangeEmails = new Set<string>();
  const moveCounts = new Map<string, { from: KnownCategory; to: KnownCategory; count: number }>();

  for (const [email, newCat] of newByEmail) {
    const oldCat = canceledByEmail.get(email);
    if (!oldCat || oldCat === newCat) continue;

    const oldRank = TIER_RANK[oldCat] ?? 0;
    const newRank = TIER_RANK[newCat] ?? 0;
    if (oldRank === 0 || newRank === 0) continue;

    planChangeEmails.add(email);
    const key = `${oldCat}→${newCat}`;
    const existing = moveCounts.get(key);
    if (existing) {
      existing.count++;
    } else {
      moveCounts.set(key, { from: oldCat, to: newCat, count: 1 });
    }
  }

  if (planChangeEmails.size === 0) {
    return { filteredNew: newAR, filteredCanceled: canceledAR, planChanges: [] };
  }

  // Build detailed plan change list
  const planChanges: PlanChangeDetail[] = [];
  for (const move of moveCounts.values()) {
    const direction = (TIER_RANK[move.to] ?? 0) > (TIER_RANK[move.from] ?? 0) ? "upgrade" : "downgrade";
    planChanges.push({ from: move.from, to: move.to, direction, count: move.count });
  }
  // Sort: upgrades first, then by count descending
  planChanges.sort((a, b) => {
    if (a.direction !== b.direction) return a.direction === "upgrade" ? -1 : 1;
    return b.count - a.count;
  });

  // Filter out plan-change emails from both arrays
  const filteredNew = newAR.filter(
    (r) => !planChangeEmails.has(r.customerEmail?.toLowerCase() || ""),
  );
  const filteredCanceled = canceledAR.filter(
    (r) => !planChangeEmails.has(r.customerEmail?.toLowerCase() || ""),
  );

  return { filteredNew, filteredCanceled, planChanges };
}

// ── Aggregation ──────────────────────────────────────────

function countByCategory(rows: StoredAutoRenew[]): Record<"MEMBER" | "SKY3" | "SKY_TING_TV", number> {
  // Deduplicate by email per category — same person with multiple subscription rows counts once
  const seen = { MEMBER: new Set<string>(), SKY3: new Set<string>(), SKY_TING_TV: new Set<string>() };
  for (const r of rows) {
    const email = r.customerEmail?.toLowerCase() || "";
    if (!email) continue;
    if (r.category === "MEMBER") seen.MEMBER.add(email);
    else if (r.category === "SKY3") seen.SKY3.add(email);
    else if (r.category === "SKY_TING_TV") seen.SKY_TING_TV.add(email);
  }
  return { MEMBER: seen.MEMBER.size, SKY3: seen.SKY3.size, SKY_TING_TV: seen.SKY_TING_TV.size };
}

async function computeWindow(
  start: string,
  end: string,
  label: string,
  sublabel: string,
): Promise<TimeWindowMetrics> {
  const [newAR, canceledAR, dropIns, introWeeks, guests, merchRevenue] = await Promise.all([
    getNewAutoRenews(start, end),
    getCanceledAutoRenews(start, end),
    getDropInCountForRange(start, end),
    getIntroWeekCountForRange(start, end),
    getGuestCountForRange(start, end),
    getShopifyRevenueForRange(start, end).catch(() => 0),
  ]);

  // Detect cross-category plan changes (upgrades/downgrades) and remove them
  // from the new/canceled arrays so category rows show only organic movement.
  const { filteredNew, filteredCanceled, planChanges } =
    detectPlanChanges(newAR, canceledAR);

  const newCounts = countByCategory(filteredNew);
  const canceledCounts = countByCategory(filteredCanceled);

  return {
    label,
    sublabel,
    startDate: start,
    endDate: end,
    subscriptions: {
      member:    { new: newCounts.MEMBER,      churned: canceledCounts.MEMBER },
      sky3:      { new: newCounts.SKY3,        churned: canceledCounts.SKY3 },
      skyTingTv: { new: newCounts.SKY_TING_TV, churned: canceledCounts.SKY_TING_TV },
      planChanges,
    },
    activity: {
      dropIns,
      introWeeks,
      guests,
    },
    revenue: {
      merch: Math.round(merchRevenue * 100) / 100,
    },
  };
}

// ── Public API ───────────────────────────────────────────

export async function getOverviewData(): Promise<OverviewData> {
  const y = getYesterday();
  const tw = getThisWeek();
  const lw = getLastWeek();
  const tm = getThisMonth();
  const lm = getLastMonth();

  const [yesterday, thisWeek, lastWeek, thisMonth, lastMonth, arStats] = await Promise.all([
    computeWindow(y.start, y.end, y.label, y.sublabel),
    computeWindow(tw.start, tw.end, tw.label, tw.sublabel),
    computeWindow(lw.start, lw.end, lw.label, lw.sublabel),
    computeWindow(tm.start, tm.end, tm.label, tm.sublabel),
    computeWindow(lm.start, lm.end, lm.label, lm.sublabel),
    getAutoRenewStats(),
  ]);

  return {
    yesterday,
    thisWeek,
    lastWeek,
    thisMonth,
    lastMonth,
    currentActive: {
      member: arStats?.active.member ?? 0,
      sky3: arStats?.active.sky3 ?? 0,
      skyTingTv: arStats?.active.skyTingTv ?? 0,
    },
  };
}
