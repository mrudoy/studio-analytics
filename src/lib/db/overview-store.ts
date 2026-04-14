/**
 * Overview store — computes time-window metrics for the Overview page.
 *
 * Returns data for 5 windows: Yesterday, This Week, Last Week, This Month, Last Month.
 * Each window has:
 *   - subscription changes + planChanges (from getSubscriberMovement — canonical)
 *   - activity counts (drop-ins, intro weeks, guests from registrations table)
 *   - merch revenue (from Shopify)
 *
 * The subscription portion is overridden from getSubscriberMovement() so the
 * Overview table's numbers always match the monthly/weekly churn cards and
 * every other consumer of subscriber counts.
 */

import { getAutoRenewStats } from "./auto-renew-store";
import { getDropInCountForRange, getIntroWeekCountForRange, getGuestCountForRange } from "./registration-store";
import { getShopifyRevenueForRange } from "./shopify-store";
import type { OverviewData, TimeWindowMetrics } from "@/types/dashboard";

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

  const fmt = (d: Date) => d.toLocaleDateString("en-US", { month: "short", day: "numeric" });
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

  const fmt = (d: Date) => d.toLocaleDateString("en-US", { month: "short", day: "numeric" });
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
  const monthName = start.toLocaleDateString("en-US", { month: "long" });
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
  const monthName = start.toLocaleDateString("en-US", { month: "long" });
  return {
    start: toISO(start),
    end: toISO(end),
    label: "Last Month",
    sublabel: `${monthName} 1 – ${lastDay.getDate()}`,
  };
}

async function computeWindow(
  start: string,
  end: string,
  label: string,
  sublabel: string,
): Promise<TimeWindowMetrics> {
  // Non-subscription fields only — subscription counts + planChanges are
  // overridden in getOverviewData() from getSubscriberMovement (canonical).
  const [dropIns, introWeeks, guests, merchRevenue] = await Promise.all([
    getDropInCountForRange(start, end),
    getIntroWeekCountForRange(start, end),
    getGuestCountForRange(start, end),
    getShopifyRevenueForRange(start, end).catch(() => 0),
  ]);

  return {
    label,
    sublabel,
    startDate: start,
    endDate: end,
    subscriptions: {
      member:    { new: 0, churned: 0 },  // overridden in getOverviewData
      sky3:      { new: 0, churned: 0 },  // overridden in getOverviewData
      skyTingTv: { new: 0, churned: 0 },  // overridden in getOverviewData
      planChanges: [],                    // overridden in getOverviewData
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

  // Fetch subscriber movement from the canonical source. We overwrite the
  // subscription counts in each window with these values so both the API
  // and the digest email path get the same numbers as the Overview table.
  // getSubscriberMovement uses STILL_PAYING_STATES exclusion and plan-changer
  // detection; its counts match the monthly churn card and weekly trends.
  const { getSubscriberMovement } = await import("../analytics/metrics/subscriber-movement");

  const [yesterday, thisWeek, lastWeek, thisMonth, lastMonth, arStats, movement] = await Promise.all([
    computeWindow(y.start, y.end, y.label, y.sublabel),
    computeWindow(tw.start, tw.end, tw.label, tw.sublabel),
    computeWindow(lw.start, lw.end, lw.label, lw.sublabel),
    computeWindow(tm.start, tm.end, tm.label, tm.sublabel),
    computeWindow(lm.start, lm.end, lm.label, lm.sublabel),
    getAutoRenewStats(),
    getSubscriberMovement(),
  ]);

  // Apply canonical subscription counts + planChanges to each window
  const catFromMovement = (m: { new: number; canceled: number }) => ({ new: m.new, churned: m.canceled });
  const overrideSubs = (win: TimeWindowMetrics, mv: typeof movement.byWindow["yesterday"]): TimeWindowMetrics => ({
    ...win,
    subscriptions: {
      member: catFromMovement(mv.member),
      sky3: catFromMovement(mv.sky3),
      skyTingTv: catFromMovement(mv.skyTingTv),
      planChanges: mv.planChanges,
    },
  });

  return {
    yesterday: overrideSubs(yesterday, movement.byWindow.yesterday),
    thisWeek: overrideSubs(thisWeek, movement.byWindow.thisWeek),
    lastWeek: overrideSubs(lastWeek, movement.byWindow.lastWeek),
    thisMonth: overrideSubs(thisMonth, movement.byWindow.thisMonth),
    lastMonth: overrideSubs(lastMonth, movement.byWindow.lastMonth),
    currentActive: {
      member: arStats?.active.member ?? 0,
      sky3: arStats?.active.sky3 ?? 0,
      skyTingTv: arStats?.active.skyTingTv ?? 0,
    },
  };
}
