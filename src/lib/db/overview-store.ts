/**
 * Overview store — computes time-window metrics for the Overview page.
 *
 * Returns data for 4 windows: Yesterday, Last Week, This Month, Last Month.
 * Each window has subscription changes, activity counts, and merch revenue.
 */

import { getNewAutoRenews, getCanceledAutoRenews } from "./auto-renew-store";
import { getDropInCountForRange, getIntroWeekCountForRange } from "./registration-store";
import { getShopifyRevenueForRange } from "./shopify-store";
import type { OverviewData, TimeWindowMetrics } from "@/types/dashboard";
import type { StoredAutoRenew } from "./auto-renew-store";

// ── Date helpers ─────────────────────────────────────────

function toISO(d: Date): string {
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}-${String(d.getDate()).padStart(2, "0")}`;
}

function getYesterday() {
  const now = new Date();
  const yesterday = new Date(now.getFullYear(), now.getMonth(), now.getDate() - 1);
  const today = new Date(now.getFullYear(), now.getMonth(), now.getDate());
  const sublabel = yesterday.toLocaleDateString("en-US", {
    weekday: "short",
    month: "short",
    day: "numeric",
  });
  return { start: toISO(yesterday), end: toISO(today), label: "Yesterday", sublabel };
}

function getLastWeek() {
  const now = new Date();
  const dayOfWeek = now.getDay() === 0 ? 7 : now.getDay(); // ISO: Mon=1, Sun=7
  const thisMonday = new Date(now.getFullYear(), now.getMonth(), now.getDate() - dayOfWeek + 1);
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
  const now = new Date();
  const start = new Date(now.getFullYear(), now.getMonth(), 1);
  const tomorrow = new Date(now.getFullYear(), now.getMonth(), now.getDate() + 1);
  const monthName = now.toLocaleDateString("en-US", { month: "long" });
  return {
    start: toISO(start),
    end: toISO(tomorrow),
    label: "This Month",
    sublabel: `${monthName} 1 – ${now.getDate()}`,
  };
}

function getLastMonth() {
  const now = new Date();
  const start = new Date(now.getFullYear(), now.getMonth() - 1, 1);
  const end = new Date(now.getFullYear(), now.getMonth(), 1);
  const lastDay = new Date(end.getFullYear(), end.getMonth(), end.getDate() - 1);
  const monthName = start.toLocaleDateString("en-US", { month: "long" });
  return {
    start: toISO(start),
    end: toISO(end),
    label: "Last Month",
    sublabel: `${monthName} 1 – ${lastDay.getDate()}`,
  };
}

// ── Aggregation ──────────────────────────────────────────

function countByCategory(rows: StoredAutoRenew[]): Record<"MEMBER" | "SKY3" | "SKY_TING_TV", number> {
  const counts = { MEMBER: 0, SKY3: 0, SKY_TING_TV: 0 };
  for (const r of rows) {
    if (r.category === "MEMBER") counts.MEMBER++;
    else if (r.category === "SKY3") counts.SKY3++;
    else if (r.category === "SKY_TING_TV") counts.SKY_TING_TV++;
  }
  return counts;
}

async function computeWindow(
  start: string,
  end: string,
  label: string,
  sublabel: string,
): Promise<TimeWindowMetrics> {
  const [newAR, canceledAR, dropIns, introWeeks, merchRevenue] = await Promise.all([
    getNewAutoRenews(start, end),
    getCanceledAutoRenews(start, end),
    getDropInCountForRange(start, end),
    getIntroWeekCountForRange(start, end),
    getShopifyRevenueForRange(start, end).catch(() => 0),
  ]);

  const newCounts = countByCategory(newAR);
  const canceledCounts = countByCategory(canceledAR);

  return {
    label,
    sublabel,
    startDate: start,
    endDate: end,
    subscriptions: {
      member:    { new: newCounts.MEMBER,      churned: canceledCounts.MEMBER },
      sky3:      { new: newCounts.SKY3,        churned: canceledCounts.SKY3 },
      skyTingTv: { new: newCounts.SKY_TING_TV, churned: canceledCounts.SKY_TING_TV },
    },
    activity: {
      dropIns,
      introWeeks,
    },
    revenue: {
      merch: Math.round(merchRevenue * 100) / 100,
    },
  };
}

// ── Public API ───────────────────────────────────────────

export async function getOverviewData(): Promise<OverviewData> {
  const y = getYesterday();
  const lw = getLastWeek();
  const tm = getThisMonth();
  const lm = getLastMonth();

  const [yesterday, lastWeek, thisMonth, lastMonth] = await Promise.all([
    computeWindow(y.start, y.end, y.label, y.sublabel),
    computeWindow(lw.start, lw.end, lw.label, lw.sublabel),
    computeWindow(tm.start, tm.end, tm.label, tm.sublabel),
    computeWindow(lm.start, lm.end, lm.label, lm.sublabel),
  ]);

  return { yesterday, lastWeek, thisMonth, lastMonth };
}
