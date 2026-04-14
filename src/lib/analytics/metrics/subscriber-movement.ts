/**
 * Single canonical source for subscriber movement (new signups + cancellations)
 * across all dashboard windows and periods.
 *
 * Replaces the parallel computations that used to live in:
 *   - computeChurnRates() in db-trends.ts (monthly only)
 *   - inline weekly logic in computeTrendsFromDB() (db-trends.ts)
 *   - getOverviewData() / computeWindow() in overview-store.ts (event-log based)
 *   - getChurnEventsInWindow() in auto-renew-events-store.ts (broken event log)
 *
 * Uses the canonical cancellation filter (plan_state NOT IN STILL_PAYING_STATES
 * with canceled_at in window) and excludes cross-category plan changers
 * (upgrade/downgrade between Member/Sky3/TV).
 */
import { getPool } from "../../db/database";
import { getCategory, isAnnualPlan } from "../categories";
import { parseDate } from "../date-utils";
import { ACTIVE_STATES, STILL_PAYING_STATES } from "./filters";

export type CategoryKey = "member" | "sky3" | "skyTingTv";

export interface CategoryMovement {
  /** Distinct emails newly created in this window */
  new: number;
  /** Distinct emails canceled in this window (excludes plan changers) */
  canceled: number;
  /** Distinct emails active at the START of the window */
  activeAtStart: number;
  /** Sum of monthly-equivalent rates for canceled subscribers */
  canceledMrr: number;
  /** Sum of monthly-equivalent rates for active-at-start */
  activeMrrAtStart: number;
}

export interface WindowMovement {
  windowStart: string; // YYYY-MM-DD inclusive
  windowEnd: string;   // YYYY-MM-DD exclusive
  member: CategoryMovement;
  sky3: CategoryMovement;
  skyTingTv: CategoryMovement;
}

export interface PeriodMovement extends WindowMovement {
  /** YYYY-MM-DD (Monday) for weeks, YYYY-MM for months */
  period: string;
  periodType: "weekly" | "monthly";
}

export interface SubscriberMovement {
  byWindow: {
    yesterday: WindowMovement;
    thisWeek: WindowMovement;
    lastWeek: WindowMovement;
    thisMonth: WindowMovement;
    lastMonth: WindowMovement;
  };
  weekly: PeriodMovement[];   // last 32 weeks (oldest to newest)
  monthly: PeriodMovement[];  // last 8 months (oldest to newest)
}

interface CategorizedRow {
  plan_name: string;
  plan_state: string;
  category: string;
  created_at: string | null;
  canceled_at: string | null;
  customer_email: string;
  monthlyRate: number;
}

function toDateStr(raw: unknown): string | null {
  if (raw == null) return null;
  if (raw instanceof Date) {
    if (isNaN(raw.getTime())) return null;
    return `${raw.getFullYear()}-${String(raw.getMonth() + 1).padStart(2, "0")}-${String(raw.getDate()).padStart(2, "0")}`;
  }
  const s = String(raw).trim();
  if (s === "") return null;
  const d = parseDate(s);
  if (!d) return null;
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}-${String(d.getDate()).padStart(2, "0")}`;
}

function categoryKey(cat: string): CategoryKey | null {
  if (cat === "MEMBER") return "member";
  if (cat === "SKY3") return "sky3";
  if (cat === "SKY_TING_TV") return "skyTingTv";
  return null;
}

/** Compute movement metrics for a single [startDate, endDate) window. */
function computeWindow(
  rows: CategorizedRow[],
  startDate: string,
  endDate: string,
): WindowMovement {
  // Build per-category sets for new, canceled (with monthlyRate), active-at-start
  const newByCat: Record<CategoryKey, Map<string, number>> = {
    member: new Map(), sky3: new Map(), skyTingTv: new Map(),
  };
  const canceledByCat: Record<CategoryKey, Map<string, number>> = {
    member: new Map(), sky3: new Map(), skyTingTv: new Map(),
  };
  const activeAtStartByCat: Record<CategoryKey, Map<string, number>> = {
    member: new Map(), sky3: new Map(), skyTingTv: new Map(),
  };

  // Plan-changer detection: by-email across categories, in this window.
  // If a customer appears as "new" in one category AND "canceled" in
  // another within the same window, they're a plan changer (upgrade/downgrade)
  // and should be excluded from BOTH the new and canceled counts.
  const newByEmail = new Map<string, CategoryKey>();
  const canceledByEmail = new Map<string, CategoryKey>();

  // First pass: identify plan changers
  for (const r of rows) {
    const ck = categoryKey(r.category);
    if (!ck) continue;
    const email = r.customer_email.toLowerCase();
    if (!email) continue;

    if (r.created_at && r.created_at >= startDate && r.created_at < endDate) {
      if (!newByEmail.has(email)) newByEmail.set(email, ck);
    }
    if (
      r.canceled_at &&
      r.canceled_at >= startDate && r.canceled_at < endDate &&
      !STILL_PAYING_STATES.includes(r.plan_state)
    ) {
      if (!canceledByEmail.has(email)) canceledByEmail.set(email, ck);
    }
  }
  const planChangers = new Set<string>();
  for (const [email, canceledCat] of canceledByEmail) {
    const newCat = newByEmail.get(email);
    if (newCat && newCat !== canceledCat) planChangers.add(email);
  }

  // Second pass: count new + canceled + active-at-start, deduped by email per category
  // For active-at-start: max monthlyRate per email. For new/canceled: any rate is fine
  // (we still want a representative rate for MRR sums).
  for (const r of rows) {
    const ck = categoryKey(r.category);
    if (!ck) continue;
    const email = r.customer_email.toLowerCase();
    if (!email) continue;

    // Active at start: created before window AND (still active OR canceled at/after window start)
    if (r.created_at && r.created_at < startDate) {
      const stillActive = ACTIVE_STATES.includes(r.plan_state);
      const canceledLater = r.canceled_at && r.canceled_at >= startDate;
      if (stillActive || canceledLater) {
        // Dedup by email: keep max monthlyRate, but always add the email even if rate=0
        const existing = activeAtStartByCat[ck].get(email);
        if (existing === undefined || r.monthlyRate > existing) {
          activeAtStartByCat[ck].set(email, r.monthlyRate);
        }
      }
    }

    // Skip plan changers from new/canceled counts
    if (planChangers.has(email)) continue;

    // New in window
    if (r.created_at && r.created_at >= startDate && r.created_at < endDate) {
      const existing = newByCat[ck].get(email);
      if (existing === undefined || r.monthlyRate > existing) {
        newByCat[ck].set(email, r.monthlyRate);
      }
    }

    // Canceled in window (excluding still-paying states)
    if (
      r.canceled_at &&
      r.canceled_at >= startDate && r.canceled_at < endDate &&
      !STILL_PAYING_STATES.includes(r.plan_state)
    ) {
      const existing = canceledByCat[ck].get(email);
      if (existing === undefined || r.monthlyRate > existing) {
        canceledByCat[ck].set(email, r.monthlyRate);
      }
    }
  }

  function mkCategoryMetrics(ck: CategoryKey): CategoryMovement {
    const sumRates = (m: Map<string, number>) =>
      Math.round(Array.from(m.values()).reduce((s, v) => s + v, 0) * 100) / 100;
    return {
      new: newByCat[ck].size,
      canceled: canceledByCat[ck].size,
      activeAtStart: activeAtStartByCat[ck].size,
      canceledMrr: sumRates(canceledByCat[ck]),
      activeMrrAtStart: sumRates(activeAtStartByCat[ck]),
    };
  }

  return {
    windowStart: startDate,
    windowEnd: endDate,
    member: mkCategoryMetrics("member"),
    sky3: mkCategoryMetrics("sky3"),
    skyTingTv: mkCategoryMetrics("skyTingTv"),
  };
}

function ymd(d: Date): string {
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}-${String(d.getDate()).padStart(2, "0")}`;
}

/** Monday of the week containing the given date (treating Mon as week start). */
function mondayOf(d: Date): Date {
  const out = new Date(d.getFullYear(), d.getMonth(), d.getDate());
  const day = out.getDay(); // 0 = Sun, 1 = Mon
  const offset = day === 0 ? 6 : day - 1;
  out.setDate(out.getDate() - offset);
  return out;
}

function addDays(d: Date, days: number): Date {
  const out = new Date(d);
  out.setDate(out.getDate() + days);
  return out;
}

function startOfMonth(year: number, monthIndex: number): Date {
  return new Date(year, monthIndex, 1);
}

export async function getSubscriberMovement(): Promise<SubscriberMovement> {
  const pool = getPool();
  const { rows: allRows } = await pool.query(
    `SELECT plan_name, plan_state, plan_price, canceled_at, created_at, customer_email
     FROM auto_renews`,
  );

  const categorized: CategorizedRow[] = allRows.map((r: Record<string, unknown>) => {
    const name = (r.plan_name as string) || "";
    const annual = isAnnualPlan(name);
    const price = Number(r.plan_price) || 0;
    return {
      plan_name: name,
      plan_state: (r.plan_state as string) || "",
      category: getCategory(name),
      created_at: toDateStr(r.created_at),
      canceled_at: toDateStr(r.canceled_at),
      customer_email: (r.customer_email as string) || "",
      monthlyRate: annual ? Math.round((price / 12) * 100) / 100 : price,
    };
  });

  // Build named windows
  const now = new Date();
  const today = new Date(now.getFullYear(), now.getMonth(), now.getDate()); // local midnight
  const yesterday = addDays(today, -1);
  const tomorrow = addDays(today, 1);

  const thisWeekStart = mondayOf(today);
  const nextWeekStart = addDays(thisWeekStart, 7);
  const lastWeekStart = addDays(thisWeekStart, -7);

  const thisMonthStart = startOfMonth(today.getFullYear(), today.getMonth());
  const nextMonthStart = startOfMonth(today.getFullYear(), today.getMonth() + 1);
  const lastMonthStart = startOfMonth(today.getFullYear(), today.getMonth() - 1);

  const byWindow = {
    yesterday: computeWindow(categorized, ymd(yesterday), ymd(today)),
    thisWeek: computeWindow(categorized, ymd(thisWeekStart), ymd(tomorrow)),
    lastWeek: computeWindow(categorized, ymd(lastWeekStart), ymd(thisWeekStart)),
    thisMonth: computeWindow(categorized, ymd(thisMonthStart), ymd(tomorrow)),
    lastMonth: computeWindow(categorized, ymd(lastMonthStart), ymd(thisMonthStart)),
  };

  // Weekly periods: last 32 weeks, oldest to newest, week starts on Monday
  const weekly: PeriodMovement[] = [];
  for (let i = 31; i >= 0; i--) {
    const wStart = addDays(thisWeekStart, -7 * i);
    const wEnd = addDays(wStart, 7);
    const w = computeWindow(categorized, ymd(wStart), ymd(wEnd));
    weekly.push({ ...w, period: ymd(wStart), periodType: "weekly" });
  }

  // Monthly periods: last 8 months, oldest to newest
  const monthly: PeriodMovement[] = [];
  for (let i = 7; i >= 0; i--) {
    const mStart = startOfMonth(today.getFullYear(), today.getMonth() - i);
    const mEnd = startOfMonth(today.getFullYear(), today.getMonth() - i + 1);
    const m = computeWindow(categorized, ymd(mStart), ymd(mEnd));
    monthly.push({
      ...m,
      period: `${mStart.getFullYear()}-${String(mStart.getMonth() + 1).padStart(2, "0")}`,
      periodType: "monthly",
    });
  }

  return { byWindow, weekly, monthly };
}
