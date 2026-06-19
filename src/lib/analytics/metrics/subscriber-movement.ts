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
 * Cancellations are counted by the click-cancel date (first observed transition into
 * a churned state, from auto_renew_events) — NOT auto_renews.canceled_at, which Union
 * sets to the period-end date for Pending Cancel rows. Cross-category plan changers
 * (upgrade/downgrade between Member/Sky3/TV) are excluded from new+canceled counts.
 */
import { getPool } from "../../db/database";
import { getFirstChurnDateByAutoRenewId } from "../../db/auto-renew-events-store";
import { getCategory, isAnnualPlan } from "../categories";
import { parseDate } from "../date-utils";
import { ACTIVE_STATES } from "./filters";

export type CategoryKey = "member" | "sky3" | "skyTingTv";

export interface CategoryMovement {
  /** Subscription rows newly created in this window */
  new: number;
  /** Subscription rows canceled in this window (excludes plan changers) */
  canceled: number;
  /** Subscription rows active at the START of the window */
  activeAtStart: number;
  /** Sum of monthly-equivalent rates for canceled subscriptions */
  canceledMrr: number;
  /** Sum of monthly-equivalent rates for active-at-start */
  activeMrrAtStart: number;
  // MEMBER-only: monthly-billed subscriber subset (excludes annual plans).
  // Used by the "Monthly-billed member churn rate" cards on the Members page.
  // Undefined for sky3 / skyTingTv (no annual variant).
  monthlyCanceled?: number;
  monthlyActiveAtStart?: number;
  monthlyCanceledMrr?: number;
  monthlyActiveMrrAtStart?: number;
}

export interface PlanChangeDetail {
  from: "MEMBER" | "SKY3" | "SKY_TING_TV";
  to: "MEMBER" | "SKY3" | "SKY_TING_TV";
  direction: "upgrade" | "downgrade";
  count: number;
}

export interface WindowMovement {
  windowStart: string; // YYYY-MM-DD inclusive
  windowEnd: string;   // YYYY-MM-DD exclusive
  member: CategoryMovement;
  sky3: CategoryMovement;
  skyTingTv: CategoryMovement;
  planChanges: PlanChangeDetail[];
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
  id: number;
  plan_name: string;
  plan_state: string;
  category: string;
  created_at: string | null;
  canceled_at: string | null;
  customer_email: string;
  monthlyRate: number;
  /** True for plans that bill annually (e.g. "Member Annual"). Drives the
   * MEMBER monthly-billed-only split on the Members page churn cards. */
  isAnnual: boolean;
  /**
   * The day we first observed this subscription transition into a churned state,
   * derived from the auto_renew_events log. This is the "click cancel" date and
   * REPLACES canceled_at for cancellation-window counting (Union sets canceled_at
   * to the period-end date, not the click date — see auto-renew-events-store.ts).
   * Null when no live churn event exists or the row failed phantom-firing guards.
   */
  churn_date: string | null;
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
  // Per-category counters — row counts per subscription, matching the
  // canonical row-count doctrine in getActiveCounts(). The Maps key on
  // `${email}|${plan_name}` so each distinct subscription becomes one entry
  // (the SQL pre-deduplicates snapshots via DISTINCT ON, so two entries with
  // the same key shouldn't occur in practice; the Map shape is kept defensively).
  const newByCat: Record<CategoryKey, Map<string, number>> = {
    member: new Map(), sky3: new Map(), skyTingTv: new Map(),
  };
  const canceledByCat: Record<CategoryKey, Map<string, number>> = {
    member: new Map(), sky3: new Map(), skyTingTv: new Map(),
  };
  const activeAtStartByCat: Record<CategoryKey, Map<string, number>> = {
    member: new Map(), sky3: new Map(), skyTingTv: new Map(),
  };
  // MEMBER-only: monthly-billed subset (excludes annual plans). Drives the
  // "Monthly-billed member churn rate" weekly/monthly cards on the Members page.
  const memberMonthlyCanceled = new Map<string, number>();
  const memberMonthlyActiveAtStart = new Map<string, number>();

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
    // Cancellations counted by churn_date (click date from auto_renew_events),
    // NOT canceled_at (which is the period-end date for Pending Cancel rows).
    // STILL_PAYING_STATES guard is unnecessary here because churn_date is only
    // populated when the row is currently in Canceled or Pending Cancel state.
    if (
      r.churn_date &&
      r.churn_date >= startDate && r.churn_date < endDate
    ) {
      if (!canceledByEmail.has(email)) canceledByEmail.set(email, ck);
    }
  }
  // Plan changers: emails that appear as new in one category AND canceled
  // in a DIFFERENT category within this window.
  const planChangers = new Set<string>();
  const planChangePairs = new Map<string, { fromKey: CategoryKey; toKey: CategoryKey; count: number }>();
  const cat2upper: Record<CategoryKey, "MEMBER" | "SKY3" | "SKY_TING_TV"> = {
    member: "MEMBER", sky3: "SKY3", skyTingTv: "SKY_TING_TV",
  };
  // Tier rank for upgrade/downgrade direction (higher = more valuable tier)
  const tierRank: Record<CategoryKey, number> = { skyTingTv: 1, sky3: 2, member: 3 };
  for (const [email, canceledCat] of canceledByEmail) {
    const newCat = newByEmail.get(email);
    if (!newCat || newCat === canceledCat) continue;
    planChangers.add(email);
    const key = `${canceledCat}→${newCat}`;
    const existing = planChangePairs.get(key);
    if (existing) existing.count++;
    else planChangePairs.set(key, { fromKey: canceledCat, toKey: newCat, count: 1 });
  }
  const planChanges: PlanChangeDetail[] = Array.from(planChangePairs.values())
    .map((p) => ({
      from: cat2upper[p.fromKey],
      to: cat2upper[p.toKey],
      direction: (tierRank[p.toKey] > tierRank[p.fromKey] ? "upgrade" : "downgrade") as "upgrade" | "downgrade",
      count: p.count,
    }))
    .sort((a, b) => {
      if (a.direction !== b.direction) return a.direction === "upgrade" ? -1 : 1;
      return b.count - a.count;
    });

  // Second pass: count new + canceled + active-at-start as ROWS per category.
  // Key on `${email}|${plan_name}` so a person with two different subscriptions
  // in the same category (e.g. SKY3 Monthly + SKYHIGH3 Monthly) counts twice.
  for (const r of rows) {
    const ck = categoryKey(r.category);
    if (!ck) continue;
    const email = r.customer_email.toLowerCase();
    if (!email) continue;
    const subKey = `${email}|${r.plan_name}`;

    // Active at start: created before window AND (still active OR canceled at/after window start)
    if (r.created_at && r.created_at < startDate) {
      const stillActive = ACTIVE_STATES.includes(r.plan_state);
      const canceledLater = r.canceled_at && r.canceled_at >= startDate;
      if (stillActive || canceledLater) {
        activeAtStartByCat[ck].set(subKey, r.monthlyRate);
        if (ck === "member" && !r.isAnnual) {
          memberMonthlyActiveAtStart.set(subKey, r.monthlyRate);
        }
      }
    }

    // Skip plan changers from new/canceled counts (still detected at email level —
    // a plan change is a person-level event, not a per-subscription event).
    if (planChangers.has(email)) continue;

    // New in window
    if (r.created_at && r.created_at >= startDate && r.created_at < endDate) {
      newByCat[ck].set(subKey, r.monthlyRate);
    }

    // Canceled in window — keyed off churn_date from auto_renew_events.
    if (
      r.churn_date &&
      r.churn_date >= startDate && r.churn_date < endDate
    ) {
      canceledByCat[ck].set(subKey, r.monthlyRate);
      if (ck === "member" && !r.isAnnual) {
        memberMonthlyCanceled.set(subKey, r.monthlyRate);
      }
    }
  }

  function mkCategoryMetrics(ck: CategoryKey): CategoryMovement {
    const sumRates = (m: Map<string, number>) =>
      Math.round(Array.from(m.values()).reduce((s, v) => s + v, 0) * 100) / 100;
    const base: CategoryMovement = {
      new: newByCat[ck].size,
      canceled: canceledByCat[ck].size,
      activeAtStart: activeAtStartByCat[ck].size,
      canceledMrr: sumRates(canceledByCat[ck]),
      activeMrrAtStart: sumRates(activeAtStartByCat[ck]),
    };
    if (ck === "member") {
      base.monthlyCanceled = memberMonthlyCanceled.size;
      base.monthlyActiveAtStart = memberMonthlyActiveAtStart.size;
      base.monthlyCanceledMrr = sumRates(memberMonthlyCanceled);
      base.monthlyActiveMrrAtStart = sumRates(memberMonthlyActiveAtStart);
    }
    return base;
  }

  return {
    windowStart: startDate,
    windowEnd: endDate,
    member: mkCategoryMetrics("member"),
    sky3: mkCategoryMetrics("sky3"),
    skyTingTv: mkCategoryMetrics("skyTingTv"),
    planChanges,
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

/**
 * Compute 6-month average churn rates from a completed monthly array.
 * Excludes the current partial month (last entry) and Oct 2025 bulk cleanup.
 */
export function computeAvgChurnRates(
  monthly: Array<{ userChurnRate: number; mrrChurnRate: number; month: string }>,
): { avgUserChurnRate: number; avgMrrChurnRate: number } {
  const completed = monthly.slice(0, -1).filter((m) => m.month !== "2025-10");
  if (completed.length === 0) return { avgUserChurnRate: 0, avgMrrChurnRate: 0 };
  return {
    avgUserChurnRate: Math.round(
      (completed.reduce((s, m) => s + m.userChurnRate, 0) / completed.length) * 10,
    ) / 10,
    avgMrrChurnRate: Math.round(
      (completed.reduce((s, m) => s + m.mrrChurnRate, 0) / completed.length) * 10,
    ) / 10,
  };
}

export async function getSubscriberMovement(): Promise<SubscriberMovement> {
  const pool = getPool();
  // No DISTINCT ON — auto_renews has a UNIQUE constraint on
  // (customer_email, plan_name, created_at), so each row already
  // represents one unique subscription. Counting rows directly matches
  // getActiveAutoRenews() and Union's admin row counts. People genuinely
  // subscribed to the same plan twice (different created_at) appear as
  // two rows and count twice — which is correct.
  const [{ rows: allRows }, churnDateById] = await Promise.all([
    pool.query(
      `SELECT id, plan_name, plan_state, plan_price,
              canceled_at, pending_canceled_at, created_at, customer_email
       FROM auto_renews`,
    ),
    getFirstChurnDateByAutoRenewId(),
  ]);

  const categorized: CategorizedRow[] = allRows.map((r: Record<string, unknown>) => {
    const name = (r.plan_name as string) || "";
    const annual = isAnnualPlan(name);
    const price = Number(r.plan_price) || 0;
    const id = Number(r.id);
    const state = (r.plan_state as string) || "";
    const canceledAt = toDateStr(r.canceled_at);
    const pendingCanceledAt = toDateStr(r.pending_canceled_at);
    const liveChurnDate = churnDateById.get(id) ?? null;
    // Click-date priority for cancellation-window counts:
    //   1. pending_canceled_at — Union's canonical click timestamp (set when
    //      the user clicked cancel, i.e. entered Pending Cancel state).
    //      Populated by the zip ingest from pass.pendingCanceledAt.
    //   2. Churn date from auto_renew_events (getFirstChurnDateByAutoRenewId):
    //      live observations of the → Pending Cancel/Canceled transition in
    //      the click-date era, PLUS the historical backfill_churn leg for rows
    //      terminally Canceled before the event log existed (< 2026-04-14) —
    //      without that leg, every month before Apr '26 reads 0% churn on the
    //      monthly history cards.
    //   3. null — no churn record. We do NOT fall back to canceled_at here:
    //      for current-era rows it's the period-end date for click-then-roll
    //      cancellations and bucketing by it produces the period-end
    //      clustering bug PR #8 fixed.
    const churnDate = pendingCanceledAt ?? liveChurnDate ?? null;
    return {
      id,
      plan_name: name,
      plan_state: state,
      category: getCategory(name),
      created_at: toDateStr(r.created_at),
      canceled_at: canceledAt,
      customer_email: (r.customer_email as string) || "",
      monthlyRate: annual ? Math.round((price / 12) * 100) / 100 : price,
      isAnnual: annual,
      churn_date: churnDate,
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
