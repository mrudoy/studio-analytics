/**
 * Auto-renew event log queries.
 *
 * Reads from the `auto_renew_events` table (see migration 019), which is
 * populated by a Postgres trigger on every state change in `auto_renews`.
 *
 * The overview dashboard uses these instead of scanning static columns on
 * auto_renews — giving accurate churn counts that include `Pending Cancel`
 * transitions (when users click cancel, not when their period ends).
 *
 * `final_cancel` events (Pending Cancel → Canceled) are excluded from churn
 * queries so the same cancellation isn't counted twice.
 *
 * Backfill events (`backfill_signup`, `backfill_churn`) are treated the same
 * as live events so historical windows are populated.
 */
import { getPool } from "./database";
import type { AutoRenewCategory } from "@/types/union-data";

export interface AutoRenewEvent {
  autoRenewId: number | null;
  customerEmail: string;
  category: AutoRenewCategory;
  planName: string;
  prevState: string | null;
  newState: string;
  observedAt: string; // ISO timestamp
  isBackfill: boolean;
}

interface RawEventRow {
  auto_renew_id: number | null;
  customer_email: string;
  plan_name: string | null;
  plan_category: string | null;
  prev_state: string | null;
  new_state: string;
  observed_at: Date | string;
  is_backfill: boolean;
}

function mapRow(r: RawEventRow): AutoRenewEvent {
  const cat = (r.plan_category ?? "UNKNOWN") as AutoRenewCategory;
  const observedAt = r.observed_at instanceof Date
    ? r.observed_at.toISOString()
    : String(r.observed_at);
  return {
    autoRenewId: r.auto_renew_id,
    customerEmail: (r.customer_email ?? "").toLowerCase(),
    category: cat,
    planName: r.plan_name ?? "",
    prevState: r.prev_state,
    newState: r.new_state,
    observedAt,
    isBackfill: r.is_backfill,
  };
}

/**
 * Signup events within [startDate, endDate). One row per (email, category) —
 * if a user had multiple signup events in the window (e.g. re-signup after
 * churn), the most recent wins.
 */
export async function getSignupEventsInWindow(
  startDate: string,
  endDate: string,
): Promise<AutoRenewEvent[]> {
  const pool = getPool();
  const { rows } = await pool.query(
    `SELECT DISTINCT ON (customer_email, plan_category)
       auto_renew_id, customer_email, plan_name, plan_category,
       prev_state, new_state, observed_at, is_backfill
     FROM auto_renew_events
     WHERE event_type IN ('signup','backfill_signup')
       AND observed_at >= $1
       AND observed_at < $2
     ORDER BY customer_email, plan_category, observed_at DESC`,
    [startDate, endDate],
  );
  return (rows as RawEventRow[]).map(mapRow);
}

/**
 * Churn events within [startDate, endDate). Excludes `final_cancel` so the
 * Pending Cancel → Canceled flip doesn't count as a new cancellation. One row
 * per (email, category).
 */
export async function getChurnEventsInWindow(
  startDate: string,
  endDate: string,
): Promise<AutoRenewEvent[]> {
  const pool = getPool();
  const { rows } = await pool.query(
    `SELECT DISTINCT ON (customer_email, plan_category)
       auto_renew_id, customer_email, plan_name, plan_category,
       prev_state, new_state, observed_at, is_backfill
     FROM auto_renew_events
     WHERE event_type IN ('churn','backfill_churn')
       AND observed_at >= $1
       AND observed_at < $2
     ORDER BY customer_email, plan_category, observed_at DESC`,
    [startDate, endDate],
  );
  return (rows as RawEventRow[]).map(mapRow);
}
