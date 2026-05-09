import { getPool } from "./database";

/**
 * Read churn-event timestamps from auto_renew_events as the canonical "click cancel" date
 * for each subscription, instead of relying on auto_renews.canceled_at.
 *
 * Background: Union sets auto_renews.canceled_at to the SCHEDULED period-end date for
 * Pending Cancel rows, NOT the day the user clicked cancel. So a sub canceled in Feb
 * with a May 8 period-end shows up in our data with canceled_at=May 8 — and naive
 * "cancellations on May 8" queries (the previous behavior of subscriber-movement.ts)
 * spike on every period-end day.
 *
 * The auto_renew_events table is written by a Postgres trigger on every plan_state
 * change in auto_renews, so the EARLIEST 'churn' event observed_at for a subscription
 * approximates when we first saw it transition into a churned state.
 *
 * Data-quality filters applied (the trigger has known noise):
 *   - Drop backfill_churn (synthesized from canceled_at — same broken signal we're trying
 *     to escape).
 *   - Require prev_state ∈ active-ish states (real transition, not Pending→Canceled rollover).
 *   - Dedup by auto_renew_id (trigger has fired multiple times for the same sub during
 *     daily-snapshot oscillations and re-imports — take the EARLIEST observed_at).
 *   - Phantom-firing guards: the row must currently be in a churned plan_state, AND
 *     either the row's canceled_at is within 1 day of the event (live cancel) or NULL,
 *     AND the row's imported_at is within 7 days of the event (we observed the row
 *     arrive recently, not weeks-stale state). Without these, the dashboard counts
 *     ~149 May 8 cancels (mostly bulk period-ends + sync-trigger phantoms) instead
 *     of the ~10 actual user clicks.
 */
export async function getFirstChurnDateByAutoRenewId(): Promise<Map<number, string>> {
  const pool = getPool();
  const { rows } = await pool.query<{ auto_renew_id: number; churn_date: string }>(
    `WITH live_churn AS (
       SELECT DISTINCT ON (auto_renew_id)
         auto_renew_id, observed_at
       FROM auto_renew_events
       WHERE event_type = 'churn'
         AND prev_state IN ('Valid Now','Paused','In Trial','Past Due','Invalid')
       ORDER BY auto_renew_id, observed_at ASC
     )
     SELECT lc.auto_renew_id,
            TO_CHAR(lc.observed_at AT TIME ZONE 'America/New_York', 'YYYY-MM-DD') AS churn_date
     FROM live_churn lc
     JOIN auto_renews ar ON ar.id = lc.auto_renew_id
     WHERE ar.plan_state IN ('Canceled','Pending Cancel')
       AND (ar.canceled_at IS NULL
            OR ar.canceled_at >= (lc.observed_at AT TIME ZONE 'America/New_York')::date - INTERVAL '1 day')
       AND ar.imported_at::date >= (lc.observed_at AT TIME ZONE 'America/New_York')::date - INTERVAL '7 days'`,
  );
  const out = new Map<number, string>();
  for (const r of rows) out.set(r.auto_renew_id, r.churn_date);
  return out;
}
