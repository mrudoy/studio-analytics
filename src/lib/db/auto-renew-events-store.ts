import { getPool } from "./database";

/**
 * The day live churn observation began: migration 019 (the auto_renew_events
 * trigger + backfill) was applied 2026-04-14. Before this date there are no
 * live 'churn' events and no pending_canceled_at ingests — the ONLY churn
 * record for that era is the backfill_churn event synthesized from each
 * Canceled row's canceled_at. On/after this date, click-date sources are
 * authoritative and backfill dates must not contribute (they carry the
 * period-end clustering this module exists to escape).
 */
export const CLICK_DATE_ERA_START = "2026-04-14";

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
 * Two legs:
 *
 * 1. LIVE (click-date era, ≥ CLICK_DATE_ERA_START) — data-quality filters
 *    (the trigger has known noise):
 *   - Require prev_state ∈ active-ish states (real transition, not Pending→Canceled rollover).
 *   - Dedup by auto_renew_id (trigger has fired multiple times for the same sub during
 *     daily-snapshot oscillations and re-imports — take the EARLIEST observed_at).
 *   - Phantom-firing guards: the row must currently be in a churned plan_state, AND
 *     either the row's canceled_at is within 1 day of the event (live cancel) or NULL,
 *     AND the row's imported_at is within 7 days of the event (we observed the row
 *     arrive recently, not weeks-stale state). Without these, the dashboard counts
 *     ~149 May 8 cancels (mostly bulk period-ends + sync-trigger phantoms) instead
 *     of the ~10 actual user clicks.
 *
 * 2. HISTORICAL BACKFILL (pre-era only) — rows that were already terminally
 *    Canceled before the event log went live have no live event and no
 *    pending_canceled_at; without this leg every month before 2026-04 reads
 *    ZERO churn on the monthly history cards (caught 2026-06-10: Sky3 showed
 *    0% for Nov–Feb against 74–90 real cancellations/month). Their
 *    backfill_churn event (observed_at = the row's canceled_at, synthesized at
 *    migration time) is the only record that exists — per CLAUDE.md, backfill
 *    events count identically to live events. Restricted to:
 *      - observed_at < CLICK_DATE_ERA_START, so period-end-dated backfill rows
 *        can never bleed into the click-date era; and
 *      - plan_state = 'Canceled' (terminal), so a Pending Cancel row's FUTURE
 *        period-end date is never used as a churn date.
 *    When a row somehow has both (canceled pre-era, resumed, churned again
 *    live), the EARLIEST date wins — matching this function's "first churn"
 *    contract.
 */
export async function getFirstChurnDateByAutoRenewId(): Promise<Map<number, string>> {
  const pool = getPool();
  const { rows } = await pool.query<{ auto_renew_id: number; churn_date: string; leg: string }>(
    `WITH live_churn AS (
       SELECT DISTINCT ON (auto_renew_id)
         auto_renew_id, observed_at
       FROM auto_renew_events
       WHERE event_type = 'churn'
         AND prev_state IN ('Valid Now','Paused','In Trial','Past Due','Invalid')
         -- Enforce the era partition in SQL (not just by construction): live
         -- click dates begin at CLICK_DATE_ERA_START; the backfill leg owns
         -- everything before it. Guards against any future data fix emitting
         -- a 'churn' event with a pre-era timestamp.
         AND (observed_at AT TIME ZONE 'America/New_York')::date >= $1::date
       ORDER BY auto_renew_id, observed_at ASC
     ),
     live AS (
       SELECT lc.auto_renew_id,
              TO_CHAR(lc.observed_at AT TIME ZONE 'America/New_York', 'YYYY-MM-DD') AS churn_date
       FROM live_churn lc
       JOIN auto_renews ar ON ar.id = lc.auto_renew_id
       WHERE ar.plan_state IN ('Canceled','Pending Cancel')
         AND (ar.canceled_at IS NULL
              OR ar.canceled_at >= (lc.observed_at AT TIME ZONE 'America/New_York')::date - INTERVAL '1 day')
         AND ar.imported_at::date >= (lc.observed_at AT TIME ZONE 'America/New_York')::date - INTERVAL '7 days'
     ),
     backfill AS (
       SELECT DISTINCT ON (bc.auto_renew_id)
              bc.auto_renew_id,
              TO_CHAR(bc.observed_at AT TIME ZONE 'America/New_York', 'YYYY-MM-DD') AS churn_date
       FROM auto_renew_events bc
       JOIN auto_renews ar ON ar.id = bc.auto_renew_id
       WHERE bc.event_type = 'backfill_churn'
         AND ar.plan_state = 'Canceled'
         AND (bc.observed_at AT TIME ZONE 'America/New_York')::date < $1::date
       ORDER BY bc.auto_renew_id, bc.observed_at ASC
     )
     SELECT auto_renew_id, churn_date, 'live' AS leg FROM live
     UNION ALL
     SELECT auto_renew_id, churn_date, 'backfill' AS leg FROM backfill`,
    [CLICK_DATE_ERA_START],
  );
  const out = new Map<number, string>();
  for (const r of rows) {
    const prev = out.get(r.auto_renew_id);
    // Earliest date wins ("first churn"); legs rarely overlap (resume + re-churn).
    if (!prev || r.churn_date < prev) out.set(r.auto_renew_id, r.churn_date);
  }
  return out;
}
