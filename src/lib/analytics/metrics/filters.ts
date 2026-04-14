/**
 * Canonical plan_state filter constants.
 *
 * Every SQL query and TypeScript filter that touches `auto_renews.plan_state`
 * MUST import from this file. No inline string arrays anywhere else.
 *
 * See CLAUDE.md `METRIC SOURCES` and `ACTIVE SUBSCRIBER COUNTING` for context.
 */

/**
 * A subscriber is "active" if they're in any of these states.
 *   - Valid Now: actively billing, fully active
 *   - Paused: on hold but committed, will resume — counted as active
 *   - Pending Cancel: chose to cancel, currently still active until canceled_at
 *   - In Trial: trial period, hasn't been billed yet
 *   - Invalid: pass-based subscribers who used all their passes — still subscribed
 *
 * Excluded: 'Canceled' (formal cancellation), 'Past Due' (payment failed).
 *
 * Use this for: counting active subscribers, computing active-at-period-start
 * for churn rates, anywhere the dashboard says "active subscribers".
 */
export const ACTIVE_STATES: readonly string[] = [
  "Valid Now",
  "Paused",
  "Pending Cancel",
  "In Trial",
  "Invalid",
];

/**
 * A subscriber is "billing this month" if they're actively having money
 * collected from them. This is the GAAP/ASC 606-aligned filter used for MRR.
 *
 * Excludes Paused (no billing during pause), Invalid (already collected,
 * no future billing until they renew), In Trial (full price not being
 * charged — trial price is different from list price stored in plan_price).
 */
export const BILLING_STATES: readonly string[] = ["Valid Now", "Pending Cancel"];

/**
 * For a row with a non-null `canceled_at`, these states mean the
 * `canceled_at` value is the NEXT BILLING DATE, not a real cancellation.
 *
 * When counting cancellations in a window using `canceled_at IN [start, end)`,
 * exclude rows whose plan_state is in this set — otherwise active subscribers
 * whose next renewal happens to fall in the window get miscounted as churn.
 *
 * Equivalently: a "real cancellation" filter is `plan_state NOT IN STILL_PAYING_STATES`.
 */
export const STILL_PAYING_STATES: readonly string[] = ["Valid Now", "Paused"];

/**
 * At-risk states — subscribers who haven't formally canceled but are
 * showing signals of imminent loss. Used by alert/insight detectors.
 */
export const AT_RISK_STATES: readonly string[] = ["Past Due", "Invalid", "Pending Cancel"];

// ─── SQL fragment helpers ────────────────────────────────────────
// Use these in raw SQL queries instead of inline arrays. They produce
// a comma-separated quoted list suitable for use inside `IN (...)`.

function asSqlList(states: readonly string[]): string {
  return states.map((s) => `'${s}'`).join(", ");
}

/** SQL fragment: `'Valid Now', 'Paused', 'Pending Cancel', 'In Trial', 'Invalid'` */
export const ACTIVE_STATES_SQL = asSqlList(ACTIVE_STATES);

/** SQL fragment: `'Valid Now', 'Pending Cancel'` */
export const BILLING_STATES_SQL = asSqlList(BILLING_STATES);

/** SQL fragment: `'Valid Now', 'Paused'` (used in `NOT IN` for canceled_at gating) */
export const STILL_PAYING_STATES_SQL = asSqlList(STILL_PAYING_STATES);

/** SQL fragment: `'Past Due', 'Invalid', 'Pending Cancel'` */
export const AT_RISK_STATES_SQL = asSqlList(AT_RISK_STATES);
