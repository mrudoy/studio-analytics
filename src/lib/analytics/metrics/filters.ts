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
export const ACTIVE_STATES = [
  "Valid Now",
  "Paused",
  "Pending Cancel",
  "In Trial",
  "Invalid",
] as const;

/**
 * A subscriber is "billing this month" if they're actively having money
 * collected from them. This is the GAAP/ASC 606-aligned filter used for MRR.
 *
 * Excludes Paused (no billing during pause), Invalid (already collected,
 * no future billing until they renew), In Trial (full price not being
 * charged — trial price is different from list price stored in plan_price).
 */
export const BILLING_STATES = ["Valid Now", "Pending Cancel"] as const;

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
export const STILL_PAYING_STATES = ["Valid Now", "Paused"] as const;

/**
 * At-risk states — subscribers who haven't formally canceled but are
 * showing signals of imminent loss. Used by alert/insight detectors.
 */
export const AT_RISK_STATES = ["Past Due", "Invalid", "Pending Cancel"] as const;
