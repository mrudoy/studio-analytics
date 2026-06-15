/**
 * Automated drift checks (Phase E). Runs after each pipeline cycle and asserts
 * invariants that would only break if the subscriber-count drift recurred —
 * with NO dependency on an external "truth" report:
 *
 *   - duplicate active subscription identities  → the duplicate-row inflation
 *     (same `order_id` active more than once, GLOBALLY — Union's order_id is
 *     unique per subscription, so any duplicate is corruption) is back.
 *   - future-dated active rows                  → the +1-day `created_at` TZ skew
 *     (a row "created tomorrow") is back.
 *   - unknown-category active plans             → a new/renamed plan name maps to
 *     UNKNOWN, so it silently drops out of subscriber counts AND revenue (the
 *     FABxSKYTING / "2 WEEK INTRO"-style rename class). Measured via the same
 *     runtime getCategory() the dashboard uses (getActiveCounts().unknown), not
 *     the stored column, so it can't be fooled by a stale plan_category.
 *   - zero-churn completed months               → a completed month renders 0%
 *     churn while subscribers were active (the monthly-churn-history regression
 *     class). Computed from getSubscriberMovement — the exact source the churn
 *     cards read — so it asserts the rendered OUTPUT, not a proxy for it.
 *   - revenue net > gross                        → revenue_categories data integrity
 *     (net should never exceed gross once fees are applied).
 *   - last full reconcile age                   → residual drift is unbounded.
 *   - active-total jump vs the previous run      → anomaly.
 *
 * Each new bug we fix should add an invariant here so its regression is caught
 * automatically — every cycle, with no human and no external truth report.
 * Each run is recorded in `drift_checks` (full metric set in the `metrics`
 * JSONB) and surfaced in the digest.
 */
import { getPool } from "./database";
import { ACTIVE_STATES_SQL } from "../analytics/metrics/filters";
import { getReconcileHealth } from "./reconcile";
import { getActiveCounts } from "./auto-renew-store";
import { getSubscriberMovement } from "../analytics/metrics/subscriber-movement";

export interface DriftMetrics {
  activeMember: number;
  activeSky3: number;
  activeTv: number;
  activeTotal: number;
  /** Active rows beyond the first sharing the same order_id (global), plus
   *  the same for union_pass_id among rows without an order_id. */
  dupActiveIdentities: number;
  /** Active rows whose created_at is in the future (ET) — TZ-skew tripwire. */
  futureDatedRows: number;
  /** Active subscriptions the runtime categorizer (getCategory) places in
   *  UNKNOWN — a plan name we don't recognize, so it vanishes from counts +
   *  revenue. Sourced from getActiveCounts().unknown (the dashboard's own path). */
  unknownActivePlans: number;
  /** Completed months in the canonical churn series (getSubscriberMovement)
   *  that render 0 total churn while subscribers were active — the exact
   *  symptom of the monthly-churn-history regression. */
  zeroChurnCompletedMonths: number;
  /** Deduped revenue_categories rows where net_revenue exceeds gross revenue —
   *  a data-integrity violation (fees only reduce net). */
  revenueNetExceedsGross: number;
  lastFullSyncAgeDays: number | null;
}

export interface DriftResult extends DriftMetrics {
  status: "ok" | "warning" | "alert";
  alerts: string[];
}

export interface DriftThresholds {
  maxSyncAgeDays: number; // warning beyond this
  totalJumpAbs: number; // warning if |Δtotal| exceeds max(this, jumpFrac*prev)
  totalJumpFrac: number;
}

export const DEFAULT_DRIFT_THRESHOLDS: DriftThresholds = {
  maxSyncAgeDays: 10,
  totalJumpAbs: 300,
  totalJumpFrac: 0.1,
};

/**
 * Pure alert derivation — unit-tested. `prevTotal` is the previous run's
 * active_total (null on the first run).
 */
export function deriveAlerts(
  m: DriftMetrics,
  prevTotal: number | null,
  t: DriftThresholds = DEFAULT_DRIFT_THRESHOLDS,
): { status: DriftResult["status"]; alerts: string[] } {
  const alerts: string[] = [];
  let hard = false;
  let soft = false;

  if (m.dupActiveIdentities > 0) {
    alerts.push(`${m.dupActiveIdentities} duplicate active subscription identities (same order_id active >1×) — inflation risk.`);
    hard = true;
  }
  if (m.futureDatedRows > 0) {
    alerts.push(`${m.futureDatedRows} active rows are dated in the future — created_at timezone skew may have returned.`);
    hard = true;
  }
  if (m.unknownActivePlans > 0) {
    alerts.push(`${m.unknownActivePlans} active subscriptions map to UNKNOWN category — a plan name the categorizer doesn't recognize is dropping out of counts + revenue (check categories.ts / a Union rename).`);
    hard = true;
  }
  if (m.zeroChurnCompletedMonths > 0) {
    alerts.push(`${m.zeroChurnCompletedMonths} completed month(s) render 0 churn despite active subscribers — monthly churn history is likely broken (auto_renew_events / backfill leg).`);
    hard = true;
  }
  if (m.revenueNetExceedsGross > 0) {
    alerts.push(`${m.revenueNetExceedsGross} revenue rows have net > gross — revenue_categories data integrity issue.`);
    hard = true;
  }
  if (m.lastFullSyncAgeDays != null && m.lastFullSyncAgeDays > t.maxSyncAgeDays) {
    alerts.push(`Last full reconcile was ${m.lastFullSyncAgeDays} days ago (> ${t.maxSyncAgeDays}).`);
    soft = true;
  }
  if (prevTotal != null) {
    const jump = Math.abs(m.activeTotal - prevTotal);
    const bound = Math.max(t.totalJumpAbs, Math.round(prevTotal * t.totalJumpFrac));
    if (jump > bound) {
      alerts.push(`Active total moved ${m.activeTotal - prevTotal >= 0 ? "+" : ""}${m.activeTotal - prevTotal} since last check (${prevTotal} → ${m.activeTotal}, bound ±${bound}).`);
      soft = true;
    }
  }

  return { status: hard ? "alert" : soft ? "warning" : "ok", alerts };
}

/** Run the checks, record to drift_checks, and return the result. */
export async function runDriftCheck(
  thresholds: DriftThresholds = DEFAULT_DRIFT_THRESHOLDS,
): Promise<DriftResult> {
  const pool = getPool();
  const activeFilter = `plan_state IN (${ACTIVE_STATES_SQL}) AND (current_state IS NULL OR current_state = 'active')`;

  // Active counts via the canonical dashboard function (runtime getCategory),
  // so drift-check sees exactly what the dashboard shows — including the
  // UNKNOWN bucket, which is the rename tripwire below.
  const counts = await getActiveCounts();
  const activeMember = counts.member;
  const activeSky3 = counts.sky3;
  const activeTv = counts.skyTingTv;
  const activeTotal = counts.total;
  const unknownActivePlans = counts.unknown;

  // Duplicate identity = more than one ACTIVE row sharing the same subscription
  // identity. Union's order_id is unique per subscription (verified 2936/2936
  // distinct in a full export), so the invariant is GLOBAL: group by order_id
  // alone — never by (email, plan, order_id), which would miss a duplicate
  // order_id spanning different emails or plans. Second leg covers legacy rows
  // without order_id via union_pass_id (also globally unique; the partial
  // unique index idx_ar_union_pass_id enforces it at the DB layer, this is a
  // belt-and-suspenders assert in case that index is ever dropped).
  const dup = await pool.query(
    `SELECT (
       (SELECT COALESCE(SUM(c - 1), 0) FROM (
          SELECT COUNT(*) c FROM auto_renews
          WHERE ${activeFilter} AND order_id IS NOT NULL
          GROUP BY order_id HAVING COUNT(*) > 1) a)
     + (SELECT COALESCE(SUM(c - 1), 0) FROM (
          SELECT COUNT(*) c FROM auto_renews
          WHERE ${activeFilter} AND order_id IS NULL AND union_pass_id IS NOT NULL
          GROUP BY union_pass_id HAVING COUNT(*) > 1) b)
     )::int AS extra`,
  );
  const dupActiveIdentities = (dup.rows[0]?.extra as number) ?? 0;

  const future = await pool.query(
    `SELECT COUNT(*)::int n FROM auto_renews
     WHERE ${activeFilter}
       AND created_at > (NOW() AT TIME ZONE 'America/New_York')::date`,
  );
  const futureDatedRows = (future.rows[0]?.n as number) ?? 0;

  // Monthly churn history regression: assert the RENDERED output, not a proxy.
  // getSubscriberMovement is the exact source the churn cards read (current_state-
  // aware churn_date, backfill leg included). A completed month (any but the last,
  // which is the partial current month) with active subscribers but zero total
  // churn is the precise symptom of the history breaking — a real month for a
  // studio this size always has cancellations. 2025-10 is the known bulk-admin
  // cleanup month and is excluded from churn averages, so exclude it here too.
  let zeroChurnCompletedMonths = 0;
  try {
    const movement = await getSubscriberMovement();
    const completed = movement.monthly.slice(0, -1).filter((m) => m.period !== "2025-10");
    for (const m of completed) {
      const activeAtStart = m.member.activeAtStart + m.sky3.activeAtStart + m.skyTingTv.activeAtStart;
      const canceled = m.member.canceled + m.sky3.canceled + m.skyTingTv.canceled;
      if (activeAtStart > 0 && canceled === 0) zeroChurnCompletedMonths++;
    }
  } catch { /* non-fatal — leave at 0 if movement can't be computed */ }

  // Revenue integrity: on the canonical deduped, single-month rows, net revenue
  // must never exceed gross (fees only reduce net). Mirrors the dedup pattern
  // every revenue-store function uses so it sees the same rows the dashboard does.
  // Columns are NUMERIC(12,2), so an exact `>` comparison is safe (no float noise).
  const revInteg = await pool.query(
    `WITH deduped AS (
       SELECT DISTINCT ON (TRIM(category), TO_CHAR(period_start, 'YYYY-MM'))
         revenue, net_revenue
       FROM revenue_categories
       WHERE DATE_TRUNC('month', period_start) = DATE_TRUNC('month', period_end)
       ORDER BY TRIM(category), TO_CHAR(period_start, 'YYYY-MM'), period_end DESC, created_at DESC
     )
     SELECT COUNT(*)::int n FROM deduped WHERE net_revenue > revenue`,
  );
  const revenueNetExceedsGross = (revInteg.rows[0]?.n as number) ?? 0;

  let lastFullSyncAgeDays: number | null = null;
  try {
    lastFullSyncAgeDays = (await getReconcileHealth(pool)).daysAgo;
  } catch { /* non-fatal */ }

  const prev = await pool.query(
    `SELECT active_total FROM drift_checks ORDER BY ran_at DESC LIMIT 1`,
  );
  const prevTotal = prev.rows.length ? (prev.rows[0].active_total as number) : null;

  const metrics: DriftMetrics = {
    activeMember, activeSky3, activeTv, activeTotal,
    dupActiveIdentities, futureDatedRows,
    unknownActivePlans, zeroChurnCompletedMonths, revenueNetExceedsGross,
    lastFullSyncAgeDays,
  };
  const { status, alerts } = deriveAlerts(metrics, prevTotal, thresholds);

  await pool.query(
    `INSERT INTO drift_checks
       (active_member, active_sky3, active_tv, active_total,
        dup_active_identities, future_dated_rows, last_full_sync_age_days,
        metrics, alerts, status)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
    [
      activeMember, activeSky3, activeTv, activeTotal,
      dupActiveIdentities, futureDatedRows, lastFullSyncAgeDays,
      JSON.stringify(metrics), JSON.stringify(alerts), status,
    ],
  );

  if (status !== "ok") {
    console.warn(`[drift-check] status=${status}: ${alerts.join(" | ")}`);
  }
  return { ...metrics, status, alerts };
}

/** Latest recorded drift check, for the digest banner. */
export async function getLatestDriftAlerts(): Promise<{ status: "ok" | "warning" | "alert"; alerts: string[] } | null> {
  const pool = getPool();
  const { rows } = await pool.query(
    `SELECT status, alerts FROM drift_checks ORDER BY ran_at DESC LIMIT 1`,
  );
  if (rows.length === 0) return null;
  const r = rows[0] as { status: "ok" | "warning" | "alert"; alerts: unknown };
  const alerts = Array.isArray(r.alerts) ? (r.alerts as string[]) : [];
  return { status: r.status, alerts };
}
