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
 *   - last full reconcile age                   → residual drift is unbounded.
 *   - active-total jump vs the previous run      → anomaly.
 *
 * Each run is recorded in `drift_checks` and surfaced in the digest.
 */
import { getPool } from "./database";
import { ACTIVE_STATES_SQL } from "../analytics/metrics/filters";
import { getReconcileHealth } from "./reconcile";

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

  const counts = await pool.query(
    `SELECT plan_category, COUNT(*)::int n FROM auto_renews WHERE ${activeFilter} GROUP BY 1`,
  );
  let activeMember = 0, activeSky3 = 0, activeTv = 0, activeTotal = 0;
  for (const r of counts.rows as { plan_category: string; n: number }[]) {
    if (r.plan_category === "MEMBER") activeMember = r.n;
    else if (r.plan_category === "SKY3") activeSky3 = r.n;
    else if (r.plan_category === "SKY_TING_TV") activeTv = r.n;
    activeTotal += r.n;
  }

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
    dupActiveIdentities, futureDatedRows, lastFullSyncAgeDays,
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
