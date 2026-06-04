import { getPool } from "./database";
import type { ShadowCancel } from "../email/zip-transformer";

/**
 * Phase B1 shadow mode: record the cancellations the daily delta implies,
 * WITHOUT touching auto_renews. One row per union_pass_id, upserted to its
 * latest observation. Compare against the next full export to measure false
 * positives before enabling real cancellation writes.
 */
export async function logDeltaCancelShadow(
  rows: ShadowCancel[],
  snapshotId: string,
): Promise<{ logged: number }> {
  if (rows.length === 0) return { logged: 0 };
  const pool = getPool();
  const client = await pool.connect();
  let logged = 0;
  try {
    await client.query("BEGIN");
    for (const r of rows) {
      await client.query(
        `INSERT INTO delta_cancel_shadow
           (union_pass_id, plan_name, customer_email, plan_category, raw_state,
            auto_renew_off, intended_action, effective_at, snapshot_id, observed_at)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,NOW())
         ON CONFLICT (union_pass_id) DO UPDATE SET
           plan_name = EXCLUDED.plan_name,
           customer_email = EXCLUDED.customer_email,
           plan_category = EXCLUDED.plan_category,
           raw_state = EXCLUDED.raw_state,
           auto_renew_off = EXCLUDED.auto_renew_off,
           intended_action = EXCLUDED.intended_action,
           effective_at = EXCLUDED.effective_at,
           snapshot_id = EXCLUDED.snapshot_id,
           observed_at = NOW()`,
        [
          r.unionPassId,
          r.planName,
          r.customerEmail.toLowerCase(),
          r.category,
          r.rawState,
          r.autoRenewOff,
          r.intendedAction,
          r.effectiveAt,
          snapshotId,
        ],
      );
      logged++;
    }
    await client.query("COMMIT");
  } catch (e) {
    await client.query("ROLLBACK");
    throw e;
  } finally {
    client.release();
  }
  console.log(`[shadow-cancel] Logged ${logged} implied delta cancellations (shadow only, no writes)`);
  return { logged };
}

export interface ShadowCancelImpact {
  /** Shadow rows whose union_pass_id maps to a currently-ACTIVE auto_renews row. */
  wouldFlipActive: number;
  /** ...of those, how many the intended action would CANCEL (vs pending_cancel). */
  wouldCancel: number;
  wouldPendingCancel: number;
}

/**
 * How many currently-active auto_renews rows the shadow log would have flipped.
 * Use to gauge B1's impact before enabling writes.
 */
export async function getShadowCancelImpact(): Promise<ShadowCancelImpact> {
  const pool = getPool();
  const { rows } = await pool.query(
    `SELECT s.intended_action, COUNT(*) AS n
       FROM delta_cancel_shadow s
       JOIN auto_renews ar ON ar.union_pass_id = s.union_pass_id
      WHERE ar.plan_state IN ('Valid Now','Paused','Pending Cancel','In Trial','Invalid','Past Due')
        AND (ar.current_state IS NULL OR ar.current_state = 'active')
      GROUP BY s.intended_action`,
  );
  let wouldCancel = 0;
  let wouldPendingCancel = 0;
  for (const r of rows as { intended_action: string; n: string }[]) {
    if (r.intended_action === "cancel") wouldCancel = Number(r.n);
    else if (r.intended_action === "pending_cancel") wouldPendingCancel = Number(r.n);
  }
  return {
    wouldFlipActive: wouldCancel + wouldPendingCancel,
    wouldCancel,
    wouldPendingCancel,
  };
}
