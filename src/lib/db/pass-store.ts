/**
 * Pass Store — Persists Union.fit pass data for DB-based revenue computation.
 *
 * The 7-step revenue algorithm requires passes for:
 *   - Step 2: Non-subscription pass revenue (uses pass.total, not order.total)
 *   - Step 3: Excluding pass orders from non-pass order revenue
 *   - Step 4A: Pass-linked refunds (uses pass.total, not refund.amount_refunded)
 *
 * Passes accumulate over time via upsert (ON CONFLICT DO UPDATE).
 */

import { getPool } from "./database";

export interface PassRow {
  id: string;
  passCategoryName: string;
  orderId: string;
  refundId: string;
  passTypeId: string;
  name: string;
  total: number;
  feeUnionTotal: number;
  feePaymentTotal: number;
  feesOutside: boolean;
  membershipId: string;
  state: string;
}

/**
 * Upsert passes to the database.
 * Uses batched INSERT ... ON CONFLICT DO UPDATE for efficiency.
 */
export async function savePasses(rows: PassRow[]): Promise<void> {
  if (rows.length === 0) return;

  const pool = getPool();
  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    // Batch insert in chunks of 500
    const BATCH_SIZE = 500;
    for (let i = 0; i < rows.length; i += BATCH_SIZE) {
      const batch = rows.slice(i, i + BATCH_SIZE);
      const values: unknown[] = [];
      const placeholders: string[] = [];

      for (let j = 0; j < batch.length; j++) {
        const r = batch[j];
        const offset = j * 12;
        placeholders.push(
          `($${offset + 1}, $${offset + 2}, $${offset + 3}, $${offset + 4}, $${offset + 5}, $${offset + 6}, $${offset + 7}, $${offset + 8}, $${offset + 9}, $${offset + 10}, $${offset + 11}, $${offset + 12})`
        );
        values.push(
          r.id,
          r.passCategoryName || null,
          r.orderId || null,
          r.refundId || null,
          r.passTypeId || null,
          r.name || null,
          r.total ?? null,
          r.feeUnionTotal ?? null,
          r.feePaymentTotal ?? null,
          r.feesOutside ?? null,
          r.membershipId || null,
          r.state || null
        );
      }

      await client.query(
        `INSERT INTO passes (id, pass_category_name, order_id, refund_id, pass_type_id, name, total, fee_union_total, fee_payment_total, fees_outside, membership_id, state)
         VALUES ${placeholders.join(", ")}
         ON CONFLICT (id) DO UPDATE SET
           pass_category_name = COALESCE(NULLIF(EXCLUDED.pass_category_name, ''), passes.pass_category_name),
           order_id = COALESCE(NULLIF(EXCLUDED.order_id, ''), passes.order_id),
           refund_id = COALESCE(NULLIF(EXCLUDED.refund_id, ''), passes.refund_id),
           pass_type_id = COALESCE(NULLIF(EXCLUDED.pass_type_id, ''), passes.pass_type_id),
           name = COALESCE(NULLIF(EXCLUDED.name, ''), passes.name),
           total = COALESCE(EXCLUDED.total, passes.total),
           fee_union_total = COALESCE(EXCLUDED.fee_union_total, passes.fee_union_total),
           fee_payment_total = COALESCE(EXCLUDED.fee_payment_total, passes.fee_payment_total),
           fees_outside = COALESCE(EXCLUDED.fees_outside, passes.fees_outside),
           membership_id = COALESCE(NULLIF(EXCLUDED.membership_id, ''), passes.membership_id),
           state = COALESCE(NULLIF(EXCLUDED.state, ''), passes.state),
           imported_at = NOW()`,
        values
      );
    }

    await client.query("COMMIT");
    console.log(`[pass-store] Upserted ${rows.length} passes`);
  } catch (err) {
    await client.query("ROLLBACK");
    throw err;
  } finally {
    client.release();
  }
}
