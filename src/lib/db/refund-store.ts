/**
 * Refund store — persists Union.fit refund data for DB-based revenue computation.
 *
 * Refunds are upserted by their Union.fit ID. The revenue_category is resolved
 * during import using cached lookup tables (revenue_category_id → name).
 */

import { getPool } from "./database";

export interface RefundRow {
  id: string;
  createdAt: string;
  orderId: string;
  revenueCategoryId: string;
  revenueCategory: string;
  state: string;
  amountRefunded: number;
  feeUnionTotalRefunded: number;
  payoutTotal: number;
  toBalance: boolean;
  reason: string;
}

/**
 * Upsert refunds (additive — inserts new rows, updates existing).
 */
export async function saveRefunds(rows: RefundRow[]): Promise<void> {
  if (rows.length === 0) return;
  const pool = getPool();
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    for (const r of rows) {
      await client.query(
        `INSERT INTO refunds (id, created_at, order_id, revenue_category_id, revenue_category,
           state, amount_refunded, fee_union_total_refunded, payout_total, to_balance, reason)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
         ON CONFLICT (id) DO UPDATE SET
           revenue_category = COALESCE(EXCLUDED.revenue_category, refunds.revenue_category),
           state = EXCLUDED.state,
           amount_refunded = EXCLUDED.amount_refunded,
           fee_union_total_refunded = EXCLUDED.fee_union_total_refunded,
           payout_total = EXCLUDED.payout_total,
           to_balance = EXCLUDED.to_balance,
           imported_at = NOW()`,
        [
          r.id,
          r.createdAt || null,
          r.orderId || null,
          r.revenueCategoryId || null,
          r.revenueCategory || null,
          r.state || null,
          r.amountRefunded,
          r.feeUnionTotalRefunded,
          r.payoutTotal,
          r.toBalance,
          r.reason || null,
        ]
      );
    }
    await client.query("COMMIT");
  } catch (e) {
    await client.query("ROLLBACK");
    throw e;
  } finally {
    client.release();
  }
  console.log(`[refund-store] Saved ${rows.length} refunds`);
}
