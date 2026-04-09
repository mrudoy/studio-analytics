/**
 * Transfer store — persists Union.fit transfer data for DB-based revenue computation.
 *
 * Transfers (video usage debits, billing charges) are upserted by their Union.fit ID.
 * They all go to the "Uncategorized" revenue category bucket.
 */

import { getPool } from "./database";

export interface TransferRow {
  id: string;
  createdAt: string;
  payoutTotal: number;
  description: string;
  type: string;
}

/**
 * Upsert transfers (additive — inserts new rows, updates existing).
 */
export async function saveTransfers(rows: TransferRow[]): Promise<void> {
  if (rows.length === 0) return;
  const pool = getPool();
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    for (const r of rows) {
      await client.query(
        `INSERT INTO transfers (id, created_at, payout_total, description, type)
         VALUES ($1, $2, $3, $4, $5)
         ON CONFLICT (id) DO UPDATE SET
           payout_total = EXCLUDED.payout_total,
           description = COALESCE(EXCLUDED.description, transfers.description),
           imported_at = NOW()`,
        [
          r.id,
          r.createdAt || null,
          r.payoutTotal,
          r.description || null,
          r.type || null,
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
  console.log(`[transfer-store] Saved ${rows.length} transfers`);
}
