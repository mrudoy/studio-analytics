/**
 * Restore Feb 2026 revenue from the JSON backup file.
 * Reads backup, extracts Feb 2026 rows, upserts into DB.
 * Does NOT delete anything — pure additive restore.
 *
 * Usage:
 *   npx tsx scripts/restore-from-backup.ts
 */

import dotenv from "dotenv";
import { resolve } from "path";
import fs from "fs";

dotenv.config({ path: resolve(__dirname, "../.env.production.local") });
dotenv.config({ path: resolve(__dirname, "../.env.local") });
dotenv.config({ path: resolve(__dirname, "../.env") });

import { initDatabase, getPool } from "../src/lib/db/database";
import { runMigrations } from "../src/lib/db/migrations";

const BACKUP_PATH = resolve(
  __dirname,
  "../data/backups/backup-2026-02-24_19-57-18-167.json"
);

async function main() {
  console.log("\n=== Restore Feb 2026 from Backup ===\n");

  const raw = fs.readFileSync(BACKUP_PATH, "utf8");
  const data = JSON.parse(raw);
  const allRows = data.tables.revenue_categories as Array<Record<string, unknown>>;
  const feb = allRows.filter(
    (r) => r.period_start && String(r.period_start).startsWith("2026-02")
  );

  console.log(`Backup has ${feb.length} Feb 2026 rows`);

  await initDatabase();
  await runMigrations();

  const pool = getPool();

  // Pure upsert — NEVER delete
  let inserted = 0;
  let updated = 0;
  for (const r of feb) {
    const res = await pool.query(
      `INSERT INTO revenue_categories
       (period_start, period_end, category, revenue, union_fees, stripe_fees, other_fees, transfers, refunded, union_fees_refunded, net_revenue, locked)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
       ON CONFLICT (period_start, period_end, category) DO UPDATE SET
         revenue = EXCLUDED.revenue,
         union_fees = EXCLUDED.union_fees,
         stripe_fees = EXCLUDED.stripe_fees,
         other_fees = EXCLUDED.other_fees,
         transfers = EXCLUDED.transfers,
         refunded = EXCLUDED.refunded,
         union_fees_refunded = EXCLUDED.union_fees_refunded,
         net_revenue = EXCLUDED.net_revenue`,
      [
        r.period_start,
        r.period_end,
        r.category,
        r.revenue,
        r.union_fees,
        r.stripe_fees,
        r.other_fees || 0,
        r.transfers || 0,
        r.refunded,
        r.union_fees_refunded,
        r.net_revenue,
        r.locked || 0,
      ]
    );
    // If xmin = current txid, it was inserted. Otherwise updated.
    if (res.rowCount && res.rowCount > 0) {
      inserted++;
    }
  }

  console.log(`Upserted ${inserted} rows from backup`);

  // Verify
  const check = await pool.query(
    `SELECT COUNT(*) as cnt, SUM(revenue)::numeric as gross, SUM(net_revenue)::numeric as net
     FROM revenue_categories
     WHERE TO_CHAR(period_start, 'YYYY-MM') = '2026-02'`
  );
  const row = check.rows[0];
  console.log(
    `\nFeb 2026 now: ${row.cnt} rows, gross=$${Number(row.gross).toFixed(0)}, net=$${Number(row.net).toFixed(0)}`
  );

  // Show by period range
  const ranges = await pool.query(
    `SELECT period_start, period_end, COUNT(*) as cnt, SUM(revenue)::numeric as gross
     FROM revenue_categories
     WHERE TO_CHAR(period_start, 'YYYY-MM') = '2026-02'
     GROUP BY period_start, period_end
     ORDER BY period_end DESC`
  );
  for (const rr of ranges.rows) {
    console.log(
      `  ${rr.period_start} to ${rr.period_end}: ${rr.cnt} categories, gross=$${Number(rr.gross).toFixed(0)}`
    );
  }

  // Bump cache version
  await pool.query(
    "UPDATE data_version SET version = version + 1, updated_at = NOW() WHERE id = 1"
  );
  console.log("Cache version bumped.");

  // Show what the dashboard dedup will show
  const deduped = await pool.query(
    `WITH deduped AS (
       SELECT DISTINCT ON (category)
         category, revenue, net_revenue
       FROM revenue_categories
       WHERE TO_CHAR(period_start, 'YYYY-MM') = '2026-02'
         AND TO_CHAR(period_end, 'YYYY-MM') = '2026-02'
       ORDER BY category, period_end DESC
     )
     SELECT COUNT(*) as cnt, SUM(revenue)::numeric as gross, SUM(net_revenue)::numeric as net
     FROM deduped`
  );
  const dd = deduped.rows[0];
  console.log(
    `\nDashboard dedup view: ${dd.cnt} categories, gross=$${Number(dd.gross).toFixed(0)}, net=$${Number(dd.net).toFixed(0)}`
  );

  // Show retreat deduction
  const retreat = await pool.query(
    `WITH deduped AS (
       SELECT DISTINCT ON (category)
         category, revenue, net_revenue
       FROM revenue_categories
       WHERE TO_CHAR(period_start, 'YYYY-MM') = '2026-02'
         AND TO_CHAR(period_end, 'YYYY-MM') = '2026-02'
       ORDER BY category, period_end DESC
     )
     SELECT SUM(revenue)::numeric as gross
     FROM deduped
     WHERE category ~* 'retreat' AND NOT category ~* 'retreat\\s*ting'`
  );
  const retreatGross = Number(retreat.rows[0]?.gross || 0);
  console.log(`Retreat gross: $${retreatGross.toFixed(0)}`);
  console.log(`Dashboard total (gross - retreat): $${(Number(dd.gross) - retreatGross).toFixed(0)}`);

  console.log("\nDone!");
  process.exit(0);
}

main().catch((err) => {
  console.error("Failed:", err);
  process.exit(1);
});
