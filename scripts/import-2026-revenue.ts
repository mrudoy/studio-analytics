/**
 * Import corrected 2026 revenue to prod.
 * Uses pg directly to avoid ESM/CJS import issues with tsx.
 */
import { Pool } from "pg";
import Papa from "papaparse";
import { readFileSync } from "fs";

const CSV_PATH = "/tmp/revenue-2026-corrected.csv";
const PERIOD_START = "2026-01-01";
const PERIOD_END = "2026-02-18";

async function main() {
  const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: { rejectUnauthorized: false },
  });

  // Parse CSV
  const content = readFileSync(CSV_PATH, "utf8");
  const parsed = Papa.parse(content, {
    header: true,
    skipEmptyLines: true,
    dynamicTyping: true,
  });

  const rows = parsed.data as Record<string, unknown>[];
  console.log("Parsed", rows.length, "rows");
  console.log("First row:", JSON.stringify(rows[0]));

  // Save each row
  let saved = 0;
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    for (const r of rows) {
      const category = r["Revenue Category"] as string;
      const revenue = Number(r["Revenue"]) || 0;
      const unionFees = Number(r["Union Fees"]) || 0;
      const stripeFees = Number(r["Stripe Fees"]) || 0;
      const otherFees = Number(r["Other Fees"]) || 0;
      const refunded = Number(r["Refunded"]) || 0;
      const unionFeesRefunded = Number(r["Refunded Union Fees"]) || 0;
      const netRevenue = Number(r["Net Revenue"]) || 0;

      await client.query(
        `INSERT INTO revenue_categories (period_start, period_end, category, revenue, union_fees, stripe_fees, other_fees, refunded, union_fees_refunded, net_revenue)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
         ON CONFLICT (period_start, period_end, category) DO UPDATE SET
           revenue = EXCLUDED.revenue,
           union_fees = EXCLUDED.union_fees,
           stripe_fees = EXCLUDED.stripe_fees,
           other_fees = EXCLUDED.other_fees,
           refunded = EXCLUDED.refunded,
           union_fees_refunded = EXCLUDED.union_fees_refunded,
           net_revenue = EXCLUDED.net_revenue`,
        [PERIOD_START, PERIOD_END, category, revenue, unionFees, stripeFees, otherFees, refunded, unionFeesRefunded, netRevenue]
      );
      saved++;
    }
    await client.query("COMMIT");
  } catch (e) {
    await client.query("ROLLBACK");
    throw e;
  } finally {
    client.release();
  }

  console.log("Saved", saved, "revenue categories");

  // Update watermark
  await pool.query(
    `INSERT INTO fetch_watermarks (report_type, last_fetched_at, high_water_date, record_count, notes)
     VALUES ('revenueCategories', NOW(), $1, $2, $3)
     ON CONFLICT (report_type) DO UPDATE SET
       last_fetched_at = NOW(),
       high_water_date = EXCLUDED.high_water_date,
       record_count = EXCLUDED.record_count,
       notes = EXCLUDED.notes`,
    [PERIOD_END, saved, "corrected 2026 import"]
  );

  // Verify
  const check = await pool.query(
    "SELECT period_start, period_end, COUNT(*) as cats, SUM(revenue)::numeric(12,2) as gross, SUM(net_revenue)::numeric(12,2) as net FROM revenue_categories GROUP BY period_start, period_end ORDER BY period_start"
  );
  console.log("\nRevenue in DB:");
  for (const r of check.rows) {
    console.log(`  ${r.period_start} to ${r.period_end}: ${r.cats} cats, $${Number(r.gross).toLocaleString()} gross, $${Number(r.net).toLocaleString()} net`);
  }

  await pool.end();
  console.log("\nDone!");
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
