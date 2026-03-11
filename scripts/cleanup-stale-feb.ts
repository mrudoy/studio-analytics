/**
 * Zero out stale duplicate Feb 2026 categories from broken pipeline runs.
 * Does NOT delete data — just sets revenue to $0 on stale rows.
 */
import dotenv from "dotenv";
import { resolve } from "path";
dotenv.config({ path: resolve(__dirname, "../.env.production.local") });
dotenv.config({ path: resolve(__dirname, "../.env.local") });
dotenv.config({ path: resolve(__dirname, "../.env") });

import { initDatabase, getPool } from "../src/lib/db/database";

async function main() {
  await initDatabase();
  const pool = getPool();

  // Get all Feb 1-28 categories
  const { rows: all } = await pool.query(`
    SELECT category, revenue FROM revenue_categories
    WHERE period_start = '2026-02-01' AND period_end = '2026-02-28'
    ORDER BY category
  `);

  const catRevs = new Map<string, number>();
  for (const r of all) {
    catRevs.set(r.category, Number(r.revenue));
  }

  const staleCats: string[] = [];

  // Find trailing-space duplicates
  for (const [cat, rev] of catRevs) {
    const trimmed = cat.trim();
    if (trimmed !== cat && catRevs.has(trimmed)) {
      staleCats.push(cat);
      console.log(`STALE (trailing space): "${cat}" -> "${trimmed}" both $${rev.toFixed(0)}`);
    }
  }

  // Find naming variant duplicates from old pipeline
  const namingVariants: [string, string][] = [
    ["SKY3 / Packs", "SKY3"],
    ["Intro / Trial", "Intro Week"],
    ["Intro / Trial", "Intro Week "],
  ];
  for (const [oldName, newName] of namingVariants) {
    if (catRevs.has(oldName) && catRevs.has(newName)) {
      staleCats.push(oldName);
      console.log(`STALE (name variant): "${oldName}" -> "${newName}"`);
    }
  }

  console.log(`\nTotal stale categories to zero: ${staleCats.length}`);
  const totalStaleRev = staleCats.reduce((s, c) => s + (catRevs.get(c) || 0), 0);
  console.log(`Revenue being zeroed: $${totalStaleRev.toFixed(0)}`);

  // Zero them out
  for (const cat of staleCats) {
    await pool.query(`
      UPDATE revenue_categories
      SET revenue = 0, union_fees = 0, stripe_fees = 0, other_fees = 0,
          transfers = 0, refunded = 0, union_fees_refunded = 0, net_revenue = 0
      WHERE period_start = '2026-02-01' AND period_end = '2026-02-28' AND category = $1
    `, [cat]);
    console.log(`  Zeroed: "${cat}"`);
  }

  // Verify dedup
  const { rows: check } = await pool.query(`
    WITH deduped AS (
      SELECT DISTINCT ON (category) category, revenue, net_revenue
      FROM revenue_categories
      WHERE TO_CHAR(period_start, 'YYYY-MM') = '2026-02'
        AND TO_CHAR(period_end, 'YYYY-MM') = '2026-02'
      ORDER BY category, period_end DESC
    )
    SELECT COUNT(*) as cnt, SUM(revenue)::numeric as gross, SUM(net_revenue)::numeric as net
    FROM deduped
  `);
  const dd = check[0];
  console.log(`\nDB dedup after cleanup: ${dd.cnt} cats, gross=$${Number(dd.gross).toFixed(0)}, net=$${Number(dd.net).toFixed(0)}`);

  const { rows: retreat } = await pool.query(`
    WITH deduped AS (
      SELECT DISTINCT ON (category) category, revenue
      FROM revenue_categories
      WHERE TO_CHAR(period_start, 'YYYY-MM') = '2026-02'
        AND TO_CHAR(period_end, 'YYYY-MM') = '2026-02'
      ORDER BY category, period_end DESC
    )
    SELECT SUM(revenue)::numeric as gross FROM deduped
    WHERE category ~* 'retreat' AND NOT category ~* 'retreat\\s*ting'
  `);
  const retreatGross = Number(retreat[0]?.gross || 0);
  console.log(`Retreat: $${retreatGross.toFixed(0)}`);
  console.log(`Dashboard (gross - retreat): $${(Number(dd.gross) - retreatGross).toFixed(0)}`);

  // Bump cache
  await pool.query("UPDATE data_version SET version = version + 1, updated_at = NOW() WHERE id = 1");
  console.log("Cache version bumped.");

  process.exit(0);
}
main().catch((e) => { console.error(e); process.exit(1); });
