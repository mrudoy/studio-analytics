import dotenv from "dotenv";
dotenv.config({ path: ".env" });
dotenv.config({ path: ".env.local", override: true });
dotenv.config({ path: ".env.production.local", override: true });

import pg from "pg";

async function main() {
  const pool = new pg.Pool({ connectionString: process.env.DATABASE_URL });

  const res = await pool.query(`
    SELECT
      CASE
        WHEN plan_name ILIKE '%member%' OR plan_name ILIKE '%unlimited%' THEN 'MEMBER'
        WHEN plan_name ILIKE '%sky3%' OR plan_name ILIKE '%skyhigh%' OR plan_name ILIKE '%pack%' THEN 'SKY3'
        WHEN plan_name ILIKE '%tv%' THEN 'TV'
        ELSE 'OTHER'
      END as category,
      plan_state,
      COUNT(*) as count
    FROM auto_renews
    WHERE plan_state IN ('Active', 'In Trial', 'Paused')
    GROUP BY category, plan_state
    ORDER BY category, plan_state
  `);

  console.log("\n=== Active Subscribers by Category & State ===");
  let total = 0;
  for (const row of res.rows) {
    console.log(`  ${row.category} (${row.plan_state}): ${row.count}`);
    total += parseInt(row.count);
  }
  console.log(`  TOTAL: ${total}`);

  const summary = await pool.query(`
    SELECT
      CASE
        WHEN plan_name ILIKE '%member%' OR plan_name ILIKE '%unlimited%' THEN 'MEMBER'
        WHEN plan_name ILIKE '%sky3%' OR plan_name ILIKE '%skyhigh%' OR plan_name ILIKE '%pack%' THEN 'SKY3'
        WHEN plan_name ILIKE '%tv%' THEN 'TV'
        ELSE 'OTHER'
      END as category,
      COUNT(*) as count
    FROM auto_renews
    WHERE plan_state IN ('Active', 'In Trial', 'Paused')
    GROUP BY category
    ORDER BY count DESC
  `);

  console.log("\n=== Summary by Category ===");
  for (const row of summary.rows) {
    console.log(`  ${row.category}: ${row.count}`);
  }

  await pool.end();
}

main().catch(console.error);
