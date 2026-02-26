import dotenv from "dotenv";
dotenv.config({ path: ".env" });
dotenv.config({ path: ".env.local", override: true });
dotenv.config({ path: ".env.production.local", override: true });

import pg from "pg";

async function main() {
  const pool = new pg.Pool({ connectionString: process.env.DATABASE_URL });

  // Plan prices from auto_renews
  const plans = await pool.query(`
    SELECT plan_name, plan_price, COUNT(*) as count
    FROM auto_renews
    WHERE plan_state IN ('Active', 'In Trial', 'Paused')
    GROUP BY plan_name, plan_price
    ORDER BY count DESC
    LIMIT 25
  `);
  console.log("=== Active Plan Prices ===");
  for (const row of plans.rows) {
    console.log(`  ${row.plan_name}: $${row.plan_price}/mo (${row.count} subscribers)`);
  }

  // Drop-in revenue per visit (from orders)
  const dropins = await pool.query(`
    SELECT AVG(total) as avg_price, COUNT(*) as count
    FROM orders
    WHERE total > 0 AND total < 50
    AND (customer IS NOT NULL)
  `);
  console.log(`\n=== Drop-in Estimate ===`);
  console.log(`  Avg order under $50: $${Number(dropins.rows[0].avg_price).toFixed(2)} (${dropins.rows[0].count} orders)`);

  // Registration types breakdown
  const regTypes = await pool.query(`
    SELECT registration_type, COUNT(*) as count
    FROM registrations
    WHERE attended_at >= '2026-01-01'
    GROUP BY registration_type
    ORDER BY count DESC
  `);
  console.log(`\n=== Registration Types (2026) ===`);
  for (const row of regTypes.rows) {
    console.log(`  ${row.registration_type || '(empty)'}: ${row.count}`);
  }

  // Repeat visitors (non-members doing drop-ins)
  const repeats = await pool.query(`
    SELECT email, COUNT(*) as visit_count
    FROM registrations
    WHERE attended_at >= '2026-01-01'
    AND email IS NOT NULL AND email != ''
    GROUP BY email
    HAVING COUNT(*) >= 4
    ORDER BY visit_count DESC
    LIMIT 20
  `);
  console.log(`\n=== Top Repeat Visitors in 2026 (4+ visits) ===`);
  console.log(`  Total people with 4+ visits: ${repeats.rows.length}+`);
  for (const row of repeats.rows.slice(0, 10)) {
    console.log(`  ${row.email}: ${row.visit_count} visits`);
  }

  // How many people have 4+ visits but no auto-renew?
  const convertible = await pool.query(`
    SELECT COUNT(DISTINCT r.email) as count
    FROM registrations r
    LEFT JOIN auto_renews ar ON LOWER(r.email) = LOWER(ar.customer_email)
      AND ar.plan_state IN ('Active', 'In Trial')
    WHERE r.attended_at >= '2026-01-01'
    AND r.email IS NOT NULL AND r.email != ''
    AND ar.customer_email IS NULL
    GROUP BY r.email
    HAVING COUNT(*) >= 4
  `);
  console.log(`\n=== Convertible: 4+ visits in 2026 but NO active subscription ===`);
  console.log(`  Count: ${convertible.rows.length} people`);

  await pool.end();
}

main().catch(console.error);
