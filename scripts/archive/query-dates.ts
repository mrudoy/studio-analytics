import dotenv from "dotenv";
dotenv.config({ path: ".env" });
dotenv.config({ path: ".env.local", override: true });
dotenv.config({ path: ".env.production.local", override: true });

import pg from "pg";

async function main() {
  const pool = new pg.Pool({ connectionString: process.env.DATABASE_URL });

  console.log("=== Data Date Ranges in Prod DB ===\n");

  // Orders
  const orders = await pool.query(`
    SELECT MIN(created_at) as earliest, MAX(created_at) as latest, COUNT(*) as total,
           COUNT(*) FILTER (WHERE created_at >= '2026-02-01') as feb_2026
    FROM orders
  `);
  console.log(`Orders: ${orders.rows[0].total} total, ${orders.rows[0].feb_2026} in Feb 2026`);
  console.log(`  Range: ${orders.rows[0].earliest} to ${orders.rows[0].latest}`);

  // First visits
  const fv = await pool.query(`
    SELECT MIN(attended_at) as earliest, MAX(attended_at) as latest, COUNT(*) as total,
           COUNT(*) FILTER (WHERE attended_at >= '2026-02-01') as feb_2026
    FROM first_visits
  `);
  console.log(`\nFirst Visits: ${fv.rows[0].total} total, ${fv.rows[0].feb_2026} in Feb 2026`);
  console.log(`  Range: ${fv.rows[0].earliest} to ${fv.rows[0].latest}`);

  // New customers
  const cust = await pool.query(`
    SELECT MIN(created_at) as earliest, MAX(created_at) as latest, COUNT(*) as total,
           COUNT(*) FILTER (WHERE created_at >= '2026-02-01') as feb_2026
    FROM new_customers
  `);
  console.log(`\nNew Customers: ${cust.rows[0].total} total, ${cust.rows[0].feb_2026} in Feb 2026`);
  console.log(`  Range: ${cust.rows[0].earliest} to ${cust.rows[0].latest}`);

  // Registrations
  const reg = await pool.query(`
    SELECT MIN(attended_at) as earliest, MAX(attended_at) as latest, COUNT(*) as total,
           COUNT(*) FILTER (WHERE attended_at >= '2026-02-01') as feb_2026
    FROM registrations
  `);
  console.log(`\nRegistrations: ${reg.rows[0].total} total, ${reg.rows[0].feb_2026} in Feb 2026`);
  console.log(`  Range: ${reg.rows[0].earliest} to ${reg.rows[0].latest}`);

  // Auto-renews (new)
  const ar = await pool.query(`
    SELECT MIN(created_at) as earliest, MAX(created_at) as latest, COUNT(*) as total,
           COUNT(*) FILTER (WHERE created_at >= '2026-02-01') as feb_2026
    FROM auto_renews
    WHERE snapshot_id LIKE 'pipeline-%'
  `);
  console.log(`\nAuto-Renews: ${ar.rows[0].total} total, ${ar.rows[0].feb_2026} in Feb 2026`);
  console.log(`  Range: ${ar.rows[0].earliest} to ${ar.rows[0].latest}`);

  // Revenue categories
  const rev = await pool.query(`
    SELECT period_start, period_end, COUNT(DISTINCT category) as categories,
           SUM(net_revenue) as total_net
    FROM revenue_categories
    GROUP BY period_start, period_end
    ORDER BY period_start DESC
    LIMIT 5
  `);
  console.log(`\nRevenue Category Periods (latest 5):`);
  for (const row of rev.rows) {
    console.log(`  ${row.period_start} to ${row.period_end}: ${row.categories} categories, $${Number(row.total_net).toFixed(2)} net`);
  }

  // Pipeline runs
  const runs = await pool.query(`
    SELECT id, date_range_start, date_range_end, created_at, duration_ms
    FROM pipeline_runs
    ORDER BY created_at DESC
    LIMIT 5
  `);
  console.log(`\nLatest Pipeline Runs:`);
  for (const row of runs.rows) {
    console.log(`  ${row.created_at}: ${row.date_range_start} to ${row.date_range_end} (${(row.duration_ms/1000).toFixed(1)}s)`);
  }

  await pool.end();
}

main().catch(console.error);
