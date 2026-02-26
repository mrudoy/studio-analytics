const { Pool } = require("pg");
const Papa = require("papaparse");
const fs = require("fs");
const pool = new Pool({ connectionString: process.env.DATABASE_URL, ssl: { rejectUnauthorized: false } });

async function run() {
  const csv = fs.readFileSync("/Users/mike.rudoy_old/Downloads/union-revenue-categories-sky-ting-20260218-2041.csv", "utf8");
  const parsed = Papa.parse(csv, { header: true, skipEmptyLines: true });
  const client = await pool.connect();
  let count = 0;
  try {
    await client.query("BEGIN");
    for (const row of parsed.data) {
      const cat = row.revenue_category;
      if (!cat) continue;
      await client.query(
        `INSERT INTO revenue_categories
         (period_start, period_end, category, revenue, union_fees, stripe_fees, other_fees, transfers, refunded, union_fees_refunded, net_revenue)
         VALUES ($1, $2, $3, $4, $5, $6, $7, 0, $8, $9, $10)
         ON CONFLICT(period_start, period_end, category)
         DO UPDATE SET revenue = EXCLUDED.revenue, union_fees = EXCLUDED.union_fees, stripe_fees = EXCLUDED.stripe_fees,
           other_fees = EXCLUDED.other_fees, refunded = EXCLUDED.refunded, union_fees_refunded = EXCLUDED.union_fees_refunded,
           net_revenue = EXCLUDED.net_revenue, created_at = NOW()`,
        ["2024-11-01", "2024-11-30", cat,
         parseFloat(row.revenue)||0, parseFloat(row.union_fees)||0, parseFloat(row.stripe_fees)||0,
         parseFloat(row.other_fees)||0, parseFloat(row.refunded)||0, parseFloat(row.refunded_union_fees)||0,
         parseFloat(row.net_revenue)||0]
      );
      count++;
    }
    await client.query("COMMIT");
    console.log("November 2024: " + count + " categories imported");
  } catch(e) { await client.query("ROLLBACK"); console.error(e.message); }
  finally { client.release(); }

  // Verify full 2024 monthly totals
  const r = await pool.query(`
    SELECT period_start, SUM(net_revenue) as net, COUNT(*) as cats
    FROM revenue_categories
    WHERE period_start LIKE '2024%' AND LEFT(period_start,7) = LEFT(period_end,7)
    GROUP BY period_start ORDER BY period_start
  `);
  console.log("\n2024 monthly data now in DB:");
  let total = 0;
  r.rows.forEach(row => {
    total += Number(row.net);
    console.log("  " + row.period_start + ": " + row.cats + " cats, net=$" + Math.round(Number(row.net)).toLocaleString());
  });
  console.log("  TOTAL: $" + Math.round(total).toLocaleString());

  // Compare to full-year row
  const fy = await pool.query(`
    SELECT SUM(net_revenue) as net FROM revenue_categories
    WHERE period_start = '2024-01-01' AND period_end = '2024-12-31'
  `);
  const fyNet = Number(fy.rows[0].net);
  console.log("\nFull-year row net: $" + Math.round(fyNet).toLocaleString());
  console.log("Difference (monthly sum - full year): $" + Math.round(total - fyNet).toLocaleString());

  pool.end();
}
run();
