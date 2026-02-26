const { Pool } = require("pg");
const Papa = require("papaparse");
const fs = require("fs");

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
});

const months = [
  { file: "/Users/mike.rudoy_old/Downloads/jan.csv", start: "2024-01-01", end: "2024-01-31" },
  { file: "/Users/mike.rudoy_old/Downloads/feb.csv", start: "2024-02-01", end: "2024-02-29" },
  { file: "/Users/mike.rudoy_old/Downloads/march.csv", start: "2024-03-01", end: "2024-03-31" },
  { file: "/Users/mike.rudoy_old/Downloads/april.csv", start: "2024-04-01", end: "2024-04-30" },
  { file: "/Users/mike.rudoy_old/Downloads/may.csv", start: "2024-05-01", end: "2024-05-31" },
  { file: "/Users/mike.rudoy_old/Downloads/june.csv", start: "2024-06-01", end: "2024-06-30" },
  { file: "/Users/mike.rudoy_old/Downloads/july.csv", start: "2024-07-01", end: "2024-07-31" },
  { file: "/Users/mike.rudoy_old/Downloads/union-revenue-categories-sky-ting-20260218-2039.csv", start: "2024-08-01", end: "2024-08-31" },
  { file: "/Users/mike.rudoy_old/Downloads/union-revenue-categories-sky-ting-20260218-2038.csv", start: "2024-09-01", end: "2024-09-30" },
  { file: "/Users/mike.rudoy_old/Downloads/oct.csv", start: "2024-10-01", end: "2024-10-31" },
  // nov.csv is byte-identical to oct.csv — skipping to avoid double-count. Nov is MISSING.
  { file: "/Users/mike.rudoy_old/Downloads/dec.csv", start: "2024-12-01", end: "2024-12-31" },
];

async function importAll() {
  const client = await pool.connect();
  let totalRows = 0;

  try {
    await client.query("BEGIN");

    for (const m of months) {
      const csv = fs.readFileSync(m.file, "utf8");
      const parsed = Papa.parse(csv, { header: true, skipEmptyLines: true });
      let monthRows = 0;

      for (const row of parsed.data) {
        const cat = row.revenue_category;
        if (!cat) continue;

        const revenue = parseFloat(row.revenue) || 0;
        const unionFees = parseFloat(row.union_fees) || 0;
        const stripeFees = parseFloat(row.stripe_fees) || 0;
        const otherFees = parseFloat(row.other_fees) || 0;
        const refunded = parseFloat(row.refunded) || 0;
        const refundedUnionFees = parseFloat(row.refunded_union_fees) || 0;
        const netRevenue = parseFloat(row.net_revenue) || 0;

        await client.query(
          `INSERT INTO revenue_categories
           (period_start, period_end, category, revenue, union_fees, stripe_fees, other_fees, transfers, refunded, union_fees_refunded, net_revenue)
           VALUES ($1, $2, $3, $4, $5, $6, $7, 0, $8, $9, $10)
           ON CONFLICT(period_start, period_end, category)
           DO UPDATE SET
             revenue = EXCLUDED.revenue,
             union_fees = EXCLUDED.union_fees,
             stripe_fees = EXCLUDED.stripe_fees,
             other_fees = EXCLUDED.other_fees,
             refunded = EXCLUDED.refunded,
             union_fees_refunded = EXCLUDED.union_fees_refunded,
             net_revenue = EXCLUDED.net_revenue,
             created_at = NOW()`,
          [m.start, m.end, cat, revenue, unionFees, stripeFees, otherFees, refunded, refundedUnionFees, netRevenue]
        );
        monthRows++;
      }

      totalRows += monthRows;
      console.log(`  ${m.start}: ${monthRows} categories imported`);
    }

    await client.query("COMMIT");
    console.log(`\nTotal: ${totalRows} rows imported across ${months.length} months`);
    console.log("Note: November 2024 is MISSING (oct.csv and nov.csv were byte-identical)");
  } catch (e) {
    await client.query("ROLLBACK");
    console.error("ROLLBACK:", e.message);
  } finally {
    client.release();
    pool.end();
  }
}

importAll();
