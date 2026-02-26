import { parse } from "papaparse";
import { readFileSync } from "fs";
import { Pool } from "pg";

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
});

const files = [
  "data/downloads/canceledAutoRenews-1771362559453.csv",
  "data/downloads/pausedAutoRenews-1771362593341.csv",
  "data/downloads/newAutoRenews-1771271208049.csv",
  "data/downloads/trialingAutoRenews-1771292017557.csv",
];

async function main() {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    await client.query("DELETE FROM auto_renews");

    let total = 0;
    const snapshotId = "manual-upload-" + new Date().toISOString().slice(0, 10);

    for (const f of files) {
      const csv = readFileSync(f, "utf8");
      const result = parse(csv, { header: true, skipEmptyLines: true });

      let fileCount = 0;
      for (const row of result.data as Record<string, string>[]) {
        if (!row.subscription_name) continue;

        await client.query(
          `INSERT INTO auto_renews (snapshot_id, plan_name, plan_state, plan_price, customer_name, customer_email, created_at, order_id, sales_channel, canceled_at, canceled_by, admin, current_state, current_plan)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)`,
          [
            snapshotId,
            row.subscription_name || "",
            row.subscription_state || "",
            parseFloat((row.subscription_price || "0").replace(/[^0-9.]/g, "")) || 0,
            row.customer_name || "",
            row.customer_email || "",
            row.created_at || "",
            row.order_id || null,
            row.sales_channel || null,
            row.canceled_at || null,
            row.canceled_by || null,
            row.admin || null,
            row.current_state || null,
            row.current_subscription || null,
          ]
        );
        fileCount++;
      }
      total += fileCount;
      console.log(`${f.split("/").pop()}: ${fileCount} rows`);
    }

    await client.query("COMMIT");
    console.log(`DONE. Total: ${total}`);

    const cnt = await client.query("SELECT COUNT(*) as c FROM auto_renews");
    console.log(`Verified: ${cnt.rows[0].c} rows`);
    const states = await client.query(
      "SELECT plan_state, COUNT(*) as c FROM auto_renews GROUP BY plan_state ORDER BY c DESC"
    );
    for (const r of states.rows) {
      console.log(`  ${r.plan_state}: ${r.c}`);
    }
    const sample = await client.query(
      "SELECT plan_name, plan_state, plan_price, created_at, canceled_at FROM auto_renews LIMIT 3"
    );
    console.log("Sample:", JSON.stringify(sample.rows, null, 2));
  } catch (e) {
    await client.query("ROLLBACK");
    console.error("ERROR:", (e as Error).message);
  } finally {
    client.release();
    await pool.end();
  }
}
main();
