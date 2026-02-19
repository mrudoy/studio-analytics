/**
 * Quick check: are credentials + DB ready for the backfill?
 */
import * as dotenv from "dotenv";
dotenv.config();

import { loadSettings } from "../src/lib/crypto/credentials";
import { getPool, initDatabase } from "../src/lib/db/database";

async function main() {
  // Check credentials
  const s = loadSettings();
  const hasCreds = s && s.credentials ? true : false;
  const hasRobot = s && s.robotEmail && s.robotEmail.address ? true : false;
  const sheetId = (s && s.analyticsSpreadsheetId) || process.env.ANALYTICS_SPREADSHEET_ID || "";

  console.log("=== Backfill Readiness Check ===\n");
  console.log("Has credentials:", hasCreds);
  console.log("Has robot email:", hasRobot);
  if (hasRobot && s && s.robotEmail) {
    console.log("Robot email:", s.robotEmail.address);
  }
  console.log("Analytics sheet:", sheetId ? sheetId.slice(0, 20) + "..." : "(not set)");

  // Check DB tables
  console.log("\n--- Database Tables ---");
  await initDatabase();
  const pool = getPool();

  const { rows: tables } = await pool.query(`
    SELECT table_name FROM information_schema.tables
    WHERE table_schema = 'public'
    ORDER BY table_name
  `);
  console.log("Tables:", tables.map((t: { table_name: string }) => t.table_name).join(", "));

  // Check row counts
  for (const t of tables) {
    try {
      const { rows } = await pool.query(`SELECT COUNT(*) as count FROM "${t.table_name}"`);
      console.log(`  ${t.table_name}: ${rows[0].count} rows`);
    } catch {
      console.log(`  ${t.table_name}: (error reading)`);
    }
  }

  // Check date ranges for key tables
  console.log("\n--- Date Ranges ---");
  const dateChecks = [
    { table: "subscriptions", col: "created_at" },
    { table: "first_visits", col: "attended_at" },
    { table: "registrations", col: "attended_at" },
    { table: "orders", col: "created_at" },
    { table: "new_customers", col: "created_at" },
  ];

  for (const { table, col } of dateChecks) {
    try {
      const { rows } = await pool.query(`
        SELECT MIN(${col}) as earliest, MAX(${col}) as latest, COUNT(*) as count
        FROM ${table}
        WHERE ${col} IS NOT NULL AND ${col} <> ''
      `);
      if (Number(rows[0].count) > 0) {
        console.log(`  ${table}: ${rows[0].earliest} -> ${rows[0].latest} (${rows[0].count} rows)`);
      } else {
        console.log(`  ${table}: (empty)`);
      }
    } catch {
      console.log(`  ${table}: (table not found or error)`);
    }
  }

  console.log("\n=== Ready to backfill:", hasCreds && hasRobot && sheetId ? "YES" : "NO", "===");

  await pool.end();
}

main().catch(err => {
  console.error("Check failed:", err);
  process.exit(1);
});
