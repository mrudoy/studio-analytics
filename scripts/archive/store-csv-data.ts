/**
 * Parse downloaded CSV email attachments and store in SQLite.
 *
 * Proves the full flow: Gmail API → download CSV → parse → store → query.
 */

import * as dotenv from "dotenv";
dotenv.config();

import Database from "better-sqlite3";
import { readFileSync } from "fs";
import { join } from "path";
import Papa from "papaparse";

const DB_PATH = join(process.cwd(), "data", "studio-analytics.db");
const ATTACHMENTS_DIR = join(process.cwd(), "data", "email-attachments");

function getDb() {
  const db = new Database(DB_PATH);
  db.pragma("journal_mode = WAL");
  return db;
}

function main() {
  const db = getDb();

  // ── Create tables for the raw CSV data ──────────────────────────
  db.exec(`
    CREATE TABLE IF NOT EXISTS first_visits (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      event_name TEXT,
      performance_starts_at TEXT,
      location_name TEXT,
      video_name TEXT,
      teacher_name TEXT,
      first_name TEXT,
      last_name TEXT,
      email TEXT,
      registered_at TEXT,
      attended_at TEXT,
      registration_type TEXT,
      state TEXT,
      pass TEXT,
      subscription TEXT,
      revenue REAL,
      imported_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS subscriptions (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      subscription_name TEXT,
      subscription_state TEXT,
      subscription_price REAL,
      customer_name TEXT,
      customer_email TEXT,
      created_at TEXT,
      order_id TEXT,
      sales_channel TEXT,
      canceled_at TEXT,
      canceled_by TEXT,
      admin TEXT,
      current_state TEXT,
      current_subscription TEXT,
      imported_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS registrations (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      event_name TEXT,
      performance_starts_at TEXT,
      location_name TEXT,
      video_name TEXT,
      teacher_name TEXT,
      first_name TEXT,
      last_name TEXT,
      email TEXT,
      registered_at TEXT,
      attended_at TEXT,
      registration_type TEXT,
      state TEXT,
      pass TEXT,
      subscription TEXT,
      revenue REAL,
      imported_at TEXT DEFAULT (datetime('now'))
    );
  `);

  console.log("Tables created.\n");

  // ── Parse and store First Visits CSV ────────────────────────────
  const firstVisitFile = join(ATTACHMENTS_DIR, "1771282469195-20260216-2242-union-registration-export-first-visit.csv");
  const firstVisitData = readFileSync(firstVisitFile, "utf8");
  const firstVisitRows = Papa.parse(firstVisitData, { header: true, skipEmptyLines: true }).data as Record<string, string>[];

  const insertFirstVisit = db.prepare(`
    INSERT INTO first_visits (event_name, performance_starts_at, location_name, video_name, teacher_name,
      first_name, last_name, email, registered_at, attended_at, registration_type, state, pass, subscription, revenue)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);

  // Clear existing data for clean import
  db.exec("DELETE FROM first_visits");

  const insertFirstVisits = db.transaction((rows: Record<string, string>[]) => {
    for (const r of rows) {
      insertFirstVisit.run(
        r.event_name, r.performance_starts_at, r.location_name, r.video_name, r.teacher_name,
        r.first_name, r.last_name, r.email, r.registered_at, r.attended_at,
        r.registration_type, r.state, r.pass, r.subscription,
        r.revenue ? parseFloat(r.revenue) : 0
      );
    }
  });

  insertFirstVisits(firstVisitRows);
  console.log(`First Visits: ${firstVisitRows.length} rows stored`);

  // ── Parse and store Subscriptions CSV ───────────────────────────
  const subsFile = join(ATTACHMENTS_DIR, "1771282469634-20260216-2241-union-sky-ting-subscriptions-changes.csv");
  const subsData = readFileSync(subsFile, "utf8");
  const subsRows = Papa.parse(subsData, { header: true, skipEmptyLines: true }).data as Record<string, string>[];

  const insertSub = db.prepare(`
    INSERT INTO subscriptions (subscription_name, subscription_state, subscription_price, customer_name,
      customer_email, created_at, order_id, sales_channel, canceled_at, canceled_by, admin, current_state, current_subscription)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);

  db.exec("DELETE FROM subscriptions");

  const insertSubs = db.transaction((rows: Record<string, string>[]) => {
    for (const r of rows) {
      insertSub.run(
        r.subscription_name, r.subscription_state, r.subscription_price ? parseFloat(r.subscription_price) : 0,
        r.customer_name, r.customer_email, r.created_at, r.order_id, r.sales_channel,
        r.canceled_at || null, r.canceled_by || null, r.admin || null,
        r.current_state || null, r.current_subscription || null
      );
    }
  });

  insertSubs(subsRows);
  console.log(`Subscriptions: ${subsRows.length} rows stored`);

  // ── Parse and store Registrations CSV ───────────────────────────
  const regFile = join(ATTACHMENTS_DIR, "1771282470249-20260216-2225-union-registration-export.csv");
  const regData = readFileSync(regFile, "utf8");
  const regRows = Papa.parse(regData, { header: true, skipEmptyLines: true }).data as Record<string, string>[];

  const insertReg = db.prepare(`
    INSERT INTO registrations (event_name, performance_starts_at, location_name, video_name, teacher_name,
      first_name, last_name, email, registered_at, attended_at, registration_type, state, pass, subscription, revenue)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);

  db.exec("DELETE FROM registrations");

  const insertRegs = db.transaction((rows: Record<string, string>[]) => {
    for (const r of rows) {
      insertReg.run(
        r.event_name, r.performance_starts_at, r.location_name, r.video_name, r.teacher_name,
        r.first_name, r.last_name, r.email, r.registered_at, r.attended_at,
        r.registration_type, r.state, r.pass, r.subscription,
        r.revenue ? parseFloat(r.revenue) : 0
      );
    }
  });

  insertRegs(regRows);
  console.log(`Registrations: ${regRows.length} rows stored`);

  // ── Query the data to prove it works ────────────────────────────
  console.log("\n=== Sample Queries ===\n");

  // First visits by month
  const fvByMonth = db.prepare(`
    SELECT substr(attended_at, 1, 7) as month, COUNT(*) as count
    FROM first_visits
    WHERE attended_at IS NOT NULL AND attended_at != ''
    GROUP BY month
    ORDER BY month
  `).all() as { month: string; count: number }[];
  console.log("First Visits by Month:");
  for (const r of fvByMonth) {
    console.log(`  ${r.month}: ${r.count}`);
  }

  // Subscription breakdown
  const subsByState = db.prepare(`
    SELECT subscription_state, COUNT(*) as count, ROUND(SUM(subscription_price), 2) as total_price
    FROM subscriptions
    GROUP BY subscription_state
    ORDER BY count DESC
  `).all() as { subscription_state: string; count: number; total_price: number }[];
  console.log("\nSubscriptions by State:");
  for (const r of subsByState) {
    console.log(`  ${r.subscription_state}: ${r.count} subs, $${r.total_price}`);
  }

  // Subscription plans
  const subsByPlan = db.prepare(`
    SELECT subscription_name, COUNT(*) as count, ROUND(AVG(subscription_price), 2) as avg_price
    FROM subscriptions
    WHERE subscription_state = 'Valid Now'
    GROUP BY subscription_name
    ORDER BY count DESC
    LIMIT 10
  `).all() as { subscription_name: string; count: number; avg_price: number }[];
  console.log("\nTop Active Subscription Plans:");
  for (const r of subsByPlan) {
    console.log(`  ${r.subscription_name}: ${r.count} active, avg $${r.avg_price}`);
  }

  // Registration types
  const regByType = db.prepare(`
    SELECT registration_type, COUNT(*) as count
    FROM registrations
    WHERE registration_type IS NOT NULL AND registration_type != ''
    GROUP BY registration_type
    ORDER BY count DESC
    LIMIT 10
  `).all() as { registration_type: string; count: number }[];
  console.log("\nRegistrations by Type:");
  for (const r of regByType) {
    console.log(`  ${r.registration_type}: ${r.count}`);
  }

  // Total revenue from registrations
  const totalRev = db.prepare(`
    SELECT ROUND(SUM(revenue), 2) as total, COUNT(*) as count
    FROM registrations
    WHERE revenue > 0
  `).get() as { total: number; count: number };
  console.log(`\nTotal Registration Revenue: $${totalRev.total} from ${totalRev.count} paid registrations`);

  db.close();
  console.log("\nDone. Data is stored in data/studio-analytics.db");
}

main();
