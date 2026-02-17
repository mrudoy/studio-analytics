/**
 * Import the latest email CSV attachments directly into PostgreSQL.
 *
 * This is simpler than the full pipeline — it just parses the raw CSVs
 * and inserts into the correct tables using the store modules.
 *
 * Usage: npx tsx scripts/import-email-csvs.ts
 */
import * as dotenv from "dotenv";
dotenv.config();

import { readFileSync, readdirSync, statSync } from "fs";
import { join } from "path";
import Papa from "papaparse";
import { getPool, initDatabase } from "../src/lib/db/database";
import { saveAutoRenews, type AutoRenewRow } from "../src/lib/db/auto-renew-store";
import { saveRegistrations, saveFirstVisits, type RegistrationRow } from "../src/lib/db/registration-store";
import { saveOrders, type OrderRow } from "../src/lib/db/order-store";
import { saveCustomers, type CustomerRow } from "../src/lib/db/customer-store";

const EMAIL_DIR = join(process.cwd(), "data", "email-attachments");
const DOWNLOADS_DIR = join(process.cwd(), "data", "downloads");

function parseCSVFile(filePath: string): Record<string, string>[] {
  const data = readFileSync(filePath, "utf8");
  return Papa.parse(data, { header: true, skipEmptyLines: true }).data as Record<string, string>[];
}

/**
 * Find the most recent file matching a pattern in a directory.
 */
function findLatestFile(dir: string, pattern: RegExp): string | null {
  try {
    const files = readdirSync(dir)
      .filter(f => pattern.test(f))
      .map(f => ({ name: f, mtime: statSync(join(dir, f)).mtimeMs }))
      .sort((a, b) => b.mtime - a.mtime);
    return files.length > 0 ? join(dir, files[0].name) : null;
  } catch {
    return null;
  }
}

async function main() {
  console.log("=== Import Email CSVs into Database ===\n");

  // Initialize DB
  await initDatabase();
  const pool = getPool();
  console.log("Database initialized.\n");

  // ── 1. Auto-Renews (from email) ──
  const subsFile = findLatestFile(EMAIL_DIR, /union-sky-ting-subscriptions/i);
  if (subsFile) {
    console.log("Auto-Renews:", subsFile.split("/").pop());
    const rows = parseCSVFile(subsFile);
    const snapshotId = "backfill-2025-" + Date.now();
    const arRows: AutoRenewRow[] = rows.map(r => ({
      planName: r.subscription_name || "",
      planState: r.subscription_state || "",
      planPrice: r.subscription_price ? parseFloat(r.subscription_price) : 0,
      customerName: r.customer_name || "",
      customerEmail: r.customer_email || "",
      createdAt: r.created_at || "",
      canceledAt: r.canceled_at || undefined,
    }));
    await saveAutoRenews(snapshotId, arRows);
    console.log("  Saved", arRows.length, "auto-renews");

    const dates = arRows.map(r => r.createdAt).filter(Boolean).sort();
    if (dates.length > 0) {
      console.log("  Range:", dates[0], "->", dates[dates.length - 1]);
    }
  } else {
    console.log("Auto-Renews: (no file found)");
  }

  // ── 2. First Visits (from email) ──
  const fvFile = findLatestFile(EMAIL_DIR, /first-visit/i);
  if (fvFile) {
    console.log("\nFirst Visits:", fvFile.split("/").pop());
    const rows = parseCSVFile(fvFile);
    const fvRows: RegistrationRow[] = rows.map(r => ({
      eventName: r.event_name || "",
      performanceStartsAt: r.performance_starts_at || "",
      locationName: r.location_name || "",
      teacherName: r.teacher_name || "",
      firstName: r.first_name || "",
      lastName: r.last_name || "",
      email: r.email || "",
      attendedAt: r.attended_at || "",
      registrationType: r.registration_type || "",
      state: r.state || "",
      pass: r.pass || "",
      subscription: r.subscription || "false",
      revenue: r.revenue ? parseFloat(r.revenue) : 0,
    }));
    await saveFirstVisits(fvRows);
    console.log("  Saved", fvRows.length, "first visits");

    const dates = fvRows.map(r => r.attendedAt).filter(Boolean).sort();
    if (dates.length > 0) {
      console.log("  Range:", dates[0], "->", dates[dates.length - 1]);
    }
  } else {
    console.log("\nFirst Visits: (no file found)");
  }

  // ── 3. Full Registrations (from email) ──
  const regFile = findLatestFile(EMAIL_DIR, /registration-export(?!.*first)/i);
  if (regFile) {
    console.log("\nFull Registrations:", regFile.split("/").pop());
    const rows = parseCSVFile(regFile);
    const regRows: RegistrationRow[] = rows.map(r => ({
      eventName: r.event_name || "",
      performanceStartsAt: r.performance_starts_at || "",
      locationName: r.location_name || "",
      teacherName: r.teacher_name || "",
      firstName: r.first_name || "",
      lastName: r.last_name || "",
      email: r.email || "",
      attendedAt: r.attended_at || "",
      registrationType: r.registration_type || "",
      state: r.state || "",
      pass: r.pass || "",
      subscription: r.subscription || "false",
      revenue: r.revenue ? parseFloat(r.revenue) : 0,
    }));
    await saveRegistrations(regRows);
    console.log("  Saved", regRows.length, "registrations");

    const dates = regRows.map(r => r.attendedAt).filter(Boolean).sort();
    if (dates.length > 0) {
      console.log("  Range:", dates[0], "->", dates[dates.length - 1]);
    }
  } else {
    console.log("\nFull Registrations: (no file found)");
  }

  // ── 4. Orders (from data/downloads) ──
  const ordersFile = findLatestFile(DOWNLOADS_DIR, /orders/i);
  if (ordersFile) {
    console.log("\nOrders:", ordersFile.split("/").pop());
    const rows = parseCSVFile(ordersFile);
    const orderRows: OrderRow[] = rows.map(r => ({
      created: r.created || r.Created || "",
      code: r.code || r.Code || "",
      customer: r.customer || r.Customer || "",
      type: r.type || r.Type || "",
      payment: r.payment || r.Payment || "",
      total: parseFloat((r.total || r.Total || "0").replace(/[$,]/g, "")) || 0,
    }));
    await saveOrders(orderRows);
    console.log("  Saved", orderRows.length, "orders");

    const dates = orderRows.map(r => r.created).filter(Boolean).sort();
    if (dates.length > 0) {
      console.log("  Range:", dates[0], "->", dates[dates.length - 1]);
    }
  } else {
    console.log("\nOrders: (no file found)");
  }

  // ── 5. New Customers (from data/downloads) ──
  const custFile = findLatestFile(DOWNLOADS_DIR, /newCustomers|customer/i);
  if (custFile) {
    console.log("\nNew Customers:", custFile.split("/").pop());
    const rows = parseCSVFile(custFile);
    const custRows: CustomerRow[] = rows.map(r => ({
      name: r.name || r.Name || "",
      email: r.email || r.Email || "",
      role: r.role || r.Role || "",
      orders: parseInt(r.orders || r.Orders || "0") || 0,
      created: r.created || r.Created || "",
    }));
    await saveCustomers(custRows);
    console.log("  Saved", custRows.length, "customers");

    const dates = custRows.map(r => r.created).filter(Boolean).sort();
    if (dates.length > 0) {
      console.log("  Range:", dates[0], "->", dates[dates.length - 1]);
    }
  } else {
    console.log("\nNew Customers: (no file found)");
  }

  // ── Summary ──
  console.log("\n=== Import Summary ===");
  const tables = ["subscriptions", "first_visits", "registrations", "orders", "new_customers"];
  for (const table of tables) {
    const { rows } = await pool.query(`SELECT COUNT(*) as count FROM ${table}`);
    console.log("  " + table + ": " + rows[0].count + " rows");
  }

  await pool.end();
}

main().catch(err => {
  console.error("Import failed:", err);
  process.exit(1);
});
