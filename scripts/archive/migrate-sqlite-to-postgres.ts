/**
 * One-time migration: read all data from local SQLite and write to PostgreSQL.
 *
 * Prerequisites:
 *   1. Postgres running (docker compose up postgres -d)
 *   2. DATABASE_URL set in .env
 *   3. better-sqlite3 still installed (devDependency)
 *
 * Usage: npx tsx scripts/migrate-sqlite-to-postgres.ts
 */
import * as dotenv from "dotenv";
dotenv.config();

import Database from "better-sqlite3";
import { Pool } from "pg";
import { join } from "path";
import { existsSync } from "fs";
import { initDatabase } from "../src/lib/db/database";

const SQLITE_PATH = join(process.cwd(), "data", "analytics.db");
const BATCH_SIZE = 500;

async function main() {
  // ── 1. Check SQLite file exists ──
  if (!existsSync(SQLITE_PATH)) {
    console.error(`SQLite database not found at: ${SQLITE_PATH}`);
    console.error("Nothing to migrate.");
    process.exit(1);
  }

  console.log("=== SQLite → PostgreSQL Migration ===\n");
  console.log(`Source: ${SQLITE_PATH}`);
  console.log(`Target: ${process.env.DATABASE_URL?.replace(/:[^:@]+@/, ":***@")}\n`);

  // ── 2. Open SQLite ──
  const sqlite = new Database(SQLITE_PATH, { readonly: true });

  // ── 3. Connect to Postgres and initialize schema ──
  await initDatabase();
  const pg = new Pool({ connectionString: process.env.DATABASE_URL });

  // ── 4. Migrate each table ──
  const migrations: { table: string; query: string; insert: string; columns: string[] }[] = [
    {
      table: "subscriptions",
      query: "SELECT * FROM subscriptions",
      insert: `INSERT INTO subscriptions (snapshot_id, plan_name, plan_state, plan_price, customer_name, customer_email, created_at, canceled_at, imported_at)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
               ON CONFLICT DO NOTHING`,
      columns: ["snapshot_id", "plan_name", "plan_state", "plan_price", "customer_name", "customer_email", "created_at", "canceled_at", "imported_at"],
    },
    {
      table: "first_visits",
      query: "SELECT * FROM first_visits",
      insert: `INSERT INTO first_visits (event_name, performance_starts_at, location_name, teacher_name, first_name, last_name, email, attended_at, registration_type, state, pass, subscription, revenue, imported_at)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
               ON CONFLICT (email, attended_at) DO NOTHING`,
      columns: ["event_name", "performance_starts_at", "location_name", "teacher_name", "first_name", "last_name", "email", "attended_at", "registration_type", "state", "pass", "subscription", "revenue", "imported_at"],
    },
    {
      table: "registrations",
      query: "SELECT * FROM registrations",
      insert: `INSERT INTO registrations (event_name, performance_starts_at, location_name, teacher_name, first_name, last_name, email, attended_at, registration_type, state, pass, subscription, revenue, imported_at)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
               ON CONFLICT (email, attended_at) DO NOTHING`,
      columns: ["event_name", "performance_starts_at", "location_name", "teacher_name", "first_name", "last_name", "email", "attended_at", "registration_type", "state", "pass", "subscription", "revenue", "imported_at"],
    },
    {
      table: "orders",
      query: "SELECT * FROM orders",
      insert: `INSERT INTO orders (created_at, code, customer, type, payment, total, imported_at)
               VALUES ($1, $2, $3, $4, $5, $6, $7)
               ON CONFLICT (code) DO NOTHING`,
      columns: ["created_at", "code", "customer", "type", "payment", "total", "imported_at"],
    },
    {
      table: "new_customers",
      query: "SELECT * FROM new_customers",
      insert: `INSERT INTO new_customers (name, email, role, orders, created_at, imported_at)
               VALUES ($1, $2, $3, $4, $5, $6)
               ON CONFLICT DO NOTHING`,
      columns: ["name", "email", "role", "orders", "created_at", "imported_at"],
    },
    {
      table: "revenue_categories",
      query: "SELECT * FROM revenue_categories",
      insert: `INSERT INTO revenue_categories (period_start, period_end, category, revenue, union_fees, stripe_fees, other_fees, transfers, refunded, union_fees_refunded, net_revenue, created_at)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
               ON CONFLICT DO NOTHING`,
      columns: ["period_start", "period_end", "category", "revenue", "union_fees", "stripe_fees", "other_fees", "transfers", "refunded", "union_fees_refunded", "net_revenue", "created_at"],
    },
    {
      table: "revenue_periods",
      query: "SELECT * FROM revenue_periods",
      insert: `INSERT INTO revenue_periods (period_start, period_end, is_locked, locked_at, created_at)
               VALUES ($1, $2, $3, $4, $5)
               ON CONFLICT DO NOTHING`,
      columns: ["period_start", "period_end", "is_locked", "locked_at", "created_at"],
    },
    {
      table: "pipeline_runs",
      query: "SELECT * FROM pipeline_runs",
      insert: `INSERT INTO pipeline_runs (period_start, period_end, record_counts, duration_ms, created_at)
               VALUES ($1, $2, $3, $4, $5)
               ON CONFLICT DO NOTHING`,
      columns: ["period_start", "period_end", "record_counts", "duration_ms", "created_at"],
    },
    {
      table: "uploaded_data",
      query: "SELECT * FROM uploaded_data",
      insert: `INSERT INTO uploaded_data (filename, data_type, period, csv_content, created_at)
               VALUES ($1, $2, $3, $4, $5)
               ON CONFLICT DO NOTHING`,
      columns: ["filename", "data_type", "period", "csv_content", "created_at"],
    },
    {
      table: "full_customers",
      query: "SELECT * FROM full_customers",
      insert: `INSERT INTO full_customers (
               email, name, first_name, last_name, phone, street_address, city,
               state, zip, country, tags, role, note, emergency_contact, emergency_phone,
               birthday, gender, referred_by, total_visits, last_visit,
               active_auto_renew, paused_auto_renew, member_since, fv_date,
               auto_renew_count, total_purchases, total_revenue, total_credits,
               created_at, imported_at)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30)
               ON CONFLICT (email) DO UPDATE SET
                 name = EXCLUDED.name, first_name = EXCLUDED.first_name, last_name = EXCLUDED.last_name,
                 phone = EXCLUDED.phone, total_visits = EXCLUDED.total_visits, last_visit = EXCLUDED.last_visit,
                 active_auto_renew = EXCLUDED.active_auto_renew, total_revenue = EXCLUDED.total_revenue,
                 imported_at = EXCLUDED.imported_at`,
      columns: ["email", "name", "first_name", "last_name", "phone", "street_address", "city",
                "state", "zip", "country", "tags", "role", "note", "emergency_contact", "emergency_phone",
                "birthday", "gender", "referred_by", "total_visits", "last_visit",
                "active_auto_renew", "paused_auto_renew", "member_since", "fv_date",
                "auto_renew_count", "total_purchases", "total_revenue", "total_credits",
                "created_at", "imported_at"],
    },
  ];

  let totalMigrated = 0;

  for (const { table, query, insert, columns } of migrations) {
    try {
      // Check if table exists in SQLite
      const tableExists = sqlite.prepare(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?"
      ).get(table);

      if (!tableExists) {
        console.log(`  ${table}: (not in SQLite, skipping)`);
        continue;
      }

      const rows = sqlite.prepare(query).all() as Record<string, unknown>[];
      if (rows.length === 0) {
        console.log(`  ${table}: (empty, skipping)`);
        continue;
      }

      console.log(`  ${table}: migrating ${rows.length} rows...`);

      const client = await pg.connect();
      try {
        await client.query("BEGIN");

        let migrated = 0;
        for (let i = 0; i < rows.length; i += BATCH_SIZE) {
          const batch = rows.slice(i, i + BATCH_SIZE);
          for (const row of batch) {
            const values = columns.map(col => {
              const val = row[col];
              // Handle SQLite NULL -> Postgres NULL
              if (val === null || val === undefined) return null;
              return val;
            });
            await client.query(insert, values);
            migrated++;
          }
          process.stdout.write(`\r    ${migrated}/${rows.length}`);
        }

        await client.query("COMMIT");
        console.log(`\r  ${table}: ${migrated} rows migrated`);
        totalMigrated += migrated;
      } catch (err) {
        await client.query("ROLLBACK");
        console.error(`\n  ${table}: FAILED - ${err instanceof Error ? err.message : err}`);
      } finally {
        client.release();
      }
    } catch (err) {
      console.error(`  ${table}: ERROR - ${err instanceof Error ? err.message : err}`);
    }
  }

  // ── 5. Verify ──
  console.log("\n=== Verification ===");
  for (const { table } of migrations) {
    try {
      const sqliteCount = (sqlite.prepare(`SELECT COUNT(*) as count FROM ${table}`).get() as { count: number })?.count ?? 0;
      const { rows } = await pg.query(`SELECT COUNT(*) as count FROM ${table}`);
      const pgCount = Number(rows[0].count);
      const match = pgCount >= sqliteCount ? "OK" : "MISMATCH";
      console.log(`  ${table}: SQLite=${sqliteCount}, Postgres=${pgCount} [${match}]`);
    } catch {
      // Table might not exist in one of the databases
    }
  }

  console.log(`\nTotal rows migrated: ${totalMigrated}`);
  console.log("Migration complete!");

  sqlite.close();
  await pg.end();
}

main().catch(err => {
  console.error("Migration failed:", err);
  process.exit(1);
});
