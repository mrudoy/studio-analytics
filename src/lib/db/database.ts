import Database from "better-sqlite3";
import { join } from "path";

const DB_PATH = join(process.cwd(), "data", "studio-analytics.db");

let db: Database.Database | null = null;

export function getDatabase(): Database.Database {
  if (db) return db;

  db = new Database(DB_PATH);
  db.pragma("journal_mode = WAL");
  db.pragma("foreign_keys = ON");

  // Create tables on first use
  db.exec(`
    CREATE TABLE IF NOT EXISTS revenue_categories (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      period_start TEXT NOT NULL,
      period_end TEXT NOT NULL,
      category TEXT NOT NULL,
      revenue REAL DEFAULT 0,
      union_fees REAL DEFAULT 0,
      stripe_fees REAL DEFAULT 0,
      other_fees REAL DEFAULT 0,
      transfers REAL DEFAULT 0,
      refunded REAL DEFAULT 0,
      union_fees_refunded REAL DEFAULT 0,
      net_revenue REAL DEFAULT 0,
      locked INTEGER DEFAULT 0,
      created_at TEXT DEFAULT (datetime('now')),
      UNIQUE(period_start, period_end, category)
    );

    CREATE TABLE IF NOT EXISTS pipeline_runs (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      ran_at TEXT DEFAULT (datetime('now')),
      date_range_start TEXT,
      date_range_end TEXT,
      record_counts TEXT,
      duration_ms INTEGER
    );

    CREATE TABLE IF NOT EXISTS uploaded_data (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      filename TEXT NOT NULL,
      data_type TEXT NOT NULL,
      period TEXT,
      content TEXT NOT NULL,
      uploaded_at TEXT DEFAULT (datetime('now'))
    );

    -- Auto-renew snapshots from Union.fit CSV exports
    CREATE TABLE IF NOT EXISTS auto_renews (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      snapshot_id TEXT,
      plan_name TEXT,
      plan_state TEXT,
      plan_price REAL,
      customer_name TEXT,
      customer_email TEXT,
      created_at TEXT,
      order_id TEXT,
      sales_channel TEXT,
      canceled_at TEXT,
      canceled_by TEXT,
      admin TEXT,
      current_state TEXT,
      current_plan TEXT,
      imported_at TEXT DEFAULT (datetime('now'))
    );

    -- First visit registrations from Union.fit CSV exports
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

    -- Full registrations from Union.fit CSV exports
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

    -- Orders from Union.fit Sales By Service CSV exports
    CREATE TABLE IF NOT EXISTS orders (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      created_at TEXT,
      code TEXT,
      customer TEXT,
      order_type TEXT,
      payment TEXT,
      total REAL DEFAULT 0,
      imported_at TEXT DEFAULT (datetime('now'))
    );

    -- New customers from Union.fit Customers CSV exports
    CREATE TABLE IF NOT EXISTS new_customers (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT,
      email TEXT,
      role TEXT,
      order_count INTEGER DEFAULT 0,
      created_at TEXT,
      imported_at TEXT DEFAULT (datetime('now'))
    );
  `);

  // Indexes for performance
  db.exec(`
    CREATE INDEX IF NOT EXISTS idx_ar_state ON auto_renews(plan_state);
    CREATE INDEX IF NOT EXISTS idx_ar_created ON auto_renews(created_at);
    CREATE INDEX IF NOT EXISTS idx_ar_canceled ON auto_renews(canceled_at);
    CREATE INDEX IF NOT EXISTS idx_fv_attended ON first_visits(attended_at);
    CREATE INDEX IF NOT EXISTS idx_reg_attended ON registrations(attended_at);
    CREATE INDEX IF NOT EXISTS idx_reg_subscription ON registrations(subscription);
    CREATE INDEX IF NOT EXISTS idx_orders_created ON orders(created_at);
    CREATE INDEX IF NOT EXISTS idx_orders_type ON orders(order_type);
    CREATE INDEX IF NOT EXISTS idx_newcust_created ON new_customers(created_at);
    CREATE INDEX IF NOT EXISTS idx_newcust_email ON new_customers(email);
    CREATE INDEX IF NOT EXISTS idx_fv_email ON first_visits(email);
    CREATE INDEX IF NOT EXISTS idx_reg_email ON registrations(email);
  `);

  // Unique indexes for dedup (INSERT OR IGNORE)
  try { db.exec(`CREATE UNIQUE INDEX IF NOT EXISTS idx_fv_dedup ON first_visits(email, attended_at)`); } catch { /* already exists or conflict */ }
  try { db.exec(`CREATE UNIQUE INDEX IF NOT EXISTS idx_reg_dedup ON registrations(email, attended_at)`); } catch { /* already exists or conflict */ }
  try { db.exec(`CREATE UNIQUE INDEX IF NOT EXISTS idx_orders_dedup ON orders(code)`); } catch { /* already exists or conflict */ }
  try { db.exec(`CREATE UNIQUE INDEX IF NOT EXISTS idx_newcust_dedup ON new_customers(email)`); } catch { /* already exists or conflict */ }

  // Migrations: add columns that may not exist yet in older databases
  try {
    db.exec(`ALTER TABLE revenue_categories ADD COLUMN other_fees REAL DEFAULT 0`);
    console.log(`[db] Migration: added other_fees column to revenue_categories`);
  } catch {
    // Column already exists
  }
  // Migration: rename old "subscriptions" table to "auto_renews"
  try {
    db.exec(`ALTER TABLE subscriptions RENAME TO auto_renews`);
    console.log(`[db] Migration: renamed subscriptions → auto_renews`);
  } catch {
    // Table already renamed or doesn't exist
  }
  // Migration: rename old column names
  try { db.exec(`ALTER TABLE auto_renews RENAME COLUMN subscription_name TO plan_name`); } catch { /* already renamed */ }
  try { db.exec(`ALTER TABLE auto_renews RENAME COLUMN subscription_state TO plan_state`); } catch { /* already renamed */ }
  try { db.exec(`ALTER TABLE auto_renews RENAME COLUMN subscription_price TO plan_price`); } catch { /* already renamed */ }
  try { db.exec(`ALTER TABLE auto_renews RENAME COLUMN current_subscription TO current_plan`); } catch { /* already renamed */ }
  try { db.exec(`ALTER TABLE auto_renews ADD COLUMN snapshot_id TEXT`); } catch { /* already exists */ }

  console.log(`[db] SQLite database initialized at ${DB_PATH}`);
  return db;
}
