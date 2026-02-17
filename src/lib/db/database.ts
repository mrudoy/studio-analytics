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

    -- Subscription snapshots from Union.fit CSV exports
    CREATE TABLE IF NOT EXISTS subscriptions (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      snapshot_id TEXT,
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
    CREATE INDEX IF NOT EXISTS idx_sub_state ON subscriptions(subscription_state);
    CREATE INDEX IF NOT EXISTS idx_sub_created ON subscriptions(created_at);
    CREATE INDEX IF NOT EXISTS idx_sub_canceled ON subscriptions(canceled_at);
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

  // Migrations: add columns that may not exist yet in older databases
  try {
    db.exec(`ALTER TABLE revenue_categories ADD COLUMN other_fees REAL DEFAULT 0`);
    console.log(`[db] Migration: added other_fees column to revenue_categories`);
  } catch {
    // Column already exists
  }
  try {
    db.exec(`ALTER TABLE subscriptions ADD COLUMN snapshot_id TEXT`);
    console.log(`[db] Migration: added snapshot_id column to subscriptions`);
  } catch {
    // Column already exists
  }

  console.log(`[db] SQLite database initialized at ${DB_PATH}`);
  return db;
}
