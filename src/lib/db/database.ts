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
  `);

  console.log(`[db] SQLite database initialized at ${DB_PATH}`);
  return db;
}
