/**
 * Simple SQL migration runner for PostgreSQL.
 *
 * Migrations are defined inline and run once, tracked in a migrations table.
 * Each migration has a unique name and an "up" SQL string.
 * They are applied in order, and each one is recorded so it never runs twice.
 */

import { getPool } from "./database";

interface Migration {
  name: string;
  up: string;
}

/**
 * Define all migrations here in chronological order.
 * Never remove or reorder existing migrations.
 * To add a new migration, append to the end of this array.
 */
const migrations: Migration[] = [
  // Example: first migration (schema is created by initDatabase, so this is a no-op marker)
  {
    name: "001_initial_schema",
    up: "SELECT 1; -- Initial schema created by initDatabase()",
  },
  // Fix period dates stored as M/D/YYYY → normalize to YYYY-MM-DD
  // e.g. "1/1/2025" → "2025-01-01", "12/31/2025" → "2025-12-31"
  {
    name: "002_normalize_period_dates",
    up: `
      UPDATE revenue_categories
      SET period_start = TO_CHAR(TO_DATE(period_start, 'MM/DD/YYYY'), 'YYYY-MM-DD')
      WHERE period_start ~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$';

      UPDATE revenue_categories
      SET period_end = TO_CHAR(TO_DATE(period_end, 'MM/DD/YYYY'), 'YYYY-MM-DD')
      WHERE period_end ~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$';

      UPDATE pipeline_runs
      SET date_range_start = TO_CHAR(TO_DATE(date_range_start, 'MM/DD/YYYY'), 'YYYY-MM-DD')
      WHERE date_range_start ~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$';

      UPDATE pipeline_runs
      SET date_range_end = TO_CHAR(TO_DATE(date_range_end, 'MM/DD/YYYY'), 'YYYY-MM-DD')
      WHERE date_range_end ~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$';
    `,
  },
  // Add fetch_watermarks table for incremental fetch tracking
  // and unique constraint on auto_renews for UPSERT instead of DELETE+INSERT
  {
    name: "003_fetch_watermarks_and_auto_renew_dedup",
    up: `
      CREATE TABLE IF NOT EXISTS fetch_watermarks (
        id SERIAL PRIMARY KEY,
        report_type TEXT UNIQUE NOT NULL,
        last_fetched_at TIMESTAMPTZ,
        high_water_date TEXT,
        record_count INTEGER DEFAULT 0,
        notes TEXT
      );

      -- Remove duplicate auto_renews before adding unique constraint.
      -- Keep the row with the highest id (most recent import).
      DELETE FROM auto_renews a
      USING auto_renews b
      WHERE a.customer_email = b.customer_email
        AND a.plan_name = b.plan_name
        AND a.created_at = b.created_at
        AND a.id < b.id;

      CREATE UNIQUE INDEX IF NOT EXISTS idx_ar_dedup
        ON auto_renews(customer_email, plan_name, created_at);
    `,
  },
];

/**
 * Run all pending migrations.
 * Creates the migrations tracking table if it doesn't exist.
 * Safe to call multiple times — already-applied migrations are skipped.
 */
export async function runMigrations(): Promise<void> {
  const pool = getPool();

  // Ensure migrations tracking table exists
  await pool.query(`
    CREATE TABLE IF NOT EXISTS _migrations (
      id SERIAL PRIMARY KEY,
      name TEXT NOT NULL UNIQUE,
      applied_at TIMESTAMPTZ DEFAULT NOW()
    );
  `);

  // Find which migrations have already been applied
  const result = await pool.query("SELECT name FROM _migrations ORDER BY id");
  const applied = new Set(result.rows.map((r: { name: string }) => r.name));

  // Apply pending migrations in order
  let count = 0;
  for (const migration of migrations) {
    if (applied.has(migration.name)) continue;

    console.log(`[migrations] Applying: ${migration.name}`);
    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      await client.query(migration.up);
      await client.query("INSERT INTO _migrations (name) VALUES ($1)", [migration.name]);
      await client.query("COMMIT");
      count++;
    } catch (err) {
      await client.query("ROLLBACK");
      console.error(`[migrations] Failed to apply ${migration.name}:`, err);
      throw err;
    } finally {
      client.release();
    }
  }

  if (count > 0) {
    console.log(`[migrations] Applied ${count} migration(s)`);
  } else {
    console.log("[migrations] All migrations up to date");
  }
}
