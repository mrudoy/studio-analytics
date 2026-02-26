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
  // Delete multi-month revenue_categories rows that overlap with monthly data.
  // The 2025 full-year row (2025-01-01 → 2025-12-31) was double-counting $2.2M
  // on top of 12 monthly rows. The 2026 YTD row also overlaps with Jan monthly.
  // Keep 2024 full-year row since no monthly data exists for that year.
  {
    name: "004_remove_overlapping_revenue_periods",
    up: `
      -- Delete multi-month periods where monthly data already exists for that year.
      -- A "multi-month" period is one where the start month ≠ end month.
      -- Only delete if that year also has at least one "monthly" period.
      DELETE FROM revenue_categories
      WHERE LEFT(period_start, 7) != LEFT(period_end, 7)
        AND LEFT(period_start, 4) IN (
          SELECT DISTINCT LEFT(period_start, 4)
          FROM revenue_categories
          WHERE LEFT(period_start, 7) = LEFT(period_end, 7)
        );
    `,
  },
  // Shopify integration — orders, products, customers, inventory
  {
    name: "005_shopify_tables",
    up: `
      -- Shopify orders with line items as JSONB
      CREATE TABLE IF NOT EXISTS shopify_orders (
        id BIGINT PRIMARY KEY,
        order_number INTEGER,
        email TEXT,
        financial_status TEXT,
        fulfillment_status TEXT,
        total_price NUMERIC(12,2),
        subtotal_price NUMERIC(12,2),
        total_tax NUMERIC(12,2),
        total_discounts NUMERIC(12,2),
        currency TEXT DEFAULT 'USD',
        line_items JSONB,
        customer_id BIGINT,
        created_at TIMESTAMPTZ,
        updated_at TIMESTAMPTZ,
        canceled_at TIMESTAMPTZ,
        synced_at TIMESTAMPTZ DEFAULT NOW()
      );
      CREATE INDEX IF NOT EXISTS idx_shopify_orders_created ON shopify_orders(created_at);
      CREATE INDEX IF NOT EXISTS idx_shopify_orders_email ON shopify_orders(email);

      -- Shopify products
      CREATE TABLE IF NOT EXISTS shopify_products (
        id BIGINT PRIMARY KEY,
        title TEXT,
        product_type TEXT,
        vendor TEXT,
        status TEXT,
        tags TEXT,
        variants JSONB,
        created_at TIMESTAMPTZ,
        updated_at TIMESTAMPTZ,
        synced_at TIMESTAMPTZ DEFAULT NOW()
      );

      -- Shopify customers
      CREATE TABLE IF NOT EXISTS shopify_customers (
        id BIGINT PRIMARY KEY,
        email TEXT,
        first_name TEXT,
        last_name TEXT,
        orders_count INTEGER,
        total_spent NUMERIC(12,2),
        tags TEXT,
        created_at TIMESTAMPTZ,
        updated_at TIMESTAMPTZ,
        synced_at TIMESTAMPTZ DEFAULT NOW()
      );
      CREATE UNIQUE INDEX IF NOT EXISTS idx_shopify_customers_email
        ON shopify_customers(email);

      -- Shopify inventory levels (composite key)
      CREATE TABLE IF NOT EXISTS shopify_inventory (
        inventory_item_id BIGINT,
        location_id BIGINT,
        available INTEGER,
        updated_at TIMESTAMPTZ,
        synced_at TIMESTAMPTZ DEFAULT NOW(),
        PRIMARY KEY (inventory_item_id, location_id)
      );
    `,
  },
  // Union.fit raw export IDs for precise dedup from zip pipeline
  {
    name: "006_union_ids_for_zip_pipeline",
    up: `
      -- Auto-renews: union_pass_id from passes.csv
      ALTER TABLE auto_renews ADD COLUMN IF NOT EXISTS union_pass_id TEXT;
      CREATE UNIQUE INDEX IF NOT EXISTS idx_ar_union_pass_id
        ON auto_renews(union_pass_id) WHERE union_pass_id IS NOT NULL;

      -- Orders: union_order_id from orders.csv
      ALTER TABLE orders ADD COLUMN IF NOT EXISTS union_order_id TEXT;
      CREATE UNIQUE INDEX IF NOT EXISTS idx_orders_union_id
        ON orders(union_order_id) WHERE union_order_id IS NOT NULL;

      -- Registrations: union_registration_id from registrations.csv
      ALTER TABLE registrations ADD COLUMN IF NOT EXISTS union_registration_id TEXT;
      CREATE UNIQUE INDEX IF NOT EXISTS idx_reg_union_id
        ON registrations(union_registration_id) WHERE union_registration_id IS NOT NULL;
    `,
  },
  // Performance: indexes for conversion pool queries that JOIN registrations × auto_renews
  {
    name: "007_conversion_pool_indexes",
    up: `
      -- registrations: the main scan target for non-subscriber visits
      CREATE INDEX IF NOT EXISTS idx_reg_email_lower_attended
        ON registrations (LOWER(email), attended_at);
      CREATE INDEX IF NOT EXISTS idx_reg_attended_at
        ON registrations (attended_at)
        WHERE attended_at IS NOT NULL AND attended_at != '';
      CREATE INDEX IF NOT EXISTS idx_reg_subscription
        ON registrations (subscription);

      -- auto_renews: join target for converter lookups
      CREATE INDEX IF NOT EXISTS idx_ar_email_lower_created
        ON auto_renews (LOWER(customer_email), created_at);
      CREATE INDEX IF NOT EXISTS idx_ar_email_lower
        ON auto_renews (LOWER(customer_email));

      -- first_visits: used in new customer queries
      CREATE INDEX IF NOT EXISTS idx_fv_email_lower_attended
        ON first_visits (LOWER(email), attended_at);
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
