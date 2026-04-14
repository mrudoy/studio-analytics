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
  // Export log for freshness tracking — records each processed API export
  {
    name: "008_export_log_table",
    up: `
      CREATE TABLE IF NOT EXISTS export_log (
        id SERIAL PRIMARY KEY,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        export_created_at TEXT NOT NULL,
        data_range_start TEXT,
        data_range_end TEXT,
        record_count INTEGER DEFAULT 0,
        export_index INTEGER DEFAULT 0,
        total_exports INTEGER DEFAULT 0
      );
      CREATE INDEX IF NOT EXISTS idx_export_log_range_end
        ON export_log (data_range_end DESC);
    `,
  },
  // ── DB Optimization Migrations ──────────────────────────────
  // Performance indexes on revenue_categories + plan_category column + is_first_visit
  {
    name: "009_indexes_plan_category_first_visit",
    up: `
      -- Revenue category indexes (most-queried table had zero indexes)
      CREATE INDEX IF NOT EXISTS idx_rc_period_month
        ON revenue_categories (LEFT(period_start, 7));
      CREATE INDEX IF NOT EXISTS idx_rc_category
        ON revenue_categories (category);
      CREATE INDEX IF NOT EXISTS idx_rc_period_end_month
        ON revenue_categories (LEFT(period_end, 7));

      -- Stored plan_category column on auto_renews (eliminates 7x regex duplication)
      ALTER TABLE auto_renews ADD COLUMN IF NOT EXISTS plan_category TEXT;

      UPDATE auto_renews SET plan_category = CASE
        WHEN UPPER(plan_name) LIKE '%SKY3%' OR UPPER(plan_name) LIKE '%SKY5%'
          OR UPPER(plan_name) LIKE '%SKYHIGH%' OR UPPER(plan_name) LIKE '%5 PACK%'
          OR UPPER(plan_name) LIKE '%5-PACK%' THEN 'SKY3'
        WHEN UPPER(plan_name) LIKE '%SKY TING TV%' OR UPPER(plan_name) LIKE '%SKYTING TV%'
          OR UPPER(plan_name) LIKE '%RETREAT TING%' OR UPPER(plan_name) LIKE '%10SKYTING%'
          OR UPPER(plan_name) LIKE '%SKY WEEK TV%' THEN 'SKY_TING_TV'
        WHEN UPPER(plan_name) LIKE '%UNLIMITED%' OR UPPER(plan_name) LIKE '%MEMBER%'
          OR UPPER(plan_name) LIKE '%ALL ACCESS%' OR UPPER(plan_name) LIKE '%TING FAM%'
          THEN 'MEMBER'
        ELSE 'UNKNOWN'
      END WHERE plan_category IS NULL;

      CREATE INDEX IF NOT EXISTS idx_ar_plan_category ON auto_renews(plan_category);

      -- is_first_visit column on registrations (merges first_visits into registrations)
      ALTER TABLE registrations ADD COLUMN IF NOT EXISTS is_first_visit BOOLEAN DEFAULT FALSE;

      UPDATE registrations r SET is_first_visit = TRUE
      FROM first_visits fv
      WHERE LOWER(r.email) = LOWER(fv.email) AND r.attended_at = fv.attended_at;

      CREATE INDEX IF NOT EXISTS idx_reg_is_first_visit
        ON registrations (is_first_visit) WHERE is_first_visit = TRUE;
    `,
  },
  // REAL → NUMERIC(12,2) for all money columns (fixes floating-point precision)
  {
    name: "010_real_to_numeric_money",
    up: `
      ALTER TABLE revenue_categories
        ALTER COLUMN revenue TYPE NUMERIC(12,2) USING revenue::NUMERIC(12,2),
        ALTER COLUMN union_fees TYPE NUMERIC(12,2) USING union_fees::NUMERIC(12,2),
        ALTER COLUMN stripe_fees TYPE NUMERIC(12,2) USING stripe_fees::NUMERIC(12,2),
        ALTER COLUMN other_fees TYPE NUMERIC(12,2) USING other_fees::NUMERIC(12,2),
        ALTER COLUMN transfers TYPE NUMERIC(12,2) USING transfers::NUMERIC(12,2),
        ALTER COLUMN refunded TYPE NUMERIC(12,2) USING refunded::NUMERIC(12,2),
        ALTER COLUMN union_fees_refunded TYPE NUMERIC(12,2) USING union_fees_refunded::NUMERIC(12,2),
        ALTER COLUMN net_revenue TYPE NUMERIC(12,2) USING net_revenue::NUMERIC(12,2);

      ALTER TABLE auto_renews
        ALTER COLUMN plan_price TYPE NUMERIC(12,2) USING plan_price::NUMERIC(12,2);

      ALTER TABLE first_visits
        ALTER COLUMN revenue TYPE NUMERIC(12,2) USING revenue::NUMERIC(12,2);

      ALTER TABLE registrations
        ALTER COLUMN revenue TYPE NUMERIC(12,2) USING revenue::NUMERIC(12,2);

      ALTER TABLE orders
        ALTER COLUMN total TYPE NUMERIC(12,2) USING total::NUMERIC(12,2);

      ALTER TABLE customers
        ALTER COLUMN total_spent TYPE NUMERIC(12,2) USING total_spent::NUMERIC(12,2),
        ALTER COLUMN ltv TYPE NUMERIC(12,2) USING ltv::NUMERIC(12,2);
    `,
  },
  // Email normalization — lowercase all emails + deduplicate
  {
    name: "011_email_normalization",
    up: `
      -- Lowercase all stored emails
      UPDATE auto_renews SET customer_email = LOWER(customer_email)
        WHERE customer_email IS NOT NULL AND customer_email != LOWER(customer_email);
      UPDATE registrations SET email = LOWER(email)
        WHERE email IS NOT NULL AND email != LOWER(email);
      UPDATE first_visits SET email = LOWER(email)
        WHERE email IS NOT NULL AND email != LOWER(email);
      UPDATE orders SET email = LOWER(email)
        WHERE email IS NOT NULL AND email != LOWER(email);
      UPDATE new_customers SET email = LOWER(email)
        WHERE email IS NOT NULL AND email != LOWER(email);
      UPDATE customers SET email = LOWER(email)
        WHERE email IS NOT NULL AND email != LOWER(email);

      -- Deduplicate rows that would conflict after lowercasing
      DELETE FROM auto_renews a USING auto_renews b
        WHERE LOWER(a.customer_email) = LOWER(b.customer_email)
          AND a.plan_name = b.plan_name AND a.created_at = b.created_at AND a.id < b.id;

      DELETE FROM registrations a USING registrations b
        WHERE LOWER(a.email) = LOWER(b.email)
          AND a.attended_at = b.attended_at AND a.id < b.id;

      DELETE FROM first_visits a USING first_visits b
        WHERE LOWER(a.email) = LOWER(b.email)
          AND a.attended_at = b.attended_at AND a.id < b.id;

      DELETE FROM new_customers a USING new_customers b
        WHERE LOWER(a.email) = LOWER(b.email) AND a.id < b.id;

      DELETE FROM customers a USING customers b
        WHERE LOWER(a.email) = LOWER(b.email) AND a.id < b.id;
    `,
  },
  // TEXT → DATE conversion for all date columns
  {
    name: "012_text_to_date_columns",
    up: `
      -- Drop indexes that depend on TEXT-specific expressions BEFORE altering column types.
      -- Functional indexes incompatible with DATE:
      DROP INDEX IF EXISTS idx_rc_period_month;
      DROP INDEX IF EXISTS idx_rc_period_end_month;
      DROP INDEX IF EXISTS idx_reg_attended_at;
      -- Unique indexes on date columns — TEXT→DATE may collapse duplicates
      -- (e.g. "2025-01-15" and "2025-01-15 00:00" both become same DATE):
      DROP INDEX IF EXISTS idx_ar_dedup;
      DROP INDEX IF EXISTS idx_fv_dedup;
      DROP INDEX IF EXISTS idx_reg_dedup;
      -- Composite indexes that include date columns:
      DROP INDEX IF EXISTS idx_reg_email_lower_attended;
      DROP INDEX IF EXISTS idx_ar_email_lower_created;
      DROP INDEX IF EXISTS idx_fv_email_lower_attended;

      -- Clean empty strings → NULL (DATE columns cannot store '')
      UPDATE revenue_categories SET period_start = NULL WHERE period_start = '';
      UPDATE revenue_categories SET period_end = NULL WHERE period_end = '';
      UPDATE auto_renews SET created_at = NULL WHERE created_at = '';
      UPDATE auto_renews SET canceled_at = NULL WHERE canceled_at = '';
      UPDATE registrations SET performance_starts_at = NULL WHERE performance_starts_at = '';
      UPDATE registrations SET registered_at = NULL WHERE registered_at = '';
      UPDATE registrations SET attended_at = NULL WHERE attended_at = '';
      UPDATE registrations SET canceled_at = NULL WHERE canceled_at = '';
      UPDATE first_visits SET performance_starts_at = NULL WHERE performance_starts_at = '';
      UPDATE first_visits SET registered_at = NULL WHERE registered_at = '';
      UPDATE first_visits SET attended_at = NULL WHERE attended_at = '';
      UPDATE orders SET created_at = NULL WHERE created_at = '';
      UPDATE new_customers SET created_at = NULL WHERE created_at = '';
      UPDATE customers SET created_at = NULL WHERE created_at = '';
      UPDATE pipeline_runs SET date_range_start = NULL WHERE date_range_start = '';
      UPDATE pipeline_runs SET date_range_end = NULL WHERE date_range_end = '';

      -- Normalize non-ISO date formats (e.g. "2/8/26 1:04 AM") → YYYY-MM-DD
      -- These come from Shopify/order imports with M/D/YY timestamps
      UPDATE orders SET created_at = TO_CHAR(
        TO_TIMESTAMP(created_at, 'MM/DD/YY HH12:MI AM'), 'YYYY-MM-DD'
      ) WHERE created_at ~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{2,4}';

      UPDATE auto_renews SET created_at = TO_CHAR(
        TO_TIMESTAMP(created_at, 'MM/DD/YY HH12:MI AM'), 'YYYY-MM-DD'
      ) WHERE created_at ~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{2,4}';

      UPDATE auto_renews SET canceled_at = TO_CHAR(
        TO_TIMESTAMP(canceled_at, 'MM/DD/YY HH12:MI AM'), 'YYYY-MM-DD'
      ) WHERE canceled_at ~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{2,4}';

      UPDATE new_customers SET created_at = TO_CHAR(
        TO_TIMESTAMP(created_at, 'MM/DD/YY HH12:MI AM'), 'YYYY-MM-DD'
      ) WHERE created_at ~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{2,4}';

      -- first_visits has "Fri, 1/16/26 12:22 PM EST" format — null these out
      -- (too varied to normalize; small number of rows)

      -- Null out any remaining unparseable values rather than crash
      UPDATE orders SET created_at = NULL
        WHERE created_at IS NOT NULL AND created_at !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}';
      UPDATE auto_renews SET created_at = NULL
        WHERE created_at IS NOT NULL AND created_at !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}';
      UPDATE auto_renews SET canceled_at = NULL
        WHERE canceled_at IS NOT NULL AND canceled_at !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}';
      UPDATE registrations SET attended_at = NULL
        WHERE attended_at IS NOT NULL AND attended_at !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}';
      UPDATE registrations SET registered_at = NULL
        WHERE registered_at IS NOT NULL AND registered_at !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}';
      UPDATE registrations SET performance_starts_at = NULL
        WHERE performance_starts_at IS NOT NULL AND performance_starts_at !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}';
      UPDATE registrations SET canceled_at = NULL
        WHERE canceled_at IS NOT NULL AND canceled_at !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}';
      UPDATE first_visits SET attended_at = NULL
        WHERE attended_at IS NOT NULL AND attended_at !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}';
      UPDATE first_visits SET registered_at = NULL
        WHERE registered_at IS NOT NULL AND registered_at !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}';
      UPDATE first_visits SET performance_starts_at = NULL
        WHERE performance_starts_at IS NOT NULL AND performance_starts_at !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}';
      UPDATE new_customers SET created_at = NULL
        WHERE created_at IS NOT NULL AND created_at !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}';
      UPDATE customers SET created_at = NULL
        WHERE created_at IS NOT NULL AND created_at !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}';
      UPDATE pipeline_runs SET date_range_start = NULL
        WHERE date_range_start IS NOT NULL AND date_range_start !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}';
      UPDATE pipeline_runs SET date_range_end = NULL
        WHERE date_range_end IS NOT NULL AND date_range_end !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}';

      -- Convert TEXT → DATE
      ALTER TABLE revenue_categories
        ALTER COLUMN period_start TYPE DATE USING period_start::DATE,
        ALTER COLUMN period_end TYPE DATE USING period_end::DATE;

      ALTER TABLE pipeline_runs
        ALTER COLUMN date_range_start TYPE DATE USING date_range_start::DATE,
        ALTER COLUMN date_range_end TYPE DATE USING date_range_end::DATE;

      ALTER TABLE auto_renews
        ALTER COLUMN created_at TYPE DATE USING created_at::DATE,
        ALTER COLUMN canceled_at TYPE DATE USING canceled_at::DATE;

      ALTER TABLE first_visits
        ALTER COLUMN performance_starts_at TYPE DATE USING performance_starts_at::DATE,
        ALTER COLUMN registered_at TYPE DATE USING registered_at::DATE,
        ALTER COLUMN attended_at TYPE DATE USING attended_at::DATE;

      ALTER TABLE registrations
        ALTER COLUMN performance_starts_at TYPE DATE USING performance_starts_at::DATE,
        ALTER COLUMN registered_at TYPE DATE USING registered_at::DATE,
        ALTER COLUMN attended_at TYPE DATE USING attended_at::DATE,
        ALTER COLUMN canceled_at TYPE DATE USING canceled_at::DATE;

      ALTER TABLE orders ALTER COLUMN created_at TYPE DATE USING created_at::DATE;
      ALTER TABLE new_customers ALTER COLUMN created_at TYPE DATE USING created_at::DATE;
      ALTER TABLE customers ALTER COLUMN created_at TYPE DATE USING created_at::DATE;

      -- Deduplicate rows that now collide after TEXT→DATE conversion
      DELETE FROM auto_renews a USING auto_renews b
        WHERE a.customer_email = b.customer_email
          AND a.plan_name = b.plan_name
          AND a.created_at = b.created_at
          AND a.id < b.id;

      DELETE FROM registrations a USING registrations b
        WHERE a.email = b.email
          AND a.attended_at = b.attended_at
          AND a.id < b.id;

      DELETE FROM first_visits a USING first_visits b
        WHERE a.email = b.email
          AND a.attended_at = b.attended_at
          AND a.id < b.id;

      -- Recreate all dropped indexes using DATE-compatible expressions
      -- Note: DATE_TRUNC is STABLE not IMMUTABLE, so use plain column index
      CREATE INDEX idx_rc_period_month ON revenue_categories(period_start);
      CREATE INDEX idx_reg_attended_at ON registrations(attended_at) WHERE attended_at IS NOT NULL;
      CREATE UNIQUE INDEX idx_ar_dedup ON auto_renews(customer_email, plan_name, created_at);
      CREATE UNIQUE INDEX idx_fv_dedup ON first_visits(email, attended_at);
      CREATE UNIQUE INDEX idx_reg_dedup ON registrations(email, attended_at);
      CREATE INDEX idx_reg_email_lower_attended ON registrations(LOWER(email), attended_at);
      CREATE INDEX idx_ar_email_lower_created ON auto_renews(LOWER(customer_email), created_at);
      CREATE INDEX idx_fv_email_lower_attended ON first_visits(LOWER(email), attended_at);
    `,
  },
  // Persistent cache for Union.fit lookup tables.
  // Full exports include populated pass_types.csv and revenue_categories.csv,
  // but daily exports have these as header-only. Cache from full exports so
  // daily pipeline runs can resolve revenue categories.
  {
    name: "013_lookup_table_cache",
    up: `
      CREATE TABLE IF NOT EXISTS pass_type_lookups (
        id TEXT PRIMARY KEY,
        name TEXT,
        revenue_category_id TEXT,
        fees_outside BOOLEAN DEFAULT FALSE,
        created_at TEXT,
        cached_at TIMESTAMPTZ DEFAULT NOW()
      );
      CREATE TABLE IF NOT EXISTS revenue_category_lookups (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        cached_at TIMESTAMPTZ DEFAULT NOW()
      );
    `,
  },
  // ── Self-Sustaining Pipeline Architecture ────────────────────
  // Enrich orders table with fields needed for DB-based revenue computation.
  // Add refunds + transfers tables so revenue can be computed from accumulated
  // DB data instead of from each daily CSV export (which is only a 24-hour delta).
  {
    name: "014_enrich_orders_and_revenue_tables",
    up: `
      -- Enrich orders table for DB-based revenue computation
      ALTER TABLE orders ADD COLUMN IF NOT EXISTS state TEXT;
      ALTER TABLE orders ADD COLUMN IF NOT EXISTS completed_at DATE;
      ALTER TABLE orders ADD COLUMN IF NOT EXISTS fee_union_total NUMERIC(12,2) DEFAULT 0;
      ALTER TABLE orders ADD COLUMN IF NOT EXISTS fee_payment_total NUMERIC(12,2) DEFAULT 0;
      ALTER TABLE orders ADD COLUMN IF NOT EXISTS fees_outside BOOLEAN DEFAULT FALSE;
      ALTER TABLE orders ADD COLUMN IF NOT EXISTS subscription_pass_id TEXT;
      ALTER TABLE orders ADD COLUMN IF NOT EXISTS revenue_category TEXT;

      CREATE INDEX IF NOT EXISTS idx_orders_revenue_month
        ON orders(created_at) WHERE created_at IS NOT NULL;
      CREATE INDEX IF NOT EXISTS idx_orders_revenue_category
        ON orders(revenue_category) WHERE revenue_category IS NOT NULL;

      -- Backfill revenue_category from order_type using inferCategoryFromName logic
      UPDATE orders SET revenue_category = CASE
        WHEN order_type ~* 'sky\\s*3|sky\\s*three|3.?pack' THEN 'SKY3 / Packs'
        WHEN order_type ~* 'sky\\s*ting\\s*tv|sttv|retreat\\s*ting' THEN 'SKY TING TV'
        WHEN order_type ~* 'intro|trial' THEN 'Intro / Trial'
        WHEN order_type ~* 'member' AND NOT order_type ~* 'sky\\s*3|sky\\s*ting\\s*tv' THEN 'Members'
        WHEN order_type ~* 'drop.?in|single\\s*class' THEN 'Drop-Ins'
        WHEN order_type ~* 'workshop' THEN 'Workshops'
        WHEN order_type ~* 'spa|wellness|massage|facial' THEN 'Wellness / Spa'
        WHEN order_type ~* 'teacher\\s*training|training' THEN 'Teacher Training'
        WHEN order_type ~* 'retail|merch|merchandise|shop' THEN 'Retail / Merch'
        WHEN order_type ~* 'private|1.on.1|one.on.one' THEN 'Privates'
        WHEN order_type ~* 'donat' THEN 'Donations'
        WHEN order_type ~* 'rental|rent' THEN 'Rentals'
        WHEN order_type ~* 'retreat' THEN 'Retreats'
        WHEN order_type ~* 'community' THEN 'Community'
        ELSE 'Uncategorized'
      END WHERE revenue_category IS NULL;

      -- Refunds table (for accurate refund tracking in DB-based revenue)
      CREATE TABLE IF NOT EXISTS refunds (
        id TEXT PRIMARY KEY,
        created_at DATE,
        order_id TEXT,
        revenue_category_id TEXT,
        revenue_category TEXT,
        state TEXT,
        amount_refunded NUMERIC(12,2) DEFAULT 0,
        fee_union_total_refunded NUMERIC(12,2) DEFAULT 0,
        payout_total NUMERIC(12,2) DEFAULT 0,
        to_balance BOOLEAN DEFAULT FALSE,
        reason TEXT,
        imported_at TIMESTAMPTZ DEFAULT NOW()
      );

      -- Transfers table (video usage debits, etc.)
      CREATE TABLE IF NOT EXISTS transfers (
        id TEXT PRIMARY KEY,
        created_at DATE,
        payout_total NUMERIC(12,2) DEFAULT 0,
        description TEXT,
        type TEXT,
        imported_at TIMESTAMPTZ DEFAULT NOW()
      );
    `,
  },
  {
    name: "015_passes_table",
    up: `
      -- Passes table for DB-based 7-step revenue computation.
      -- The algorithm requires passes for:
      --   Step 2: non-subscription pass revenue (uses pass.total, not order.total)
      --   Step 3: excluding pass orders from non-pass order revenue
      --   Step 4A: pass-linked refunds (uses pass.total, not refund.amount_refunded)
      CREATE TABLE IF NOT EXISTS passes (
        id TEXT PRIMARY KEY,
        pass_category_name TEXT,
        order_id TEXT,
        refund_id TEXT,
        pass_type_id TEXT,
        name TEXT,
        total NUMERIC(12,2) DEFAULT 0,
        fee_union_total NUMERIC(12,2) DEFAULT 0,
        fee_payment_total NUMERIC(12,2) DEFAULT 0,
        fees_outside BOOLEAN DEFAULT FALSE,
        membership_id TEXT,
        state TEXT,
        imported_at TIMESTAMPTZ DEFAULT NOW()
      );
      CREATE INDEX IF NOT EXISTS idx_passes_order_id ON passes(order_id);
      CREATE INDEX IF NOT EXISTS idx_passes_refund_id ON passes(refund_id) WHERE refund_id IS NOT NULL AND refund_id != '';
      CREATE INDEX IF NOT EXISTS idx_passes_pass_type ON passes(pass_type_id);
    `,
  },
  // Usage redesign: weekly visit snapshots for tier tracking
  {
    name: "016_member_weekly_visits",
    up: `
      CREATE TABLE IF NOT EXISTS member_weekly_visits (
        id BIGSERIAL PRIMARY KEY,
        member_email TEXT NOT NULL,
        segment TEXT NOT NULL,
        week_start DATE NOT NULL,
        visit_count INTEGER NOT NULL DEFAULT 0,
        tier TEXT NOT NULL DEFAULT '',
        created_at TIMESTAMPTZ DEFAULT NOW()
      );
      CREATE UNIQUE INDEX IF NOT EXISTS idx_mwv_email_seg_week
        ON member_weekly_visits(member_email, segment, week_start);
      CREATE INDEX IF NOT EXISTS idx_mwv_segment_week
        ON member_weekly_visits(segment, week_start);
      CREATE INDEX IF NOT EXISTS idx_mwv_email_week
        ON member_weekly_visits(member_email, week_start);
    `,
  },
  // Usage redesign: tier-to-tier transitions for migration tracking
  {
    name: "017_member_tier_transitions",
    up: `
      CREATE TABLE IF NOT EXISTS member_tier_transitions (
        id BIGSERIAL PRIMARY KEY,
        member_email TEXT NOT NULL,
        member_name TEXT,
        segment TEXT NOT NULL,
        period_start DATE NOT NULL,
        period_end DATE NOT NULL,
        prior_tier TEXT NOT NULL,
        current_tier TEXT NOT NULL,
        direction TEXT NOT NULL,
        prior_visits INTEGER NOT NULL DEFAULT 0,
        current_visits INTEGER NOT NULL DEFAULT 0,
        subscribed_both BOOLEAN NOT NULL DEFAULT false,
        created_at TIMESTAMPTZ DEFAULT NOW()
      );
      CREATE UNIQUE INDEX IF NOT EXISTS idx_mtt_email_seg_period
        ON member_tier_transitions(member_email, segment, period_start);
      CREATE INDEX IF NOT EXISTS idx_mtt_segment_period
        ON member_tier_transitions(segment, period_start);
      CREATE INDEX IF NOT EXISTS idx_mtt_direction_seg_period
        ON member_tier_transitions(direction, segment, period_start);
    `,
  },
  // Usage redesign: week annotations for marking holidays, weather, promos
  {
    name: "018_week_annotations",
    up: `
      CREATE TABLE IF NOT EXISTS week_annotations (
        id SERIAL PRIMARY KEY,
        week_start DATE NOT NULL UNIQUE,
        label TEXT NOT NULL,
        created_at TIMESTAMPTZ DEFAULT NOW()
      );
    `,
  },
  // Append-only event log for subscription state changes.
  //
  // Problem: auto_renews is UPSERT'd in place, so state-change history is
  // overwritten. The overview dashboard's churn counts miss most cancellations
  // because Union.fit leaves users in 'Pending Cancel' until their paid period
  // ends and sets canceled_at to the period-end date, not the cancel-click date.
  //
  // Fix: a DB trigger on auto_renews writes one event row per state change.
  // Dashboard counts churn/signups by window on this log instead of static
  // columns. Trigger-based so every code path that modifies auto_renews
  // (saveAutoRenews UPDATE-by-pass-id path, INSERT ... ON CONFLICT DO UPDATE,
  // reconcileAutoRenews bulk cancel) is captured uniformly.
  //
  // Semantic change: churn counts are higher than before because Pending Cancel
  // transitions now count as churn. Pending Cancel → Canceled is logged as
  // 'final_cancel' and excluded from the dashboard so it's not double-counted.
  {
    name: "019_auto_renew_events_log",
    up: `
      CREATE TABLE IF NOT EXISTS auto_renew_events (
        id BIGSERIAL PRIMARY KEY,
        auto_renew_id INTEGER,
        customer_email TEXT NOT NULL,
        plan_name TEXT,
        plan_category TEXT,
        prev_state TEXT,
        new_state TEXT NOT NULL,
        event_type TEXT NOT NULL CHECK (event_type IN
          ('signup','churn','final_cancel','resume','transition','backfill_signup','backfill_churn')),
        snapshot_id TEXT,
        observed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        is_backfill BOOLEAN NOT NULL DEFAULT FALSE
      );

      CREATE INDEX IF NOT EXISTS idx_are_observed
        ON auto_renew_events(observed_at);
      CREATE INDEX IF NOT EXISTS idx_are_type_observed
        ON auto_renew_events(event_type, observed_at);
      CREATE INDEX IF NOT EXISTS idx_are_cat_type_observed
        ON auto_renew_events(plan_category, event_type, observed_at);
      CREATE INDEX IF NOT EXISTS idx_are_email_observed
        ON auto_renew_events(customer_email, observed_at DESC);

      -- Makes backfill idempotent: re-running the migration block won't dupe.
      CREATE UNIQUE INDEX IF NOT EXISTS idx_are_backfill_dedup
        ON auto_renew_events(auto_renew_id, event_type)
        WHERE is_backfill = TRUE;

      -- ── Trigger function: log state transitions ────────────
      CREATE OR REPLACE FUNCTION auto_renew_log_event() RETURNS TRIGGER AS $fn$
      DECLARE
        churned_states CONSTANT TEXT[] := ARRAY['Canceled','Pending Cancel'];
        old_churned BOOLEAN;
        new_churned BOOLEAN;
        ev_type TEXT;
      BEGIN
        IF TG_OP = 'INSERT' THEN
          new_churned := NEW.plan_state = ANY(churned_states);
          ev_type := CASE WHEN new_churned THEN 'churn' ELSE 'signup' END;
          INSERT INTO auto_renew_events (
            auto_renew_id, customer_email, plan_name, plan_category,
            prev_state, new_state, event_type, snapshot_id, observed_at, is_backfill
          ) VALUES (
            NEW.id, NEW.customer_email, NEW.plan_name, NEW.plan_category,
            NULL, NEW.plan_state, ev_type, NEW.snapshot_id, NOW(), FALSE
          );
          RETURN NEW;
        END IF;

        -- UPDATE: only log on actual state change
        IF OLD.plan_state IS NOT DISTINCT FROM NEW.plan_state THEN
          RETURN NEW;
        END IF;

        old_churned := OLD.plan_state = ANY(churned_states);
        new_churned := NEW.plan_state = ANY(churned_states);

        IF new_churned AND NOT old_churned THEN
          ev_type := 'churn';
        ELSIF old_churned AND NOT new_churned THEN
          ev_type := 'resume';
        ELSIF OLD.plan_state = 'Pending Cancel' AND NEW.plan_state = 'Canceled' THEN
          ev_type := 'final_cancel';
        ELSE
          ev_type := 'transition';
        END IF;

        INSERT INTO auto_renew_events (
          auto_renew_id, customer_email, plan_name, plan_category,
          prev_state, new_state, event_type, snapshot_id, observed_at, is_backfill
        ) VALUES (
          NEW.id, NEW.customer_email, NEW.plan_name, NEW.plan_category,
          OLD.plan_state, NEW.plan_state, ev_type, NEW.snapshot_id, NOW(), FALSE
        );
        RETURN NEW;
      END;
      $fn$ LANGUAGE plpgsql;

      DROP TRIGGER IF EXISTS trg_auto_renew_log_event ON auto_renews;
      CREATE TRIGGER trg_auto_renew_log_event
        AFTER INSERT OR UPDATE ON auto_renews
        FOR EACH ROW
        EXECUTE FUNCTION auto_renew_log_event();

      -- ── Backfill from existing data ────────────────────────
      -- Signup event per row with a known created_at. Treats created_at (DATE)
      -- as ET local midnight. ON CONFLICT uses the partial unique index.
      INSERT INTO auto_renew_events (
        auto_renew_id, customer_email, plan_name, plan_category,
        prev_state, new_state, event_type, snapshot_id, observed_at, is_backfill
      )
      SELECT
        id, customer_email, plan_name, plan_category,
        NULL, plan_state, 'backfill_signup', snapshot_id,
        (created_at::timestamp AT TIME ZONE 'America/New_York'),
        TRUE
      FROM auto_renews
      WHERE created_at IS NOT NULL
        AND customer_email IS NOT NULL
      ON CONFLICT (auto_renew_id, event_type) WHERE is_backfill = TRUE DO NOTHING;

      -- Churn event per currently-churned row with a known canceled_at.
      -- Rows with NULL canceled_at are intentionally skipped to avoid piling
      -- synthetic events on migration day.
      INSERT INTO auto_renew_events (
        auto_renew_id, customer_email, plan_name, plan_category,
        prev_state, new_state, event_type, snapshot_id, observed_at, is_backfill
      )
      SELECT
        id, customer_email, plan_name, plan_category,
        NULL, plan_state, 'backfill_churn', snapshot_id,
        (canceled_at::timestamp AT TIME ZONE 'America/New_York'),
        TRUE
      FROM auto_renews
      WHERE plan_state IN ('Canceled','Pending Cancel')
        AND canceled_at IS NOT NULL
        AND customer_email IS NOT NULL
      ON CONFLICT (auto_renew_id, event_type) WHERE is_backfill = TRUE DO NOTHING;
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
