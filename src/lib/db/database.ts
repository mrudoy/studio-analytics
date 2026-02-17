import { Pool } from "pg";

// Singleton pool with HMR guard for Next.js dev mode
const globalForDb = globalThis as unknown as { pgPool?: Pool };

export function getPool(): Pool {
  if (globalForDb.pgPool) return globalForDb.pgPool;

  const connectionString = process.env.DATABASE_URL;
  if (!connectionString) {
    throw new Error(
      "DATABASE_URL environment variable is required. " +
      "Example: postgresql://postgres:postgres@localhost:5432/studio_analytics"
    );
  }

  const pool = new Pool({
    connectionString,
    max: 10,
    // Railway Postgres requires SSL
    ssl: connectionString.includes("railway")
      ? { rejectUnauthorized: false }
      : undefined,
  });

  pool.on("error", (err) => {
    console.error("[db] Unexpected pool error:", err);
  });

  globalForDb.pgPool = pool;
  console.log("[db] PostgreSQL pool created");
  return pool;
}

/**
 * Initialize database schema. Must be called once on app startup
 * (e.g. from instrumentation.ts) before any store functions.
 */
export async function initDatabase(): Promise<void> {
  const pool = getPool();

  // Create all tables
  await pool.query(`
    CREATE TABLE IF NOT EXISTS revenue_categories (
      id SERIAL PRIMARY KEY,
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
      created_at TIMESTAMPTZ DEFAULT NOW(),
      UNIQUE(period_start, period_end, category)
    );

    CREATE TABLE IF NOT EXISTS pipeline_runs (
      id SERIAL PRIMARY KEY,
      ran_at TIMESTAMPTZ DEFAULT NOW(),
      date_range_start TEXT,
      date_range_end TEXT,
      record_counts TEXT,
      duration_ms INTEGER
    );

    CREATE TABLE IF NOT EXISTS uploaded_data (
      id SERIAL PRIMARY KEY,
      filename TEXT NOT NULL,
      data_type TEXT NOT NULL,
      period TEXT,
      content TEXT NOT NULL,
      uploaded_at TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS auto_renews (
      id SERIAL PRIMARY KEY,
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
      imported_at TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS first_visits (
      id SERIAL PRIMARY KEY,
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
      imported_at TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS registrations (
      id SERIAL PRIMARY KEY,
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
      imported_at TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS orders (
      id SERIAL PRIMARY KEY,
      created_at TEXT,
      code TEXT,
      customer TEXT,
      order_type TEXT,
      payment TEXT,
      total REAL DEFAULT 0,
      imported_at TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS new_customers (
      id SERIAL PRIMARY KEY,
      name TEXT,
      email TEXT,
      role TEXT,
      order_count INTEGER DEFAULT 0,
      created_at TEXT,
      imported_at TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS customers (
      id SERIAL PRIMARY KEY,
      union_id TEXT,
      first_name TEXT,
      last_name TEXT,
      email TEXT UNIQUE,
      phone TEXT,
      role TEXT,
      total_spent REAL DEFAULT 0,
      ltv REAL DEFAULT 0,
      order_count INTEGER DEFAULT 0,
      current_free_pass INTEGER DEFAULT 0,
      current_free_auto_renew INTEGER DEFAULT 0,
      current_paid_pass INTEGER DEFAULT 0,
      current_paid_auto_renew INTEGER DEFAULT 0,
      current_payment_plan INTEGER DEFAULT 0,
      livestream_registrations INTEGER DEFAULT 0,
      inperson_registrations INTEGER DEFAULT 0,
      replay_registrations INTEGER DEFAULT 0,
      livestream_redeemed INTEGER DEFAULT 0,
      inperson_redeemed INTEGER DEFAULT 0,
      replay_redeemed INTEGER DEFAULT 0,
      instagram TEXT,
      notes TEXT,
      birthday TEXT,
      how_heard TEXT,
      goals TEXT,
      neighborhood TEXT,
      inspiration TEXT,
      practice_frequency TEXT,
      created_at TEXT,
      imported_at TIMESTAMPTZ DEFAULT NOW()
    );
  `);

  // Performance indexes
  await pool.query(`
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
    CREATE INDEX IF NOT EXISTS idx_customers_email ON customers(email);
    CREATE INDEX IF NOT EXISTS idx_ar_email ON auto_renews(customer_email);
  `);

  // Unique indexes for dedup (used with ON CONFLICT DO NOTHING)
  await pool.query(`CREATE UNIQUE INDEX IF NOT EXISTS idx_fv_dedup ON first_visits(email, attended_at)`);
  await pool.query(`CREATE UNIQUE INDEX IF NOT EXISTS idx_reg_dedup ON registrations(email, attended_at)`);
  await pool.query(`CREATE UNIQUE INDEX IF NOT EXISTS idx_orders_dedup ON orders(code)`);
  await pool.query(`CREATE UNIQUE INDEX IF NOT EXISTS idx_newcust_dedup ON new_customers(email)`);

  console.log("[db] PostgreSQL schema initialized");
}
