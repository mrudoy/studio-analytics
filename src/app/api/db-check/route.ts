import { NextResponse } from "next/server";
import { getPool } from "@/lib/db/database";

export const dynamic = "force-dynamic";

/**
 * POST /api/db-check — Manually run migration 012 step-by-step in a transaction.
 * Each step gets a SAVEPOINT so we can capture the exact failing statement.
 * On success, records the migration in _migrations.
 */
export async function POST() {
  const pool = getPool();
  const client = await pool.connect();

  // Check if 012 already applied
  const already = await client.query(
    "SELECT 1 FROM _migrations WHERE name = '012_text_to_date_columns'"
  );
  if (already.rows.length > 0) {
    client.release();
    return NextResponse.json({ status: "already_applied" });
  }

  const steps = [
    { name: "drop idx_rc_period_month", sql: "DROP INDEX IF EXISTS idx_rc_period_month" },
    { name: "drop idx_rc_period_end_month", sql: "DROP INDEX IF EXISTS idx_rc_period_end_month" },
    { name: "drop idx_reg_attended_at", sql: "DROP INDEX IF EXISTS idx_reg_attended_at" },
    // Clean empty strings → NULL
    { name: "clean revenue_categories.period_start", sql: "UPDATE revenue_categories SET period_start = NULL WHERE period_start = ''" },
    { name: "clean revenue_categories.period_end", sql: "UPDATE revenue_categories SET period_end = NULL WHERE period_end = ''" },
    { name: "clean auto_renews.created_at", sql: "UPDATE auto_renews SET created_at = NULL WHERE created_at = ''" },
    { name: "clean auto_renews.canceled_at", sql: "UPDATE auto_renews SET canceled_at = NULL WHERE canceled_at = ''" },
    { name: "clean registrations.performance_starts_at", sql: "UPDATE registrations SET performance_starts_at = NULL WHERE performance_starts_at = ''" },
    { name: "clean registrations.registered_at", sql: "UPDATE registrations SET registered_at = NULL WHERE registered_at = ''" },
    { name: "clean registrations.attended_at", sql: "UPDATE registrations SET attended_at = NULL WHERE attended_at = ''" },
    { name: "clean registrations.canceled_at", sql: "UPDATE registrations SET canceled_at = NULL WHERE canceled_at = ''" },
    { name: "clean first_visits.performance_starts_at", sql: "UPDATE first_visits SET performance_starts_at = NULL WHERE performance_starts_at = ''" },
    { name: "clean first_visits.registered_at", sql: "UPDATE first_visits SET registered_at = NULL WHERE registered_at = ''" },
    { name: "clean first_visits.attended_at", sql: "UPDATE first_visits SET attended_at = NULL WHERE attended_at = ''" },
    { name: "clean orders.created_at", sql: "UPDATE orders SET created_at = NULL WHERE created_at = ''" },
    { name: "clean new_customers.created_at", sql: "UPDATE new_customers SET created_at = NULL WHERE created_at = ''" },
    { name: "clean customers.created_at", sql: "UPDATE customers SET created_at = NULL WHERE created_at = ''" },
    { name: "clean pipeline_runs.date_range_start", sql: "UPDATE pipeline_runs SET date_range_start = NULL WHERE date_range_start = ''" },
    { name: "clean pipeline_runs.date_range_end", sql: "UPDATE pipeline_runs SET date_range_end = NULL WHERE date_range_end = ''" },
    // Normalize M/D/YY timestamps
    { name: "normalize orders M/D/YY", sql: `UPDATE orders SET created_at = TO_CHAR(TO_TIMESTAMP(created_at, 'MM/DD/YY HH12:MI AM'), 'YYYY-MM-DD') WHERE created_at ~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{2,4}'` },
    { name: "normalize auto_renews.created_at M/D/YY", sql: `UPDATE auto_renews SET created_at = TO_CHAR(TO_TIMESTAMP(created_at, 'MM/DD/YY HH12:MI AM'), 'YYYY-MM-DD') WHERE created_at ~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{2,4}'` },
    { name: "normalize auto_renews.canceled_at M/D/YY", sql: `UPDATE auto_renews SET canceled_at = TO_CHAR(TO_TIMESTAMP(canceled_at, 'MM/DD/YY HH12:MI AM'), 'YYYY-MM-DD') WHERE canceled_at ~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{2,4}'` },
    // NULL out remaining non-ISO values
    { name: "null non-ISO orders.created_at", sql: `UPDATE orders SET created_at = NULL WHERE created_at IS NOT NULL AND created_at !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'` },
    { name: "null non-ISO auto_renews.created_at", sql: `UPDATE auto_renews SET created_at = NULL WHERE created_at IS NOT NULL AND created_at !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'` },
    { name: "null non-ISO auto_renews.canceled_at", sql: `UPDATE auto_renews SET canceled_at = NULL WHERE canceled_at IS NOT NULL AND canceled_at !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'` },
    { name: "null non-ISO registrations.attended_at", sql: `UPDATE registrations SET attended_at = NULL WHERE attended_at IS NOT NULL AND attended_at !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'` },
    { name: "null non-ISO registrations.registered_at", sql: `UPDATE registrations SET registered_at = NULL WHERE registered_at IS NOT NULL AND registered_at !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'` },
    { name: "null non-ISO registrations.performance_starts_at", sql: `UPDATE registrations SET performance_starts_at = NULL WHERE performance_starts_at IS NOT NULL AND performance_starts_at !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'` },
    { name: "null non-ISO registrations.canceled_at", sql: `UPDATE registrations SET canceled_at = NULL WHERE canceled_at IS NOT NULL AND canceled_at !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'` },
    { name: "null non-ISO first_visits.attended_at", sql: `UPDATE first_visits SET attended_at = NULL WHERE attended_at IS NOT NULL AND attended_at !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'` },
    { name: "null non-ISO first_visits.registered_at", sql: `UPDATE first_visits SET registered_at = NULL WHERE registered_at IS NOT NULL AND registered_at !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'` },
    { name: "null non-ISO first_visits.performance_starts_at", sql: `UPDATE first_visits SET performance_starts_at = NULL WHERE performance_starts_at IS NOT NULL AND performance_starts_at !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'` },
    { name: "null non-ISO new_customers.created_at", sql: `UPDATE new_customers SET created_at = NULL WHERE created_at IS NOT NULL AND created_at !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'` },
    { name: "null non-ISO customers.created_at", sql: `UPDATE customers SET created_at = NULL WHERE created_at IS NOT NULL AND created_at !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'` },
    { name: "null non-ISO pipeline_runs.date_range_start", sql: `UPDATE pipeline_runs SET date_range_start = NULL WHERE date_range_start IS NOT NULL AND date_range_start !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'` },
    { name: "null non-ISO pipeline_runs.date_range_end", sql: `UPDATE pipeline_runs SET date_range_end = NULL WHERE date_range_end IS NOT NULL AND date_range_end !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'` },
    // ALTER TABLE — TEXT → DATE
    { name: "ALTER revenue_categories dates", sql: `ALTER TABLE revenue_categories ALTER COLUMN period_start TYPE DATE USING period_start::DATE, ALTER COLUMN period_end TYPE DATE USING period_end::DATE` },
    { name: "ALTER pipeline_runs dates", sql: `ALTER TABLE pipeline_runs ALTER COLUMN date_range_start TYPE DATE USING date_range_start::DATE, ALTER COLUMN date_range_end TYPE DATE USING date_range_end::DATE` },
    { name: "ALTER auto_renews dates", sql: `ALTER TABLE auto_renews ALTER COLUMN created_at TYPE DATE USING created_at::DATE, ALTER COLUMN canceled_at TYPE DATE USING canceled_at::DATE` },
    { name: "ALTER first_visits dates", sql: `ALTER TABLE first_visits ALTER COLUMN performance_starts_at TYPE DATE USING performance_starts_at::DATE, ALTER COLUMN registered_at TYPE DATE USING registered_at::DATE, ALTER COLUMN attended_at TYPE DATE USING attended_at::DATE` },
    { name: "ALTER registrations dates", sql: `ALTER TABLE registrations ALTER COLUMN performance_starts_at TYPE DATE USING performance_starts_at::DATE, ALTER COLUMN registered_at TYPE DATE USING registered_at::DATE, ALTER COLUMN attended_at TYPE DATE USING attended_at::DATE, ALTER COLUMN canceled_at TYPE DATE USING canceled_at::DATE` },
    { name: "ALTER orders.created_at", sql: `ALTER TABLE orders ALTER COLUMN created_at TYPE DATE USING created_at::DATE` },
    { name: "ALTER new_customers.created_at", sql: `ALTER TABLE new_customers ALTER COLUMN created_at TYPE DATE USING created_at::DATE` },
    { name: "ALTER customers.created_at", sql: `ALTER TABLE customers ALTER COLUMN created_at TYPE DATE USING created_at::DATE` },
    // Recreate indexes
    { name: "CREATE idx_rc_period_month", sql: `CREATE INDEX idx_rc_period_month ON revenue_categories(DATE_TRUNC('month', period_start))` },
    { name: "CREATE idx_reg_attended_at", sql: `CREATE INDEX idx_reg_attended_at ON registrations(attended_at) WHERE attended_at IS NOT NULL` },
    // Record migration
    { name: "record migration", sql: `INSERT INTO _migrations (name) VALUES ('012_text_to_date_columns')` },
  ];

  const results: { step: string; status: string; rows?: number; error?: string }[] = [];

  try {
    await client.query("BEGIN");

    for (const step of steps) {
      try {
        const r = await client.query(step.sql);
        results.push({ step: step.name, status: "OK", rows: r.rowCount ?? 0 });
      } catch (e: unknown) {
        const errMsg = (e as Error).message;
        results.push({ step: step.name, status: "FAIL", error: errMsg });
        // Abort — rollback and return results
        await client.query("ROLLBACK");
        client.release();
        return NextResponse.json({ status: "failed", failedAt: step.name, error: errMsg, results }, { status: 500 });
      }
    }

    await client.query("COMMIT");
    client.release();
    return NextResponse.json({ status: "success", results });
  } catch (e: unknown) {
    try { await client.query("ROLLBACK"); } catch { /* ignore */ }
    client.release();
    return NextResponse.json({ status: "error", error: (e as Error).message, results }, { status: 500 });
  }
}

export async function GET(req: Request) {
  const pool = getPool();
  const url = new URL(req.url);
  const dryRun = url.searchParams.get("dry-run") === "1";

  try {
    // Check migrations
    const migrations = await pool.query("SELECT name, applied_at FROM _migrations ORDER BY id");

    // Check column types for ALL tables with date columns
    const allTypes = await pool.query(`
      SELECT table_name, column_name, data_type
      FROM information_schema.columns
      WHERE (table_name, column_name) IN (
        ('revenue_categories', 'period_start'), ('revenue_categories', 'period_end'),
        ('auto_renews', 'created_at'), ('auto_renews', 'canceled_at'),
        ('registrations', 'attended_at'), ('registrations', 'registered_at'),
        ('registrations', 'canceled_at'), ('registrations', 'performance_starts_at'),
        ('first_visits', 'attended_at'), ('first_visits', 'registered_at'),
        ('first_visits', 'performance_starts_at'),
        ('orders', 'created_at'), ('new_customers', 'created_at'),
        ('customers', 'created_at'), ('pipeline_runs', 'date_range_start')
      )
      ORDER BY table_name, column_name
    `);

    // Check for invalid date values in ALL TEXT date columns
    const invalidValues: Record<string, unknown> = {};
    for (const row of allTypes.rows) {
      if (row.data_type !== "text") continue;
      try {
        const r = await pool.query(
          `SELECT "${row.column_name}" AS val, COUNT(*) AS cnt FROM ${row.table_name}
           WHERE "${row.column_name}" IS NOT NULL AND "${row.column_name}" != ''
             AND "${row.column_name}" !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
           GROUP BY "${row.column_name}" ORDER BY cnt DESC LIMIT 5`
        );
        if (r.rows.length > 0) {
          invalidValues[`${row.table_name}.${row.column_name}`] = r.rows;
        }
      } catch (e: unknown) {
        invalidValues[`${row.table_name}.${row.column_name}`] = (e as Error).message;
      }
    }

    // Check problematic indexes
    const indexes = await pool.query(`
      SELECT indexname, indexdef FROM pg_indexes
      WHERE indexdef LIKE '%<>%' OR indexdef LIKE '%left(%' OR indexdef LIKE '%"left"%'
      ORDER BY tablename, indexname
    `);

    // If dry-run, test each migration step
    const stepResults: { step: string; result: string }[] = [];
    if (dryRun) {
      const steps = [
        { name: "drop idx_rc_period_month", sql: "DROP INDEX IF EXISTS idx_rc_period_month" },
        { name: "drop idx_rc_period_end_month", sql: "DROP INDEX IF EXISTS idx_rc_period_end_month" },
        { name: "drop idx_reg_attended_at", sql: "DROP INDEX IF EXISTS idx_reg_attended_at" },
        { name: "test cast revenue_categories.period_start", sql: "SELECT period_start::DATE FROM revenue_categories WHERE period_start IS NOT NULL AND period_start != '' LIMIT 1" },
        { name: "test cast auto_renews.created_at", sql: "SELECT created_at::DATE FROM auto_renews WHERE created_at IS NOT NULL AND created_at != '' AND created_at ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}' LIMIT 1" },
        { name: "test cast registrations.attended_at", sql: "SELECT attended_at::DATE FROM registrations WHERE attended_at IS NOT NULL AND attended_at != '' AND attended_at ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}' LIMIT 1" },
        { name: "test cast orders.created_at", sql: "SELECT created_at::DATE FROM orders WHERE created_at IS NOT NULL AND created_at != '' AND created_at ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}' LIMIT 1" },
        { name: "count orders with M/D/YY dates", sql: "SELECT COUNT(*) as cnt FROM orders WHERE created_at ~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{2,4}'" },
        { name: "count auto_renews with non-ISO dates", sql: "SELECT COUNT(*) as cnt FROM auto_renews WHERE created_at IS NOT NULL AND created_at != '' AND created_at !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'" },
        { name: "sample auto_renews non-ISO", sql: "SELECT DISTINCT created_at FROM auto_renews WHERE created_at IS NOT NULL AND created_at != '' AND created_at !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}' LIMIT 5" },
        { name: "sample registrations non-ISO attended_at", sql: "SELECT DISTINCT attended_at FROM registrations WHERE attended_at IS NOT NULL AND attended_at != '' AND attended_at !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}' LIMIT 5" },
        { name: "sample customers non-ISO", sql: "SELECT DISTINCT created_at FROM customers WHERE created_at IS NOT NULL AND created_at != '' AND created_at !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}' LIMIT 5" },
        { name: "test TO_TIMESTAMP orders", sql: "SELECT created_at, TO_CHAR(TO_TIMESTAMP(created_at, 'MM/DD/YY HH12:MI AM'), 'YYYY-MM-DD') as normalized FROM orders WHERE created_at ~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{2,4}' LIMIT 3" },
      ];

      for (const step of steps) {
        try {
          const r = await pool.query(step.sql);
          stepResults.push({ step: step.name, result: `OK (${r.rowCount} rows) ${JSON.stringify(r.rows?.slice(0, 3))}` });
        } catch (e: unknown) {
          stepResults.push({ step: step.name, result: `FAIL: ${(e as Error).message}` });
        }
      }
    }

    return NextResponse.json({
      migrations: migrations.rows.map((r: Record<string, unknown>) => r.name),
      columnTypes: allTypes.rows,
      invalidValues,
      problematicIndexes: indexes.rows,
      ...(dryRun ? { stepResults } : {}),
    });
  } catch (e: unknown) {
    return NextResponse.json({ error: (e as Error).message }, { status: 500 });
  }
}
