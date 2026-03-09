import { NextResponse } from "next/server";
import { getPool } from "@/lib/db/database";

export const dynamic = "force-dynamic";

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
