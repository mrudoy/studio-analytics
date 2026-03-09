import { NextResponse } from "next/server";
import { getPool } from "@/lib/db/database";

export const dynamic = "force-dynamic";

export async function GET() {
  const pool = getPool();

  try {
    // Check migrations
    const migrations = await pool.query("SELECT name, applied_at FROM _migrations ORDER BY id");

    // Check column types for key tables
    const rcCols = await pool.query(
      "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'revenue_categories' AND column_name IN ('period_start', 'period_end', 'revenue') ORDER BY ordinal_position"
    );

    const arCols = await pool.query(
      "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'auto_renews' AND column_name IN ('created_at', 'canceled_at', 'plan_price', 'plan_category') ORDER BY ordinal_position"
    );

    const regCols = await pool.query(
      "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'registrations' AND column_name IN ('attended_at', 'registered_at', 'canceled_at', 'revenue', 'is_first_visit') ORDER BY ordinal_position"
    );

    // Check for invalid date values in key TEXT columns
    const checks: Record<string, unknown> = {};
    for (const [table, col] of [
      ["revenue_categories", "period_start"],
      ["auto_renews", "created_at"],
      ["registrations", "attended_at"],
      ["orders", "created_at"],
    ]) {
      try {
        const colType = await pool.query(
          `SELECT data_type FROM information_schema.columns WHERE table_name = $1 AND column_name = $2`,
          [table, col]
        );
        if (colType.rows[0]?.data_type === "text") {
          const r = await pool.query(
            `SELECT "${col}" AS val, COUNT(*) AS cnt FROM ${table}
             WHERE "${col}" IS NOT NULL AND "${col}" != ''
               AND "${col}" !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
             GROUP BY "${col}" ORDER BY cnt DESC LIMIT 5`
          );
          checks[`${table}.${col}_invalid`] = r.rows;
        } else {
          checks[`${table}.${col}_invalid`] = `already ${colType.rows[0]?.data_type}`;
        }
      } catch (e: unknown) {
        checks[`${table}.${col}_invalid`] = (e as Error).message;
      }
    }

    // Check indexes on registrations
    const indexes = await pool.query(
      `SELECT indexname, indexdef FROM pg_indexes WHERE tablename IN ('registrations', 'revenue_categories') ORDER BY tablename, indexname`
    );

    return NextResponse.json({
      migrations: migrations.rows,
      columnTypes: {
        revenue_categories: rcCols.rows,
        auto_renews: arCols.rows,
        registrations: regCols.rows,
      },
      invalidDates: checks,
      indexes: indexes.rows,
    });
  } catch (e: unknown) {
    return NextResponse.json({ error: (e as Error).message }, { status: 500 });
  }
}
