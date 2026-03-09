import { NextResponse } from "next/server";
import { getPool } from "@/lib/db/database";

export const dynamic = "force-dynamic";

export async function GET() {
  const pool = getPool();

  try {
    // Check migrations
    const migrations = await pool.query("SELECT name, applied_at FROM _migrations ORDER BY id");

    // Check column types for date and money columns
    const columnTypes = await pool.query(`
      SELECT table_name, column_name, data_type
      FROM information_schema.columns
      WHERE table_schema = 'public'
        AND (
          column_name IN ('period_start','period_end','created_at','canceled_at',
                         'attended_at','registered_at','performance_starts_at',
                         'date_range_start','date_range_end')
          OR data_type = 'numeric'
        )
      ORDER BY table_name, column_name
    `);

    // Table row counts
    const counts = await pool.query(`
      SELECT relname AS table_name, n_live_tup AS row_count
      FROM pg_stat_user_tables
      ORDER BY n_live_tup DESC
    `);

    return NextResponse.json({
      migrations: migrations.rows.map((r: Record<string, unknown>) => ({
        name: r.name,
        appliedAt: r.applied_at,
      })),
      columnTypes: columnTypes.rows,
      tableCounts: counts.rows,
    });
  } catch (e: unknown) {
    return NextResponse.json({ error: (e as Error).message }, { status: 500 });
  }
}
