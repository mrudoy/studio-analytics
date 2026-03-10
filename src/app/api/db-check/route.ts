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

    // Revenue period overlap check
    const revPeriods = await pool.query(`
      SELECT
        TO_CHAR(period_start, 'YYYY-MM') AS month,
        period_start::text AS ps,
        period_end::text AS pe,
        COUNT(*) AS cat_count,
        ROUND(SUM(revenue)::numeric, 0) AS total_gross,
        ROUND(SUM(net_revenue)::numeric, 0) AS total_net
      FROM revenue_categories
      WHERE period_start >= '2025-01-01'
      GROUP BY period_start, period_end
      ORDER BY period_start DESC, period_end DESC
    `);

    // Retreat revenue by month
    const retreatRevenue = await pool.query(`
      SELECT
        TO_CHAR(period_start, 'YYYY-MM') AS month,
        period_start::text AS ps,
        period_end::text AS pe,
        category,
        revenue AS gross,
        net_revenue AS net
      FROM revenue_categories
      WHERE category ~* 'retreat'
        AND period_start >= '2025-01-01'
      ORDER BY period_start DESC
    `);

    return NextResponse.json({
      migrations: migrations.rows.map((r: Record<string, unknown>) => ({
        name: r.name,
        appliedAt: r.applied_at,
      })),
      columnTypes: columnTypes.rows,
      tableCounts: counts.rows,
      revenuePeriods: revPeriods.rows,
      retreatRevenue: retreatRevenue.rows,
    });
  } catch (e: unknown) {
    return NextResponse.json({ error: (e as Error).message }, { status: 500 });
  }
}
