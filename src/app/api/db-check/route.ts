import { NextResponse } from "next/server";
import { getPool } from "@/lib/db/database";
import { unlockMonth, deleteMonthData } from "@/lib/db/revenue-store";

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

    // Category breakdown for recent months
    const catDetail = await pool.query(`
      SELECT
        TO_CHAR(period_start, 'YYYY-MM') AS month,
        category,
        revenue AS gross,
        net_revenue AS net
      FROM revenue_categories
      WHERE period_start >= '2026-01-01'
        AND DATE_TRUNC('month', period_start) = DATE_TRUNC('month', period_end)
      ORDER BY period_start DESC, revenue DESC
    `);

    // Order state breakdown by month (to check if pre-pays are being filtered out)
    const orderStates = await pool.query(`
      SELECT
        TO_CHAR(created_at, 'YYYY-MM') AS month,
        COUNT(*) AS order_count,
        ROUND(SUM(total)::numeric, 0) AS total_revenue
      FROM orders
      WHERE created_at >= '2026-01-01'
      GROUP BY TO_CHAR(created_at, 'YYYY-MM')
      ORDER BY month DESC
    `);

    // Check uploaded_data for revenue uploads
    const uploads = await pool.query(`
      SELECT id, type, file_name, row_count, created_at
      FROM uploaded_data
      ORDER BY created_at DESC
      LIMIT 20
    `);

    // Revenue categories source check — which months have pipeline vs uploaded data?
    const rcSourceCheck = await pool.query(`
      SELECT
        TO_CHAR(period_start, 'YYYY-MM') AS month,
        COUNT(*) AS cat_count,
        MIN(period_start)::text AS first_start,
        MAX(period_end)::text AS last_end,
        ROUND(SUM(revenue)::numeric, 0) AS total_gross
      FROM revenue_categories
      WHERE period_start >= '2025-10-01'
      GROUP BY TO_CHAR(period_start, 'YYYY-MM')
      ORDER BY month DESC
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
      categoryDetail: catDetail.rows,
      ordersByMonth: orderStates.rows,
      uploads: uploads.rows,
      rcSourceCheck: rcSourceCheck.rows,
    });
  } catch (e: unknown) {
    return NextResponse.json({ error: (e as Error).message }, { status: 500 });
  }
}

/**
 * POST /api/db-check — Unlock and clear revenue data for specified months.
 * Body: { months: ["2026-01", "2026-02"], action: "unlock-and-clear" }
 */
export async function POST(request: Request) {
  try {
    const body = await request.json();
    const { months, action } = body as { months: string[]; action: string };

    if (action !== "unlock-and-clear") {
      return NextResponse.json({ error: "Unknown action" }, { status: 400 });
    }

    if (!months || !Array.isArray(months) || months.length === 0) {
      return NextResponse.json({ error: "months array required" }, { status: 400 });
    }

    const results: Record<string, { unlocked: number; deleted: number }> = {};

    for (const month of months) {
      const [yearStr, monthStr] = month.split("-");
      const year = parseInt(yearStr);
      const monthNum = parseInt(monthStr);
      if (isNaN(year) || isNaN(monthNum)) continue;

      const unlocked = await unlockMonth(year, monthNum);
      const deleted = await deleteMonthData(year, monthNum);
      results[month] = { unlocked, deleted };
    }

    return NextResponse.json({ action, results });
  } catch (e: unknown) {
    return NextResponse.json({ error: (e as Error).message }, { status: 500 });
  }
}
