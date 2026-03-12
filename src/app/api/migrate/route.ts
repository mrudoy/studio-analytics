import { NextRequest, NextResponse } from "next/server";
import { getPool } from "@/lib/db/database";
import { runMigrations } from "@/lib/db/migrations";

export const dynamic = "force-dynamic";

/**
 * POST /api/migrate — Force-run pending database migrations.
 * GET  /api/migrate — Check which migrations have been applied.
 */
export async function GET(request: NextRequest) {
  const authHeader = request.headers.get("authorization");
  const cronSecret = process.env.CRON_SECRET;
  if (cronSecret && authHeader !== `Bearer ${cronSecret}`) {
    return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
  }

  try {
    const pool = getPool();

    // Check applied migrations
    const { rows: migrations } = await pool.query(
      "SELECT name, applied_at FROM _migrations ORDER BY id"
    );

    // Check orders table columns
    const { rows: columns } = await pool.query(`
      SELECT column_name, data_type
      FROM information_schema.columns
      WHERE table_name = 'orders'
      ORDER BY ordinal_position
    `);

    // Check if passes table exists
    const { rows: tables } = await pool.query(`
      SELECT table_name FROM information_schema.tables
      WHERE table_schema = 'public' AND table_name IN ('passes', 'refunds', 'transfers')
      ORDER BY table_name
    `);

    return NextResponse.json({
      appliedMigrations: migrations,
      ordersColumns: columns.map((c) => c.column_name),
      tablesExist: tables.map((t) => t.table_name),
    });
  } catch (e: unknown) {
    return NextResponse.json({ error: (e as Error).message }, { status: 500 });
  }
}

export async function POST(request: NextRequest) {
  const authHeader = request.headers.get("authorization");
  const cronSecret = process.env.CRON_SECRET;
  if (cronSecret && authHeader !== `Bearer ${cronSecret}`) {
    return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
  }

  try {
    await runMigrations();

    // Verify the orders columns after migration
    const pool = getPool();
    const { rows: columns } = await pool.query(`
      SELECT column_name FROM information_schema.columns
      WHERE table_name = 'orders' ORDER BY ordinal_position
    `);

    return NextResponse.json({
      success: true,
      message: "Migrations applied successfully",
      ordersColumns: columns.map((c) => c.column_name),
    });
  } catch (e: unknown) {
    return NextResponse.json({ error: (e as Error).message }, { status: 500 });
  }
}
