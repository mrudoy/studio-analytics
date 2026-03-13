import { NextRequest, NextResponse } from "next/server";
import { getPool } from "@/lib/db/database";

export const dynamic = "force-dynamic";

export async function GET(request: NextRequest) {
  const month = request.nextUrl.searchParams.get("month") || "2026-01";

  try {
    const pool = getPool();

    const [
      ordersFiltered,
      passesFiltered,
      refundsFiltered,
      transfersFiltered,
      passesTotal,
      ordersTotal,
    ] = await Promise.all([
      pool.query(
        `SELECT COUNT(*) FROM orders WHERE TO_CHAR(created_at, 'YYYY-MM') = $1`,
        [month]
      ),
      pool.query(
        `SELECT COUNT(*) FROM passes p JOIN orders o ON p.order_id = o.union_order_id WHERE TO_CHAR(COALESCE(o.completed_at, o.created_at), 'YYYY-MM') = $1`,
        [month]
      ),
      pool.query(
        `SELECT COUNT(*) FROM refunds WHERE TO_CHAR(created_at, 'YYYY-MM') = $1`,
        [month]
      ),
      pool.query(
        `SELECT COUNT(*) FROM transfers WHERE TO_CHAR(created_at, 'YYYY-MM') = $1`,
        [month]
      ),
      pool.query(`SELECT COUNT(*) FROM passes`),
      pool.query(`SELECT COUNT(*) FROM orders`),
    ]);

    return NextResponse.json({
      month,
      orders: parseInt(ordersFiltered.rows[0].count, 10),
      passes: parseInt(passesFiltered.rows[0].count, 10),
      refunds: parseInt(refundsFiltered.rows[0].count, 10),
      transfers: parseInt(transfersFiltered.rows[0].count, 10),
      totalPasses: parseInt(passesTotal.rows[0].count, 10),
      totalOrders: parseInt(ordersTotal.rows[0].count, 10),
    });
  } catch (e: unknown) {
    return NextResponse.json({ error: (e as Error).message }, { status: 500 });
  }
}
