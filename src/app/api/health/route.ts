import { NextResponse } from "next/server";
import { getPool } from "@/lib/db/database";
import { getPipelineQueue } from "@/lib/queue/pipeline-queue";

export async function GET() {
  const checks: Record<string, { status: "ok" | "error"; latencyMs?: number; error?: string }> = {};

  // Check PostgreSQL
  try {
    const pool = getPool();
    const start = Date.now();
    const result = await pool.query("SELECT 1 AS ok");
    checks.postgres = {
      status: result.rows[0]?.ok === 1 ? "ok" : "error",
      latencyMs: Date.now() - start,
    };
  } catch (err) {
    checks.postgres = {
      status: "error",
      error: err instanceof Error ? err.message : "Unknown error",
    };
  }

  // Check Redis via BullMQ queue connection
  try {
    const q = getPipelineQueue();
    const start = Date.now();
    const client = await q.client;
    const pong = await client.ping();
    checks.redis = {
      status: pong === "PONG" ? "ok" : "error",
      latencyMs: Date.now() - start,
    };
  } catch (err) {
    checks.redis = {
      status: "error",
      error: err instanceof Error ? err.message : "Unknown error",
    };
  }

  // Debug: revenue_categories periods
  let dbDebug: Record<string, unknown> = {};
  try {
    const pool = getPool();
    const { rows: periods } = await pool.query(
      `SELECT period_start, period_end, COUNT(*) as cnt, SUM(net_revenue) as total_net
       FROM revenue_categories GROUP BY period_start, period_end ORDER BY period_end DESC LIMIT 5`
    );
    dbDebug.revenuePeriods = periods;

    // Also check auto_renews count
    const { rows: arCount } = await pool.query(`SELECT COUNT(*) as cnt FROM auto_renews`);
    dbDebug.autoRenewCount = arCount[0]?.cnt;

    // Check what getAllPeriods would return
    const { rows: allP } = await pool.query(
      `SELECT period_start, period_end, MAX(locked) as locked, COUNT(*) as category_count,
              SUM(revenue) as total_revenue, SUM(net_revenue) as total_net_revenue
       FROM revenue_categories
       GROUP BY period_start, period_end
       ORDER BY period_end DESC`
    );
    dbDebug.allPeriods = allP.map((r: Record<string, unknown>) => ({
      periodStart: r.period_start,
      periodEnd: r.period_end,
      categoryCount: Number(r.category_count),
      totalRevenue: Number(r.total_revenue),
      totalNetRevenue: Number(r.total_net_revenue),
    }));
  } catch (err) {
    dbDebug.error = err instanceof Error ? err.message : "Unknown";
  }

  const allOk = Object.values(checks).every((c) => c.status === "ok");

  return NextResponse.json(
    {
      status: allOk ? "healthy" : "degraded",
      uptime: Math.round(process.uptime()),
      checks,
      dbDebug,
    },
    { status: allOk ? 200 : 503 }
  );
}
