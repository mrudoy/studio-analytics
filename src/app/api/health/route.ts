/** Operational endpoint: health check for Railway deployment monitoring. */
import { NextResponse } from "next/server";
import { getPool } from "@/lib/db/database";
import { getPipelineQueue } from "@/lib/queue/pipeline-queue";
import { listBackups } from "@/lib/db/backup";

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

  // Check last backup
  let lastBackup: string | null = null;
  try {
    const backups = await listBackups();
    if (backups.length > 0) {
      lastBackup = backups[0].createdAt;
    }
  } catch {
    // Non-critical — backup table may not exist yet
  }

  const allOk = Object.values(checks).every((c) => c.status === "ok");

  return NextResponse.json(
    {
      status: allOk ? "healthy" : "degraded",
      uptime: Math.round(process.uptime()),
      checks,
      lastBackup,
    },
    { status: allOk ? 200 : 503 }
  );
}
