/** Operational endpoint: health check for Railway deployment monitoring. */
import { NextResponse } from "next/server";
import { getPool } from "@/lib/db/database";
import { getPipelineQueue } from "@/lib/queue/pipeline-queue";
import { listBackups } from "@/lib/db/backup";
import { getDataFreshness } from "@/lib/union-api/fetch-export";
import { checkAndCatchUp } from "@/lib/queue/catchup";
import { sendPipelineAlert } from "@/lib/email/pipeline-alerts";

export async function GET() {
  const checks: Record<string, { status: "ok" | "error"; latencyMs?: number; error?: string; detail?: string }> = {};

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

  // Check data freshness
  let freshness: Awaited<ReturnType<typeof getDataFreshness>> = null;
  try {
    freshness = await getDataFreshness();
    if (freshness) {
      checks.dataFreshness = {
        status: freshness.isFresh ? "ok" : "error",
        detail: freshness.isFresh
          ? `Data current (${freshness.daysStale}d behind)`
          : `Data stale — ${freshness.daysStale} days behind (latest: ${freshness.latestDataDate})`,
      };
    }
  } catch {
    // Non-critical
  }

  // Check BullMQ queue stats
  let queueStats: { active: number; waiting: number; failed: number } | null = null;
  try {
    const q = getPipelineQueue();
    queueStats = {
      active: await q.getActiveCount(),
      waiting: await q.getWaitingCount(),
      failed: await q.getFailedCount(),
    };
    checks.queue = {
      status: "ok",
      detail: `active: ${queueStats.active}, waiting: ${queueStats.waiting}, failed: ${queueStats.failed}`,
    };
  } catch {
    // Non-critical
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

  // If data is stale, trigger catch-up (non-blocking) and send alert
  if (freshness && !freshness.isFresh) {
    checkAndCatchUp().catch(() => {}); // fire-and-forget
    sendPipelineAlert(
      "stale_data",
      `Data is ${freshness.daysStale} days behind.\nLatest export covers through ${freshness.latestDataDate}.`,
    ).catch(() => {});
  }

  const allOk = Object.values(checks).every((c) => c.status === "ok");

  return NextResponse.json(
    {
      status: allOk ? "healthy" : "degraded",
      uptime: Math.round(process.uptime()),
      checks,
      queueStats,
      lastBackup,
      freshness: freshness
        ? {
            isFresh: freshness.isFresh,
            latestDataDate: freshness.latestDataDate,
            daysStale: freshness.daysStale,
          }
        : null,
    },
    { status: allOk ? 200 : 503 }
  );
}
