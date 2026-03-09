/**
 * Catch-up mechanism — detects missed pipeline runs and auto-enqueues a new one.
 *
 * Checks the export_log table for the most recent successful run.
 * If it's been >8 hours since the last run, enqueues a pipeline job.
 * Handles "already running" gracefully.
 */

import { getPool } from "../db/database";

const STALE_THRESHOLD_MS = 8 * 60 * 60 * 1000; // 8 hours

/**
 * Check if the pipeline has run recently. If not, enqueue a catch-up run.
 * Safe to call from multiple places — the queue's own guard prevents duplicates.
 */
export async function checkAndCatchUp(): Promise<{ triggered: boolean; reason: string }> {
  try {
    const pool = getPool();

    // Check when the last export was processed
    const { rows } = await pool.query(
      `SELECT created_at FROM export_log ORDER BY created_at DESC LIMIT 1`,
    );

    if (rows.length === 0) {
      // No exports ever — might be a fresh install. Don't auto-trigger.
      return { triggered: false, reason: "No export history — skipping catch-up" };
    }

    const lastRun = new Date(rows[0].created_at as string);
    const elapsed = Date.now() - lastRun.getTime();

    if (elapsed < STALE_THRESHOLD_MS) {
      const hoursAgo = Math.round(elapsed / (60 * 60 * 1000) * 10) / 10;
      return { triggered: false, reason: `Last run ${hoursAgo}h ago — within threshold` };
    }

    // Pipeline is stale — enqueue a catch-up run
    const hoursStale = Math.round(elapsed / (60 * 60 * 1000) * 10) / 10;
    console.log(`[catchup] Last pipeline run was ${hoursStale}h ago — triggering catch-up`);

    try {
      const { enqueuePipeline } = await import("./pipeline-queue");
      await enqueuePipeline({ triggeredBy: "catchup" });
      return { triggered: true, reason: `Triggered catch-up (last run ${hoursStale}h ago)` };
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      // "already running" is expected and fine
      if (msg.includes("already running") || msg.includes("already queued")) {
        return { triggered: false, reason: `Pipeline already active — no catch-up needed` };
      }
      return { triggered: false, reason: `Catch-up enqueue failed: ${msg}` };
    }
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return { triggered: false, reason: `Catch-up check failed: ${msg}` };
  }
}
