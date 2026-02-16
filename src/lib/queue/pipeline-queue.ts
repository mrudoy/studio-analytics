import { Queue } from "bullmq";
import { getRedisConnection } from "./connection";
import type { PipelineJobData } from "@/types/pipeline";

// Store on globalThis so the singleton survives Next.js HMR reloads.
const g = globalThis as unknown as { __pipelineQueue?: Queue };

export function getPipelineQueue(): Queue {
  if (g.__pipelineQueue) return g.__pipelineQueue;

  g.__pipelineQueue = new Queue("pipeline", {
    connection: getRedisConnection(),
    defaultJobOptions: {
      attempts: 2,
      backoff: { type: "fixed", delay: 10_000 }, // 10s delay before retry
      removeOnComplete: { count: 10 },
      removeOnFail: { count: 10 },
    },
  });

  return g.__pipelineQueue;
}

/**
 * Clear all stale/stuck jobs from the queue.
 * Useful when a job is stuck in "active" state but the worker isn't processing it.
 *
 * Each job removal is wrapped in a 5s timeout so a hung worker
 * doesn't block the "Reset if stuck" button from completing.
 */

/** Race a promise against a short timeout — used for job removal. */
async function withTimeout<T>(promise: Promise<T>, ms: number): Promise<T> {
  let timer: ReturnType<typeof setTimeout>;
  const timeout = new Promise<never>((_, reject) => {
    timer = setTimeout(() => reject(new Error("timeout")), ms);
  });
  try {
    return await Promise.race([promise, timeout]);
  } finally {
    clearTimeout(timer!);
  }
}

export async function clearQueue(): Promise<{ cleared: number }> {
  const q = getPipelineQueue();
  const active = await q.getJobs(["active"]);
  const waiting = await q.getJobs(["waiting"]);
  const delayed = await q.getJobs(["delayed"]);

  let cleared = 0;
  for (const job of [...active, ...waiting, ...delayed]) {
    try {
      await withTimeout(job.remove(), 5_000);
      cleared++;
    } catch {
      // Job may have already been removed or completed, or removal timed out
      try {
        await withTimeout(
          job.moveToFailed(new Error("Cleared by user"), "0", false),
          5_000
        );
        cleared++;
      } catch {
        // Ignore — already gone or timed out
      }
    }
  }

  console.log(`[queue] Cleared ${cleared} stale jobs`);
  return { cleared };
}

/** Max age (ms) before an active job is considered stale and auto-cleared. */
const STALE_JOB_THRESHOLD_MS = 25 * 60 * 1000; // 25 minutes

export async function enqueuePipeline(data: PipelineJobData): Promise<string> {
  const q = getPipelineQueue();

  // Auto-clear stale jobs that have been running too long
  const activeJobs = await q.getJobs(["active"]);
  for (const j of activeJobs) {
    const elapsed = Date.now() - (j.processedOn || j.timestamp);
    if (elapsed > STALE_JOB_THRESHOLD_MS) {
      console.warn(
        `[queue] Auto-clearing stale job ${j.id} (running ${Math.round(elapsed / 60_000)}m)`
      );
      try {
        await withTimeout(
          j.moveToFailed(
            new Error(`Auto-cleared: exceeded ${Math.round(STALE_JOB_THRESHOLD_MS / 60_000)} minute limit`),
            "0",
            false
          ),
          5_000
        );
      } catch {
        // Best-effort — if it fails, the normal check below will still block
      }
    }
  }

  // Check for active jobs
  const active = await q.getActiveCount();
  const waiting = await q.getWaitingCount();
  if (active > 0 || waiting > 0) {
    throw new Error(
      `A pipeline job is already running or queued (active: ${active}, waiting: ${waiting}). ` +
      `Use the Reset button to clear stale jobs.`
    );
  }

  const job = await q.add("run-pipeline", data, {
    jobId: `pipeline-${Date.now()}`,
  });

  return job.id!;
}
