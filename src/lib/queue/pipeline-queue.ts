import { Queue } from "bullmq";
import { getRedisConnection } from "./connection";
import type { PipelineJobData } from "@/types/pipeline";

let queue: Queue | null = null;

export function getPipelineQueue(): Queue {
  if (queue) return queue;

  queue = new Queue("pipeline", {
    connection: getRedisConnection(),
    defaultJobOptions: {
      attempts: 1,
      removeOnComplete: { count: 10 },
      removeOnFail: { count: 10 },
    },
  });

  return queue;
}

/**
 * Clear all stale/stuck jobs from the queue.
 * Useful when a job is stuck in "active" state but the worker isn't processing it.
 */
export async function clearQueue(): Promise<{ cleared: number }> {
  const q = getPipelineQueue();
  const active = await q.getJobs(["active"]);
  const waiting = await q.getJobs(["waiting"]);
  const delayed = await q.getJobs(["delayed"]);

  let cleared = 0;
  for (const job of [...active, ...waiting, ...delayed]) {
    try {
      await job.remove();
      cleared++;
    } catch {
      // Job may have already been removed or completed
      try {
        await job.moveToFailed(new Error("Cleared by user"), "0", false);
        cleared++;
      } catch {
        // Ignore — already gone
      }
    }
  }

  console.log(`[queue] Cleared ${cleared} stale jobs`);
  return { cleared };
}

export async function enqueuePipeline(data: PipelineJobData): Promise<string> {
  const q = getPipelineQueue();

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
