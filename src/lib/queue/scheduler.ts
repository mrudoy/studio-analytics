import { getPipelineQueue } from "./pipeline-queue";
import { loadSettings } from "../crypto/credentials";

const REPEATABLE_JOB_NAME = "scheduled-pipeline";

/**
 * Synchronise BullMQ repeatable jobs with the schedule stored in AppSettings.
 * Idempotent: removes ALL existing repeatable jobs, then re-adds if enabled.
 * Call on every server startup and after schedule config changes.
 */
export async function syncSchedule(): Promise<void> {
  const q = getPipelineQueue();
  const settings = loadSettings();
  const schedule = settings?.schedule;

  // Remove all existing repeatable jobs (clean slate)
  const existing = await q.getRepeatableJobs();
  for (const job of existing) {
    await q.removeRepeatableByKey(job.key);
  }

  if (!schedule?.enabled || !schedule.cronPattern) {
    console.log("[scheduler] Schedule disabled or not configured");
    return;
  }

  // Add the repeatable job
  await q.add(
    REPEATABLE_JOB_NAME,
    { triggeredBy: "scheduler" },
    {
      repeat: {
        pattern: schedule.cronPattern,
        tz: schedule.timezone || "America/New_York",
      },
    }
  );

  console.log(
    `[scheduler] Scheduled pipeline: "${schedule.cronPattern}" (${schedule.timezone || "America/New_York"})`
  );
}

/**
 * Return the current schedule status for the API.
 */
export async function getScheduleStatus(): Promise<{
  enabled: boolean;
  cronPattern: string;
  timezone: string;
  nextRun: number | null;
}> {
  const settings = loadSettings();
  const schedule = settings?.schedule;
  const q = getPipelineQueue();
  const repeatable = await q.getRepeatableJobs();
  const next = repeatable.length > 0 ? repeatable[0].next : null;

  return {
    enabled: schedule?.enabled ?? false,
    cronPattern: schedule?.cronPattern ?? "",
    timezone: schedule?.timezone ?? "America/New_York",
    nextRun: next ?? null,
  };
}
