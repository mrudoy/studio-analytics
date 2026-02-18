import { Worker, Job } from "bullmq";
import { getRedisConnection } from "./connection";
import { loadSettings } from "../crypto/credentials";
import { runPipelineFromFiles } from "./pipeline-core";
import { runEmailPipeline } from "./email-pipeline";
import { cleanupDownloads } from "../scraper/download-manager";
import type { PipelineJobData, PipelineResult } from "@/types/pipeline";
import { getWatermark, buildDateRangeForReport } from "../db/watermark-store";

/** Maximum total time the pipeline is allowed to run before being killed. */
const PIPELINE_TIMEOUT_MS = 20 * 60 * 1000; // 20 minutes

/**
 * Race a promise against a timeout. If the timeout fires first, throw.
 */
async function withTimeout<T>(promise: Promise<T>, ms: number, label: string): Promise<T> {
  let timer: ReturnType<typeof setTimeout>;
  const timeout = new Promise<never>((_, reject) => {
    timer = setTimeout(() => reject(new Error(`Timeout: ${label} (after ${ms / 1000}s)`)), ms);
  });
  try {
    return await Promise.race([promise, timeout]);
  } finally {
    clearTimeout(timer!);
  }
}

let pipelineStartedAt = 0;

function updateProgress(job: Job, step: string, percent: number) {
  job.updateProgress({ step, percent, startedAt: pipelineStartedAt });
}

async function runPipeline(job: Job): Promise<PipelineResult> {
  return withTimeout(
    runPipelineInner(job),
    PIPELINE_TIMEOUT_MS,
    "Pipeline timed out after 20 minutes — try again or check Union.fit connectivity"
  );
}

async function runPipelineInner(job: Job): Promise<PipelineResult> {
  pipelineStartedAt = Date.now();

  // Step 1: Load credentials
  updateProgress(job, "Loading credentials", 5);
  const settings = loadSettings();
  if (!settings?.credentials) {
    throw new Error("No Union.fit credentials configured. Go to Settings to configure.");
  }

  const analyticsSheetId = settings.analyticsSpreadsheetId || process.env.ANALYTICS_SPREADSHEET_ID;
  const rawDataSheetId = settings.rawDataSpreadsheetId || process.env.RAW_DATA_SPREADSHEET_ID;

  if (!analyticsSheetId) {
    throw new Error("Analytics Spreadsheet ID not configured.");
  }

  // Build date range from job params or per-report watermarks
  let dateRange = job.data.dateRangeStart && job.data.dateRangeEnd
    ? `${job.data.dateRangeStart} - ${job.data.dateRangeEnd}`
    : "";

  if (!dateRange) {
    dateRange = await buildDateRange();
  }

  // ── Email pipeline (primary path) ──
  // All CSV downloads go through: Playwright clicks "Download CSV" button →
  // Union.fit either downloads directly or emails the CSV → Gmail API picks up emails.
  // No HTML scraping.
  if (!settings.robotEmail?.address) {
    throw new Error(
      "Robot email not configured. The pipeline requires email-based CSV delivery. " +
      "Go to Settings to configure the robot email address."
    );
  }

  updateProgress(job, "Starting email-based pipeline", 5);
  console.log(`[pipeline] Email pipeline mode: robot=${settings.robotEmail.address}`);

  const result = await runEmailPipeline({
    unionEmail: settings.credentials.email,
    unionPassword: settings.credentials.password,
    robotEmail: settings.robotEmail.address,
    analyticsSheetId,
    rawDataSheetId,
    dateRange,
    onProgress: (step, percent) => updateProgress(job, step, percent),
  });

  // Cleanup temp files
  cleanupDownloads();

  return result;
}

/**
 * Build date range based on watermarks (incremental fetch).
 *
 * Checks watermarks for key report types and uses the oldest high-water mark
 * as the start date. If no watermarks exist, does a full backfill to 2024-01-01.
 */
async function buildDateRange(): Promise<string> {
  const reportTypes = [
    "autoRenews",
    "revenueCategories",
    "registrations",
    "orders",
    "newCustomers",
    "firstVisits",
  ];

  let oldestStart: string | null = null;

  for (const rt of reportTypes) {
    try {
      const wm = await getWatermark(rt);
      const range = buildDateRangeForReport(wm);
      const startPart = range.split(" - ")[0];

      // Parse to comparable format
      const parts = startPart.split("/");
      if (parts.length === 3) {
        const isoDate = `${parts[2]}-${parts[0].padStart(2, "0")}-${parts[1].padStart(2, "0")}`;
        if (!oldestStart || isoDate < oldestStart) {
          oldestStart = isoDate;
        }
      }
    } catch {
      // Skip — watermark table might not exist yet
    }
  }

  const now = new Date();
  const endStr = `${now.getMonth() + 1}/${now.getDate()}/${now.getFullYear()}`;

  if (oldestStart) {
    const d = new Date(oldestStart);
    const startStr = `${d.getMonth() + 1}/${d.getDate()}/${d.getFullYear()}`;
    console.log(`[pipeline] Incremental mode: date range ${startStr} - ${endStr}`);
    return `${startStr} - ${endStr}`;
  }

  // Full backfill
  console.log(`[pipeline] First run: no watermarks, fetching from 1/1/2024`);
  return `1/1/2024 - ${endStr}`;
}

// Store on globalThis so the singleton survives Next.js HMR reloads.
// Without this, each hot reload resets `let worker = null` in the new module
// scope while the old Worker stays alive in Redis, orphaning the queue.
const g = globalThis as unknown as { __pipelineWorker?: Worker };

export function startPipelineWorker(): Worker {
  if (g.__pipelineWorker) return g.__pipelineWorker;

  const w = new Worker(
    "pipeline",
    async (job) => runPipeline(job),
    {
      connection: getRedisConnection(),
      concurrency: 1,
      lockDuration: 1_500_000, // 25 minutes — pipeline timeout is 20 min + margin
    }
  );

  w.on("completed", (job) => {
    console.log(`[worker] Pipeline job ${job.id} completed`);
  });

  w.on("failed", (job, err) => {
    console.error(`[worker] Pipeline job ${job?.id} failed:`, err.message);
  });

  w.on("error", (err) => {
    console.error("[worker] Worker error:", err.message);
  });

  w.on("stalled", (jobId) => {
    console.warn(`[worker] Job ${jobId} stalled — lock may have expired`);
  });

  g.__pipelineWorker = w;
  console.log("[worker] Pipeline worker started and listening for jobs");
  return w;
}
