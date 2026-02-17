import { Worker, Job } from "bullmq";
import { getRedisConnection } from "./connection";
import { UnionClient } from "../scraper/union-client";
import { loadSettings } from "../crypto/credentials";
import { runPipelineFromFiles } from "./pipeline-core";
import { runEmailPipeline } from "./email-pipeline";
import { cleanupDownloads } from "../scraper/download-manager";
import type { PipelineJobData, PipelineResult } from "@/types/pipeline";
import { getLatestPeriod } from "../db/revenue-store";
import { copyFileSync } from "fs";
import { join } from "path";

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

function updateProgress(job: Job, step: string, percent: number) {
  job.updateProgress({ step, percent });
}

async function runPipeline(job: Job): Promise<PipelineResult> {
  return withTimeout(
    runPipelineInner(job),
    PIPELINE_TIMEOUT_MS,
    "Pipeline timed out after 20 minutes — try again or check Union.fit connectivity"
  );
}

async function runPipelineInner(job: Job): Promise<PipelineResult> {
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

  let dateRange = job.data.dateRangeStart && job.data.dateRangeEnd
    ? `${job.data.dateRangeStart} - ${job.data.dateRangeEnd}`
    : "";

  // Build date range if not provided
  if (!dateRange) {
    dateRange = await buildDateRange();
  }

  // ── Try email pipeline first (if robot email is configured) ──
  if (settings.robotEmail?.address) {
    try {
      updateProgress(job, "Using email-based pipeline", 5);
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

      return result;
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      console.warn(`[pipeline] Email pipeline failed, falling back to direct scraping: ${msg}`);
      updateProgress(job, "Email pipeline failed, falling back to direct scrape...", 10);
    }
  }

  // ── Fallback: Direct scraping via Playwright ─────────────────
  updateProgress(job, "Launching browser (direct scrape)", 10);
  const client = new UnionClient({
    onProgress: (step, percent) => updateProgress(job, step, percent),
  });

  let files;
  try {
    await client.initialize();
    updateProgress(job, "Logging into Union.fit", 15);
    await client.login(settings.credentials.email, settings.credentials.password);

    updateProgress(job, "Downloading CSV reports", 20);
    files = await client.downloadAllReports(dateRange);
    updateProgress(job, "All CSVs downloaded", 45);
  } finally {
    await client.cleanup();
  }

  // Save debug copy of orders CSV before cleanup
  try {
    const debugPath = join(process.cwd(), "data", "debug-orders-sample.csv");
    copyFileSync(files.orders, debugPath);
    console.log(`[pipeline] Saved debug orders CSV to ${debugPath}`);
  } catch { /* ignore */ }

  // Steps 3-6: Parse CSVs → Run Analytics → Export to Sheets
  const result = await runPipelineFromFiles(files, analyticsSheetId, {
    rawDataSheetId,
    dateRange,
    onProgress: (step, percent) => updateProgress(job, step, percent),
  });

  // Cleanup scraped temp files
  cleanupDownloads();

  return result;
}

/**
 * Build date range based on incremental mode.
 * If we have historical data, only fetch from last month forward.
 * First run = full 12 months.
 */
async function buildDateRange(): Promise<string> {
  const now = new Date();
  const endStr = `${now.getMonth() + 1}/${now.getDate()}/${now.getFullYear()}`;
  let startDate: Date;

  try {
    const latestPeriod = await getLatestPeriod();
    if (latestPeriod) {
      const lastMonth = new Date(now.getFullYear(), now.getMonth() - 1, 1);
      startDate = lastMonth;
      console.log(`[pipeline] Incremental mode: have data through ${latestPeriod.periodEnd}, fetching from ${lastMonth.toISOString().slice(0, 10)}`);
    } else {
      startDate = new Date(now.getFullYear() - 1, now.getMonth(), now.getDate());
      console.log(`[pipeline] First run: no historical data, fetching last 12 months`);
    }
  } catch {
    startDate = new Date(now.getFullYear() - 1, now.getMonth(), now.getDate());
    console.log(`[pipeline] Database not available, defaulting to last 12 months`);
  }

  const startStr = `${startDate.getMonth() + 1}/${startDate.getDate()}/${startDate.getFullYear()}`;
  const dateRange = `${startStr} - ${endStr}`;
  console.log(`[pipeline] Date range: ${dateRange}`);
  return dateRange;
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
