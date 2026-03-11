import { Worker, Job } from "bullmq";
import { getRedisConnection } from "./connection";
import { loadSettings } from "../crypto/credentials";
import { runZipWebhookPipeline } from "../email/zip-download-pipeline";
import { runShopifySync } from "../shopify/shopify-sync";
import { cleanupDownloads } from "../scraper/download-manager";
import type { PipelineResult } from "@/types/pipeline";
import { getWatermark, buildDateRangeForReport } from "../db/watermark-store";
import { createBackup, saveBackupToDisk, saveBackupMetadata, pruneBackups } from "../db/backup";
import { uploadBackupToGitHub } from "../db/backup-cloud";
import { invalidateStatsCache, bumpDataVersion } from "../cache/stats-cache";
import { fetchAllExports, markExportProcessed, logExport } from "../union-api/fetch-export";
import { sendPipelineAlert } from "../email/pipeline-alerts";

/** Maximum total time the pipeline is allowed to run before being killed.
 *  30 min — processes all available API exports (typically 10-12 daily exports). */
const PIPELINE_TIMEOUT_MS = 30 * 60 * 1000; // 30 minutes

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

import type { CategoryProgress } from "@/types/pipeline";

function updateProgress(job: Job, step: string, percent: number, categories?: Record<string, CategoryProgress>) {
  job.updateProgress({ step, percent, startedAt: pipelineStartedAt, categories });
}

async function runPipeline(job: Job): Promise<PipelineResult> {
  return withTimeout(
    runPipelineInner(job),
    PIPELINE_TIMEOUT_MS,
    "Pipeline timed out after 15 minutes — try again or check Union.fit API connectivity"
  );
}

async function runPipelineInner(job: Job): Promise<PipelineResult> {
  pipelineStartedAt = Date.now();

  // Step 1: Load credentials
  updateProgress(job, "Loading credentials", 5);
  const settings = loadSettings();
  const hasApiKey = !!(settings?.unionApiKey);
  const hasWebhookUrl = !!job.data.downloadUrl;

  if (!hasApiKey && !hasWebhookUrl) {
    throw new Error(
      "No data source configured. Set a Union API key or provide a webhook download URL in Settings."
    );
  }

  // ── RULE: API ONLY — NO SCRAPING ──
  // Only two paths: (1) webhook URL if provided, (2) Union Data Exporter API.
  // Gmail polling and Playwright/browser scraping are permanently disabled.
  let result: PipelineResult | null = null;

  // Path 1: Direct URL from webhook
  if (job.data.downloadUrl) {
    try {
      updateProgress(job, "Downloading zip from webhook...", 5);
      console.log(`[pipeline] Webhook path: ${job.data.downloadUrl.slice(0, 80)}...`);

      const zipResult = await runZipWebhookPipeline({
        downloadUrl: job.data.downloadUrl,
        onProgress: (step, percent) => updateProgress(job, step, percent),
      });

      if (zipResult.success) {
        console.log(`[pipeline] Webhook pipeline succeeded in ${zipResult.duration}s`);
        result = zipResult;
      }
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      console.warn(`[pipeline] Webhook pipeline failed: ${msg}`);
    }
  }

  // Path 2: Union Data Exporter API — fetch and process ALL available exports.
  // Daily exports are incremental (only recently changed records), so we must
  // process every available export to build a complete dataset. DB upserts
  // handle deduplication — processing the same record twice is safe.
  if (!result && hasApiKey) {
    try {
      updateProgress(job, "Checking Union Data Exporter API...", 5);
      console.log("[pipeline] Trying Union Data Exporter API");

      const allExports = await fetchAllExports(settings!.unionApiKey!);
      if (allExports.length > 0) {
        console.log(`[pipeline] Processing ${allExports.length} exports (newest first)`);

        // Process newest first so the latest data wins any upsert conflicts
        for (let i = 0; i < allExports.length; i++) {
          const exportInfo = allExports[i];
          const pctBase = 5 + Math.round((i / allExports.length) * 80);
          updateProgress(
            job,
            `Processing export ${i + 1}/${allExports.length} (${exportInfo.dataRange.start?.slice(0, 10) || "full"})...`,
            pctBase
          );
          console.log(`[pipeline] Export ${i + 1}/${allExports.length}: ${exportInfo.createdAt}`);

          try {
            const zipResult = await runZipWebhookPipeline({
              downloadUrl: exportInfo.downloadUrl,
              dataRange: exportInfo.dataRange,
              onProgress: (step, percent) => {
                // Scale sub-progress within this export's slice
                const scaledPct = pctBase + Math.round((percent / 100) * (80 / allExports.length));
                updateProgress(job, step, Math.min(scaledPct, 90));
              },
            });

            if (zipResult.success) {
              const totalRecords = Object.values(zipResult.recordCounts ?? {}).reduce(
                (a, b) => a + (typeof b === "number" ? b : 0), 0
              );
              console.log(`[pipeline] Export ${i + 1} succeeded: ${totalRecords} records`);
              // Log this export for freshness tracking
              await logExport(exportInfo, totalRecords, i, allExports.length);
              // Mark the latest (first) export as the watermark
              if (i === 0) {
                await markExportProcessed(exportInfo.createdAt, totalRecords);
                result = zipResult;
              }
            }
          } catch (err) {
            const msg = err instanceof Error ? err.message : String(err);
            console.warn(`[pipeline] Export ${i + 1} failed: ${msg} — continuing`);
          }
        }

        if (result) {
          console.log(`[pipeline] All exports processed. Pipeline succeeded in ${result.duration}s`);
        }
      } else {
        console.log("[pipeline] No API exports available");
      }
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      console.warn(`[pipeline] API export fetch failed: ${msg}`);
    }
  }

  // ── Shopify sync (non-fatal — dashboard still works without it) ──
  if (settings?.shopify?.storeName && settings?.shopify?.clientId && settings?.shopify?.clientSecret) {
    try {
      updateProgress(job, "Syncing Shopify data", 85);
      const shopifyResult = await runShopifySync({
        storeName: settings.shopify.storeName,
        clientId: settings.shopify.clientId,
        clientSecret: settings.shopify.clientSecret,
        onProgress: (step, pct) => updateProgress(job, `Shopify: ${step}`, 85 + pct * 10),
      });
      console.log(
        `[pipeline] Shopify sync complete: ${shopifyResult.orderCount} orders, ` +
        `${shopifyResult.productCount} products, ${shopifyResult.customerCount} customers`
      );
    } catch (err) {
      console.warn("[pipeline] Shopify sync failed (non-fatal):", err instanceof Error ? err.message : err);
    }
  }

  // Cleanup temp files
  cleanupDownloads();

  if (!result) {
    throw new Error(
      "Pipeline could not fetch data from any source. " +
      "Check your Union API key or webhook configuration in Settings. " +
      "(API-only mode — no Gmail or browser scraping.)"
    );
  }

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
      lockDuration: 1_200_000, // 20 minutes — API-only pipeline timeout is 15 min + margin
    }
  );

  w.on("completed", async (job) => {
    console.log(`[worker] Pipeline job ${job.id} completed`);

    // Bump DB version + invalidate in-memory cache so next request gets fresh data.
    // bumpDataVersion() ensures even external processes (seed scripts, manual SQL)
    // trigger cache invalidation — the in-memory invalidate is belt-and-suspenders.
    await bumpDataVersion();
    invalidateStatsCache();

    // Auto-backup after successful pipeline run (local + cloud)
    try {
      const backup = await createBackup();
      const { filePath, metadata } = await saveBackupToDisk(backup);
      await saveBackupMetadata(metadata, filePath);
      await pruneBackups(7);
      const totalRows = Object.values(metadata.tables).reduce((a, b) => a + b, 0);
      console.log(`[worker] Post-pipeline backup: ${totalRows} rows saved to ${filePath}`);

      // Upload to GitHub Releases (non-fatal)
      try {
        const cloud = await uploadBackupToGitHub(backup);
        console.log(`[worker] Cloud backup uploaded: ${cloud.tag} (${cloud.compressedBytes} bytes)`);
      } catch (cloudErr) {
        console.warn(`[worker] Cloud backup failed (non-fatal):`, cloudErr instanceof Error ? cloudErr.message : cloudErr);
      }
    } catch (err) {
      console.warn(`[worker] Post-pipeline backup failed:`, err instanceof Error ? err.message : err);
    }

    // Send daily digest email (non-fatal)
    // The once-per-day atomic guard is now inside sendDigestEmail() itself,
    // so no matter who calls it, only one email per day is sent.
    try {
      const { sendDigestEmail } = await import("../email/email-sender");
      const emailResult = await sendDigestEmail();
      if (emailResult.sent > 0) {
        console.log(`[worker] Digest email sent to ${emailResult.sent} recipients`);
      } else if (emailResult.skipped) {
        console.log(`[worker] Digest email skipped: ${emailResult.skipped}`);
      }
    } catch (emailErr) {
      console.warn(`[worker] Digest email failed (non-fatal):`, emailErr instanceof Error ? emailErr.message : emailErr);
    }
  });

  w.on("failed", async (job, err) => {
    console.error(`[worker] Pipeline job ${job?.id} failed:`, err.message);

    // Send alert email if all retries exhausted
    if (job && job.attemptsMade >= (job.opts?.attempts ?? 5)) {
      console.warn(`[worker] All ${job.attemptsMade} retries exhausted for job ${job.id}`);
      try {
        await sendPipelineAlert(
          "retry_exhausted",
          `Job ${job.id} failed after ${job.attemptsMade} attempts.\n\nLast error: ${err.message}`,
        );
      } catch (alertErr) {
        console.error("[worker] Failed to send pipeline alert:", alertErr instanceof Error ? alertErr.message : alertErr);
      }
    }
  });

  w.on("error", (err) => {
    console.error("[worker] Worker error:", err.message);
  });

  w.on("stalled", (jobId) => {
    console.warn(`[worker] Job ${jobId} stalled — lock may have expired`);
  });

  // Zombie job cleanup: every 5 minutes, check for active jobs >20 min old
  // and force-fail them so the queue isn't permanently blocked.
  const ZOMBIE_CHECK_INTERVAL = 5 * 60 * 1000; // 5 min
  const ZOMBIE_THRESHOLD_MS = 20 * 60 * 1000;   // 20 min

  setInterval(async () => {
    try {
      const { getPipelineQueue } = await import("./pipeline-queue");
      const q = getPipelineQueue();
      const activeJobs = await q.getJobs(["active"]);

      for (const job of activeJobs) {
        const elapsed = Date.now() - (job.processedOn || job.timestamp);
        if (elapsed > ZOMBIE_THRESHOLD_MS) {
          console.warn(
            `[worker] Zombie cleanup: force-failing job ${job.id} (running ${Math.round(elapsed / 60_000)}m)`,
          );
          try {
            await job.moveToFailed(
              new Error(`Zombie cleanup: exceeded ${Math.round(ZOMBIE_THRESHOLD_MS / 60_000)} minute limit`),
              "0",
              false,
            );
          } catch {
            // Best-effort
          }
        }
      }
    } catch {
      // Non-fatal — Redis might be temporarily unavailable
    }
  }, ZOMBIE_CHECK_INTERVAL);

  g.__pipelineWorker = w;
  console.log("[worker] Pipeline worker started and listening for jobs");
  return w;
}
