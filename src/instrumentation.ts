/** How often to run the pipeline (ms). Default: every 4 hours. */
const PIPELINE_INTERVAL_MS = 4 * 60 * 60 * 1000;

let pipelineTimer: ReturnType<typeof setInterval> | null = null;

/**
 * Run the data pipeline: fetch exports from Union API, process zips,
 * recompute revenue, send digest email.
 */
async function runScheduledPipeline() {
  try {
    const { loadSettings } = await import("./lib/crypto/credentials");
    const settings = loadSettings();
    if (!settings?.unionApiKey) {
      console.log("[scheduler] No Union API key — skipping pipeline run");
      return;
    }

    console.log("[scheduler] Starting scheduled pipeline run...");
    const startTime = Date.now();

    const { fetchAllExports, markExportProcessed, logExport } = await import("./lib/union-api/fetch-export");
    const { runZipWebhookPipeline } = await import("./lib/email/zip-download-pipeline");
    const { bumpDataVersion, invalidateStatsCache } = await import("./lib/cache/stats-cache");

    const allExports = await fetchAllExports(settings.unionApiKey);
    if (allExports.length === 0) {
      console.log("[scheduler] No exports available");
      return;
    }

    console.log(`[scheduler] Processing ${allExports.length} exports...`);
    let successCount = 0;

    for (let i = 0; i < allExports.length; i++) {
      const exp = allExports[i];
      try {
        const zipResult = await runZipWebhookPipeline({
          downloadUrl: exp.downloadUrl,
          dataRange: exp.dataRange,
          onProgress: (step, pct) => {
            if (pct % 25 === 0) console.log(`[scheduler] Export ${i + 1}: ${step} (${pct}%)`);
          },
        });

        if (zipResult.success) {
          const totalRecords = Object.values(zipResult.recordCounts ?? {}).reduce(
            (a, b) => a + (typeof b === "number" ? b : 0), 0
          );
          console.log(`[scheduler] Export ${i + 1} succeeded: ${totalRecords} records`);
          await logExport(exp, totalRecords, i, allExports.length);
          if (i === 0) await markExportProcessed(exp.createdAt, totalRecords);
          successCount++;
        }
      } catch (err) {
        console.warn(`[scheduler] Export ${i + 1} failed: ${err instanceof Error ? err.message : err}`);
      }
    }

    await bumpDataVersion();
    invalidateStatsCache();

    // Auto-backup (non-fatal)
    try {
      const { createBackup, saveBackupToDisk, saveBackupMetadata, pruneBackups } = await import("./lib/db/backup");
      const { uploadBackupToGitHub } = await import("./lib/db/backup-cloud");
      const backup = await createBackup();
      const { filePath, metadata } = await saveBackupToDisk(backup);
      await saveBackupMetadata(metadata, filePath);
      await pruneBackups(7);
      try { await uploadBackupToGitHub(backup); } catch { /* non-fatal */ }
    } catch (err) {
      console.warn(`[scheduler] Backup failed:`, err instanceof Error ? err.message : err);
    }

    // Recompute usage weekly visits and tier transitions (non-fatal)
    if (successCount > 0) {
      try {
        const { computeWeeklyVisits, computeTierTransitions } = await import("./lib/db/usage-store");
        // Get current ISO week start (Monday)
        const now = new Date();
        const day = now.getUTCDay();
        const diff = day === 0 ? -6 : 1 - day;
        const weekStart = new Date(now);
        weekStart.setUTCDate(weekStart.getUTCDate() + diff);
        const ws = weekStart.toISOString().slice(0, 10);
        const visitCount = await computeWeeklyVisits(ws);
        const transCount = await computeTierTransitions(ws, 4);
        console.log(`[scheduler] Usage: ${visitCount} weekly visit rows, ${transCount} tier transitions`);
      } catch (err) {
        console.warn(`[scheduler] Usage computation failed:`, err instanceof Error ? err.message : err);
      }
    }

    // Send digest email (non-fatal, once-per-day guard inside)
    if (successCount > 0) {
      try {
        const { sendDigestEmail } = await import("./lib/email/email-sender");
        const emailResult = await sendDigestEmail();
        if (emailResult.sent > 0) {
          console.log(`[scheduler] Digest email sent to ${emailResult.sent} recipients`);
        } else if (emailResult.skipped) {
          console.log(`[scheduler] Digest email skipped: ${emailResult.skipped}`);
        }
      } catch (emailErr) {
        console.warn(`[scheduler] Digest email failed:`, emailErr instanceof Error ? emailErr.message : emailErr);
      }
    }

    const duration = Math.round((Date.now() - startTime) / 1000);
    console.log(`[scheduler] Done in ${duration}s. ${successCount}/${allExports.length} exports succeeded.`);
  } catch (err) {
    console.error("[scheduler] Pipeline run failed:", err instanceof Error ? err.message : err);
  }
}

export async function register() {
  if (process.env.NEXT_RUNTIME === "nodejs") {
    try {
      // Initialize PostgreSQL schema
      console.log("[instrumentation] Initializing database schema...");
      const { initDatabase } = await import("./lib/db/database");
      await initDatabase();
      console.log("[instrumentation] Database schema initialized");

      // Run pending migrations
      const { runMigrations } = await import("./lib/db/migrations");
      await runMigrations();

      // Ensure data_version table for DB-based cache invalidation
      const { ensureDataVersionTable } = await import("./lib/cache/stats-cache");
      await ensureDataVersionTable();

      console.log("[instrumentation] Database ready.");

      // Start pipeline scheduler — runs immediately on startup, then every 4 hours.
      // The pipeline is idempotent (DB upserts handle dedup), and the digest email
      // has a once-per-day atomic guard, so frequent runs are safe.
      console.log("[instrumentation] Starting pipeline scheduler (every 4h)...");
      // Delay first run by 30s to let the server finish starting
      setTimeout(() => {
        runScheduledPipeline();
        pipelineTimer = setInterval(runScheduledPipeline, PIPELINE_INTERVAL_MS);
      }, 30_000);

      // Graceful shutdown
      const shutdown = async (signal: string) => {
        console.log(`[instrumentation] ${signal} received, shutting down...`);
        if (pipelineTimer) clearInterval(pipelineTimer);
        try {
          const { closePool } = await import("./lib/db/database");
          await closePool();
        } catch (err) {
          console.error("[instrumentation] Error during shutdown:", err);
        }
        process.exit(0);
      };
      process.on("SIGTERM", () => shutdown("SIGTERM"));
      process.on("SIGINT", () => shutdown("SIGINT"));
    } catch (err) {
      console.error("[instrumentation] Failed to initialize database:", err);
    }
  } else {
    console.log(`[instrumentation] Skipping init (runtime: ${process.env.NEXT_RUNTIME || "unknown"})`);
  }
}
