import { NextResponse } from "next/server";
import { loadSettings } from "@/lib/crypto/credentials";
import { fetchAllExports, markExportProcessed, logExport } from "@/lib/union-api/fetch-export";
import { runZipWebhookPipeline } from "@/lib/email/zip-download-pipeline";
import { bumpDataVersion, invalidateStatsCache } from "@/lib/cache/stats-cache";
import { createBackup, saveBackupToDisk, saveBackupMetadata, pruneBackups } from "@/lib/db/backup";
import { uploadBackupToGitHub } from "@/lib/db/backup-cloud";

export const dynamic = "force-dynamic";
export const maxDuration = 300; // 5 minutes

/**
 * POST /api/cron/pipeline — CRON-triggered pipeline run.
 *
 * Fetches ALL available exports from the Union Data Exporter API,
 * processes each one (DB upserts handle dedup), then recomputes
 * revenue from accumulated DB data.
 *
 * Auth: Bearer token from CRON_SECRET env var.
 */
export async function POST(request: Request) {
  // Auth check
  const authHeader = request.headers.get("authorization");
  const cronSecret = process.env.CRON_SECRET;
  if (cronSecret && authHeader !== `Bearer ${cronSecret}`) {
    return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
  }

  const startTime = Date.now();

  try {
    const settings = loadSettings();
    if (!settings?.unionApiKey) {
      return NextResponse.json({ error: "No Union API key configured" }, { status: 500 });
    }

    console.log("[cron/pipeline] Starting pipeline run...");

    const allExports = await fetchAllExports(settings.unionApiKey);
    if (allExports.length === 0) {
      return NextResponse.json({ message: "No exports available", duration: 0 });
    }

    console.log(`[cron/pipeline] Processing ${allExports.length} exports...`);

    let successCount = 0;
    let lastResult: Awaited<ReturnType<typeof runZipWebhookPipeline>> | null = null;

    for (let i = 0; i < allExports.length; i++) {
      const exp = allExports[i];
      console.log(`[cron/pipeline] Export ${i + 1}/${allExports.length}: ${exp.createdAt}`);

      try {
        const zipResult = await runZipWebhookPipeline({
          downloadUrl: exp.downloadUrl,
          dataRange: exp.dataRange,
          onProgress: (step, pct) => {
            if (pct % 25 === 0) console.log(`[cron/pipeline] Export ${i + 1}: ${step} (${pct}%)`);
          },
        });

        if (zipResult.success) {
          const totalRecords = Object.values(zipResult.recordCounts ?? {}).reduce(
            (a, b) => a + (typeof b === "number" ? b : 0), 0
          );
          console.log(`[cron/pipeline] Export ${i + 1} succeeded: ${totalRecords} records`);
          await logExport(exp, totalRecords, i, allExports.length);
          if (i === 0) {
            await markExportProcessed(exp.createdAt, totalRecords);
            lastResult = zipResult;
          }
          successCount++;
        }
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        console.warn(`[cron/pipeline] Export ${i + 1} failed: ${msg} — continuing`);
      }
    }

    // Bump cache
    await bumpDataVersion();
    invalidateStatsCache();

    // Auto-backup (non-fatal)
    try {
      const backup = await createBackup();
      const { filePath, metadata } = await saveBackupToDisk(backup);
      await saveBackupMetadata(metadata, filePath);
      await pruneBackups(7);
      const totalRows = Object.values(metadata.tables).reduce((a, b) => a + b, 0);
      console.log(`[cron/pipeline] Backup: ${totalRows} rows saved`);

      try {
        const cloud = await uploadBackupToGitHub(backup);
        console.log(`[cron/pipeline] Cloud backup: ${cloud.tag}`);
      } catch (cloudErr) {
        console.warn(`[cron/pipeline] Cloud backup failed:`, cloudErr instanceof Error ? cloudErr.message : cloudErr);
      }
    } catch (err) {
      console.warn(`[cron/pipeline] Backup failed:`, err instanceof Error ? err.message : err);
    }

    // Send digest email (non-fatal)
    try {
      const { sendDigestEmail } = await import("@/lib/email/email-sender");
      const emailResult = await sendDigestEmail();
      if (emailResult.sent > 0) {
        console.log(`[cron/pipeline] Digest email sent to ${emailResult.sent} recipients`);
      } else if (emailResult.skipped) {
        console.log(`[cron/pipeline] Digest email skipped: ${emailResult.skipped}`);
      }
    } catch (emailErr) {
      console.warn(`[cron/pipeline] Digest email failed:`, emailErr instanceof Error ? emailErr.message : emailErr);
    }

    const duration = Math.round((Date.now() - startTime) / 1000);
    console.log(`[cron/pipeline] Done in ${duration}s. ${successCount}/${allExports.length} exports succeeded.`);

    return NextResponse.json({
      success: successCount > 0,
      duration,
      exportsProcessed: allExports.length,
      exportsSucceeded: successCount,
    });
  } catch (e: unknown) {
    const duration = Math.round((Date.now() - startTime) / 1000);
    console.error(`[cron/pipeline] Fatal error after ${duration}s:`, (e as Error).message);
    return NextResponse.json({ error: (e as Error).message, duration }, { status: 500 });
  }
}
