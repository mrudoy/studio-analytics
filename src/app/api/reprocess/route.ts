import { NextResponse } from "next/server";
import { loadSettings } from "@/lib/crypto/credentials";
import { fetchAllExports } from "@/lib/union-api/fetch-export";
import { runZipWebhookPipeline } from "@/lib/email/zip-download-pipeline";
import { bumpDataVersion, invalidateStatsCache } from "@/lib/cache/stats-cache";

export const dynamic = "force-dynamic";
export const maxDuration = 300; // 5 minutes

/**
 * POST /api/reprocess — Directly fetch and process Union API exports,
 * bypassing BullMQ queue. Use when the queue worker is stuck.
 */
export async function POST() {
  const startTime = Date.now();

  try {
    const settings = loadSettings();
    if (!settings?.unionApiKey) {
      return NextResponse.json({ error: "No Union API key configured" }, { status: 500 });
    }

    console.log("[reprocess] Fetching exports from Union API...");
    const allExports = await fetchAllExports(settings.unionApiKey);

    if (allExports.length === 0) {
      return NextResponse.json({ error: "No exports available from Union API" }, { status: 404 });
    }

    console.log(`[reprocess] Found ${allExports.length} exports. Processing newest first...`);

    const results: Array<{
      index: number;
      createdAt: string;
      dataRange: { start: string; end: string };
      success: boolean;
      duration?: number;
      error?: string;
    }> = [];

    // Process ALL exports (newest first so latest data wins upsert conflicts)
    for (let i = 0; i < allExports.length; i++) {
      const exp = allExports[i];
      console.log(`[reprocess] Export ${i + 1}/${allExports.length}: ${exp.createdAt} (${exp.dataRange.start} → ${exp.dataRange.end})`);

      try {
        const zipResult = await runZipWebhookPipeline({
          downloadUrl: exp.downloadUrl,
          onProgress: (step, pct) => {
            if (pct % 20 === 0) console.log(`[reprocess] Export ${i + 1}: ${step} (${pct}%)`);
          },
        });

        results.push({
          index: i,
          createdAt: exp.createdAt,
          dataRange: exp.dataRange,
          success: zipResult.success,
          duration: zipResult.duration,
        });
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        console.warn(`[reprocess] Export ${i + 1} failed: ${msg}`);
        results.push({
          index: i,
          createdAt: exp.createdAt,
          dataRange: exp.dataRange,
          success: false,
          error: msg,
        });
      }
    }

    // Bump cache
    await bumpDataVersion();
    invalidateStatsCache();

    const duration = Math.round((Date.now() - startTime) / 1000);
    const successCount = results.filter((r) => r.success).length;

    console.log(`[reprocess] Done in ${duration}s. ${successCount}/${allExports.length} exports succeeded.`);

    return NextResponse.json({
      success: successCount > 0,
      duration,
      exportsProcessed: allExports.length,
      exportsSucceeded: successCount,
      results,
    });
  } catch (e: unknown) {
    return NextResponse.json({ error: (e as Error).message }, { status: 500 });
  }
}
