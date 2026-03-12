import { NextRequest, NextResponse } from "next/server";
import { loadSettings } from "@/lib/crypto/credentials";
import { fetchAllExports } from "@/lib/union-api/fetch-export";
import { runZipWebhookPipeline } from "@/lib/email/zip-download-pipeline";
import { bumpDataVersion, invalidateStatsCache } from "@/lib/cache/stats-cache";

export const dynamic = "force-dynamic";
export const maxDuration = 300; // 5 minutes

/**
 * POST /api/reprocess?limit=N — Fetch and process the N most recent Union API
 * exports (default: 1 = latest only). Bypasses BullMQ queue.
 *
 * Each Union export is a daily delta. Processing just the latest is usually
 * enough to get fresh data. Use limit=3 or higher to backfill missed days.
 */
export async function POST(request: NextRequest) {
  const startTime = Date.now();
  const limit = Math.min(
    Number(request.nextUrl.searchParams.get("limit") || "1"),
    15
  );
  const offset = Math.max(
    Number(request.nextUrl.searchParams.get("offset") || "0"),
    0
  );

  try {
    const settings = loadSettings();
    if (!settings?.unionApiKey) {
      return NextResponse.json({ error: "No Union API key configured" }, { status: 500 });
    }

    console.log(`[reprocess] Fetching exports from Union API (limit=${limit})...`);
    const allExports = await fetchAllExports(settings.unionApiKey);

    if (allExports.length === 0) {
      return NextResponse.json({ error: "No exports available from Union API" }, { status: 404 });
    }

    // Take exports with offset and limit (API returns newest first)
    const toProcess = allExports.slice(offset, offset + limit);
    console.log(`[reprocess] Processing ${toProcess.length} exports (offset=${offset}) of ${allExports.length} available...`);

    const results: Array<{
      index: number;
      createdAt: string;
      dataRange: { start: string; end: string };
      success: boolean;
      duration?: number;
      error?: string;
    }> = [];

    for (let i = 0; i < toProcess.length; i++) {
      const exp = toProcess[i];
      console.log(`[reprocess] Export ${i + 1}/${toProcess.length}: ${exp.createdAt} (${exp.dataRange.start} → ${exp.dataRange.end})`);

      try {
        const zipResult = await runZipWebhookPipeline({
          downloadUrl: exp.downloadUrl,
          dataRange: exp.dataRange,
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

    console.log(`[reprocess] Done in ${duration}s. ${successCount}/${toProcess.length} exports succeeded.`);

    return NextResponse.json({
      success: successCount > 0,
      duration,
      exportsProcessed: toProcess.length,
      exportsSucceeded: successCount,
      totalAvailable: allExports.length,
      results,
    });
  } catch (e: unknown) {
    return NextResponse.json({ error: (e as Error).message }, { status: 500 });
  }
}
