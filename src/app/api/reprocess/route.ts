import { NextRequest, NextResponse } from "next/server";
import { loadSettings } from "@/lib/crypto/credentials";
import { fetchAllExports } from "@/lib/union-api/fetch-export";
import { runZipWebhookPipeline } from "@/lib/email/zip-download-pipeline";
import { bumpDataVersion, invalidateStatsCache } from "@/lib/cache/stats-cache";
import { sendDigestEmail } from "@/lib/email/email-sender";

export const dynamic = "force-dynamic";
export const maxDuration = 300; // 5 minutes

/**
 * GET /api/reprocess — List available exports with their date ranges.
 */
export async function GET() {
  try {
    const settings = loadSettings();
    if (!settings?.unionApiKey) {
      return NextResponse.json({ error: "No Union API key configured" }, { status: 500 });
    }
    const allExports = await fetchAllExports(settings.unionApiKey);
    return NextResponse.json({
      totalAvailable: allExports.length,
      exports: allExports.map((exp, i) => ({
        index: i,
        createdAt: exp.createdAt,
        dataRange: exp.dataRange,
      })),
    });
  } catch (e: unknown) {
    return NextResponse.json({ error: (e as Error).message }, { status: 500 });
  }
}

/**
 * POST /api/reprocess?limit=N&offset=M — Process Union API exports.
 * Default: limit=1, offset=0 (latest only).
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

    console.log(`[reprocess] Fetching exports from Union API (limit=${limit}, offset=${offset})...`);
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
          index: offset + i,
          createdAt: exp.createdAt,
          dataRange: exp.dataRange,
          success: zipResult.success,
          duration: zipResult.duration,
        });
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        console.warn(`[reprocess] Export ${i + 1} failed: ${msg}`);
        results.push({
          index: offset + i,
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

    // Send daily digest email (non-fatal).
    // The reprocess route bypasses BullMQ, so the worker's email trigger never fires.
    // The once-per-day atomic guard inside sendDigestEmail() prevents duplicates.
    if (successCount > 0) {
      try {
        const emailResult = await sendDigestEmail();
        if (emailResult.sent > 0) {
          console.log(`[reprocess] Digest email sent to ${emailResult.sent} recipients`);
        } else if (emailResult.skipped) {
          console.log(`[reprocess] Digest email skipped: ${emailResult.skipped}`);
        }
      } catch (emailErr) {
        console.warn(`[reprocess] Digest email failed (non-fatal):`, emailErr instanceof Error ? emailErr.message : emailErr);
      }
    }

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
