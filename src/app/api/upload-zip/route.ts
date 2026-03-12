import { NextRequest, NextResponse } from "next/server";
import { writeFileSync, mkdirSync, existsSync, copyFileSync } from "fs";
import { join, basename } from "path";
import { extractCSVsFromZip } from "@/lib/email/zip-extract";
import { runZipLocalPipeline } from "@/lib/email/zip-download-pipeline";
import { bumpDataVersion, invalidateStatsCache } from "@/lib/cache/stats-cache";

export const dynamic = "force-dynamic";
export const maxDuration = 300; // 5 minutes

const UPLOADS_DIR = join(process.cwd(), "data", "zip-uploads");

/**
 * POST /api/upload-zip — Upload a Union.fit data export zip file directly.
 *
 * Accepts a multipart form upload with a "file" field containing the zip.
 * Extracts CSVs, runs the full import pipeline (passes, orders, refunds,
 * transfers, lookups → DB), then recomputes revenue.
 *
 * Auth: Bearer token from CRON_SECRET env var (or no auth if CRON_SECRET not set).
 *
 * Usage:
 *   curl -X POST https://your-app.up.railway.app/api/upload-zip \
 *     -H "Authorization: Bearer $CRON_SECRET" \
 *     -F "file=@/path/to/union_data_export.zip"
 */
export async function POST(request: NextRequest) {
  // Auth check
  const authHeader = request.headers.get("authorization");
  const cronSecret = process.env.CRON_SECRET;
  if (cronSecret && authHeader !== `Bearer ${cronSecret}`) {
    return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
  }

  const startTime = Date.now();

  try {
    const formData = await request.formData();
    const file = formData.get("file") as File | null;

    if (!file) {
      return NextResponse.json(
        { error: "No file uploaded. Send as 'file' in multipart form data." },
        { status: 400 }
      );
    }

    const sizeMB = (file.size / 1024 / 1024).toFixed(1);
    console.log(`[upload-zip] Received file: ${file.name} (${sizeMB} MB)`);

    // Save zip to disk
    if (!existsSync(UPLOADS_DIR)) {
      mkdirSync(UPLOADS_DIR, { recursive: true });
    }

    const zipBuffer = Buffer.from(await file.arrayBuffer());
    const zipPath = join(UPLOADS_DIR, `${Date.now()}-${file.name}`);
    writeFileSync(zipPath, zipBuffer);

    console.log(`[upload-zip] Saved to ${zipPath}`);

    // Extract CSVs from zip
    const extracted = extractCSVsFromZip(zipPath);

    if (extracted.length === 0) {
      return NextResponse.json(
        { error: "No CSV files found in the zip archive." },
        { status: 400 }
      );
    }

    console.log(
      `[upload-zip] Extracted ${extracted.length} CSVs: ${extracted.map((f) => f.originalName).join(", ")}`
    );

    // Create a temp directory with clean-named CSVs for runZipLocalPipeline.
    // The pipeline expects files named like "orders.csv", "passes.csv", etc.
    // but extractCSVsFromZip prepends timestamps to avoid collisions.
    const cleanDir = join(UPLOADS_DIR, `clean-${Date.now()}`);
    mkdirSync(cleanDir, { recursive: true });

    for (const f of extracted) {
      const cleanName = f.originalName.toLowerCase();
      copyFileSync(f.filePath, join(cleanDir, cleanName));
    }

    // Auto-infer data range from filename
    // e.g. union_data_export-sky-ting-20230830-20260223-xxx.zip
    let dataRange: { start: string; end: string } | undefined;
    const dateMatch = file.name.match(
      /(\d{4})(\d{2})(\d{2})-(\d{4})(\d{2})(\d{2})/
    );
    if (dateMatch) {
      dataRange = {
        start: `${dateMatch[1]}-${dateMatch[2]}-${dateMatch[3]}`,
        end: `${dateMatch[4]}-${dateMatch[5]}-${dateMatch[6]}`,
      };
      console.log(
        `[upload-zip] Inferred data range: ${dataRange.start} to ${dataRange.end}`
      );
    }

    // Run the full pipeline
    const result = await runZipLocalPipeline({
      csvDir: cleanDir,
      dataRange,
      onProgress: (msg, pct) => {
        console.log(`[upload-zip] ${pct}% — ${msg}`);
      },
    });

    // Bump cache after successful import
    await bumpDataVersion();
    invalidateStatsCache();

    const duration = Math.round((Date.now() - startTime) / 1000);
    console.log(`[upload-zip] Complete in ${duration}s`);

    return NextResponse.json({
      ...result,
      uploadedFile: file.name,
      uploadSizeMB: sizeMB,
      dataRange,
      duration,
    });
  } catch (e: unknown) {
    const duration = Math.round((Date.now() - startTime) / 1000);
    const msg = (e as Error).message;
    console.error(`[upload-zip] Error after ${duration}s:`, msg);
    return NextResponse.json({ error: msg, duration }, { status: 500 });
  }
}
