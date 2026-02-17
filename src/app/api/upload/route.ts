import { NextResponse } from "next/server";
import { saveUploadedData, saveRevenueCategories } from "@/lib/db/revenue-store";
import { saveAutoRenews, type AutoRenewRow } from "@/lib/db/auto-renew-store";
import { parseCSV } from "@/lib/parser/csv-parser";
import { RevenueCategorySchema, AutoRenewSchema } from "@/lib/parser/schemas";
import { writeFileSync } from "fs";
import { join } from "path";
import { mkdirSync, existsSync } from "fs";
import type { RevenueCategory, AutoRenew } from "@/types/union-data";

const UPLOAD_DIR = join(process.cwd(), "data", "uploads");

/**
 * Auto-renew data types that can be uploaded.
 * Multiple files can be uploaded for different states (active, canceled, etc.)
 * and they'll all be merged into the auto_renews table.
 */
const AUTO_RENEW_TYPES = [
  "auto_renews",
  "active_auto_renews",
  "canceled_auto_renews",
  "paused_auto_renews",
  "trialing_auto_renews",
  "new_auto_renews",
];

export async function POST(request: Request) {
  try {
    const formData = await request.formData();
    const file = formData.get("file") as File | null;
    const dataType = formData.get("type") as string | null;
    const periodStart = formData.get("periodStart") as string | null;
    const periodEnd = formData.get("periodEnd") as string | null;

    if (!file) {
      return NextResponse.json({ error: "No file provided" }, { status: 400 });
    }

    if (!dataType) {
      return NextResponse.json({ error: "No data type specified" }, { status: 400 });
    }

    const content = await file.text();
    const filename = file.name || "upload.csv";

    // Ensure upload directory exists
    if (!existsSync(UPLOAD_DIR)) {
      mkdirSync(UPLOAD_DIR, { recursive: true });
    }

    // Save raw file
    const savedPath = join(UPLOAD_DIR, `${Date.now()}-${filename}`);
    writeFileSync(savedPath, content, "utf8");

    // Save to uploaded_data table
    const period = periodStart && periodEnd ? `${periodStart} - ${periodEnd}` : null;
    const uploadId = await saveUploadedData(filename, dataType, period, content);

    let parsedCount = 0;
    const warnings: string[] = [];

    // ── Revenue categories upload ────────────────────────────
    if (dataType === "revenue_categories" && periodStart && periodEnd) {
      const result = parseCSV<RevenueCategory>(savedPath, RevenueCategorySchema);
      if (result.data.length > 0) {
        await saveRevenueCategories(periodStart, periodEnd, result.data);
        parsedCount = result.data.length;
      }
      warnings.push(...result.warnings);
    }

    // ── Auto-renew CSV upload ────────────────────────────────
    if (AUTO_RENEW_TYPES.includes(dataType)) {
      const result = parseCSV<AutoRenew>(savedPath, AutoRenewSchema);
      if (result.data.length > 0) {
        const snapshotId = `upload-${Date.now()}`;
        const arRows: AutoRenewRow[] = result.data.map((ar) => ({
          planName: ar.name,
          planState: ar.state,
          planPrice: ar.price,
          customerName: ar.customer,
          customerEmail: ar.email || "",
          createdAt: ar.created || "",
          canceledAt: ar.canceledAt || undefined,
        }));
        await saveAutoRenews(snapshotId, arRows);
        parsedCount = arRows.length;
        console.log(`[api/upload] Saved ${arRows.length} auto-renews from ${filename} (snapshot: ${snapshotId})`);
      }
      warnings.push(...result.warnings);
    }

    return NextResponse.json({
      success: true,
      uploadId,
      filename,
      dataType,
      parsedCount,
      warnings: warnings.slice(0, 10),
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Upload failed";
    console.error("[api/upload] Error:", message);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
