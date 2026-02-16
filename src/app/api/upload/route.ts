import { NextResponse } from "next/server";
import { saveUploadedData, saveRevenueCategories } from "@/lib/db/revenue-store";
import { parseCSV } from "@/lib/parser/csv-parser";
import { RevenueCategorySchema } from "@/lib/parser/schemas";
import { writeFileSync } from "fs";
import { join } from "path";
import { mkdirSync, existsSync } from "fs";
import type { RevenueCategory } from "@/types/union-data";

const UPLOAD_DIR = join(process.cwd(), "data", "uploads");

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
    const uploadId = saveUploadedData(filename, dataType, period, content);

    let parsedCount = 0;
    const warnings: string[] = [];

    // If it's a revenue_categories upload, also parse and insert into revenue_categories table
    if (dataType === "revenue_categories" && periodStart && periodEnd) {
      const result = parseCSV<RevenueCategory>(savedPath, RevenueCategorySchema);
      if (result.data.length > 0) {
        saveRevenueCategories(periodStart, periodEnd, result.data);
        parsedCount = result.data.length;
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
