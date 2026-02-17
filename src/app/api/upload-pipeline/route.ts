import { NextResponse } from "next/server";
import { loadSettings } from "@/lib/crypto/credentials";
import { runPipelineFromFiles } from "@/lib/queue/pipeline-core";
import type { DownloadedFiles } from "@/types/union-data";
import { writeFileSync, mkdirSync, existsSync } from "fs";
import { join } from "path";

const UPLOAD_DIR = join(process.cwd(), "data", "uploads");

/** All report types that can be uploaded */
const REQUIRED_REPORTS = [
  "newCustomers",
  "orders",
  "firstVisits",
  "canceledAutoRenews",
  "activeAutoRenews",
  "pausedAutoRenews",
  "trialingAutoRenews",
  "newAutoRenews",
] as const;

const OPTIONAL_REPORTS = [
  "allRegistrations",
  "revenueCategories",
  "fullRegistrations",
] as const;

const ALL_REPORTS = [...REQUIRED_REPORTS, ...OPTIONAL_REPORTS] as const;

type ReportType = (typeof ALL_REPORTS)[number];

export async function POST(request: Request) {
  try {
    // Load settings for Sheets ID
    const settings = loadSettings();
    const analyticsSheetId =
      settings?.analyticsSpreadsheetId || process.env.ANALYTICS_SPREADSHEET_ID;
    const rawDataSheetId =
      settings?.rawDataSpreadsheetId || process.env.RAW_DATA_SPREADSHEET_ID;

    if (!analyticsSheetId) {
      return NextResponse.json(
        { error: "Analytics Spreadsheet ID not configured. Go to Settings." },
        { status: 503 }
      );
    }

    const formData = await request.formData();

    // Ensure upload directory exists
    if (!existsSync(UPLOAD_DIR)) {
      mkdirSync(UPLOAD_DIR, { recursive: true });
    }

    // Save each uploaded file and build DownloadedFiles map
    const savedPaths: Partial<Record<ReportType, string>> = {};
    const receivedReports: string[] = [];

    for (const reportType of ALL_REPORTS) {
      const file = formData.get(reportType) as File | null;
      if (!file || file.size === 0) continue;

      const content = await file.text();
      const filename = `${Date.now()}-${reportType}.csv`;
      const savedPath = join(UPLOAD_DIR, filename);
      writeFileSync(savedPath, content, "utf8");
      savedPaths[reportType] = savedPath;
      receivedReports.push(reportType);
    }

    // Check required reports
    const missing = REQUIRED_REPORTS.filter((r) => !savedPaths[r]);
    if (missing.length > 0) {
      return NextResponse.json(
        {
          error: `Missing required reports: ${missing.join(", ")}`,
          received: receivedReports,
          missing,
        },
        { status: 400 }
      );
    }

    // Build DownloadedFiles (required fields guaranteed present by check above)
    const files: DownloadedFiles = {
      newCustomers: savedPaths.newCustomers!,
      orders: savedPaths.orders!,
      firstVisits: savedPaths.firstVisits!,
      allRegistrations: savedPaths.allRegistrations,
      canceledAutoRenews: savedPaths.canceledAutoRenews!,
      activeAutoRenews: savedPaths.activeAutoRenews!,
      pausedAutoRenews: savedPaths.pausedAutoRenews!,
      trialingAutoRenews: savedPaths.trialingAutoRenews!,
      newAutoRenews: savedPaths.newAutoRenews!,
      revenueCategories: savedPaths.revenueCategories,
      fullRegistrations: savedPaths.fullRegistrations!,
    };

    console.log(`[upload-pipeline] Received ${receivedReports.length} reports: ${receivedReports.join(", ")}`);

    // Run the full pipeline from uploaded files
    const result = await runPipelineFromFiles(files, analyticsSheetId, {
      rawDataSheetId,
      onProgress: (step, percent) => {
        console.log(`[upload-pipeline] ${step} (${percent}%)`);
      },
    });

    return NextResponse.json({
      success: result.success,
      sheetUrl: result.sheetUrl,
      duration: result.duration,
      recordCounts: result.recordCounts,
      warnings: result.warnings.slice(0, 20),
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Upload pipeline failed";
    console.error("[upload-pipeline] Error:", message);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
