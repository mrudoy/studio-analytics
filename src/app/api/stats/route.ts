import { NextResponse } from "next/server";
import { loadSettings } from "@/lib/crypto/credentials";
import { readDashboardStats, readTrendsData } from "@/lib/sheets/read-dashboard";
import { getSheetUrl } from "@/lib/sheets/sheets-client";

export async function GET() {
  try {
    const settings = loadSettings();
    const spreadsheetId =
      settings?.analyticsSpreadsheetId || process.env.ANALYTICS_SPREADSHEET_ID;

    if (!spreadsheetId) {
      return NextResponse.json(
        { error: "Analytics spreadsheet not configured" },
        { status: 503 }
      );
    }

    const [stats, trends] = await Promise.all([
      readDashboardStats(spreadsheetId),
      readTrendsData(spreadsheetId),
    ]);

    return NextResponse.json({
      ...stats,
      trends,
      spreadsheetUrl: getSheetUrl(spreadsheetId),
    });
  } catch (error) {
    const message =
      error instanceof Error ? error.message : "Failed to load stats";
    console.error("[api/stats] Error:", message);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
