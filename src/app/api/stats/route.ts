import { NextResponse } from "next/server";
import { loadSettings } from "@/lib/crypto/credentials";
import { readDashboardStats } from "@/lib/sheets/read-dashboard";
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

    const stats = await readDashboardStats(spreadsheetId);
    return NextResponse.json({
      ...stats,
      spreadsheetUrl: getSheetUrl(spreadsheetId),
    });
  } catch (error) {
    const message =
      error instanceof Error ? error.message : "Failed to load stats";
    console.error("[api/stats] Error:", message);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
