import { NextResponse } from "next/server";
import { loadSettings } from "@/lib/crypto/credentials";
import { readDashboardStats, readTrendsData } from "@/lib/sheets/read-dashboard";
import { getSheetUrl } from "@/lib/sheets/sheets-client";
import { getLatestPeriod, getRevenueForPeriod } from "@/lib/db/revenue-store";
import { analyzeRevenueCategories } from "@/lib/analytics/revenue-categories";
import type { RevenueCategory } from "@/types/union-data";

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

    // Load revenue category data from SQLite (if available)
    let revenueCategories = null;
    try {
      const latestPeriod = getLatestPeriod();
      if (latestPeriod) {
        const rows = getRevenueForPeriod(latestPeriod.periodStart, latestPeriod.periodEnd);
        if (rows.length > 0) {
          const asRevenueCategory: RevenueCategory[] = rows.map((r) => ({
            revenueCategory: r.category,
            revenue: r.revenue,
            unionFees: r.unionFees,
            stripeFees: r.stripeFees,
            transfers: r.transfers,
            refunded: r.refunded,
            unionFeesRefunded: r.unionFeesRefunded,
            netRevenue: r.netRevenue,
          }));
          revenueCategories = {
            periodStart: latestPeriod.periodStart,
            periodEnd: latestPeriod.periodEnd,
            ...analyzeRevenueCategories(asRevenueCategory),
          };
        }
      }
    } catch (dbErr) {
      console.warn("[api/stats] Failed to load revenue categories from SQLite:", dbErr);
    }

    return NextResponse.json({
      ...stats,
      trends,
      revenueCategories,
      spreadsheetUrl: getSheetUrl(spreadsheetId),
    });
  } catch (error) {
    const message =
      error instanceof Error ? error.message : "Failed to load stats";
    console.error("[api/stats] Error:", message);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
