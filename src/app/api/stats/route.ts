import { NextResponse } from "next/server";
import { loadSettings } from "@/lib/crypto/credentials";
import { readDashboardStats, readTrendsData } from "@/lib/sheets/read-dashboard";
import { getSheetUrl } from "@/lib/sheets/sheets-client";
import { getLatestPeriod, getRevenueForPeriod } from "@/lib/db/revenue-store";
import { analyzeRevenueCategories } from "@/lib/analytics/revenue-categories";
import { computeStatsFromDB } from "@/lib/analytics/db-stats";
import { computeTrendsFromDB } from "@/lib/analytics/db-trends";
import type { RevenueCategory } from "@/types/union-data";
import type { DashboardStats } from "@/lib/sheets/read-dashboard";
import type { TrendsData } from "@/lib/sheets/read-dashboard";

export async function GET() {
  try {
    const settings = loadSettings();
    const spreadsheetId =
      settings?.analyticsSpreadsheetId || process.env.ANALYTICS_SPREADSHEET_ID;

    // ── 1. Try database first ─────────────────────────────────
    let stats: DashboardStats | null = null;
    let trends: TrendsData | null = null;
    let dataSource: "database" | "sheets" | "hybrid" = "sheets";

    try {
      stats = await computeStatsFromDB();
      if (stats) {
        console.log("[api/stats] Loaded stats from database");
      }
    } catch (err) {
      console.warn("[api/stats] Database stats failed, will try Sheets:", err);
    }

    try {
      trends = await computeTrendsFromDB();
      if (trends) {
        console.log("[api/stats] Loaded trends from database");
      }
    } catch (err) {
      console.warn("[api/stats] Database trends failed, will try Sheets:", err);
    }

    // ── 2. Fall back to Sheets if needed ────────────────────
    if (!stats || !trends) {
      if (!spreadsheetId) {
        if (!stats) {
          return NextResponse.json(
            { error: "No data available — database empty and analytics spreadsheet not configured" },
            { status: 503 }
          );
        }
        // We have stats from database but no trends and no Sheets — return what we have
      } else {
        try {
          if (!stats) {
            stats = await readDashboardStats(spreadsheetId);
            console.log("[api/stats] Loaded stats from Sheets (database had no data)");
          }
          if (!trends) {
            trends = await readTrendsData(spreadsheetId);
            console.log("[api/stats] Loaded trends from Sheets (database had no data)");
          }
        } catch (sheetsErr) {
          console.warn("[api/stats] Sheets fallback also failed:", sheetsErr);
          if (!stats) {
            const message = sheetsErr instanceof Error ? sheetsErr.message : "Failed to load stats";
            return NextResponse.json({ error: message }, { status: 500 });
          }
        }
      }
    }

    // ── 3. Determine data source label ──────────────────────
    const statsFromDB = stats?.lastUpdated && !stats.spreadsheetUrl;
    const trendsFromDB = trends !== null && stats !== null && statsFromDB;

    if (statsFromDB && trendsFromDB) {
      dataSource = "database";
    } else if (statsFromDB || trendsFromDB) {
      dataSource = "hybrid";
    } else {
      dataSource = "sheets";
    }

    // ── 4. Revenue categories from database ───────────────────
    let revenueCategories = null;
    try {
      const latestPeriod = await getLatestPeriod();
      if (latestPeriod) {
        const rows = await getRevenueForPeriod(latestPeriod.periodStart, latestPeriod.periodEnd);
        if (rows.length > 0) {
          const asRevenueCategory: RevenueCategory[] = rows.map((r) => ({
            revenueCategory: r.category,
            revenue: r.revenue,
            unionFees: r.unionFees,
            stripeFees: r.stripeFees,
            otherFees: r.otherFees,
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
      console.warn("[api/stats] Failed to load revenue categories from database:", dbErr);
    }

    // ── 5. Return response ──────────────────────────────────
    return NextResponse.json({
      ...(stats || {}),
      trends,
      revenueCategories,
      dataSource,
      spreadsheetUrl: spreadsheetId ? getSheetUrl(spreadsheetId) : undefined,
    });
  } catch (error) {
    const message =
      error instanceof Error ? error.message : "Failed to load stats";
    console.error("[api/stats] Error:", message);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
