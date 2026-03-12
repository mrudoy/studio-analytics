import { NextRequest, NextResponse } from "next/server";
import { recomputeMonthRevenue, recomputeRevenueFromDB } from "@/lib/analytics/db-revenue";
import { bumpDataVersion, invalidateStatsCache } from "@/lib/cache/stats-cache";

export const dynamic = "force-dynamic";

/**
 * POST /api/cron/revenue — Recompute revenue from accumulated DB data.
 *
 * Lightweight endpoint (pure DB queries, no API calls, no downloads).
 * Can run frequently as a self-healing check.
 *
 * Query params:
 *   ?month=YYYY-MM — recompute a specific month (useful for one-time fixes)
 *
 * Auth: Bearer token from CRON_SECRET env var.
 */
export async function POST(request: NextRequest) {
  // Auth check
  const authHeader = request.headers.get("authorization");
  const cronSecret = process.env.CRON_SECRET;
  if (cronSecret && authHeader !== `Bearer ${cronSecret}`) {
    return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
  }

  const startTime = Date.now();
  const specificMonth = request.nextUrl.searchParams.get("month");

  try {
    if (specificMonth) {
      // Recompute a specific month
      if (!/^\d{4}-\d{2}$/.test(specificMonth)) {
        return NextResponse.json({ error: "Invalid month format. Use YYYY-MM." }, { status: 400 });
      }

      console.log(`[cron/revenue] Recomputing revenue for ${specificMonth}...`);
      const saved = await recomputeMonthRevenue(specificMonth);

      await bumpDataVersion();
      invalidateStatsCache();

      const duration = Math.round((Date.now() - startTime) / 1000);
      return NextResponse.json({
        success: true,
        month: specificMonth,
        saved,
        duration,
      });
    }

    // Default: recompute current + previous month
    console.log("[cron/revenue] Recomputing revenue (current + previous month)...");
    await recomputeRevenueFromDB();

    await bumpDataVersion();
    invalidateStatsCache();

    const duration = Math.round((Date.now() - startTime) / 1000);
    console.log(`[cron/revenue] Done in ${duration}s`);

    return NextResponse.json({
      success: true,
      duration,
    });
  } catch (e: unknown) {
    const duration = Math.round((Date.now() - startTime) / 1000);
    console.error(`[cron/revenue] Error after ${duration}s:`, (e as Error).message);
    return NextResponse.json({ error: (e as Error).message, duration }, { status: 500 });
  }
}
