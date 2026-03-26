import { NextRequest, NextResponse } from "next/server";
import { backfillUsageData } from "@/lib/db/usage-store";

export async function POST(request: NextRequest) {
  const { searchParams } = request.nextUrl;
  const weeks = Number(searchParams.get("weeks")) || 12;

  try {
    console.log(`[api/usage/backfill] Starting backfill for ${weeks} weeks...`);
    const start = Date.now();
    await backfillUsageData(weeks);
    const duration = Math.round((Date.now() - start) / 1000);
    console.log(`[api/usage/backfill] Complete in ${duration}s`);
    return NextResponse.json({ success: true, weeks, durationSeconds: duration });
  } catch (err) {
    console.error("[api/usage/backfill] Error:", err);
    return NextResponse.json(
      { error: err instanceof Error ? err.message : "Internal server error" },
      { status: 500 }
    );
  }
}
