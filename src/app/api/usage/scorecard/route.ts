import { NextRequest, NextResponse } from "next/server";
import { getUsageScorecard, type Segment } from "@/lib/db/usage-store";

export async function GET(request: NextRequest) {
  const { searchParams } = request.nextUrl;
  const periodWeeks = Number(searchParams.get("period_weeks")) || 4;
  const segment = (searchParams.get("segment") || "all") as Segment | "all";

  try {
    const cards = await getUsageScorecard(periodWeeks, segment);
    return NextResponse.json({ cards });
  } catch (err) {
    console.error("[api/usage/scorecard] Error:", err);
    return NextResponse.json({ error: "Internal server error" }, { status: 500 });
  }
}
