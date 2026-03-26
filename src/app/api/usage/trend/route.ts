import { NextRequest, NextResponse } from "next/server";
import { getUsageTrend, type Segment } from "@/lib/db/usage-store";

export async function GET(request: NextRequest) {
  const { searchParams } = request.nextUrl;
  const weeks = Number(searchParams.get("weeks")) || 12;
  const segment = (searchParams.get("segment") || "all") as Segment | "all";

  try {
    const series = await getUsageTrend(weeks, segment);
    return NextResponse.json({ series });
  } catch (err) {
    console.error("[api/usage/trend] Error:", err);
    return NextResponse.json({ error: "Internal server error" }, { status: 500 });
  }
}
