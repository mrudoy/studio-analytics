import { NextRequest, NextResponse } from "next/server";
import { getUsageSegments } from "@/lib/db/usage-store";

export async function GET(request: NextRequest) {
  const { searchParams } = request.nextUrl;
  const periodWeeks = Number(searchParams.get("period_weeks")) || 4;

  try {
    const segments = await getUsageSegments(periodWeeks);
    return NextResponse.json({ segments });
  } catch (err) {
    console.error("[api/usage/segments] Error:", err);
    return NextResponse.json({ error: "Internal server error" }, { status: 500 });
  }
}
