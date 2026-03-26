import { NextRequest, NextResponse } from "next/server";
import { getUsageMovement, type Segment } from "@/lib/db/usage-store";

export async function GET(request: NextRequest) {
  const { searchParams } = request.nextUrl;
  const periodWeeks = Number(searchParams.get("period_weeks")) || 4;
  const segment = (searchParams.get("segment") || "all") as Segment | "all";

  try {
    const movement = await getUsageMovement(periodWeeks, segment);
    return NextResponse.json(movement);
  } catch (err) {
    console.error("[api/usage/movement] Error:", err);
    return NextResponse.json({ error: "Internal server error" }, { status: 500 });
  }
}
