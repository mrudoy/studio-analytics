import { NextRequest, NextResponse } from "next/server";
import { getUsageFlow, type Segment } from "@/lib/db/usage-store";

export async function GET(request: NextRequest) {
  const { searchParams } = request.nextUrl;
  const segment = (searchParams.get("segment") || "members") as Segment;
  if (!["members", "sky3", "tv"].includes(segment)) {
    return NextResponse.json({ error: "Invalid segment" }, { status: 400 });
  }
  try {
    const data = await getUsageFlow(segment);
    return NextResponse.json(data);
  } catch (err) {
    console.error("[api/usage/flow] Error:", err);
    return NextResponse.json({ error: "Internal server error" }, { status: 500 });
  }
}
