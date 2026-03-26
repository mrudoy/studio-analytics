import { NextResponse } from "next/server";
import { getTvEngagementDistribution } from "@/lib/db/usage-store";

export async function GET() {
  try {
    const tiers = await getTvEngagementDistribution();
    return NextResponse.json(tiers);
  } catch (err) {
    console.error("[api/usage/tv-engagement] Error:", err);
    return NextResponse.json({ error: "Internal server error" }, { status: 500 });
  }
}
