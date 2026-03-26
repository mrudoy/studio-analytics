import { NextResponse } from "next/server";
import { getSky3TierDistribution } from "@/lib/db/usage-store";

export async function GET() {
  try {
    const tiers = await getSky3TierDistribution();
    return NextResponse.json(tiers);
  } catch (err) {
    console.error("[api/usage/sky3-tiers] Error:", err);
    return NextResponse.json({ error: "Internal server error" }, { status: 500 });
  }
}
