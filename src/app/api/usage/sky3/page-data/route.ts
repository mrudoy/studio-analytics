import { NextRequest, NextResponse } from "next/server";
import { getSky3PageData } from "@/lib/db/usage-store";

export async function GET(request: NextRequest) {
  const { searchParams } = request.nextUrl;
  const periodWeeks = Number(searchParams.get("period_weeks")) || 4;

  try {
    const data = await getSky3PageData(periodWeeks);
    return NextResponse.json(data);
  } catch (err) {
    console.error("[api/usage/sky3/page-data] Error:", err);
    return NextResponse.json({ error: "Internal server error" }, { status: 500 });
  }
}
