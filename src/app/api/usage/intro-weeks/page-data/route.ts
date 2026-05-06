import { NextResponse } from "next/server";
import { getIntroWeeksPageData } from "@/lib/db/usage-store";

export async function GET() {
  try {
    const data = await getIntroWeeksPageData();
    return NextResponse.json(data);
  } catch (err) {
    console.error("[api/usage/intro-weeks/page-data] Error:", err);
    return NextResponse.json({ error: "Internal server error" }, { status: 500 });
  }
}
