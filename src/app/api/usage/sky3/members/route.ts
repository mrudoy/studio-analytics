import { NextRequest, NextResponse } from "next/server";
import { getSky3MembersByBand } from "@/lib/db/usage-store";

export async function GET(request: NextRequest) {
  const { searchParams } = request.nextUrl;
  const band = searchParams.get("band");
  const periodWeeks = Number(searchParams.get("period_weeks")) || 4;
  const fieldsOnly = searchParams.get("fields") as "email" | undefined;
  const page = Number(searchParams.get("page")) || 1;
  const perPage = Number(searchParams.get("per_page")) || 25;

  if (!band) {
    return NextResponse.json({ error: "Missing 'band' parameter" }, { status: 400 });
  }

  try {
    const result = await getSky3MembersByBand({
      band,
      periodWeeks,
      fieldsOnly: fieldsOnly === "email" ? "email" : undefined,
      page,
      perPage,
    });
    return NextResponse.json(result);
  } catch (err) {
    console.error("[api/usage/sky3/members] Error:", err);
    return NextResponse.json({ error: "Internal server error" }, { status: 500 });
  }
}
