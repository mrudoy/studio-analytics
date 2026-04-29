import { NextRequest, NextResponse } from "next/server";
import { getTvMembersByBand } from "@/lib/db/usage-store";

export async function GET(request: NextRequest) {
  const { searchParams } = request.nextUrl;
  const band = searchParams.get("band");
  const fieldsOnly = searchParams.get("fields") as "email" | undefined;
  const page = Number(searchParams.get("page")) || 1;
  const perPage = Number(searchParams.get("per_page")) || 25;

  if (!band) {
    return NextResponse.json({ error: "Missing 'band' parameter" }, { status: 400 });
  }

  try {
    const result = await getTvMembersByBand({
      band,
      fieldsOnly: fieldsOnly === "email" ? "email" : undefined,
      page,
      perPage,
    });
    return NextResponse.json(result);
  } catch (err) {
    console.error("[api/usage/tv/members] Error:", err);
    return NextResponse.json({ error: "Internal server error" }, { status: 500 });
  }
}
