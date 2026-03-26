import { NextRequest, NextResponse } from "next/server";
import { getSky3MovementMembers } from "@/lib/db/usage-store";

export async function GET(request: NextRequest) {
  const { searchParams } = request.nextUrl;
  const direction = searchParams.get("direction") as "improving" | "stable" | "declining";
  const periodWeeks = Number(searchParams.get("period_weeks")) || 4;
  const from = searchParams.get("from") || undefined;
  const to = searchParams.get("to") || undefined;
  const fieldsOnly = searchParams.get("fields") === "email" ? "email" as const : undefined;
  const page = Number(searchParams.get("page")) || 1;
  const perPage = Number(searchParams.get("per_page")) || 25;

  if (!direction || !["improving", "stable", "declining"].includes(direction)) {
    return NextResponse.json({ error: "direction param required (improving|stable|declining)" }, { status: 400 });
  }

  try {
    const data = await getSky3MovementMembers({ direction, periodWeeks, from, to, fieldsOnly, page, perPage });
    return NextResponse.json(data);
  } catch (err) {
    console.error("[api/usage/sky3/movement/members] Error:", err);
    return NextResponse.json({ error: "Internal server error" }, { status: 500 });
  }
}
