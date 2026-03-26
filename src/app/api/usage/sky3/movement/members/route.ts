import { NextRequest, NextResponse } from "next/server";
import { getSky3MovementMembers } from "@/lib/db/usage-store";

const VALID_GROUPS = [
  "boundary_into_success",
  "boundary_into_risk",
  "within_risk_improving",
  "within_risk_declining",
  "within_success_improving",
  "within_success_declining",
] as const;

export async function GET(request: NextRequest) {
  const { searchParams } = request.nextUrl;
  const group = searchParams.get("group") as typeof VALID_GROUPS[number];
  const periodWeeks = Number(searchParams.get("period_weeks")) || 4;
  const from = searchParams.get("from") || undefined;
  const to = searchParams.get("to") || undefined;
  const fieldsOnly = searchParams.get("fields") === "email" ? "email" as const : undefined;
  const page = Number(searchParams.get("page")) || 1;
  const perPage = Number(searchParams.get("per_page")) || 25;

  if (!group || !VALID_GROUPS.includes(group)) {
    return NextResponse.json({ error: `group param required (${VALID_GROUPS.join("|")})` }, { status: 400 });
  }

  try {
    const data = await getSky3MovementMembers({ group, periodWeeks, from, to, fieldsOnly, page, perPage });
    return NextResponse.json(data);
  } catch (err) {
    console.error("[api/usage/sky3/movement/members] Error:", err);
    return NextResponse.json({ error: "Internal server error" }, { status: 500 });
  }
}
