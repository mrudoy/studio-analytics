import { NextRequest, NextResponse } from "next/server";
import { getUsageMembers, type Segment } from "@/lib/db/usage-store";

export async function GET(request: NextRequest) {
  const { searchParams } = request.nextUrl;
  const segment = searchParams.get("segment") as Segment;
  const periodWeeks = Number(searchParams.get("period_weeks")) || 4;
  const filter = searchParams.get("filter") as "at_risk" | "newly_on_target" | "dormant" | "improving" | null;
  const sortBy = searchParams.get("sort_by") || undefined;
  const sortDir = (searchParams.get("sort_dir") || "desc") as "asc" | "desc";
  const page = Number(searchParams.get("page")) || 1;
  const perPage = Number(searchParams.get("per_page")) || 25;

  if (!segment || !["members", "sky3", "tv"].includes(segment)) {
    return NextResponse.json({ error: "Missing or invalid 'segment' parameter" }, { status: 400 });
  }

  try {
    const result = await getUsageMembers({
      segment,
      periodWeeks,
      filter,
      sortBy,
      sortDir,
      page,
      perPage,
    });
    return NextResponse.json(result);
  } catch (err) {
    console.error("[api/usage/members] Error:", err);
    return NextResponse.json({ error: "Internal server error" }, { status: 500 });
  }
}
