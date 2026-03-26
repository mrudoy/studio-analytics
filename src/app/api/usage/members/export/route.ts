import { NextRequest } from "next/server";
import { exportUsageMembers, type Segment } from "@/lib/db/usage-store";

export async function GET(request: NextRequest) {
  const { searchParams } = request.nextUrl;
  const segment = searchParams.get("segment") as Segment;
  const periodWeeks = Number(searchParams.get("period_weeks")) || 4;
  const filter = searchParams.get("filter") as "at_risk" | "newly_on_target" | "dormant" | "improving" | null;

  if (!segment || !["members", "sky3", "tv"].includes(segment)) {
    return new Response("Missing or invalid 'segment' parameter", { status: 400 });
  }

  try {
    const csv = await exportUsageMembers({ segment, periodWeeks, filter });
    const date = new Date().toISOString().slice(0, 10);
    const filename = `sky-ting-${segment}-${date}.csv`;

    return new Response(csv, {
      headers: {
        "Content-Type": "text/csv; charset=utf-8",
        "Content-Disposition": `attachment; filename="${filename}"`,
      },
    });
  } catch (err) {
    console.error("[api/usage/members/export] Error:", err);
    return new Response("Internal server error", { status: 500 });
  }
}
