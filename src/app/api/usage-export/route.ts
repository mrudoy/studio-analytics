import { NextRequest } from "next/server";
import { getUsageDetailByCategory } from "@/lib/db/registration-store";

const VALID_CATEGORIES = ["MEMBER", "SKY3", "SKY_TING_TV"] as const;
const CATEGORY_LABELS: Record<string, string> = {
  MEMBER: "members",
  SKY3: "sky3",
  SKY_TING_TV: "sky-ting-tv",
};

export async function GET(request: NextRequest) {
  const { searchParams } = request.nextUrl;
  const category = searchParams.get("category");
  const segment = searchParams.get("segment") || undefined;

  if (!category || !VALID_CATEGORIES.includes(category as typeof VALID_CATEGORIES[number])) {
    return new Response("Missing or invalid 'category' parameter", { status: 400 });
  }

  try {
    const rows = await getUsageDetailByCategory(category, segment);

    // Build CSV
    const lines: string[] = ["Name,Email,Plan,Segment,Visits (90d),Avg/Mo"];
    for (const r of rows) {
      // Escape fields that may contain commas or quotes
      const name = csvEscape(r.name);
      const email = csvEscape(r.email);
      const plan = csvEscape(r.planName);
      const seg = csvEscape(r.segment);
      lines.push(`${name},${email},${plan},${seg},${r.totalVisits},${r.avgPerMonth}`);
    }
    const csv = lines.join("\n") + "\n";

    // Build filename: "members-dormant-2026-02-25.csv"
    const label = CATEGORY_LABELS[category] || category.toLowerCase();
    const segSlug = segment ? `-${segment.toLowerCase().replace(/[^a-z0-9]+/g, "-")}` : "";
    const date = new Date().toISOString().slice(0, 10);
    const filename = `${label}${segSlug}-${date}.csv`;

    return new Response(csv, {
      headers: {
        "Content-Type": "text/csv",
        "Content-Disposition": `attachment; filename="${filename}"`,
      },
    });
  } catch (err) {
    console.error("Usage export error:", err);
    return new Response("Internal server error", { status: 500 });
  }
}

function csvEscape(value: string): string {
  if (value.includes(",") || value.includes('"') || value.includes("\n")) {
    return `"${value.replace(/"/g, '""')}"`;
  }
  return value;
}
