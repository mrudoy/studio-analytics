import { getActiveIntroWeekCustomers } from "@/lib/db/registration-store";

export async function GET() {
  try {
    // Default lookback (21d) covers the 14-day "2 WEEK INTRO" window.
    const rows = await getActiveIntroWeekCustomers();

    const lines: string[] = [
      "Name,Email,Intro Week Start,Intro Week End,Days Left,Classes Attended",
    ];
    for (const r of rows) {
      lines.push(
        `${csvEscape(r.name)},${csvEscape(r.email)},${r.startDate},${r.endDate},${r.daysLeft},${r.classesAttended}`
      );
    }
    const csv = lines.join("\n") + "\n";

    const date = new Date().toISOString().slice(0, 10);
    const filename = `intro-weeks-${date}.csv`;

    return new Response(csv, {
      headers: {
        "Content-Type": "text/csv",
        "Content-Disposition": `attachment; filename="${filename}"`,
      },
    });
  } catch (err) {
    console.error("Intro week export error:", err);
    return new Response("Internal server error", { status: 500 });
  }
}

function csvEscape(value: string): string {
  if (value.includes(",") || value.includes('"') || value.includes("\n")) {
    return `"${value.replace(/"/g, '""')}"`;
  }
  return value;
}
