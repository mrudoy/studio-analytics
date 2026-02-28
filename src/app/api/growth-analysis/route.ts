import { NextResponse } from "next/server";
import { getPool } from "@/lib/db/database";
import { getAnnualRevenueBreakdown, getAllMonthlyRevenue } from "@/lib/db/revenue-store";
import { getCategory, isAnnualPlan } from "@/lib/analytics/categories";
import { parseDate } from "@/lib/analytics/date-utils";

export const dynamic = "force-dynamic";

export async function GET() {
  try {
    // ── 1. Actual revenue by year and segment ──
    const annualBreakdown = await getAnnualRevenueBreakdown();
    const monthlyRevenue = await getAllMonthlyRevenue();

    // ── 2. Monthly revenue by category from revenue_categories ──
    const pool = getPool();
    const { rows: catMonthlyRows } = await pool.query(`
      WITH deduped AS (
        SELECT DISTINCT ON (category, LEFT(period_start, 7))
          category, revenue, net_revenue, period_start
        FROM revenue_categories
        WHERE LEFT(period_start, 7) = LEFT(period_end, 7)
        ORDER BY category, LEFT(period_start, 7), period_end DESC
      )
      SELECT
        LEFT(period_start, 7) AS month,
        CASE
          WHEN category ~* 'sky\\s*ting\\s*tv|10skyting|digital\\s*all|a\\s*la\\s*carte\\s*sky\\s*ting|sky\\s*week\\s*tv|friends\\s*of\\s*sky\\s*ting|new\\s*subscriber\\s*special|limited\\s*edition\\s*sky\\s*ting|come\\s*back\\s*sky\\s*ting|retreat\\s*ting'
            THEN 'Digital'
          WHEN category ~* 'sky\\s*3|sky\\s*5|skyhigh|5[\\s-]*pack'
            THEN 'Sky3'
          WHEN category ~* 'sky\\s*unlimited|all\\s*access|10member|sky\\s*ting\\s*(monthly\\s*)?membership|ting\\s*fam|sky\\s*virgin|founding\\s*member|new\\s*member|back\\s*to\\s*school|secret\\s*membership|monthly\\s*membership'
            THEN 'Membership'
          WHEN category ~* 'drop[\\s-]*in|droplet'
            THEN 'Drop-In'
          WHEN category ~* 'intro\\s*week|unlimited\\s*week|sky\\s*virgin\\s*2\\s*week'
            THEN 'Intro'
          WHEN category ~* 'infrared|sauna|spa\\s*lounge|cupping|contrast\\s*suite|treatment\\s*room|cold\\s*plunge'
            THEN 'Spa'
          WHEN category ~* 'retreat' AND NOT category ~* 'retreat\\s*ting'
            THEN 'Retreats'
          WHEN category ~* 'merch|product|food.*bev|gift\\s*card'
            THEN 'Merch'
          WHEN category ~* 'workshop|masterclass|specialty\\s*class'
            THEN 'Workshops'
          WHEN category ~* 'private'
            THEN 'Privates'
          WHEN category ~* 'teacher\\s*training|200hr|training'
            THEN 'Teacher Training'
          WHEN category ~* 'rental'
            THEN 'Rentals'
          ELSE 'Other'
        END AS segment,
        SUM(revenue) AS gross,
        SUM(net_revenue) AS net
      FROM deduped
      GROUP BY month, segment
      ORDER BY month, gross DESC
    `);

    // Build monthly segment map
    const monthlyBySegment: Record<string, Record<string, { gross: number; net: number }>> = {};
    for (const r of catMonthlyRows) {
      const month = r.month as string;
      const segment = r.segment as string;
      if (!monthlyBySegment[month]) monthlyBySegment[month] = {};
      monthlyBySegment[month][segment] = {
        gross: parseFloat(r.gross) || 0,
        net: parseFloat(r.net) || 0,
      };
    }

    // ── 3. Subscriber counts from auto_renews ──
    const { rows: allSubs } = await pool.query(
      `SELECT plan_name, plan_state, plan_price, canceled_at, created_at FROM auto_renews`
    );

    function toDateStr(raw: string | null | undefined): string | null {
      if (!raw || raw.trim() === "") return null;
      const d = parseDate(raw);
      if (!d) return null;
      return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}-${String(d.getDate()).padStart(2, "0")}`;
    }

    const ACTIVE_STATES = ["Valid Now", "Pending Cancel", "Paused", "Past Due", "In Trial"];
    const categorized = allSubs.map((r: Record<string, unknown>) => ({
      category: getCategory(r.plan_name as string),
      plan_state: r.plan_state as string,
      canceled_at: toDateStr(r.canceled_at as string | null),
      created_at: toDateStr(r.created_at as string | null),
    }));

    // Current active counts
    const currentCounts: Record<string, number> = {};
    for (const cat of ["MEMBER", "SKY3", "SKY_TING_TV"]) {
      currentCounts[cat] = categorized.filter(
        (r) => r.category === cat && ACTIVE_STATES.includes(r.plan_state)
      ).length;
    }

    // Monthly active subscriber counts for last 24 months
    const now = new Date();
    const months: string[] = [];
    for (let i = 23; i >= 0; i--) {
      const d = new Date(now.getFullYear(), now.getMonth() - i, 1);
      months.push(d.getFullYear() + "-" + String(d.getMonth() + 1).padStart(2, "0"));
    }

    const monthlySubCounts: Record<string, { month: string; active: number; newSubs: number; canceled: number }[]> = {};
    for (const cat of ["MEMBER", "SKY3", "SKY_TING_TV"]) {
      const catRows = categorized.filter((r) => r.category === cat);
      const data: { month: string; active: number; newSubs: number; canceled: number }[] = [];
      for (const month of months) {
        const monthStart = month + "-01";
        const [yearStr, moStr] = month.split("-");
        const nextMonth = new Date(parseInt(yearStr), parseInt(moStr), 1);
        const monthEnd = nextMonth.getFullYear() + "-" +
          String(nextMonth.getMonth() + 1).padStart(2, "0") + "-01";

        const active = catRows.filter((r) => {
          if (!r.created_at || r.created_at >= monthStart) return false;
          if (ACTIVE_STATES.includes(r.plan_state)) return true;
          if (r.canceled_at && r.canceled_at >= monthStart) return true;
          return false;
        }).length;

        const newInMonth = catRows.filter((r) =>
          r.created_at && r.created_at >= monthStart && r.created_at < monthEnd
        ).length;

        const canceledInMonth = catRows.filter((r) =>
          r.canceled_at && r.canceled_at >= monthStart && r.canceled_at < monthEnd
        ).length;

        data.push({ month, active, newSubs: newInMonth, canceled: canceledInMonth });
      }
      monthlySubCounts[cat] = data;
    }

    return NextResponse.json({
      annualBreakdown,
      monthlyRevenue: monthlyRevenue.map((m) => ({
        month: m.periodStart.slice(0, 7),
        gross: Math.round(m.totalRevenue),
        net: Math.round(m.totalNetRevenue),
      })),
      monthlyBySegment,
      currentSubCounts: currentCounts,
      monthlySubCounts,
    });
  } catch (err: unknown) {
    return NextResponse.json({ error: String(err) }, { status: 500 });
  }
}
