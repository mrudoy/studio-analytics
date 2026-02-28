import { NextResponse } from "next/server";
import { getSky3PreSubscribeBehavior } from "@/lib/db/registration-store";

export const dynamic = "force-dynamic";

export async function GET() {
  try {
    const rows = await getSky3PreSubscribeBehavior();

    // Categorize pre-subscribe behavior
    const newToStudio = rows.filter((r) => r.visits_ever_before_sub === 0);
    const recentTrialers = rows.filter(
      (r) => r.visits_ever_before_sub > 0 && r.visits_90d_before_sub > 0 && r.visits_90d_before_sub <= 3
    );
    const activeDropIns = rows.filter(
      (r) => r.visits_90d_before_sub >= 4
    );
    const lapsedReturners = rows.filter(
      (r) => r.visits_ever_before_sub > 0 && r.visits_90d_before_sub === 0
    );

    // Attendance change during vs before
    const withPreActivity = rows.filter((r) => r.visits_90d_before_sub > 0 && r.tenure_months >= 1);
    let increased = 0, decreased = 0, neutral = 0;
    for (const r of withPreActivity) {
      const monthsDuring = Math.max(parseFloat(r.tenure_months), 1);
      const perMonthBefore = r.visits_90d_before_sub / 3; // 90 days = ~3 months
      const perMonthDuring = r.visits_during_sub / monthsDuring;
      if (perMonthDuring > perMonthBefore * 1.2) increased++;
      else if (perMonthDuring < perMonthBefore * 0.8) decreased++;
      else neutral++;
    }

    const summary = {
      total: rows.length,
      preSkyBehavior: {
        newToStudio: { count: newToStudio.length, pct: ((newToStudio.length / rows.length) * 100).toFixed(1) },
        recentTrialers: { count: recentTrialers.length, pct: ((recentTrialers.length / rows.length) * 100).toFixed(1), desc: "1-3 visits in 90d before sub" },
        activeDropIns: { count: activeDropIns.length, pct: ((activeDropIns.length / rows.length) * 100).toFixed(1), desc: "4+ visits in 90d before sub" },
        lapsedReturners: { count: lapsedReturners.length, pct: ((lapsedReturners.length / rows.length) * 100).toFixed(1), desc: "Had visited before but not in 90d pre-sub" },
      },
      attendanceChange: {
        withPreActivity: withPreActivity.length,
        increased: { count: increased, pct: ((increased / withPreActivity.length) * 100).toFixed(1) },
        decreased: { count: decreased, pct: ((decreased / withPreActivity.length) * 100).toFixed(1) },
        neutral: { count: neutral, pct: ((neutral / withPreActivity.length) * 100).toFixed(1) },
      },
      // First 30 rows with details
      sample: rows.slice(0, 30).map((r) => ({
        email: r.email,
        name: r.name,
        plan: r.plan_name,
        subscribedAt: r.created_at,
        canceledAt: r.canceled_at,
        tenureMonths: r.tenure_months,
        visitsEverBeforeSub: r.visits_ever_before_sub,
        visits90dBeforeSub: r.visits_90d_before_sub,
        visits30dBeforeSub: r.visits_30d_before_sub,
        visitsDuringSub: r.visits_during_sub,
        daysAsCustomerBeforeSub: r.days_as_customer_before_sub,
      })),
    };

    return NextResponse.json(summary);
  } catch (err: unknown) {
    return NextResponse.json({ error: String(err) }, { status: 500 });
  }
}
