import { NextResponse } from "next/server";
import { getPool } from "@/lib/db/database";
import { getCategory, isAnnualPlan } from "@/lib/analytics/categories";
import { parseDate } from "@/lib/analytics/date-utils";

export const dynamic = "force-dynamic";

export async function GET() {
  try {
    const pool = getPool();
    const { rows: allRows } = await pool.query(
      `SELECT plan_name, plan_state, plan_price, canceled_at, created_at, customer_email FROM auto_renews`
    );

    function toDateStr(raw: string | null | undefined): string | null {
      if (!raw || raw.trim() === "") return null;
      const d = parseDate(raw);
      if (!d) return null;
      return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}-${String(d.getDate()).padStart(2, "0")}`;
    }

    const ACTIVE_STATES = ["Valid Now", "Pending Cancel", "Paused", "Past Due", "In Trial"];
    const categorized = allRows.map((r: Record<string, unknown>) => {
      const name = r.plan_name as string;
      const price = (r.plan_price as number) || 0;
      const annual = isAnnualPlan(name);
      return {
        plan_name: name,
        plan_state: r.plan_state as string,
        plan_price: price,
        canceled_at: toDateStr(r.canceled_at as string | null),
        created_at: toDateStr(r.created_at as string | null),
        category: getCategory(name),
        isAnnual: annual,
        monthlyRate: annual ? Math.round((price / 12) * 100) / 100 : price,
        customer_email: ((r.customer_email as string) || "").toLowerCase(),
      };
    });

    // Generate last 24 months
    const now = new Date();
    const months: string[] = [];
    for (let i = 23; i >= 0; i--) {
      const d = new Date(now.getFullYear(), now.getMonth() - i, 1);
      months.push(d.getFullYear() + "-" + String(d.getMonth() + 1).padStart(2, "0"));
    }

    const CATEGORIES = ["MEMBER", "SKY3", "SKY_TING_TV"] as const;
    type CatKey = typeof CATEGORIES[number];

    interface MonthData {
      month: string;
      activeCount: number;
      mrr: number;
      newSubs: number;
      canceledCount: number;
      canceledMrr: number;
      netGrowth: number;
    }

    const result: Record<string, MonthData[]> = {};

    for (const cat of CATEGORIES) {
      const catRows = categorized.filter((r) => r.category === cat);
      const monthData: MonthData[] = [];

      for (const month of months) {
        const monthStart = month + "-01";
        const [yearStr, moStr] = month.split("-");
        const nextMonth = new Date(parseInt(yearStr), parseInt(moStr), 1);
        const monthEnd = nextMonth.getFullYear() + "-" +
          String(nextMonth.getMonth() + 1).padStart(2, "0") + "-01";

        // Active at start of month
        const activeAtStart = catRows.filter((r) => {
          if (!r.created_at || r.created_at >= monthStart) return false;
          if (ACTIVE_STATES.includes(r.plan_state)) return true;
          if (r.canceled_at && r.canceled_at >= monthStart) return true;
          return false;
        });

        // New subscribers this month
        const newInMonth = catRows.filter((r) =>
          r.created_at && r.created_at >= monthStart && r.created_at < monthEnd
        );

        // Canceled during this month
        const canceledInMonth = catRows.filter((r) =>
          r.canceled_at && r.canceled_at >= monthStart && r.canceled_at < monthEnd
        );

        const mrr = activeAtStart.reduce((s, r) => s + r.monthlyRate, 0);
        const canceledMrr = canceledInMonth.reduce((s, r) => s + r.monthlyRate, 0);

        monthData.push({
          month,
          activeCount: activeAtStart.length,
          mrr: Math.round(mrr),
          newSubs: newInMonth.length,
          canceledCount: canceledInMonth.length,
          canceledMrr: Math.round(canceledMrr),
          netGrowth: newInMonth.length - canceledInMonth.length,
        });
      }

      result[cat] = monthData;
    }

    // Current snapshot
    const currentSnapshot: Record<string, { active: number; mrr: number; arr: number }> = {};
    for (const cat of CATEGORIES) {
      const active = categorized.filter(
        (r) => r.category === cat && ACTIVE_STATES.includes(r.plan_state)
      );
      const mrr = active.reduce((s, r) => s + r.monthlyRate, 0);
      currentSnapshot[cat] = {
        active: active.length,
        mrr: Math.round(mrr),
        arr: Math.round(mrr * 12),
      };
    }

    // YoY comparison: last 12 months vs prior 12 months
    const yoyComparison: Record<string, {
      last12: { avgActive: number; avgMrr: number; totalNew: number; totalCanceled: number; totalArr: number };
      prior12: { avgActive: number; avgMrr: number; totalNew: number; totalCanceled: number; totalArr: number };
      growthPct: number;
    }> = {};

    for (const cat of CATEGORIES) {
      const data = result[cat];
      const last12 = data.slice(-12);
      const prior12 = data.slice(0, 12);

      const l = {
        avgActive: Math.round(last12.reduce((s, m) => s + m.activeCount, 0) / last12.length),
        avgMrr: Math.round(last12.reduce((s, m) => s + m.mrr, 0) / last12.length),
        totalNew: last12.reduce((s, m) => s + m.newSubs, 0),
        totalCanceled: last12.reduce((s, m) => s + m.canceledCount, 0),
        totalArr: 0,
      };
      l.totalArr = l.avgMrr * 12;

      const p = {
        avgActive: Math.round(prior12.reduce((s, m) => s + m.activeCount, 0) / prior12.length),
        avgMrr: Math.round(prior12.reduce((s, m) => s + m.mrr, 0) / prior12.length),
        totalNew: prior12.reduce((s, m) => s + m.newSubs, 0),
        totalCanceled: prior12.reduce((s, m) => s + m.canceledCount, 0),
        totalArr: 0,
      };
      p.totalArr = p.avgMrr * 12;

      yoyComparison[cat] = {
        last12: l,
        prior12: p,
        growthPct: p.avgMrr > 0 ? Math.round(((l.avgMrr - p.avgMrr) / p.avgMrr) * 1000) / 10 : 0,
      };
    }

    // Monthly growth rates (last 12 months, month-over-month active count change)
    const growthRates: Record<string, { month: string; rate: number }[]> = {};
    for (const cat of CATEGORIES) {
      const data = result[cat];
      const rates: { month: string; rate: number }[] = [];
      for (let i = 13; i < data.length; i++) {
        const prev = data[i - 1].activeCount;
        const curr = data[i].activeCount;
        rates.push({
          month: data[i].month,
          rate: prev > 0 ? Math.round(((curr - prev) / prev) * 1000) / 10 : 0,
        });
      }
      growthRates[cat] = rates;
    }

    // Total combined
    const totalCurrentMrr = Object.values(currentSnapshot).reduce((s, c) => s + c.mrr, 0);
    const totalCurrentArr = totalCurrentMrr * 12;

    return NextResponse.json({
      currentSnapshot,
      totalCurrentMrr,
      totalCurrentArr,
      monthlyData: result,
      yoyComparison,
      growthRates,
    });
  } catch (err: unknown) {
    return NextResponse.json({ error: String(err) }, { status: 500 });
  }
}
