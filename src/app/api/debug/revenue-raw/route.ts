import { NextRequest, NextResponse } from "next/server";
import { getPool } from "@/lib/db/database";

export const dynamic = "force-dynamic";

/**
 * Debug endpoint: shows raw revenue_categories entries for a given month,
 * plus order/pass amount sums to compare against.
 */
export async function GET(request: NextRequest) {
  const month = request.nextUrl.searchParams.get("month") || "2026-01";

  try {
    const pool = getPool();

    // 1. Raw revenue_categories entries for this month (all periods, not deduped)
    const { rows: rawCats } = await pool.query(
      `SELECT period_start, period_end, category, revenue, net_revenue, refunded,
              union_fees, stripe_fees, other_fees, union_fees_refunded, created_at
       FROM revenue_categories
       WHERE TO_CHAR(period_start, 'YYYY-MM') = $1
       ORDER BY category, period_end DESC`,
      [month]
    );

    // 2. Deduped view (what the dashboard actually uses)
    const { rows: dedupedCats } = await pool.query(
      `WITH deduped AS (
         SELECT DISTINCT ON (category, TO_CHAR(period_start, 'YYYY-MM'))
           period_start, period_end, category, revenue, net_revenue, refunded
         FROM revenue_categories
         WHERE DATE_TRUNC('month', period_start) = DATE_TRUNC('month', period_end)
           AND TO_CHAR(period_start, 'YYYY-MM') = $1
         ORDER BY category, TO_CHAR(period_start, 'YYYY-MM'), period_end DESC
       )
       SELECT * FROM deduped ORDER BY revenue DESC`,
      [month]
    );

    // 3. Sum of order totals (raw DB data) for comparison
    const { rows: orderSum } = await pool.query(
      `SELECT COUNT(*) AS cnt, SUM(total) AS total_sum,
              SUM(CASE WHEN total > 0 THEN total ELSE 0 END) AS positive_sum,
              COUNT(CASE WHEN total > 0 THEN 1 END) AS positive_cnt
       FROM orders
       WHERE TO_CHAR(COALESCE(completed_at, created_at), 'YYYY-MM') = $1
         AND (state IS NULL OR LOWER(state) IN ('completed', 'refunded'))`,
      [month]
    );

    // 4. Sum of pass totals (non-subscription, the Step 2 data)
    const { rows: passSum } = await pool.query(
      `SELECT COUNT(*) AS cnt, SUM(p.total) AS total_sum
       FROM passes p
       JOIN orders o ON p.order_id = o.union_order_id
       WHERE LOWER(p.pass_category_name) != 'subscription'
         AND p.total > 0
         AND p.order_id IS NOT NULL AND p.order_id != ''
         AND TO_CHAR(COALESCE(o.completed_at, o.created_at), 'YYYY-MM') = $1
         AND (o.state IS NULL OR LOWER(o.state) IN ('completed', 'refunded'))`,
      [month]
    );

    // 5. Non-pass orders (Step 3 — orders NOT linked to passes)
    const { rows: nonPassOrderSum } = await pool.query(
      `WITH pass_order_ids AS (
         SELECT DISTINCT p.order_id
         FROM passes p
         JOIN orders o ON p.order_id = o.union_order_id
         WHERE LOWER(p.pass_category_name) != 'subscription'
           AND p.total > 0
           AND p.order_id IS NOT NULL AND p.order_id != ''
           AND TO_CHAR(COALESCE(o.completed_at, o.created_at), 'YYYY-MM') = $1
           AND (o.state IS NULL OR LOWER(o.state) IN ('completed', 'refunded'))
       )
       SELECT COUNT(*) AS cnt, SUM(o.total) AS total_sum
       FROM orders o
       WHERE TO_CHAR(COALESCE(o.completed_at, o.created_at), 'YYYY-MM') = $1
         AND (o.state IS NULL OR LOWER(o.state) IN ('completed', 'refunded'))
         AND o.total > 0
         AND o.union_order_id NOT IN (SELECT order_id FROM pass_order_ids)`,
      [month]
    );

    // 6. Check for multi-month periods that might cover this month
    const { rows: multiMonthPeriods } = await pool.query(
      `SELECT period_start, period_end, category, revenue, net_revenue
       FROM revenue_categories
       WHERE period_start <= ($1 || '-01')::date
         AND period_end >= (($1 || '-01')::date + interval '1 month' - interval '1 day')::date
         AND DATE_TRUNC('month', period_start) != DATE_TRUNC('month', period_end)
       ORDER BY category`,
      [month]
    );

    // Compute totals
    const dedupedTotal = dedupedCats.reduce((s, r) => s + Number(r.revenue), 0);
    const dedupedNetTotal = dedupedCats.reduce((s, r) => s + Number(r.net_revenue), 0);
    const retreatTotal = dedupedCats
      .filter((r) => /retreat/i.test(r.category) && !/retreat\s*ting/i.test(r.category))
      .reduce((s, r) => s + Number(r.revenue), 0);

    return NextResponse.json({
      month,
      rawEntryCount: rawCats.length,
      dedupedEntryCount: dedupedCats.length,
      dedupedGrossTotal: Math.round(dedupedTotal),
      dedupedNetTotal: Math.round(dedupedNetTotal),
      retreatGross: Math.round(retreatTotal),
      grossMinusRetreat: Math.round(dedupedTotal - retreatTotal),
      multiMonthPeriodCount: multiMonthPeriods.length,
      dbOrdersTotal: {
        count: Number(orderSum[0]?.cnt || 0),
        totalSum: Math.round(Number(orderSum[0]?.total_sum || 0)),
        positiveSum: Math.round(Number(orderSum[0]?.positive_sum || 0)),
        positiveCount: Number(orderSum[0]?.positive_cnt || 0),
      },
      dbPassTotal: {
        count: Number(passSum[0]?.cnt || 0),
        totalSum: Math.round(Number(passSum[0]?.total_sum || 0)),
      },
      dbNonPassOrderTotal: {
        count: Number(nonPassOrderSum[0]?.cnt || 0),
        totalSum: Math.round(Number(nonPassOrderSum[0]?.total_sum || 0)),
      },
      computedGross: Math.round(
        Number(passSum[0]?.total_sum || 0) +
        Number(nonPassOrderSum[0]?.total_sum || 0)
      ),
      dedupedCategories: dedupedCats.map((r) => ({
        category: r.category,
        revenue: Math.round(Number(r.revenue)),
        net: Math.round(Number(r.net_revenue)),
        refunded: Math.round(Number(r.refunded)),
        periodStart: r.period_start,
        periodEnd: r.period_end,
      })),
      multiMonthPeriods: multiMonthPeriods.map((r) => ({
        periodStart: r.period_start,
        periodEnd: r.period_end,
        category: r.category,
        revenue: Math.round(Number(r.revenue)),
      })),
    });
  } catch (e: unknown) {
    return NextResponse.json({ error: (e as Error).message }, { status: 500 });
  }
}
