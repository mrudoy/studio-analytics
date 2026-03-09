/**
 * TEMPORARY endpoint — Sky3 Churn Profile + Historical Subscriber Counts
 * DELETE after reviewing data
 */
import { NextResponse } from "next/server";
import { getSky3ChurnProfile } from "@/lib/db/registration-store";
import { getPool } from "@/lib/db/database";

export const dynamic = "force-dynamic";

export async function GET(req: Request) {
  const url = new URL(req.url);
  const mode = url.searchParams.get("mode");

  if (mode === "quarterly") {
    return getQuarterlySnapshot();
  }

  if (mode === "revenue-debug") {
    return revenueDebug();
  }

  try {
    const data = await getSky3ChurnProfile();
    const summary = {
      totalCanceledSky3Last6Mo: data.total,
      alreadyActiveBeforeSubscribing: `${data.alreadyActivePercent}% (${data.alreadyActive} of ${data.total})`,
      brandNew: `${data.brandNewPercent}% (${data.byType["brand-new"] || 0} of ${data.total})`,
      priorCustomerBreakdown: data.byType,
      medianTenureMonths: data.medianTenure,
      avgTenureMonths: data.avgTenure,
    };
    return NextResponse.json({ summary, rows: data.rows });
  } catch (err) {
    console.error("Sky3 churn analysis error:", err);
    return NextResponse.json({ error: String(err) }, { status: 500 });
  }
}

async function revenueDebug() {
  const pool = getPool();
  const results: Record<string, unknown> = {};

  try {
    const marchRaw = await pool.query(
      `SELECT category, revenue, net_revenue, period_start, period_end
       FROM revenue_categories
       WHERE SUBSTR(period_start, 1, 7) = '2026-03'
       ORDER BY revenue DESC`
    );
    results.marchRawCategories = marchRaw.rows;
  } catch (e) { results.marchRawError = String(e); }

  try {
    const spaLike = await pool.query(
      `SELECT category, SUM(revenue) as total
       FROM revenue_categories
       WHERE category ILIKE '%sauna%' OR category ILIKE '%spa%'
         OR category ILIKE '%plunge%' OR category ILIKE '%contrast%'
         OR category ILIKE '%infrared%' OR category ILIKE '%cupping%'
         OR category ILIKE '%treatment%' OR category ILIKE '%cold%'
         OR category ILIKE '%lounge%'
       GROUP BY category
       ORDER BY total DESC`
    );
    results.allSpaLikeCategories = spaLike.rows;
  } catch (e) { results.spaLikeError = String(e); }

  try {
    const febSpa = await pool.query(
      `SELECT category, revenue, net_revenue
       FROM revenue_categories
       WHERE SUBSTR(period_start, 1, 7) = '2026-02'
         AND (category ILIKE '%sauna%' OR category ILIKE '%spa%'
           OR category ILIKE '%plunge%' OR category ILIKE '%contrast%'
           OR category ILIKE '%infrared%' OR category ILIKE '%cupping%'
           OR category ILIKE '%treatment%' OR category ILIKE '%cold%'
           OR category ILIKE '%lounge%')
       ORDER BY revenue DESC`
    );
    results.febSpaCategoriesDetail = febSpa.rows;
  } catch (e) { results.febSpaError = String(e); }

  try {
    const pipeline = await pool.query(
      `SELECT * FROM pipeline_runs ORDER BY ran_at DESC LIMIT 3`
    );
    results.recentPipelineRuns = pipeline.rows;
  } catch (e) { results.pipelineError = String(e); }

  // Also check: what does getSpaMTDRevenue see?
  try {
    const spaMtd = await pool.query(
      `SELECT category, revenue, net_revenue, period_start, period_end
       FROM revenue_categories
       WHERE category IN ('Infrared Sauna Suite', 'Contrast Suite', 'Spa Lounge', 'Treatment Room', 'CUPPING')
         AND SUBSTR(period_start, 1, 7) = '2026-03'`
    );
    results.exactSpaCategoryMatch = spaMtd.rows;
  } catch (e) { results.exactSpaError = String(e); }

  // Check what categories exist for current month
  try {
    const allCats = await pool.query(
      `SELECT DISTINCT category FROM revenue_categories WHERE SUBSTR(period_start, 1, 7) = '2026-03' ORDER BY category`
    );
    results.marchDistinctCategories = allCats.rows.map((r: { category: string }) => r.category);
  } catch (e) { results.marchCatsError = String(e); }

  return NextResponse.json(results);
}

async function getQuarterlySnapshot() {
  const pool = getPool();

  // Snapshot dates: end of each quarter
  const dates = [
    { label: "Q1 2025", date: "2025-03-31" },
    { label: "Q2 2025", date: "2025-06-30" },
    { label: "Q3 2025", date: "2025-09-30" },
    { label: "Q4 2025", date: "2025-12-31" },
    { label: "Q1 2026 (now)", date: "CURRENT_DATE" },
  ];

  const results = [];

  for (const { label, date } of dates) {
    const dateExpr = date === "CURRENT_DATE" ? "CURRENT_DATE" : `'${date}'::date`;

    const { rows } = await pool.query(`
      WITH deduped AS (
        SELECT LOWER(customer_email) AS email,
          plan_name,
          plan_state,
          created_at,
          canceled_at,
          monthly_rate,
          ROW_NUMBER() OVER (
            PARTITION BY LOWER(customer_email),
            CASE
              WHEN UPPER(plan_name) LIKE '%SKY3%' OR UPPER(plan_name) LIKE '%SKY5%'
                   OR UPPER(plan_name) LIKE '%SKYHIGH%' OR UPPER(plan_name) LIKE '%5 PACK%'
                   OR UPPER(plan_name) LIKE '%5-PACK%'
                   OR UPPER(plan_name) LIKE '%WELCOME SKY3%' THEN 'sky3'
              WHEN UPPER(plan_name) LIKE '%SKY TING TV%' OR UPPER(plan_name) LIKE '%SKYTING TV%' THEN 'tv'
              WHEN UPPER(plan_name) LIKE '%UNLIMITED%' OR UPPER(plan_name) LIKE '%MEMBER%'
                   OR UPPER(plan_name) LIKE '%ALL ACCESS%' OR UPPER(plan_name) LIKE '%TING FAM%' THEN 'member'
              ELSE 'other'
            END
            ORDER BY monthly_rate DESC NULLS LAST, created_at DESC
          ) AS rn,
          CASE
            WHEN UPPER(plan_name) LIKE '%SKY3%' OR UPPER(plan_name) LIKE '%SKY5%'
                 OR UPPER(plan_name) LIKE '%SKYHIGH%' OR UPPER(plan_name) LIKE '%5 PACK%'
                 OR UPPER(plan_name) LIKE '%5-PACK%'
                 OR UPPER(plan_name) LIKE '%WELCOME SKY3%' THEN 'sky3'
            WHEN UPPER(plan_name) LIKE '%SKY TING TV%' OR UPPER(plan_name) LIKE '%SKYTING TV%' THEN 'tv'
            WHEN UPPER(plan_name) LIKE '%UNLIMITED%' OR UPPER(plan_name) LIKE '%MEMBER%'
                 OR UPPER(plan_name) LIKE '%ALL ACCESS%' OR UPPER(plan_name) LIKE '%TING FAM%' THEN 'member'
            ELSE 'other'
          END AS category
        FROM auto_renews
        WHERE customer_email IS NOT NULL AND customer_email != ''
          AND created_at IS NOT NULL AND created_at ~ '^\\d{4}-'
          AND LEFT(created_at, 10)::date <= ${dateExpr}
          AND (
            plan_state NOT IN ('Canceled', 'Invalid')
            OR (
              canceled_at IS NOT NULL AND canceled_at ~ '^\\d{4}-'
              AND LEFT(canceled_at, 10)::date > ${dateExpr}
            )
          )
      )
      SELECT category, COUNT(*) AS active_count
      FROM deduped
      WHERE rn = 1
      GROUP BY category
      ORDER BY category
    `);

    const counts: Record<string, number> = {};
    for (const r of rows) {
      counts[r.category] = parseInt(r.active_count);
    }

    results.push({
      label,
      date: date === "CURRENT_DATE" ? "now" : date,
      members: counts["member"] || 0,
      sky3: counts["sky3"] || 0,
      tv: counts["tv"] || 0,
      other: counts["other"] || 0,
      total: (counts["member"] || 0) + (counts["sky3"] || 0) + (counts["tv"] || 0),
    });
  }

  return NextResponse.json({ snapshots: results });
}
