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
