import { NextResponse } from "next/server";
import { getPool } from "@/lib/db/database";

export const dynamic = "force-dynamic";

export async function GET() {
  const pool = getPool();

  // 1. Basic stats
  const { rows: [stats] } = await pool.query(`
    SELECT COUNT(*) as total, MIN(attended_at)::text as earliest, MAX(attended_at)::text as latest,
           COUNT(DISTINCT email) as unique_emails
    FROM registrations WHERE state IN ('redeemed','confirmed') AND attended_at IS NOT NULL
  `);

  // 2. For active auto-renew MEMBERS (not sky3/tv), compute their weekly attendance
  // Join registrations with auto_renews to identify current subscribers
  // Then compare: churned members' attendance pattern vs active members

  // 2a. Attendance velocity for CHURNED members in their last 8 weeks before cancel
  const { rows: churnedVelocity } = await pool.query(`
    WITH churned AS (
      SELECT customer_email as email, canceled_at::date as cancel_date
      FROM auto_renews
      WHERE plan_state = 'Canceled'
        AND canceled_at IS NOT NULL
        AND plan_name NOT ILIKE '%sky3%'
        AND plan_name NOT ILIKE '%tv%'
        AND plan_name NOT ILIKE '%annual%'
    ),
    churned_visits AS (
      SELECT c.email, c.cancel_date,
        (c.cancel_date - attended_at::date) as days_before_cancel,
        CASE
          WHEN (c.cancel_date - attended_at::date) BETWEEN 0 AND 6 THEN 'wk1_before'
          WHEN (c.cancel_date - attended_at::date) BETWEEN 7 AND 13 THEN 'wk2_before'
          WHEN (c.cancel_date - attended_at::date) BETWEEN 14 AND 20 THEN 'wk3_before'
          WHEN (c.cancel_date - attended_at::date) BETWEEN 21 AND 27 THEN 'wk4_before'
          WHEN (c.cancel_date - attended_at::date) BETWEEN 28 AND 34 THEN 'wk5_before'
          WHEN (c.cancel_date - attended_at::date) BETWEEN 35 AND 41 THEN 'wk6_before'
          WHEN (c.cancel_date - attended_at::date) BETWEEN 42 AND 48 THEN 'wk7_before'
          WHEN (c.cancel_date - attended_at::date) BETWEEN 49 AND 55 THEN 'wk8_before'
        END as week_bucket
      FROM churned c
      JOIN registrations r ON r.email = c.email
        AND r.state IN ('redeemed','confirmed')
        AND r.attended_at IS NOT NULL
        AND r.attended_at::date BETWEEN (c.cancel_date - 56) AND c.cancel_date
    )
    SELECT week_bucket,
      COUNT(DISTINCT email) as members_with_visits,
      COUNT(*) as total_visits,
      ROUND(COUNT(*)::numeric / NULLIF(COUNT(DISTINCT email),0), 2) as avg_visits_per_member
    FROM churned_visits
    WHERE week_bucket IS NOT NULL
    GROUP BY week_bucket
    ORDER BY week_bucket DESC
  `);

  // 2b. For ACTIVE members: avg weekly visits over the last 8 weeks
  const { rows: activeVelocity } = await pool.query(`
    WITH active AS (
      SELECT customer_email as email
      FROM auto_renews
      WHERE plan_state NOT IN ('Canceled','Invalid')
        AND plan_name NOT ILIKE '%sky3%'
        AND plan_name NOT ILIKE '%tv%'
        AND plan_name NOT ILIKE '%annual%'
    ),
    active_visits AS (
      SELECT a.email,
        CASE
          WHEN (CURRENT_DATE - attended_at::date) BETWEEN 0 AND 6 THEN 'wk1_ago'
          WHEN (CURRENT_DATE - attended_at::date) BETWEEN 7 AND 13 THEN 'wk2_ago'
          WHEN (CURRENT_DATE - attended_at::date) BETWEEN 14 AND 20 THEN 'wk3_ago'
          WHEN (CURRENT_DATE - attended_at::date) BETWEEN 21 AND 27 THEN 'wk4_ago'
          WHEN (CURRENT_DATE - attended_at::date) BETWEEN 28 AND 34 THEN 'wk5_ago'
          WHEN (CURRENT_DATE - attended_at::date) BETWEEN 35 AND 41 THEN 'wk6_ago'
          WHEN (CURRENT_DATE - attended_at::date) BETWEEN 42 AND 48 THEN 'wk7_ago'
          WHEN (CURRENT_DATE - attended_at::date) BETWEEN 49 AND 55 THEN 'wk8_ago'
        END as week_bucket
      FROM active a
      JOIN registrations r ON r.email = a.email
        AND r.state IN ('redeemed','confirmed')
        AND r.attended_at IS NOT NULL
        AND r.attended_at::date >= (CURRENT_DATE - 56)
    )
    SELECT week_bucket,
      COUNT(DISTINCT email) as members_with_visits,
      COUNT(*) as total_visits,
      ROUND(COUNT(*)::numeric / NULLIF(COUNT(DISTINCT email),0), 2) as avg_visits_per_member
    FROM active_visits
    WHERE week_bucket IS NOT NULL
    GROUP BY week_bucket
    ORDER BY week_bucket DESC
  `);

  // 3. Distribution of attendance drop severity for churned members
  // Compare their visits in weeks 5-8 before cancel (early) vs weeks 1-4 before cancel (late)
  const { rows: dropSeverity } = await pool.query(`
    WITH churned AS (
      SELECT customer_email as email, canceled_at::date as cancel_date
      FROM auto_renews
      WHERE plan_state = 'Canceled'
        AND canceled_at IS NOT NULL
        AND plan_name NOT ILIKE '%sky3%'
        AND plan_name NOT ILIKE '%tv%'
        AND plan_name NOT ILIKE '%annual%'
    ),
    early_visits AS (
      SELECT c.email, COUNT(r.*) as visits
      FROM churned c
      LEFT JOIN registrations r ON r.email = c.email
        AND r.state IN ('redeemed','confirmed')
        AND r.attended_at IS NOT NULL
        AND r.attended_at::date BETWEEN (c.cancel_date - 55) AND (c.cancel_date - 28)
      GROUP BY c.email
    ),
    late_visits AS (
      SELECT c.email, COUNT(r.*) as visits
      FROM churned c
      LEFT JOIN registrations r ON r.email = c.email
        AND r.state IN ('redeemed','confirmed')
        AND r.attended_at IS NOT NULL
        AND r.attended_at::date BETWEEN (c.cancel_date - 27) AND c.cancel_date
      GROUP BY c.email
    ),
    combined AS (
      SELECT e.email, e.visits as early, l.visits as late,
        CASE
          WHEN e.visits = 0 AND l.visits = 0 THEN 'no_visits'
          WHEN e.visits = 0 THEN 'only_late'
          WHEN l.visits = 0 THEN 'dropped_to_zero'
          WHEN l.visits::float / e.visits <= 0.25 THEN 'dropped_75pct_plus'
          WHEN l.visits::float / e.visits <= 0.5 THEN 'dropped_50_75pct'
          WHEN l.visits::float / e.visits <= 0.75 THEN 'dropped_25_50pct'
          ELSE 'stable_or_increased'
        END as pattern
      FROM early_visits e
      JOIN late_visits l ON l.email = e.email
    )
    SELECT pattern, COUNT(*) as members,
      ROUND(AVG(early),1) as avg_early_visits,
      ROUND(AVG(late),1) as avg_late_visits
    FROM combined
    GROUP BY pattern
    ORDER BY members DESC
  `);

  // 4. Current members showing attendance drops (the actual alert list)
  const { rows: currentDrops } = await pool.query(`
    WITH active AS (
      SELECT customer_email as email, customer_name as name, plan_name, created_at
      FROM auto_renews
      WHERE plan_state NOT IN ('Canceled','Invalid')
        AND plan_name NOT ILIKE '%sky3%'
        AND plan_name NOT ILIKE '%tv%'
        AND plan_name NOT ILIKE '%annual%'
    ),
    recent AS (
      SELECT a.email, a.name, a.plan_name, a.created_at,
        COUNT(CASE WHEN r.attended_at::date >= (CURRENT_DATE - 13) AND r.attended_at::date < (CURRENT_DATE) THEN 1 END) as visits_last_2wk,
        COUNT(CASE WHEN r.attended_at::date >= (CURRENT_DATE - 27) AND r.attended_at::date < (CURRENT_DATE - 14) THEN 1 END) as visits_prior_2wk,
        COUNT(CASE WHEN r.attended_at::date >= (CURRENT_DATE - 55) THEN 1 END) as visits_8wk
      FROM active a
      LEFT JOIN registrations r ON r.email = a.email
        AND r.state IN ('redeemed','confirmed')
        AND r.attended_at IS NOT NULL
        AND r.attended_at::date >= (CURRENT_DATE - 55)
      GROUP BY a.email, a.name, a.plan_name, a.created_at
    )
    SELECT email, name, plan_name, created_at::text,
      visits_last_2wk, visits_prior_2wk, visits_8wk,
      ROUND(visits_8wk::numeric / 8, 1) as avg_weekly
    FROM recent
    WHERE visits_prior_2wk >= 3 AND visits_last_2wk <= 1
    ORDER BY (visits_prior_2wk - visits_last_2wk) DESC
    LIMIT 50
  `);

  return NextResponse.json({
    stats,
    churnedVelocity,
    activeVelocity,
    dropSeverity,
    currentDrops,
  });
}
