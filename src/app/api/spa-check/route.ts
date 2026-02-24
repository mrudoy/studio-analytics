/** Temporary diagnostic route to understand spa customer behavior. */
import { NextResponse } from "next/server";
import { getPool } from "@/lib/db/database";

export async function GET() {
  const pool = getPool();

  // Spa location filter: bookings at SPA Lounge or Treatment Room
  const SPA_LOC = `(location_name ~* 'spa lounge|treatment room')`;

  // 1. Distinct spa event names & pass types
  const eventPasses = await pool.query(`
    SELECT DISTINCT event_name, pass, COUNT(*) AS cnt
    FROM registrations
    WHERE ${SPA_LOC}
    GROUP BY event_name, pass
    ORDER BY cnt DESC
  `);

  // 2. How many unique spa customers?
  const uniqueCustomers = await pool.query(`
    SELECT COUNT(DISTINCT email) AS unique_customers
    FROM registrations
    WHERE ${SPA_LOC} AND email IS NOT NULL AND email != ''
  `);

  // 3. Visit frequency: how many times does each spa customer visit?
  const frequency = await pool.query(`
    SELECT bucket, COUNT(*) AS customers FROM (
      SELECT email,
        CASE
          WHEN COUNT(*) = 1 THEN '1 visit'
          WHEN COUNT(*) BETWEEN 2 AND 3 THEN '2-3 visits'
          WHEN COUNT(*) BETWEEN 4 AND 6 THEN '4-6 visits'
          WHEN COUNT(*) BETWEEN 7 AND 10 THEN '7-10 visits'
          ELSE '11+ visits'
        END AS bucket
      FROM registrations
      WHERE ${SPA_LOC} AND email IS NOT NULL AND email != ''
      GROUP BY email
    ) sub
    GROUP BY bucket
    ORDER BY
      CASE bucket
        WHEN '1 visit' THEN 1
        WHEN '2-3 visits' THEN 2
        WHEN '4-6 visits' THEN 3
        WHEN '7-10 visits' THEN 4
        ELSE 5
      END
  `);

  // 4. Cross-over: do spa customers also take classes?
  // Find spa customer emails, then check if they have non-spa registrations
  const crossover = await pool.query(`
    WITH spa_customers AS (
      SELECT DISTINCT email
      FROM registrations
      WHERE ${SPA_LOC} AND email IS NOT NULL AND email != ''
    )
    SELECT
      COUNT(*) AS total_spa_customers,
      COUNT(CASE WHEN class_count > 0 THEN 1 END) AS also_take_classes,
      COUNT(CASE WHEN class_count = 0 THEN 1 END) AS spa_only
    FROM (
      SELECT sc.email, COUNT(r.email) AS class_count
      FROM spa_customers sc
      LEFT JOIN registrations r
        ON r.email = sc.email
        AND NOT ${SPA_LOC.replace(/location_name/g, 'r.location_name')}
        AND (r.location_name IS NULL OR r.location_name NOT IN ('SPA Lounge', 'SPA LOUNGE', 'TREATMENT ROOM'))
      GROUP BY sc.email
    ) sub
  `);

  // 5. Spa customers that ARE subscribers (members, sky3 etc.)
  // Join spa emails with auto_renews table
  const subscriberOverlap = await pool.query(`
    WITH spa_emails AS (
      SELECT DISTINCT email
      FROM registrations
      WHERE ${SPA_LOC} AND email IS NOT NULL AND email != ''
    )
    SELECT
      COUNT(DISTINCT se.email) AS total_spa_customers,
      COUNT(DISTINCT ar.email) AS are_subscribers,
      COUNT(DISTINCT CASE WHEN ar.email IS NULL THEN se.email END) AS not_subscribers
    FROM spa_emails se
    LEFT JOIN auto_renews ar ON LOWER(ar.email) = LOWER(se.email)
  `);

  // 6. What plans do spa-subscriber customers have?
  const subscriberPlans = await pool.query(`
    WITH spa_emails AS (
      SELECT DISTINCT email
      FROM registrations
      WHERE ${SPA_LOC} AND email IS NOT NULL AND email != ''
    )
    SELECT ar.plan_name, COUNT(DISTINCT ar.email) AS customers
    FROM spa_emails se
    JOIN auto_renews ar ON LOWER(ar.email) = LOWER(se.email)
    GROUP BY ar.plan_name
    ORDER BY customers DESC
    LIMIT 20
  `);

  // 7. Monthly spa visit counts (bookings per month)
  const monthlyVisits = await pool.query(`
    SELECT
      SUBSTR(attended_at, 1, 7) AS month,
      COUNT(*) AS visits,
      COUNT(DISTINCT email) AS unique_visitors
    FROM registrations
    WHERE ${SPA_LOC}
      AND attended_at IS NOT NULL
    GROUP BY SUBSTR(attended_at, 1, 7)
    ORDER BY month DESC
    LIMIT 12
  `);

  // 8. Top spa customers by visit count
  const topCustomers = await pool.query(`
    SELECT email, first_name, last_name, COUNT(*) AS visits,
           SUM(COALESCE(revenue, 0)) AS total_revenue,
           MIN(attended_at) AS first_visit,
           MAX(attended_at) AS last_visit
    FROM registrations
    WHERE ${SPA_LOC} AND email IS NOT NULL AND email != ''
    GROUP BY email, first_name, last_name
    ORDER BY visits DESC
    LIMIT 15
  `);

  return NextResponse.json({
    eventPasses: eventPasses.rows,
    uniqueCustomers: Number(uniqueCustomers.rows[0].unique_customers),
    frequency: frequency.rows,
    crossover: crossover.rows[0],
    subscriberOverlap: subscriberOverlap.rows[0],
    subscriberPlans: subscriberPlans.rows,
    monthlyVisits: monthlyVisits.rows,
    topCustomers: topCustomers.rows,
  });
}
