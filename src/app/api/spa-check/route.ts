/** Temporary diagnostic route to understand spa customer behavior. */
import { NextResponse } from "next/server";
import { getPool } from "@/lib/db/database";

export async function GET() {
  try {
    const pool = getPool();

    // Spa bookings are at SPA Lounge or Treatment Room locations
    const SPA_FILTER = `location_name IN ('SPA Lounge', 'SPA LOUNGE', 'TREATMENT ROOM')`;

    // 1. Distinct spa event names & pass types
    const eventPasses = await pool.query(`
      SELECT event_name, pass, COUNT(*) AS cnt
      FROM registrations
      WHERE ${SPA_FILTER}
      GROUP BY event_name, pass
      ORDER BY cnt DESC
    `);

    // 2. How many unique spa customers?
    const uniqueRes = await pool.query(`
      SELECT COUNT(DISTINCT email) AS cnt
      FROM registrations
      WHERE ${SPA_FILTER} AND email IS NOT NULL AND email != ''
    `);

    // 3. Visit frequency
    const frequency = await pool.query(`
      SELECT bucket, COUNT(*) AS customers FROM (
        SELECT
          CASE
            WHEN COUNT(*) = 1 THEN '1 visit'
            WHEN COUNT(*) BETWEEN 2 AND 3 THEN '2-3 visits'
            WHEN COUNT(*) BETWEEN 4 AND 6 THEN '4-6 visits'
            WHEN COUNT(*) BETWEEN 7 AND 10 THEN '7-10 visits'
            ELSE '11+ visits'
          END AS bucket
        FROM registrations
        WHERE ${SPA_FILTER} AND email IS NOT NULL AND email != ''
        GROUP BY email
      ) sub
      GROUP BY bucket
      ORDER BY MIN(CASE bucket
        WHEN '1 visit' THEN 1 WHEN '2-3 visits' THEN 2
        WHEN '4-6 visits' THEN 3 WHEN '7-10 visits' THEN 4 ELSE 5 END)
    `);

    // 4. Cross-over: how many spa customers also take classes?
    // Step A: get all spa customer emails
    const spaEmailsRes = await pool.query(`
      SELECT DISTINCT email
      FROM registrations
      WHERE ${SPA_FILTER} AND email IS NOT NULL AND email != ''
    `);
    const spaEmails = spaEmailsRes.rows.map((r: Record<string, unknown>) => r.email as string);

    // Step B: check how many also have non-spa registrations
    let alsoTakeClasses = 0;
    let spaOnly = 0;
    if (spaEmails.length > 0) {
      const classCheckRes = await pool.query(`
        SELECT DISTINCT email
        FROM registrations
        WHERE email = ANY($1)
          AND NOT ${SPA_FILTER}
      `, [spaEmails]);
      const classEmails = new Set(classCheckRes.rows.map((r: Record<string, unknown>) => r.email));
      alsoTakeClasses = classEmails.size;
      spaOnly = spaEmails.length - alsoTakeClasses;
    }

    // 5. Subscriber overlap: how many spa customers are auto-renew subscribers?
    let areSubscribers = 0;
    let subscriberPlans: { plan_name: string; customers: number }[] = [];
    if (spaEmails.length > 0) {
      const subRes = await pool.query(`
        SELECT COUNT(DISTINCT customer_email) AS cnt
        FROM auto_renews
        WHERE LOWER(customer_email) = ANY($1)
      `, [spaEmails.map(e => e.toLowerCase())]);
      areSubscribers = Number(subRes.rows[0].cnt);

      const planRes = await pool.query(`
        SELECT plan_name, COUNT(DISTINCT customer_email) AS customers
        FROM auto_renews
        WHERE LOWER(customer_email) = ANY($1)
        GROUP BY plan_name
        ORDER BY customers DESC
        LIMIT 20
      `, [spaEmails.map(e => e.toLowerCase())]);
      subscriberPlans = planRes.rows.map((r: Record<string, unknown>) => ({
        plan_name: r.plan_name as string,
        customers: Number(r.customers),
      }));
    }

    // 6. Monthly spa visits
    const monthlyVisits = await pool.query(`
      SELECT
        SUBSTR(attended_at, 1, 7) AS month,
        COUNT(*) AS visits,
        COUNT(DISTINCT email) AS unique_visitors
      FROM registrations
      WHERE ${SPA_FILTER} AND attended_at IS NOT NULL
      GROUP BY SUBSTR(attended_at, 1, 7)
      ORDER BY month DESC
      LIMIT 18
    `);

    // 7. Top spa customers
    const topCustomers = await pool.query(`
      SELECT first_name, last_name, COUNT(*) AS visits,
             SUM(COALESCE(revenue, 0)) AS total_revenue,
             MIN(attended_at) AS first_visit,
             MAX(attended_at) AS last_visit
      FROM registrations
      WHERE ${SPA_FILTER} AND email IS NOT NULL AND email != ''
      GROUP BY email, first_name, last_name
      ORDER BY visits DESC
      LIMIT 15
    `);

    return NextResponse.json({
      eventPasses: eventPasses.rows,
      uniqueCustomers: spaEmails.length,
      frequency: frequency.rows,
      crossover: {
        total: spaEmails.length,
        alsoTakeClasses,
        spaOnly,
      },
      subscriberOverlap: {
        total: spaEmails.length,
        areSubscribers,
        notSubscribers: spaEmails.length - areSubscribers,
      },
      subscriberPlans,
      monthlyVisits: monthlyVisits.rows,
      topCustomers: topCustomers.rows,
    });
  } catch (err) {
    return NextResponse.json({
      error: err instanceof Error ? err.message : String(err),
    }, { status: 500 });
  }
}
