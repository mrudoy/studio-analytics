/** Temporary diagnostic route to understand spa customer behavior. */
import { NextResponse } from "next/server";
import { getPool } from "@/lib/db/database";

export async function GET() {
  const pool = getPool();
  const results: Record<string, unknown> = {};

  // Spa bookings are at SPA Lounge or Treatment Room locations
  const SPA_FILTER = `location_name IN ('SPA Lounge', 'SPA LOUNGE', 'TREATMENT ROOM')`;

  // 1. Distinct spa event names & pass types
  try {
    const eventPasses = await pool.query(`
      SELECT event_name, pass, COUNT(*) AS cnt
      FROM registrations
      WHERE ${SPA_FILTER}
      GROUP BY event_name, pass
      ORDER BY cnt DESC
    `);
    results.eventPasses = eventPasses.rows;
  } catch (err) {
    results.eventPassesError = err instanceof Error ? err.message : String(err);
  }

  // 2. Unique spa customer emails
  let spaEmails: string[] = [];
  try {
    const spaEmailsRes = await pool.query(`
      SELECT DISTINCT email
      FROM registrations
      WHERE ${SPA_FILTER} AND email IS NOT NULL AND email != ''
    `);
    spaEmails = spaEmailsRes.rows.map((r: Record<string, unknown>) => r.email as string);
    results.uniqueCustomers = spaEmails.length;
  } catch (err) {
    results.uniqueCustomersError = err instanceof Error ? err.message : String(err);
  }

  // 3. Visit frequency
  try {
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
    results.frequency = frequency.rows;
  } catch (err) {
    results.frequencyError = err instanceof Error ? err.message : String(err);
  }

  // 4. Cross-over: how many spa customers also take classes?
  try {
    if (spaEmails.length > 0) {
      const classCheckRes = await pool.query(`
        SELECT DISTINCT email
        FROM registrations
        WHERE email = ANY($1)
          AND NOT (${SPA_FILTER})
      `, [spaEmails]);
      const classEmails = new Set(classCheckRes.rows.map((r: Record<string, unknown>) => r.email));
      results.crossover = {
        total: spaEmails.length,
        alsoTakeClasses: classEmails.size,
        spaOnly: spaEmails.length - classEmails.size,
      };
    }
  } catch (err) {
    results.crossoverError = err instanceof Error ? err.message : String(err);
  }

  // 5. Subscriber overlap
  try {
    if (spaEmails.length > 0) {
      const subRes = await pool.query(`
        SELECT COUNT(DISTINCT customer_email) AS cnt
        FROM auto_renews
        WHERE LOWER(customer_email) = ANY($1)
      `, [spaEmails.map(e => e.toLowerCase())]);
      const areSubscribers = Number(subRes.rows[0].cnt);

      const planRes = await pool.query(`
        SELECT plan_name, COUNT(DISTINCT customer_email) AS customers
        FROM auto_renews
        WHERE LOWER(customer_email) = ANY($1)
        GROUP BY plan_name
        ORDER BY customers DESC
        LIMIT 20
      `, [spaEmails.map(e => e.toLowerCase())]);
      const subscriberPlans = planRes.rows.map((r: Record<string, unknown>) => ({
        plan_name: r.plan_name as string,
        customers: Number(r.customers),
      }));

      results.subscriberOverlap = {
        total: spaEmails.length,
        areSubscribers,
        notSubscribers: spaEmails.length - areSubscribers,
      };
      results.subscriberPlans = subscriberPlans;
    }
  } catch (err) {
    results.subscriberOverlapError = err instanceof Error ? err.message : String(err);
  }

  // 6. Monthly spa visits
  try {
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
    results.monthlyVisits = monthlyVisits.rows;
  } catch (err) {
    results.monthlyVisitsError = err instanceof Error ? err.message : String(err);
  }

  // 7. Top spa customers
  try {
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
    results.topCustomers = topCustomers.rows;
  } catch (err) {
    results.topCustomersError = err instanceof Error ? err.message : String(err);
  }

  // 8. What pass type are spa customers using? (member? drop-in? class pack?)
  try {
    if (spaEmails.length > 0) {
      const passTypeRes = await pool.query(`
        SELECT
          CASE
            WHEN subscription IS NOT NULL AND subscription != '' THEN 'subscriber'
            ELSE 'non-subscriber'
          END AS sub_status,
          pass,
          COUNT(*) AS visits,
          COUNT(DISTINCT email) AS customers
        FROM registrations
        WHERE ${SPA_FILTER} AND email IS NOT NULL AND email != ''
        GROUP BY sub_status, pass
        ORDER BY visits DESC
      `);
      results.passTypeBreakdown = passTypeRes.rows;
    }
  } catch (err) {
    results.passTypeBreakdownError = err instanceof Error ? err.message : String(err);
  }

  return NextResponse.json(results);
}
