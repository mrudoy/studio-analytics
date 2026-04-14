import { NextRequest, NextResponse } from "next/server";
import { getPool } from "@/lib/db/database";

export const dynamic = "force-dynamic";

// Canonical "active subscriber" definition — see CLAUDE.md.
// Every query in this file uses this set so overlap and email-lookup agree.
const ACTIVE_STATES = [
  "Valid Now",
  "Paused",
  "In Trial",
  "Invalid",
  "Pending Cancel",
  "Past Due",
];

export async function GET(request: NextRequest) {
  try {
    const pool = getPool();
    const email = request.nextUrl.searchParams.get("email");

    // If email provided, look up all subs + intro week status for that person
    if (email) {
      const [subs, intros, activeMatch] = await Promise.all([
        pool.query(
          `SELECT plan_name, plan_state, plan_category, created_at, canceled_at
           FROM auto_renews
           WHERE LOWER(customer_email) = LOWER($1)
           ORDER BY created_at DESC`,
          [email],
        ),
        pool.query(
          `SELECT pass, state, attended_at, first_name, last_name
           FROM registrations
           WHERE LOWER(email) = LOWER($1)
             AND UPPER(pass) LIKE '%INTRO WEEK%'
           ORDER BY attended_at`,
          [email],
        ),
        pool.query(
          `SELECT plan_name, plan_state
           FROM auto_renews
           WHERE LOWER(customer_email) = LOWER($1)
             AND plan_state = ANY($2)
             AND plan_category IN ('MEMBER', 'SKY3')`,
          [email, ACTIVE_STATES],
        ),
      ]);
      return NextResponse.json({
        email,
        subscriptions: subs.rows,
        introVisits: intros.rows,
        activeInStudioMatch: activeMatch.rows,
      });
    }

    const res = await pool.query(
      `SELECT LOWER(customer_email) AS email,
              array_agg(DISTINCT plan_category ORDER BY plan_category) AS categories,
              array_agg(DISTINCT plan_name ORDER BY plan_name) AS plans
       FROM auto_renews
       WHERE plan_state = ANY($1)
         AND plan_category IN ('MEMBER', 'SKY3')
       GROUP BY LOWER(customer_email)
       HAVING COUNT(DISTINCT plan_category) > 1
       ORDER BY email`,
      [ACTIVE_STATES],
    );

    return NextResponse.json({
      count: res.rows.length,
      people: res.rows,
    });
  } catch (e: unknown) {
    return NextResponse.json({ error: (e as Error).message }, { status: 500 });
  }
}
