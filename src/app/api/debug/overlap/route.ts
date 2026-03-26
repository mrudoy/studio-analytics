import { NextRequest, NextResponse } from "next/server";
import { getPool } from "@/lib/db/database";

export const dynamic = "force-dynamic";

export async function GET(request: NextRequest) {
  try {
    const pool = getPool();
    const email = request.nextUrl.searchParams.get("email");

    // If email provided, look up all subs for that person
    if (email) {
      const res = await pool.query(
        `SELECT plan_name, plan_state, plan_category, created_at, canceled_at
         FROM auto_renews
         WHERE LOWER(customer_email) = LOWER($1)
         ORDER BY created_at DESC`,
        [email],
      );
      return NextResponse.json({ email, subscriptions: res.rows });
    }

    const res = await pool.query(`
      SELECT LOWER(customer_email) AS email,
             array_agg(DISTINCT plan_category ORDER BY plan_category) AS categories,
             array_agg(DISTINCT plan_name ORDER BY plan_name) AS plans
      FROM auto_renews
      WHERE plan_state NOT IN ('Canceled', 'Invalid')
        AND plan_category IN ('MEMBER', 'SKY3')
      GROUP BY LOWER(customer_email)
      HAVING COUNT(DISTINCT plan_category) > 1
      ORDER BY email
    `);

    return NextResponse.json({
      count: res.rows.length,
      people: res.rows,
    });
  } catch (e: unknown) {
    return NextResponse.json({ error: (e as Error).message }, { status: 500 });
  }
}
