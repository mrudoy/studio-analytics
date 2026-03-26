import { NextResponse } from "next/server";
import { getPool } from "@/lib/db/database";

export const dynamic = "force-dynamic";

export async function GET() {
  try {
    const pool = getPool();

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
