import { NextResponse } from "next/server";
import { getPool } from "@/lib/db/database";
import { parseDate } from "@/lib/analytics/date-utils";

/**
 * Debug endpoint to inspect raw auto_renews data and date parsing.
 * GET /api/debug?table=auto_renews&limit=10
 * Only available in development.
 */
export async function GET(req: Request) {
  if (process.env.NODE_ENV === "production") {
    return NextResponse.json({ error: "Not available in production" }, { status: 403 });
  }
  const url = new URL(req.url);
  const table = url.searchParams.get("table") || "auto_renews";
  const limit = Math.min(parseInt(url.searchParams.get("limit") || "10"), 100);

  try {
    const pool = getPool();

    if (table === "auto_renews") {
      // Get sample rows to inspect date formats
      const { rows: sample } = await pool.query(
        `SELECT plan_name, plan_state, plan_price, created_at, canceled_at
         FROM auto_renews LIMIT $1`,
        [limit]
      );

      // Get counts
      const { rows: counts } = await pool.query(`
        SELECT
          COUNT(*) as total,
          COUNT(NULLIF(created_at, '')) as has_created,
          COUNT(NULLIF(canceled_at, '')) as has_canceled,
          COUNT(CASE WHEN plan_state = 'Valid Now' THEN 1 END) as valid_now,
          COUNT(CASE WHEN plan_state = 'Pending Cancel' THEN 1 END) as pending_cancel,
          COUNT(CASE WHEN plan_state = 'Paused' THEN 1 END) as paused,
          COUNT(CASE WHEN plan_state = 'Past Due' THEN 1 END) as past_due,
          COUNT(CASE WHEN plan_state = 'In Trial' THEN 1 END) as in_trial,
          COUNT(CASE WHEN plan_state = 'Canceled' THEN 1 END) as canceled
        FROM auto_renews
      `);

      // Test date parsing on sample rows
      const parsedSamples = sample.map((r: Record<string, unknown>) => ({
        plan_name: r.plan_name,
        plan_state: r.plan_state,
        raw_created_at: r.created_at,
        raw_canceled_at: r.canceled_at,
        parsed_created: r.created_at ? (() => {
          const d = parseDate(r.created_at as string);
          return d ? d.toISOString() : "PARSE_FAILED";
        })() : null,
        parsed_canceled: r.canceled_at ? (() => {
          const d = parseDate(r.canceled_at as string);
          return d ? d.toISOString() : "PARSE_FAILED";
        })() : null,
      }));

      // Get distinct date formats (first 5 characters to see pattern)
      const { rows: datePatterns } = await pool.query(`
        SELECT
          LEFT(created_at, 15) as created_pattern,
          COUNT(*) as count
        FROM auto_renews
        WHERE created_at IS NOT NULL AND created_at != ''
        GROUP BY LEFT(created_at, 15)
        ORDER BY count DESC
        LIMIT 20
      `);

      return NextResponse.json({
        table: "auto_renews",
        counts: counts[0],
        datePatterns,
        samples: parsedSamples,
      });
    }

    return NextResponse.json({ error: `Unknown table: ${table}` }, { status: 400 });
  } catch (err) {
    return NextResponse.json(
      { error: err instanceof Error ? err.message : "Unknown error" },
      { status: 500 }
    );
  }
}
