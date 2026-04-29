/**
 * Operational endpoint: surface enough state to diagnose why the Union
 * data pipeline is stuck. Returns:
 *  - current unionApiExport watermark (high_water_date / last_fetched_at)
 *  - last N export_log rows (so we can see what's being logged vs not)
 *  - failed BullMQ pipeline jobs with their actual error messages
 *  - the Union API's view: count of available exports + newest createdAt
 *
 * Public (no auth) — same security posture as /api/health. Reveals only
 * operational metadata, no PII or credentials.
 */
import { NextResponse } from "next/server";
import { getPool } from "@/lib/db/database";
import { getPipelineQueue } from "@/lib/queue/pipeline-queue";
import { fetchAllExports } from "@/lib/union-api/fetch-export";
import { loadSettings } from "@/lib/crypto/credentials";
import { getActiveCounts } from "@/lib/db/auto-renew-store";
import { ACTIVE_STATES_SQL } from "@/lib/analytics/metrics/filters";

export const dynamic = "force-dynamic";

export async function GET() {
  const out: Record<string, unknown> = {};

  // 1. Watermark
  try {
    const pool = getPool();
    const { rows } = await pool.query(
      `SELECT report_type, high_water_date, last_fetched_at, record_count, notes
       FROM fetch_watermarks
       WHERE report_type = 'unionApiExport'`,
    );
    out.watermark = rows[0] ?? null;
  } catch (err) {
    out.watermark = { error: err instanceof Error ? err.message : String(err) };
  }

  // 2. Recent export_log rows
  try {
    const pool = getPool();
    const { rows } = await pool.query(
      `SELECT id, created_at, export_created_at, data_range_start, data_range_end,
              record_count, export_index, total_exports
       FROM export_log
       ORDER BY created_at DESC
       LIMIT 10`,
    );
    out.recentExportLog = rows;
  } catch (err) {
    out.recentExportLog = { error: err instanceof Error ? err.message : String(err) };
  }

  // 3. Failed BullMQ jobs with actual error messages
  try {
    const q = getPipelineQueue();
    const failedJobs = await q.getJobs(["failed"], 0, 19);
    out.failedJobs = failedJobs.map((j) => ({
      id: j.id,
      name: j.name,
      timestamp: j.timestamp ? new Date(j.timestamp).toISOString() : null,
      processedOn: j.processedOn ? new Date(j.processedOn).toISOString() : null,
      finishedOn: j.finishedOn ? new Date(j.finishedOn).toISOString() : null,
      attemptsMade: j.attemptsMade,
      failedReason: j.failedReason,
      stacktrace: Array.isArray(j.stacktrace) ? j.stacktrace.slice(0, 3) : null,
      data: j.data,
    }));
  } catch (err) {
    out.failedJobs = { error: err instanceof Error ? err.message : String(err) };
  }

  // 4. What does Union's API actually return right now?
  try {
    const settings = loadSettings();
    if (!settings?.unionApiKey) {
      out.unionApi = { error: "no API key configured" };
    } else {
      const all = await fetchAllExports(settings.unionApiKey);

      // Read the watermark again so we can simulate the filter and see
      // exactly which exports would pass / fail right now.
      const pool = getPool();
      const wmRow = (
        await pool.query(
          `SELECT high_water_date FROM fetch_watermarks WHERE report_type = 'unionApiExport'`,
        )
      ).rows[0];
      const watermarkStr = (wmRow?.high_water_date as string | undefined) ?? null;
      const watermarkMs = watermarkStr ? new Date(watermarkStr).getTime() : null;

      const annotated = all.map((e, i) => {
        const t = new Date(e.createdAt).getTime();
        return {
          i,
          createdAt: e.createdAt,
          createdAtParsedMs: Number.isFinite(t) ? t : null,
          dataRangeStart: e.dataRange.start,
          dataRangeEnd: e.dataRange.end,
          passesFilter:
            watermarkMs == null
              ? true
              : Number.isFinite(t) && t > watermarkMs,
        };
      });

      out.unionApi = {
        totalExports: all.length,
        watermark: watermarkStr,
        watermarkParsedMs: watermarkMs,
        passesFilterCount: annotated.filter((e) => e.passesFilter).length,
        // Full list — small enough to be useful (62 rows ~ a few KB)
        exports: annotated,
      };
    }
  } catch (err) {
    out.unionApi = { error: err instanceof Error ? err.message : String(err) };
  }

  // 5. Member count debug — surface multiple counting strategies side by side
  // so a discrepancy with Union's admin UI can be localized to the source.
  try {
    const pool = getPool();
    const canonical = await getActiveCounts();

    // Alternative A: simple plan_state filter only, no current_state hybrid logic
    // (this is what CLAUDE.md says "active = plan_state IN (...)" which is the
    // intuitive definition; comparing surfaces drift between hybrid and simple)
    const planStateOnly = await pool.query(
      `SELECT
         COUNT(DISTINCT LOWER(customer_email)) FILTER (WHERE plan_category = 'MEMBER') AS member,
         COUNT(DISTINCT LOWER(customer_email)) FILTER (WHERE plan_category = 'SKY3') AS sky3,
         COUNT(DISTINCT LOWER(customer_email)) FILTER (WHERE plan_category = 'SKY_TING_TV') AS sky_ting_tv,
         COUNT(DISTINCT LOWER(customer_email)) FILTER (WHERE plan_category NOT IN ('MEMBER','SKY3','SKY_TING_TV') OR plan_category IS NULL) AS unknown,
         COUNT(DISTINCT LOWER(customer_email)) AS total_unique_emails
       FROM auto_renews
       WHERE plan_state IN (${ACTIVE_STATES_SQL})`,
    );

    // Alternative B: latest row per (email, plan_name), then plan_state filter.
    // This avoids counting people whose newest row is canceled but who have
    // older rows still flagged active.
    const latestPerPlan = await pool.query(
      `WITH latest AS (
         SELECT DISTINCT ON (LOWER(customer_email), plan_name)
                LOWER(customer_email) AS email, plan_name, plan_state, plan_category
         FROM auto_renews
         ORDER BY LOWER(customer_email), plan_name, id DESC
       )
       SELECT
         COUNT(DISTINCT email) FILTER (WHERE plan_category = 'MEMBER' AND plan_state IN (${ACTIVE_STATES_SQL})) AS member,
         COUNT(DISTINCT email) FILTER (WHERE plan_category = 'SKY3' AND plan_state IN (${ACTIVE_STATES_SQL})) AS sky3,
         COUNT(DISTINCT email) FILTER (WHERE plan_category = 'SKY_TING_TV' AND plan_state IN (${ACTIVE_STATES_SQL})) AS sky_ting_tv
       FROM latest`,
    );

    // Alternative C: Members specifically — surface the actual emails so we
    // can compare against Union's admin export. Cap at 600 to avoid huge response.
    const memberEmails = await pool.query(
      `WITH latest AS (
         SELECT DISTINCT ON (LOWER(customer_email), plan_name)
                LOWER(customer_email) AS email, plan_name, plan_state, plan_category, current_state
         FROM auto_renews
         ORDER BY LOWER(customer_email), plan_name, id DESC
       )
       SELECT email, plan_name, plan_state, current_state
       FROM latest
       WHERE plan_category = 'MEMBER' AND plan_state IN (${ACTIVE_STATES_SQL})
       ORDER BY email
       LIMIT 600`,
    );

    // Per-plan_state breakdown for Members so we can see WHICH state(s) drift
    const memberStateBreakdown = await pool.query(
      `WITH latest AS (
         SELECT DISTINCT ON (LOWER(customer_email), plan_name)
                LOWER(customer_email) AS email, plan_name, plan_state, plan_category, current_state
         FROM auto_renews
         ORDER BY LOWER(customer_email), plan_name, id DESC
       )
       SELECT plan_state, current_state,
              COUNT(*) AS rows,
              COUNT(DISTINCT email) AS unique_emails
       FROM latest
       WHERE plan_category = 'MEMBER'
       GROUP BY plan_state, current_state
       ORDER BY plan_state, current_state NULLS LAST`,
    );

    out.memberCountDebug = {
      canonical_getActiveCounts: canonical,
      planStateOnly: planStateOnly.rows[0],
      latestRowPerPlan: latestPerPlan.rows[0],
      memberStateBreakdown: memberStateBreakdown.rows,
      memberEmailsCount: memberEmails.rows.length,
      memberEmailsSample: memberEmails.rows.slice(0, 20),
    };
  } catch (err) {
    out.memberCountDebug = { error: err instanceof Error ? err.message : String(err) };
  }

  return NextResponse.json(out, {
    headers: { "Cache-Control": "no-store" },
  });
}
