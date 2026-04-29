/**
 * Operational endpoint: compare the dashboard's "Weekly New Customers" convert
 * counts to the actual count of new Sky3 + Member subscriptions over the same
 * period, and break down where any gap comes from.
 *
 * The chart counts converts as: emails in the new-customer cohort (drop-in
 * first visit, is_first_visit=TRUE) whose MIN(created_at) for an in-studio
 * plan falls within 0-41 days of that first visit. Anything else (online-only
 * signups, comeback customers whose first visit predated the cohort, first
 * visits on non-drop-in passes) is excluded by design.
 *
 * Public (no auth) — same security posture as /api/diagnose-pipeline.
 * Operational metadata only, no PII.
 */
import { NextResponse } from "next/server";
import { getPool } from "@/lib/db/database";
import {
  IN_STUDIO_PLAN_FILTER,
  dropInPassFilter,
  getNewCustomerCohorts,
} from "@/lib/db/registration-store";

export const dynamic = "force-dynamic";

interface SubsByWeekRow {
  wk: string;
  totalSubStarts: number;
  firstEverSubs: number;
  matchedToRecentCohort: number;
  matchedToOlderCohort: number;
  noPriorVisit: number;
  notInDropInPool: number;
}

interface CohortMatchRow {
  cohortStart: string;
  matchedFirstEverSubs: number;
}

export async function GET() {
  const out: Record<string, unknown> = {};

  try {
    const pool = getPool();
    const passFilter = dropInPassFilter("r.pass");

    // 1. Per-subscription-week breakdown.
    const subsByWeekQuery = `
      WITH new_subs AS (
        SELECT DATE_TRUNC('week', created_at)::date AS wk,
               LOWER(customer_email) AS email,
               created_at AS sub_date,
               MIN(created_at) OVER (PARTITION BY LOWER(customer_email)) AS first_ever_sub
        FROM auto_renews
        WHERE customer_email IS NOT NULL
          AND created_at IS NOT NULL
          ${IN_STUDIO_PLAN_FILTER}
      ),
      first_visit AS (
        SELECT DISTINCT ON (LOWER(r.email))
               LOWER(r.email) AS email,
               r.attended_at AS first_visit_date,
               (UPPER(r.pass) LIKE '%DROP-IN%' OR UPPER(r.pass) LIKE '%DROP IN%' OR UPPER(r.pass) LIKE '%DROPIN%'
                OR UPPER(r.pass) LIKE '%DROPLET%'
                OR UPPER(r.pass) LIKE '%INTRO WEEK%'
                OR UPPER(r.pass) LIKE '%TRIAL%'
                OR UPPER(r.pass) LIKE '%FIRST%'
                OR UPPER(r.pass) LIKE '%SINGLE CLASS%'
                OR UPPER(r.pass) LIKE '%WELLHUB%'
                OR UPPER(r.pass) LIKE '%GUEST%'
                OR UPPER(r.pass) LIKE '%COMMUNITY DAY%'
                OR UPPER(r.pass) LIKE '%POKER CHIP%'
                OR UPPER(r.pass) LIKE '%ON RUNNING%') AS has_dropin_first_visit
        FROM registrations r
        WHERE r.is_first_visit = TRUE
          AND r.email IS NOT NULL
          AND r.attended_at IS NOT NULL
        ORDER BY LOWER(r.email), r.attended_at
      )
      SELECT ns.wk::text AS wk,
             COUNT(*)::int AS "totalSubStarts",
             COUNT(*) FILTER (WHERE ns.sub_date = ns.first_ever_sub)::int AS "firstEverSubs",
             COUNT(*) FILTER (
               WHERE ns.sub_date = ns.first_ever_sub
                 AND fv.first_visit_date IS NOT NULL
                 AND fv.has_dropin_first_visit
                 AND ns.sub_date - fv.first_visit_date BETWEEN 0 AND 41
             )::int AS "matchedToRecentCohort",
             COUNT(*) FILTER (
               WHERE ns.sub_date = ns.first_ever_sub
                 AND fv.first_visit_date IS NOT NULL
                 AND fv.has_dropin_first_visit
                 AND ns.sub_date - fv.first_visit_date > 41
             )::int AS "matchedToOlderCohort",
             COUNT(*) FILTER (
               WHERE ns.sub_date = ns.first_ever_sub
                 AND fv.first_visit_date IS NULL
             )::int AS "noPriorVisit",
             COUNT(*) FILTER (
               WHERE ns.sub_date = ns.first_ever_sub
                 AND fv.first_visit_date IS NOT NULL
                 AND NOT fv.has_dropin_first_visit
             )::int AS "notInDropInPool"
      FROM new_subs ns
      LEFT JOIN first_visit fv ON fv.email = ns.email
      WHERE ns.wk >= DATE_TRUNC('week', CURRENT_DATE - INTERVAL '14 weeks')::date
      GROUP BY ns.wk
      ORDER BY ns.wk
    `;
    const subsByWeek = await pool.query<SubsByWeekRow>(subsByWeekQuery);

    // 2. Per-cohort-week count of first-ever-subs whose first visit landed in
    //    that cohort week and whose sub came within 41 days. This is what
    //    SHOULD equal the chart's total6Week per cohort.
    const cohortMatchQuery = `
      WITH first_ever_sub AS (
        SELECT LOWER(customer_email) AS email,
               MIN(created_at) AS first_sub_date
        FROM auto_renews
        WHERE customer_email IS NOT NULL AND created_at IS NOT NULL
          ${IN_STUDIO_PLAN_FILTER}
        GROUP BY LOWER(customer_email)
      ),
      new_custs AS (
        SELECT LOWER(r.email) AS email,
               MIN(r.attended_at) AS first_date,
               DATE_TRUNC('week', MIN(r.attended_at))::date AS cohort_start
        FROM registrations r
        WHERE r.is_first_visit = TRUE
          AND r.attended_at IS NOT NULL
          AND r.email IS NOT NULL
          ${passFilter}
        GROUP BY LOWER(r.email)
      )
      SELECT nc.cohort_start::text AS "cohortStart",
             COUNT(*) FILTER (
               WHERE fe.first_sub_date IS NOT NULL
                 AND fe.first_sub_date - nc.first_date BETWEEN 0 AND 41
             )::int AS "matchedFirstEverSubs"
      FROM new_custs nc
      LEFT JOIN first_ever_sub fe ON fe.email = nc.email
      WHERE nc.cohort_start >= DATE_TRUNC('week', CURRENT_DATE - INTERVAL '14 weeks')::date
      GROUP BY nc.cohort_start
      ORDER BY nc.cohort_start
    `;
    const cohortMatch = await pool.query<CohortMatchRow>(cohortMatchQuery);

    // 3. Pull the cohort table the chart uses, for direct comparison.
    const cohortRows = await getNewCustomerCohorts();

    // Merge cohort rows + cohortMatch into a single by-cohort view.
    const matchByCohort = new Map(
      cohortMatch.rows.map((r) => [r.cohortStart, r.matchedFirstEverSubs]),
    );
    const today = new Date();
    today.setHours(0, 0, 0, 0);
    const byCohortWeek = cohortRows.map((c) => {
      const start = new Date(c.cohortStart + "T00:00:00");
      const daysElapsed = Math.floor(
        (today.getTime() - start.getTime()) / (1000 * 60 * 60 * 24),
      );
      const matched = matchByCohort.get(c.cohortStart) ?? 0;
      return {
        cohortStart: c.cohortStart,
        cohortEnd: c.cohortEnd,
        daysElapsed,
        windowClosed: daysElapsed >= 42,
        newCustomers: c.newCustomers,
        total3Week: c.total3Week,
        total6Week: c.total6Week,
        matchedFirstEverSubs: matched,
        gap: c.total6Week - matched, // Should be 0 for healthy weeks.
      };
    });

    out.bySubscriptionWeek = subsByWeek.rows;
    out.byCohortWeek = byCohortWeek;
    out.legend = {
      bySubscriptionWeek: {
        totalSubStarts:
          "All in-studio sub rows (Sky3 + Member) created in this week, including re-subs.",
        firstEverSubs:
          "Subset where this is the email's first-ever in-studio sub (MIN(created_at) for that email).",
        matchedToRecentCohort:
          "First-ever subs whose first in-studio drop-in visit was 0-41 days before sub date. Should equal chart's total6Week credited to that cohort.",
        matchedToOlderCohort:
          "First-ever subs whose first drop-in visit was >41 days ago. Comeback / delayed converts — chart drops them.",
        noPriorVisit:
          "First-ever subs with no in-studio visit on record. Online-only signups.",
        notInDropInPool:
          "First-ever subs whose first visit was not a drop-in/intro pass (e.g. workshop, class pack). Chart's pool excludes them by design.",
      },
      byCohortWeek: {
        total6Week:
          "What the dashboard chart shows as 'Converts' for this cohort.",
        matchedFirstEverSubs:
          "First-ever in-studio subs from emails in this cohort that landed within 41 days. Should match total6Week exactly.",
        gap: "total6Week - matchedFirstEverSubs. >0 means chart credits more than the SQL says; <0 means there's a missing-conversions bug.",
      },
    };
  } catch (err) {
    out.error = err instanceof Error ? err.message : String(err);
  }

  return NextResponse.json(out);
}
