import "dotenv/config";
import { Pool } from "pg";

// Read-only verification of the dashboard's "Intro Week" conversion rate.
// See .claude/plans/we-re-looking-to-cinfirm-quiet-tarjan.md for context.

const pool = new Pool({ connectionString: process.env.DATABASE_URL, ssl: { rejectUnauthorized: false } });

const WEEKS_BACK = 8;

// Mirrors introPassFilter() in src/lib/db/registration-store.ts
const introPassClause = (col: string) =>
  `(UPPER(${col}) LIKE '%INTRO WEEK%' OR UPPER(${col}) LIKE '%TRIAL%' OR UPPER(${col}) LIKE '%FIRST%')`;

// Mirrors dropInPassFilter() in src/lib/db/registration-store.ts
const dropInPassClause = (col: string) => `
  (
    UPPER(${col}) LIKE '%DROP-IN%' OR UPPER(${col}) LIKE '%DROP IN%' OR UPPER(${col}) LIKE '%DROPIN%'
    OR UPPER(${col}) LIKE '%DROPLET%'
    OR UPPER(${col}) LIKE '%INTRO WEEK%'
    OR UPPER(${col}) LIKE '%TRIAL%'
    OR UPPER(${col}) LIKE '%FIRST%'
    OR UPPER(${col}) LIKE '%SINGLE CLASS%'
    OR UPPER(${col}) LIKE '%WELLHUB%'
    OR UPPER(${col}) LIKE '%GUEST%'
    OR UPPER(${col}) LIKE '%COMMUNITY DAY%'
    OR UPPER(${col}) LIKE '%POKER CHIP%'
    OR UPPER(${col}) LIKE '%ON RUNNING%'
  )`;

// Mirrors IN_STUDIO_PLAN_FILTER (Member + Sky3 only, excludes TV)
const planFilterMemberSky3Only = `
  AND (
    UPPER(plan_name) LIKE '%SKY3%' OR UPPER(plan_name) LIKE '%SKY5%'
    OR UPPER(plan_name) LIKE '%SKYHIGH%' OR UPPER(plan_name) LIKE '%5 PACK%'
    OR UPPER(plan_name) LIKE '%5-PACK%'
    OR UPPER(plan_name) LIKE '%UNLIMITED%' OR UPPER(plan_name) LIKE '%MEMBER%'
    OR UPPER(plan_name) LIKE '%ALL ACCESS%' OR UPPER(plan_name) LIKE '%TING FAM%'
    OR UPPER(plan_name) LIKE '%WELCOME SKY3%'
  )
  AND UPPER(plan_name) NOT LIKE '%SKY TING TV%'
  AND UPPER(plan_name) NOT LIKE '%SKYTING TV%'`;

// Same plan-name list, but TV is allowed.
const planFilterIncludingTv = `
  AND (
    UPPER(plan_name) LIKE '%SKY3%' OR UPPER(plan_name) LIKE '%SKY5%'
    OR UPPER(plan_name) LIKE '%SKYHIGH%' OR UPPER(plan_name) LIKE '%5 PACK%'
    OR UPPER(plan_name) LIKE '%5-PACK%'
    OR UPPER(plan_name) LIKE '%UNLIMITED%' OR UPPER(plan_name) LIKE '%MEMBER%'
    OR UPPER(plan_name) LIKE '%ALL ACCESS%' OR UPPER(plan_name) LIKE '%TING FAM%'
    OR UPPER(plan_name) LIKE '%WELCOME SKY3%'
    OR UPPER(plan_name) LIKE '%SKY TING TV%' OR UPPER(plan_name) LIKE '%SKYTING TV%'
  )`;

function fmtPct(num: number, den: number): string {
  if (den === 0) return "n/a";
  return `${((num / den) * 100).toFixed(1)}%`;
}

async function variantADashboardReproduction() {
  // Replicates getConversionByJourneyWeekly(8, "intro-week")
  const query = `
    WITH week_series AS (
      SELECT generate_series(
        (DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '${WEEKS_BACK} weeks')::date,
        (DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '1 week')::date,
        '1 week'::interval
      )::date as week_start
    ),
    non_auto_pool AS (
      SELECT DATE_TRUNC('week', r.attended_at)::date as week_start,
             COUNT(DISTINCT LOWER(r.email)) as pool_size
      FROM registrations r
      WHERE r.attended_at IS NOT NULL
        AND r.email IS NOT NULL
        AND (r.subscription = 'false' OR r.subscription IS NULL)
        AND ${dropInPassClause("r.pass")}
        AND r.attended_at >= (DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '${WEEKS_BACK} weeks')::date
        AND r.attended_at < DATE_TRUNC('week', CURRENT_DATE)::date
        AND ${introPassClause("r.pass")}
      GROUP BY DATE_TRUNC('week', r.attended_at)::date
    ),
    all_sub_starts AS (
      SELECT LOWER(customer_email) as email,
             created_at as sub_date,
             DATE_TRUNC('week', created_at)::date as convert_week
      FROM auto_renews
      WHERE customer_email IS NOT NULL
        AND created_at IS NOT NULL
        ${planFilterMemberSky3Only}
        AND created_at >= (DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '${WEEKS_BACK} weeks')::date
        AND created_at < DATE_TRUNC('week', CURRENT_DATE)::date
    ),
    converters AS (
      SELECT DISTINCT a.email, a.convert_week
      FROM all_sub_starts a
      WHERE EXISTS (
        SELECT 1 FROM registrations r
        WHERE LOWER(r.email) = a.email
          AND r.attended_at IS NOT NULL
          AND r.attended_at < a.sub_date
          AND (r.subscription = 'false' OR r.subscription IS NULL)
          AND ${introPassClause("r.pass")}
      )
    ),
    weekly_converts AS (
      SELECT convert_week as week_start, COUNT(*) as converts
      FROM converters
      GROUP BY convert_week
    )
    SELECT ws.week_start::text as "weekStart",
           COALESCE(p.pool_size, 0)::int as pool,
           COALESCE(wc.converts, 0)::int as converts
    FROM week_series ws
    LEFT JOIN non_auto_pool p ON p.week_start = ws.week_start
    LEFT JOIN weekly_converts wc ON wc.week_start = ws.week_start
    ORDER BY ws.week_start
  `;
  const { rows } = await pool.query(query);
  const totalPool = rows.reduce((s: number, r: { pool: number }) => s + Number(r.pool), 0);
  const totalConverts = rows.reduce((s: number, r: { converts: number }) => s + Number(r.converts), 0);

  console.log("=== VARIANT A: Dashboard reproduction (Member+Sky3 only, bucket math) ===");
  console.log("Per-week (week_start, pool, converts):");
  for (const r of rows) {
    console.log(`  ${r.weekStart}  pool=${r.pool}  converts=${r.converts}`);
  }
  console.log(`TOTAL pool=${totalPool}  converts=${totalConverts}  rate=${fmtPct(totalConverts, totalPool)}`);
  console.log(`Dashboard shows: pool=215  converts=83  rate=38.6%`);
  const matches = totalPool === 215 && totalConverts === 83;
  console.log(`MATCH? ${matches ? "YES ✓" : "NO ✗ — investigate before trusting other variants"}\n`);
  return { totalPool, totalConverts };
}

async function variantBCohortIncludingTv() {
  // Step 1: define the cohort — emails with intro-week attendance in the last 8 complete weeks
  const cohortQuery = `
    SELECT LOWER(r.email) as email,
           MIN(r.attended_at) as first_intro_attended,
           DATE_TRUNC('week', MIN(r.attended_at))::date as cohort_week
    FROM registrations r
    WHERE r.attended_at IS NOT NULL
      AND r.email IS NOT NULL
      AND (r.subscription = 'false' OR r.subscription IS NULL)
      AND ${introPassClause("r.pass")}
      AND r.attended_at >= (DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '${WEEKS_BACK} weeks')::date
      AND r.attended_at < DATE_TRUNC('week', CURRENT_DATE)::date
    GROUP BY LOWER(r.email)
  `;
  const { rows: cohort } = await pool.query(cohortQuery);
  const cohortEmails = new Set<string>(cohort.map((r: { email: string }) => r.email));

  // Step 2: of those emails, who started a Member/Sky3/TV auto-renew at-or-after their intro-week start?
  const convertQuery = `
    WITH cohort AS (${cohortQuery})
    SELECT DISTINCT ON (c.email)
           c.email,
           c.first_intro_attended,
           c.cohort_week,
           ar.plan_name,
           ar.plan_category,
           ar.plan_state,
           ar.created_at as sub_started_at
    FROM cohort c
    JOIN auto_renews ar
      ON LOWER(ar.customer_email) = c.email
     AND ar.created_at IS NOT NULL
     AND ar.created_at >= c.first_intro_attended
    WHERE 1=1
      ${planFilterIncludingTv}
    ORDER BY c.email, ar.created_at ASC
  `;
  const { rows: converts } = await pool.query(convertQuery);

  const poolSize = cohortEmails.size;
  const convertCount = converts.length;

  // Per-week breakdown anchored on the intro-week cohort week
  const perWeek: Record<string, { pool: number; converts: number }> = {};
  for (const r of cohort) {
    const wk = (r.cohort_week instanceof Date ? r.cohort_week.toISOString().slice(0, 10) : String(r.cohort_week));
    perWeek[wk] = perWeek[wk] || { pool: 0, converts: 0 };
    perWeek[wk].pool += 1;
  }
  for (const r of converts) {
    const wk = (r.cohort_week instanceof Date ? r.cohort_week.toISOString().slice(0, 10) : String(r.cohort_week));
    perWeek[wk] = perWeek[wk] || { pool: 0, converts: 0 };
    perWeek[wk].converts += 1;
  }

  // Plan-category split
  const byCategory: Record<string, number> = {};
  const byPlanName: Record<string, number> = {};
  const byState: Record<string, number> = {};
  for (const r of converts) {
    const planName = String(r.plan_name || "");
    const upper = planName.toUpperCase();
    let bucket = "OTHER";
    if (upper.includes("SKY TING TV") || upper.includes("SKYTING TV")) bucket = "TV";
    else if (upper.includes("MEMBER") || upper.includes("ALL ACCESS") || upper.includes("UNLIMITED") || upper.includes("TING FAM")) bucket = "Member";
    else if (upper.includes("SKY3") || upper.includes("SKY5") || upper.includes("SKYHIGH") || upper.includes("5 PACK") || upper.includes("5-PACK") || upper.includes("WELCOME SKY3")) bucket = "Sky3";
    byCategory[bucket] = (byCategory[bucket] || 0) + 1;
    byPlanName[planName] = (byPlanName[planName] || 0) + 1;
    const state = String(r.plan_state || "Unknown");
    byState[state] = (byState[state] || 0) + 1;
  }

  console.log("=== VARIANT B: User's definition (cohort math, includes TV) ===");
  console.log(`Window: last ${WEEKS_BACK} complete Mon–Sun weeks ending ${new Date().toISOString().slice(0, 10)}`);
  console.log(`Pool (unique emails who attended intro week in window): ${poolSize}`);
  console.log(`Converters (started Member/Sky3/TV sub at-or-after their intro-week date): ${convertCount}`);
  console.log(`RATE: ${fmtPct(convertCount, poolSize)}`);
  console.log("");
  console.log("Per cohort week (anchored on intro-week start, NOT sub start):");
  for (const wk of Object.keys(perWeek).sort()) {
    const { pool: p, converts: c } = perWeek[wk];
    console.log(`  ${wk}  pool=${p}  converts=${c}  rate=${fmtPct(c, p)}`);
  }
  console.log("");
  console.log("Converters by plan category (first qualifying sub):");
  for (const [k, v] of Object.entries(byCategory).sort((a, b) => b[1] - a[1])) {
    console.log(`  ${k.padEnd(8)} ${v}`);
  }
  console.log("");
  console.log("Converters by current plan_state of that sub:");
  for (const [k, v] of Object.entries(byState).sort((a, b) => b[1] - a[1])) {
    console.log(`  ${k.padEnd(16)} ${v}`);
  }
  console.log("");
  console.log("Converters by plan_name (first qualifying sub):");
  for (const [k, v] of Object.entries(byPlanName).sort((a, b) => b[1] - a[1])) {
    console.log(`  ${String(v).padStart(3)}  ${k}`);
  }
  console.log("");
  return { poolSize, convertCount };
}

async function variantCDashboardWithTv() {
  // Same as Variant A but with TV included in the plan filter
  const query = `
    WITH non_auto_pool AS (
      SELECT COUNT(DISTINCT LOWER(r.email)) as pool_size
      FROM registrations r
      WHERE r.attended_at IS NOT NULL
        AND r.email IS NOT NULL
        AND (r.subscription = 'false' OR r.subscription IS NULL)
        AND ${dropInPassClause("r.pass")}
        AND r.attended_at >= (DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '${WEEKS_BACK} weeks')::date
        AND r.attended_at < DATE_TRUNC('week', CURRENT_DATE)::date
        AND ${introPassClause("r.pass")}
    ),
    all_sub_starts AS (
      SELECT LOWER(customer_email) as email,
             created_at as sub_date
      FROM auto_renews
      WHERE customer_email IS NOT NULL
        AND created_at IS NOT NULL
        ${planFilterIncludingTv}
        AND created_at >= (DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '${WEEKS_BACK} weeks')::date
        AND created_at < DATE_TRUNC('week', CURRENT_DATE)::date
    ),
    converters AS (
      SELECT DISTINCT a.email
      FROM all_sub_starts a
      WHERE EXISTS (
        SELECT 1 FROM registrations r
        WHERE LOWER(r.email) = a.email
          AND r.attended_at IS NOT NULL
          AND r.attended_at < a.sub_date
          AND (r.subscription = 'false' OR r.subscription IS NULL)
          AND ${introPassClause("r.pass")}
      )
    )
    SELECT
      (SELECT pool_size FROM non_auto_pool)::int as pool,
      (SELECT COUNT(*) FROM converters)::int as converts
  `;
  const { rows } = await pool.query(query);
  const { pool: p, converts: c } = rows[0];
  console.log("=== VARIANT C: Dashboard math, but TV included in conversions ===");
  console.log(`pool=${p}  converts=${c}  rate=${fmtPct(c, p)}`);
  console.log("(Same window/pool as the dashboard's 38.6%, only the plan filter widened)");
  console.log("");
}

async function variantDViaCanonicalFn() {
  const { getIntroWeekCohortConversionWeekly } = await import("../src/lib/db/registration-store");
  const rows = await getIntroWeekCohortConversionWeekly(WEEKS_BACK);
  const totalPool = rows.reduce((s, r) => s + r.pool, 0);
  const totalConv = rows.reduce((s, r) => s + r.converts, 0);
  console.log("=== VARIANT D: via getIntroWeekCohortConversionWeekly() (the wired canonical fn) ===");
  for (const r of rows) {
    console.log(`  ${r.weekStart}  pool=${r.pool}  converts=${r.converts}`);
  }
  console.log(`TOTAL pool=${totalPool}  converts=${totalConv}  rate=${fmtPct(totalConv, totalPool)}`);
  console.log("");
}

async function main() {
  await variantADashboardReproduction();
  await variantCDashboardWithTv();
  await variantBCohortIncludingTv();
  await variantDViaCanonicalFn();
  await pool.end();
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
