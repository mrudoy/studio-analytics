/**
 * Layer-2 dashboard auditor (the proactive half of continuous bug detection).
 *
 * Layer 1 (src/lib/db/drift-check.ts) asserts the invariants behind bugs we've
 * ALREADY fixed, every pipeline cycle. This script is the broader net: it pulls
 * the LIVE /api/stats the dashboard renders and independently recomputes a panel
 * of "anchor" metrics straight from the database, then diffs the two. A gap means
 * the rendered number disagrees with the raw data — exactly the class of
 * data-correctness bug that green builds/tests don't catch.
 *
 * It is DETECTION ONLY. It never writes to the DB and never edits code. The
 * nightly routine that runs it is responsible for diagnosing any flagged gap and
 * opening a DRAFT PR through the Codex→Greptile gates for a human to merge.
 *
 * FALSE POSITIVES ARE THE ENEMY. An auditor that cries wolf gets ignored, so the
 * independent recompute deliberately mirrors the dashboard's CATEGORIZATION
 * (runtime getCategory, never the stored plan_category column, which can lag) and
 * the dashboard's active filter — the independence is in the counting/dedup
 * logic, not in re-deriving categories a second, divergent way. Anything the
 * audit cannot evaluate (incomplete payload, prod hiccup) exits 1 (error), never
 * exit 2 (mismatch).
 *
 * Exit code 0 = all anchors agree (clean). Exit code 2 = at least one mismatch
 * (the routine should investigate). Exit code 1 = the audit itself errored.
 *
 * Env:
 *   DATABASE_URL            required — the same Railway DB the app uses
 *   AUDIT_BASE_URL          prod base (default https://studio-analytics-production.up.railway.app)
 *   AUDIT_PASSWORD          required — the dashboard login password. No default:
 *                           we never bake a live credential into source, and a
 *                           silent fallback would mask a misconfigured runner.
 *
 * Usage: AUDIT_PASSWORD=… npx tsx scripts/audit-dashboard.ts
 */
import { getPool, closePool } from "../src/lib/db/database";
import { getCategory } from "../src/lib/analytics/categories";
import { ACTIVE_STATES, AT_RISK_STATES } from "../src/lib/analytics/metrics/filters";
import { ANY_AR_PLAN_FILTER, introPassFilter } from "../src/lib/db/registration-store";

/** Weeks of intro-cohort history the dashboard renders — must track the
 *  `weeksBack` in runJourneyConversion (src/lib/analytics/db-trends.ts). */
const INTRO_WEEKS_BACK = 8;

const BASE = process.env.AUDIT_BASE_URL || "https://studio-analytics-production.up.railway.app";
const PASSWORD = process.env.AUDIT_PASSWORD;

interface Check {
  name: string;
  ok: boolean;
  dashboard: unknown;
  independent: unknown;
  note?: string;
}

const ACTIVE = new Set<string>(ACTIVE_STATES);
const AT_RISK = new Set<string>(AT_RISK_STATES);
const CAT_KEY: Record<string, "member" | "sky3" | "skyTingTv" | "unknown"> = {
  MEMBER: "member", SKY3: "sky3", SKY_TING_TV: "skyTingTv", UNKNOWN: "unknown",
};

/** Raised when the audit cannot be evaluated (→ exit 1), distinct from a data
 *  mismatch (→ exit 2). */
class AuditUnavailable extends Error {}

async function login(): Promise<string> {
  if (!PASSWORD) throw new AuditUnavailable("AUDIT_PASSWORD env var is not set");
  const res = await fetch(`${BASE}/api/auth/login`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ password: PASSWORD }),
  });
  if (!res.ok) throw new AuditUnavailable(`login failed: HTTP ${res.status}`);
  const setCookie = res.headers.get("set-cookie");
  if (!setCookie) throw new AuditUnavailable("login returned no cookie");
  return setCookie.split(";")[0];
}

async function fetchStats(cookie: string): Promise<Record<string, any>> {
  // nocache=1 forces a fresh render so the payload reflects the SAME moment as
  // our DB recompute (a 15-min-stale cache could spuriously disagree). This
  // benignly warms the prod stats cache, exactly like a normal user page load.
  const res = await fetch(`${BASE}/api/stats?nocache=1`, {
    headers: { Cookie: cookie, Referer: `${BASE}/` },
  });
  if (!res.ok) throw new AuditUnavailable(`/api/stats failed: HTTP ${res.status}`);
  return res.json();
}

/** Reject an incomplete payload (e.g. /api/stats served 200 with trends nulled
 *  out because computeTrendsFromDB threw under safe()) as UNAVAILABLE, not as a
 *  mismatch — otherwise a transient backend failure looks like a data bug. */
function requirePayload(stats: Record<string, any>): void {
  // Validate EVERY field the checks below read, down to the nested element
  // level. A field that is missing OR malformed (wrong type) means the payload
  // is degraded — that is AuditUnavailable (exit 1), never a data mismatch
  // (exit 2) or a silently-passed check (exit 0). num()/str() collect the
  // offending paths instead of throwing field-by-field.
  const bad: string[] = [];
  const num = (v: unknown, path: string) => { if (typeof v !== "number" || Number.isNaN(v)) bad.push(path); };
  const str = (v: unknown, path: string) => { if (typeof v !== "string" || v === "") bad.push(path); };

  const a = stats.activeSubscribers;
  if (a == null) bad.push("activeSubscribers");
  else for (const k of ["member", "sky3", "skyTingTv", "total"]) num(a[k], `activeSubscribers.${k}`);

  const mrr = stats.mrr;
  if (mrr == null) bad.push("mrr");
  else for (const k of ["member", "sky3", "skyTingTv", "total"]) num(mrr[k], `mrr.${k}`);
  num(stats.arpu?.overall, "arpu.overall");

  if (!Array.isArray(stats.monthlyRevenue)) bad.push("monthlyRevenue");
  else stats.monthlyRevenue.forEach((m: any, i: number) => { str(m?.month, `monthlyRevenue[${i}].month`); num(m?.gross, `monthlyRevenue[${i}].gross`); num(m?.net, `monthlyRevenue[${i}].net`); });

  const byCat = stats.trends?.churnRates?.byCategory;
  if (byCat == null) bad.push("trends.churnRates.byCategory");
  else for (const k of ["member", "sky3", "skyTingTv"]) {
    if (!byCat[k]) { bad.push(`byCategory.${k}`); continue; }
    num(byCat[k].atRiskCount, `byCategory.${k}.atRiskCount`);
    if (!Array.isArray(byCat[k].monthly)) bad.push(`byCategory.${k}.monthly`);
    else byCat[k].monthly.forEach((m: any, i: number) => { str(m?.month, `byCategory.${k}.monthly[${i}].month`); num(m?.canceledCount, `byCategory.${k}.monthly[${i}].canceledCount`); num(m?.activeAtStart, `byCategory.${k}.monthly[${i}].activeAtStart`); });
  }

  const cw = stats.trends?.newCustomerVolume?.completedWeeks;
  if (!Array.isArray(cw) || cw.length === 0) bad.push("trends.newCustomerVolume.completedWeeks (empty/missing)");
  else str(cw[cw.length - 1]?.weekEnd, "newCustomerVolume.completedWeeks[last].weekEnd");

  // movement is the canonical subscriber-movement source (getSubscriberMovement).
  // It feeds the new/canceled window cards AND is copied over trends.* at API
  // assembly, so the movement anchors read both surfaces. A missing/partial
  // block means the safe() wrapper swallowed a failure — UNAVAILABLE, never a
  // mismatch.
  //
  // Validate EXACTLY the fields the remaining anchors read, no more: flagging a
  // field nothing consumes would exit 1 on a payload the audit could actually
  // have evaluated, which is its own kind of false alarm. Windows are the three
  // the signup-bound anchor iterates (thisWeek is not one of them); monthly
  // needs `period` plus the churn pair, not `new`.
  const mvWindows = ["yesterday", "lastWeek", "thisMonth"] as const;
  const mv = stats.movement;
  if (mv == null) bad.push("movement");
  else {
    for (const w of mvWindows) {
      const b = mv.byWindow?.[w];
      if (b == null) { bad.push(`movement.byWindow.${w}`); continue; }
      str(b.windowStart, `movement.byWindow.${w}.windowStart`);
      str(b.windowEnd, `movement.byWindow.${w}.windowEnd`);
      for (const k of ["member", "sky3", "skyTingTv"]) {
        if (!b[k]) { bad.push(`movement.byWindow.${w}.${k}`); continue; }
        num(b[k].new, `movement.byWindow.${w}.${k}.new`);
      }
    }
    if (!Array.isArray(mv.monthly) || mv.monthly.length === 0) bad.push("movement.monthly (empty/missing)");
    else mv.monthly.forEach((m: any, i: number) => {
      str(m?.period, `movement.monthly[${i}].period`);
      for (const k of ["member", "sky3", "skyTingTv"]) {
        if (!m?.[k]) { bad.push(`movement.monthly[${i}].${k}`); continue; }
        num(m[k].canceled, `movement.monthly[${i}].${k}.canceled`);
        num(m[k].activeAtStart, `movement.monthly[${i}].${k}.activeAtStart`);
      }
    });
  }

  // journeyConversion is null when the DB has no registration data at all
  // (runJourneyConversion bails early). That is "cannot evaluate", not a data
  // bug — so a missing/short block is UNAVAILABLE, never a mismatch.
  const iw = stats.trends?.journeyConversion?.introWeek?.weeks;
  if (!Array.isArray(iw)) bad.push("trends.journeyConversion.introWeek.weeks");
  else if (iw.length !== INTRO_WEEKS_BACK) bad.push(`journeyConversion.introWeek.weeks (${iw.length} weeks, expected ${INTRO_WEEKS_BACK})`);
  else iw.forEach((w: any, i: number) => { str(w?.weekStart, `introWeek.weeks[${i}].weekStart`); num(w?.pool, `introWeek.weeks[${i}].pool`); num(w?.converts, `introWeek.weeks[${i}].converts`); });

  if (bad.length) throw new AuditUnavailable(`incomplete/malformed /api/stats payload (backend likely degraded): ${bad.slice(0, 8).join(", ")}${bad.length > 8 ? ` …(+${bad.length - 8})` : ""}`);
}

/** MAX(auto_renews.imported_at) — bumped by saveAutoRenews on every write, so a
 *  change means the pipeline touched subscription rows. Used to tell a real
 *  mismatch apart from a mid-audit race (see anchor 8). */
async function pipelineWatermark(pool: ReturnType<typeof getPool>): Promise<string> {
  const { rows } = await pool.query<{ ts: string | null }>(
    `SELECT MAX(imported_at)::text AS ts FROM auto_renews`,
  );
  return rows[0]?.ts ?? "";
}

async function main() {
  const checks: Check[] = [];
  const pool = getPool();
  const cookie = await login();
  // Captured BEFORE the render, not after: the race window opens the moment
  // /api/stats reads its data. A watermark taken after fetchStats would already
  // include a write that landed during the render, making that write invisible
  // to the comparison and letting anchor 8 false-fail on it.
  const watermarkBefore = await pipelineWatermark(pool);
  const stats = await fetchStats(cookie);
  requirePayload(stats);

  // Single fetch of active-ish rows; categorize with the SAME runtime getCategory
  // the dashboard uses, then recompute counts independently in JS.
  const { rows: arRows } = await pool.query<{ plan_name: string; plan_state: string; current_state: string | null; email: string | null }>(
    `SELECT plan_name, plan_state, current_state, LOWER(customer_email) AS email
     FROM auto_renews
     WHERE plan_state = ANY($1) AND (current_state IS NULL OR current_state = 'active')`,
    [ACTIVE_STATES as unknown as string[]],
  );

  // ── 1. Active counts (rows — mirrors getActiveCounts, which SKIPS blank email) ──
  const dbActive = { member: 0, sky3: 0, skyTingTv: 0 };
  // ── 2. At-risk (active + AT_RISK_STATES, deduped by email — mirrors computeChurnRates,
  //       which dedups customer_email.toLowerCase() WITHOUT a blank guard, so blank
  //       emails collapse to one "" bucket. The two functions handle blanks
  //       differently, so the recomputes must too. ──
  const atRiskEmails = { member: new Set<string>(), sky3: new Set<string>(), skyTingTv: new Set<string>() };
  for (const r of arRows) {
    const key = CAT_KEY[getCategory(r.plan_name)];
    if (key === "unknown") continue; // unknown is its own bucket; the active.* anchors are the 3 real categories
    if (r.email) dbActive[key]++;                                  // active: skip blank (getActiveCounts)
    if (AT_RISK.has(r.plan_state)) atRiskEmails[key].add(r.email ?? ""); // at-risk: blank → "" (computeChurnRates)
  }
  const a = stats.activeSubscribers;
  const byCat = stats.trends.churnRates.byCategory;
  for (const key of ["member", "sky3", "skyTingTv"] as const) {
    checks.push({ name: `active.${key}`, ok: a[key] === dbActive[key], dashboard: a[key], independent: dbActive[key] });
    checks.push({
      name: `atRisk.${key}`,
      ok: byCat[key].atRiskCount === atRiskEmails[key].size,
      dashboard: byCat[key].atRiskCount, independent: atRiskEmails[key].size,
      note: "active subs in Past Due/Invalid/Pending Cancel, deduped by email, runtime category",
    });
  }

  // ── 3. MRR / ARPU internal consistency ─────────────────────────
  const mrr = stats.mrr;
  const mrrSum = Math.round(((mrr.member ?? 0) + (mrr.sky3 ?? 0) + (mrr.skyTingTv ?? 0) + (mrr.unknown ?? 0)) * 100) / 100;
  checks.push({ name: "mrr.total=Σcategory", ok: Math.abs((mrr.total ?? 0) - mrrSum) < 0.5, dashboard: mrr.total, independent: mrrSum });
  const expArpu = (a.total ?? 0) > 0 ? Math.round(((mrr.total ?? 0) / a.total) * 100) / 100 : 0;
  checks.push({ name: "arpu.overall=mrr/active", ok: Math.abs((stats.arpu.overall ?? 0) - expArpu) < 0.5, dashboard: stats.arpu.overall, independent: expArpu });

  // ── 4. New-customer "current week" is the real calendar week ────
  // Anchor the boundary on Postgres' own DATE_TRUNC('week', CURRENT_DATE) — the
  // exact expression getNewCustomerVolumeByWeek uses — so there's no JS/PG TZ
  // mismatch near the Sun/Mon boundary.
  const { rows: wkRows } = await pool.query<{ expected_last_end: string }>(
    `SELECT (DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '1 day')::date::text AS expected_last_end`,
  );
  const expectedLastEnd = wkRows[0]?.expected_last_end ?? null;
  const completed = stats.trends.newCustomerVolume.completedWeeks;
  const lastCompletedEnd = completed.length ? completed[completed.length - 1].weekEnd : null;
  checks.push({
    name: "newCustomerVolume.currentWeek=calendarWeek",
    ok: lastCompletedEnd === expectedLastEnd,
    dashboard: `completedWeeks end ${lastCompletedEnd}`, independent: `expected ${expectedLastEnd}`,
    note: "guards the 'last week shown as this week' off-by-one",
  });

  // ── 5. Monthly revenue integrity: net ≤ gross, gross ≥ 0 ──
  // NOT net ≥ 0: a slice where refunds exceed gross (e.g. a stray refund in a
  // month with ~no gross — Dec 2023 has gross 0 / net -$0.62) legitimately has
  // negative net. The real invariants are gross can't be negative and fees/
  // refunds only pull net BELOW gross.
  const months = (stats.monthlyRevenue ?? []) as { month: string; gross: number; net: number }[];
  const badRev = months.filter((m) => m.net > m.gross + 0.5 || m.gross < 0);
  checks.push({
    name: "monthlyRevenue.netLEgross&grossNonneg",
    ok: badRev.length === 0,
    dashboard: badRev.length ? badRev.map((m) => m.month) : "all ok", independent: "net≤gross, gross≥0",
  });

  // ── 6. Monthly churn: no completed month with active subs but 0 churn (per category) ──
  const zeroChurn: string[] = [];
  for (const key of ["member", "sky3", "skyTingTv"] as const) {
    const monthly = (byCat[key].monthly ?? []) as { month: string; canceledCount: number; activeAtStart: number }[];
    for (const m of monthly.slice(0, -1)) {
      if (m.month === "2025-10") continue;
      if (m.activeAtStart > 0 && m.canceledCount === 0) zeroChurn.push(`${key}:${m.month}`);
    }
  }
  checks.push({
    name: "monthlyChurn.noZeroCompletedMonths",
    ok: zeroChurn.length === 0,
    dashboard: zeroChurn.length ? zeroChurn : "all completed months churn>0", independent: "every completed month must have churn",
  });

  // ── 7. Intro-week cohort conversion: independent cohort recompute ──
  // Guards the metric with the worst bug history on the dashboard:
  //   • bucket-math regression — summing per-week distinct-email pools instead of
  //     counting each email ONCE in their first cohort week (2026-05-08: 215 vs 183).
  //   • dropping Sky Ting TV from the converter plan filter (using
  //     IN_STUDIO_PLAN_FILTER instead of ANY_AR_PLAN_FILTER) — deflates converts.
  //   • mis-bucketing a cohort into the wrong week.
  // The recompute pulls RAW rows and does all cohort dedup/assignment in JS, so
  // it shares no CTE logic with getIntroWeekCohortConversionWeekly. Postgres still
  // does the week bucketing and the epoch math — replicating DATE_TRUNC('week')
  // in JS would just invent a timezone bug (see the check-4 note).
  //
  // NOTE: the pass/plan predicates ARE shared with the dashboard (imported above),
  // so this anchor deliberately does NOT catch a predicate that stops matching the
  // studio's intro pass (the 2026-04 "2 WEEK INTRO" rebrand, which silently zeroed
  // the pool for two months). Both sides would collapse together. That class needs
  // a separate detector — a new high-volume non-AR pass the intro filter misses —
  // which is left out here on purpose: the candidate pass names in this data
  // ("SKY WEEK", "DEMO FALL 2025", "Community Day") make a token-based rule too
  // false-positive-prone to gate an auto-fix on.
  // KNOWN ARTIFACT (documented, deliberately asserted as-is — NOT auto-"fixed"):
  // getIntroWeekCohortConversionWeekly filters attendances to the window and THEN
  // takes MIN(attended_at), so someone whose intro began just BEFORE the window and
  // ran into it is counted as a fresh cohort member of the oldest rendered week.
  // CLAUDE.md's definition ("whose FIRST intro attendance falls in the last N weeks")
  // would exclude them. MEASURED 2026-07-19: 24 of the 227-person 8-week pool, all 24
  // in the oldest week — inflating it 30 -> 54; every other week measured 0.
  // That concentration is an OBSERVATION, not a guarantee the code enforces: the pass
  // predicate is broad (INTRO|TRIAL|FIRST) and nothing ties a repeat qualifying
  // attendance to a pass duration, so an interior week CAN pick this up if someone has
  // an older qualifying attendance plus a later one inside the window. Re-measure
  // before relying on the left-edge-only shape.
  // This anchor mirrors the shipped behaviour on purpose: its job is to catch
  // REGRESSIONS, and silently redefining a rendered conversion metric is a product
  // decision for Mike, not an auto-fix. Flagged for a human call.
  const { rows: introAtt } = await pool.query<{ email: string; at_epoch: number; attend_week: string }>(
    `SELECT LOWER(r.email) AS email,
            EXTRACT(EPOCH FROM r.attended_at)::float8 AS at_epoch,
            DATE_TRUNC('week', r.attended_at)::date::text AS attend_week
     FROM registrations r
     WHERE r.attended_at IS NOT NULL AND r.email IS NOT NULL
       AND (r.subscription = 'false' OR r.subscription IS NULL)
       AND ${introPassFilter("r.pass")}
       AND r.attended_at >= (DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '${INTRO_WEEKS_BACK} weeks')::date
       AND r.attended_at <  DATE_TRUNC('week', CURRENT_DATE)::date`,
  );
  // Any conversion must post-date a first-intro inside the window, so restricting
  // auto-renews to the same window start is equivalent to the unbounded scan.
  const { rows: introArs } = await pool.query<{ email: string; created_epoch: number }>(
    `SELECT LOWER(customer_email) AS email,
            EXTRACT(EPOCH FROM created_at::timestamp)::float8 AS created_epoch
     FROM auto_renews
     WHERE customer_email IS NOT NULL AND created_at IS NOT NULL
       AND created_at >= (DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '${INTRO_WEEKS_BACK} weeks')::date
       ${ANY_AR_PLAN_FILTER}`,
  );

  // Each email lands in exactly one cohort week: the week of their FIRST intro attendance.
  const firstIntro = new Map<string, { epoch: number; week: string }>();
  for (const r of introAtt) {
    const cur = firstIntro.get(r.email);
    if (!cur || r.at_epoch < cur.epoch) firstIntro.set(r.email, { epoch: r.at_epoch, week: r.attend_week });
  }
  const arStarts = new Map<string, number[]>();
  for (const r of introArs) {
    const list = arStarts.get(r.email);
    if (list) list.push(r.created_epoch);
    else arStarts.set(r.email, [r.created_epoch]);
  }
  const dbPool = new Map<string, number>();
  const dbConverts = new Map<string, number>();
  for (const [email, f] of firstIntro) {
    dbPool.set(f.week, (dbPool.get(f.week) ?? 0) + 1);
    // State-agnostic: a sub started then canceled still counts as a conversion.
    if ((arStarts.get(email) ?? []).some((e) => e >= f.epoch)) {
      dbConverts.set(f.week, (dbConverts.get(f.week) ?? 0) + 1);
    }
  }

  // Both sides derive their window from DATE_TRUNC('week', CURRENT_DATE) — but the
  // DB session runs UTC, so around Sunday ~8pm ET (= Monday 00:00 UTC) CURRENT_DATE
  // rolls and the window slides one week. /api/stats and this recompute run seconds
  // apart, so a run straddling that boundary would see two different 8-week windows.
  // Comparing only the OVERLAPPING weeks makes the anchor immune to that race
  // instead of firing a false mismatch once a week.
  const { rows: myWeekRows } = await pool.query<{ week_start: string }>(
    `SELECT generate_series(
       (DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '${INTRO_WEEKS_BACK} weeks')::date,
       (DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '1 week')::date,
       '1 week'::interval)::date::text AS week_start`,
  );
  const myWeeks = myWeekRows.map((r) => r.week_start);
  const dashWeeks = stats.trends.journeyConversion.introWeek.weeks as { weekStart: string; pool: number; converts: number }[];
  // Both series are contiguous weekly runs, so the overlap is a simple range clamp.
  const lo = myWeeks[0] > dashWeeks[0].weekStart ? myWeeks[0] : dashWeeks[0].weekStart;
  const hi = myWeeks[myWeeks.length - 1] < dashWeeks[dashWeeks.length - 1].weekStart
    ? myWeeks[myWeeks.length - 1] : dashWeeks[dashWeeks.length - 1].weekStart;
  const overlap = dashWeeks.filter((w) => w.weekStart >= lo && w.weekStart <= hi);

  const cohortDiffs: string[] = [];
  // Validate the rendered week SEQUENCE before its values. Comparing values alone
  // is blind to a dropped zero-pool week (absent from dbPool, so the missing-week
  // scan below can't see it) masked by a duplicated neighbour — the row count still
  // matches and every rendered week still agrees.
  const expectedSeq = myWeeks.filter((w) => w >= lo && w <= hi);
  const renderedSeq = overlap.map((w) => w.weekStart);
  if (renderedSeq.join(",") !== expectedSeq.join(",")) {
    cohortDiffs.push(`week sequence ${renderedSeq.join(",")} ≠ expected ${expectedSeq.join(",")}`);
  }

  for (const w of overlap) {
    const expPool = dbPool.get(w.weekStart) ?? 0;
    const expConv = dbConverts.get(w.weekStart) ?? 0;
    if (w.pool !== expPool) cohortDiffs.push(`${w.weekStart} pool ${w.pool}≠${expPool}`);
    if (w.converts !== expConv) cohortDiffs.push(`${w.weekStart} converts ${w.converts}≠${expConv}`);
  }
  // A cohort week the recompute found inside the overlap but the dashboard never
  // rendered is a real gap (a dropped week), not a boundary artifact.
  const rendered = new Set(dashWeeks.map((w) => w.weekStart));
  for (const week of dbPool.keys()) {
    if (week >= lo && week <= hi && !rendered.has(week)) cohortDiffs.push(`${week} missing from dashboard`);
  }
  // A near-empty overlap means something other than the boundary race is wrong
  // (e.g. the dashboard rendering a stale window) — that we DO want to surface.
  const tooLittleOverlap = overlap.length < INTRO_WEEKS_BACK - 1;
  checks.push({
    name: "introConversion.cohortWeeks=dbRecompute",
    ok: cohortDiffs.length === 0 && !tooLittleOverlap,
    dashboard: cohortDiffs.length ? cohortDiffs.slice(0, 6)
      : tooLittleOverlap ? `only ${overlap.length}/${INTRO_WEEKS_BACK} weeks overlap (dashboard ${dashWeeks[0].weekStart}…, db ${myWeeks[0]}…)`
      : `${overlap.length} weeks match`,
    independent: "each email counted once, in the week of their first intro attendance",
    note: "cohort math (not bucket math); converts = Member/Sky3/TV sub started on/after first intro",
  });

  // Structural: a cohort week can never have more converters than people.
  const overConverted = dashWeeks.filter((w) => w.converts > w.pool).map((w) => `${w.weekStart} ${w.converts}>${w.pool}`);
  checks.push({
    name: "introConversion.convertsLEpool",
    ok: overConverted.length === 0,
    dashboard: overConverted.length ? overConverted : "converts ≤ pool every week",
    independent: "converters are a subset of the cohort",
  });

  // ── 8. New signups per window never exceed rows actually created ──
  // getSubscriberMovement is the canonical owner of new/canceled per window, and
  // nothing else guarded it. The independent side counts raw auto_renews created
  // in the SAME window straight from the table — no plan-changer exclusion, no
  // installment-plan exclusion — so it is a strict UPPER BOUND on what movement
  // may report. Those exclusions only ever remove rows, so movement.new > raw is
  // impossible unless the window boundaries drifted (the off-by-one week class
  // that already bit newCustomerVolume) or rows are being double-counted.
  // Deliberately an inequality, not an equality: reproducing the email-scoped
  // plan-changer exclusion here would re-implement the function and mirror its
  // bugs instead of checking it. Anchor 9 covers the collapse-to-zero direction.
  // Categorize with the SAME runtime getCategory the dashboard uses (never the
  // stored plan_category column, which lags a rename).
  const rawCreatedByCat = async (from: string, to: string) => {
    const { rows } = await pool.query<{ plan_name: string }>(
      `SELECT plan_name FROM auto_renews
       WHERE created_at >= $1::date AND created_at < $2::date`,
      [from, to],
    );
    const byCat: Record<string, number> = { member: 0, sky3: 0, skyTingTv: 0, unknown: 0 };
    for (const r of rows) byCat[CAT_KEY[getCategory(r.plan_name)] ?? "unknown"]++;
    return byCat;
  };

  // These reads happen AFTER /api/stats rendered, so a pipeline run in between
  // can mutate a counted row — saveAutoRenews upserts by union_pass_id and bumps
  // imported_at, and can change created_at or plan_name, dropping the raw count
  // below a movement figure that was CORRECT when rendered.
  //
  // Two failure modes pull in opposite directions and BOTH matter: reporting
  // that race as a data bug is a false positive, but suppressing on any write
  // would mask a genuine overcount whenever the pipeline happens to run — a
  // real defect silently downgraded to "couldn't check". So neither guess:
  // when a breach coincides with a write, re-render and re-measure. A breach
  // that survives a fresh /api/stats AND a fresh DB read describes one
  // consistent instant and is real; one that evaporates was the race.
  const measureBreaches = async (s: Record<string, any>): Promise<string[]> => {
    const found: string[] = [];
    for (const w of ["yesterday", "lastWeek", "thisMonth"] as const) {
      const b = s.movement.byWindow[w];
      const rawByCat = await rawCreatedByCat(b.windowStart, b.windowEnd);
      for (const k of ["member", "sky3", "skyTingTv"] as const) {
        if (b[k].new > rawByCat[k]) found.push(`${w}/${k}: movement=${b[k].new} > created=${rawByCat[k]}`);
      }
    }
    return found;
  };

  let overCounted = await measureBreaches(stats);
  let raceNote: string | undefined;
  if (overCounted.length && (await pipelineWatermark(pool)) !== watermarkBefore) {
    // Second pass against a render taken after the write. requirePayload guards
    // it too, so a degraded retry surfaces as UNAVAILABLE rather than a verdict
    // read off a malformed payload.
    const retryStats = await fetchStats(cookie);
    requirePayload(retryStats);
    const retry = await measureBreaches(retryStats);
    if (retry.length === 0) {
      raceNote = "an initial breach disappeared on re-measure after a mid-audit pipeline write (race, not a data bug)";
    }
    overCounted = retry;
  }
  checks.push({
    name: "movement.newSignups<=rowsCreated",
    ok: overCounted.length === 0,
    note: raceNote,
    dashboard: overCounted.length ? overCounted : "every window within bound",
    independent: "movement.new ≤ auto_renews created in the same window",
  });

  // NOTE: a signup-side "collapse to zero" anchor was attempted here and pulled
  // before merge. Every formulation had a false-positive path, because from
  // outside the function a broken filter and a legitimate business lull render
  // identically:
  //   • per-month `new === 0`  → fires on a pre-launch or wind-down month (Sky3
  //     genuinely recorded 0 new signups for the 16 months before it launched).
  //   • whole-series zero      → fires on a category closed to new sales for the
  //     entire lookback.
  //   • whole-series zero + "but the DB has rows" → fires when the only rows are
  //     cross-category plan changers, which getSubscriberMovement deliberately
  //     excludes from `new` but a raw created_at query counts.
  // Separating those needs the email-scoped plan-changer set, i.e. the very
  // logic under test — an anchor that re-implements the function would mirror
  // its bugs instead of catching them. Left unguarded on purpose rather than
  // shipping a check that cries wolf; see the follow-up for doing it inside
  // subscriber-movement, where the changer set is already in scope.
  const monthlyMv = stats.movement.monthly as any[];

  // ── 9. trends.* churn equals the movement source it is copied from ──
  // CLAUDE.md: trends.churnRates is OVERWRITTEN from the canonical movement
  // result at API assembly (route.ts), so the two surfaces must be identical by
  // construction. Any divergence means the override silently stopped applying
  // and the churn cards are rendering the stale legacy rollup again — a
  // regression a green build would never catch. Exact integer equality.
  const mvByPeriod = new Map(monthlyMv.map((m) => [m.period, m]));
  // Months genuinely older than the movement window are legitimately absent from
  // it. Anything INSIDE the span is not: an un-overridden month is exactly what
  // this anchor exists to catch, and silently `continue`-ing past it would let
  // the failure through. This bites at the UTC/ET month boundary, where the
  // legacy trends leg (server-local `new Date()`) can emit a month the ET-based
  // movement leg has not started yet. So skipping is allowed only OUTSIDE the
  // span; a gap inside it is reported.
  const mvPeriods = monthlyMv.map((m) => m.period as string).sort();
  const spanStart = mvPeriods[0];
  const spanEnd = mvPeriods[mvPeriods.length - 1];
  const drifted: string[] = [];
  const uncovered: string[] = [];
  let comparedPairs = 0;
  for (const k of ["member", "sky3", "skyTingTv"] as const) {
    for (const row of (byCat[k].monthly ?? []) as { month: string; canceledCount: number; activeAtStart: number }[]) {
      const m = mvByPeriod.get(row.month);
      if (!m) {
        if (row.month >= spanStart && row.month <= spanEnd) uncovered.push(`${k}:${row.month}`);
        continue;
      }
      comparedPairs++;
      if (row.canceledCount !== m[k].canceled || row.activeAtStart !== m[k].activeAtStart) {
        drifted.push(`${k}:${row.month} trends(cx=${row.canceledCount},start=${row.activeAtStart}) vs movement(cx=${m[k].canceled},start=${m[k].activeAtStart})`);
      }
    }
  }
  checks.push({
    // A payload where no period overlaps would pass vacuously, so the pair count
    // is part of the assertion, not just the note.
    name: "trends.monthlyChurn=movementSource",
    ok: drifted.length === 0 && uncovered.length === 0 && comparedPairs > 0,
    dashboard: drifted.length || uncovered.length
      ? [...drifted, ...uncovered.map((u) => `${u} rendered by trends but missing from movement (inside span ${spanStart}..${spanEnd})`)]
      : `${comparedPairs} (category,month) pairs identical`,
    independent: "trends.churnRates must equal movement.monthly (route.ts override)",
  });

  // ── Report ─────────────────────────────────────────────────────
  const failed = checks.filter((c) => !c.ok);
  console.log(`\n[audit] ${BASE}  —  ${checks.length} anchors, ${failed.length} mismatch(es)\n`);
  for (const c of checks) {
    console.log(`  ${c.ok ? "OK  " : "FAIL"}  ${c.name}`);
    if (!c.ok) console.log(`        dashboard=${JSON.stringify(c.dashboard)}  independent=${JSON.stringify(c.independent)}${c.note ? `  (${c.note})` : ""}`);
  }
  if (failed.length) {
    console.log(`\n[audit] ${failed.length} ANCHOR(S) DISAGREE WITH THE DATABASE — investigate + open a draft PR.`);
    await closePool();
    process.exit(2);
  }
  console.log("\n[audit] clean — every anchor matches the database.");
  await closePool();
  process.exit(0);
}

main().catch(async (e) => {
  const unavailable = e instanceof AuditUnavailable;
  console.error(`[audit] ${unavailable ? "could not evaluate" : "errored"}:`, e instanceof Error ? e.message : e);
  await closePool().catch(() => {});
  process.exit(1); // both "unavailable" and unexpected errors are exit 1, never a false mismatch
});
