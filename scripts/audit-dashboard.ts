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
 *   AUDIT_PASSWORD          dashboard login (default 'skyting')
 *
 * Usage: npx tsx scripts/audit-dashboard.ts
 */
import { getPool } from "../src/lib/db/database";
import { getCategory } from "../src/lib/analytics/categories";
import { ACTIVE_STATES, AT_RISK_STATES } from "../src/lib/analytics/metrics/filters";

const BASE = process.env.AUDIT_BASE_URL || "https://studio-analytics-production.up.railway.app";
const PASSWORD = process.env.AUDIT_PASSWORD || "skyting";

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
  const need: Array<[string, unknown]> = [
    ["activeSubscribers", stats.activeSubscribers],
    ["mrr", stats.mrr],
    ["arpu", stats.arpu],
    ["monthlyRevenue", stats.monthlyRevenue],
    ["trends.churnRates.byCategory", stats.trends?.churnRates?.byCategory],
    ["trends.newCustomerVolume.completedWeeks", stats.trends?.newCustomerVolume?.completedWeeks],
  ];
  const missing = need.filter(([, v]) => v == null).map(([k]) => k);
  const byCat = stats.trends?.churnRates?.byCategory;
  if (byCat) for (const k of ["member", "sky3", "skyTingTv"]) {
    if (!byCat[k] || byCat[k].atRiskCount == null || !Array.isArray(byCat[k].monthly)) missing.push(`byCategory.${k}`);
  }
  if (missing.length) throw new AuditUnavailable(`incomplete /api/stats payload (backend likely degraded): missing ${missing.join(", ")}`);
}

async function main() {
  const checks: Check[] = [];
  const pool = getPool();
  const cookie = await login();
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

  // ── 1. Active counts (rows, skip blank email — mirrors getActiveCounts) ──
  const dbActive = { member: 0, sky3: 0, skyTingTv: 0 };
  // ── 2. At-risk (active + AT_RISK_STATES, deduped by email — mirrors atRiskCount) ──
  const atRiskEmails = { member: new Set<string>(), sky3: new Set<string>(), skyTingTv: new Set<string>() };
  for (const r of arRows) {
    if (!r.email) continue;
    const key = CAT_KEY[getCategory(r.plan_name)];
    if (key === "unknown") continue; // unknown is its own bucket; the active.* anchors are the 3 real categories
    dbActive[key]++;
    if (AT_RISK.has(r.plan_state)) atRiskEmails[key].add(r.email);
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

  // ── Report ─────────────────────────────────────────────────────
  const failed = checks.filter((c) => !c.ok);
  console.log(`\n[audit] ${BASE}  —  ${checks.length} anchors, ${failed.length} mismatch(es)\n`);
  for (const c of checks) {
    console.log(`  ${c.ok ? "OK  " : "FAIL"}  ${c.name}`);
    if (!c.ok) console.log(`        dashboard=${JSON.stringify(c.dashboard)}  independent=${JSON.stringify(c.independent)}${c.note ? `  (${c.note})` : ""}`);
  }
  if (failed.length) {
    console.log(`\n[audit] ${failed.length} ANCHOR(S) DISAGREE WITH THE DATABASE — investigate + open a draft PR.`);
    process.exit(2);
  }
  console.log("\n[audit] clean — every anchor matches the database.");
  process.exit(0);
}

main().catch((e) => {
  const unavailable = e instanceof AuditUnavailable;
  console.error(`[audit] ${unavailable ? "could not evaluate" : "errored"}:`, e instanceof Error ? e.message : e);
  process.exit(1); // both "unavailable" and unexpected errors are exit 1, never a false mismatch
});
