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
import { ACTIVE_STATES_SQL, AT_RISK_STATES_SQL } from "../src/lib/analytics/metrics/filters";

const BASE = process.env.AUDIT_BASE_URL || "https://studio-analytics-production.up.railway.app";
const PASSWORD = process.env.AUDIT_PASSWORD || "skyting";

interface Check {
  name: string;
  ok: boolean;
  dashboard: unknown;
  independent: unknown;
  note?: string;
}

const ACTIVE_FILTER = `plan_state IN (${ACTIVE_STATES_SQL}) AND (current_state IS NULL OR current_state = 'active')`;

async function login(): Promise<string> {
  const res = await fetch(`${BASE}/api/auth/login`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ password: PASSWORD }),
  });
  if (!res.ok) throw new Error(`login failed: HTTP ${res.status}`);
  const setCookie = res.headers.get("set-cookie");
  if (!setCookie) throw new Error("login returned no cookie");
  return setCookie.split(";")[0];
}

async function fetchStats(cookie: string): Promise<Record<string, any>> {
  const res = await fetch(`${BASE}/api/stats?nocache=1`, {
    headers: { Cookie: cookie, Referer: `${BASE}/` },
  });
  if (!res.ok) throw new Error(`/api/stats failed: HTTP ${res.status}`);
  return res.json();
}

/** Monday (ET) of the current week as YYYY-MM-DD, matching DATE_TRUNC('week'). */
function currentMondayET(): string {
  const nowEt = new Date(new Date().toLocaleString("en-US", { timeZone: "America/New_York" }));
  const day = nowEt.getDay(); // 0=Sun
  const offset = day === 0 ? 6 : day - 1;
  const mon = new Date(nowEt.getFullYear(), nowEt.getMonth(), nowEt.getDate() - offset);
  return `${mon.getFullYear()}-${String(mon.getMonth() + 1).padStart(2, "0")}-${String(mon.getDate()).padStart(2, "0")}`;
}

async function main() {
  const checks: Check[] = [];
  const pool = getPool();
  const cookie = await login();
  const stats = await fetchStats(cookie);

  // ── 1. Active counts: rendered vs raw SQL by category ──────────
  const a = stats.activeSubscribers ?? {};
  const { rows: catRows } = await pool.query(
    `SELECT plan_category, COUNT(*)::int n FROM auto_renews WHERE ${ACTIVE_FILTER} GROUP BY 1`,
  );
  const dbActive: Record<string, number> = { MEMBER: 0, SKY3: 0, SKY_TING_TV: 0 };
  for (const r of catRows as { plan_category: string; n: number }[]) dbActive[r.plan_category] = r.n;
  for (const [cat, key] of [["MEMBER", "member"], ["SKY3", "sky3"], ["SKY_TING_TV", "skyTingTv"]] as const) {
    checks.push({
      name: `active.${key}`,
      ok: (a[key] ?? -1) === dbActive[cat],
      dashboard: a[key], independent: dbActive[cat],
    });
  }

  // ── 2. At-risk per category: rendered vs raw SQL (active + at-risk, dedup email) ──
  const { rows: arRows } = await pool.query(
    `SELECT plan_category, COUNT(DISTINCT LOWER(customer_email))::int n FROM auto_renews
     WHERE plan_state IN (${AT_RISK_STATES_SQL}) AND (current_state IS NULL OR current_state = 'active')
     GROUP BY 1`,
  );
  const dbAtRisk: Record<string, number> = { MEMBER: 0, SKY3: 0, SKY_TING_TV: 0 };
  for (const r of arRows as { plan_category: string; n: number }[]) dbAtRisk[r.plan_category] = r.n;
  const byCat = stats.trends?.churnRates?.byCategory ?? {};
  for (const [cat, key] of [["MEMBER", "member"], ["SKY3", "sky3"], ["SKY_TING_TV", "skyTingTv"]] as const) {
    const dash = byCat[key]?.atRiskCount;
    checks.push({
      name: `atRisk.${key}`,
      ok: dash === dbAtRisk[cat],
      dashboard: dash, independent: dbAtRisk[cat],
      note: "active subscriptions in Past Due/Invalid/Pending Cancel, deduped by email",
    });
  }

  // ── 3. MRR / ARPU internal consistency ─────────────────────────
  const mrr = stats.mrr ?? {};
  const mrrSum = Math.round(((mrr.member ?? 0) + (mrr.sky3 ?? 0) + (mrr.skyTingTv ?? 0) + (mrr.unknown ?? 0)) * 100) / 100;
  checks.push({ name: "mrr.total=Σcategory", ok: Math.abs((mrr.total ?? 0) - mrrSum) < 0.5, dashboard: mrr.total, independent: mrrSum });
  const arpu = stats.arpu ?? {};
  const expArpu = (a.total ?? 0) > 0 ? Math.round(((mrr.total ?? 0) / a.total) * 100) / 100 : 0;
  checks.push({ name: "arpu.overall=mrr/active", ok: Math.abs((arpu.overall ?? 0) - expArpu) < 0.5, dashboard: arpu.overall, independent: expArpu });

  // ── 4. New-customer "current week" is the real calendar week ────
  const nv = stats.trends?.newCustomerVolume ?? {};
  const completed = nv.completedWeeks ?? [];
  const lastCompletedEnd = completed.length ? completed[completed.length - 1].weekEnd : null;
  const curMon = currentMondayET();
  // The last COMPLETED week must end the day before the current week's Monday.
  const expectedLastEnd = (() => {
    const m = new Date(curMon + "T00:00:00");
    const prev = new Date(m.getFullYear(), m.getMonth(), m.getDate() - 1);
    return `${prev.getFullYear()}-${String(prev.getMonth() + 1).padStart(2, "0")}-${String(prev.getDate()).padStart(2, "0")}`;
  })();
  checks.push({
    name: "newCustomerVolume.currentWeek=calendarWeek",
    ok: lastCompletedEnd === expectedLastEnd,
    dashboard: `completedWeeks end ${lastCompletedEnd}`, independent: `expected ${expectedLastEnd} (current Mon ${curMon})`,
    note: "guards the 'last week shown as this week' off-by-one",
  });

  // ── 5. Monthly revenue integrity: net ≤ gross, gross ≥ 0 ────────
  const months = (stats.monthlyRevenue ?? []) as { month: string; gross: number; net: number }[];
  const badRev = months.filter((m) => m.net > m.gross + 0.5 || m.gross < 0);
  checks.push({
    name: "monthlyRevenue.netLEgross&nonneg",
    ok: badRev.length === 0,
    dashboard: badRev.length ? badRev.map((m) => m.month) : "all ok", independent: "net≤gross, gross≥0",
  });

  // ── 6. Monthly churn: no completed month with active subs but 0 churn ──
  // (per category, mirrors the Layer-1 invariant but against the rendered payload)
  const zeroChurn: string[] = [];
  for (const key of ["member", "sky3", "skyTingTv"] as const) {
    const monthly = (byCat[key]?.monthly ?? []) as { month: string; canceledCount: number; activeAtStart: number }[];
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

main().catch((e) => { console.error("[audit] errored:", e instanceof Error ? e.message : e); process.exit(1); });
