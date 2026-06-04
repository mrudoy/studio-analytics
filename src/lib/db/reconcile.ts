/**
 * Full-export reconciliation — the hardened, auditable path that re-aligns the
 * `auto_renews` table to a fresh, COMPLETE Union active-subscriptions export.
 *
 * Why this exists (see the plan + CLAUDE.md): the daily Union export is a delta
 * and the pipeline never reconciles, so canceled subscriptions that don't
 * reappear in a delta accumulate as "ghost" active rows. The only safe way to
 * clear them is to compare against a full export and cancel the DB-active rows
 * that are absent from it.
 *
 * Cancelling hundreds of rows is a logically destructive write, so this module
 * is built like a destructive migration:
 *   - READ-ONLY diff (4 quadrants) you can inspect before any write.
 *   - Hard preflight gates that ABORT (not warn) on anything suspicious.
 *   - One transaction: upsert (re-activates wrongly-canceled) → snapshot → cancel.
 *   - Row snapshot + audit row (reconcile_runs) → rollback(runId) restores state.
 *   - Churn-trigger suppression (SET LOCAL app.reconcile) so the cancel pass logs
 *     'reconcile_churn' (excluded from churn metrics), not a fake churn spike.
 *
 * Reconciliation identity: the export carries no union_pass_id, so a DB-active
 * row is considered "present in the export" iff its tuple
 *   `LOWER(email) | plan_name | created_date(YYYY-MM-DD)`
 * appears in the export's active set. The match-rate gate guards against a
 * date-format/key mismatch silently turning everyone into a cancel candidate.
 */
import type { Pool, PoolClient } from "pg";
import { getPool } from "./database";
import { ACTIVE_STATES, ACTIVE_STATES_SQL } from "../analytics/metrics/filters";
import { getCategory } from "../analytics/categories";
import { upsertAutoRenewRowsTx, AUTO_RENEW_WRITE_LOCK, type AutoRenewRow } from "./auto-renew-store";

type Queryable = Pool | PoolClient;

// Canonical active states (CLAUDE.md: state strings live only in filters.ts).
// Kept in lockstep with the DB-side ACTIVE_STATES_SQL used in queryDbActiveRows.
const ACTIVE_CSV_STATES = new Set<string>(ACTIVE_STATES);

export interface CategoryCounts {
  member: number;
  sky3: number;
  skyTingTv: number;
  unknown: number;
  total: number;
}

function emptyCounts(): CategoryCounts {
  return { member: 0, sky3: 0, skyTingTv: 0, unknown: 0, total: 0 };
}

function bump(c: CategoryCounts, cat: string, n = 1): void {
  const key =
    cat === "MEMBER" ? "member"
    : cat === "SKY3" ? "sky3"
    : cat === "SKY_TING_TV" ? "skyTingTv"
    : "unknown";
  c[key] += n;
  c.total += n;
}

function tupleKey(email: string, plan: string, createdDate: string): string {
  return `${email.toLowerCase().trim()}|${plan.trim()}|${createdDate}`;
}

/** Date portion (YYYY-MM-DD) of a created_at value, or "" if unparseable. */
function dateKey(created: string | null | undefined): string {
  if (!created) return "";
  const m = String(created).match(/^(\d{4}-\d{2}-\d{2})/);
  if (m) return m[1];
  const ms = Date.parse(String(created));
  if (Number.isNaN(ms)) return "";
  return new Date(ms).toISOString().slice(0, 10);
}

// ── Export-side helpers ──────────────────────────────────────

export interface ParsedExport {
  rows: AutoRenewRow[];
  /** Active subscription tuples present in the export. */
  activeTuples: Set<string>;
  /** Per-category counts of the active rows in the export. */
  counts: CategoryCounts;
  /** Active export rows whose plan maps to UNKNOWN (possible new unmapped plans). */
  unknownActive: { plan: string; email: string }[];
  /** Latest created_at (ms) seen in the export. */
  maxCreatedMs: number;
}

/**
 * Derive the active tuple set + category counts from parsed export rows.
 * An export row counts as active iff its state is an ACTIVE state and its
 * current_state (if present) is "" or "active".
 */
export function summarizeExport(rows: AutoRenewRow[]): ParsedExport {
  const activeTuples = new Set<string>();
  const counts = emptyCounts();
  const unknownActive: { plan: string; email: string }[] = [];
  let maxCreatedMs = 0;

  for (const r of rows) {
    const ms = Date.parse(r.createdAt || "");
    if (!Number.isNaN(ms) && ms > maxCreatedMs) maxCreatedMs = ms;
  }

  for (const r of rows) {
    if (!ACTIVE_CSV_STATES.has(r.planState)) continue;
    const cs = (r.currentState || "").trim();
    if (cs !== "" && cs !== "active") continue;
    const email = (r.customerEmail || "").toLowerCase().trim();
    const plan = (r.planName || "").trim();
    const date = dateKey(r.createdAt);
    if (!email || !plan || !date) continue;

    activeTuples.add(tupleKey(email, plan, date));
    const cat = getCategory(plan);
    bump(counts, cat);
    if (cat === "UNKNOWN") unknownActive.push({ plan, email });
  }

  return { rows, activeTuples, counts, unknownActive, maxCreatedMs };
}

// ── DB-side helpers ──────────────────────────────────────────

interface DbActiveRow {
  id: number;
  email: string;
  planName: string;
  category: string;
  createdDate: string;
  createdMs: number;
  importedMs: number;
}

async function queryDbActiveRows(q: Queryable): Promise<DbActiveRow[]> {
  const { rows } = await q.query(
    `SELECT id, LOWER(customer_email) AS email, plan_name, plan_category,
            TO_CHAR(created_at, 'YYYY-MM-DD') AS created_date,
            created_at, imported_at
     FROM auto_renews
     WHERE plan_state IN (${ACTIVE_STATES_SQL})
       AND (current_state IS NULL OR current_state = 'active')`,
  );
  return (rows as Record<string, unknown>[]).map((r) => ({
    id: r.id as number,
    email: (r.email as string) || "",
    planName: (r.plan_name as string) || "",
    category: (r.plan_category as string) || getCategory((r.plan_name as string) || ""),
    createdDate: (r.created_date as string) || "",
    createdMs: r.created_at ? new Date(r.created_at as string).getTime() : 0,
    importedMs: r.imported_at ? new Date(r.imported_at as string).getTime() : 0,
  }));
}

function countActive(rows: DbActiveRow[]): CategoryCounts {
  const c = emptyCounts();
  for (const r of rows) bump(c, r.category);
  return c;
}

// ── Diff (read-only) ─────────────────────────────────────────

export interface ReconcileDiff {
  dbCounts: CategoryCounts;
  exportCounts: CategoryCounts;
  /** (a) DB-active rows absent from the export, created on/before cutoff → cancel candidates. */
  candidates: { id: number; email: string; planName: string; category: string }[];
  candidateCounts: CategoryCounts;
  /** (b) Export-active tuples not currently active in DB → upsert will (re)activate these. */
  exportOnlyActiveCount: number;
  /** (c) DB-active rows absent from export but protected (created/imported after cutoff). */
  protectedCount: number;
  /** (d) Active export rows whose plan maps to UNKNOWN. */
  unknownActiveCount: number;
  /** Fraction of DB-active rows found in the export. */
  matchRate: number;
}

/**
 * Compute the 4-quadrant diff between the DB and a full export. READ-ONLY.
 * `cutoffMs` is the report's source cutoff — rows created OR imported after it
 * are protected (they changed after the snapshot the export represents).
 */
export async function computeReconcileDiff(
  exp: ParsedExport,
  cutoffMs: number,
  q: Queryable = getPool(),
): Promise<ReconcileDiff> {
  const dbRows = await queryDbActiveRows(q);
  const dbActiveTuples = new Set<string>();
  for (const r of dbRows) dbActiveTuples.add(tupleKey(r.email, r.planName, r.createdDate));

  const candidates: ReconcileDiff["candidates"] = [];
  const candidateCounts = emptyCounts();
  let matched = 0;
  let protectedCount = 0;

  for (const r of dbRows) {
    const tuple = tupleKey(r.email, r.planName, r.createdDate);
    if (exp.activeTuples.has(tuple)) {
      matched++;
      continue;
    }
    // Absent from export. Protect rows that changed after the report cutoff.
    if ((r.createdMs && r.createdMs > cutoffMs) || (r.importedMs && r.importedMs > cutoffMs)) {
      protectedCount++;
      continue;
    }
    candidates.push({ id: r.id, email: r.email, planName: r.planName, category: r.category });
    bump(candidateCounts, r.category);
  }

  let exportOnlyActiveCount = 0;
  for (const t of exp.activeTuples) if (!dbActiveTuples.has(t)) exportOnlyActiveCount++;

  return {
    dbCounts: countActive(dbRows),
    exportCounts: exp.counts,
    candidates,
    candidateCounts,
    exportOnlyActiveCount,
    protectedCount,
    unknownActiveCount: exp.unknownActive.length,
    matchRate: dbRows.length === 0 ? 1 : matched / dbRows.length,
  };
}

// ── Preflight gates ──────────────────────────────────────────

export interface PreflightOptions {
  /** Authoritative per-category active counts the export MUST match (from the summary report). */
  expectedCounts?: { member: number; sky3: number; skyTingTv: number };
  /** Min fraction of DB-active rows that must be found in the export. Default 0.80. */
  minMatchRate?: number;
  /** Abort if cancel candidates exceed this, unless `force`. Default 600. */
  maxCandidates?: number;
  /** Abort if more than this many active export rows map to UNKNOWN. Default 10. */
  maxUnknownActive?: number;
  /** Number of CSV parse failures (caller-supplied). Any > 0 aborts. */
  parseFailures?: number;
  /** Override the candidate-count gate (explicit large-reconcile approval). */
  force?: boolean;
}

export interface PreflightResult {
  ok: boolean;
  failures: string[];
  warnings: string[];
}

export function runPreflight(
  exp: ParsedExport,
  diff: ReconcileDiff,
  opts: PreflightOptions = {},
): PreflightResult {
  const failures: string[] = [];
  const warnings: string[] = [];
  const minMatchRate = opts.minMatchRate ?? 0.8;
  const maxCandidates = opts.maxCandidates ?? 600;
  const maxUnknownActive = opts.maxUnknownActive ?? 10;

  if ((opts.parseFailures ?? 0) > 0) {
    failures.push(`CSV had ${opts.parseFailures} parse failures (must be 0).`);
  }
  if (exp.activeTuples.size === 0) {
    failures.push("Export contains zero active subscriptions — refusing to reconcile (would cancel everyone).");
  }

  // Per-category counts must match the authoritative summary. This is the
  // primary guard against an export that filters states too narrowly (e.g.
  // omits Invalid/Past Due) — which would otherwise mass-cancel valid rows.
  if (opts.expectedCounts) {
    const e = opts.expectedCounts;
    const got = exp.counts;
    const mism: string[] = [];
    if (got.member !== e.member) mism.push(`member ${got.member}≠${e.member}`);
    if (got.sky3 !== e.sky3) mism.push(`sky3 ${got.sky3}≠${e.sky3}`);
    if (got.skyTingTv !== e.skyTingTv) mism.push(`skyTingTv ${got.skyTingTv}≠${e.skyTingTv}`);
    if (mism.length) {
      failures.push(
        `Export per-category active counts do not match the authoritative summary (${mism.join(", ")}). ` +
        `The export is likely filtering states too narrowly (e.g. excluding Invalid/Past Due). Aborting.`,
      );
    }
  } else {
    warnings.push("No expectedCounts provided — skipping the authoritative per-category count gate.");
  }

  if (diff.matchRate < minMatchRate) {
    failures.push(
      `Only ${(diff.matchRate * 100).toFixed(1)}% of DB-active rows matched the export ` +
      `(min ${(minMatchRate * 100).toFixed(0)}%). Likely a created_at format / key mismatch — aborting before mass false-cancel.`,
    );
  }

  if (exp.unknownActive.length > maxUnknownActive) {
    failures.push(
      `${exp.unknownActive.length} active export rows map to UNKNOWN category (max ${maxUnknownActive}). ` +
      `Likely new unmapped plan names — add them to categories.ts before reconciling. Examples: ` +
      exp.unknownActive.slice(0, 5).map((u) => u.plan).join(", "),
    );
  }

  if (diff.candidates.length > maxCandidates) {
    const msg =
      `Cancel candidates (${diff.candidates.length}) exceed the safety threshold (${maxCandidates}).`;
    if (opts.force) warnings.push(msg + " Proceeding due to --force.");
    else failures.push(msg + " Re-run with --force to approve a large reconcile.");
  }

  return { ok: failures.length === 0, failures, warnings };
}

// ── Apply (transactional) + rollback ─────────────────────────

export interface ReconcileApplyOptions {
  source?: { filename?: string; hash?: string };
  /**
   * Report source cutoff (ms). Rows created/imported after this are protected.
   * Defaults to the latest created_at in the export (best available proxy when
   * the caller has no explicit report-generation timestamp).
   */
  cutoffMs?: number;
  expectedCounts?: { member: number; sky3: number; skyTingTv: number };
  preflight?: PreflightOptions;
}

export interface ReconcileApplyResult {
  runId: number;
  applied: number;
  preCounts: CategoryCounts;
  postCounts: CategoryCounts;
  diff: ReconcileDiff;
}

/**
 * Transactionally re-align the DB to a full export. Runs all preflight gates
 * first and THROWS (no write) if any fail. On success: upserts every export
 * row, snapshots the rows it will cancel, suppresses the churn trigger, cancels
 * the absent rows, and records a reconcile_runs audit row. Rollback via
 * rollbackReconcile(runId).
 */
export async function reconcileFromFullExport(
  rows: AutoRenewRow[],
  opts: ReconcileApplyOptions,
): Promise<ReconcileApplyResult> {
  const pool = getPool();
  const exp = summarizeExport(rows);
  const cutoffMs = opts.cutoffMs ?? exp.maxCreatedMs;
  const preDiff = await computeReconcileDiff(exp, cutoffMs, pool);

  const pre = runPreflight(exp, preDiff, { ...opts.preflight, expectedCounts: opts.expectedCounts });
  if (!pre.ok) {
    await recordAbortedRun(pool, { ...opts, cutoffMs }, exp, preDiff, pre.failures.join(" | "));
    throw new Error("[reconcile] Preflight failed — aborted:\n  - " + pre.failures.join("\n  - "));
  }

  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    // Serialize against a concurrent daily-pipeline write (Phase 0.5).
    await client.query("SELECT pg_advisory_xact_lock($1)", [AUTO_RENEW_WRITE_LOCK]);
    // Silence the movement log for the ENTIRE reconcile (upsert + cancel). A
    // reconcile is a bulk correction, not member movement — migration 023's
    // trigger skips all event logging while this flag is on. Audit lives in
    // reconcile_runs + reconcile_row_snapshot.
    await client.query("SET LOCAL app.reconcile = 'on'");

    const runIns = await client.query(
      `INSERT INTO reconcile_runs
         (source_filename, source_hash, report_cutoff, parsed_rows, parse_failures,
          export_counts, db_pre_counts, candidate_count, status)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'pending') RETURNING id`,
      [
        opts.source?.filename ?? null,
        opts.source?.hash ?? null,
        cutoffMs ? new Date(cutoffMs).toISOString() : null,
        rows.length,
        opts.preflight?.parseFailures ?? 0,
        JSON.stringify(exp.counts),
        JSON.stringify(preDiff.dbCounts),
        preDiff.candidates.length,
      ],
    );
    const runId = runIns.rows[0].id as number;

    // 1. Upsert all export rows (re-activates any wrongly-canceled rows).
    //    app.reconcile='on' (set above) silences the trigger for this ENTIRE
    //    transaction — both this upsert and the cancel pass emit no movement
    //    events (migration 023). Audit lives in reconcile_runs + reconcile_row_snapshot.
    const snapshotId = `sync-${runId}-${cutoffMs || Date.now()}`;
    await upsertAutoRenewRowsTx(client, snapshotId, rows);

    // 2. Recompute candidates against post-upsert state (within the txn).
    const diff = await computeReconcileDiff(exp, cutoffMs, client);
    const candidateIds = diff.candidates.map((c) => c.id);

    let applied = 0;
    if (candidateIds.length > 0) {
      // Snapshot prior state of the rows we will cancel — the rollback source.
      await client.query(
        `INSERT INTO reconcile_row_snapshot (run_id, auto_renew_id, prev_plan_state, prev_canceled_at)
         SELECT $1, id, plan_state, canceled_at FROM auto_renews WHERE id = ANY($2)`,
        [runId, candidateIds],
      );

      // app.reconcile is already 'on' for the whole transaction → this cancel
      // (and the upsert above) emit no movement events.
      const upd = await client.query(
        `UPDATE auto_renews
           SET plan_state = 'Canceled', canceled_at = COALESCE(canceled_at, NOW())
         WHERE id = ANY($1)`,
        [candidateIds],
      );
      applied = upd.rowCount ?? 0;
    }

    const postCounts = countActive(await queryDbActiveRows(client));

    await client.query(
      `UPDATE reconcile_runs
         SET status='applied', applied_count=$2, protected_count=$3,
             db_post_counts=$4, finished_at=NOW()
       WHERE id=$1`,
      [runId, applied, diff.protectedCount, JSON.stringify(postCounts)],
    );

    await client.query("COMMIT");
    console.log(
      `[reconcile] run ${runId}: canceled ${applied} ghost rows. ` +
      `post-counts member=${postCounts.member} sky3=${postCounts.sky3} tv=${postCounts.skyTingTv}`,
    );
    return { runId, applied, preCounts: preDiff.dbCounts, postCounts, diff };
  } catch (e) {
    await client.query("ROLLBACK");
    throw e;
  } finally {
    client.release();
  }
}

async function recordAbortedRun(
  pool: Pool,
  opts: ReconcileApplyOptions,
  exp: ParsedExport,
  diff: ReconcileDiff,
  reason: string,
): Promise<void> {
  try {
    await pool.query(
      `INSERT INTO reconcile_runs
         (source_filename, source_hash, report_cutoff, parsed_rows, parse_failures,
          export_counts, db_pre_counts, candidate_count, status, notes, finished_at)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'aborted',$9,NOW())`,
      [
        opts.source?.filename ?? null,
        opts.source?.hash ?? null,
        opts.cutoffMs ? new Date(opts.cutoffMs).toISOString() : null,
        exp.rows.length,
        opts.preflight?.parseFailures ?? 0,
        JSON.stringify(exp.counts),
        JSON.stringify(diff.dbCounts),
        diff.candidates.length,
        reason.slice(0, 2000),
      ],
    );
  } catch {
    /* audit is best-effort; never mask the real abort reason */
  }
}

// ── Monitoring (Phase B2) ────────────────────────────────────

export interface ReconcileHealth {
  lastAppliedAt: string | null;
  daysAgo: number | null;
  /** OK ≤7d, WARNING 8–10d, OVERDUE >10d, NEVER if no successful run. */
  status: "OK" | "WARNING" | "OVERDUE" | "NEVER";
  applied: number | null;
}

/**
 * Health of the recurring full reconciliation, for the digest overdue alert.
 * Reads only successfully-APPLIED runs — a failed/aborted run never makes the
 * system look healthy (watermark-on-success).
 */
export async function getReconcileHealth(q: Queryable = getPool()): Promise<ReconcileHealth> {
  const { rows } = await q.query(
    `SELECT finished_at, applied_count FROM reconcile_runs
      WHERE status = 'applied' AND finished_at IS NOT NULL
      ORDER BY finished_at DESC LIMIT 1`,
  );
  if (rows.length === 0) return { lastAppliedAt: null, daysAgo: null, status: "NEVER", applied: null };
  const last = new Date(rows[0].finished_at as string);
  const daysAgo = Math.floor((Date.now() - last.getTime()) / 86_400_000);
  const status = daysAgo <= 7 ? "OK" : daysAgo <= 10 ? "WARNING" : "OVERDUE";
  return { lastAppliedAt: last.toISOString(), daysAgo, status, applied: Number(rows[0].applied_count) };
}

/**
 * Restore every row a reconcile run canceled back to its pre-run state, and
 * remove the run's reconcile_churn events. Idempotent: a second call is a no-op
 * once the run is marked rolled_back.
 */
export async function rollbackReconcile(runId: number): Promise<{ restored: number }> {
  const pool = getPool();
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    const snap = await client.query(
      `SELECT auto_renew_id, prev_plan_state, prev_canceled_at
         FROM reconcile_row_snapshot WHERE run_id = $1`,
      [runId],
    );
    let restored = 0;
    // Suppress the trigger here too — restoring 'Canceled'→active would otherwise
    // log spurious 'resume' events.
    await client.query("SET LOCAL app.reconcile = 'on'");
    for (const r of snap.rows as Record<string, unknown>[]) {
      const upd = await client.query(
        `UPDATE auto_renews SET plan_state=$2, canceled_at=$3 WHERE id=$1`,
        [r.auto_renew_id, r.prev_plan_state, r.prev_canceled_at],
      );
      restored += upd.rowCount ?? 0;
    }
    await client.query("SET LOCAL app.reconcile = 'off'");
    // Drop the reconcile_churn events this run created.
    await client.query(
      `DELETE FROM auto_renew_events
         WHERE event_type = 'reconcile_churn'
           AND auto_renew_id IN (SELECT auto_renew_id FROM reconcile_row_snapshot WHERE run_id = $1)`,
      [runId],
    );
    await client.query(`UPDATE reconcile_runs SET status='rolled_back', finished_at=NOW() WHERE id=$1`, [runId]);
    await client.query("COMMIT");
    console.log(`[reconcile] run ${runId} rolled back — restored ${restored} rows`);
    return { restored };
  } catch (e) {
    await client.query("ROLLBACK");
    throw e;
  } finally {
    client.release();
  }
}
