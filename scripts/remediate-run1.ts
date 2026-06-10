/**
 * One-time, COUNT-NEUTRAL remediation of the 2026-06-04 reconcile (run 1).
 *
 * ⚠️  SUPERSEDED — DO NOT RUN. Kept for audit only. Requires the explicit
 *     `--i-understand-this-is-superseded` flag to execute at all. The correct,
 *     durable path is the audited full-export reconcile (`reconcileFromFullExport`
 *     via `scripts/sync-auto-renews-from-csv.ts`), which re-derives the correct
 *     order_id per row through its ON CONFLICT upsert and re-activates wrongly-
 *     canceled rows. See migration 025's superseded note + CLAUDE.md.
 *
 * Run 1 matched DB↔export on `email|plan|created_date`, but `created_at` was
 * stored +1 day (the UTC/DATE timezone bug). So ~285 ACTIVE subscribers looked
 * "missing from the export" and were canceled; the upsert re-added them under the
 * correct date. Net: the headline count is right today, but each canceled victim
 * still carries its `union_pass_id`, so the next daily delta would re-activate it
 * into a DUPLICATE → re-inflation.
 *
 * This script makes the current numbers STABLE without changing them:
 *   1. Protect victims: set current_state='canceled' on run-1 canceled rows whose
 *      subscription is still active in the export — so a delta can never re-count
 *      them. (plan_state is NOT touched → no count change, no churn event.)
 *   2. Enrich order_id onto current active rows from the export (stable key for the
 *      order_id-keyed reconcile + drift checks). Also count-neutral.
 *
 * SUBSCRIPTION IDENTITY: matching is keyed on the UNIQUE tuple
 * `email | plan | created_date(ET)`, NEVER `email|plan` alone. A person can hold
 * several active subscriptions of the same plan (each its own order_id); an
 * `email|plan` key collapses them and stamps ONE order_id across all of them —
 * the exact corruption that caused the 2026-06-06 under-count.
 *
 * DATE SKEW: the export side is normalized through toEasternDate(), but the DB
 * side reads the stored DATE column, which on rows not yet backfill-corrected
 * is +1 day (the original UTC/DATE bug). A stored-date-only match would treat
 * those rows as "not in the export". So each DB row is matched on TWO candidate
 * keys: its stored date and stored date − 1 (the known skew direction), with
 * global safety passes so a match is never guessed:
 *   - a row whose two candidates resolve to different order_ids is AMBIGUOUS → skipped;
 *   - an exact-date hit is distrusted when an ACTIVE row occupies that same
 *     tuple (the export sub at that date is the sibling's, not this row's);
 *   - an enrichment write is dropped if its order_id is already stored on a
 *     different active row, or if two rows resolve to the same order_id —
 *     a wrong stamp can therefore never create a duplicate-active-order_id.
 * Every skipped class is counted and logged.
 *
 * HARD GUARANTEE: aborts unless post-counts == pre-counts.
 *
 * Usage (guarded):
 *   npx tsx scripts/remediate-run1.ts <export.csv> --i-understand-this-is-superseded            # dry run
 *   npx tsx scripts/remediate-run1.ts <export.csv> --i-understand-this-is-superseded --apply    # write
 */
import { readFileSync } from "fs";
import { basename } from "path";
import Papa from "papaparse";
import type { Pool, PoolClient } from "pg";
import { getPool } from "../src/lib/db/database";
import { runMigrations } from "../src/lib/db/migrations";
import { toEasternDate } from "../src/lib/db/eastern-date";
import { ACTIVE_STATES, ACTIVE_STATES_SQL } from "../src/lib/analytics/metrics/filters";

type Queryable = Pool | PoolClient;
const ACTIVE = new Set<string>(ACTIVE_STATES);

// Unique subscription identity. NEVER key on `email|plan` alone — see header.
function subKey(email: string, plan: string, createdDate: string | null): string {
  return `${(email || "").toLowerCase().trim()}|${(plan || "").trim()}|${createdDate || ""}`;
}

interface Csv { subscription_name: string; subscription_state: string; customer_email: string; created_at: string; order_id: string; current_state: string; }

// Pass the open transaction client when verifying inside a transaction, so the
// count reflects this txn's uncommitted writes (a separate pool connection would
// read pre-commit state and the guard would never fire).
async function counts(q: Queryable = getPool()): Promise<Record<string, number>> {
  const { rows } = await q.query(
    `SELECT plan_category, COUNT(*)::int n FROM auto_renews
      WHERE plan_state IN (${ACTIVE_STATES_SQL}) AND (current_state IS NULL OR current_state='active')
      GROUP BY 1`,
  );
  const c: Record<string, number> = { MEMBER: 0, SKY3: 0, SKY_TING_TV: 0, total: 0 };
  for (const r of rows as { plan_category: string; n: number }[]) { c[r.plan_category] = r.n; c.total += r.n; }
  return c;
}

async function main() {
  const csvPath = process.argv[2];
  const apply = process.argv.includes("--apply");
  if (!csvPath || csvPath.startsWith("--")) { console.error("Usage: remediate-run1.ts <export.csv> --i-understand-this-is-superseded [--apply]"); process.exit(1); }
  if (!process.argv.includes("--i-understand-this-is-superseded")) {
    console.error(
      "[remediate] REFUSING TO RUN — this one-time script is SUPERSEDED by the audited\n" +
      "            full-export reconcile (scripts/sync-auto-renews-from-csv.ts). Running it\n" +
      "            again risks re-introducing the order_id corruption it was patched for.\n" +
      "            Pass --i-understand-this-is-superseded only if you are certain.",
    );
    process.exit(1);
  }
  await runMigrations();
  const pool = getPool();

  // ── export: (email|plan|createdDate) → orderId for active subs ──
  // Keyed on the UNIQUE subscription tuple. An `email|plan` key would collapse a
  // person's multiple same-plan subscriptions and stamp one order_id across all
  // of them (the corruption this script's earlier version caused).
  const parsed = Papa.parse<Csv>(readFileSync(csvPath, "utf8"), { header: true, skipEmptyLines: true });
  const expOrderId = new Map<string, string>();
  let tupleCollisions = 0;
  for (const r of parsed.data) {
    if (!ACTIVE.has(r.subscription_state)) continue;
    const cs = (r.current_state || "").trim(); if (cs !== "" && cs !== "active") continue;
    const email = (r.customer_email || "").toLowerCase().trim(); const plan = (r.subscription_name || "").trim();
    const oid = (r.order_id || "").trim();
    if (!email || !plan || !oid) continue;
    const key = subKey(email, plan, toEasternDate(r.created_at));
    if (expOrderId.has(key) && expOrderId.get(key) !== oid) tupleCollisions++;
    expOrderId.set(key, oid);
  }
  console.log(`[remediate] export active (email|plan|createdDate) with order_id: ${expOrderId.size}`);
  if (tupleCollisions > 0) {
    // The tuple is supposed to be unique per subscription. If it is not, the
    // enrichment cannot be trusted — abort rather than risk re-corrupting.
    console.error(`[remediate] ABORT: ${tupleCollisions} export rows collide on (email,plan,createdDate) with different order_ids — tuple is not unique; refusing to enrich.`);
    process.exit(1);
  }

  const before = await counts();
  console.log(`[remediate] BEFORE: member=${before.MEMBER} sky3=${before.SKY3} tv=${before.SKY_TING_TV} total=${before.total}`);

  // ── Load active rows first: they anchor both steps below ──
  const active = await pool.query(
    `SELECT id, LOWER(customer_email) email, plan_name, order_id,
            TO_CHAR(created_at, 'YYYY-MM-DD') AS created_date,
            TO_CHAR(created_at - 1, 'YYYY-MM-DD') AS created_date_minus1
       FROM auto_renews WHERE plan_state IN (${ACTIVE_STATES_SQL}) AND (current_state IS NULL OR current_state='active')`,
  );
  type ActiveRow = { id: number; email: string; plan_name: string; order_id: string | null; created_date: string | null; created_date_minus1: string | null };
  const activeRows = active.rows as ActiveRow[];
  // Tuple occupancy: which (email|plan|date) identities are held by an ACTIVE row.
  const activeTupleSet = new Set<string>();
  // order_id ownership among active rows: an oid already stored on an active row
  // is OWNED by it — no other row may be enriched to that oid (it would create a
  // duplicate-active-order_id, the exact corruption the drift check alerts on).
  const ownerByOid = new Map<string, number>();
  for (const r of activeRows) {
    activeTupleSet.add(subKey(r.email, r.plan_name, r.created_date));
    if (r.order_id) {
      if (ownerByOid.has(r.order_id) && ownerByOid.get(r.order_id) !== r.id) {
        console.warn(`[remediate] WARNING: order_id ${r.order_id} already active on >1 row — pre-existing corruption; drift check should be alerting.`);
      }
      ownerByOid.set(r.order_id, r.id);
    }
  }

  // ── 1. run-1 victims still active in the export (protect from re-inflation) ──
  // Two candidate dates per row: stored, and stored − 1 day (the known +1 skew
  // direction for rows whose created_at was never backfill-corrected).
  const victims = await pool.query(
    `SELECT ar.id, LOWER(ar.customer_email) email, ar.plan_name, ar.current_state,
            TO_CHAR(ar.created_at, 'YYYY-MM-DD') AS created_date,
            TO_CHAR(ar.created_at - 1, 'YYYY-MM-DD') AS created_date_minus1
       FROM reconcile_row_snapshot s JOIN auto_renews ar ON ar.id = s.auto_renew_id
      WHERE s.run_id = 1 AND ar.plan_state = 'Canceled' AND ar.union_pass_id IS NOT NULL`,
  );
  const toProtect: number[] = [];
  let alreadyProtected = 0, genuineGhost = 0, victimSkewMatched = 0, victimSiblingExplained = 0;
  for (const r of victims.rows as { id: number; email: string; plan_name: string; current_state: string | null; created_date: string | null; created_date_minus1: string | null }[]) {
    // Protection is an existence check ("is this victim's subscription still
    // active in the export?"). A skew hit (stored − 1) is the expected signal —
    // run-1 victims are the +1-skewed rows, and their re-added twin sits at the
    // corrected date. An EXACT-date hit is only trusted when no ACTIVE DB row
    // occupies that same (email|plan|date) tuple: if one does, the export sub at
    // that date is that sibling's, not the victim's (Codex round-2 finding) —
    // the victim's own sub is gone → genuine ghost, leave it alone.
    const skewHit = expOrderId.has(subKey(r.email, r.plan_name, r.created_date_minus1));
    const exactKey = subKey(r.email, r.plan_name, r.created_date);
    const exactHit = expOrderId.has(exactKey) && !activeTupleSet.has(exactKey);
    if (expOrderId.has(exactKey) && activeTupleSet.has(exactKey) && !skewHit) victimSiblingExplained++;
    if (!skewHit && !exactHit) { genuineGhost++; continue; } // genuinely gone → leave canceled
    if (skewHit && !expOrderId.has(exactKey)) victimSkewMatched++;
    if ((r.current_state || "") === "canceled") { alreadyProtected++; continue; }
    toProtect.push(r.id);
  }
  console.log(`[remediate] victims in export to protect (current_state='canceled'): ${toProtect.length}  (already=${alreadyProtected}, genuine-ghost left canceled=${genuineGhost}, matched via -1d skew=${victimSkewMatched}, exact hit explained by active sibling=${victimSiblingExplained})`);

  // ── 2. active rows to enrich with order_id (count-neutral) ──
  // Candidate resolution per row, then GLOBAL safety passes: a write is queued
  // only if (a) the row's two candidate dates don't disagree, (b) the resolved
  // oid isn't already owned by a DIFFERENT active row, and (c) no other row
  // resolved to the same oid this run. (b)+(c) make a wrong stamp structurally
  // unable to create a duplicate-active-order_id state — the harmful outcome.
  const claims = new Map<string, { id: number; cur: string | null }[]>();
  let enrichSkewMatched = 0, enrichAmbiguous = 0, enrichOwnedElsewhere = 0, enrichClaimCollision = 0;
  for (const r of activeRows) {
    const exactOid = expOrderId.get(subKey(r.email, r.plan_name, r.created_date));
    const skewOid = expOrderId.get(subKey(r.email, r.plan_name, r.created_date_minus1));
    if (exactOid && skewOid && exactOid !== skewOid) { enrichAmbiguous++; continue; } // never guess
    const oid = exactOid ?? skewOid;
    if (!oid || (r.order_id || "") === oid) continue; // no candidate, or already correct
    if (!exactOid && skewOid) enrichSkewMatched++;
    const owner = ownerByOid.get(oid);
    if (owner !== undefined && owner !== r.id) { enrichOwnedElsewhere++; continue; } // oid lives on another active row
    const list = claims.get(oid) || [];
    list.push({ id: r.id, cur: r.order_id });
    claims.set(oid, list);
  }
  const enrich: { id: number; oid: string }[] = [];
  for (const [oid, rows] of claims) {
    if (rows.length > 1) { enrichClaimCollision += rows.length; continue; } // two rows want one oid — skip both
    enrich.push({ id: rows[0].id, oid });
  }
  console.log(`[remediate] active rows to enrich with order_id: ${enrich.length} (matched via -1d skew=${enrichSkewMatched}, ambiguous=${enrichAmbiguous}, oid owned by another row=${enrichOwnedElsewhere}, claim collisions=${enrichClaimCollision} — all skipped, never guessed)`);

  if (!apply) {
    console.log("\n[remediate] DRY RUN — no writes. Neither op changes plan_state, so the active counts are unchanged by design. Re-run with --apply.");
    return;
  }

  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    await client.query("SELECT pg_advisory_xact_lock(472900)"); // serialize vs pipeline
    const run = await client.query(
      `INSERT INTO reconcile_runs (source_filename, db_pre_counts, status, notes)
       VALUES ($1,$2,'pending',$3) RETURNING id`,
      [basename(csvPath), JSON.stringify(before), `remediate-run1: protect ${toProtect.length} victims, enrich ${enrich.length} order_ids`],
    );
    const runId = run.rows[0].id as number;
    const preTx = await counts(client); // in-transaction baseline (sees committed state)
    // snapshot the rows we touch (current_state changes) for rollback
    if (toProtect.length) {
      await client.query(
        `INSERT INTO reconcile_row_snapshot (run_id, auto_renew_id, prev_plan_state, prev_canceled_at)
         SELECT $1, id, plan_state, canceled_at FROM auto_renews WHERE id = ANY($2)`,
        [runId, toProtect],
      );
      await client.query(`UPDATE auto_renews SET current_state='canceled' WHERE id = ANY($1)`, [toProtect]);
    }
    for (const e of enrich) {
      await client.query(`UPDATE auto_renews SET order_id=$2 WHERE id=$1`, [e.id, e.oid]);
    }
    // Verify THROUGH the same client so uncommitted writes are visible.
    const after = await counts(client);
    if (after.MEMBER !== preTx.MEMBER || after.SKY3 !== preTx.SKY3 || after.SKY_TING_TV !== preTx.SKY_TING_TV || after.total !== preTx.total) {
      throw new Error(`COUNT CHANGED (before ${JSON.stringify(preTx)} → after ${JSON.stringify(after)}) — rolling back.`);
    }
    await client.query(
      `UPDATE reconcile_runs SET status='applied', applied_count=$2, db_post_counts=$3, finished_at=NOW() WHERE id=$1`,
      [runId, toProtect.length, JSON.stringify(after)],
    );
    await client.query("COMMIT");
    console.log(`\n[remediate] APPLIED run ${runId}. AFTER: member=${after.MEMBER} sky3=${after.SKY3} tv=${after.SKY_TING_TV} total=${after.total} (unchanged ✓)`);
    console.log(`[remediate] Rollback: restore current_state from reconcile_row_snapshot run_id=${runId}.`);
  } catch (e) {
    await client.query("ROLLBACK");
    throw e;
  } finally {
    client.release();
  }
}

main().catch((e) => { console.error(e instanceof Error ? e.message : e); process.exit(1); });
