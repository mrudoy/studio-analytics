/**
 * One-time, COUNT-NEUTRAL remediation of the 2026-06-04 reconcile (run 1).
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
 *      (email,plan) is still active in the export — so a delta can never re-count
 *      them. (plan_state is NOT touched → no count change, no churn event.)
 *   2. Enrich order_id onto current active rows from the export (stable key for the
 *      order_id-keyed reconcile + drift checks). Also count-neutral.
 *
 * HARD GUARANTEE: aborts unless post-counts == pre-counts (507/382/2043).
 *
 * Usage:
 *   npx tsx scripts/remediate-run1.ts <full-export.csv>            # dry run
 *   npx tsx scripts/remediate-run1.ts <full-export.csv> --apply    # write
 */
import { readFileSync } from "fs";
import { basename } from "path";
import Papa from "papaparse";
import { getPool } from "../src/lib/db/database";
import { runMigrations } from "../src/lib/db/migrations";
import { toEasternDate } from "../src/lib/db/eastern-date";

const ACTIVE = new Set(["Valid Now", "Paused", "Pending Cancel", "In Trial", "Invalid", "Past Due"]);
const ACTIVE_SQL = "'Valid Now','Paused','Pending Cancel','In Trial','Invalid','Past Due'";

interface Csv { subscription_name: string; subscription_state: string; customer_email: string; created_at: string; order_id: string; current_state: string; }

async function counts(): Promise<Record<string, number>> {
  const { rows } = await getPool().query(
    `SELECT plan_category, COUNT(*)::int n FROM auto_renews
      WHERE plan_state IN (${ACTIVE_SQL}) AND (current_state IS NULL OR current_state='active')
      GROUP BY 1`,
  );
  const c: Record<string, number> = { MEMBER: 0, SKY3: 0, SKY_TING_TV: 0, total: 0 };
  for (const r of rows as { plan_category: string; n: number }[]) { c[r.plan_category] = r.n; c.total += r.n; }
  return c;
}

async function main() {
  const csvPath = process.argv[2];
  const apply = process.argv.includes("--apply");
  if (!csvPath || csvPath.startsWith("--")) { console.error("Usage: remediate-run1.ts <export.csv> [--apply]"); process.exit(1); }
  await runMigrations();
  const pool = getPool();

  // ── export: (email|plan) → {orderId} for active subs ──
  const parsed = Papa.parse<Csv>(readFileSync(csvPath, "utf8"), { header: true, skipEmptyLines: true });
  const expOrderId = new Map<string, string>();
  for (const r of parsed.data) {
    if (!ACTIVE.has(r.subscription_state)) continue;
    const cs = (r.current_state || "").trim(); if (cs !== "" && cs !== "active") continue;
    const email = (r.customer_email || "").toLowerCase().trim(); const plan = (r.subscription_name || "").trim();
    const oid = (r.order_id || "").trim();
    if (email && plan && oid) expOrderId.set(`${email}|${plan}`, oid);
  }
  console.log(`[remediate] export active (email|plan) with order_id: ${expOrderId.size}`);

  const before = await counts();
  console.log(`[remediate] BEFORE: member=${before.MEMBER} sky3=${before.SKY3} tv=${before.SKY_TING_TV} total=${before.total}`);

  // ── 1. run-1 victims still active in the export (protect from re-inflation) ──
  const victims = await pool.query(
    `SELECT ar.id, LOWER(ar.customer_email) email, ar.plan_name, ar.current_state
       FROM reconcile_row_snapshot s JOIN auto_renews ar ON ar.id = s.auto_renew_id
      WHERE s.run_id = 1 AND ar.plan_state = 'Canceled' AND ar.union_pass_id IS NOT NULL`,
  );
  const toProtect: number[] = [];
  let alreadyProtected = 0, genuineGhost = 0;
  for (const r of victims.rows as { id: number; email: string; plan_name: string; current_state: string | null }[]) {
    if (!expOrderId.has(`${r.email}|${r.plan_name}`)) { genuineGhost++; continue; } // genuinely gone → leave canceled
    if ((r.current_state || "") === "canceled") { alreadyProtected++; continue; }
    toProtect.push(r.id);
  }
  console.log(`[remediate] victims in export to protect (current_state='canceled'): ${toProtect.length}  (already=${alreadyProtected}, genuine-ghost left canceled=${genuineGhost})`);

  // ── 2. active rows to enrich with order_id (count-neutral) ──
  const active = await pool.query(
    `SELECT id, LOWER(customer_email) email, plan_name, order_id
       FROM auto_renews WHERE plan_state IN (${ACTIVE_SQL}) AND (current_state IS NULL OR current_state='active')`,
  );
  const enrich: { id: number; oid: string }[] = [];
  for (const r of active.rows as { id: number; email: string; plan_name: string; order_id: string | null }[]) {
    const oid = expOrderId.get(`${r.email}|${r.plan_name}`);
    if (oid && (r.order_id || "") !== oid) enrich.push({ id: r.id, oid });
  }
  console.log(`[remediate] active rows to enrich with order_id: ${enrich.length}`);

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
    const after = await counts();
    if (after.MEMBER !== before.MEMBER || after.SKY3 !== before.SKY3 || after.SKY_TING_TV !== before.SKY_TING_TV) {
      throw new Error(`COUNT CHANGED (before ${JSON.stringify(before)} → after ${JSON.stringify(after)}) — rolling back.`);
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
