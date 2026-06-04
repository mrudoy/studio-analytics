/**
 * Hardened full-export reconciliation CLI (the canonical operational path — NOT
 * the UI upload, which is a convenience path only).
 *
 * Re-aligns the auto_renews table to a fresh, COMPLETE Union active-subscriptions
 * export. Reconciliation cancels DB-active rows absent from the export, which is
 * a logically destructive write — so this script:
 *   - DRY-RUNS by default: prints the 4-quadrant DB-vs-Union diff + preflight
 *     gate results, and writes NOTHING.
 *   - On --apply: runs all gates again inside reconcileFromFullExport(), which
 *     ABORTS (no write) on any gate failure, snapshots every row it cancels,
 *     suppresses the churn trigger, and records an auditable reconcile_runs row.
 *   - --rollback <runId> restores a prior run.
 *
 * Usage:
 *   npx tsx scripts/sync-auto-renews-from-csv.ts <csv> --expect 506,382,2043           # dry run
 *   npx tsx scripts/sync-auto-renews-from-csv.ts <csv> --expect 506,382,2043 --apply   # write
 *   npx tsx scripts/sync-auto-renews-from-csv.ts <csv> --expect 506,382,2043 --apply --force
 *   npx tsx scripts/sync-auto-renews-from-csv.ts --rollback 7
 *
 * Flags:
 *   --expect m,s,tv   Authoritative active counts (member,sky3,tv) from the Union
 *                     subscriptions summary. Enforced as a hard gate. STRONGLY recommended.
 *   --cutoff <iso>    Report generation time (rows created/imported after it are protected).
 *                     Defaults to the CSV file's mtime.
 *   --apply           Write changes (default is dry run).
 *   --force           Approve a reconcile whose cancel count exceeds the safety threshold.
 *   --rollback <id>   Roll back a prior reconcile run and exit.
 *
 * MUST be a FULL export (every active subscription), never a daily delta.
 */
import { readFileSync, statSync } from "fs";
import { createHash } from "crypto";
import { basename } from "path";
import Papa from "papaparse";
import type { AutoRenewRow } from "../src/lib/db/auto-renew-store";
import { runMigrations } from "../src/lib/db/migrations";
import {
  summarizeExport,
  computeReconcileDiff,
  runPreflight,
  reconcileFromFullExport,
  rollbackReconcile,
  type CategoryCounts,
} from "../src/lib/db/reconcile";

// ── Flexible header → AutoRenewRow adapter ───────────────────
// Union has shipped a few CSV shapes (per-subscriber "active auto renews" list,
// the older subscriber_sync export). Normalize headers to lowercase and map by
// alias so the same script handles either.
const ALIASES: Record<keyof AutoRenewRow, string[]> = {
  planName: ["subscription_name", "subscription", "name", "plan", "plan_name"],
  planState: ["subscription_state", "state", "status"],
  planPrice: ["subscription_price", "price"],
  customerName: ["customer_name", "customer"],
  customerEmail: ["customer_email", "email"],
  createdAt: ["created_at", "created", "created_date"],
  orderId: ["order_id", "order"],
  salesChannel: ["sales_channel", "channel"],
  canceledAt: ["canceled_at", "canceledat", "cancelled_at"],
  canceledBy: ["canceled_by", "cancelledby"],
  admin: ["admin"],
  currentState: ["current_state", "currentstate"],
  currentPlan: ["current_subscription", "current_plan"],
  unionPassId: ["union_pass_id", "pass_id"],
};

function pick(row: Record<string, string>, field: keyof AutoRenewRow): string {
  for (const alias of ALIASES[field]) {
    if (alias in row && row[alias] != null && row[alias] !== "") return row[alias];
  }
  return "";
}

function adaptRows(parsed: Record<string, string>[]): { rows: AutoRenewRow[]; failures: number } {
  let failures = 0;
  const rows: AutoRenewRow[] = [];
  for (const raw of parsed) {
    // lowercase keys
    const r: Record<string, string> = {};
    for (const k of Object.keys(raw)) r[k.toLowerCase().trim()] = raw[k];

    const planName = pick(r, "planName");
    const customerEmail = pick(r, "customerEmail");
    const planState = pick(r, "planState");
    if (!planName || !customerEmail || !planState) {
      failures++;
      continue;
    }
    rows.push({
      planName,
      planState,
      planPrice: parseFloat(pick(r, "planPrice")) || 0,
      customerName: pick(r, "customerName"),
      customerEmail,
      createdAt: pick(r, "createdAt"),
      orderId: pick(r, "orderId") || undefined,
      salesChannel: pick(r, "salesChannel") || undefined,
      canceledAt: pick(r, "canceledAt") || undefined,
      canceledBy: pick(r, "canceledBy") || undefined,
      admin: pick(r, "admin") || undefined,
      currentState: pick(r, "currentState") || undefined,
      currentPlan: pick(r, "currentPlan") || undefined,
      unionPassId: pick(r, "unionPassId") || undefined,
    });
  }
  return { rows, failures };
}

function fmt(c: CategoryCounts): string {
  return `member=${c.member} sky3=${c.sky3} tv=${c.skyTingTv} unknown=${c.unknown} (total ${c.total})`;
}

function parseExpect(arg?: string): { member: number; sky3: number; skyTingTv: number } | undefined {
  if (!arg) return undefined;
  const [m, s, tv] = arg.split(",").map((x) => parseInt(x.trim(), 10));
  if ([m, s, tv].some((n) => Number.isNaN(n))) return undefined;
  return { member: m, sky3: s, skyTingTv: tv };
}

function arg(name: string): string | undefined {
  const i = process.argv.indexOf(name);
  return i >= 0 ? process.argv[i + 1] : undefined;
}

async function main() {
  // Rollback path
  const rb = arg("--rollback");
  if (rb) {
    const runId = parseInt(rb, 10);
    if (Number.isNaN(runId)) throw new Error("--rollback needs a numeric run id");
    await runMigrations(); // rollback touches the audit tables
    const res = await rollbackReconcile(runId);
    console.log(`[sync] Rolled back run ${runId} — restored ${res.restored} rows.`);
    return;
  }

  const csvPath = process.argv[2];
  const apply = process.argv.includes("--apply");
  const force = process.argv.includes("--force");
  const expected = parseExpect(arg("--expect"));
  if (!csvPath || csvPath.startsWith("--")) {
    console.error("Usage: npx tsx scripts/sync-auto-renews-from-csv.ts <csv> --expect m,s,tv [--apply] [--force] [--cutoff <iso>]");
    process.exit(1);
  }

  const csv = readFileSync(csvPath, "utf8");
  const hash = createHash("sha256").update(csv).digest("hex");
  const cutoffArg = arg("--cutoff");
  const cutoffMs = cutoffArg ? Date.parse(cutoffArg) : statSync(csvPath).mtimeMs;
  if (Number.isNaN(cutoffMs)) throw new Error(`--cutoff not a valid date: ${cutoffArg}`);

  const parsed = Papa.parse<Record<string, string>>(csv, { header: true, skipEmptyLines: true });
  const { rows, failures } = adaptRows(parsed.data);
  const parseFailures = parsed.errors.length + failures;

  console.log(`[sync] File: ${basename(csvPath)}  sha256=${hash.slice(0, 12)}…`);
  console.log(`[sync] Parsed ${rows.length} rows (${parseFailures} parse/adapt failures)`);
  console.log(`[sync] Report cutoff: ${new Date(cutoffMs).toISOString()}${cutoffArg ? "" : " (from file mtime)"}`);
  if (!expected) console.warn("[sync] WARNING: no --expect m,s,tv given — the authoritative per-category count gate will be SKIPPED.");

  const exp = summarizeExport(rows);
  const diff = await computeReconcileDiff(exp, cutoffMs);
  const preflight = runPreflight(exp, diff, { expectedCounts: expected, parseFailures, force });

  console.log("\n── DB vs Union diff ──────────────────────────────");
  console.log(`  DB active now:     ${fmt(diff.dbCounts)}`);
  console.log(`  Export active:     ${fmt(diff.exportCounts)}`);
  console.log(`  (a) cancel candidates (DB-active absent from export): ${diff.candidates.length}  ${fmt(diff.candidateCounts)}`);
  console.log(`  (b) export-only active (upsert will (re)activate):    ${diff.exportOnlyActiveCount}`);
  console.log(`  (c) protected (changed after cutoff):                 ${diff.protectedCount}`);
  console.log(`  (d) export rows mapping to UNKNOWN:                   ${diff.unknownActiveCount}`);
  console.log(`  match rate (DB-active found in export):               ${(diff.matchRate * 100).toFixed(1)}%`);

  console.log("\n── Preflight gates ───────────────────────────────");
  for (const w of preflight.warnings) console.log(`  ⚠︎  ${w}`);
  for (const f of preflight.failures) console.log(`  ✗  ${f}`);
  console.log(preflight.ok ? "  ✓  all gates pass" : "  ✗  GATES FAILED — apply will abort");

  if (!apply) {
    console.log("\n[sync] DRY RUN — no writes. Re-run with --apply (and --expect) to reconcile.");
    return;
  }

  console.log("\n[sync] Applying reconciliation…");
  await runMigrations(); // ensure reconcile audit tables + trigger exist before writing
  const result = await reconcileFromFullExport(rows, {
    source: { filename: basename(csvPath), hash },
    cutoffMs,
    expectedCounts: expected,
    preflight: { parseFailures, force },
  });
  console.log(`[sync] Done. run ${result.runId} canceled ${result.applied} ghost rows.`);
  console.log(`[sync]   before: ${fmt(result.preCounts)}`);
  console.log(`[sync]   after:  ${fmt(result.postCounts)}`);
  console.log(`[sync] Rollback with: npx tsx scripts/sync-auto-renews-from-csv.ts --rollback ${result.runId}`);
  console.log("[sync] Refresh dashboard via /api/stats?nocache=1.");
}

main().catch((e) => {
  console.error(e instanceof Error ? e.message : e);
  process.exit(1);
});
