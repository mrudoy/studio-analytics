/**
 * One-shot sync: upsert all rows from a Union.fit "active auto renews" CSV,
 * then reconcile subscription-level (mark DB-active rows not in the CSV as
 * Canceled). Use this when the dashboard's row counts overshoot Union after
 * the daily delta missed plan-change cancellations.
 *
 * Usage:
 *   npx tsx scripts/sync-auto-renews-from-csv.ts <csv-path>             # dry run
 *   npx tsx scripts/sync-auto-renews-from-csv.ts <csv-path> --apply     # write
 *
 * Equivalent to /api/upload?type=subscriber_sync but runs as a script.
 * MUST be a full export, not a daily delta.
 */
import { readFileSync } from "fs";
import Papa from "papaparse";
import { saveAutoRenews, reconcileAutoRenewsFromExport, type AutoRenewRow } from "../src/lib/db/auto-renew-store";

interface CsvRow {
  subscription_name: string;
  subscription_state: string;
  subscription_price: string;
  customer_name: string;
  customer_email: string;
  created_at: string;
  order_id: string;
  sales_channel: string;
  canceled_at: string;
  canceled_by: string;
  admin: string;
  current_state: string;
  current_subscription: string;
}

const ACTIVE_STATES = new Set([
  "Valid Now",
  "Paused",
  "Pending Cancel",
  "In Trial",
  "Invalid",
  "Past Due",
]);

async function main() {
  const csvPath = process.argv[2];
  const apply = process.argv.includes("--apply");
  if (!csvPath) {
    console.error("Usage: npx tsx scripts/sync-auto-renews-from-csv.ts <csv-path> [--apply]");
    process.exit(1);
  }

  const csv = readFileSync(csvPath, "utf8");
  const parsed = Papa.parse<CsvRow>(csv, { header: true, skipEmptyLines: true });
  if (parsed.errors.length > 0) {
    console.warn(`[sync] CSV had ${parsed.errors.length} parse errors; first:`, parsed.errors[0]);
  }
  console.log(`[sync] Parsed ${parsed.data.length} CSV rows`);

  // Build the AutoRenewRow[] for saveAutoRenews
  const arRows: AutoRenewRow[] = parsed.data
    .filter((r) => r.customer_email && r.subscription_name)
    .map((r) => ({
      planName: r.subscription_name,
      planState: r.subscription_state,
      planPrice: parseFloat(r.subscription_price) || 0,
      customerName: r.customer_name,
      customerEmail: r.customer_email,
      createdAt: r.created_at || "",
      orderId: r.order_id || undefined,
      salesChannel: r.sales_channel || undefined,
      canceledAt: r.canceled_at || undefined,
      canceledBy: r.canceled_by || undefined,
      admin: r.admin || undefined,
      currentState: r.current_state || undefined,
      currentPlan: r.current_subscription || undefined,
    }));

  // Build active subscription tuple set (for row-level reconciliation)
  const activeTuples = new Set<string>();
  let csvMaxCreatedMs = 0;
  for (const r of parsed.data) {
    const ms = Date.parse(r.created_at || "");
    if (!Number.isNaN(ms) && ms > csvMaxCreatedMs) csvMaxCreatedMs = ms;
  }
  for (const r of parsed.data) {
    if (!ACTIVE_STATES.has(r.subscription_state)) continue;
    const cs = (r.current_state || "").trim();
    if (cs !== "" && cs !== "active") continue;
    const email = (r.customer_email || "").toLowerCase().trim();
    const plan = (r.subscription_name || "").trim();
    const dateMatch = (r.created_at || "").match(/^(\d{4}-\d{2}-\d{2})/);
    const date = dateMatch ? dateMatch[1] : "";
    if (!email || !plan || !date) continue;
    activeTuples.add(`${email}|${plan}|${date}`);
  }
  console.log(`[sync] Active subscription tuples (CSV): ${activeTuples.size}`);

  if (!apply) {
    console.log("\n[sync] DRY RUN — re-run with --apply to write.");
    console.log(`  Would upsert ${arRows.length} rows via saveAutoRenews()`);
    console.log(`  Would reconcile against ${activeTuples.size} active tuples`);
    return;
  }

  // Step 1: upsert
  const snapshotId = `sync-${Date.now()}`;
  console.log(`\n[sync] Step 1: upserting ${arRows.length} rows (snapshot: ${snapshotId})...`);
  await saveAutoRenews(snapshotId, arRows);

  // Step 2: row-level reconciliation
  console.log(`\n[sync] Step 2: row-level reconciliation...`);
  const result = await reconcileAutoRenewsFromExport(activeTuples, csvMaxCreatedMs);
  console.log(`[sync] Reconciled ${result.reconciled} stale rows`);

  console.log("\n[sync] Done. Hit /api/stats?nocache=1 to refresh dashboard.");
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
