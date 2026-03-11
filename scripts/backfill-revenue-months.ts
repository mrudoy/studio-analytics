/**
 * Backfill missing revenue months from the full export.
 *
 * Computes revenue by category using the actual TypeScript pipeline code,
 * then saves ONLY the specified months to the database.
 *
 * Usage:
 *   npx tsx scripts/backfill-revenue-months.ts [months...] [--export-dir path]
 *
 * Examples:
 *   npx tsx scripts/backfill-revenue-months.ts 2026-01 2026-02
 *   npx tsx scripts/backfill-revenue-months.ts 2026-01 2026-02 --export-dir ~/Downloads/export
 *   npx tsx scripts/backfill-revenue-months.ts --all  # backfill ALL months from export
 */

import { config } from "dotenv";
import { resolve, join } from "path";
import { readdirSync } from "fs";
import { parseCSV } from "../src/lib/parser/csv-parser";
import {
  RawMembershipSchema,
  RawPassSchema,
  RawOrderSchema,
  RawPerformanceSchema,
  RawEventSchema,
  RawLocationSchema,
  RawPassTypeSchema,
  RawRevenueCategoryLookupSchema,
  RawRefundSchema,
  RawTransferSchema,
  type RawMembership,
  type RawPass,
  type RawOrder,
  type RawPerformance,
  type RawEvent,
  type RawLocation,
  type RawPassType,
  type RawRevenueCategoryLookup,
  type RawRefund,
  type RawTransfer,
} from "../src/lib/email/zip-schemas";
import { ZipTransformer, type RawZipTables } from "../src/lib/email/zip-transformer";
import { saveRevenueCategories, isMonthLocked } from "../src/lib/db/revenue-store";
import { getPool } from "../src/lib/db/database";

// Load env for DATABASE_URL
config({ path: resolve(process.cwd(), ".env.production.local") });
config({ path: resolve(process.cwd(), ".env.local") });

function parseTable<T>(
  filePath: string | undefined,
  schema: { parse: (v: unknown) => T },
  tableName: string
): T[] {
  if (!filePath) {
    console.warn(`Missing table: ${tableName}`);
    return [];
  }
  const result = parseCSV<T>(filePath, schema as never);
  if (result.warnings.length > 0) {
    console.warn(`${tableName}: ${result.warnings.length} parse warnings`);
  }
  console.log(`${tableName}: ${result.data.length} rows`);
  return result.data;
}

async function main() {
  // Parse CLI args
  const args = process.argv.slice(2);
  let exportDir = resolve(
    process.env.HOME || "~",
    "Downloads/union_data_export-sky-ting-20230830-20260223-zFoHME8GZ2ESfu8LzvDe27NU"
  );
  let targetMonths: string[] = [];
  let backfillAll = false;

  for (let i = 0; i < args.length; i++) {
    if (args[i] === "--export-dir" && args[i + 1]) {
      exportDir = resolve(args[i + 1]);
      i++;
    } else if (args[i] === "--all") {
      backfillAll = true;
    } else if (/^\d{4}-\d{2}$/.test(args[i])) {
      targetMonths.push(args[i]);
    }
  }

  if (!backfillAll && targetMonths.length === 0) {
    console.error("Usage: npx tsx scripts/backfill-revenue-months.ts 2026-01 2026-02 [--export-dir path]");
    console.error("   or: npx tsx scripts/backfill-revenue-months.ts --all [--export-dir path]");
    process.exit(1);
  }

  console.log(`Export dir: ${exportDir}`);
  console.log(`Target: ${backfillAll ? "ALL months" : targetMonths.join(", ")}`);

  // Build file map
  const files = readdirSync(exportDir).filter((f) => f.endsWith(".csv"));
  const fileMap = new Map<string, string>();
  for (const f of files) {
    const name = f.toLowerCase().replace(/\.csv$/, "");
    fileMap.set(name, join(exportDir, f));
  }
  console.log(`CSV files found: ${files.length}`);

  // Parse tables
  const memberships = parseTable<RawMembership>(fileMap.get("memberships"), RawMembershipSchema, "memberships");
  const passes = parseTable<RawPass>(fileMap.get("passes"), RawPassSchema, "passes");
  const events = parseTable<RawEvent>(fileMap.get("events"), RawEventSchema, "events");
  const performances = parseTable<RawPerformance>(fileMap.get("performances"), RawPerformanceSchema, "performances");
  const locations = parseTable<RawLocation>(fileMap.get("locations"), RawLocationSchema, "locations");
  const passTypes = parseTable<RawPassType>(fileMap.get("pass_types"), RawPassTypeSchema, "pass_types");
  const revCatLookups = parseTable<RawRevenueCategoryLookup>(fileMap.get("revenue_categories"), RawRevenueCategoryLookupSchema, "revenue_categories");
  const refunds = parseTable<RawRefund>(fileMap.get("refunds"), RawRefundSchema, "refunds");
  const transfers = parseTable<RawTransfer>(fileMap.get("transfers"), RawTransferSchema, "transfers");
  const rawOrders = parseTable<RawOrder>(fileMap.get("orders"), RawOrderSchema, "orders");

  // Build transformer
  const tables: RawZipTables = {
    memberships,
    passes,
    events,
    performances,
    locations,
    passTypes,
    revenueCategoryLookups: revCatLookups,
  };
  const transformer = new ZipTransformer(tables);

  // Compute revenue by category
  console.log(`\nComputing revenue by category from ${rawOrders.length} orders, ${refunds.length} refunds, ${transfers.length} transfers...`);
  const monthlyRevenue = transformer.computeRevenueByCategory(rawOrders, refunds, transfers);

  console.log(`Months produced: ${monthlyRevenue.size}`);

  // Filter to target months
  const monthsToSave = backfillAll
    ? [...monthlyRevenue.keys()].sort()
    : targetMonths.filter((m) => monthlyRevenue.has(m));

  const missingTargets = targetMonths.filter((m) => !monthlyRevenue.has(m));
  if (missingTargets.length > 0) {
    console.warn(`\nWARNING: These target months have no data in the export: ${missingTargets.join(", ")}`);
  }

  console.log(`\nWill save ${monthsToSave.length} months:`);
  for (const month of monthsToSave) {
    const cats = monthlyRevenue.get(month)!;
    const totalRev = cats.reduce((s, c) => s + c.revenue, 0);
    console.log(`  ${month}: ${cats.length} categories, gross=$${totalRev.toLocaleString()}`);
  }

  // Confirm
  console.log(`\nSaving to database...`);

  let saved = 0;
  let skipped = 0;
  let failed = 0;

  for (const month of monthsToSave) {
    const categories = monthlyRevenue.get(month)!;
    const year = parseInt(month.slice(0, 4));
    const monthNum = parseInt(month.slice(5, 7));

    // Check lock
    const locked = await isMonthLocked(year, monthNum);
    if (locked) {
      console.log(`  ${month}: SKIPPED (locked)`);
      skipped++;
      continue;
    }

    // Build period
    const periodStart = `${month}-01`;
    const lastDay = new Date(year, monthNum, 0).getDate();
    const periodEnd = `${month}-${String(lastDay).padStart(2, "0")}`;

    try {
      await saveRevenueCategories(periodStart, periodEnd, categories);
      const totalRev = categories.reduce((s, c) => s + c.revenue, 0);
      console.log(`  ${month}: SAVED (${categories.length} categories, $${totalRev.toLocaleString()})`);
      saved++;
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      console.error(`  ${month}: FAILED — ${msg}`);
      failed++;
    }
  }

  console.log(`\nDone: ${saved} saved, ${skipped} skipped (locked), ${failed} failed`);

  const pool = getPool();
  await pool.end();
}

main().catch((err) => {
  console.error("Failed:", err);
  process.exit(1);
});
