/**
 * Backfill revenue categories from a full Union.fit data export.
 *
 * Uses the 7-step algorithm from Union's official documentation to compute
 * monthly revenue by category from raw CSVs (orders, passes, pass_types,
 * refunds, transfers, revenue_categories).
 *
 * Usage:
 *   npx tsx scripts/backfill-revenue.ts [csv-directory]
 *
 * Options:
 *   --dry-run    Show computed revenue without saving to DB
 *   --unlock     Also overwrite locked months (manual uploads)
 *
 * Default directory:
 *   ~/Downloads/union_data_export-sky-ting-20230830-20260223-zFoHME8GZ2ESfu8LzvDe27NU/
 */

import dotenv from "dotenv";
import { resolve, join } from "path";

dotenv.config({ path: resolve(__dirname, "../.env.production.local") });
dotenv.config({ path: resolve(__dirname, "../.env.local") });
dotenv.config({ path: resolve(__dirname, "../.env") });

import { existsSync, readdirSync } from "fs";
import { ZipTransformer, type RawZipTables } from "../src/lib/email/zip-transformer";
import {
  RawMembershipSchema,
  RawPassSchema,
  RawOrderSchema,
  RawEventSchema,
  RawPerformanceSchema,
  RawLocationSchema,
  RawPassTypeSchema,
  RawRevenueCategoryLookupSchema,
  RawRefundSchema,
  RawTransferSchema,
  type RawMembership,
  type RawPass,
  type RawOrder,
  type RawEvent,
  type RawPerformance,
  type RawLocation,
  type RawPassType,
  type RawRevenueCategoryLookup,
  type RawRefund,
  type RawTransfer,
} from "../src/lib/email/zip-schemas";
import { parseCSV } from "../src/lib/parser/csv-parser";
import { initDatabase } from "../src/lib/db/database";
import { runMigrations } from "../src/lib/db/migrations";
import { saveRevenueCategories, isMonthLocked } from "../src/lib/db/revenue-store";
import { bumpDataVersion } from "../src/lib/cache/stats-cache";

const DEFAULT_CSV_DIR = resolve(
  process.env.HOME || "~",
  "Downloads/union_data_export-sky-ting-20230830-20260223-zFoHME8GZ2ESfu8LzvDe27NU"
);

function parseTable<T>(filePath: string | undefined, schema: { parse: (v: unknown) => T }, label: string): T[] {
  if (!filePath || !existsSync(filePath)) {
    console.log(`  [skip] ${label}: file not found`);
    return [];
  }
  const result = parseCSV<T>(filePath, schema as never);
  if (result.warnings.length > 0) {
    console.log(`  ${label}: ${result.data.length} rows (${result.warnings.length} warnings)`);
  } else {
    console.log(`  ${label}: ${result.data.length} rows`);
  }
  return result.data;
}

async function main() {
  const args = process.argv.slice(2);
  const dryRun = args.includes("--dry-run");
  const unlock = args.includes("--unlock");
  const csvDir = args.find((a) => !a.startsWith("--")) || DEFAULT_CSV_DIR;

  if (!existsSync(csvDir)) {
    console.error(`CSV directory not found: ${csvDir}`);
    process.exit(1);
  }

  console.log(`\n=== Revenue Categories Backfill ===`);
  console.log(`CSV directory: ${csvDir}`);
  console.log(`Mode: ${dryRun ? "DRY RUN (no DB writes)" : "LIVE"}`);
  console.log(`Locked months: ${unlock ? "will OVERWRITE" : "will SKIP"}`);
  console.log(`DATABASE_URL: ${process.env.DATABASE_URL ? "set" : "NOT SET"}\n`);

  if (!dryRun) {
    if (!process.env.DATABASE_URL) {
      console.error("DATABASE_URL is not set. Check your .env files.");
      process.exit(1);
    }
    console.log("Initializing database...");
    await initDatabase();
    await runMigrations();
    console.log("Database ready.\n");
  }

  // Build file map
  const files = readdirSync(csvDir).filter((f) => f.endsWith(".csv"));
  const fileMap = new Map<string, string>();
  for (const f of files) {
    const name = f.toLowerCase().replace(/\.csv$/, "");
    fileMap.set(name, join(csvDir, f));
  }

  console.log(`Found ${files.length} CSV files.\n`);
  console.log("Parsing tables:");

  // Parse all required tables
  const memberships = parseTable<RawMembership>(fileMap.get("memberships"), RawMembershipSchema, "memberships");
  const passes = parseTable<RawPass>(fileMap.get("passes"), RawPassSchema, "passes");
  const events = parseTable<RawEvent>(fileMap.get("events"), RawEventSchema, "events");
  const performances = parseTable<RawPerformance>(fileMap.get("performances"), RawPerformanceSchema, "performances");
  const locations = parseTable<RawLocation>(fileMap.get("locations"), RawLocationSchema, "locations");
  const passTypes = parseTable<RawPassType>(fileMap.get("pass_types"), RawPassTypeSchema, "pass_types");
  const revenueCategoryLookups = parseTable<RawRevenueCategoryLookup>(
    fileMap.get("revenue_categories"),
    RawRevenueCategoryLookupSchema,
    "revenue_categories (lookup)"
  );
  const orders = parseTable<RawOrder>(fileMap.get("orders"), RawOrderSchema, "orders");
  const refunds = parseTable<RawRefund>(fileMap.get("refunds"), RawRefundSchema, "refunds");
  const transfers = parseTable<RawTransfer>(fileMap.get("transfers"), RawTransferSchema, "transfers");

  // Build transformer
  console.log("\nBuilding transformer...");
  const tables: RawZipTables = {
    memberships,
    passes,
    events,
    performances,
    locations,
    passTypes,
    revenueCategoryLookups,
  };
  const transformer = new ZipTransformer(tables);

  // Compute revenue by category
  console.log("\nComputing revenue by category (Union 7-step algorithm)...\n");
  const monthlyRevenue = transformer.computeRevenueByCategory(orders, refunds, transfers);

  // Display results
  console.log("\n=== Monthly Revenue Summary ===\n");
  console.log("Month       | Categories | Gross Revenue | Net Revenue  | Status");
  console.log("------------|------------|---------------|--------------|-------");

  const months = [...monthlyRevenue.entries()].sort(([a], [b]) => a.localeCompare(b));
  let totalSaved = 0;
  let totalSkipped = 0;

  for (const [month, categories] of months) {
    const gross = categories.reduce((s, c) => s + c.revenue, 0);
    const net = categories.reduce((s, c) => s + c.netRevenue, 0);
    const year = parseInt(month.slice(0, 4));
    const monthNum = parseInt(month.slice(5, 7));

    let status = "";
    if (!dryRun) {
      const locked = await isMonthLocked(year, monthNum);
      if (locked && !unlock) {
        status = "LOCKED (skip)";
        totalSkipped += categories.length;
      } else {
        // Save to DB
        const periodStart = `${month}-01`;
        const lastDay = new Date(year, monthNum, 0).getDate();
        const periodEnd = `${month}-${String(lastDay).padStart(2, "0")}`;
        await saveRevenueCategories(periodStart, periodEnd, categories);
        totalSaved += categories.length;
        status = locked ? "OVERWRITTEN" : "saved";
      }
    } else {
      status = "dry-run";
    }

    console.log(
      `${month}     | ${String(categories.length).padStart(10)} | ` +
        `$${gross.toLocaleString("en-US", { minimumFractionDigits: 0, maximumFractionDigits: 0 }).padStart(12)} | ` +
        `$${net.toLocaleString("en-US", { minimumFractionDigits: 0, maximumFractionDigits: 0 }).padStart(11)} | ` +
        `${status}`
    );
  }

  console.log("------------|------------|---------------|--------------|-------");
  console.log(
    `Total: ${months.length} months, ` +
      `${months.reduce((s, [, c]) => s + c.length, 0)} category rows`
  );

  if (!dryRun) {
    await bumpDataVersion();
    console.log(`\nSaved: ${totalSaved} rows, Skipped: ${totalSkipped} rows (locked)`);
  }

  console.log("\nDone.");
  process.exit(0);
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
