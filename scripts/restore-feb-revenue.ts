/**
 * Restore Feb 2026 revenue from the historical export.
 * Only computes and saves revenue for Feb 2026 — does NOT touch other months.
 *
 * Usage:
 *   BATCH_IMPORT=1 npx tsx scripts/restore-feb-revenue.ts [csv-directory]
 */

import dotenv from "dotenv";
import { resolve, join } from "path";

dotenv.config({ path: resolve(__dirname, "../.env.production.local") });
dotenv.config({ path: resolve(__dirname, "../.env.local") });
dotenv.config({ path: resolve(__dirname, "../.env") });

import { initDatabase } from "../src/lib/db/database";
import { runMigrations } from "../src/lib/db/migrations";
import { parseCSV } from "../src/lib/parser/csv-parser";
import { ZipTransformer } from "../src/lib/email/zip-transformer";
import { saveRevenueCategories } from "../src/lib/db/revenue-store";
import {
  RawMembershipSchema, RawPassSchema, RawEventSchema,
  RawPerformanceSchema, RawLocationSchema, RawPassTypeSchema,
  RawRevenueCategoryLookupSchema, RawOrderSchema, RawRefundSchema,
  RawTransferSchema,
  type RawOrder, type RawRefund, type RawTransfer,
} from "../src/lib/email/zip-schemas";

const DEFAULT_CSV_DIR = resolve(
  process.env.HOME || "~",
  "Downloads/union_data_export-sky-ting-20230830-20260223-zFoHME8GZ2ESfu8LzvDe27NU"
);

function parse<T>(dir: string, name: string, schema: { parse: (v: unknown) => T }): T[] {
  const path = join(dir, `${name}.csv`);
  const result = parseCSV<T>(path, schema as never);
  console.log(`  ${name}: ${result.data.length} rows`);
  return result.data;
}

async function main() {
  const csvDir = process.argv[2] || DEFAULT_CSV_DIR;
  console.log(`\n=== Restore Feb 2026 Revenue ===`);
  console.log(`CSV directory: ${csvDir}\n`);

  await initDatabase();
  await runMigrations();
  console.log("Database ready.\n");

  console.log("Parsing CSVs...");
  const memberships = parse(csvDir, "memberships", RawMembershipSchema);
  const passes = parse(csvDir, "passes", RawPassSchema);
  const events = parse(csvDir, "events", RawEventSchema);
  const performances = parse(csvDir, "performances", RawPerformanceSchema);
  const locations = parse(csvDir, "locations", RawLocationSchema);
  const passTypes = parse(csvDir, "pass_types", RawPassTypeSchema);
  const revenueCategoryLookups = parse(csvDir, "revenue_categories", RawRevenueCategoryLookupSchema);
  const orders = parse(csvDir, "orders", RawOrderSchema);
  const refunds = parse(csvDir, "refunds", RawRefundSchema);
  const transfers = parse(csvDir, "transfers", RawTransferSchema);

  console.log("\nBuilding transformer...");
  const transformer = new ZipTransformer({
    memberships, passes, events, performances, locations, passTypes, revenueCategoryLookups,
  });

  console.log("Computing revenue by category...");
  const monthlyRevenue = transformer.computeRevenueByCategory(
    orders as RawOrder[],
    refunds as RawRefund[],
    transfers as RawTransfer[],
  );

  console.log(`Found revenue for ${monthlyRevenue.size} months`);

  // Only save Feb 2026
  const FEB_KEY = "2026-02";
  const febCategories = monthlyRevenue.get(FEB_KEY);
  if (!febCategories) {
    console.error(`No revenue data found for ${FEB_KEY}`);
    process.exit(1);
  }

  const totalGross = febCategories.reduce((s, c) => s + (c.revenue ?? 0), 0);
  const totalNet = febCategories.reduce((s, c) => s + (c.netRevenue ?? 0), 0);
  console.log(`\nFeb 2026: ${febCategories.length} categories, gross=$${totalGross.toFixed(0)}, net=$${totalNet.toFixed(0)}`);

  console.log(`\nSaving Feb 2026 revenue...`);
  await saveRevenueCategories("2026-02-01", "2026-02-28", febCategories);
  console.log("Done!");

  process.exit(0);
}

main().catch((err) => {
  console.error("Failed:", err);
  process.exit(1);
});
