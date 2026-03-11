/**
 * Debug script: run computeRevenueByCategory on the full export
 * and report which months are produced.
 *
 * Usage: npx tsx scripts/debug-revenue-months.ts [path-to-export-dir]
 */

import { resolve } from "path";
import { readdirSync } from "fs";
import { join } from "path";
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
    for (const w of result.warnings.slice(0, 3)) console.warn(`  ${w}`);
  }
  console.log(`${tableName}: ${result.data.length} rows`);
  return result.data;
}

async function main() {
  const exportDir =
    process.argv[2] ||
    resolve(
      process.env.HOME || "~",
      "Downloads/union_data_export-sky-ting-20230830-20260223-zFoHME8GZ2ESfu8LzvDe27NU"
    );

  console.log(`Export dir: ${exportDir}`);

  // Build file map
  const files = readdirSync(exportDir).filter((f) => f.endsWith(".csv"));
  const fileMap = new Map<string, string>();
  for (const f of files) {
    const name = f.toLowerCase().replace(/\.csv$/, "");
    fileMap.set(name, join(exportDir, f));
  }

  console.log(`\nCSV files found: ${files.length}`);

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

  // Report months
  console.log(`\nMonths produced: ${monthlyRevenue.size}`);
  console.log("\nRevenue by month:");
  const months = [...monthlyRevenue.keys()].sort();
  for (const month of months) {
    const cats = monthlyRevenue.get(month)!;
    const totalRev = cats.reduce((s, c) => s + c.revenue, 0);
    const totalNet = cats.reduce((s, c) => s + c.netRevenue, 0);
    console.log(`  ${month}: ${cats.length} categories, gross=$${totalRev.toLocaleString()}, net=$${totalNet.toLocaleString()}`);
  }

  // Specifically check for the gap
  console.log("\n=== GAP CHECK ===");
  for (const m of ["2025-11", "2025-12", "2026-01", "2026-02", "2026-03"]) {
    if (monthlyRevenue.has(m)) {
      const cats = monthlyRevenue.get(m)!;
      const totalRev = cats.reduce((s, c) => s + c.revenue, 0);
      console.log(`  ${m}: PRESENT (${cats.length} cats, $${totalRev.toLocaleString()})`);
    } else {
      console.log(`  ${m}: *** MISSING ***`);
    }
  }
}

main().catch((err) => {
  console.error("Failed:", err);
  process.exit(1);
});
