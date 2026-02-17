/**
 * Test script: parse the actual revenue categories CSV from Union.fit
 * and store it in the database.
 */
import * as dotenv from "dotenv";
dotenv.config();

import { parseCSV } from "../src/lib/parser/csv-parser";
import { RevenueCategorySchema } from "../src/lib/parser/schemas";
import { saveRevenueCategories, getRevenueForPeriod } from "../src/lib/db/revenue-store";
import { initDatabase } from "../src/lib/db/database";

const CSV_PATH = "/Users/mike.rudoy_old/Downloads/union-revenue-categories-sky-ting-20260216-2304.csv";

// The date range for this CSV (Jan 16 - Feb 16 based on Union.fit default 30-day range)
const PERIOD_START = "2026-01-16";
const PERIOD_END = "2026-02-16";

async function main() {
  await initDatabase();

  console.log("=== Parsing Revenue Categories CSV ===");
  console.log(`File: ${CSV_PATH}`);

  const { data, warnings } = parseCSV(CSV_PATH, RevenueCategorySchema);

  console.log(`\nParsed ${data.length} categories`);
  if (warnings.length > 0) {
    console.log(`Warnings (${warnings.length}):`);
    warnings.forEach((w) => console.log(`  - ${w}`));
  }

  // Show first few rows to verify
  console.log("\n=== Sample Rows ===");
  data.slice(0, 5).forEach((row) => {
    console.log(`  ${row.revenueCategory}: revenue=$${row.revenue.toFixed(2)}, net=$${row.netRevenue.toFixed(2)}, otherFees=$${row.otherFees.toFixed(2)}, unionFeesRefunded=$${row.unionFeesRefunded.toFixed(2)}`);
  });

  // Totals
  const totalRevenue = data.reduce((sum, r) => sum + r.revenue, 0);
  const totalNet = data.reduce((sum, r) => sum + r.netRevenue, 0);
  const totalOtherFees = data.reduce((sum, r) => sum + r.otherFees, 0);
  console.log(`\n=== Totals ===`);
  console.log(`  Total Revenue: $${totalRevenue.toFixed(2)}`);
  console.log(`  Total Net Revenue: $${totalNet.toFixed(2)}`);
  console.log(`  Total Other Fees: $${totalOtherFees.toFixed(2)}`);

  // Save to database
  console.log(`\n=== Saving to Database ===`);
  await saveRevenueCategories(PERIOD_START, PERIOD_END, data);

  // Read back to verify
  const stored = await getRevenueForPeriod(PERIOD_START, PERIOD_END);
  console.log(`\nStored ${stored.length} rows. Top 5 by revenue:`);
  stored.slice(0, 5).forEach((row) => {
    console.log(`  ${row.category}: $${row.revenue.toFixed(2)} revenue, $${row.netRevenue.toFixed(2)} net`);
  });

  console.log("\nDone!");
}

main().catch(err => {
  console.error("Test failed:", err);
  process.exit(1);
});
