/**
 * Import revenue category CSVs into the database.
 * Usage: npx tsx scripts/import-revenue.ts <csv-path> <period-start> <period-end>
 * Example: npx tsx scripts/import-revenue.ts ~/Downloads/revenue-2026.csv 2026-01-01 2026-02-16
 */
import { initDatabase } from "../src/lib/db/database";
import { saveRevenueCategories } from "../src/lib/db/revenue-store";
import { setWatermark } from "../src/lib/db/watermark-store";
import { parseCSV } from "../src/lib/parser/csv-parser";
import { RevenueCategorySchema } from "../src/lib/parser/schemas";
import type { RevenueCategory } from "../src/types/union-data";

async function main() {
  const [csvPath, periodStart, periodEnd] = process.argv.slice(2);

  if (!csvPath || !periodStart || !periodEnd) {
    console.error("Usage: npx tsx scripts/import-revenue.ts <csv-path> <period-start> <period-end>");
    console.error("Example: npx tsx scripts/import-revenue.ts ~/Downloads/revenue-2026.csv 2026-01-01 2026-02-16");
    process.exit(1);
  }

  console.log(`Importing revenue: ${csvPath}`);
  console.log(`Period: ${periodStart} to ${periodEnd}`);

  await initDatabase();

  const result = parseCSV<RevenueCategory>(csvPath, RevenueCategorySchema);
  console.log(`Parsed: ${result.data.length} categories, ${result.warnings.length} warnings`);

  if (result.warnings.length > 0) {
    console.log("Warnings:", result.warnings.slice(0, 5));
  }

  if (result.data.length === 0) {
    console.error("No data parsed — check CSV format");
    process.exit(1);
  }

  const totalNet = result.data.reduce((sum, r) => sum + r.netRevenue, 0);
  console.log(`Total net revenue: $${totalNet.toLocaleString("en-US", { minimumFractionDigits: 2 })}`);

  await saveRevenueCategories(periodStart, periodEnd, result.data);

  // Update watermark to the later of existing and new period
  await setWatermark(
    "revenueCategories",
    periodEnd,
    result.data.length,
    `imported ${periodStart} to ${periodEnd}`
  );

  console.log("Done!");
  process.exit(0);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
