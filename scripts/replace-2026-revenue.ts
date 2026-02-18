import { getPool, initDatabase } from "../src/lib/db/database";
import { setWatermark } from "../src/lib/db/watermark-store";
import { parseCSV } from "../src/lib/parser/csv-parser";
import { RevenueCategorySchema } from "../src/lib/parser/schemas";
import { saveRevenueCategories } from "../src/lib/db/revenue-store";
import type { RevenueCategory } from "../src/types/union-data";

async function main() {
  await initDatabase();
  const pool = getPool();

  // Delete old 2026 data
  const del = await pool.query("DELETE FROM revenue_categories WHERE period_start = '2026-01-01'");
  console.log("Deleted", del.rowCount, "old 2026 rows");

  // Import corrected data
  const result = parseCSV<RevenueCategory>("/tmp/revenue-2026-corrected.csv", RevenueCategorySchema);
  console.log("Parsed:", result.data.length, "categories,", result.warnings.length, "warnings");

  if (result.warnings.length > 0) {
    console.log("Warnings:", result.warnings.slice(0, 5));
  }

  const totalGross = result.data.reduce((s, r) => s + r.revenue, 0);
  const totalNet = result.data.reduce((s, r) => s + r.netRevenue, 0);
  console.log("Gross:", totalGross.toFixed(2), "Net:", totalNet.toFixed(2));

  // Today's date as period end
  const now = new Date();
  const periodEnd = now.toISOString().slice(0, 10);
  await saveRevenueCategories("2026-01-01", periodEnd, result.data);
  await setWatermark("revenueCategories", periodEnd, result.data.length, `corrected 2026 import`);

  // Verify
  const check = await pool.query("SELECT period_start, period_end, COUNT(*) as cats, SUM(revenue)::numeric(12,2) as gross, SUM(net_revenue)::numeric(12,2) as net FROM revenue_categories GROUP BY period_start, period_end ORDER BY period_start");
  console.log("\nRevenue in DB:");
  for (const r of check.rows) {
    console.log(`  ${r.period_start} to ${r.period_end}: ${r.cats} cats, $${Number(r.gross).toLocaleString()} gross, $${Number(r.net).toLocaleString()} net`);
  }

  await pool.end();
  console.log("\nDone!");
}

main().catch(e => { console.error(e); process.exit(1); });
