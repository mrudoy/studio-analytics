/**
 * Push existing downloaded CSV files to production database.
 * No scraping, no emails — just parse the CSVs on disk and save to DB.
 */

import dotenv from "dotenv";
dotenv.config({ path: ".env" });
dotenv.config({ path: ".env.local", override: true });
dotenv.config({ path: ".env.production.local", override: true });

import path from "path";
import { initDatabase, closePool } from "../src/lib/db/database";
import { runPipelineFromFiles } from "../src/lib/queue/pipeline-core";
import type { DownloadedFiles } from "../src/types/union-data";

const DOWNLOADS = path.join(process.cwd(), "data/downloads");
const EMAILS = path.join(process.cwd(), "data/email-attachments");

async function main() {
  console.log("=== Push Existing Data to Prod ===");
  console.log(`DATABASE_URL: ${process.env.DATABASE_URL?.replace(/:[^@]+@/, ":***@")}`);

  await initDatabase();

  // Use the most recent file for each report type
  // Downloads dir has files like: reportType-timestamp.csv
  // Email dir has files like: timestamp-filename.csv
  const files: DownloadedFiles = {
    // From downloads/ — use most recent by timestamp
    activeAutoRenews: `${DOWNLOADS}/activeAutoRenews-1771364866599.csv`,        // Feb 17
    canceledAutoRenews: `${DOWNLOADS}/canceledAutoRenews-1771362559453.csv`,    // Feb 17
    newAutoRenews: `${DOWNLOADS}/newAutoRenews-1771271208049.csv`,              // Feb 16
    newCustomers: `${DOWNLOADS}/newCustomers-1771271969843.csv`,                // Feb 16
    orders: `${DOWNLOADS}/orders-1771271398927.csv`,                            // Feb 16
    firstVisits: `${DOWNLOADS}/firstVisits-1771271983228.csv`,                  // Feb 16
    fullRegistrations: `${DOWNLOADS}/fullRegistrations-1771271812564.csv`,      // Feb 16
    revenueCategories: `${DOWNLOADS}/revenueCategories-1771272059039.csv`,      // Feb 16
    pausedAutoRenews: `${DOWNLOADS}/pausedAutoRenews-1771867376185.csv`,        // Feb 23 (today)
    trialingAutoRenews: `${DOWNLOADS}/trialingAutoRenews-1771867384902.csv`,    // Feb 23 (today)
  };

  // Verify all files exist
  const fs = await import("fs");
  for (const [report, filePath] of Object.entries(files)) {
    if (filePath && fs.existsSync(filePath)) {
      const size = fs.statSync(filePath).size;
      console.log(`  ${report}: ${filePath.split("/").pop()} (${(size / 1024).toFixed(1)} KB)`);
    } else if (filePath) {
      console.warn(`  ${report}: MISSING — ${filePath}`);
    }
  }

  console.log("\nRunning pipeline...");
  const result = await runPipelineFromFiles(files, undefined, {
    dateRange: "12/31/2023 - 2/23/2026",
    onProgress: (step, percent) => {
      console.log(`  ${percent}% — ${step}`);
    },
  });

  console.log("\n=== Pipeline Complete ===");
  console.log(`Records: ${JSON.stringify(result.recordCounts, null, 2)}`);
  console.log(`Warnings: ${result.warnings.length}`);
  if (result.warnings.length > 0) {
    console.log(`First 5 warnings: ${result.warnings.slice(0, 5).join("\n  ")}`);
  }

  await closePool();
  console.log("Done — data pushed to prod DB.");
}

main().catch((err) => {
  console.error("FATAL:", err);
  closePool().then(() => process.exit(1));
});
