/**
 * Backfill all of 2025 data into SQLite.
 *
 * Runs the full email pipeline with date range 1/1/2025 - 2/16/2026:
 *  1. Playwright logs into Union.fit
 *  2. Sets date range to 1/1/2025 - 2/16/2026 on each report page
 *  3. Clicks "Download CSV" — direct downloads + email exports
 *  4. Gmail API polls for email attachments
 *  5. Parses all CSVs and saves to SQLite + Google Sheets
 *
 * Usage: npx tsx scripts/backfill-2025.ts
 */

import * as dotenv from "dotenv";
dotenv.config();

import { loadSettings } from "../src/lib/crypto/credentials";
import { runEmailPipeline } from "../src/lib/queue/email-pipeline";

const DATE_RANGE = "1/1/2025 - 2/16/2026";

async function main() {
  console.log("=== Backfill 2025 Data ===\n");
  console.log(`Date range: ${DATE_RANGE}\n`);

  // Load credentials
  const settings = loadSettings();
  if (!settings?.credentials) {
    console.error("No Union.fit credentials found. Run settings page to configure.");
    process.exit(1);
  }

  if (!settings.robotEmail?.address) {
    console.error("No robot email configured. Run settings page to configure.");
    process.exit(1);
  }

  console.log(`Union.fit email: ${settings.credentials.email}`);
  console.log(`Robot email:     ${settings.robotEmail.address}`);
  console.log(`Email timeout:   15 minutes`);
  console.log("");

  const startTime = Date.now();

  try {
    const result = await runEmailPipeline({
      unionEmail: settings.credentials.email,
      unionPassword: settings.credentials.password,
      robotEmail: settings.robotEmail.address,
      dateRange: DATE_RANGE,
      emailTimeoutMs: 900_000, // 15 minutes
      onProgress: (step, percent) => {
        const elapsed = ((Date.now() - startTime) / 1000).toFixed(0);
        console.log(`  [${elapsed}s] ${percent}% — ${step}`);
      },
    });

    console.log("\n=== Backfill Completed Successfully ===");
    console.log(`Total time: ${((Date.now() - startTime) / 1000).toFixed(1)}s`);
    console.log(`Record counts:`);
    if (result.recordCounts) {
      for (const [key, count] of Object.entries(result.recordCounts)) {
        console.log(`  ${key}: ${count}`);
      }
    }
    if (result.warnings && result.warnings.length > 0) {
      console.log(`\nWarnings (${result.warnings.length}):`);
      for (const w of result.warnings.slice(0, 20)) {
        console.log(`  - ${w}`);
      }
    }
    console.log(`\nSheet URL: ${result.sheetUrl}`);
  } catch (err) {
    console.error("\n=== Backfill Failed ===");
    console.error(err instanceof Error ? err.message : String(err));
    if (err instanceof Error && err.stack) {
      console.error("\nStack trace:");
      console.error(err.stack);
    }
    process.exit(1);
  }
}

main();
