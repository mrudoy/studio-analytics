/**
 * End-to-end test of the email pipeline.
 *
 * Runs the full flow:
 *  1. Playwright logs into Union.fit as robot@skyting.com
 *  2. Clicks "Download CSV" on each report page
 *  3. Gmail API polls robot@skyting.com inbox for CSV emails
 *  4. Downloads CSV attachments
 *  5. Maps to report types
 *  6. Runs analytics pipeline → exports to Google Sheets
 *
 * Usage: npx tsx scripts/test-email-pipeline.ts
 */

import * as dotenv from "dotenv";
dotenv.config();

import { loadSettings } from "../src/lib/crypto/credentials";
import { runEmailPipeline } from "../src/lib/queue/email-pipeline";

async function main() {
  console.log("=== Email Pipeline End-to-End Test ===\n");

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
      emailTimeoutMs: 900_000, // 15 minutes — Union.fit emails can be very slow
      onProgress: (step, percent) => {
        const elapsed = ((Date.now() - startTime) / 1000).toFixed(0);
        console.log(`  [${elapsed}s] ${percent}% — ${step}`);
      },
    });

    console.log("\n=== Pipeline Completed Successfully ===");
    console.log(`Total time: ${((Date.now() - startTime) / 1000).toFixed(1)}s`);
    console.log(`Result: ${JSON.stringify(result, null, 2)}`);
  } catch (err) {
    console.error("\n=== Pipeline Failed ===");
    console.error(err instanceof Error ? err.message : String(err));
    if (err instanceof Error && err.stack) {
      console.error("\nStack trace:");
      console.error(err.stack);
    }
    process.exit(1);
  }
}

main();
