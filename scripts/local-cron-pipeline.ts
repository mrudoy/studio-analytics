/**
 * Standalone pipeline runner for macOS launchd cron.
 *
 * Runs the full pipeline (Union.fit CSV fetch → parse → DB) + Shopify sync +
 * cloud backup, writing directly to the Railway production Postgres.
 * No Next.js server, no BullMQ, no Redis required.
 *
 * Usage:
 *   npx tsx scripts/local-cron-pipeline.ts
 *
 * Environment:
 *   Loads .env (ENCRYPTION_MASTER_KEY, GITHUB_TOKEN), then .env.local,
 *   then .env.production.local (DATABASE_URL override for Railway).
 */

import * as dotenv from "dotenv";
import * as path from "path";

// Load env files in priority order (last wins for overlapping keys)
const root = path.resolve(__dirname, "..");
dotenv.config({ path: path.join(root, ".env") });
dotenv.config({ path: path.join(root, ".env.local"), override: true });
dotenv.config({ path: path.join(root, ".env.production.local"), override: true });

import { initDatabase } from "../src/lib/db/database";
import { closePool } from "../src/lib/db/database";
import { loadSettings } from "../src/lib/crypto/credentials";
import { runZipDownloadPipeline } from "../src/lib/email/zip-download-pipeline";
import { runShopifySync } from "../src/lib/shopify/shopify-sync";
import { uploadBackupToGitHub } from "../src/lib/db/backup-cloud";
import { sendDigestEmail } from "../src/lib/email/email-sender";

function log(msg: string) {
  const ts = new Date().toISOString().slice(11, 19);
  console.log(`[${ts}] ${msg}`);
}

async function main() {
  const startTime = Date.now();
  log("=== Local Pipeline Cron ===");
  log(`DATABASE_URL: ${(process.env.DATABASE_URL || "").replace(/:[^:@]*@/, ":***@")}`);

  // 1. Init database schema
  log("Initializing database...");
  await initDatabase();

  // 2. Load settings
  const settings = loadSettings();
  if (!settings?.credentials) {
    throw new Error("No Union.fit credentials configured. Go to Settings to configure.");
  }
  if (!settings.robotEmail?.address) {
    throw new Error("Robot email not configured. Go to Settings to configure.");
  }

  log(`Union.fit: ${settings.credentials.email}`);
  log(`Robot email: ${settings.robotEmail.address}`);

  // 3. Run the zip download pipeline (API-only — no Playwright, no scraping).
  // Per the permanent NO SCRAPING rule in CLAUDE.md: if the zip pipeline
  // fails, the script fails loudly — it does NOT fall back to browser
  // automation.
  log("Running zip download pipeline...");
  const result = await runZipDownloadPipeline({
    robotEmail: settings.robotEmail.address,
    lookbackHours: 48,
    onProgress: (step, percent) => {
      log(`  ${percent}% — ${step}`);
    },
  });

  if (!result.success) {
    throw new Error("Zip download pipeline did not succeed (API-only mode — no fallback).");
  }

  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  log(`Pipeline complete in ${elapsed}s`);
  log(`Result: ${JSON.stringify(result)}`);

  // 5. Send daily digest email (non-fatal)
  try {
    const emailResult = await sendDigestEmail();
    if (emailResult.sent > 0) {
      log(`Digest email sent to ${emailResult.sent} recipients`);
    } else if (emailResult.skipped) {
      log(`Digest email skipped: ${emailResult.skipped}`);
    }
  } catch (err) {
    log(`Digest email failed (non-fatal): ${err instanceof Error ? err.message : err}`);
  }

  // 7. Shopify sync (non-fatal)
  if (settings.shopify?.storeName && settings.shopify?.clientId && settings.shopify?.clientSecret) {
    try {
      log("Starting Shopify sync...");
      const shopifyResult = await runShopifySync({
        storeName: settings.shopify.storeName,
        clientId: settings.shopify.clientId,
        clientSecret: settings.shopify.clientSecret,
      });
      log(
        `Shopify sync: ${shopifyResult.orderCount} orders, ` +
        `${shopifyResult.productCount} products, ${shopifyResult.customerCount} customers`
      );
    } catch (err) {
      log(`Shopify sync failed (non-fatal): ${err instanceof Error ? err.message : err}`);
    }
  } else {
    log("Shopify not configured, skipping sync");
  }

  // 8. Cloud backup (non-fatal)
  if (process.env.GITHUB_TOKEN) {
    try {
      log("Uploading cloud backup...");
      const cloud = await uploadBackupToGitHub();
      log(`Cloud backup: ${cloud.tag} (${cloud.totalRows} rows, ${(cloud.compressedBytes / 1024).toFixed(0)}KB)`);
    } catch (err) {
      log(`Cloud backup failed (non-fatal): ${err instanceof Error ? err.message : err}`);
    }
  } else {
    log("GITHUB_TOKEN not set, skipping cloud backup");
  }

  const totalElapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  log(`=== Done in ${totalElapsed}s ===`);
}

main()
  .then(async () => {
    await closePool();
    process.exit(0);
  })
  .catch(async (err) => {
    log(`FATAL: ${err instanceof Error ? err.message : String(err)}`);
    if (err instanceof Error && err.stack) {
      console.error(err.stack);
    }
    await closePool().catch(() => {});
    process.exit(1);
  });
