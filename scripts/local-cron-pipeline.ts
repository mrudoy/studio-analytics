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
import { runEmailPipeline } from "../src/lib/queue/email-pipeline";
import { runShopifySync } from "../src/lib/shopify/shopify-sync";
import { uploadBackupToGitHub } from "../src/lib/db/backup-cloud";
import { getWatermark, buildDateRangeForReport } from "../src/lib/db/watermark-store";

const PIPELINE_TIMEOUT_MS = 20 * 60 * 1000; // 20 minutes

function log(msg: string) {
  const ts = new Date().toISOString().slice(11, 19);
  console.log(`[${ts}] ${msg}`);
}

/**
 * Build date range from watermarks (same logic as pipeline-worker.ts).
 */
async function buildDateRange(): Promise<string> {
  const reportTypes = [
    "autoRenews",
    "revenueCategories",
    "registrations",
    "orders",
    "newCustomers",
    "firstVisits",
  ];

  let oldestStart: string | null = null;

  for (const rt of reportTypes) {
    try {
      const wm = await getWatermark(rt);
      const range = buildDateRangeForReport(wm);
      const startPart = range.split(" - ")[0];
      const parts = startPart.split("/");
      if (parts.length === 3) {
        const isoDate = `${parts[2]}-${parts[0].padStart(2, "0")}-${parts[1].padStart(2, "0")}`;
        if (!oldestStart || isoDate < oldestStart) {
          oldestStart = isoDate;
        }
      }
    } catch {
      // Skip — watermark table might not exist yet
    }
  }

  const now = new Date();
  const endStr = `${now.getMonth() + 1}/${now.getDate()}/${now.getFullYear()}`;

  if (oldestStart) {
    const d = new Date(oldestStart);
    const startStr = `${d.getMonth() + 1}/${d.getDate()}/${d.getFullYear()}`;
    log(`Incremental mode: ${startStr} - ${endStr}`);
    return `${startStr} - ${endStr}`;
  }

  log(`First run: no watermarks, fetching from 1/1/2024`);
  return `1/1/2024 - ${endStr}`;
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

  // 3. Build date range from watermarks
  const dateRange = await buildDateRange();

  // 4. Run email pipeline (Playwright + Gmail + parse + save)
  log("Starting email pipeline...");
  const pipelinePromise = runEmailPipeline({
    unionEmail: settings.credentials.email,
    unionPassword: settings.credentials.password,
    robotEmail: settings.robotEmail.address,
    dateRange,
    emailTimeoutMs: 900_000, // 15 min
    onProgress: (step, percent) => {
      log(`  ${percent}% — ${step}`);
    },
  });

  // Race against timeout
  const timeoutPromise = new Promise<never>((_, reject) =>
    setTimeout(() => reject(new Error("Pipeline timed out after 20 minutes")), PIPELINE_TIMEOUT_MS)
  );

  const result = await Promise.race([pipelinePromise, timeoutPromise]);
  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  log(`Pipeline complete in ${elapsed}s`);
  log(`Result: ${JSON.stringify(result)}`);

  // 5. Shopify sync (non-fatal)
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

  // 6. Cloud backup (non-fatal)
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
