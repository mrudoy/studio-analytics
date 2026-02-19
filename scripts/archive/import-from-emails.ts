/**
 * Download all available Union.fit CSV email attachments and import into SQLite.
 *
 * This bypasses the full pipeline and directly:
 * 1. Downloads all CSV attachments from Union.fit emails
 * 2. Maps them to report types
 * 3. Merges with any direct downloads in data/downloads/
 * 4. Runs the pipeline core (parse → analyze → save to SQLite + Sheets)
 *
 * Usage: npx tsx scripts/import-from-emails.ts
 */
import * as dotenv from "dotenv";
dotenv.config();

import { loadSettings } from "../src/lib/crypto/credentials";
import { GmailClient } from "../src/lib/email/gmail-client";
import { runPipelineFromFiles } from "../src/lib/queue/pipeline-core";
import type { DownloadedFiles } from "../src/types/union-data";
import { readdirSync, statSync } from "fs";
import { join } from "path";

const DOWNLOADS_DIR = join(process.cwd(), "data", "downloads");

/**
 * Map email subject to report type.
 */
function subjectToReportType(subject: string): string | null {
  const s = subject.toLowerCase();
  if (s.includes("subscription") && s.includes("audit")) return "subscriptions";
  if (s.includes("first visit")) return "firstVisits";
  if (s.includes("registration export")) return "fullRegistrations";
  if (s.includes("revenue")) return "revenueCategories";
  if (s.includes("customer")) return "newCustomers";
  return null;
}

/**
 * Map filename to report type.
 */
function filenameToReportType(filename: string): string | null {
  const f = filename.toLowerCase();
  if (f.includes("subscription")) return "subscriptions";
  if (f.includes("first-visit")) return "firstVisits";
  if (f.includes("registration-export") && !f.includes("first-visit")) return "fullRegistrations";
  if (f.includes("revenue")) return "revenueCategories";
  if (f.includes("customer")) return "newCustomers";
  return null;
}

async function main() {
  console.log("=== Import From Emails + Downloads ===\n");

  const settings = loadSettings();
  if (!settings?.robotEmail?.address) {
    console.error("No robot email configured.");
    process.exit(1);
  }

  // Database is the source of truth — no Sheets needed

  // Step 1: Gather all available CSV email attachments
  console.log("Step 1: Searching for CSV email attachments...");
  const gmail = new GmailClient({ robotEmail: settings.robotEmail.address });
  const since = new Date(Date.now() - 24 * 60 * 60 * 1000); // Last 24 hours
  const emails = await gmail.findReportEmails(since);
  console.log("Found", emails.length, "emails with CSV attachments");

  // Download all attachments
  console.log("\nStep 2: Downloading attachments...");
  const downloaded = await gmail.downloadAllAttachments(emails);

  const emailFiles: Record<string, string> = {};
  for (const email of emails) {
    for (const att of email.attachments) {
      const key = email.subject + "::" + att.filename;
      const filePath = downloaded.get(key);
      if (!filePath) continue;

      const reportType = subjectToReportType(email.subject) || filenameToReportType(att.filename);
      if (reportType && !emailFiles[reportType]) {
        emailFiles[reportType] = filePath;
        console.log("  " + reportType + " -> " + att.filename);
      }
    }
  }

  // Step 3: Gather direct downloads from data/downloads/
  console.log("\nStep 3: Checking data/downloads/ for direct downloads...");
  const directFiles: Record<string, string> = {};
  try {
    const files = readdirSync(DOWNLOADS_DIR);
    for (const f of files) {
      if (!f.endsWith(".csv")) continue;
      const full = join(DOWNLOADS_DIR, f);
      const stat = statSync(full);
      // Only use files from last 24 hours
      if (Date.now() - stat.mtimeMs > 24 * 60 * 60 * 1000) continue;

      const fl = f.toLowerCase();
      if (fl.includes("canceledautorenews") || fl.includes("cancelled")) {
        if (!directFiles["canceledAutoRenews"]) directFiles["canceledAutoRenews"] = full;
      } else if (fl.includes("newautorenews") || fl.includes("subscriptions-new")) {
        if (!directFiles["newAutoRenews"]) directFiles["newAutoRenews"] = full;
      } else if (fl.includes("activeautorenews") || fl.includes("subscriptions-active")) {
        if (!directFiles["activeAutoRenews"]) directFiles["activeAutoRenews"] = full;
      } else if (fl.includes("pausedautorenews") || fl.includes("subscriptions-paused")) {
        if (!directFiles["pausedAutoRenews"]) directFiles["pausedAutoRenews"] = full;
      } else if (fl.includes("trialingautorenews") || fl.includes("subscriptions-trialing")) {
        if (!directFiles["trialingAutoRenews"]) directFiles["trialingAutoRenews"] = full;
      } else if (fl.includes("firstvisit")) {
        if (!directFiles["firstVisits"]) directFiles["firstVisits"] = full;
      } else if (fl.includes("fullregistrations") || fl.includes("allregistrations") || (fl.includes("registration") && !fl.includes("first"))) {
        if (!directFiles["fullRegistrations"]) directFiles["fullRegistrations"] = full;
      } else if (fl.includes("newcustomers") || fl.includes("customer")) {
        if (!directFiles["newCustomers"]) directFiles["newCustomers"] = full;
      } else if (fl.includes("orders")) {
        if (!directFiles["orders"]) directFiles["orders"] = full;
      } else if (fl.includes("revenuecategories") || fl.includes("revenue")) {
        if (!directFiles["revenueCategories"]) directFiles["revenueCategories"] = full;
      }
      console.log("  " + f + " -> " + Object.keys(directFiles).find(k => directFiles[k] === full));
    }
  } catch {
    console.log("  (no downloads directory)");
  }

  // Step 4: Merge — email files fill gaps from direct downloads
  console.log("\nStep 4: Merging sources...");

  // The subscription email contains ALL states — we need to split it
  // For now, if we have a subscriptions email, use it for all subscription slots
  const subsFile = emailFiles["subscriptions"] || "";

  const merged: DownloadedFiles = {
    canceledAutoRenews: directFiles["canceledAutoRenews"] || subsFile || "",
    newAutoRenews: directFiles["newAutoRenews"] || subsFile || "",
    activeAutoRenews: directFiles["activeAutoRenews"] || subsFile || "",
    pausedAutoRenews: directFiles["pausedAutoRenews"] || "",
    trialingAutoRenews: directFiles["trialingAutoRenews"] || "",
    newCustomers: directFiles["newCustomers"] || emailFiles["newCustomers"] || "",
    orders: directFiles["orders"] || emailFiles["orders"] || "",
    firstVisits: directFiles["firstVisits"] || emailFiles["firstVisits"] || "",
    allRegistrations: "",
    fullRegistrations: directFiles["fullRegistrations"] || emailFiles["fullRegistrations"] || "",
    revenueCategories: directFiles["revenueCategories"] || emailFiles["revenueCategories"] || "",
  };

  console.log("\nFinal file mapping:");
  for (const [key, val] of Object.entries(merged)) {
    if (val) {
      console.log("  " + key + ": " + val.split("/").pop());
    } else {
      console.log("  " + key + ": (missing)");
    }
  }

  // Step 5: Run pipeline
  console.log("\nStep 5: Running pipeline...");
  const startTime = Date.now();

  try {
    const result = await runPipelineFromFiles(merged, undefined, {
      dateRange: "1/1/2025 - 2/16/2026",
      onProgress: (step, percent) => {
        const elapsed = ((Date.now() - startTime) / 1000).toFixed(0);
        console.log("  [" + elapsed + "s] " + percent + "% -- " + step);
      },
    });

    console.log("\n=== Import Complete ===");
    console.log("Duration: " + ((Date.now() - startTime) / 1000).toFixed(1) + "s");
    console.log("Record counts:");
    if (result.recordCounts) {
      for (const [key, count] of Object.entries(result.recordCounts)) {
        console.log("  " + key + ": " + count);
      }
    }
    if (result.warnings && result.warnings.length > 0) {
      console.log("\nWarnings (" + result.warnings.length + "):");
      for (const w of result.warnings.slice(0, 10)) {
        console.log("  - " + w);
      }
    }
  } catch (err) {
    console.error("\n=== Pipeline Failed ===");
    console.error(err instanceof Error ? err.message : String(err));
    process.exit(1);
  }
}

main();
