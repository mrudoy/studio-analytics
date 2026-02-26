/**
 * Search for ALL emails from union.fit using the GmailClient
 */
import * as dotenv from "dotenv";
dotenv.config();

import { loadSettings } from "../src/lib/crypto/credentials";
import { GmailClient } from "../src/lib/email/gmail-client";

async function main() {
  const settings = loadSettings();
  if (!settings?.robotEmail?.address) {
    console.error("No robot email configured.");
    process.exit(1);
  }

  const gmail = new GmailClient({ robotEmail: settings.robotEmail.address });

  // Use findReportEmails with progressively wider time windows
  console.log("=== Last 30 minutes ===");
  const thirtyMin = new Date(Date.now() - 30 * 60 * 1000);
  const r1 = await gmail.findReportEmails(thirtyMin);
  console.log("Found:", r1.length, "emails with CSV attachments");
  for (const e of r1) {
    console.log("  [" + e.date.toLocaleTimeString() + "] " + e.subject + " -- " + e.attachments.map(a => a.filename).join(", "));
  }

  console.log("\n=== Last 1 hour ===");
  const oneHour = new Date(Date.now() - 60 * 60 * 1000);
  const r2 = await gmail.findReportEmails(oneHour);
  console.log("Found:", r2.length, "emails with CSV attachments");
  for (const e of r2) {
    console.log("  [" + e.date.toLocaleTimeString() + "] " + e.subject + " -- " + e.attachments.map(a => a.filename).join(", "));
  }

  console.log("\n=== Last 24 hours ===");
  const dayAgo = new Date(Date.now() - 24 * 60 * 60 * 1000);
  const r3 = await gmail.findReportEmails(dayAgo);
  console.log("Found:", r3.length, "emails with CSV attachments");
  for (const e of r3) {
    console.log("  [" + e.date.toLocaleTimeString() + "] " + e.subject + " -- " + e.attachments.map(a => a.filename).join(", "));
  }
}

main().catch(console.error);
