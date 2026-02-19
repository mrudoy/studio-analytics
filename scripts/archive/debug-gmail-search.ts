/**
 * Debug why Gmail isn't finding all the Union.fit CSV emails.
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

  // Search 1: Same query the pipeline would use (last 30 minutes)
  const thirtyMinAgo = new Date(Date.now() - 30 * 60 * 1000);
  console.log("=== Search 1: Last 30 minutes ===");
  console.log(`Since: ${thirtyMinAgo.toISOString()}`);
  const recent = await gmail.findReportEmails(thirtyMinAgo);
  console.log(`Found: ${recent.length} emails`);
  for (const e of recent) {
    console.log(`  Subject: ${e.subject}`);
    console.log(`  Date: ${e.date}`);
    console.log(`  Attachments: ${e.attachments.map(a => a.filename).join(", ")}`);
  }

  // Search 2: Last 2 hours
  const twoHoursAgo = new Date(Date.now() - 2 * 60 * 60 * 1000);
  console.log("\n=== Search 2: Last 2 hours ===");
  console.log(`Since: ${twoHoursAgo.toISOString()}`);
  const twoHr = await gmail.findReportEmails(twoHoursAgo);
  console.log(`Found: ${twoHr.length} emails`);
  for (const e of twoHr) {
    console.log(`  Subject: ${e.subject}`);
    console.log(`  Date: ${e.date}`);
    console.log(`  Attachments: ${e.attachments.map(a => a.filename).join(", ")}`);
  }

  // Search 3: Last 24 hours — raw Gmail query
  console.log("\n=== Search 3: Raw Gmail search (last 24h, from:union.fit) ===");
  const dayAgo = new Date(Date.now() - 24 * 60 * 60 * 1000);
  const dayAgoEpoch = Math.floor(dayAgo.getTime() / 1000);
  const query = `from:union.fit has:attachment after:${dayAgoEpoch}`;
  console.log(`Query: ${query}`);
  const dayResults = await gmail.findReportEmails(dayAgo);
  console.log(`Found: ${dayResults.length} emails`);
  for (const e of dayResults) {
    console.log(`  Subject: ${e.subject}`);
    console.log(`  Date: ${e.date}`);
    console.log(`  Attachments: ${e.attachments.map(a => a.filename).join(", ")}`);
  }

  // Search 4: Try without date filter
  console.log("\n=== Search 4: No date filter (all union.fit emails with attachments) ===");
  const veryOld = new Date(2025, 0, 1);
  const allResults = await gmail.findReportEmails(veryOld);
  console.log(`Found: ${allResults.length} emails`);
  for (const e of allResults.slice(0, 15)) {
    console.log(`  Subject: ${e.subject}`);
    console.log(`  Date: ${e.date}`);
    console.log(`  Attachments: ${e.attachments.map(a => a.filename).join(", ")}`);
  }
}

main().catch(console.error);
