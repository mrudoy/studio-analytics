/**
 * Download CSV attachments from Union.fit export emails and show first few rows.
 */
import * as dotenv from "dotenv";
dotenv.config();

import { GmailClient } from "../src/lib/email/gmail-client";
import { readFileSync } from "fs";

async function main() {
  const gmail = new GmailClient({ robotEmail: "robot@skyting.com" });

  // Find all CSV emails from Union.fit in the last 24 hours
  const since = new Date(Date.now() - 24 * 3600_000);
  const emails = await gmail.findReportEmails(since);

  console.log(`Found ${emails.length} CSV emails:\n`);

  if (emails.length === 0) {
    console.log("No CSV emails found.");
    return;
  }

  // Download all attachments
  const downloaded = await gmail.downloadAllAttachments(emails);

  console.log(`\nDownloaded ${downloaded.size} files:\n`);

  for (const [key, filePath] of downloaded.entries()) {
    console.log(`=== ${key} ===`);
    console.log(`File: ${filePath}`);

    // Read first 5 lines to show column headers and sample data
    const content = readFileSync(filePath, "utf8");
    const lines = content.split("\n").slice(0, 6);
    console.log("First 5 rows:");
    for (const line of lines) {
      console.log(`  ${line.slice(0, 200)}`);
    }
    console.log(`Total lines: ${content.split("\n").length}`);
    console.log("");
  }
}

main().catch((err) => console.error("Error:", err.message));
