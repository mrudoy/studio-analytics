/**
 * Broad inbox search to find any emails that might be CSV exports from Union.fit.
 * Searches last 30 minutes with various queries.
 */
import * as dotenv from "dotenv";
dotenv.config();

import { google } from "googleapis";

async function main() {
  const saEmail = process.env.GOOGLE_SERVICE_ACCOUNT_EMAIL!;
  const saKey = process.env.GOOGLE_PRIVATE_KEY!;

  const auth = new google.auth.GoogleAuth({
    credentials: {
      client_email: saEmail,
      private_key: saKey.replace(/\\n/g, "\n"),
    },
    scopes: ["https://www.googleapis.com/auth/gmail.readonly"],
    clientOptions: { subject: "robot@skyting.com" },
  });

  const gmail = google.gmail({ version: "v1", auth });

  // Search for ALL emails in the last hour
  const oneHourAgo = Math.floor((Date.now() - 3600_000) / 1000);
  const queries = [
    `after:${oneHourAgo}`,                                    // All recent emails
    `has:attachment after:${oneHourAgo}`,                      // Any with attachments
    `from:union.fit after:${oneHourAgo}`,                      // From union.fit
    `(csv OR export OR download) after:${oneHourAgo}`,         // CSV-related
    `from:noreply after:${oneHourAgo}`,                        // From noreply addresses
  ];

  for (const query of queries) {
    const res = await gmail.users.messages.list({
      userId: "me",
      q: query,
      maxResults: 5,
    });

    const messages = res.data.messages || [];
    console.log(`\nQuery: "${query}" → ${messages.length} results`);

    for (const msg of messages) {
      if (!msg.id) continue;
      const detail = await gmail.users.messages.get({
        userId: "me",
        id: msg.id,
        format: "metadata",
        metadataHeaders: ["Subject", "From", "Date"],
      });
      const headers = detail.data.payload?.headers || [];
      const subject = headers.find((h) => h.name === "Subject")?.value || "(no subject)";
      const from = headers.find((h) => h.name === "From")?.value || "(unknown)";
      const date = headers.find((h) => h.name === "Date")?.value || "";
      console.log(`  [${date}] From: ${from} — Subject: ${subject}`);
    }
  }
}

main().catch((err) => console.error("Error:", err.message));
