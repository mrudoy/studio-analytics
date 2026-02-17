/**
 * Check for Union.fit CSV export emails specifically.
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

  // Search for CSV/export emails from today
  const todayEpoch = Math.floor((Date.now() - 24 * 3600_000) / 1000);
  const query = `from:union.fit (csv OR export) after:${todayEpoch}`;

  const res = await gmail.users.messages.list({
    userId: "me",
    q: query,
    maxResults: 20,
  });

  const messages = res.data.messages || [];
  console.log(`Found ${messages.length} CSV/export emails from Union.fit:\n`);

  for (const msg of messages) {
    if (!msg.id) continue;
    const detail = await gmail.users.messages.get({
      userId: "me",
      id: msg.id,
      format: "full",
    });

    const headers = detail.data.payload?.headers || [];
    const subject = headers.find((h) => h.name === "Subject")?.value || "(no subject)";
    const from = headers.find((h) => h.name === "From")?.value || "(unknown)";
    const date = headers.find((h) => h.name === "Date")?.value || "";

    // Check for attachments
    const parts = detail.data.payload?.parts || [];
    const attachments = parts.filter((p) => p.filename && p.filename.length > 0);
    const attInfo = attachments.map((a) => `${a.filename} (${a.mimeType})`).join(", ");

    console.log(`  [${date}] Subject: ${subject}`);
    console.log(`    From: ${from}`);
    console.log(`    Attachments: ${attInfo || "(none)"}`);
    console.log("");
  }
}

main().catch((err) => console.error("Error:", err.message));
