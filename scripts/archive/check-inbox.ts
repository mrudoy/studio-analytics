import * as dotenv from "dotenv";
dotenv.config();

import { google } from "googleapis";

async function checkInbox() {
  const saEmail = process.env.GOOGLE_SERVICE_ACCOUNT_EMAIL;
  const saKey = process.env.GOOGLE_PRIVATE_KEY;

  if (!saEmail || !saKey) {
    console.log("Missing service account credentials in .env");
    return;
  }

  const auth = new google.auth.GoogleAuth({
    credentials: {
      client_email: saEmail,
      private_key: saKey.replace(/\\n/g, "\n"),
    },
    scopes: ["https://www.googleapis.com/auth/gmail.readonly"],
    clientOptions: { subject: "robot@skyting.com" },
  });

  const gmail = google.gmail({ version: "v1", auth });

  const res = await gmail.users.messages.list({
    userId: "me",
    maxResults: 10,
  });

  const messages = res.data.messages || [];
  console.log("Total messages found:", messages.length);

  if (messages.length === 0) {
    console.log("Inbox is empty — no emails yet.");
    return;
  }

  for (const msg of messages) {
    const detail = await gmail.users.messages.get({
      userId: "me",
      id: msg.id!,
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

checkInbox().catch((err) => console.error("Error:", err.message));
