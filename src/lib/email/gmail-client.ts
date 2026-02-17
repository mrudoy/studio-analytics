/**
 * Gmail API client for reading CSV report emails from Union.fit.
 *
 * Uses the existing Google service account with domain-wide delegation
 * to read emails from a dedicated robot inbox (e.g. robot@skyting.com).
 *
 * Alternatively supports Gmail App Password + IMAP if delegation isn't set up.
 */

import { google, gmail_v1 } from "googleapis";
import { writeFileSync, mkdirSync, existsSync } from "fs";
import { join } from "path";

const ATTACHMENTS_DIR = join(process.cwd(), "data", "email-attachments");

export interface ReportEmail {
  id: string;
  subject: string;
  from: string;
  date: Date;
  attachments: EmailAttachment[];
}

export interface EmailAttachment {
  attachmentId: string;
  filename: string;
  mimeType: string;
  size: number;
}

export interface GmailClientOptions {
  /** The robot email address to read from (e.g. robot@skyting.com) */
  robotEmail: string;
  /** Google service account email (reuses existing GOOGLE_SERVICE_ACCOUNT_EMAIL) */
  serviceAccountEmail?: string;
  /** Google service account private key (reuses existing GOOGLE_PRIVATE_KEY) */
  privateKey?: string;
}

/**
 * Clean up private key from environment variables.
 * Same logic as sheets-client.ts.
 */
function cleanPrivateKey(rawKey: string): string {
  let key = rawKey;
  key = key.replace(/\\\\n/g, "\n");
  key = key.replace(/\\n/g, "\n");
  key = key.replace(/^["']|["']$/g, "");
  if (!key.includes("-----BEGIN")) {
    key = `-----BEGIN PRIVATE KEY-----\n${key}\n-----END PRIVATE KEY-----\n`;
  }
  return key;
}

export class GmailClient {
  private gmail: gmail_v1.Gmail;
  private robotEmail: string;

  constructor(options: GmailClientOptions) {
    this.robotEmail = options.robotEmail;

    const saEmail = options.serviceAccountEmail || process.env.GOOGLE_SERVICE_ACCOUNT_EMAIL;
    const saKey = options.privateKey || process.env.GOOGLE_PRIVATE_KEY;

    if (!saEmail || !saKey) {
      throw new Error(
        "Google service account not configured. Set GOOGLE_SERVICE_ACCOUNT_EMAIL and GOOGLE_PRIVATE_KEY."
      );
    }

    // Create GoogleAuth with service account credentials + domain-wide delegation.
    // Use google.auth.GoogleAuth to avoid version mismatch with standalone google-auth-library.
    const auth = new google.auth.GoogleAuth({
      credentials: {
        client_email: saEmail,
        private_key: cleanPrivateKey(saKey),
      },
      scopes: ["https://www.googleapis.com/auth/gmail.readonly"],
      clientOptions: {
        subject: this.robotEmail, // Impersonate the robot account via domain-wide delegation
      },
    });

    this.gmail = google.gmail({ version: "v1", auth });
  }

  /**
   * Search for Union.fit report emails received since a given time.
   * Looks for emails from Union.fit with CSV attachments.
   */
  async findReportEmails(since: Date): Promise<ReportEmail[]> {
    const sinceEpoch = Math.floor(since.getTime() / 1000);
    // Search for emails from union.fit with attachments, after the given time
    const query = `from:union.fit has:attachment after:${sinceEpoch}`;

    const response = await this.gmail.users.messages.list({
      userId: "me",
      q: query,
      maxResults: 50,
    });

    const messageIds = response.data.messages || [];
    if (messageIds.length === 0) return [];

    const emails: ReportEmail[] = [];
    for (const msg of messageIds) {
      if (!msg.id) continue;
      const detail = await this.getMessageDetail(msg.id);
      if (detail) emails.push(detail);
    }

    return emails;
  }

  /**
   * Get full message detail including attachment metadata.
   */
  private async getMessageDetail(messageId: string): Promise<ReportEmail | null> {
    const msg = await this.gmail.users.messages.get({
      userId: "me",
      id: messageId,
      format: "full",
    });

    const headers = msg.data.payload?.headers || [];
    const subject = headers.find((h) => h.name?.toLowerCase() === "subject")?.value || "";
    const from = headers.find((h) => h.name?.toLowerCase() === "from")?.value || "";
    const dateStr = headers.find((h) => h.name?.toLowerCase() === "date")?.value || "";

    // Find CSV attachments
    const attachments: EmailAttachment[] = [];
    this.extractAttachments(msg.data.payload, attachments);

    // Only include emails that have CSV attachments
    const csvAttachments = attachments.filter(
      (a) =>
        a.filename.toLowerCase().endsWith(".csv") ||
        a.mimeType === "text/csv" ||
        a.mimeType === "application/csv"
    );

    if (csvAttachments.length === 0) return null;

    return {
      id: messageId,
      subject,
      from,
      date: dateStr ? new Date(dateStr) : new Date(),
      attachments: csvAttachments,
    };
  }

  /**
   * Recursively extract attachment metadata from message parts.
   */
  private extractAttachments(
    part: gmail_v1.Schema$MessagePart | undefined,
    results: EmailAttachment[]
  ): void {
    if (!part) return;

    if (part.filename && part.body?.attachmentId) {
      results.push({
        attachmentId: part.body.attachmentId,
        filename: part.filename,
        mimeType: part.mimeType || "application/octet-stream",
        size: part.body.size || 0,
      });
    }

    // Recurse into multipart messages
    if (part.parts) {
      for (const child of part.parts) {
        this.extractAttachments(child, results);
      }
    }
  }

  /**
   * Download a CSV attachment and save it to disk.
   * Returns the file path where the CSV was saved.
   */
  async downloadAttachment(
    messageId: string,
    attachmentId: string,
    filename: string
  ): Promise<string> {
    if (!existsSync(ATTACHMENTS_DIR)) {
      mkdirSync(ATTACHMENTS_DIR, { recursive: true });
    }

    const response = await this.gmail.users.messages.attachments.get({
      userId: "me",
      messageId,
      id: attachmentId,
    });

    const data = response.data.data;
    if (!data) throw new Error(`Empty attachment data for ${filename}`);

    // Gmail API returns base64url-encoded data
    const buffer = Buffer.from(data, "base64url");
    const savePath = join(ATTACHMENTS_DIR, `${Date.now()}-${filename}`);
    writeFileSync(savePath, buffer);

    return savePath;
  }

  /**
   * Download all CSV attachments from a list of report emails.
   * Returns a map of filename → saved file path.
   */
  async downloadAllAttachments(
    emails: ReportEmail[]
  ): Promise<Map<string, string>> {
    const downloaded = new Map<string, string>();

    for (const email of emails) {
      for (const att of email.attachments) {
        const path = await this.downloadAttachment(email.id, att.attachmentId, att.filename);
        // Use subject + filename as key for disambiguation
        const key = `${email.subject}::${att.filename}`;
        downloaded.set(key, path);
        console.log(`[gmail] Downloaded: ${att.filename} from "${email.subject}" → ${path}`);
      }
    }

    return downloaded;
  }

  /**
   * Mark an email as read (remove UNREAD label).
   */
  async markAsRead(messageId: string): Promise<void> {
    await this.gmail.users.messages.modify({
      userId: "me",
      id: messageId,
      requestBody: {
        removeLabelIds: ["UNREAD"],
      },
    });
  }

  /**
   * Poll for report emails, retrying until they arrive or timeout.
   */
  async waitForReportEmails(
    since: Date,
    expectedCount: number,
    options?: {
      timeoutMs?: number;
      pollIntervalMs?: number;
      onPoll?: (found: number) => void;
    }
  ): Promise<ReportEmail[]> {
    const timeout = options?.timeoutMs ?? 120_000; // 2 minutes default
    const interval = options?.pollIntervalMs ?? 10_000; // 10 seconds
    const startTime = Date.now();

    while (Date.now() - startTime < timeout) {
      const emails = await this.findReportEmails(since);
      options?.onPoll?.(emails.length);

      if (emails.length >= expectedCount) {
        return emails;
      }

      // Wait before polling again
      await new Promise((resolve) => setTimeout(resolve, interval));
    }

    // Return whatever we found (may be partial)
    return this.findReportEmails(since);
  }
}
