/**
 * Gmail-only pipeline — no browser, no scraping.
 *
 * Reads CSV report emails from the robot inbox (sent by Union.fit)
 * and runs the analytics pipeline from those files.
 *
 * Flow:
 *  1. Poll Gmail for recent Union.fit CSV emails (sent within lookbackHours)
 *  2. Download CSV attachments
 *  3. Map email subjects/filenames to report types
 *  4. Run analytics pipeline from downloaded files
 *
 * The CSV emails are triggered manually by the user clicking
 * "Download CSV" in Union.fit, or by Union.fit's own scheduled exports.
 * This pipeline just picks them up from the inbox.
 */

import { GmailClient, type ReportEmail } from "../email/gmail-client";
import { runPipelineFromFiles } from "./pipeline-core";
import type { PipelineResult } from "@/types/pipeline";
import type { DownloadedFiles } from "@/types/union-data";
import type { ReportType } from "../scraper/selectors";

type ProgressCallback = (step: string, percent: number) => void;

export interface GmailPipelineOptions {
  /** Robot email address for reading emails */
  robotEmail: string;
  /** @deprecated No longer used — DB is source of truth */
  analyticsSheetId?: string;
  /** Date range string, e.g. "1/1/2025 - 2/16/2025" */
  dateRange?: string;
  /** Progress callback */
  onProgress?: ProgressCallback;
  /** How many hours back to look for emails. Default 24. */
  lookbackHours?: number;
}

/**
 * Map email subjects from Union.fit to report types.
 */
const SUBJECT_TO_REPORT: { pattern: RegExp; reportType: ReportType }[] = [
  { pattern: /first.?visit/i, reportType: "firstVisits" },
  { pattern: /new.?customer/i, reportType: "newCustomers" },
  { pattern: /order|transaction/i, reportType: "orders" },
  { pattern: /cancel/i, reportType: "canceledAutoRenews" },
  { pattern: /new.*(auto.?renew|subscription)/i, reportType: "newAutoRenews" },
  { pattern: /active.*(auto.?renew|subscription)/i, reportType: "activeAutoRenews" },
  { pattern: /pause/i, reportType: "pausedAutoRenews" },
  { pattern: /trial/i, reportType: "trialingAutoRenews" },
  { pattern: /registration/i, reportType: "allRegistrations" },
  { pattern: /revenue.?categor/i, reportType: "revenueCategories" },
  { pattern: /full.?registration/i, reportType: "fullRegistrations" },
];

function subjectToReportType(subject: string): ReportType | undefined {
  for (const { pattern, reportType } of SUBJECT_TO_REPORT) {
    if (pattern.test(subject)) return reportType;
  }
  return undefined;
}

function filenameToReportType(filename: string): ReportType | undefined {
  const lower = filename.toLowerCase();
  if (lower.includes("first_visit") || lower.includes("first-visit")) return "firstVisits";
  if (lower.includes("new_customer") || lower.includes("new-customer")) return "newCustomers";
  if (lower.includes("order") || lower.includes("transaction")) return "orders";
  if (lower.includes("cancel")) return "canceledAutoRenews";
  if (lower.includes("new") && (lower.includes("renew") || lower.includes("subscription"))) return "newAutoRenews";
  if (lower.includes("active") && (lower.includes("renew") || lower.includes("subscription"))) return "activeAutoRenews";
  if (lower.includes("pause")) return "pausedAutoRenews";
  if (lower.includes("trial")) return "trialingAutoRenews";
  if (lower.includes("registration") && !lower.includes("first")) return "allRegistrations";
  if (lower.includes("revenue")) return "revenueCategories";
  return undefined;
}

/**
 * Run the Gmail-only pipeline.
 *
 * 1. Find recent Union.fit CSV emails in the robot inbox
 * 2. Download CSV attachments
 * 3. Map to report types
 * 4. Run analytics pipeline
 */
export async function runGmailPipeline(options: GmailPipelineOptions): Promise<PipelineResult> {
  const {
    robotEmail,
    onProgress,
    lookbackHours = 24,
  } = options;

  const progress = onProgress ?? (() => {});
  const dateRange = options.dateRange ?? buildDefaultDateRange();

  // ── Phase 1: Find recent CSV emails ──────────────────────────
  progress("Scanning Gmail for recent Union.fit CSV reports...", 5);

  const lookbackTime = new Date(Date.now() - lookbackHours * 60 * 60 * 1000);
  const gmail = new GmailClient({ robotEmail });

  let emails: ReportEmail[];
  try {
    emails = await gmail.findReportEmails(lookbackTime);
  } catch (err) {
    throw new Error(
      `Failed to read Gmail: ${err instanceof Error ? err.message : String(err)}`
    );
  }

  if (emails.length === 0) {
    throw new Error(
      `No Union.fit CSV emails found in the last ${lookbackHours} hours. ` +
      `Trigger CSV exports manually from Union.fit, or increase the lookback window.`
    );
  }

  progress(`Found ${emails.length} CSV emails, downloading attachments...`, 20);
  console.log(`[gmail-pipeline] Found ${emails.length} report emails since ${lookbackTime.toISOString()}`);
  for (const e of emails) {
    console.log(`[gmail-pipeline]   "${e.subject}" (${e.attachments.length} attachments, ${e.date.toISOString()})`);
  }

  // ── Phase 2: Download attachments & map to report types ──────
  progress("Downloading CSV attachments...", 30);

  const downloaded = await gmail.downloadAllAttachments(emails);
  const reportFiles: Partial<Record<ReportType, string>> = {};

  for (const email of emails) {
    for (const att of email.attachments) {
      const key = `${email.subject}::${att.filename}`;
      const filePath = downloaded.get(key);
      if (!filePath) continue;

      const reportType = subjectToReportType(email.subject) ?? filenameToReportType(att.filename);
      if (reportType) {
        // Use the most recent email for each report type
        if (!reportFiles[reportType]) {
          reportFiles[reportType] = filePath;
          console.log(`[gmail-pipeline] Mapped "${email.subject}" / ${att.filename} -> ${reportType}`);
        }
      } else {
        console.warn(`[gmail-pipeline] Could not map: "${email.subject}" / ${att.filename}`);
      }
    }
  }

  // Mark emails as read
  for (const email of emails) {
    try {
      await gmail.markAsRead(email.id);
    } catch { /* non-critical */ }
  }

  // ── Phase 3: Validate we have enough reports ─────────────────
  const requiredReports: ReportType[] = [
    "newCustomers",
    "orders",
    "firstVisits",
    "canceledAutoRenews",
    "activeAutoRenews",
    "pausedAutoRenews",
    "trialingAutoRenews",
    "newAutoRenews",
  ];

  const have = Object.keys(reportFiles);
  const missing = requiredReports.filter((r) => !reportFiles[r]);

  console.log(`[gmail-pipeline] Have reports: ${have.join(", ")}`);
  if (missing.length > 0) {
    console.warn(`[gmail-pipeline] Missing reports: ${missing.join(", ")}`);
  }

  if (missing.length > 0) {
    throw new Error(
      `Missing required CSV reports: ${missing.join(", ")}. ` +
      `Found: ${have.join(", ")}. ` +
      `Trigger the missing reports in Union.fit and try again.`
    );
  }

  progress("All CSVs collected, running analytics pipeline...", 45);

  // ── Phase 4: Run analytics pipeline ──────────────────────────
  const files: DownloadedFiles = {
    newCustomers: reportFiles.newCustomers!,
    orders: reportFiles.orders!,
    firstVisits: reportFiles.firstVisits!,
    allRegistrations: reportFiles.allRegistrations,
    canceledAutoRenews: reportFiles.canceledAutoRenews!,
    activeAutoRenews: reportFiles.activeAutoRenews!,
    pausedAutoRenews: reportFiles.pausedAutoRenews!,
    trialingAutoRenews: reportFiles.trialingAutoRenews!,
    newAutoRenews: reportFiles.newAutoRenews!,
    revenueCategories: reportFiles.revenueCategories,
    fullRegistrations: reportFiles.fullRegistrations || "",
  };

  const result = await runPipelineFromFiles(files, undefined, {
    dateRange,
    onProgress: (step, percent) => {
      progress(step, percent);
    },
  });

  return result;
}

/**
 * Build a default date range: last 12 months to today.
 */
function buildDefaultDateRange(): string {
  const now = new Date();
  const start = new Date(now.getFullYear() - 1, now.getMonth(), now.getDate());
  const startStr = `${start.getMonth() + 1}/${start.getDate()}/${start.getFullYear()}`;
  const endStr = `${now.getMonth() + 1}/${now.getDate()}/${now.getFullYear()}`;
  return `${startStr} - ${endStr}`;
}
