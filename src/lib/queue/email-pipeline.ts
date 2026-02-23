/**
 * Email-based pipeline orchestrator.
 *
 * Flow:
 *  1. Playwright clicks "Download CSV" on each Union.fit report page (~30s)
 *  2. Some reports download directly in the browser (captured by Playwright)
 *  3. Others: Union.fit emails the CSVs to the robot account
 *  4. Gmail API polls the inbox for CSV attachments (~30-900s)
 *  5. Downloads attachments to disk
 *  6. Maps email subjects to report types
 *  7. Merges direct downloads + email downloads
 *  8. Calls runPipelineFromFiles() — existing parse -> analyze -> export
 *
 * Dual delivery: Union.fit delivers CSVs either as direct browser downloads
 * (small reports) or via email (large reports). The trigger phase captures
 * direct downloads automatically and only waits for emails for the remainder.
 */

import { triggerCSVDownloads, type TriggerResult } from "../scraper/csv-trigger";
import { GmailClient, type ReportEmail } from "../email/gmail-client";
import { extractCSVsFromZip } from "../email/zip-extract";
import { runPipelineFromFiles } from "./pipeline-core";
import type { PipelineResult } from "@/types/pipeline";
import type { DownloadedFiles } from "@/types/union-data";
import type { ReportType } from "../scraper/selectors";

type ProgressCallback = (step: string, percent: number) => void;

export interface EmailPipelineOptions {
  /** Union.fit login email (robot@skyting.com) */
  unionEmail: string;
  /** Union.fit login password */
  unionPassword: string;
  /** Robot email address for reading emails (same as unionEmail typically) */
  robotEmail: string;
  /** @deprecated No longer used — DB is source of truth */
  analyticsSheetId?: string;
  /** Date range string, e.g. "1/1/2025 - 2/16/2025" */
  dateRange?: string;
  /** Progress callback */
  onProgress?: ProgressCallback;
  /** Max time to wait for emails (ms). Default 900_000 (15 min) -- Union.fit is slow. */
  emailTimeoutMs?: number;
}

/**
 * Map email subjects from Union.fit to report types.
 *
 * Union.fit email subjects follow patterns like:
 *   "SKY TING First Visit Registration Export" -> firstVisits
 *   "SKY TING Subscription Export Audit Report" -> subscription changes
 *   "SKY TING Registration Export CSV" -> allRegistrations/fullRegistrations
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

/**
 * Try to determine the report type from an email subject.
 * Returns undefined if no match found.
 */
function subjectToReportType(subject: string): ReportType | undefined {
  for (const { pattern, reportType } of SUBJECT_TO_REPORT) {
    if (pattern.test(subject)) return reportType;
  }
  return undefined;
}

/**
 * Also try to match from the attachment filename.
 * Union.fit CSV filenames often contain the report name.
 */
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
 * Run the full email-based pipeline.
 *
 * 1. Trigger CSV downloads on Union.fit (Playwright clicks buttons)
 * 2. Use direct downloads immediately for reports that downloaded in-browser
 * 3. Wait for emails for reports that are pending email delivery
 * 4. Download CSV attachments from emails
 * 5. Map to report types
 * 6. Run analytics pipeline
 */
export async function runEmailPipeline(options: EmailPipelineOptions): Promise<PipelineResult> {
  const {
    unionEmail,
    unionPassword,
    robotEmail,
    onProgress,
    emailTimeoutMs = 900_000,
  } = options;

  const progress = onProgress ?? (() => {});
  const dateRange = options.dateRange ?? buildDefaultDateRange();

  // ── Phase 1: Trigger CSV downloads on Union.fit ──────────────
  progress("Phase 1: Triggering CSV downloads on Union.fit", 5);

  let triggerResult: TriggerResult;
  try {
    triggerResult = await triggerCSVDownloads(unionEmail, unionPassword, dateRange, {
      onProgress: (step) => progress(step, 10),
      includeOptional: true,
    });
  } catch (err) {
    throw new Error(
      `Failed to trigger CSV downloads: ${err instanceof Error ? err.message : String(err)}`
    );
  }

  const expectedCount = triggerResult.triggered.length;
  if (expectedCount === 0) {
    throw new Error(
      `No CSV downloads were triggered. Failed reports: ${triggerResult.failed.map((f) => f.report).join(", ")}`
    );
  }

  // Start with direct downloads already captured
  const reportFiles: Partial<Record<ReportType, string>> = {};
  for (const { report, filePath } of triggerResult.directDownloads) {
    reportFiles[report as ReportType] = filePath;
    console.log(`[email-pipeline] Direct download available: ${report} -> ${filePath}`);
  }

  const directCount = triggerResult.directDownloads.length;
  const emailPendingCount = triggerResult.emailPending.length;

  console.log(`[email-pipeline] Triggered: ${triggerResult.triggered.join(", ")}`);
  console.log(`[email-pipeline] Direct downloads: ${directCount}, email pending: ${emailPendingCount}`);
  if (triggerResult.failed.length > 0) {
    console.warn(
      `[email-pipeline] Failed to trigger: ${triggerResult.failed.map((f) => `${f.report}: ${f.error}`).join("; ")}`
    );
  }

  // ── Phase 2: Quick email check (don't block — save what we have) ──
  // Instead of waiting 25 min for all emails, do a quick poll (3 min max).
  // Whatever we have after that gets saved. Emails that arrive later will
  // be picked up on the next pipeline run.
  const EMAIL_QUICK_TIMEOUT_MS = 3 * 60 * 1000; // 3 minutes

  if (emailPendingCount > 0) {
    progress(`Phase 2: Quick email check for ${emailPendingCount} pending reports (${directCount} already downloaded)`, 25);

    const triggerTime = new Date(Date.now() - 60_000); // Look back 1 minute to catch fast emails
    const gmail = new GmailClient({ robotEmail });

    let emails: ReportEmail[] = [];
    try {
      emails = await gmail.waitForReportEmails(triggerTime, emailPendingCount, {
        timeoutMs: EMAIL_QUICK_TIMEOUT_MS,
        pollIntervalMs: 10_000,
        onPoll: (found) => {
          const pct = 25 + Math.min(20, (found / emailPendingCount) * 20);
          progress(`Received ${found}/${emailPendingCount} CSV emails (${directCount} direct downloads already captured)...`, Math.round(pct));
        },
      });
    } catch (err) {
      console.warn(`[email-pipeline] Email check failed (non-fatal): ${err instanceof Error ? err.message : String(err)}`);
    }

    if (emails.length > 0) {
      progress(`Received ${emails.length} emails, downloading attachments...`, 45);
      console.log(`[email-pipeline] Found ${emails.length} report emails`);

      // ── Phase 3: Download email attachments & map to report types ──
      progress("Phase 3: Downloading CSV attachments from email", 48);

      const downloaded = await gmail.downloadAllAttachments(emails);

      for (const email of emails) {
        for (const att of email.attachments) {
          const key = `${email.subject}::${att.filename}`;
          const filePath = downloaded.get(key);
          if (!filePath) continue;

          // If it's a .zip, extract CSVs and map each one individually
          if (att.filename.toLowerCase().endsWith(".zip")) {
            console.log(`[email-pipeline] Extracting zip: ${att.filename}`);
            const extracted = extractCSVsFromZip(filePath);
            for (const csv of extracted) {
              const reportType = filenameToReportType(csv.originalName) ?? subjectToReportType(email.subject);
              if (reportType) {
                if (!reportFiles[reportType]) {
                  reportFiles[reportType] = csv.filePath;
                  console.log(`[email-pipeline] Mapped zip entry ${csv.originalName} -> ${reportType}`);
                }
              } else {
                console.warn(`[email-pipeline] Could not map zip entry: ${csv.originalName}`);
              }
            }
            continue;
          }

          // Regular CSV — try subject first, then filename
          const reportType = subjectToReportType(email.subject) ?? filenameToReportType(att.filename);

          if (reportType) {
            if (!reportFiles[reportType]) {
              reportFiles[reportType] = filePath;
              console.log(`[email-pipeline] Mapped email "${email.subject}" / ${att.filename} -> ${reportType}`);
            } else {
              console.log(`[email-pipeline] Skipping email for ${reportType} — already have direct download`);
            }
          } else {
            console.warn(
              `[email-pipeline] Could not map email to report type: "${email.subject}" / ${att.filename}`
            );
          }
        }
      }

      // Mark emails as read
      for (const email of emails) {
        try {
          await gmail.markAsRead(email.id);
        } catch {
          // Non-critical
        }
      }
    } else {
      console.log(`[email-pipeline] No emails arrived in ${EMAIL_QUICK_TIMEOUT_MS / 1000}s — proceeding with direct downloads only`);
    }
  } else {
    progress("All reports downloaded directly — no emails to wait for!", 45);
    console.log(`[email-pipeline] All ${directCount} reports downloaded directly — skipping email phase`);
  }

  // ── Phase 4: Check what we have (warn on missing, but proceed with partial data) ──
  const allReports: ReportType[] = [
    "newCustomers",
    "orders",
    "firstVisits",
    "fullRegistrations",
    "canceledAutoRenews",
    "activeAutoRenews",
    "pausedAutoRenews",
    "trialingAutoRenews",
    "newAutoRenews",
  ];

  const collected = Object.keys(reportFiles);
  const missing = allReports.filter((r) => !reportFiles[r]);
  if (missing.length > 0) {
    console.warn(`[email-pipeline] Missing reports (will proceed with partial data): ${missing.join(", ")}`);
    console.log(`[email-pipeline] Have: ${collected.join(", ")}`);
  }

  if (collected.length === 0) {
    throw new Error("No CSV reports collected at all — nothing to process.");
  }

  progress(`Running analytics pipeline with ${collected.length}/${allReports.length} reports...`, 50);

  // ── Phase 5: Run the existing pipeline with whatever we have ──
  const files: DownloadedFiles = {
    newCustomers: reportFiles.newCustomers,
    orders: reportFiles.orders,
    firstVisits: reportFiles.firstVisits,
    allRegistrations: reportFiles.allRegistrations,
    canceledAutoRenews: reportFiles.canceledAutoRenews,
    activeAutoRenews: reportFiles.activeAutoRenews,
    pausedAutoRenews: reportFiles.pausedAutoRenews,
    trialingAutoRenews: reportFiles.trialingAutoRenews,
    newAutoRenews: reportFiles.newAutoRenews,
    revenueCategories: reportFiles.revenueCategories,
    fullRegistrations: reportFiles.fullRegistrations,
  };

  const result = await runPipelineFromFiles(files, undefined, {
    dateRange,
    onProgress: (step, percent) => {
      // Map pipeline-core's 50-100% range to our 50-100% range
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
