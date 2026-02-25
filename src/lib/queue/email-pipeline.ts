/**
 * Email-based pipeline orchestrator (streaming version).
 *
 * Flow:
 *  1. Playwright clicks "Download CSV" on each Union.fit report page (~30s)
 *  2. Direct downloads are parsed + saved to DB immediately
 *  3. Gmail polling runs up to 60 min — each email is processed the instant it lands
 *  4. Cross-category analytics run after all data is in (or timeout)
 *
 * Key difference from legacy: data is saved to DB per-category as it arrives,
 * not batch-all-then-process. The UI shows per-category progress in real time.
 */

import { triggerCSVDownloads, type TriggerResult } from "../scraper/csv-trigger";
import { GmailClient, type ReportEmail } from "../email/gmail-client";
import { extractCSVsFromZip } from "../email/zip-extract";
import { runPipelineFromFiles } from "./pipeline-core";
import { parseCSV } from "../parser/csv-parser";
import {
  NewCustomerSchema,
  OrderSchema,
  FirstVisitSchema,
  AutoRenewSchema,
  RevenueCategorySchema,
  FullRegistrationSchema,
} from "../parser/schemas";
import type {
  NewCustomer,
  Order,
  FirstVisit,
  AutoRenew,
  RevenueCategory,
  FullRegistration,
} from "@/types/union-data";
import { saveAutoRenews, type AutoRenewRow } from "../db/auto-renew-store";
import { saveRegistrations, saveFirstVisits, type RegistrationRow } from "../db/registration-store";
import { saveOrders, type OrderRow } from "../db/order-store";
import { saveCustomers, type CustomerRow } from "../db/customer-store";
import { saveRevenueCategories, lockPeriod } from "../db/revenue-store";
import { setWatermark, getWatermark, buildDateRangeForReport } from "../db/watermark-store";
import type { PipelineResult, CategoryProgress } from "@/types/pipeline";
import type { DownloadedFiles } from "@/types/union-data";
import type { ReportType } from "../scraper/selectors";

// ── Types ────────────────────────────────────────────────────────

type StreamingProgressCallback = (
  step: string,
  percent: number,
  categories?: Record<string, CategoryProgress>
) => void;

export interface EmailPipelineOptions {
  unionEmail: string;
  unionPassword: string;
  robotEmail: string;
  dateRange?: string;
  onProgress?: StreamingProgressCallback;
  /** Max time to wait for emails (ms). Default 3_600_000 (60 min). */
  emailTimeoutMs?: number;
}

// ── Email → Report Type Mapping ──────────────────────────────────

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

// ── All expected report types (excluding optional ones) ─────────

const ALL_REPORTS: ReportType[] = [
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

/**
 * Map ReportType → watermark key in fetch_watermarks table.
 * All auto-renew sub-types share the "autoRenews" watermark.
 * Subscription list reports (active, paused, trialing) are snapshots —
 * they don't use date ranges, but we still track freshness.
 */
const REPORT_TO_WATERMARK: Record<string, string> = {
  newCustomers: "newCustomers",
  orders: "orders",
  firstVisits: "firstVisits",
  fullRegistrations: "registrations",
  canceledAutoRenews: "autoRenews",
  activeAutoRenews: "autoRenews",
  pausedAutoRenews: "autoRenews",
  trialingAutoRenews: "autoRenews",
  newAutoRenews: "autoRenews",
  revenueCategories: "revenueCategories",
};

/**
 * Build per-report date ranges from watermarks.
 * Each report gets its own range based on how fresh its data is.
 * Reports already up-to-date get a small range → small CSV → direct download.
 * Reports with no watermark get full backfill from Jan 2024.
 */
async function buildPerReportDateRanges(): Promise<Record<string, string>> {
  const ranges: Record<string, string> = {};
  const seen = new Set<string>();

  for (const report of ALL_REPORTS) {
    const wmKey = REPORT_TO_WATERMARK[report] || report;

    // Avoid duplicate watermark lookups for reports sharing a key
    if (!seen.has(wmKey)) {
      seen.add(wmKey);
      try {
        const wm = await getWatermark(wmKey);
        const range = buildDateRangeForReport(wm);
        // Store under the watermark key for shared lookup
        ranges[`_wm_${wmKey}`] = range;
      } catch {
        // No watermark — full backfill
      }
    }

    // Use the shared watermark range, or fall back to full backfill
    ranges[report] = ranges[`_wm_${wmKey}`] || buildDateRangeForReport(null);
  }

  // Log the per-report ranges
  for (const report of ALL_REPORTS) {
    console.log(`[streaming] Date range for ${report}: ${ranges[report]}`);
  }

  return ranges;
}

// Human-readable labels for the UI
const REPORT_LABELS: Record<string, string> = {
  newCustomers: "New Customers",
  orders: "Orders",
  firstVisits: "First Visits",
  fullRegistrations: "Registrations",
  canceledAutoRenews: "Canceled Auto-Renews",
  activeAutoRenews: "Active Auto-Renews",
  pausedAutoRenews: "Paused Auto-Renews",
  trialingAutoRenews: "Trialing Auto-Renews",
  newAutoRenews: "New Auto-Renews",
  revenueCategories: "Revenue Categories",
  allRegistrations: "All Registrations",
};

// ── Per-Category Save Logic ──────────────────────────────────────
// Extracted from pipeline-core.ts so each category can be saved independently
// as soon as its CSV arrives (direct download or email).

const AUTO_RENEW_TYPES = new Set<ReportType>([
  "canceledAutoRenews",
  "activeAutoRenews",
  "pausedAutoRenews",
  "trialingAutoRenews",
  "newAutoRenews",
]);

/**
 * Parse a single CSV and save it to the database immediately.
 * Returns the number of records saved.
 */
async function processAndSaveCategory(
  reportType: ReportType,
  filePath: string,
  dateRange: string,
  snapshotId: string,
): Promise<number> {
  // ── Auto-renew reports ──
  if (AUTO_RENEW_TYPES.has(reportType)) {
    const result = parseCSV<AutoRenew>(filePath, AutoRenewSchema);
    if (result.data.length === 0) return 0;
    const rows: AutoRenewRow[] = result.data.map((ar) => ({
      planName: ar.name,
      planState: ar.state,
      planPrice: ar.price,
      customerName: ar.customer,
      customerEmail: ar.email || "",
      createdAt: ar.created || "",
      canceledAt: ar.canceledAt || undefined,
    }));
    await saveAutoRenews(snapshotId, rows);
    const latestDate = rows.reduce((max, r) => (r.createdAt > max ? r.createdAt : max), "");
    if (latestDate) {
      await setWatermark("autoRenews", latestDate.slice(0, 10), rows.length, `pipeline snapshot ${snapshotId}`);
    }
    console.log(`[streaming] Saved ${rows.length} ${reportType} to database`);
    return rows.length;
  }

  // ── Orders ──
  if (reportType === "orders") {
    const result = parseCSV<Order>(filePath, OrderSchema);
    if (result.data.length === 0) return 0;
    const rows: OrderRow[] = result.data.map((o) => ({
      created: o.created,
      code: o.code,
      customer: o.customer,
      email: o.email || "",
      type: o.type,
      payment: o.payment,
      total: o.total,
    }));
    await saveOrders(rows);
    const latestDate = rows.reduce((max, r) => (r.created > max ? r.created : max), "");
    if (latestDate) await setWatermark("orders", latestDate.slice(0, 10), rows.length, "pipeline run");
    console.log(`[streaming] Saved ${rows.length} orders to database`);
    return rows.length;
  }

  // ── New Customers ──
  if (reportType === "newCustomers") {
    const result = parseCSV<NewCustomer>(filePath, NewCustomerSchema);
    if (result.data.length === 0) return 0;
    const rows: CustomerRow[] = result.data.map((c) => ({
      name: c.name,
      email: c.email,
      role: c.role,
      orders: c.orders,
      created: c.created,
    }));
    await saveCustomers(rows);
    const latestDate = rows.reduce((max, r) => (r.created > max ? r.created : max), "");
    if (latestDate) await setWatermark("newCustomers", latestDate.slice(0, 10), rows.length, "pipeline run");
    console.log(`[streaming] Saved ${rows.length} customers to database`);
    return rows.length;
  }

  // ── First Visits ──
  if (reportType === "firstVisits") {
    const result = parseCSV<FirstVisit>(filePath, FirstVisitSchema);
    if (result.data.length === 0) return 0;
    const rows: RegistrationRow[] = result.data.map((fv) => ({
      eventName: fv.performance || "",
      performanceStartsAt: "",
      locationName: "",
      teacherName: "",
      firstName: "",
      lastName: "",
      email: "",
      attendedAt: fv.redeemedAt || "",
      registrationType: fv.type || "",
      state: fv.status || "",
      pass: fv.pass || "",
      subscription: "false",
      revenue: 0,
    }));
    await saveFirstVisits(rows);
    const latestDate = rows.reduce((max, r) => (r.attendedAt > max ? r.attendedAt : max), "");
    if (latestDate) await setWatermark("firstVisits", latestDate.slice(0, 10), rows.length, "pipeline run");
    console.log(`[streaming] Saved ${rows.length} first visits to database`);
    return rows.length;
  }

  // ── Full Registrations ──
  if (reportType === "fullRegistrations" || reportType === "allRegistrations") {
    const result = parseCSV<FullRegistration>(filePath, FullRegistrationSchema);
    if (result.data.length === 0) return 0;
    const rows: RegistrationRow[] = result.data.map((r) => ({
      eventName: r.eventName,
      eventId: r.eventId || undefined,
      performanceId: r.performanceId || undefined,
      performanceStartsAt: r.performanceStartsAt || "",
      locationName: r.locationName,
      videoName: r.videoName || undefined,
      videoId: r.videoId || undefined,
      teacherName: r.teacherName,
      firstName: r.firstName,
      lastName: r.lastName,
      email: r.email,
      phone: r.phoneNumber || undefined,
      role: r.role || undefined,
      registeredAt: r.registeredAt || undefined,
      canceledAt: r.canceledAt || undefined,
      attendedAt: r.attendedAt,
      registrationType: r.registrationType,
      state: r.state,
      pass: r.pass,
      subscription: String(r.subscription),
      revenueState: r.revenueState || undefined,
      revenue: r.revenue,
    }));
    await saveRegistrations(rows);
    const latestDate = rows.reduce((max, r) => (r.attendedAt > max ? r.attendedAt : max), "");
    if (latestDate) await setWatermark("registrations", latestDate.slice(0, 10), rows.length, "pipeline run");
    console.log(`[streaming] Saved ${rows.length} registrations to database`);
    return rows.length;
  }

  // ── Revenue Categories ──
  if (reportType === "revenueCategories") {
    const result = parseCSV<RevenueCategory>(filePath, RevenueCategorySchema);
    if (result.data.length === 0) return 0;
    const drParts = dateRange.split(" - ").map((s) => s.trim());
    const toISO = (s: string): string => {
      const d = new Date(s);
      return !isNaN(d.getTime()) ? d.toISOString().slice(0, 10) : s;
    };
    const periodStart = toISO(drParts[0] || new Date().toISOString().slice(0, 10));
    const periodEnd = toISO(drParts[1] || new Date().toISOString().slice(0, 10));
    await saveRevenueCategories(periodStart, periodEnd, result.data);
    await setWatermark("revenueCategories", periodEnd, result.data.length, `pipeline run ${periodStart} to ${periodEnd}`);
    // Auto-lock completed periods
    const currentMonthStart = new Date(new Date().getFullYear(), new Date().getMonth(), 1);
    const periodEndDate = new Date(periodEnd);
    if (!isNaN(periodEndDate.getTime()) && periodEndDate < currentMonthStart) {
      await lockPeriod(periodStart, periodEnd);
    }
    console.log(`[streaming] Saved ${result.data.length} revenue categories to database`);
    return result.data.length;
  }

  console.warn(`[streaming] No save handler for report type: ${reportType}`);
  return 0;
}

// ── Email Processing ─────────────────────────────────────────────

/**
 * Process a single email: download attachment, map to report type, parse + save.
 */
async function processEmailAttachments(
  email: ReportEmail,
  gmail: GmailClient,
  categories: Record<string, CategoryProgress>,
  reportFiles: Partial<Record<ReportType, string>>,
  dateRanges: Record<string, string>,
  snapshotId: string,
  emitProgress: () => void,
): Promise<void> {
  const downloaded = await gmail.downloadAllAttachments([email]);

  for (const att of email.attachments) {
    const key = `${email.subject}::${att.filename}`;
    const filePath = downloaded.get(key);
    if (!filePath) continue;

    // Handle ZIP files
    if (att.filename.toLowerCase().endsWith(".zip")) {
      console.log(`[streaming] Extracting zip: ${att.filename}`);
      const extracted = extractCSVsFromZip(filePath);
      for (const csv of extracted) {
        const reportType = filenameToReportType(csv.originalName) ?? subjectToReportType(email.subject);
        if (reportType && !reportFiles[reportType]) {
          const range = dateRanges[reportType] || '';
          await processSingleFile(reportType, csv.filePath, categories, reportFiles, range, snapshotId, "email");
          emitProgress();
        }
      }
      continue;
    }

    // Regular CSV
    const reportType = subjectToReportType(email.subject) ?? filenameToReportType(att.filename);
    if (reportType && !reportFiles[reportType]) {
      const range = dateRanges[reportType] || '';
      await processSingleFile(reportType, filePath, categories, reportFiles, range, snapshotId, "email");
      emitProgress();
    } else if (reportType && reportFiles[reportType]) {
      console.log(`[streaming] Skipping email for ${reportType} -- already have data`);
    } else {
      console.warn(`[streaming] Could not map email: "${email.subject}" / ${att.filename}`);
    }
  }

  // Mark email as read
  try { await gmail.markAsRead(email.id); } catch { /* non-critical */ }
}

/**
 * Parse a CSV file, save to DB, and update tracking state.
 */
async function processSingleFile(
  reportType: ReportType,
  filePath: string,
  categories: Record<string, CategoryProgress>,
  reportFiles: Partial<Record<ReportType, string>>,
  dateRange: string,
  snapshotId: string,
  method: "direct" | "email",
): Promise<void> {
  categories[reportType] = { state: "parsing", deliveryMethod: method };
  try {
    const count = await processAndSaveCategory(reportType, filePath, dateRange, snapshotId);
    reportFiles[reportType] = filePath;
    categories[reportType] = { state: "saved", recordCount: count, deliveryMethod: method };
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    console.error(`[streaming] Failed to save ${reportType}: ${msg}`);
    categories[reportType] = { state: "failed", error: msg, deliveryMethod: method };
  }
}

// ── Gmail Long-Polling ───────────────────────────────────────────

async function pollAndProcessEmails(
  gmail: GmailClient,
  emailPending: string[],
  categories: Record<string, CategoryProgress>,
  reportFiles: Partial<Record<ReportType, string>>,
  dateRanges: Record<string, string>,
  snapshotId: string,
  timeoutMs: number,
  emitProgress: () => void,
): Promise<void> {
  const triggerTime = new Date(Date.now() - 60_000); // Look back 1 min
  const startTime = Date.now();
  const processedEmailIds = new Set<string>();
  const pendingSet = new Set(emailPending);

  console.log(`[streaming] Polling Gmail for ${pendingSet.size} pending reports (timeout: ${timeoutMs / 1000}s)`);

  while (Date.now() - startTime < timeoutMs) {
    // Check if all pending reports have been satisfied
    const remaining = [...pendingSet].filter((r) => !reportFiles[r as ReportType]);
    if (remaining.length === 0) {
      console.log(`[streaming] All email-pending reports received -- stopping poll`);
      break;
    }

    // Poll Gmail
    let emails: ReportEmail[] = [];
    try {
      emails = await gmail.findReportEmails(triggerTime);
    } catch (err) {
      console.warn(`[streaming] Gmail poll error (will retry): ${err instanceof Error ? err.message : String(err)}`);
    }

    // Process only NEW emails
    const newEmails = emails.filter((e) => !processedEmailIds.has(e.id));
    for (const email of newEmails) {
      processedEmailIds.add(email.id);
      console.log(`[streaming] New email: "${email.subject}" (${email.attachments.length} attachments)`);
      try {
        await processEmailAttachments(email, gmail, categories, reportFiles, dateRanges, snapshotId, emitProgress);
      } catch (err) {
        console.error(`[streaming] Failed processing email "${email.subject}": ${err instanceof Error ? err.message : String(err)}`);
      }
    }

    if (newEmails.length > 0) {
      // Update progress with remaining count
      const stillWaiting = [...pendingSet].filter((r) => !reportFiles[r as ReportType]);
      if (stillWaiting.length > 0) {
        console.log(`[streaming] Still waiting for: ${stillWaiting.join(", ")}`);
      }
    }

    // Wait 30 seconds before next poll (60-min window, no need to hammer)
    await new Promise((resolve) => setTimeout(resolve, 30_000));
  }

  const elapsed = Math.round((Date.now() - startTime) / 1000);
  const unsatisfied = [...pendingSet].filter((r) => !reportFiles[r as ReportType]);
  if (unsatisfied.length > 0) {
    console.warn(`[streaming] Email poll ended after ${elapsed}s. Missing: ${unsatisfied.join(", ")}`);
  } else {
    console.log(`[streaming] Email poll complete in ${elapsed}s -- all reports received`);
  }
}

// ── Main Orchestrator ────────────────────────────────────────────

export async function runEmailPipeline(options: EmailPipelineOptions): Promise<PipelineResult> {
  const {
    unionEmail,
    unionPassword,
    robotEmail,
    onProgress,
    emailTimeoutMs = 3_600_000, // 60 minutes
  } = options;

  const snapshotId = `pipeline-${Date.now()}`;

  // Category tracking state
  const categories: Record<string, CategoryProgress> = {};
  const reportFiles: Partial<Record<ReportType, string>> = {};

  // Initialize all known categories as pending
  for (const r of ALL_REPORTS) {
    categories[r] = { state: "pending" };
  }

  // Progress helper: computes overall % from category states
  const totalCategories = ALL_REPORTS.length;
  const progress = onProgress ?? (() => {});

  function emitProgress(step?: string) {
    const savedCount = Object.values(categories).filter((c) => c.state === "saved").length;
    const failedCount = Object.values(categories).filter((c) => c.state === "failed").length;
    const doneCount = savedCount + failedCount;
    // 0-15%: trigger phase, 15-75%: per-category saves, 75-100%: analytics
    const pct = 15 + Math.round((doneCount / totalCategories) * 60);
    const label = step || `${savedCount} of ${totalCategories} categories saved`;
    progress(label, Math.min(pct, 75), categories);
  }

  // ── Build per-report date ranges from watermarks ─────────────
  // Each report gets its own date range based on how fresh its DB data is.
  // Reports already up-to-date get a tiny range → small CSV → direct download.
  progress("Building per-report date ranges from watermarks", 3, categories);
  let dateRanges: Record<string, string>;
  if (options.dateRange) {
    // Manual override: use the same range for all reports
    dateRanges = {};
    for (const r of ALL_REPORTS) dateRanges[r] = options.dateRange;
  } else {
    dateRanges = await buildPerReportDateRanges();
  }

  // ── Phase 1: Trigger CSV downloads on Union.fit ──────────────
  progress("Triggering CSV downloads on Union.fit", 5, categories);

  let triggerResult: TriggerResult;
  try {
    triggerResult = await triggerCSVDownloads(unionEmail, unionPassword, dateRanges, {
      onProgress: (step) => progress(step, 10, categories),
      includeOptional: true,
    });
  } catch (err) {
    throw new Error(
      `Failed to trigger CSV downloads: ${err instanceof Error ? err.message : String(err)}`
    );
  }

  // Mark trigger outcomes in category state
  for (const { report } of triggerResult.directDownloads) {
    categories[report] = { state: "downloading", deliveryMethod: "direct" };
  }
  for (const report of triggerResult.emailPending) {
    categories[report] = { state: "pending", deliveryMethod: "email" };
  }
  for (const { report, error } of triggerResult.failed) {
    categories[report] = { state: "failed", error, deliveryMethod: undefined };
  }

  const directCount = triggerResult.directDownloads.length;
  const emailPendingCount = triggerResult.emailPending.length;

  console.log(`[streaming] Triggered: ${triggerResult.triggered.join(", ")}`);
  console.log(`[streaming] Direct: ${directCount}, Email pending: ${emailPendingCount}, Failed: ${triggerResult.failed.length}`);

  if (triggerResult.triggered.length === 0 && triggerResult.directDownloads.length === 0) {
    throw new Error(
      `No CSV downloads were triggered. Failed: ${triggerResult.failed.map((f) => f.report).join(", ")}`
    );
  }

  emitProgress("Processing downloads...");

  // ── Phase 2: Process direct downloads + poll emails concurrently ──

  // 2a: Process direct downloads immediately (in parallel)
  const directSavePromises = triggerResult.directDownloads.map(async ({ report, filePath }) => {
    await processSingleFile(
      report as ReportType, filePath, categories, reportFiles,
      dateRanges[report] || dateRanges['_default'] || '', snapshotId, "direct"
    );
    emitProgress();
  });

  // 2b: Start email polling concurrently (don't await direct downloads first)
  let emailPollPromise: Promise<void> = Promise.resolve();
  if (emailPendingCount > 0) {
    const gmail = new GmailClient({ robotEmail });
    emailPollPromise = pollAndProcessEmails(
      gmail, triggerResult.emailPending, categories, reportFiles,
      dateRanges, snapshotId, emailTimeoutMs, () => emitProgress()
    );
  }

  // Wait for both direct processing and email polling to complete
  await Promise.allSettled(directSavePromises);
  await emailPollPromise;

  // ── Summary ──
  const savedCount = Object.values(categories).filter((c) => c.state === "saved").length;
  const collected = Object.keys(reportFiles);
  console.log(`[streaming] Final: ${savedCount}/${totalCategories} categories saved (${collected.join(", ")})`);

  if (collected.length === 0) {
    throw new Error("No CSV reports collected at all -- nothing to process.");
  }

  // ── Phase 3: Cross-category analytics ──────────────────────────
  progress(`Running analytics (${savedCount}/${totalCategories} categories)...`, 78, categories);

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

  // Use the widest date range across all reports for analytics
  const widestRange = options.dateRange || computeWidestRange(dateRanges);
  const result = await runPipelineFromFiles(files, undefined, {
    dateRange: widestRange,
    onProgress: (step, percent) => {
      progress(step, Math.max(78, percent), categories);
    },
  });

  progress("Pipeline complete!", 100, categories);
  return result;
}

// ── Helpers ──────────────────────────────────────────────────────

function buildDefaultDateRange(): string {
  const now = new Date();
  const start = new Date(now.getFullYear() - 1, now.getMonth(), now.getDate());
  const startStr = `${start.getMonth() + 1}/${start.getDate()}/${start.getFullYear()}`;
  const endStr = `${now.getMonth() + 1}/${now.getDate()}/${now.getFullYear()}`;
  return `${startStr} - ${endStr}`;
}

/**
 * Compute the widest date range from a map of per-report ranges.
 * Used for the cross-category analytics phase.
 */
function computeWidestRange(dateRanges: Record<string, string>): string {
  let earliestStart = '';
  let latestEnd = '';

  for (const range of Object.values(dateRanges)) {
    if (!range || range.startsWith('_')) continue;
    const parts = range.split(' - ').map((s) => s.trim());
    if (parts.length !== 2) continue;
    if (!earliestStart || parts[0] < earliestStart) earliestStart = parts[0];
    if (!latestEnd || parts[1] > latestEnd) latestEnd = parts[1];
  }

  if (earliestStart && latestEnd) return `${earliestStart} - ${latestEnd}`;
  return buildDefaultDateRange();
}

// Export labels for use in the UI
export { REPORT_LABELS };
