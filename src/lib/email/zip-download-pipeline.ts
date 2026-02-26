/**
 * Daily Zip Download Pipeline — PRIMARY data ingestion path.
 *
 * Union.fit sends a daily email to robot@skyting.com with subject
 * "SKY TING Data Export Ready". The email body contains a "Download Now"
 * link that triggers a zip download with 33 relational CSVs.
 *
 * Flow:
 *  1. Poll Gmail for the export email
 *  2. Extract download URL from email HTML body
 *  3. HTTP fetch the zip file
 *  4. Extract CSVs from zip
 *  5. Parse small lookup tables (memberships, passes, events, etc.)
 *  6. Build ZipTransformer with lookup maps
 *  7. Transform + save auto-renews, orders, registrations, customers
 *  8. Mark email as read + set watermark
 */

import { GmailClient } from "./gmail-client";
import { extractCSVsFromZip, type ExtractedFile } from "./zip-extract";
import { ZipTransformer, type RawZipTables } from "./zip-transformer";
import {
  RawMembershipSchema,
  RawPassSchema,
  RawOrderSchema,
  RawRegistrationSchema,
  RawPerformanceSchema,
  RawEventSchema,
  RawLocationSchema,
  RawPassTypeSchema,
  RawRevenueCategoryLookupSchema,
  RawRefundSchema,
  type RawMembership,
  type RawPass,
  type RawOrder,
  type RawRegistration,
  type RawPerformance,
  type RawEvent,
  type RawLocation,
  type RawPassType,
  type RawRevenueCategoryLookup,
  type RawRefund,
} from "./zip-schemas";
import { saveAutoRenews } from "../db/auto-renew-store";
import { saveOrders } from "../db/order-store";
import { saveRegistrations } from "../db/registration-store";
import { saveCustomers } from "../db/customer-store";
import { saveRevenueCategories, isMonthLocked } from "../db/revenue-store";
import { setWatermark } from "../db/watermark-store";
import { parseCSV } from "../parser/csv-parser";
import { writeFileSync, mkdirSync, existsSync } from "fs";
import { join, basename } from "path";
import type { PipelineResult } from "@/types/pipeline";

const DOWNLOADS_DIR = join(process.cwd(), "data", "email-attachments");

type ProgressCallback = (step: string, percent: number) => void;

export interface ZipPipelineOptions {
  /** Robot email address for reading emails */
  robotEmail: string;
  /** How many hours back to look for emails. Default 48. */
  lookbackHours?: number;
  /** Progress callback */
  onProgress?: ProgressCallback;
}

/**
 * Webhook path: run pipeline from a direct download URL
 * (no Gmail polling needed).
 */
export interface ZipWebhookOptions {
  /** Direct download URL for the zip export */
  downloadUrl: string;
  /** Progress callback */
  onProgress?: ProgressCallback;
}

/**
 * Alternative: run the pipeline from a local zip directory
 * (for testing without Gmail).
 */
export interface ZipLocalOptions {
  /** Path to directory containing extracted CSVs */
  csvDir: string;
  /** Progress callback */
  onProgress?: ProgressCallback;
}

// ── URL Extraction ──────────────────────────────────────────

/**
 * Extract the download URL from the email HTML body.
 * Looks for href in the "Download Now" link.
 */
function extractDownloadUrl(html: string): string | null {
  // Try multiple patterns to find the download link

  // Pattern 1: <a ... href="URL" ...>Download Now</a>
  const downloadNowMatch = html.match(
    /href=["']([^"']+)["'][^>]*>(?:[^<]*?)Download\s*Now/i
  );
  if (downloadNowMatch) return downloadNowMatch[1];

  // Pattern 2: <a ... href="URL" ...>Download</a>
  const downloadMatch = html.match(
    /href=["']([^"']+)["'][^>]*>(?:[^<]*?)Download/i
  );
  if (downloadMatch) return downloadMatch[1];

  // Pattern 3: Any link with "download" or "export" in the URL
  const urlMatch = html.match(
    /href=["'](https?:\/\/[^"']*(?:download|export)[^"']*)["']/i
  );
  if (urlMatch) return urlMatch[1];

  return null;
}

// ── CSV File Mapping ────────────────────────────────────────

/** Map extracted filenames to table names (case-insensitive) */
function mapExtractedFiles(
  files: ExtractedFile[]
): Map<string, string> {
  const map = new Map<string, string>();
  for (const f of files) {
    // Strip timestamp prefix and .csv suffix to get the table name
    const name = f.originalName.toLowerCase().replace(/\.csv$/, "");
    map.set(name, f.filePath);
  }
  return map;
}

// ── Parse Helpers ───────────────────────────────────────────

function parseTable<T>(
  filePath: string | undefined,
  schema: { parse: (v: unknown) => T },
  tableName: string
): T[] {
  if (!filePath) {
    console.warn(`[zip-pipeline] Missing table: ${tableName}`);
    return [];
  }

  const result = parseCSV<T>(filePath, schema as never);
  if (result.warnings.length > 0) {
    console.warn(
      `[zip-pipeline] ${tableName}: ${result.warnings.length} parse warnings`
    );
  }
  console.log(`[zip-pipeline] ${tableName}: ${result.data.length} rows`);
  return result.data;
}

// ── Main Pipeline: Gmail → Download → Transform → Save ─────

export async function runZipDownloadPipeline(
  options: ZipPipelineOptions
): Promise<PipelineResult> {
  const startTime = Date.now();
  const { robotEmail, lookbackHours = 48, onProgress } = options;
  const progress = onProgress ?? (() => {});

  // ── Phase 1: Find export email ──────────────────────────
  progress("Searching Gmail for data export email...", 5);

  const lookbackTime = new Date(
    Date.now() - lookbackHours * 60 * 60 * 1000
  );
  const gmail = new GmailClient({ robotEmail });
  const exportEmails = await gmail.findExportEmails(lookbackTime);

  if (exportEmails.length === 0) {
    throw new Error(
      `No Union.fit data export emails found in the last ${lookbackHours} hours.`
    );
  }

  const latestEmail = exportEmails[0]; // Already sorted by date desc
  console.log(
    `[zip-pipeline] Found export email: "${latestEmail.subject}" (${latestEmail.date.toISOString()})`
  );

  // ── Phase 2: Extract download URL ───────────────────────
  progress("Extracting download URL from email...", 10);

  const htmlBody = await gmail.getMessageBody(latestEmail.id);
  if (!htmlBody) {
    throw new Error("Export email has no HTML body");
  }

  const downloadUrl = extractDownloadUrl(htmlBody);
  if (!downloadUrl) {
    throw new Error(
      "Could not find download URL in export email body. " +
        `Body preview: ${htmlBody.slice(0, 200)}...`
    );
  }

  console.log(`[zip-pipeline] Download URL: ${downloadUrl.slice(0, 80)}...`);

  // ── Phase 3: Download zip ───────────────────────────────
  progress("Downloading zip file...", 15);

  if (!existsSync(DOWNLOADS_DIR)) {
    mkdirSync(DOWNLOADS_DIR, { recursive: true });
  }

  const response = await fetch(downloadUrl);
  if (!response.ok) {
    throw new Error(
      `Zip download failed: ${response.status} ${response.statusText}`
    );
  }

  const zipBuffer = Buffer.from(await response.arrayBuffer());
  const zipPath = join(DOWNLOADS_DIR, `${Date.now()}-export.zip`);
  writeFileSync(zipPath, zipBuffer);

  console.log(
    `[zip-pipeline] Downloaded zip: ${(zipBuffer.length / 1024 / 1024).toFixed(1)} MB → ${zipPath}`
  );

  // ── Phase 4: Extract CSVs ──────────────────────────────
  progress("Extracting CSVs from zip...", 20);

  const extracted = extractCSVsFromZip(zipPath);
  const fileMap = mapExtractedFiles(extracted);

  console.log(
    `[zip-pipeline] Extracted ${extracted.length} files: ${[...fileMap.keys()].join(", ")}`
  );

  // ── Phase 5: Run transform + save from extracted files ──
  return runZipImport(fileMap, progress, startTime, latestEmail.id, gmail);
}

// ── Local Pipeline (for testing) ────────────────────────────

/**
 * Run the import from a local directory of CSVs (no Gmail, no download).
 * Useful for testing with the manually-downloaded export.
 */
export async function runZipLocalPipeline(
  options: ZipLocalOptions
): Promise<PipelineResult> {
  const startTime = Date.now();
  const progress = options.onProgress ?? (() => {});
  const { csvDir } = options;

  progress("Reading local CSVs...", 5);

  // Build file map from directory contents
  const { readdirSync } = await import("fs");
  const files = readdirSync(csvDir).filter((f) => f.endsWith(".csv"));
  const fileMap = new Map<string, string>();
  for (const f of files) {
    const name = f.toLowerCase().replace(/\.csv$/, "");
    fileMap.set(name, join(csvDir, f));
  }

  console.log(
    `[zip-pipeline] Local files: ${files.length} CSVs from ${csvDir}`
  );

  return runZipImport(fileMap, progress, startTime);
}

// ── Webhook Pipeline (direct URL, no Gmail) ─────────────────

/**
 * Run the import from a direct download URL (webhook-triggered).
 * Skips Gmail entirely — just downloads, extracts, and imports.
 */
export async function runZipWebhookPipeline(
  options: ZipWebhookOptions
): Promise<PipelineResult> {
  const startTime = Date.now();
  const progress = options.onProgress ?? (() => {});
  const { downloadUrl } = options;

  // ── Download zip ─────────────────────────────────────────
  progress("Downloading zip from webhook URL...", 10);

  if (!existsSync(DOWNLOADS_DIR)) {
    mkdirSync(DOWNLOADS_DIR, { recursive: true });
  }

  console.log(`[zip-pipeline] Webhook download: ${downloadUrl.slice(0, 80)}...`);

  const response = await fetch(downloadUrl);
  if (!response.ok) {
    throw new Error(
      `Zip download failed: ${response.status} ${response.statusText}`
    );
  }

  const zipBuffer = Buffer.from(await response.arrayBuffer());
  const zipPath = join(DOWNLOADS_DIR, `${Date.now()}-webhook-export.zip`);
  writeFileSync(zipPath, zipBuffer);

  console.log(
    `[zip-pipeline] Downloaded zip: ${(zipBuffer.length / 1024 / 1024).toFixed(1)} MB → ${zipPath}`
  );

  // ── Extract CSVs ────────────────────────────────────────
  progress("Extracting CSVs from zip...", 20);

  const extracted = extractCSVsFromZip(zipPath);
  const fileMap = mapExtractedFiles(extracted);

  console.log(
    `[zip-pipeline] Extracted ${extracted.length} files: ${[...fileMap.keys()].join(", ")}`
  );

  // ── Run transform + save ────────────────────────────────
  return runZipImport(fileMap, progress, startTime);
}

// ── Shared Import Logic ─────────────────────────────────────

async function runZipImport(
  fileMap: Map<string, string>,
  progress: ProgressCallback,
  startTime: number,
  emailId?: string,
  gmail?: GmailClient
): Promise<PipelineResult> {
  const allWarnings: string[] = [];
  const recordCounts: Record<string, number> = {};

  // ── Parse lookup tables (small, load fully) ─────────────
  progress("Parsing lookup tables (memberships, passes, events)...", 25);

  const memberships = parseTable<RawMembership>(
    fileMap.get("memberships"),
    RawMembershipSchema,
    "memberships"
  );
  const passes = parseTable<RawPass>(
    fileMap.get("passes"),
    RawPassSchema,
    "passes"
  );
  const events = parseTable<RawEvent>(
    fileMap.get("events"),
    RawEventSchema,
    "events"
  );
  const performances = parseTable<RawPerformance>(
    fileMap.get("performances"),
    RawPerformanceSchema,
    "performances"
  );
  const locations = parseTable<RawLocation>(
    fileMap.get("locations"),
    RawLocationSchema,
    "locations"
  );
  const passTypes = parseTable<RawPassType>(
    fileMap.get("pass_types"),
    RawPassTypeSchema,
    "pass_types"
  );
  const revenueCategoryLookups = parseTable<RawRevenueCategoryLookup>(
    fileMap.get("revenue_categories"),
    RawRevenueCategoryLookupSchema,
    "revenue_categories (lookup)"
  );
  const refunds = parseTable<RawRefund>(
    fileMap.get("refunds"),
    RawRefundSchema,
    "refunds"
  );

  // ── Build transformer ───────────────────────────────────
  progress("Building lookup maps...", 35);

  const tables: RawZipTables = {
    memberships,
    passes,
    events,
    performances,
    locations,
    passTypes,
    revenueCategoryLookups,
  };
  const transformer = new ZipTransformer(tables);

  // ── Transform + save auto-renews ────────────────────────
  progress("Importing auto-renew subscriptions...", 40);

  const autoRenewRows = transformer.transformAutoRenews();
  if (autoRenewRows.length > 0) {
    const snapshotId = `zip-${Date.now()}`;
    const result = await saveAutoRenews(snapshotId, autoRenewRows);
    recordCounts.autoRenews = autoRenewRows.length;
    console.log(
      `[zip-pipeline] Auto-renews saved: ${result.inserted} new, ${result.updated} updated`
    );
  }

  // ── Transform + save orders (batched) ───────────────────
  progress("Importing orders...", 55);

  let rawOrders: RawOrder[] = [];
  const ordersFile = fileMap.get("orders");
  if (ordersFile) {
    rawOrders = parseTable<RawOrder>(
      ordersFile,
      RawOrderSchema,
      "orders"
    );
    const BATCH_SIZE = 10_000;
    let totalOrders = 0;

    for (let i = 0; i < rawOrders.length; i += BATCH_SIZE) {
      const batch = rawOrders.slice(i, i + BATCH_SIZE);
      const orderRows = transformer.transformOrdersBatch(batch);
      await saveOrders(orderRows);
      totalOrders += orderRows.length;

      const pct = 55 + Math.round((i / rawOrders.length) * 15);
      progress(`Importing orders... (${totalOrders.toLocaleString()})`, pct);
    }

    recordCounts.orders = totalOrders;
    console.log(`[zip-pipeline] Orders saved: ${totalOrders.toLocaleString()}`);
  }

  // ── Transform + save registrations (batched) ────────────
  progress("Importing registrations...", 70);

  const regsFile = fileMap.get("registrations");
  if (regsFile) {
    const rawRegs = parseTable<RawRegistration>(
      regsFile,
      RawRegistrationSchema,
      "registrations"
    );
    const BATCH_SIZE = 10_000;
    let totalRegs = 0;

    for (let i = 0; i < rawRegs.length; i += BATCH_SIZE) {
      const batch = rawRegs.slice(i, i + BATCH_SIZE);
      const regRows = transformer.transformRegistrationsBatch(batch);
      await saveRegistrations(regRows);
      totalRegs += regRows.length;

      const pct = 70 + Math.round((i / rawRegs.length) * 15);
      progress(
        `Importing registrations... (${totalRegs.toLocaleString()})`,
        pct
      );
    }

    recordCounts.registrations = totalRegs;
    console.log(
      `[zip-pipeline] Registrations saved: ${totalRegs.toLocaleString()}`
    );
  }

  // ── Transform + save customers ──────────────────────────
  progress("Importing customers...", 88);

  const customerRows = transformer.transformCustomers();
  if (customerRows.length > 0) {
    await saveCustomers(customerRows);
    recordCounts.customers = customerRows.length;
    console.log(
      `[zip-pipeline] Customers saved: ${customerRows.length.toLocaleString()}`
    );
  }

  // ── Compute + save revenue by category ──────────────────
  progress("Computing revenue by category...", 90);

  if (rawOrders.length > 0 && revenueCategoryLookups.length > 0) {
    const monthlyRevenue = transformer.computeRevenueByCategory(rawOrders, refunds);
    let totalRevCatsSaved = 0;

    for (const [month, categories] of monthlyRevenue.entries()) {
      const year = parseInt(month.slice(0, 4));
      const monthNum = parseInt(month.slice(5, 7));

      // Check if this month is locked (manually uploaded data)
      const locked = await isMonthLocked(year, monthNum);
      if (locked) {
        console.log(`[zip-pipeline] Skipping locked revenue period ${month}`);
        continue;
      }

      // Build period: first day to last day of month
      const periodStart = `${month}-01`;
      const lastDay = new Date(year, monthNum, 0).getDate();
      const periodEnd = `${month}-${String(lastDay).padStart(2, "0")}`;

      await saveRevenueCategories(periodStart, periodEnd, categories);
      totalRevCatsSaved += categories.length;
    }

    recordCounts.revenueCategories = totalRevCatsSaved;
    console.log(
      `[zip-pipeline] Revenue categories saved: ${totalRevCatsSaved} across ${monthlyRevenue.size} months`
    );
  }

  // ── Mark email as read + set watermark ──────────────────
  progress("Finalizing...", 95);

  if (emailId && gmail) {
    try {
      await gmail.markAsRead(emailId);
    } catch {
      /* non-critical */
    }
  }

  // Set watermarks
  const now = new Date().toISOString();
  const totalRecords = Object.values(recordCounts).reduce((a, b) => a + b, 0);
  await setWatermark("zipExport", now, totalRecords, "Daily zip pipeline");
  if (recordCounts.autoRenews) await setWatermark("autoRenews", now, recordCounts.autoRenews);
  if (recordCounts.registrations) await setWatermark("registrations", now, recordCounts.registrations);
  if (recordCounts.orders) await setWatermark("orders", now, recordCounts.orders);
  if (recordCounts.revenueCategories) await setWatermark("revenueCategories", now, recordCounts.revenueCategories, "Computed from zip export");

  // ── Done ────────────────────────────────────────────────
  const duration = Math.round((Date.now() - startTime) / 1000);
  progress("Zip pipeline complete", 100);

  console.log(
    `[zip-pipeline] Complete in ${duration}s. Records: ` +
      Object.entries(recordCounts)
        .map(([k, v]) => `${k}=${v.toLocaleString()}`)
        .join(", ")
  );

  return {
    success: true,
    sheetUrl: "",
    rawDataSheetUrl: "",
    duration,
    recordCounts: {
      newCustomers: recordCounts.customers || 0,
      orders: recordCounts.orders || 0,
      firstVisits: 0,
      registrations: recordCounts.registrations || 0,
      canceledAutoRenews: 0,
      activeAutoRenews: recordCounts.autoRenews || 0,
      newAutoRenews: recordCounts.autoRenews || 0,
    },
    warnings: allWarnings,
    validation: {
      passed: true,
      checks: [],
    },
  };
}
