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
  RawTransferSchema,
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
  type RawTransfer,
} from "./zip-schemas";
import { saveAutoRenews } from "../db/auto-renew-store";
import { saveOrders } from "../db/order-store";
import {
  saveRegistrations,
  ensurePassEmailCache,
  savePassEmailCache,
  loadPassEmailCache,
  backfillRegistrationEmails,
  recomputeFirstVisitFlags,
} from "../db/registration-store";
import { saveCustomers } from "../db/customer-store";
import { saveRefunds } from "../db/refund-store";
import { saveTransfers } from "../db/transfer-store";
import { savePasses } from "../db/pass-store";
import { recomputeRevenueFromDB } from "../analytics/db-revenue";
import { setWatermark } from "../db/watermark-store";
import {
  ensureLookupTables,
  savePassTypeLookups,
  saveRevenueCategoryLookups,
  loadPassTypeLookups,
  loadRevenueCategoryLookups,
} from "../db/lookup-store";
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
  /** Date range the export covers — used to restrict revenue saves to only months within range.
   *  Without this, daily exports can overwrite complete historical revenue with partial data. */
  dataRange?: { start: string; end: string };
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
  /** Date range the export covers — auto-inferred from directory name if not provided */
  dataRange?: { start: string; end: string };
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

  // Auto-infer dataRange from directory name if not explicitly provided.
  // Union.fit zip directories are named like:
  //   union_data_export-sky-ting-20260307-20260307-exZBrU3FBhnbMKJS3tVgNpR4
  // The two 8-digit dates are the start and end of the export period.
  let dataRange = options.dataRange;
  if (!dataRange) {
    const dirName = basename(csvDir);
    const dateMatch = dirName.match(/(\d{4})(\d{2})(\d{2})-(\d{4})(\d{2})(\d{2})/);
    if (dateMatch) {
      dataRange = {
        start: `${dateMatch[1]}-${dateMatch[2]}-${dateMatch[3]}`,
        end: `${dateMatch[4]}-${dateMatch[5]}-${dateMatch[6]}`,
      };
      console.log(`[zip-pipeline] Auto-inferred dataRange from directory name: ${dataRange.start} to ${dataRange.end}`);
    }
  }

  return runZipImport(fileMap, progress, startTime, undefined, undefined, dataRange);
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
  return runZipImport(fileMap, progress, startTime, undefined, undefined, options.dataRange);
}

// ── Shared Import Logic ─────────────────────────────────────

async function runZipImport(
  fileMap: Map<string, string>,
  progress: ProgressCallback,
  startTime: number,
  emailId?: string,
  gmail?: GmailClient,
  dataRange?: { start: string; end: string },
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
  const transfers = parseTable<RawTransfer>(
    fileMap.get("transfers"),
    RawTransferSchema,
    "transfers"
  );

  // ── Cache / load lookup tables ───────────────────────────
  progress("Syncing lookup table cache...", 33);

  await ensureLookupTables();

  // Strategy: always upsert any CSV data we got (full dump or delta),
  // then always load the full cache for the transformer. This handles:
  //  - Full export (1700 rows): upsert all → load all from cache
  //  - Daily delta with changes (5 rows): upsert 5 → load all 1700+ from cache
  //  - Daily empty (0 rows): skip upsert → load all from cache

  if (passTypes.length > 0) {
    await savePassTypeLookups(
      passTypes.map((pt) => ({
        id: pt.id,
        name: pt.name ?? null,
        revenueCategoryId: pt.revenueCategoryId ?? null,
        feesOutside: pt.feesOutside === "true",
        createdAt: pt.createdAt ?? null,
      }))
    );
    console.log(`[zip-pipeline] Upserted ${passTypes.length} pass types to cache`);
  }

  if (revenueCategoryLookups.length > 0) {
    await saveRevenueCategoryLookups(
      revenueCategoryLookups.map((rc) => ({
        id: rc.id,
        name: rc.name ?? "",
      }))
    );
    console.log(`[zip-pipeline] Upserted ${revenueCategoryLookups.length} revenue categories to cache`);
  }

  // Always load full cache for transformer (merges full dump + any deltas)
  const cachedPassTypes = await loadPassTypeLookups();
  const effectivePassTypes: typeof passTypes = cachedPassTypes.size > 0
    ? Array.from(cachedPassTypes.values()).map((c) => ({
        id: c.id,
        name: c.name ?? "",
        revenueCategoryId: c.revenueCategoryId ?? "",
        feesOutside: c.feesOutside ? "true" : "false",
        passCategoryName: "",
        createdAt: "",
      }))
    : passTypes; // fallback to CSV if cache is empty (first run)

  const cachedRevCats = await loadRevenueCategoryLookups();
  const effectiveRevCatLookups: typeof revenueCategoryLookups = cachedRevCats.size > 0
    ? Array.from(cachedRevCats.values()).map((c) => ({
        id: c.id,
        name: c.name,
      }))
    : revenueCategoryLookups;

  // ── Build transformer ───────────────────────────────────
  progress("Building lookup maps...", 35);

  const tables: RawZipTables = {
    memberships,
    passes,
    events,
    performances,
    locations,
    passTypes: effectivePassTypes,
    revenueCategoryLookups: effectiveRevCatLookups,
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

  // ── Populate pass-email cache ──────────────────────────
  // Each pipeline run adds current passes → email mappings to the DB cache.
  // Over time this accumulates ALL passes ever seen, so even expired passes
  // can be resolved in future runs.
  progress("Updating pass-email cache...", 48);

  await ensurePassEmailCache();

  // Build a quick membership lookup for cache population
  const membershipLookup = new Map(memberships.map((m) => [m.id, m]));
  const cacheEntries: { passId: string; membershipId: string; email: string; firstName: string; lastName: string; passName: string }[] = [];
  for (const pass of passes) {
    if (!pass.membershipId) continue;
    const membership = membershipLookup.get(pass.membershipId);
    if (!membership?.email) continue;
    cacheEntries.push({
      passId: pass.id,
      membershipId: pass.membershipId,
      email: membership.email,
      firstName: membership.firstName || "",
      lastName: membership.lastName || "",
      passName: pass.name || "",
    });
  }

  const cachedCount = await savePassEmailCache(cacheEntries);
  console.log(`[zip-pipeline] Pass-email cache: ${cachedCount} entries from current export`);

  // Load the full cache (current + all prior runs) for transformer fallback
  const passEmailCache = await loadPassEmailCache();
  transformer.setPassEmailCache(passEmailCache);

  // ── Save passes to DB for revenue computation ─────────
  // The 7-step revenue algorithm requires passes for:
  //   Step 2: pass.total (not order.total) for non-subscription pass revenue
  //   Step 3: excluding pass orders from non-pass order revenue
  //   Step 4A: pass.total for pass-linked refund amounts
  progress("Importing passes...", 52);

  if (passes.length > 0) {
    const passRows = passes.map((p) => ({
      id: p.id,
      passCategoryName: p.passCategoryName || "",
      orderId: p.orderId || "",
      refundId: p.refundId || "",
      passTypeId: p.passTypeId || "",
      name: p.name || "",
      total: p.total || 0,
      feeUnionTotal: p.feeUnionTotal || 0,
      feePaymentTotal: p.feePaymentTotal || 0,
      feesOutside: typeof p.feesOutside === "boolean" ? p.feesOutside : p.feesOutside === "true",
      membershipId: p.membershipId || "",
      state: p.state || "",
    }));
    await savePasses(passRows);
    recordCounts.passes = passRows.length;
    console.log(`[zip-pipeline] Passes saved: ${passRows.length.toLocaleString()}`);
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
    let totalResolvedZip = 0;
    let totalResolvedCache = 0;
    let totalUnresolved = 0;

    for (let i = 0; i < rawRegs.length; i += BATCH_SIZE) {
      const batch = rawRegs.slice(i, i + BATCH_SIZE);
      const { rows: regRows, stats } = transformer.transformRegistrationsBatch(batch);
      await saveRegistrations(regRows);
      totalRegs += regRows.length;
      totalResolvedZip += stats.resolvedZip;
      totalResolvedCache += stats.resolvedCache;
      totalUnresolved += stats.unresolved;

      const pct = 70 + Math.round((i / rawRegs.length) * 15);
      progress(
        `Importing registrations... (${totalRegs.toLocaleString()})`,
        pct
      );
    }

    recordCounts.registrations = totalRegs;
    const totalResolved = totalResolvedZip + totalResolvedCache;
    const resolutionPct = totalRegs > 0
      ? ((totalResolved / totalRegs) * 100).toFixed(1)
      : "0";
    console.log(
      `[zip-pipeline] Registrations saved: ${totalRegs.toLocaleString()}. ` +
        `Email resolution: ${totalResolved}/${totalRegs} (${resolutionPct}%) — ` +
        `${totalResolvedZip} from zip, ${totalResolvedCache} from cache, ${totalUnresolved} unresolved`
    );

    // Backfill existing empty-email records using timestamp matching
    progress("Backfilling empty-email registrations...", 86);
    const backfilled = await backfillRegistrationEmails();
    if (backfilled > 0) {
      console.log(`[zip-pipeline] Backfilled ${backfilled} empty-email registrations`);
    }

    // Recompute is_first_visit flags from registration data.
    // Each email's earliest attended_at = their first visit.
    // Dashboard metrics (new customer volume, cohort conversion, returning
    // visitors) all depend on `registrations WHERE is_first_visit = TRUE`.
    progress("Computing first visit flags...", 87);
    const firstVisitCount = await recomputeFirstVisitFlags();
    recordCounts.firstVisits = firstVisitCount;
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

  // ── Save refunds to DB ──────────────────────────────────
  progress("Importing refunds...", 89);

  if (refunds.length > 0) {
    // Build a revenue category ID → name lookup for refund resolution
    const revCatIdToName = new Map(
      effectiveRevCatLookups.map((rc) => [rc.id, rc.name])
    );

    const refundRows = refunds.map((r) => ({
      id: r.id,
      createdAt: r.createdAt || r.created,
      orderId: r.orderId || "",
      revenueCategoryId: r.revenueCategoryId || "",
      revenueCategory: (r.revenueCategoryId ? revCatIdToName.get(r.revenueCategoryId) : undefined) || "",
      state: r.state || "",
      amountRefunded: r.amountRefunded || 0,
      feeUnionTotalRefunded: r.feeUnionTotalRefunded || 0,
      payoutTotal: r.payoutTotal || 0,
      toBalance: r.toBalance,
      reason: r.reason || "",
    }));
    await saveRefunds(refundRows);
    recordCounts.refunds = refundRows.length;
    console.log(`[zip-pipeline] Refunds saved: ${refundRows.length.toLocaleString()}`);
  }

  // ── Save transfers to DB ───────────────────────────────
  if (transfers.length > 0) {
    const transferRows = transfers.map((t) => ({
      id: t.id,
      createdAt: t.createdAt || t.created,
      payoutTotal: t.payoutTotal || 0,
      description: t.description || "",
      type: t.type || "",
    }));
    await saveTransfers(transferRows);
    recordCounts.transfers = transferRows.length;
    console.log(`[zip-pipeline] Transfers saved: ${transferRows.length.toLocaleString()}`);
  }

  // ── Recompute revenue from accumulated DB data ─────────
  // Instead of computing revenue from this single export's CSV data (which
  // may be a 24-hour delta), recompute from ALL accumulated orders in the DB.
  // This ensures monthly totals are always complete.
  progress("Recomputing revenue from database...", 90);

  try {
    await recomputeRevenueFromDB();
    console.log(`[zip-pipeline] Revenue recomputed from DB (current + previous month)`);
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    console.error(`[zip-pipeline] Revenue recomputation failed: ${msg}`);
    allWarnings.push(`Revenue recomputation failed: ${msg}`);
  }

  // ── Mark email as read + set watermark ──────────────────
  progress("Finalizing...", 95);

  if (emailId && gmail) {
    try {
      await gmail.markAsRead(emailId);
      await gmail.archiveEmail(emailId);
      console.log(`[zip-pipeline] Email ${emailId} marked as read and archived`);
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
  if (recordCounts.firstVisits) await setWatermark("firstVisits", now, recordCounts.firstVisits);
  await setWatermark("revenueCategories", now, 0, "Recomputed from DB");

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
