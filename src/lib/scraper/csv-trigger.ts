/**
 * CSV download trigger for Union.fit.
 *
 * Logs in via Playwright and clicks "Download CSV" on each report page.
 * Union.fit has TWO delivery methods:
 *   1. Email export — queues CSV and emails to the logged-in account (large reports)
 *   2. Direct browser download — sends the file immediately (small reports)
 *
 * This module handles BOTH: it sets up a Playwright download listener before
 * clicking, and captures the file if one arrives. If no download starts within
 * a few seconds, the click is treated as an email export trigger.
 *
 * Two click strategies per report:
 *  1. View dropdown → "Download CSV" link (most reports)
 *  2. button[value="csv"] form submit (subscription reports)
 */

import { UnionClient } from "./union-client";
import { REPORT_URLS, type ReportType } from "./selectors";
import type { Page, Download } from "playwright";
import { join } from "path";
import { mkdirSync } from "fs";

const BASE_URL = "https://www.union.fit/admin/orgs/sky-ting";

/** Where direct browser downloads get saved */
const DOWNLOADS_DIR = join(process.cwd(), "data", "downloads");

/**
 * All reports we want to trigger CSV exports for.
 * Order matters — subscription reports are fastest (simple form submit),
 * so we do those first.
 */
const REPORTS_TO_TRIGGER: ReportType[] = [
  // Subscription reports (have button[value="csv"] AND View dropdown)
  "canceledAutoRenews",
  "newAutoRenews",
  "activeAutoRenews",
  "pausedAutoRenews",
  "trialingAutoRenews",
  // Other reports (View dropdown → Download CSV)
  "newCustomers",
  "orders",
  "firstVisits",
  "fullRegistrations",
];

/**
 * Optional reports — trigger if available, don't fail the pipeline if missing.
 */
const OPTIONAL_REPORTS: ReportType[] = [
  "revenueCategories",
];

export interface TriggerResult {
  /** Reports where CSV download was successfully triggered (email or direct) */
  triggered: string[];
  /** Reports that failed to trigger */
  failed: { report: string; error: string }[];
  /** Reports where a direct browser download was captured (file path included) */
  directDownloads: { report: string; filePath: string }[];
  /** Reports that were triggered but delivery is via email (need to poll inbox) */
  emailPending: string[];
}

/**
 * Trigger CSV exports on Union.fit report pages.
 *
 * For each report: navigate to the page, set date range, click "Download CSV".
 * Captures direct browser downloads when they occur; otherwise marks the report
 * as pending email delivery.
 *
 * Returns which reports were triggered, which downloaded directly, and which
 * are awaiting email delivery.
 */
export async function triggerCSVDownloads(
  email: string,
  password: string,
  dateRange: string,
  options?: {
    onProgress?: (step: string) => void;
    includeOptional?: boolean;
  }
): Promise<TriggerResult> {
  const result: TriggerResult = {
    triggered: [],
    failed: [],
    directDownloads: [],
    emailPending: [],
  };

  // Ensure downloads directory exists
  mkdirSync(DOWNLOADS_DIR, { recursive: true });

  const onProgress = options?.onProgress;
  const client = new UnionClient({
    onProgress: (step) => onProgress?.(step),
  });

  try {
    onProgress?.("Launching browser");
    await client.initialize();

    onProgress?.("Logging into Union.fit");
    await client.login(email, password);

    const page = client.getPage();
    if (!page) throw new Error("No page available after login");

    // Trigger required reports
    for (const reportType of REPORTS_TO_TRIGGER) {
      try {
        onProgress?.(`Triggering CSV download: ${reportType}`);
        const downloadResult = await triggerDownloadCSV(page, reportType, dateRange);
        result.triggered.push(reportType);

        if (downloadResult.filePath) {
          result.directDownloads.push({ report: reportType, filePath: downloadResult.filePath });
          onProgress?.(`Direct download captured: ${reportType}`);
        } else {
          result.emailPending.push(reportType);
        }
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        console.warn(`[csv-trigger] Failed to trigger ${reportType}: ${msg}`);
        result.failed.push({ report: reportType, error: msg });
      }
    }

    // Trigger optional reports
    if (options?.includeOptional !== false) {
      for (const reportType of OPTIONAL_REPORTS) {
        try {
          onProgress?.(`Triggering CSV download: ${reportType} (optional)`);
          const downloadResult = await triggerDownloadCSV(page, reportType, dateRange);
          result.triggered.push(reportType);

          if (downloadResult.filePath) {
            result.directDownloads.push({ report: reportType, filePath: downloadResult.filePath });
            onProgress?.(`Direct download captured: ${reportType} (optional)`);
          } else {
            result.emailPending.push(reportType);
          }
        } catch (err) {
          const msg = err instanceof Error ? err.message : String(err);
          console.warn(`[csv-trigger] Optional report ${reportType} skipped: ${msg}`);
          // Don't add to failed — these are optional
        }
      }
    }

    const directCount = result.directDownloads.length;
    const emailCount = result.emailPending.length;
    onProgress?.(`Triggered ${result.triggered.length} CSV downloads (${directCount} direct, ${emailCount} pending email)`);
  } finally {
    await client.cleanup();
  }

  return result;
}

interface DownloadResult {
  /** True if the "Download CSV" click succeeded */
  clicked: boolean;
  /** Path to the downloaded file if it was a direct browser download */
  filePath?: string;
}

/**
 * Navigate to a report page and click "Download CSV".
 *
 * Sets up a Playwright download listener BEFORE clicking. If the click
 * triggers a direct browser download, we capture it. If not (email export),
 * we just record that it was triggered.
 *
 * Tries two click strategies:
 *  1. Click the "View" dropdown, then click "Download CSV" link
 *  2. Click button[value="csv"] (subscription report form submit)
 */
async function triggerDownloadCSV(
  page: Page,
  reportType: ReportType,
  dateRange: string
): Promise<DownloadResult> {
  const reportUrl = `${BASE_URL}${REPORT_URLS[reportType]}`;

  // Navigate to report page and wait for it to fully render
  await page.goto(reportUrl, { waitUntil: "domcontentloaded", timeout: 30000 });
  await page.waitForLoadState("networkidle", { timeout: 15000 }).catch(() => {});

  // Wait for the View button to appear — this confirms the report toolbar has loaded
  await page.locator('button.form-control:has-text("View")').first()
    .waitFor({ state: "visible", timeout: 10000 })
    .catch(() => {}); // Non-fatal — proceed to try clicking anyway

  // Check for Cloudflare
  const content = await page.content();
  if (content.includes("Verify you are human") || content.includes("cf-challenge")) {
    throw new Error("Cloudflare blocked access — session may have expired");
  }

  // Set date range if the input exists
  if (dateRange) {
    const dateInput = page.locator('input[name="daterange"]').first();
    if (await dateInput.isVisible({ timeout: 3000 }).catch(() => false)) {
      await dateInput.clear();
      await dateInput.fill(dateRange);
      // Press Enter or click Apply to update the report with the new date range
      await page.keyboard.press("Enter");
      // Wait briefly for the page to update with new date range
      await page.waitForTimeout(1500);
    }
  }

  // Set up download listener BEFORE clicking
  // This races: if a download starts within 5 seconds, we capture it.
  // If no download starts, the click was an email export trigger.
  let downloadPromise: Promise<Download | null>;

  // Create a promise that resolves to the download if one starts, or null after timeout
  downloadPromise = Promise.race([
    page.waitForEvent("download", { timeout: 8000 }).catch(() => null),
    new Promise<null>((resolve) => setTimeout(() => resolve(null), 8000)),
  ]);

  // Strategy 1: Try "View" dropdown → "Download CSV" link
  const viewClicked = await tryViewDropdownCSV(page);
  if (viewClicked) {
    console.log(`[csv-trigger] Triggered via View dropdown: ${reportType}`);
    const filePath = await captureDownload(downloadPromise, reportType);
    return { clicked: true, filePath };
  }

  // Reset download listener for strategy 2
  downloadPromise = Promise.race([
    page.waitForEvent("download", { timeout: 8000 }).catch(() => null),
    new Promise<null>((resolve) => setTimeout(() => resolve(null), 8000)),
  ]);

  // Strategy 2: Try button[value="csv"] form submit (subscription reports)
  const csvButton = page.locator('button[value="csv"]').first();
  if (await csvButton.isVisible({ timeout: 3000 }).catch(() => false)) {
    await csvButton.click();
    console.log(`[csv-trigger] Triggered via CSV button: ${reportType}`);
    const filePath = await captureDownload(downloadPromise, reportType);
    return { clicked: true, filePath };
  }

  throw new Error(`No CSV download option found on ${reportType} page`);
}

/**
 * Wait for a download event and save the file if one arrives.
 * Returns the file path if a direct download was captured, or undefined
 * if the click triggered an email export instead.
 */
async function captureDownload(
  downloadPromise: Promise<Download | null>,
  reportType: string
): Promise<string | undefined> {
  const download = await downloadPromise;

  if (!download) {
    // No direct download — this was an email export trigger
    console.log(`[csv-trigger] No direct download for ${reportType} — will arrive via email`);
    return undefined;
  }

  // Direct download captured! Save it.
  const suggestedName = download.suggestedFilename() || `${reportType}.csv`;
  const timestamp = Date.now();
  const savePath = join(DOWNLOADS_DIR, `${reportType}-${timestamp}.csv`);

  try {
    await download.saveAs(savePath);
    console.log(`[csv-trigger] Direct download saved: ${savePath} (original: ${suggestedName})`);
    return savePath;
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    console.warn(`[csv-trigger] Failed to save download for ${reportType}: ${msg}`);
    return undefined;
  }
}

/**
 * Try to click "Download CSV" from the "View" dropdown menu.
 * Returns true if successful, false if the dropdown/option wasn't found.
 *
 * Union.fit UI structure (Bootstrap btn-group):
 *   <div class="btn-group">
 *     <button class="form-control button-min-width-70">View</button>   <- main button
 *     <button class="dropdown-toggle dropdown-toggle-split">...</button> <- caret toggle
 *     <div class="dropdown-menu">
 *       <button class="dropdown-item">Download CSV</button>              <- target
 *     </div>
 *   </div>
 *
 * Important: Many rows in report tables also have "View" links/buttons,
 * so we must target the btn-group in the toolbar, not table row buttons.
 */
async function tryViewDropdownCSV(page: Page): Promise<boolean> {
  // Strategy 1: Find the "Download CSV" dropdown-item directly.
  // It's inside a dropdown-menu that's a sibling of the View button.
  // First, try to open the dropdown by clicking the toggle button.
  try {
    // The btn-group containing "View" + dropdown toggle.
    // Target the dropdown-toggle-split inside a btn-group that also contains
    // a button with text "View" and class form-control (the toolbar View button).
    const btnGroup = page.locator('.btn-group:has(button.form-control:has-text("View"))').first();

    if (await btnGroup.isVisible({ timeout: 3000 }).catch(() => false)) {
      // Click the dropdown toggle (the caret button next to "View")
      const toggle = btnGroup.locator('.dropdown-toggle-split, .dropdown-toggle').first();
      if (await toggle.isVisible({ timeout: 2000 }).catch(() => false)) {
        await toggle.click();
        await page.waitForTimeout(500);

        // Now "Download CSV" should be visible in the dropdown menu
        const downloadBtn = btnGroup.locator('button:has-text("Download CSV"), a:has-text("Download CSV")').first();
        if (await downloadBtn.isVisible({ timeout: 2000 }).catch(() => false)) {
          await downloadBtn.click();
          // Give the download a moment to start (or confirmation banner to appear)
          await page.waitForTimeout(2000);

          // Check for confirmation text on the page (email export indicator)
          const pageContent = await page.content();
          if (pageContent.includes("emailed") || pageContent.includes("export")) {
            console.log(`[csv-trigger] Confirmation banner detected after click`);
          }
          return true;
        }

        // Close dropdown
        await page.keyboard.press("Escape");
      }
    }
  } catch {
    // Fall through to strategy 2
  }

  // Strategy 2: Some pages have a slightly different layout.
  // Try clicking any visible "Download CSV" button/link directly.
  try {
    const downloadBtn = page.locator('button.dropdown-item:has-text("Download CSV")').first();
    // First open any dropdown that might contain it
    const inputGroup = page.locator('.input-group:has(button:has-text("View"))').first();
    if (await inputGroup.isVisible({ timeout: 2000 }).catch(() => false)) {
      const toggle = inputGroup.locator('.dropdown-toggle-split, .dropdown-toggle').first();
      if (await toggle.isVisible({ timeout: 1000 }).catch(() => false)) {
        await toggle.click();
        await page.waitForTimeout(500);
      }
    }

    if (await downloadBtn.isVisible({ timeout: 2000 }).catch(() => false)) {
      await downloadBtn.click();
      await page.waitForTimeout(2000);

      const pageContent = await page.content();
      if (pageContent.includes("emailed") || pageContent.includes("export")) {
        console.log(`[csv-trigger] Confirmation banner detected after click (strategy 2)`);
      }
      return true;
    }
  } catch {
    // Fall through
  }

  return false;
}
