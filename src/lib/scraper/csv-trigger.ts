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
import { mkdirSync, writeFileSync } from "fs";

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
 * `dateRanges` is a per-report map so each report uses its own watermark-based
 * date range. Smaller ranges → smaller CSVs → more direct downloads (no email wait).
 *
 * Returns which reports were triggered, which downloaded directly, and which
 * are awaiting email delivery.
 */
export async function triggerCSVDownloads(
  email: string,
  password: string,
  dateRanges: Record<string, string>,
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

    // Trigger required reports — retry once on failure (page may need more load time)
    for (const reportType of REPORTS_TO_TRIGGER) {
      let lastError = "";
      let succeeded = false;

      for (let attempt = 1; attempt <= 2; attempt++) {
        try {
          const reportDateRange = dateRanges[reportType] || dateRanges['_default'] || '';
          onProgress?.(`Triggering CSV download: ${reportType}${attempt > 1 ? " (retry)" : ""}`);
          const downloadResult = await triggerDownloadCSV(page, reportType, reportDateRange);
          result.triggered.push(reportType);

          if (downloadResult.filePath) {
            result.directDownloads.push({ report: reportType, filePath: downloadResult.filePath });
            onProgress?.(`Direct download captured: ${reportType}`);
          } else {
            result.emailPending.push(reportType);
          }
          succeeded = true;
          break;
        } catch (err) {
          lastError = err instanceof Error ? err.message : String(err);
          console.warn(`[csv-trigger] Attempt ${attempt} failed for ${reportType}: ${lastError}`);

          // On first failure, take diagnostic screenshot and retry
          if (attempt === 1) {
            try {
              const screenshotPath = join(DOWNLOADS_DIR, `debug-${reportType}-${Date.now()}.png`);
              await page.screenshot({ path: screenshotPath, fullPage: true });
              console.log(`[csv-trigger] Debug screenshot saved: ${screenshotPath}`);
              // Also dump visible button/link text for debugging
              const buttons = await page.locator('button, a, input[type="submit"]').allTextContents();
              const meaningful = buttons.filter((t) => t.trim().length > 0 && t.trim().length < 50);
              console.log(`[csv-trigger] Visible buttons on ${reportType}: ${meaningful.join(" | ")}`);
            } catch {
              // Screenshot failed — continue anyway
            }
            // Brief pause before retry
            await page.waitForTimeout(2000);
          }
        }
      }

      if (!succeeded) {
        result.failed.push({ report: reportType, error: lastError });
      }
    }

    // Trigger optional reports
    if (options?.includeOptional !== false) {
      for (const reportType of OPTIONAL_REPORTS) {
        try {
          const optDateRange = dateRanges[reportType] || dateRanges['_default'] || '';
          onProgress?.(`Triggering CSV download: ${reportType} (optional)`);
          const downloadResult = await triggerDownloadCSV(page, reportType, optDateRange);
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

  // Wait for the report toolbar to load — try multiple indicators
  // Also look for <a> and <button> elements containing "View" (covers both element types)
  await Promise.race([
    page.locator('button:has-text("View"), a:has-text("View")').first().waitFor({ state: "visible", timeout: 15000 }),
    page.locator('.dropdown-toggle').first().waitFor({ state: "visible", timeout: 15000 }),
    page.locator('.btn-group').first().waitFor({ state: "visible", timeout: 15000 }),
    page.locator('button[value="csv"]').first().waitFor({ state: "visible", timeout: 15000 }),
    new Promise((resolve) => setTimeout(resolve, 15000)), // fallback max wait
  ]).catch(() => {}); // Non-fatal — proceed to try clicking anyway

  // Extra stability wait — some pages have client-side routing that can destroy the context
  await page.waitForTimeout(1000);

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
  const viewClicked = await tryViewDropdownCSV(page, reportUrl);
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
 * Union.fit UI: The toolbar has a [View] button and a [∨] chevron button
 * as siblings inside a parent container. Clicking ∨ opens a dropdown
 * with "Download CSV". The actual CSS classes vary across pages, so
 * we use class-agnostic detection as the primary strategy.
 */
async function tryViewDropdownCSV(page: Page, reportUrl?: string): Promise<boolean> {
  // ── Strategy 0 (PRIMARY): Use JavaScript to find toolbar View dropdown ──
  // The toolbar "View" element can be a <button> or <a> tag. On pages with
  // data tables (New Customers, Canceled Auto-Renews), each row also has a
  // "View" button. We use JS to find View elements NOT inside <tr>/<td> tags.
  try {
    // Wait for page to be fully stable before evaluating
    await page.waitForLoadState("domcontentloaded").catch(() => {});

    // Find toolbar View elements via JS — returns their indices for Playwright to click
    let toolbarViewCount = 0;
    for (let evalAttempt = 0; evalAttempt < 2; evalAttempt++) {
      try {
        toolbarViewCount = await page.evaluate(() => {
          const elements = document.querySelectorAll('button, a');
          let count = 0;
          for (const el of elements) {
            const text = el.textContent?.trim() || '';
            if (!/^View\b/i.test(text)) continue;
            // Skip elements inside table rows (these are per-row View buttons)
            if (el.closest('tr') || el.closest('td') || el.closest('tbody')) continue;
            // Mark toolbar View elements with a data attribute for Playwright
            (el as HTMLElement).setAttribute('data-csv-toolbar-view', String(count));
            count++;
          }
          return count;
        });
        break; // evaluate succeeded
      } catch (evalErr) {
        const msg = evalErr instanceof Error ? evalErr.message : String(evalErr);
        console.log(`[csv-trigger] Strategy 0 evaluate attempt ${evalAttempt + 1} failed: ${msg}`);
        if (evalAttempt === 0) {
          // Re-navigate to the page and wait for stability
          if (reportUrl) {
            await page.goto(reportUrl, { waitUntil: "domcontentloaded", timeout: 30000 }).catch(() => {});
          }
          await page.waitForLoadState("networkidle", { timeout: 10000 }).catch(() => {});
          await page.waitForTimeout(2000);
        }
      }
    }

    console.log(`[csv-trigger] Strategy 0: found ${toolbarViewCount} toolbar "View" elements (excluding table rows)`);

    // Try each toolbar View element (last to first — toolbar is usually after nav)
    for (let i = toolbarViewCount - 1; i >= 0; i--) {
      const viewEl = page.locator(`[data-csv-toolbar-view="${i}"]`);
      if (!(await viewEl.isVisible({ timeout: 2000 }).catch(() => false))) continue;

      // Click it — might be a dropdown toggle itself
      await viewEl.click();
      await page.waitForTimeout(600);

      const csvBtn = page.locator('text="Download CSV"').first();
      if (await csvBtn.isVisible({ timeout: 1500 }).catch(() => false)) {
        console.log(`[csv-trigger] Found CSV by clicking toolbar View #${i} (strategy 0a)`);
        await csvBtn.click();
        await page.waitForTimeout(2000);
        return true;
      }
      await page.keyboard.press('Escape');
      await page.waitForTimeout(300);

      // Try sibling elements (split-button pattern: [View] [▾])
      const parent = viewEl.locator('..');
      const siblings = parent.locator('button, a');
      const sibCount = await siblings.count();

      for (let j = 0; j < sibCount; j++) {
        const sibling = siblings.nth(j);
        const text = (await sibling.textContent().catch(() => ''))?.trim();
        if (text && /^View\b/i.test(text)) continue; // skip the View element itself
        if (!(await sibling.isVisible().catch(() => false))) continue;

        await sibling.click();
        await page.waitForTimeout(600);

        const csvBtnSib = page.locator('text="Download CSV"').first();
        if (await csvBtnSib.isVisible({ timeout: 1500 }).catch(() => false)) {
          console.log(`[csv-trigger] Found CSV via toolbar View sibling (strategy 0b)`);
          await csvBtnSib.click();
          await page.waitForTimeout(2000);
          return true;
        }
        await page.keyboard.press('Escape');
        await page.waitForTimeout(300);
      }
    }
  } catch (err) {
    console.log(`[csv-trigger] Strategy 0 error: ${err instanceof Error ? err.message : String(err)}`);
  }

  // ── Strategy 1: Bootstrap btn-group with specific classes ──
  try {
    // Also check for <a> tags in btn-group (some pages use links instead of buttons)
    const btnGroup = page.locator('.btn-group').filter({
      has: page.locator('button:has-text("View"), a:has-text("View")')
    }).first();
    if (await btnGroup.isVisible({ timeout: 2000 }).catch(() => false)) {
      const toggle = btnGroup.locator('.dropdown-toggle-split, .dropdown-toggle').first();
      if (await toggle.isVisible({ timeout: 1500 }).catch(() => false)) {
        await toggle.click();
        await page.waitForTimeout(500);

        const downloadBtn = page.locator('text="Download CSV"').first();
        if (await downloadBtn.isVisible({ timeout: 2000 }).catch(() => false)) {
          console.log(`[csv-trigger] Found CSV via btn-group dropdown (strategy 1)`);
          await downloadBtn.click();
          await page.waitForTimeout(2000);
          return true;
        }
        await page.keyboard.press("Escape");
      }
    }
  } catch {
    // Fall through
  }

  // ── Strategy 2: Find ANY dropdown toggle on the page ──
  try {
    const allToggles = page.locator('.dropdown-toggle, .dropdown-toggle-split');
    const count = await allToggles.count();

    for (let i = 0; i < Math.min(count, 8); i++) {
      const toggle = allToggles.nth(i);
      if (!(await toggle.isVisible().catch(() => false))) continue;
      // Skip toggles inside table rows
      const inTable = await toggle.evaluate((el) => !!(el.closest('tr') || el.closest('td') || el.closest('tbody')));
      if (inTable) continue;

      await toggle.click();
      await page.waitForTimeout(500);

      const csvOption = page.locator('text="Download CSV"').first();
      if (await csvOption.isVisible({ timeout: 1000 }).catch(() => false)) {
        console.log(`[csv-trigger] Found CSV option via dropdown toggle #${i + 1} (strategy 2)`);
        await csvOption.click();
        await page.waitForTimeout(2000);
        return true;
      }

      await page.keyboard.press("Escape");
      await page.waitForTimeout(300);
    }
  } catch {
    // Fall through
  }

  // ── Strategy 3: Direct CSV/Export link already visible on page ──
  try {
    const exportLink = page.locator(
      'a:has-text("Download CSV"), a:has-text("Export CSV"), ' +
      'button:has-text("Download CSV"), button:has-text("Export CSV"), ' +
      'a[href*="csv"], a[href*="export"]'
    ).first();

    if (await exportLink.isVisible({ timeout: 2000 }).catch(() => false)) {
      console.log(`[csv-trigger] Found direct CSV/export link (strategy 3)`);
      await exportLink.click();
      await page.waitForTimeout(2000);
      return true;
    }
  } catch {
    // Fall through
  }

  return false;
}
