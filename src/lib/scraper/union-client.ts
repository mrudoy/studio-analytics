import { chromium, Browser, BrowserContext, Page } from "playwright";
import { join } from "path";
import { mkdirSync, existsSync, writeFileSync, readFileSync } from "fs";
import { SELECTORS, REPORT_URLS, ReportType } from "./selectors";
import type { DownloadedFiles } from "@/types/union-data";

const BASE_URL = "https://www.union.fit";
const ORG_SLUG = "sky-ting";
const ADMIN_BASE = `${BASE_URL}/admin/orgs/${ORG_SLUG}`;
const DOWNLOADS_DIR = join(process.cwd(), "data", "downloads");
const COOKIE_FILE = join(process.cwd(), "data", "union-cookies.json");

/**
 * Reports that support direct CSV download via fetch() with format=csv.
 * These use the /subscriptions/growth endpoint which returns CSV directly.
 *
 * Note: activeAutoRenews is NOT included — growth?filter=active frequently
 * returns 503 timeouts. It's faster to scrape the HTML table for that report.
 */
const FETCH_CSV_REPORTS: Set<ReportType> = new Set([
  "canceledAutoRenews",
  "newAutoRenews",
  "activeAutoRenews",
  "pausedAutoRenews",
  "orders",
  "newCustomers",
  "firstVisits",
  "allRegistrations",
]);

export class UnionClient {
  private browser: Browser | null = null;
  private context: BrowserContext | null = null;
  private page: Page | null = null;
  private onProgress?: (step: string, percent: number) => void;

  constructor(options?: { onProgress?: (step: string, percent: number) => void }) {
    this.onProgress = options?.onProgress;
  }

  private progress(step: string, percent?: number) {
    this.onProgress?.(step, percent ?? 0);
  }

  async initialize(): Promise<void> {
    if (!existsSync(DOWNLOADS_DIR)) {
      mkdirSync(DOWNLOADS_DIR, { recursive: true });
    }

    // Must use headed Chromium — Cloudflare blocks headless browsers.
    // Position window off-screen so it doesn't steal focus during pipeline runs.
    try {
      this.browser = await chromium.launch({
        headless: false,
        args: [
          "--no-sandbox",
          "--disable-setuid-sandbox",
          "--disable-blink-features=AutomationControlled",
          "--disable-gpu",
          "--disable-dev-shm-usage",
          "--window-position=-2400,-2400",
          "--window-size=1440,900",
        ],
      });
    } catch (err) {
      throw new Error(`Failed to launch browser: ${err instanceof Error ? err.message : String(err)}`);
    }

    this.context = await this.browser.newContext({
      acceptDownloads: true,
      viewport: { width: 1440, height: 900 },
    });

    // Load saved cookies if they exist
    if (existsSync(COOKIE_FILE)) {
      try {
        const cookies = JSON.parse(readFileSync(COOKIE_FILE, "utf8"));
        await this.context.addCookies(cookies);
        this.progress("Loaded saved session cookies");
      } catch {
        // Invalid cookie file, ignore
      }
    }

    this.page = await this.context.newPage();
  }

  /**
   * Initialize in headed (visible) mode for manual Cloudflare verification.
   * Uses real Chrome (not Playwright's Chromium) to pass Cloudflare.
   */
  async initializeHeaded(): Promise<void> {
    if (!existsSync(DOWNLOADS_DIR)) {
      mkdirSync(DOWNLOADS_DIR, { recursive: true });
    }

    // Use Playwright's bundled Chromium in headed mode — visible to user for Cloudflare verification
    try {
      this.browser = await chromium.launch({
        headless: false,
        args: [
          "--no-sandbox",
          "--disable-setuid-sandbox",
          "--disable-blink-features=AutomationControlled",
          "--disable-gpu",
          "--disable-dev-shm-usage",
        ],
        slowMo: 100,
      });
    } catch (err) {
      throw new Error(`Failed to launch browser: ${err instanceof Error ? err.message : String(err)}`);
    }

    this.context = await this.browser.newContext({
      acceptDownloads: true,
      viewport: { width: 1440, height: 900 },
    });

    this.page = await this.context.newPage();
  }

  /**
   * Save current browser cookies to disk for future headless sessions.
   */
  private async saveCookies(): Promise<void> {
    if (!this.context) return;
    try {
      const cookies = await this.context.cookies();
      const dataDir = join(process.cwd(), "data");
      if (!existsSync(dataDir)) mkdirSync(dataDir, { recursive: true });
      writeFileSync(COOKIE_FILE, JSON.stringify(cookies, null, 2));
    } catch {
      // Ignore cookie save errors
    }
  }

  async login(email: string, password: string): Promise<void> {
    if (!this.page) throw new Error("Client not initialized");

    this.progress("Navigating to Union.fit sign-in page");
    await this.page.goto(`${BASE_URL}/signin`, { waitUntil: "domcontentloaded", timeout: 60000 });
    await this.page.waitForLoadState("networkidle", { timeout: 10000 }).catch(() => {}); // Wait for JS hydration

    // Check if we hit a Cloudflare challenge
    const pageContent = await this.page.content();
    if (pageContent.includes("Verify you are human") || pageContent.includes("cf-challenge") || pageContent.includes("cf-turnstile")) {
      this.progress("Cloudflare challenge detected - waiting for it to resolve...");
      try {
        await this.page.waitForURL((url) => !url.pathname.includes("challenge"), { timeout: 30000 });
        await this.page.waitForTimeout(2000);
      } catch {
        await this.screenshotOnError("cloudflare-blocked");
        throw new Error(
          "Cloudflare bot protection is blocking access. Please use the 'Test Connection' button in Settings to authenticate in a visible browser first."
        );
      }
    }

    // Check if already logged in (redirected to dashboard or has session)
    if (this.page.url().includes("/dashboard")) {
      this.progress("Already logged in");
      await this.saveCookies();
      return;
    }

    // Wait for the login form to appear
    this.progress("Waiting for login form");
    try {
      await this.page.waitForSelector('input[type="email"], input[name="email"]', { timeout: 15000 });
    } catch {
      await this.screenshotOnError("no-login-form");
      throw new Error(
        `Login form not found. Page may be blocked by Cloudflare. Current URL: ${this.page.url()}. Use 'Test Connection' in Settings to authenticate manually first.`
      );
    }

    this.progress("Filling login credentials");
    await this.page.fill('input[type="email"], input[name="email"]', email);
    await this.page.fill('input[type="password"]', password);

    this.progress("Submitting login form");
    const signInButton = this.page.locator(
      'button:has-text("Sign In"), button:has-text("Log In"), button:has-text("Sign in"), button:has-text("Log in")'
    ).first();
    if (await signInButton.isVisible({ timeout: 5000 }).catch(() => false)) {
      await signInButton.click();
    } else {
      await this.page.locator('button[type="submit"]').last().click();
    }

    // Wait for navigation to dashboard
    await this.page.waitForURL("**/dashboard**", { timeout: 30000 }).catch(async () => {
      await this.screenshotOnError("login-failed");
      throw new Error(
        `Login failed - did not redirect to dashboard. Current URL: ${this.page!.url()}`
      );
    });

    this.progress("Successfully logged in");
    await this.saveCookies();
  }

  /**
   * Interactive login flow for "Test Connection" — opens a visible browser,
   * lets the user pass Cloudflare manually, then saves cookies.
   */
  async interactiveLogin(email: string, password: string): Promise<{ success: boolean; message: string }> {
    if (!this.page) throw new Error("Client not initialized");

    this.progress("Opening Union.fit sign-in page (visible browser)");
    await this.page.goto(`${BASE_URL}/signin`, { waitUntil: "domcontentloaded", timeout: 60000 });

    this.progress("Waiting for Cloudflare verification (complete it in the browser window)...");
    const startWait = Date.now();
    const maxWait = 120000;

    while (Date.now() - startWait < maxWait) {
      const content = await this.page.content();
      if (!content.includes("Verify you are human") && !content.includes("cf-challenge") && !content.includes("cf-turnstile")) {
        break;
      }
      await this.page.waitForTimeout(1000);
    }

    const pageContent = await this.page.content();
    if (pageContent.includes("Verify you are human")) {
      return { success: false, message: "Cloudflare verification was not completed within 2 minutes." };
    }

    this.progress("Cloudflare passed! Logging in...");
    await this.page.waitForTimeout(2000);

    if (this.page.url().includes("/dashboard")) {
      await this.saveCookies();
      return { success: true, message: "Already logged in! Session cookies saved." };
    }

    try {
      await this.page.waitForSelector('input[type="email"], input[name="email"]', { timeout: 10000 });
      await this.page.fill('input[type="email"], input[name="email"]', email);
      await this.page.fill('input[type="password"]', password);

      const signInButton = this.page.locator(
        'button:has-text("Sign In"), button:has-text("Log In"), button:has-text("Sign in"), button:has-text("Log in")'
      ).first();
      if (await signInButton.isVisible({ timeout: 5000 }).catch(() => false)) {
        await signInButton.click();
      } else {
        await this.page.locator('button[type="submit"]').last().click();
      }

      await this.page.waitForURL("**/dashboard**", { timeout: 30000 });
      await this.saveCookies();
      return { success: true, message: "Login successful! Session cookies saved for future runs." };
    } catch {
      await this.screenshotOnError("interactive-login-failed");
      return { success: false, message: `Login failed. Current URL: ${this.page.url()}` };
    }
  }

  // ──────────────────────────────────────────────────────────────
  //  CSV DOWNLOAD — HYBRID APPROACH
  //
  //  Strategy A (fast): For subscription/growth reports, use in-page
  //  fetch() with format=csv — returns full CSV directly in ~5-25s.
  //
  //  Strategy B (fallback): For all other reports, scrape HTML tables
  //  with parallel tabs across paginated pages.
  // ──────────────────────────────────────────────────────────────

  /**
   * Download CSV for a report using the best available strategy.
   */
  async downloadCSV(reportType: ReportType, dateRange?: string, pctStart = 0, pctEnd = 100): Promise<string> {
    if (!this.page) throw new Error("Client not initialized");

    if (FETCH_CSV_REPORTS.has(reportType)) {
      return this.downloadViaFetch(reportType, dateRange, pctStart, pctEnd);
    }
    return this.downloadViaScrape(reportType, dateRange, pctStart, pctEnd);
  }

  /**
   * Strategy A: Direct CSV fetch via in-page fetch().
   * Works for /report/subscriptions/growth endpoints (canceled, new, active).
   * Falls back to HTML scraping if fetch returns HTML or 503.
   */
  private async downloadViaFetch(
    reportType: ReportType,
    dateRange?: string,
    pctStart = 0,
    pctEnd = 100
  ): Promise<string> {
    if (!this.page) throw new Error("Client not initialized");

    this.progress(`Fetching ${reportType} CSV directly`, Math.round(pctStart));

    // Navigate to the report page first to establish the page context
    const reportUrl = `${ADMIN_BASE}${REPORT_URLS[reportType]}`;
    await this.page.goto(reportUrl, { waitUntil: "domcontentloaded", timeout: 60000 });
    await this.page.waitForLoadState("networkidle", { timeout: 10000 }).catch(() => {});

    // Check for Cloudflare
    const pageContent = await this.page.content();
    if (pageContent.includes("Verify you are human") || pageContent.includes("cf-challenge")) {
      throw new Error(
        "Cloudflare is blocking access. Session may have expired. Use 'Test Connection' in Settings to re-authenticate."
      );
    }

    // Read the form's date range (or use provided one)
    const formDateRange = dateRange || await this.page.evaluate(() => {
      const input = document.querySelector('input[name="daterange"]') as HTMLInputElement | null;
      return input?.value || "";
    });

    // Build the fetch URL from the form action
    const fetchUrl = await this.page.evaluate((args: { reportType: string; dateRange: string }) => {
      const form = document.querySelector('button[value="csv"]')?.closest("form") as HTMLFormElement | null;
      if (!form) return "";

      const params: Record<string, string> = {};
      form.querySelectorAll("input[name], select[name]").forEach((el) => {
        const inp = el as HTMLInputElement;
        if (inp.name && inp.value) params[inp.name] = inp.value;
      });

      // Override date range if provided
      if (args.dateRange) params["daterange"] = args.dateRange;
      params["format"] = "csv";

      // Remove empty params
      const qs = Object.entries(params)
        .filter(([, v]) => v)
        .map(([k, v]) => k + "=" + encodeURIComponent(v))
        .join("&");

      return form.action + (form.action.includes("?") ? "&" : "?") + qs;
    }, { reportType, dateRange: formDateRange });

    if (!fetchUrl) {
      this.progress(`No CSV form found for ${reportType}, falling back to scraping`, Math.round(pctStart));
      // Page is already loaded, no need to force re-navigate since we're on the right page
      return this.downloadViaScrape(reportType, dateRange, pctStart, pctEnd, false);
    }

    this.progress(`Downloading ${reportType} CSV via API`, Math.round(pctStart + (pctEnd - pctStart) * 0.2));

    // Fetch the CSV data from within the page context (uses browser cookies/session)
    const csvResult = await this.page.evaluate(async (url: string) => {
      try {
        const resp = await fetch(url, {
          method: "GET",
          credentials: "include",
          redirect: "follow",
        });
        const text = await resp.text();
        const isCSV = resp.headers.get("content-type")?.includes("text/csv") ||
                       (!text.includes("<html") && !text.includes("<!DOCTYPE") && text.includes(","));
        return {
          ok: isCSV && resp.status === 200,
          status: resp.status,
          data: isCSV ? text : "",
          lineCount: isCSV ? text.split("\n").length : 0,
        };
      } catch (e) {
        return { ok: false, status: 0, data: "", lineCount: 0, error: String(e) };
      }
    }, fetchUrl);

    if (!csvResult.ok || !csvResult.data) {
      this.progress(`CSV fetch failed for ${reportType} (status ${csvResult.status}), falling back to scraping`, Math.round(pctStart));
      // Force re-navigation since the current page may be showing a 503 error
      return this.downloadViaScrape(reportType, dateRange, pctStart, pctEnd, true);
    }

    // Save the CSV to disk
    const filePath = join(DOWNLOADS_DIR, `${reportType}-${Date.now()}.csv`);
    writeFileSync(filePath, csvResult.data, "utf8");
    this.progress(
      `Saved ${reportType} CSV (${csvResult.lineCount} rows via direct download)`,
      Math.round(pctEnd)
    );

    return filePath;
  }

  /**
   * Strategy B: Scrape HTML table data from all paginated pages using parallel tabs.
   * Used for reports that don't support format=csv (customers, orders, registrations, firstVisits).
   */
  private async downloadViaScrape(
    reportType: ReportType,
    dateRange?: string,
    pctStart = 0,
    pctEnd = 100,
    forceNavigate = false
  ): Promise<string> {
    if (!this.page) throw new Error("Client not initialized");

    const reportUrl = `${ADMIN_BASE}${REPORT_URLS[reportType]}`;
    this.progress(`Navigating to ${reportType} report`, Math.round(pctStart));

    // Navigate if we're not already on this page, or if forced (e.g. after fetch failure/503)
    if (forceNavigate || !this.page.url().includes(REPORT_URLS[reportType].split("?")[0])) {
      await this.page.goto(reportUrl, { waitUntil: "domcontentloaded", timeout: 60000 });
      await this.page.waitForLoadState("networkidle", { timeout: 10000 }).catch(() => {});

      // Check for Cloudflare
      const pageContent = await this.page.content();
      if (pageContent.includes("Verify you are human") || pageContent.includes("cf-challenge")) {
        throw new Error(
          "Cloudflare is blocking access to report pages. Session may have expired. Use 'Test Connection' in Settings to re-authenticate."
        );
      }
    }

    // Wait for table content
    await this.page.waitForSelector("table, [class*='table'], [class*='report']", { timeout: 30000 }).catch(async () => {
      await this.page!.waitForLoadState("networkidle", { timeout: 5000 }).catch(() => {});
    });

    // Set date range if provided
    if (dateRange) {
      try {
        const dateInput = this.page.locator(SELECTORS.reports.dateRangeInput).first();
        if (await dateInput.isVisible({ timeout: 5000 })) {
          await dateInput.clear();
          await dateInput.fill(dateRange);
          await this.page.keyboard.press("Enter");
          await this.page.waitForSelector("table tbody tr", { timeout: 10000 }).catch(() => {});
        }
      } catch {
        // Date range may not be available on all pages
      }
    }

    const pctRange = pctEnd - pctStart;
    this.progress(`Scraping ${reportType} table data`, Math.round(pctStart));

    // Helper: extract table data from a page
    const extractTable = async (pg: Page) => {
      return pg.evaluate(() => {
        const table = document.querySelector("table");
        if (!table) return null;

        const hdrs: string[] = [];
        table.querySelectorAll("thead th, thead td").forEach((th) => {
          hdrs.push(th.textContent?.trim() || "");
        });

        const headerCount = hdrs.length;
        const rows: string[][] = [];
        table.querySelectorAll("tbody tr").forEach((tr) => {
          const cells: string[] = [];
          tr.querySelectorAll("td").forEach((td) => {
            let text = "";
            const link = td.querySelector("a:not(.btn):not(.dropdown-toggle)");
            if (link) {
              text = link.textContent?.trim() || "";
            } else {
              const clone = td.cloneNode(true) as HTMLElement;
              clone.querySelectorAll(".dropdown, .dropdown-menu, .btn, button, .dropdown-toggle").forEach(el => el.remove());
              text = clone.textContent?.trim() || "";
            }
            cells.push(text);
          });
          if (cells.length > 0) {
            // Handle header/data mismatch: some tables have an extra avatar/initials <td>
            if (cells.length > headerCount) {
              cells.splice(0, cells.length - headerCount);
            }
            rows.push(cells);
          }
        });

        return { headers: hdrs, rows };
      });
    };

    // Load page 1 to get headers and determine total pages
    const firstPageData = await extractTable(this.page);
    if (!firstPageData || firstPageData.rows.length === 0) {
      await this.screenshotOnError(`no-table-${reportType}`);
      throw new Error(`No table data found on ${reportType} report page`);
    }

    const headers = firstPageData.headers;
    const allRows: string[][] = [...firstPageData.rows];

    // Find total page count
    const totalPages = await this.page.evaluate(() => {
      const links = document.querySelectorAll(".pagination a");
      for (const link of links) {
        const text = link.textContent?.trim() || "";
        if (text.startsWith("Last")) {
          const match = (link as HTMLAnchorElement).href.match(/page=(\d+)/);
          return match ? parseInt(match[1]) : null;
        }
      }
      let maxPage = 1;
      for (const link of links) {
        const num = parseInt(link.textContent?.trim() || "");
        if (!isNaN(num) && num > maxPage) maxPage = num;
      }
      return maxPage > 1 ? maxPage : 1;
    });

    const pageCount = totalPages ?? 1;
    this.progress(`Scraping ${reportType}: page 1/${pageCount} (${allRows.length} rows)`, Math.round(pctStart + pctRange * (1 / pageCount)));

    if (pageCount > 1) {
      const BATCH_SIZE = 5;
      const baseUrl = reportUrl.includes("?") ? reportUrl : `${reportUrl}?`;
      const separator = reportUrl.includes("?") ? "&" : "";

      for (let batchStart = 2; batchStart <= pageCount; batchStart += BATCH_SIZE) {
        const batchEnd = Math.min(batchStart + BATCH_SIZE - 1, pageCount);
        const pageNums = Array.from({ length: batchEnd - batchStart + 1 }, (_, i) => batchStart + i);

        const batchPct = Math.round(pctStart + pctRange * (batchEnd / pageCount));
        this.progress(`Scraping ${reportType}: pages ${batchStart}-${batchEnd}/${pageCount} (${allRows.length} rows)`, batchPct);

        const tabPromises = pageNums.map(async (pageNum) => {
          const tab = await this.context!.newPage();
          try {
            const pageUrl = `${baseUrl}${separator}page=${pageNum}`;
            await tab.goto(pageUrl, { waitUntil: "domcontentloaded", timeout: 60000 });
            await tab.waitForSelector("table tbody tr", { timeout: 15000 }).catch(() => {});
            const data = await extractTable(tab);
            return { pageNum, rows: data?.rows || [] };
          } catch (err) {
            console.warn(`[scraper] Tab failed for ${reportType} page=${pageNum}: ${err instanceof Error ? err.message.split("\n")[0] : err}`);
            return { pageNum, rows: [] as string[][] };
          } finally {
            await tab.close();
          }
        });

        const results = await Promise.all(tabPromises);
        results.sort((a, b) => a.pageNum - b.pageNum);
        for (const r of results) {
          allRows.push(...r.rows);
        }
      }
    }

    // Build CSV content — filter out empty header columns
    const validCols = headers.map((h, i) => ({ header: h, index: i })).filter(c => c.header.length > 0);
    const csvHeaders = validCols.map(c => c.header);
    const csvRows = allRows.map(row => validCols.map(c => row[c.index] || ""));

    const csvLines = [
      csvHeaders.map(h => `"${h.replace(/"/g, '""')}"`).join(","),
      ...csvRows.map(row => row.map(cell => `"${cell.replace(/"/g, '""')}"`).join(",")),
    ];

    const filePath = join(DOWNLOADS_DIR, `${reportType}-${Date.now()}.csv`);
    writeFileSync(filePath, csvLines.join("\n"), "utf8");
    this.progress(`Saved ${reportType} CSV (${allRows.length} rows across ${pageCount} pages)`, Math.round(pctEnd));

    return filePath;
  }

  /**
   * Download a single report via fetch using its own dedicated tab.
   * Returns the file path or null if fetch fails (needs scrape fallback).
   */
  private async downloadViaFetchInTab(
    reportType: ReportType,
    dateRange?: string
  ): Promise<string | null> {
    if (!this.context) throw new Error("Client not initialized");

    const tab = await this.context.newPage();
    try {
      const reportUrl = `${ADMIN_BASE}${REPORT_URLS[reportType]}`;
      await tab.goto(reportUrl, { waitUntil: "domcontentloaded", timeout: 60000 });
      await tab.waitForLoadState("networkidle", { timeout: 10000 }).catch(() => {});

      // Check for Cloudflare
      const pageContent = await tab.content();
      if (pageContent.includes("Verify you are human") || pageContent.includes("cf-challenge")) {
        return null; // Will fall back to serial scrape on main page
      }

      // Read the form's date range (or use provided one)
      const formDateRange = dateRange || await tab.evaluate(() => {
        const input = document.querySelector('input[name="daterange"]') as HTMLInputElement | null;
        return input?.value || "";
      });

      // Build the fetch URL from the form action
      const fetchUrl = await tab.evaluate((args: { dateRange: string }) => {
        const form = document.querySelector('button[value="csv"]')?.closest("form") as HTMLFormElement | null;
        if (!form) return "";
        const params: Record<string, string> = {};
        form.querySelectorAll("input[name], select[name]").forEach((el) => {
          const inp = el as HTMLInputElement;
          if (inp.name && inp.value) params[inp.name] = inp.value;
        });
        if (args.dateRange) params["daterange"] = args.dateRange;
        params["format"] = "csv";
        const qs = Object.entries(params)
          .filter(([, v]) => v)
          .map(([k, v]) => k + "=" + encodeURIComponent(v))
          .join("&");
        return form.action + (form.action.includes("?") ? "&" : "?") + qs;
      }, { dateRange: formDateRange });

      if (!fetchUrl) return null;

      // Fetch the CSV data
      const csvResult = await tab.evaluate(async (url: string) => {
        try {
          const resp = await fetch(url, {
            method: "GET",
            credentials: "include",
            redirect: "follow",
          });
          const text = await resp.text();
          const isCSV = resp.headers.get("content-type")?.includes("text/csv") ||
                         (!text.includes("<html") && !text.includes("<!DOCTYPE") && text.includes(","));
          return {
            ok: isCSV && resp.status === 200,
            status: resp.status,
            data: isCSV ? text : "",
            lineCount: isCSV ? text.split("\n").length : 0,
          };
        } catch (e) {
          return { ok: false, status: 0, data: "", lineCount: 0, error: String(e) };
        }
      }, fetchUrl);

      if (!csvResult.ok || !csvResult.data) return null;

      const filePath = join(DOWNLOADS_DIR, `${reportType}-${Date.now()}.csv`);
      writeFileSync(filePath, csvResult.data, "utf8");
      return filePath;
    } catch (err) {
      console.warn(`[scraper] Parallel fetch failed for ${reportType}: ${err instanceof Error ? err.message.split("\n")[0] : err}`);
      return null;
    } finally {
      await tab.close();
    }
  }

  async downloadAllReports(dateRange?: string): Promise<DownloadedFiles> {
    const files: Partial<DownloadedFiles> = {};

    const reportTypes: ReportType[] = [
      "canceledAutoRenews",
      "newAutoRenews",
      "newCustomers",
      "orders",
      "firstVisits",
      "allRegistrations",
      "activeAutoRenews",
      "pausedAutoRenews",
    ];

    const SCRAPE_START = 15;
    const SCRAPE_END = 45;
    const SCRAPE_RANGE = SCRAPE_END - SCRAPE_START;

    const phaseStart = Date.now();

    // Phase 1: Try all CSV-fetch-eligible reports in parallel (4 at a time)
    const fetchEligible = reportTypes.filter((r) => FETCH_CSV_REPORTS.has(r));
    const PARALLEL_LIMIT = 4;
    const needsScrape: ReportType[] = [];

    this.progress("Downloading reports in parallel", SCRAPE_START);

    for (let i = 0; i < fetchEligible.length; i += PARALLEL_LIMIT) {
      const batch = fetchEligible.slice(i, i + PARALLEL_LIMIT);
      const t0 = Date.now();
      const results = await Promise.all(
        batch.map(async (rt) => {
          const filePath = await this.downloadViaFetchInTab(rt, dateRange);
          return { reportType: rt, filePath };
        })
      );
      const batchElapsed = ((Date.now() - t0) / 1000).toFixed(1);
      const batchNames = batch.join(", ");

      for (const { reportType, filePath } of results) {
        if (filePath) {
          files[reportType] = filePath;
          console.log(`[scraper] ${reportType}: OK (parallel fetch)`);
        } else {
          needsScrape.push(reportType);
          console.log(`[scraper] ${reportType}: fetch failed, queued for scrape`);
        }
      }
      console.log(`[scraper] Parallel batch [${batchNames}]: ${batchElapsed}s`);

      const pct = SCRAPE_START + ((i + batch.length) / reportTypes.length) * SCRAPE_RANGE;
      this.progress(`Downloaded ${Object.keys(files).length}/${reportTypes.length} reports`, Math.round(pct));
    }

    // Phase 2: Serially scrape any reports that failed parallel fetch
    // Also scrape any reports not in FETCH_CSV_REPORTS (currently none, but future-proof)
    const nonFetchReports = reportTypes.filter((r) => !FETCH_CSV_REPORTS.has(r));
    const scrapeList = [...needsScrape, ...nonFetchReports];

    if (scrapeList.length > 0) {
      console.log(`[scraper] Falling back to serial scrape for: ${scrapeList.join(", ")}`);
      for (const reportType of scrapeList) {
        const t0 = Date.now();
        const scrapeStart = SCRAPE_START + ((reportTypes.indexOf(reportType)) / reportTypes.length) * SCRAPE_RANGE;
        const scrapeEnd = SCRAPE_START + ((reportTypes.indexOf(reportType) + 1) / reportTypes.length) * SCRAPE_RANGE;
        files[reportType] = await this.downloadViaScrape(reportType, dateRange, scrapeStart, scrapeEnd, true);
        const elapsed = ((Date.now() - t0) / 1000).toFixed(1);
        console.log(`[scraper] ${reportType}: ${elapsed}s (scrape fallback)`);
      }
    }

    console.log(`[scraper] Total download phase: ${((Date.now() - phaseStart) / 1000).toFixed(1)}s`);

    return files as DownloadedFiles;
  }

  private async screenshotOnError(name: string): Promise<void> {
    if (!this.page) return;
    try {
      const screenshotDir = join(DOWNLOADS_DIR, "error-screenshots");
      if (!existsSync(screenshotDir)) mkdirSync(screenshotDir, { recursive: true });
      await this.page.screenshot({
        path: join(screenshotDir, `${name}-${Date.now()}.png`),
        fullPage: true,
      });
    } catch {
      // Ignore screenshot errors
    }
  }

  async cleanup(): Promise<void> {
    await this.context?.close().catch(() => {});
    await this.browser?.close().catch(() => {});
    this.page = null;
    this.context = null;
    this.browser = null;
  }
}
