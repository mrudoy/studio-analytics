import { chromium as pwChromium, Browser, BrowserContext, Page } from "playwright";
import { chromium as stealthChromium } from "playwright-extra";
import StealthPlugin from "puppeteer-extra-plugin-stealth";
import { join } from "path";
import { mkdirSync, existsSync, writeFileSync, readFileSync } from "fs";
import { SELECTORS, REPORT_URLS, ReportType } from "./selectors";
import type { DownloadedFiles } from "@/types/union-data";

// Apply stealth plugin to playwright-extra chromium
stealthChromium.use(StealthPlugin());

const BASE_URL = "https://www.union.fit";
const ORG_SLUG = "sky-ting";
const ADMIN_BASE = `${BASE_URL}/admin/orgs/${ORG_SLUG}`;
const DOWNLOADS_DIR = join(process.cwd(), "data", "downloads");
const COOKIE_FILE = join(process.cwd(), "data", "union-cookies.json");

/** Maximum time (ms) for a single fetch-tab operation before it's forcibly killed. */
const TAB_TIMEOUT_MS = 90_000;
/** Longer timeout for paginated scrape tabs (100+ pages can take minutes). */
const SCRAPE_TAB_TIMEOUT_MS = 300_000;

/**
 * Race a promise against a timeout. If the timeout fires first,
 * run the cleanup callback (e.g. close the tab) and throw.
 */
async function withTabTimeout<T>(
  promise: Promise<T>,
  label: string,
  cleanup?: () => Promise<void>,
  timeoutMs: number = TAB_TIMEOUT_MS
): Promise<T> {
  let timer: ReturnType<typeof setTimeout>;
  const timeout = new Promise<never>((_, reject) => {
    timer = setTimeout(
      () => reject(new Error(`Tab timeout after ${timeoutMs / 1000}s: ${label}`)),
      timeoutMs
    );
  });
  try {
    return await Promise.race([promise, timeout]);
  } catch (err) {
    if (cleanup) await cleanup().catch(() => {});
    throw err;
  } finally {
    clearTimeout(timer!);
  }
}

/**
 * Reports that support direct CSV download via fetch() with format=csv.
 * Only subscription endpoints have the CSV download button in their form.
 * Other reports (orders, customers, registrations, firstVisits) don't have
 * button[value="csv"] — they go straight to HTML table scraping.
 */
const FETCH_CSV_REPORTS: Set<ReportType> = new Set([
  "canceledAutoRenews",
  "newAutoRenews",
  "activeAutoRenews",
  "pausedAutoRenews",
  "trialingAutoRenews",
  "fullRegistrations", // Try CSV fetch; auto-falls back to HTML scrape if no CSV button
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

  /** Expose the active page for external modules (e.g. csv-trigger). */
  getPage(): Page | null {
    return this.page;
  }

  async initialize(): Promise<void> {
    if (!existsSync(DOWNLOADS_DIR)) {
      mkdirSync(DOWNLOADS_DIR, { recursive: true });
    }

    // Use playwright-extra with stealth plugin to bypass Cloudflare detection.
    // Must use headed mode (Xvfb on Railway) — Cloudflare blocks headless browsers.
    // Position window off-screen so it doesn't steal focus during local runs.
    try {
      this.browser = await stealthChromium.launch({
        headless: false,
        args: [
          "--no-sandbox",
          "--disable-setuid-sandbox",
          "--disable-blink-features=AutomationControlled",
          "--disable-gpu",
          "--disable-dev-shm-usage",
          "--disable-infobars",
          "--window-position=-2400,-2400",
          "--window-size=1440,900",
        ],
      });
    } catch (err) {
      // Fall back to standard Playwright if stealth fails (e.g. compatibility issues)
      console.warn("[scraper] Stealth launch failed, falling back to standard Playwright:", err);
      this.browser = await pwChromium.launch({
        headless: false,
        args: [
          "--no-sandbox",
          "--disable-setuid-sandbox",
          "--disable-blink-features=AutomationControlled",
          "--disable-gpu",
          "--disable-dev-shm-usage",
          "--disable-infobars",
          "--window-position=-2400,-2400",
          "--window-size=1440,900",
        ],
      });
    }

    // Real Chrome user agent to avoid detection
    const userAgent =
      "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36";

    this.context = await this.browser.newContext({
      acceptDownloads: true,
      viewport: { width: 1440, height: 900 },
      userAgent,
      locale: "en-US",
      timezoneId: "America/New_York",
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

    // Additional anti-detection: patch navigator properties
    await this.page.addInitScript(() => {
      // Remove webdriver flag
      Object.defineProperty(navigator, "webdriver", { get: () => false });
      // Ensure chrome object exists (Cloudflare checks for it)
      if (!(window as unknown as Record<string, unknown>).chrome) {
        (window as unknown as Record<string, unknown>).chrome = {
          runtime: {},
          loadTimes: function () {},
          csi: function () {},
          app: { isInstalled: false },
        };
      }
      // Fake plugins array (headless usually has empty plugins)
      Object.defineProperty(navigator, "plugins", {
        get: () => [1, 2, 3, 4, 5],
      });
      // Fake languages
      Object.defineProperty(navigator, "languages", {
        get: () => ["en-US", "en"],
      });
    });
  }

  /**
   * Initialize in headed (visible) mode for manual Cloudflare verification.
   * Uses real Chrome (not Playwright's Chromium) to pass Cloudflare.
   */
  async initializeHeaded(): Promise<void> {
    if (!existsSync(DOWNLOADS_DIR)) {
      mkdirSync(DOWNLOADS_DIR, { recursive: true });
    }

    // Use stealth Chromium in headed mode — visible to user for Cloudflare verification
    try {
      this.browser = await stealthChromium.launch({
        headless: false,
        args: [
          "--no-sandbox",
          "--disable-setuid-sandbox",
          "--disable-blink-features=AutomationControlled",
          "--disable-gpu",
          "--disable-dev-shm-usage",
          "--disable-infobars",
        ],
        slowMo: 100,
      });
    } catch (err) {
      // Fall back to standard Playwright if stealth fails
      console.warn("[scraper] Stealth launch failed, falling back to standard Playwright:", err);
      this.browser = await pwChromium.launch({
        headless: false,
        args: [
          "--no-sandbox",
          "--disable-setuid-sandbox",
          "--disable-blink-features=AutomationControlled",
          "--disable-gpu",
          "--disable-dev-shm-usage",
          "--disable-infobars",
        ],
        slowMo: 100,
      });
    }

    const userAgent =
      "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36";

    this.context = await this.browser.newContext({
      acceptDownloads: true,
      viewport: { width: 1440, height: 900 },
      userAgent,
      locale: "en-US",
      timezoneId: "America/New_York",
    });

    this.page = await this.context.newPage();

    // Anti-detection scripts
    await this.page.addInitScript(() => {
      Object.defineProperty(navigator, "webdriver", { get: () => false });
      if (!(window as unknown as Record<string, unknown>).chrome) {
        (window as unknown as Record<string, unknown>).chrome = {
          runtime: {},
          loadTimes: function () {},
          csi: function () {},
          app: { isInstalled: false },
        };
      }
      Object.defineProperty(navigator, "plugins", {
        get: () => [1, 2, 3, 4, 5],
      });
      Object.defineProperty(navigator, "languages", {
        get: () => ["en-US", "en"],
      });
    });
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

    // Check for and handle Cloudflare challenge with extended patience
    await this.handleCloudflareChallenge();

    // Check if already logged in (redirected to dashboard or admin area)
    const currentUrl = this.page.url();
    if (currentUrl.includes("/dashboard") || currentUrl.includes("/admin/")) {
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
      const content = await this.page.content();
      const hasCf = content.includes("cf-") || content.includes("Cloudflare") || content.includes("Verify you are human");
      throw new Error(
        `Login form not found${hasCf ? " (Cloudflare still blocking)" : ""}. Current URL: ${this.page.url()}. Use 'Test Connection' in Settings to authenticate manually first.`
      );
    }

    // Add small random delays between actions to appear more human-like
    await this.page.waitForTimeout(500 + Math.random() * 1000);

    this.progress("Filling login credentials");
    // Type slowly instead of instant fill to avoid bot detection
    await this.page.click('input[type="email"], input[name="email"]');
    await this.page.keyboard.type(email, { delay: 50 + Math.random() * 80 });
    await this.page.waitForTimeout(300 + Math.random() * 500);
    await this.page.click('input[type="password"]');
    await this.page.keyboard.type(password, { delay: 50 + Math.random() * 80 });
    await this.page.waitForTimeout(300 + Math.random() * 500);

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
   * Handle Cloudflare challenge with patience — wait up to 45 seconds for
   * the challenge to auto-resolve (stealth plugin helps with Turnstile).
   */
  private async handleCloudflareChallenge(): Promise<void> {
    if (!this.page) return;

    const isCloudflare = (content: string) =>
      content.includes("Verify you are human") ||
      content.includes("cf-challenge") ||
      content.includes("cf-turnstile") ||
      content.includes("challenge-platform") ||
      content.includes("Just a moment");

    let content = await this.page.content();
    if (!isCloudflare(content)) return;

    this.progress("Cloudflare challenge detected — waiting for auto-resolution...");
    const startWait = Date.now();
    const maxWait = 45000; // 45 seconds

    while (Date.now() - startWait < maxWait) {
      await this.page.waitForTimeout(2000);
      content = await this.page.content();
      if (!isCloudflare(content)) {
        this.progress("Cloudflare challenge resolved!");
        await this.page.waitForTimeout(1000); // Let page settle
        return;
      }

      // Try clicking the Turnstile checkbox if it appeared
      try {
        const turnstileFrame = this.page.frameLocator('iframe[src*="challenges.cloudflare.com"]');
        const checkbox = turnstileFrame.locator('input[type="checkbox"], .cb-lb');
        if (await checkbox.isVisible({ timeout: 500 }).catch(() => false)) {
          this.progress("Clicking Cloudflare Turnstile checkbox...");
          await checkbox.click();
          await this.page.waitForTimeout(3000);
        }
      } catch {
        // No turnstile checkbox found, keep waiting
      }
    }

    // Still blocked after max wait
    await this.screenshotOnError("cloudflare-blocked");
    throw new Error(
      "Cloudflare bot protection is blocking access after 45s. Please use the 'Test Connection' button in Settings to authenticate in a visible browser first."
    );
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

    // Load page 1 to get headers and determine total pages
    const firstPageData = await this.extractTable(this.page);
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
          const label = `${reportType} page=${pageNum}`;

          const work = async (): Promise<{ pageNum: number; rows: string[][] }> => {
            try {
              const pageUrl = `${baseUrl}${separator}page=${pageNum}`;
              await tab.goto(pageUrl, { waitUntil: "domcontentloaded", timeout: 60000 });
              await tab.waitForSelector("table tbody tr", { timeout: 15000 }).catch(() => {});
              const data = await this.extractTable(tab);
              return { pageNum, rows: data?.rows || [] };
            } catch (err) {
              console.warn(`[scraper] Tab failed for ${label}: ${err instanceof Error ? err.message.split("\n")[0] : err}`);
              return { pageNum, rows: [] as string[][] };
            } finally {
              await tab.close().catch(() => {});
            }
          };

          try {
            return await withTabTimeout(work(), label, async () => {
              await tab.close().catch(() => {});
            });
          } catch (err) {
            console.warn(`[scraper] Tab timed out for ${label}: ${err instanceof Error ? err.message.split("\n")[0] : err}`);
            return { pageNum, rows: [] as string[][] };
          }
        });

        const settled = await Promise.allSettled(tabPromises);
        const results = settled.map((s, i) =>
          s.status === "fulfilled"
            ? s.value
            : { pageNum: pageNums[i], rows: [] as string[][] }
        );
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
    const label = `fetch-${reportType}`;

    const work = async (): Promise<string | null> => {
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
        await tab.close().catch(() => {});
      }
    };

    try {
      return await withTabTimeout(work(), label, async () => {
        await tab.close().catch(() => {});
      });
    } catch (err) {
      console.warn(`[scraper] Tab timed out for ${label}: ${err instanceof Error ? err.message.split("\n")[0] : err}`);
      return null;
    }
  }

  /**
   * Scrape an HTML-table report in its own dedicated tab (no dependency on this.page).
   * Used for reports that don't support CSV fetch (orders, customers, registrations, firstVisits).
   * Opens sub-tabs for paginated pages, same pattern as downloadViaScrape.
   */
  private async scrapeReportInTab(
    reportType: ReportType,
    dateRange?: string
  ): Promise<string> {
    if (!this.context) throw new Error("Client not initialized");

    const tab = await this.context.newPage();
    const label = `scrape-tab-${reportType}`;

    const work = async (): Promise<string> => {
      try {
        const reportUrl = `${ADMIN_BASE}${REPORT_URLS[reportType]}`;
        await tab.goto(reportUrl, { waitUntil: "domcontentloaded", timeout: 60000 });
        await tab.waitForLoadState("networkidle", { timeout: 10000 }).catch(() => {});

        // Check for Cloudflare
        const pageContent = await tab.content();
        if (pageContent.includes("Verify you are human") || pageContent.includes("cf-challenge")) {
          throw new Error("Cloudflare is blocking access. Use 'Test Connection' in Settings.");
        }

        // Wait for table
        await tab.waitForSelector("table, [class*='table'], [class*='report']", { timeout: 30000 }).catch(async () => {
          await tab.waitForLoadState("networkidle", { timeout: 5000 }).catch(() => {});
        });

        // Set date range if provided
        if (dateRange) {
          try {
            const dateInput = tab.locator(SELECTORS.reports.dateRangeInput).first();
            if (await dateInput.isVisible({ timeout: 5000 })) {
              await dateInput.clear();
              await dateInput.fill(dateRange);
              await tab.keyboard.press("Enter");
              await tab.waitForSelector("table tbody tr", { timeout: 10000 }).catch(() => {});
            }
          } catch { /* Date range may not be available */ }
        }

        // Extract first page
        const firstPageData = await this.extractTable(tab);
        if (!firstPageData || firstPageData.rows.length === 0) {
          throw new Error(`No table data found on ${reportType} report page`);
        }

        const headers = firstPageData.headers;
        const allRows: string[][] = [...firstPageData.rows];

        // Detect pagination
        const totalPages = await tab.evaluate(() => {
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

        // Scrape remaining pages in parallel sub-tabs (batches of 8)
        if (pageCount > 1) {
          const BATCH_SIZE = 8;
          const baseUrl = reportUrl.includes("?") ? reportUrl : `${reportUrl}?`;
          const separator = reportUrl.includes("?") ? "&" : "";

          for (let batchStart = 2; batchStart <= pageCount; batchStart += BATCH_SIZE) {
            const batchEnd = Math.min(batchStart + BATCH_SIZE - 1, pageCount);
            const pageNums = Array.from({ length: batchEnd - batchStart + 1 }, (_, i) => batchStart + i);

            const tabPromises = pageNums.map(async (pageNum) => {
              const subTab = await this.context!.newPage();
              const subLabel = `${reportType} page=${pageNum}`;
              const subWork = async (): Promise<{ pageNum: number; rows: string[][] }> => {
                try {
                  await subTab.goto(`${baseUrl}${separator}page=${pageNum}`, { waitUntil: "domcontentloaded", timeout: 60000 });
                  await subTab.waitForSelector("table tbody tr", { timeout: 15000 }).catch(() => {});
                  const data = await this.extractTable(subTab);
                  return { pageNum, rows: data?.rows || [] };
                } catch (err) {
                  console.warn(`[scraper] Tab failed for ${subLabel}: ${err instanceof Error ? err.message.split("\n")[0] : err}`);
                  return { pageNum, rows: [] as string[][] };
                } finally {
                  await subTab.close().catch(() => {});
                }
              };
              try {
                return await withTabTimeout(subWork(), subLabel, async () => { await subTab.close().catch(() => {}); });
              } catch (err) {
                console.warn(`[scraper] Tab timed out for ${subLabel}: ${err instanceof Error ? err.message.split("\n")[0] : err}`);
                return { pageNum, rows: [] as string[][] };
              }
            });

            const settled = await Promise.allSettled(tabPromises);
            const results = settled.map((s, i) =>
              s.status === "fulfilled" ? s.value : { pageNum: pageNums[i], rows: [] as string[][] }
            );
            results.sort((a, b) => a.pageNum - b.pageNum);
            for (const r of results) allRows.push(...r.rows);
          }
        }

        // Build CSV
        const validCols = headers.map((h, i) => ({ header: h, index: i })).filter(c => c.header.length > 0);
        const csvHeaders = validCols.map(c => c.header);
        const csvRows = allRows.map(row => validCols.map(c => row[c.index] || ""));
        const csvLines = [
          csvHeaders.map(h => `"${h.replace(/"/g, '""')}"`).join(","),
          ...csvRows.map(row => row.map(cell => `"${cell.replace(/"/g, '""')}"`).join(",")),
        ];

        const filePath = join(DOWNLOADS_DIR, `${reportType}-${Date.now()}.csv`);
        writeFileSync(filePath, csvLines.join("\n"), "utf8");
        console.log(`[scraper] ${reportType}: ${allRows.length} rows across ${pageCount} pages (parallel scrape)`);
        return filePath;
      } finally {
        await tab.close().catch(() => {});
      }
    };

    try {
      return await withTabTimeout(work(), label, async () => { await tab.close().catch(() => {}); }, SCRAPE_TAB_TIMEOUT_MS);
    } catch (err) {
      console.warn(`[scraper] scrapeReportInTab timed out for ${label}: ${err instanceof Error ? err.message.split("\n")[0] : err}`);
      throw err; // Re-throw so caller can fall back to serial scrape
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
      "trialingAutoRenews",
      "fullRegistrations",
      // revenueCategories excluded — scraped in Phase 3 (lightweight, no pagination)
    ];

    // Lightweight reports are scraped in a separate Phase 3 (no pagination, no parallelism pressure)
    const LIGHTWEIGHT_REPORTS: ReportType[] = ["revenueCategories"];

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
      const PER_FETCH_TIMEOUT = 120_000; // 2 minutes per report fetch
      const settled = await Promise.allSettled(
        batch.map(async (rt) => {
          const filePath = await withTabTimeout(
            this.downloadViaFetchInTab(rt, dateRange),
            `CSV fetch: ${rt}`,
            undefined,
            PER_FETCH_TIMEOUT
          );
          return { reportType: rt, filePath };
        })
      );
      const results = settled.map((s, i) =>
        s.status === "fulfilled"
          ? s.value
          : { reportType: batch[i], filePath: null as string | null }
      );
      const batchElapsed = ((Date.now() - t0) / 1000).toFixed(1);
      const batchNames = batch.join(", ");

      const retryQueue: ReportType[] = [];
      for (const { reportType, filePath } of results) {
        if (filePath) {
          files[reportType] = filePath;
          console.log(`[scraper] ${reportType}: OK (parallel fetch)`);
        } else {
          retryQueue.push(reportType);
          console.log(`[scraper] ${reportType}: fetch failed, will retry once`);
        }
      }
      console.log(`[scraper] Parallel batch [${batchNames}]: ${batchElapsed}s`);

      // Retry failed CSV fetches once before falling back to scrape
      for (const rt of retryQueue) {
        console.log(`[scraper] ${rt}: retrying CSV fetch...`);
        let retryPath: string | null = null;
        try {
          retryPath = await withTabTimeout(
            this.downloadViaFetchInTab(rt, dateRange),
            `CSV fetch retry: ${rt}`,
            undefined,
            PER_FETCH_TIMEOUT
          );
        } catch (err) {
          console.warn(`[scraper] ${rt}: retry timed out — ${err instanceof Error ? err.message : err}`);
        }
        if (retryPath) {
          files[rt] = retryPath;
          console.log(`[scraper] ${rt}: OK (CSV fetch retry)`);
        } else {
          needsScrape.push(rt);
          console.log(`[scraper] ${rt}: retry failed, queued for scrape`);
        }
      }

      const totalReportCount = reportTypes.length + LIGHTWEIGHT_REPORTS.length;
      const pct = SCRAPE_START + ((i + batch.length) / totalReportCount) * SCRAPE_RANGE;
      this.progress(`Downloaded ${Object.keys(files).length}/${totalReportCount} reports`, Math.round(pct));
    }

    // Phase 2: Scrape reports that don't support CSV fetch — parallel batches of 3
    const nonFetchReports = reportTypes.filter((r) => !FETCH_CSV_REPORTS.has(r));
    const scrapeList = [...needsScrape, ...nonFetchReports];

    if (scrapeList.length > 0) {
      const SCRAPE_PARALLEL = 3;
      console.log(`[scraper] Scraping ${scrapeList.length} reports (${SCRAPE_PARALLEL} at a time): ${scrapeList.join(", ")}`);

      for (let i = 0; i < scrapeList.length; i += SCRAPE_PARALLEL) {
        const batch = scrapeList.slice(i, i + SCRAPE_PARALLEL);
        const t0 = Date.now();

        const PER_SCRAPE_TIMEOUT = 300_000; // 5 minutes per report scrape
        const settled = await Promise.allSettled(
          batch.map((rt) =>
            withTabTimeout(
              this.scrapeReportInTab(rt, dateRange),
              `Scrape: ${rt}`,
              undefined,
              PER_SCRAPE_TIMEOUT
            )
          )
        );

        for (let j = 0; j < batch.length; j++) {
          const rt = batch[j];
          const result = settled[j];
          if (result.status === "fulfilled") {
            files[rt] = result.value;
            console.log(`[scraper] ${rt}: OK (parallel scrape)`);
          } else {
            console.warn(`[scraper] ${rt}: parallel scrape failed, trying serial fallback`);
            try {
              files[rt] = await withTabTimeout(
                this.downloadViaScrape(rt, dateRange, 0, 100, true),
                `Serial scrape fallback: ${rt}`,
                undefined,
                PER_SCRAPE_TIMEOUT
              );
              console.log(`[scraper] ${rt}: OK (serial scrape fallback)`);
            } catch (fallbackErr) {
              // fullRegistrations is optional — don't fail the pipeline if it can't be downloaded
              if (rt === "fullRegistrations") {
                console.warn(`[scraper] ${rt}: serial scrape also failed — skipping (non-fatal)`);
              } else {
                throw new Error(`Failed to download ${rt}: ${fallbackErr instanceof Error ? fallbackErr.message : fallbackErr}`);
              }
            }
          }
        }

        const batchElapsed = ((Date.now() - t0) / 1000).toFixed(1);
        console.log(`[scraper] Scrape batch [${batch.join(", ")}]: ${batchElapsed}s`);

        const totalReports = reportTypes.length + LIGHTWEIGHT_REPORTS.length;
        const pct = SCRAPE_START + ((Object.keys(files).length) / totalReports) * SCRAPE_RANGE;
        this.progress(`Scraping ${batch.join(", ")} (${Object.keys(files).length}/${totalReports} done)`, Math.round(pct));
      }
    }

    // Phase 3: Scrape lightweight single-page reports (no pagination competition)
    const lightweightToScrape = LIGHTWEIGHT_REPORTS.filter((rt) => !files[rt]);

    if (lightweightToScrape.length > 0) {
      console.log(`[scraper] Phase 3: Scraping ${lightweightToScrape.length} lightweight reports: ${lightweightToScrape.join(", ")}`);
      for (const rt of lightweightToScrape) {
        try {
          files[rt] = await withTabTimeout(
            this.scrapeReportInTab(rt, dateRange),
            `Lightweight scrape: ${rt}`,
            undefined,
            120_000 // 2-minute timeout — plenty for a single-page table
          );
          console.log(`[scraper] ${rt}: OK (lightweight scrape)`);
        } catch (err) {
          console.warn(`[scraper] ${rt}: lightweight scrape failed — ${err instanceof Error ? err.message : err}`);
          // Non-fatal: pipeline continues without this data
        }
      }
    }

    console.log(`[scraper] Total download phase: ${((Date.now() - phaseStart) / 1000).toFixed(1)}s`);

    return files as DownloadedFiles;
  }

  /** Extract headers + rows from an HTML table on the given page. */
  private async extractTable(pg: Page): Promise<{ headers: string[]; rows: string[][] } | null> {
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
          if (cells.length > headerCount) {
            cells.splice(0, cells.length - headerCount);
          }
          rows.push(cells);
        }
      });

      return { headers: hdrs, rows };
    });
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
