import { chromium as pwChromium, Browser, BrowserContext, Page } from "playwright";
import { chromium as stealthChromium } from "playwright-extra";
import StealthPlugin from "puppeteer-extra-plugin-stealth";
import { join } from "path";
import { mkdirSync, existsSync, readFileSync, writeFileSync } from "fs";

// Apply stealth plugin to playwright-extra chromium
stealthChromium.use(StealthPlugin());

const BASE_URL = "https://www.union.fit";
const DOWNLOADS_DIR = join(process.cwd(), "data", "downloads");
const COOKIE_FILE = join(process.cwd(), "data", "union-cookies.json");

/**
 * UnionClient manages browser sessions for Union.fit.
 *
 * Responsibilities:
 *   - Launch Playwright browser (stealth mode)
 *   - Login to Union.fit
 *   - Handle Cloudflare challenges
 *   - Manage cookies for session persistence
 *   - Expose the active page for csv-trigger.ts to use
 *
 * CSV downloading is handled entirely by csv-trigger.ts (clicks "Download CSV"
 * buttons) and email-pipeline.ts (polls Gmail for emailed CSVs). This class
 * does NOT scrape HTML tables or fetch CSV data directly.
 */
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

    const signinUrl = `${BASE_URL}/signin`;

    // Navigate with retry — sometimes pages redirect to unexpected URLs (favicon.ico, etc.)
    for (let navAttempt = 0; navAttempt < 3; navAttempt++) {
      this.progress(`Navigating to Union.fit sign-in page${navAttempt > 0 ? ` (attempt ${navAttempt + 1})` : ""}`);
      await this.page.goto(signinUrl, { waitUntil: "domcontentloaded", timeout: 60000 });
      await this.page.waitForLoadState("networkidle", { timeout: 10000 }).catch(() => {});

      const urlAfterNav = this.page.url();
      console.log(`[union-client] Post-navigation URL: ${urlAfterNav}`);

      // If we ended up at favicon.ico or a completely wrong URL, clear session cookies and retry
      // Keep cf_clearance (Cloudflare bypass) but remove session cookies that may cause bad redirects
      if (urlAfterNav.includes("favicon.ico") || (!urlAfterNav.includes("union.fit") && !urlAfterNav.includes("localhost"))) {
        console.warn(`[union-client] Unexpected URL after navigation: ${urlAfterNav} — clearing session cookies and retrying`);
        if (this.context) {
          // Get current cookies, keep only Cloudflare and analytics ones
          const currentCookies = await this.context.cookies();
          const keepNames = new Set(["cf_clearance", "_ga", "_ga_E4S9TLB3M8", "_fbp"]);
          const sessionCookies = currentCookies.filter((c) => !keepNames.has(c.name));
          console.log(`[union-client] Clearing ${sessionCookies.length} session cookies, keeping ${keepNames.size} essential cookies`);
          await this.context.clearCookies();
          // Re-add only the essential cookies
          const essentialCookies = currentCookies.filter((c) => keepNames.has(c.name));
          if (essentialCookies.length > 0) {
            await this.context.addCookies(essentialCookies);
          }
        }
        await this.page.waitForTimeout(2000);
        continue;
      }

      break; // Navigation succeeded to a reasonable URL
    }

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
      return { success: false, message: `Login failed. Current URL: ${this.page.url()}` };
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
