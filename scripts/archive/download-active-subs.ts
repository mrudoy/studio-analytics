/**
 * Download activeAutoRenews CSV from Union.fit using saved cookies.
 *
 * Usage: npx tsx scripts/download-active-subs.ts
 *
 * This script:
 * 1. Launches a visible Playwright browser
 * 2. Loads saved cookies from data/union-cookies.json
 * 3. Navigates to the active subscriptions report
 * 4. Tries CSV download button, falls back to clicking "Download CSV" in View dropdown
 * 5. Saves to data/downloads/activeAutoRenews-{timestamp}.csv
 */

import { chromium } from "playwright";
import { readFileSync, writeFileSync, existsSync, mkdirSync } from "fs";
import { join } from "path";

const BASE_URL = "https://www.union.fit";
const ORG_SLUG = "sky-ting";
const ADMIN_BASE = `${BASE_URL}/admin/orgs/${ORG_SLUG}`;
const REPORT_PATH = "/report/subscriptions/list?status=active";
const COOKIE_FILE = join(process.cwd(), "data", "union-cookies.json");
const DOWNLOADS_DIR = join(process.cwd(), "data", "downloads");

async function main() {
  if (!existsSync(COOKIE_FILE)) {
    console.error("No cookies found. Run POST /api/test-connection first.");
    process.exit(1);
  }

  if (!existsSync(DOWNLOADS_DIR)) {
    mkdirSync(DOWNLOADS_DIR, { recursive: true });
  }

  console.log("Launching browser...");
  const browser = await chromium.launch({ headless: false });
  const context = await browser.newContext({
    viewport: { width: 1280, height: 900 },
  });

  // Load saved cookies
  const cookies = JSON.parse(readFileSync(COOKIE_FILE, "utf8"));
  await context.addCookies(cookies);
  console.log(`Loaded ${cookies.length} cookies`);

  const page = await context.newPage();

  // Navigate to the report page
  const url = `${ADMIN_BASE}${REPORT_PATH}`;
  console.log(`Navigating to: ${url}`);
  await page.goto(url, { waitUntil: "domcontentloaded", timeout: 60000 });
  await page.waitForLoadState("networkidle", { timeout: 15000 }).catch(() => {});

  // Check for Cloudflare
  const content = await page.content();
  if (content.includes("Verify you are human") || content.includes("cf-challenge")) {
    console.error("Cloudflare is blocking. Need to re-authenticate.");
    console.log("Waiting 30s for manual verification...");
    await page.waitForTimeout(30000);
  }

  // Check page title
  const title = await page.title();
  console.log(`Page title: ${title}`);
  console.log(`Current URL: ${page.url()}`);

  // Strategy 1: Look for CSV form button
  console.log("\nStrategy 1: Looking for CSV form button...");
  const csvButton = await page.$('button[value="csv"]');
  if (csvButton) {
    console.log("Found CSV button! Building fetch URL...");

    const fetchUrl = await page.evaluate(() => {
      const form = document.querySelector('button[value="csv"]')?.closest("form") as HTMLFormElement | null;
      if (!form) return "";
      const params: Record<string, string> = {};
      form.querySelectorAll("input[name], select[name]").forEach((el) => {
        const inp = el as HTMLInputElement;
        if (inp.name && inp.value) params[inp.name] = inp.value;
      });
      params["format"] = "csv";
      const qs = Object.entries(params)
        .filter(([, v]) => v)
        .map(([k, v]) => k + "=" + encodeURIComponent(v))
        .join("&");
      return form.action + (form.action.includes("?") ? "&" : "?") + qs;
    });

    if (fetchUrl) {
      console.log(`Fetch URL: ${fetchUrl}`);
      const csvResult = await page.evaluate(async (u: string) => {
        const resp = await fetch(u, { credentials: "include" });
        const text = await resp.text();
        return { status: resp.status, length: text.length, lines: text.split("\n").length, data: text };
      }, fetchUrl);

      if (csvResult.status === 200 && csvResult.length > 100) {
        const filePath = join(DOWNLOADS_DIR, `activeAutoRenews-${Date.now()}.csv`);
        writeFileSync(filePath, csvResult.data, "utf8");
        console.log(`Saved! ${csvResult.lines} lines, ${csvResult.length} bytes -> ${filePath}`);
        await browser.close();
        return;
      } else {
        console.log(`CSV fetch returned status=${csvResult.status}, length=${csvResult.length}`);
      }
    }
  } else {
    console.log("No CSV button found on page.");
  }

  // Strategy 2: Look for "View" dropdown with "Download CSV" option
  console.log("\nStrategy 2: Looking for View dropdown...");
  const viewButton = await page.$('button:has-text("View")');
  if (viewButton) {
    console.log("Found View button, clicking...");
    await viewButton.click();
    await page.waitForTimeout(1000);

    const downloadLink = await page.$('a:has-text("Download CSV"), button:has-text("Download CSV")');
    if (downloadLink) {
      console.log("Found Download CSV option!");

      // Set up download handler
      const [download] = await Promise.all([
        page.waitForEvent("download", { timeout: 30000 }).catch(() => null),
        downloadLink.click(),
      ]);

      if (download) {
        const filePath = join(DOWNLOADS_DIR, `activeAutoRenews-${Date.now()}.csv`);
        await download.saveAs(filePath);
        console.log(`Downloaded via click! -> ${filePath}`);
        await browser.close();
        return;
      } else {
        console.log("Click didn't trigger a download. May have triggered an email export.");
      }
    } else {
      console.log("No Download CSV option in dropdown.");
    }
  } else {
    console.log("No View button found.");
  }

  // Strategy 3: Look for any download/export links
  console.log("\nStrategy 3: Looking for any export links...");
  const allLinks = await page.$$eval("a, button", (els) =>
    els.map((e) => ({ tag: e.tagName, text: e.textContent?.trim() || "", href: (e as HTMLAnchorElement).href || "" }))
      .filter((e) => /csv|download|export/i.test(e.text) || /csv|download|export/i.test(e.href))
  );
  console.log("Export-related elements found:", JSON.stringify(allLinks, null, 2));

  // Strategy 4: Check if URL works with format=csv appended
  console.log("\nStrategy 4: Trying direct URL with format=csv...");
  const directUrl = `${ADMIN_BASE}${REPORT_PATH}&format=csv`;
  const directResult = await page.evaluate(async (u: string) => {
    const resp = await fetch(u, { credentials: "include" });
    const contentType = resp.headers.get("content-type") || "";
    const text = await resp.text();
    const preview = text.substring(0, 500);
    return { status: resp.status, contentType, length: text.length, preview, data: text };
  }, directUrl);

  console.log(`Direct URL result: status=${directResult.status}, type=${directResult.contentType}, length=${directResult.length}`);
  console.log(`Preview: ${directResult.preview.substring(0, 200)}`);

  if (directResult.status === 200 && directResult.length > 100 && !directResult.data.includes("<html")) {
    const filePath = join(DOWNLOADS_DIR, `activeAutoRenews-${Date.now()}.csv`);
    writeFileSync(filePath, directResult.data, "utf8");
    console.log(`Saved via direct URL! -> ${filePath}`);
    await browser.close();
    return;
  }

  // If we got here, take a screenshot for debugging
  const screenshotPath = join(DOWNLOADS_DIR, "error-screenshots", `active-subs-debug-${Date.now()}.png`);
  await page.screenshot({ path: screenshotPath, fullPage: true });
  console.log(`\nAll strategies failed. Screenshot saved to: ${screenshotPath}`);
  console.log("Keeping browser open for 60s for manual inspection...");
  await page.waitForTimeout(60000);

  await browser.close();
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
