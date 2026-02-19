/**
 * Debug: Check how many pages and rows the active auto-renews report has
 */
import { chromium } from "playwright";
import { readFileSync, existsSync } from "fs";
import { join } from "path";

const BASE_URL = "https://www.union.fit";
const ORG_SLUG = "sky-ting";
const COOKIE_FILE = join(process.cwd(), "data", "union-cookies.json");

async function main() {
  const browser = await chromium.launch({
    channel: "chrome",
    headless: false,
    args: ["--disable-blink-features=AutomationControlled", "--window-position=-2400,-2400"],
  });

  const context = await browser.newContext({ viewport: { width: 1440, height: 900 } });

  if (existsSync(COOKIE_FILE)) {
    const cookies = JSON.parse(readFileSync(COOKIE_FILE, "utf8"));
    await context.addCookies(cookies);
  }

  const page = await context.newPage();

  const testUrl = BASE_URL + "/admin/orgs/" + ORG_SLUG + "/report/subscriptions/growth?filter=active";
  console.log("Loading:", testUrl);
  await page.goto(testUrl, { waitUntil: "domcontentloaded", timeout: 60000 });
  await page.waitForTimeout(5000);

  // Check table and pagination
  const info = await page.evaluate(() => {
    const table = document.querySelector("table");
    if (!table) return { error: "No table found", url: window.location.href, bodySnippet: document.body.textContent?.substring(0, 500) };

    const headers: string[] = [];
    table.querySelectorAll("thead th, thead td").forEach(th => headers.push(th.textContent?.trim() || ""));

    const rowCount = table.querySelectorAll("tbody tr").length;

    const pageLinks: string[] = [];
    document.querySelectorAll(".pagination a").forEach(a => {
      pageLinks.push((a as HTMLAnchorElement).textContent?.trim() + " -> " + (a as HTMLAnchorElement).href);
    });

    const dateRange = (document.querySelector('input[name="daterange"]') as HTMLInputElement)?.value || "none";

    return { headers, rowCount, pageLinks: pageLinks.slice(0, 10), dateRange, url: window.location.href };
  });

  console.log("\n=== ACTIVE AUTO-RENEWS REPORT ===");
  console.log(JSON.stringify(info, null, 2));

  await browser.close();
}

main().catch(console.error);
