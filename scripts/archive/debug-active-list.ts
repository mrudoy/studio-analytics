/**
 * Debug: Check the /list?status=active endpoint for active auto-renews
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

  // Try the list endpoint
  const testUrl = BASE_URL + "/admin/orgs/" + ORG_SLUG + "/report/subscriptions/list?status=active";
  console.log("Loading:", testUrl);
  await page.goto(testUrl, { waitUntil: "domcontentloaded", timeout: 60000 });
  await page.waitForTimeout(5000);

  const info = await page.evaluate(() => {
    const table = document.querySelector("table");
    if (!table) return { error: "No table found", url: window.location.href, bodySnippet: document.body.textContent?.substring(0, 500) };

    const headers: string[] = [];
    table.querySelectorAll("thead th, thead td").forEach(th => headers.push(th.textContent?.trim() || ""));

    const rowCount = table.querySelectorAll("tbody tr").length;

    const pageLinks: string[] = [];
    document.querySelectorAll(".pagination a").forEach(a => {
      pageLinks.push((a as HTMLAnchorElement).textContent?.trim() + " -> " + (a as HTMLAnchorElement).href.substring(0, 120));
    });

    // Check for export/download options
    const flashMessages: string[] = [];
    document.querySelectorAll(".flash, .alert, [class*='flash'], [class*='alert']").forEach(el => {
      flashMessages.push(el.textContent?.trim().substring(0, 200) || "");
    });

    return { headers, rowCount, pageLinks: pageLinks.slice(0, 10), url: window.location.href, flashMessages };
  });

  console.log("\n=== SUBSCRIPTION LIST (active) ===");
  console.log(JSON.stringify(info, null, 2));

  // Also try the People > All People page
  const peopleUrl = BASE_URL + "/admin/orgs/" + ORG_SLUG + "/memberships";
  console.log("\nLoading:", peopleUrl);
  await page.goto(peopleUrl, { waitUntil: "domcontentloaded", timeout: 60000 });
  await page.waitForTimeout(5000);

  const peopleInfo = await page.evaluate(() => {
    const table = document.querySelector("table");
    if (!table) return { error: "No table found", url: window.location.href };

    const headers: string[] = [];
    table.querySelectorAll("thead th, thead td").forEach(th => headers.push(th.textContent?.trim() || ""));

    const rowCount = table.querySelectorAll("tbody tr").length;

    const pageLinks: string[] = [];
    document.querySelectorAll(".pagination a").forEach(a => {
      pageLinks.push((a as HTMLAnchorElement).textContent?.trim() + " -> " + (a as HTMLAnchorElement).href.substring(0, 120));
    });

    return { headers, rowCount, pageLinks: pageLinks.slice(0, 10), url: window.location.href };
  });

  console.log("\n=== PEOPLE / MEMBERSHIPS ===");
  console.log(JSON.stringify(peopleInfo, null, 2));

  await browser.close();
}

main().catch(console.error);
