/**
 * Debug: Check if first visit attendee names match new customer names
 */
import { chromium } from "playwright";
import { readFileSync, existsSync, writeFileSync } from "fs";
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

  // Load the First Visits report page
  const url = BASE_URL + "/admin/orgs/" + ORG_SLUG + "/report/registrations/first_visit";
  console.log("Loading:", url);
  await page.goto(url, { waitUntil: "domcontentloaded", timeout: 60000 });
  await page.waitForTimeout(5000);

  const info = await page.evaluate(() => {
    const table = document.querySelector("table");
    if (!table) return { error: "No table found", url: window.location.href };

    const headers: string[] = [];
    table.querySelectorAll("thead th, thead td").forEach(th => headers.push(th.textContent?.trim() || ""));

    const rows: string[][] = [];
    table.querySelectorAll("tbody tr").forEach(tr => {
      const cells: string[] = [];
      tr.querySelectorAll("td").forEach(td => cells.push(td.textContent?.trim().substring(0, 80) || ""));
      rows.push(cells);
    });

    return { headers, sampleRows: rows.slice(0, 10), totalRows: rows.length };
  });

  console.log("\n=== FIRST VISITS REPORT ===");
  console.log("Headers:", JSON.stringify(info));

  await browser.close();
}

main().catch(console.error);
