/**
 * Debug script: Check what columns each report table has.
 */
import { chromium } from "playwright";
import { readFileSync, existsSync } from "fs";
import { join } from "path";

const BASE_URL = "https://www.union.fit";
const ORG_SLUG = "sky-ting";
const COOKIE_FILE = join(process.cwd(), "data", "union-cookies.json");

const REPORT_URLS: Record<string, string> = {
  newCustomers: "/report/customers/created_within",
  orders: "/reports/transactions?transaction_type=orders",
  firstVisits: "/report/registrations/first_visit",
  allRegistrations: "/report/registrations/remaining",
  canceledAutoRenews: "/report/subscriptions/growth?filter=cancelled",
  activeAutoRenews: "/report/subscriptions/list?status=active",
  newAutoRenews: "/report/subscriptions/growth?filter=new",
};

async function main() {
  const browser = await chromium.launch({
    channel: "chrome",
    headless: false,
    args: ["--disable-blink-features=AutomationControlled"],
  });

  const context = await browser.newContext({ viewport: { width: 1440, height: 900 } });

  if (existsSync(COOKIE_FILE)) {
    const cookies = JSON.parse(readFileSync(COOKIE_FILE, "utf8"));
    await context.addCookies(cookies);
  }

  const page = await context.newPage();

  for (const [name, path] of Object.entries(REPORT_URLS)) {
    const url = `${BASE_URL}/admin/orgs/${ORG_SLUG}${path}`;
    console.log(`\n=== ${name} ===`);
    await page.goto(url, { waitUntil: "domcontentloaded", timeout: 30000 });
    await page.waitForTimeout(4000);

    const info = await page.evaluate(() => {
      const table = document.querySelector("table");
      if (!table) return { headers: [], firstRow: [], rowCount: 0 };

      const headers: string[] = [];
      table.querySelectorAll("thead th, thead td").forEach(th => {
        headers.push(th.textContent?.trim() || "(empty)");
      });

      const rows = table.querySelectorAll("tbody tr");
      const firstRow: string[] = [];
      if (rows.length > 0) {
        rows[0].querySelectorAll("td").forEach(td => {
          const clone = td.cloneNode(true) as HTMLElement;
          clone.querySelectorAll(".dropdown, .dropdown-menu, .btn, button, .dropdown-toggle").forEach(el => el.remove());
          firstRow.push(clone.textContent?.trim().substring(0, 60) || "(empty)");
        });
      }

      return { headers, firstRow, rowCount: rows.length };
    });

    console.log(`  Headers: ${info.headers.join(" | ")}`);
    console.log(`  Row count: ${info.rowCount}`);
    console.log(`  First row: ${info.firstRow.join(" | ")}`);
  }

  await browser.close();
}

main().catch(console.error);
