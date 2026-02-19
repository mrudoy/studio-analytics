/**
 * Debug: Check exact header/cell counts per report table.
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
    args: ["--disable-blink-features=AutomationControlled", "--window-position=-2400,-2400"],
  });

  const context = await browser.newContext({ viewport: { width: 1440, height: 900 } });

  if (existsSync(COOKIE_FILE)) {
    const cookies = JSON.parse(readFileSync(COOKIE_FILE, "utf8"));
    await context.addCookies(cookies);
  }

  const page = await context.newPage();

  for (const [name, path] of Object.entries(REPORT_URLS)) {
    const url = BASE_URL + "/admin/orgs/" + ORG_SLUG + path;
    console.log("\n=== " + name + " ===");
    await page.goto(url, { waitUntil: "domcontentloaded", timeout: 30000 });
    await page.waitForTimeout(4000);

    const info = await page.evaluate(() => {
      const table = document.querySelector("table");
      if (!table) return { theadCount: 0, theadTexts: [] as string[], rowCount: 0, rowTexts: [] as string[] };

      const ths = table.querySelectorAll("thead th, thead td");
      const theadTexts: string[] = [];
      ths.forEach(th => {
        const text = (th.textContent || "").trim();
        theadTexts.push(text || "(empty)");
      });

      const rows = table.querySelectorAll("tbody tr");
      const rowTexts: string[] = [];
      if (rows.length > 0) {
        const tds = rows[0].querySelectorAll("td");
        tds.forEach(td => {
          const clone = td.cloneNode(true) as HTMLElement;
          clone.querySelectorAll(".dropdown, .dropdown-menu, .btn, button, .dropdown-toggle").forEach(el => el.remove());
          const text = (clone.textContent || "").trim().substring(0, 40);
          rowTexts.push(text || "(empty)");
        });
        return { theadCount: ths.length, theadTexts, rowCount: tds.length, rowTexts };
      }

      return { theadCount: ths.length, theadTexts, rowCount: 0, rowTexts };
    });

    console.log("  Header cells: " + info.theadCount + " → [" + info.theadTexts.join(", ") + "]");
    console.log("  Data cells:   " + info.rowCount + " → [" + info.rowTexts.join(", ") + "]");
    if (info.theadCount !== info.rowCount) {
      console.log("  ⚠️  MISMATCH: " + info.theadCount + " headers vs " + info.rowCount + " data cells!");
    }
  }

  await browser.close();
}

main().catch(console.error);
