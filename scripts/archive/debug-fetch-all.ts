/**
 * Debug: Test fetch() CSV approach on all 7 reports.
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

  // First load the dashboard to establish session
  await page.goto(BASE_URL + "/admin/orgs/" + ORG_SLUG + "/dashboard", {
    waitUntil: "domcontentloaded",
    timeout: 30000,
  });
  await page.waitForTimeout(3000);

  for (const [name, path] of Object.entries(REPORT_URLS)) {
    const reportUrl = BASE_URL + "/admin/orgs/" + ORG_SLUG + path;
    const sep = path.includes("?") ? "&" : "?";
    const csvUrl = reportUrl + sep + "format=csv";

    console.log("\n=== " + name + " ===");
    console.log("URL: " + csvUrl);

    const start = Date.now();
    const result = await page.evaluate(async (url: string) => {
      try {
        const resp = await fetch(url, {
          method: "GET",
          credentials: "include",
          redirect: "follow",
        });
        const text = await resp.text();
        const lines = text.split("\n");
        const headers = lines[0] || "";
        const firstDataLine = lines.length > 1 ? lines[1] : "";
        return {
          status: resp.status,
          contentType: resp.headers.get("content-type") || "",
          bodyLength: text.length,
          lineCount: lines.length,
          headers: headers,
          firstDataLine: firstDataLine.substring(0, 200),
          isCSV: text.includes(",") && !text.includes("<html"),
        };
      } catch (e) {
        return { error: String(e) };
      }
    }, csvUrl);

    const elapsed = Date.now() - start;
    console.log("  Time: " + elapsed + "ms");

    if ("error" in result) {
      console.log("  ERROR: " + result.error);
    } else {
      console.log("  Status: " + result.status);
      console.log("  Content-Type: " + result.contentType);
      console.log("  Size: " + result.bodyLength + " bytes, " + result.lineCount + " lines");
      console.log("  Is CSV: " + result.isCSV);
      console.log("  Headers: " + result.headers);
      console.log("  First row: " + result.firstDataLine);
    }
  }

  await browser.close();
}

main().catch(console.error);
