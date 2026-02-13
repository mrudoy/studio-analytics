/**
 * Debug: Check the CSV export form on each report page — what URL + params does it submit?
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
      // Find the CSV button
      const csvBtn = document.querySelector('button[value="csv"]') as HTMLButtonElement | null;
      if (!csvBtn) return { hasCsvButton: false };

      const form = csvBtn.closest("form");
      if (!form) return { hasCsvButton: true, hasForm: false };

      // Build the full URL that would be submitted
      const formAction = form.action;
      const method = form.method;

      // Collect all form inputs
      const params: Record<string, string> = {};
      const inputs = form.querySelectorAll("input[name], select[name], button[name]");
      inputs.forEach((el) => {
        const inp = el as HTMLInputElement;
        if (inp.name) params[inp.name] = inp.value || "";
      });

      // The CSV button itself
      params["format"] = "csv";

      // Build the query string
      const qs = Object.entries(params)
        .filter(([, v]) => v)
        .map(([k, v]) => k + "=" + encodeURIComponent(v))
        .join("&");

      const fullUrl = formAction + (formAction.includes("?") ? "&" : "?") + qs;

      return {
        hasCsvButton: true,
        hasForm: true,
        formAction,
        method,
        params,
        fullUrl,
      };
    });

    console.log(JSON.stringify(info, null, 2));

    // If there's a CSV URL, try fetching it
    if (info && "fullUrl" in info && info.fullUrl) {
      console.log("  Attempting fetch...");
      const start = Date.now();
      const result = await page.evaluate(async (fetchUrl: string) => {
        try {
          const resp = await fetch(fetchUrl, {
            method: "GET",
            credentials: "include",
          });
          const text = await resp.text();
          const lines = text.split("\n");
          return {
            status: resp.status,
            contentType: resp.headers.get("content-type") || "",
            lineCount: lines.length,
            size: text.length,
            headers: lines[0]?.substring(0, 200) || "",
            isCSV: !text.includes("<html") && !text.includes("<!DOCTYPE"),
          };
        } catch (e) {
          return { error: String(e) };
        }
      }, info.fullUrl);
      const elapsed = Date.now() - start;
      console.log("  Time: " + elapsed + "ms");
      console.log("  Result: " + JSON.stringify(result, null, 2));
    }
  }

  await browser.close();
}

main().catch(console.error);
