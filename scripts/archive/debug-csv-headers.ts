/**
 * Quick check: what column headers does the direct CSV have?
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

  // Test canceled auto-renews (known to work)
  const testUrl = `${BASE_URL}/admin/orgs/${ORG_SLUG}/report/subscriptions/growth?filter=cancelled`;
  console.log("Loading:", testUrl);
  await page.goto(testUrl, { waitUntil: "domcontentloaded", timeout: 30000 });
  await page.waitForTimeout(4000);

  // Read form params
  const formUrl = await page.evaluate(() => {
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

  console.log("Fetch URL:", formUrl);

  const result = await page.evaluate(async (url: string) => {
    const resp = await fetch(url, { credentials: "include" });
    const text = await resp.text();
    const lines = text.split("\n");
    return {
      status: resp.status,
      contentType: resp.headers.get("content-type"),
      headerLine: lines[0],
      firstDataRow: lines[1] || "",
      secondDataRow: lines[2] || "",
      totalLines: lines.length,
    };
  }, formUrl);

  console.log("\n=== CANCELED AUTO-RENEWS CSV ===");
  console.log("Status:", result.status);
  console.log("Content-Type:", result.contentType);
  console.log("Total lines:", result.totalLines);
  console.log("HEADER:", result.headerLine);
  console.log("ROW 1:", result.firstDataRow);
  console.log("ROW 2:", result.secondDataRow);

  await browser.close();
}

main().catch(console.error);
