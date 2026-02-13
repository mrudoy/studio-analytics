/**
 * Debug: Investigate CSV export — what network request does the button trigger?
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

  // Use canceled auto-renews as test (small dataset)
  const testUrl = BASE_URL + "/admin/orgs/" + ORG_SLUG + "/report/subscriptions/growth?filter=cancelled";
  console.log("Loading: " + testUrl);
  await page.goto(testUrl, { waitUntil: "domcontentloaded", timeout: 30000 });
  await page.waitForTimeout(4000);

  // 1. Find and examine the CSV button's parent form
  const formInfo = await page.evaluate(() => {
    const csvBtn = document.querySelector('button[value="csv"]') as HTMLButtonElement | null;
    if (!csvBtn) return { found: false, info: "no csv button" };

    const form = csvBtn.closest("form");
    if (!form) return { found: true, info: "csv button exists but no parent form" };

    const formData: Record<string, string> = {};
    const inputs = form.querySelectorAll("input, button, select");
    inputs.forEach((inp) => {
      const el = inp as HTMLInputElement;
      formData[el.name || el.tagName] = el.value || "";
    });

    return {
      found: true,
      action: form.action,
      method: form.method,
      formData,
      formHTML: form.outerHTML.substring(0, 1000),
    };
  });
  console.log("\n1. CSV button form info:");
  console.log(JSON.stringify(formInfo, null, 2));

  // 2. Intercept the request that happens when clicking CSV
  console.log("\n2. Intercepting request from CSV button click...");

  const capturedRequests: Array<{url: string, method: string, postData: string}> = [];
  page.on("request", (req) => {
    const url = req.url();
    if (url.includes("format") || url.includes("csv") || url.includes("export") || req.method() === "POST") {
      capturedRequests.push({
        url: url.substring(0, 300),
        method: req.method(),
        postData: (req.postData() || "").substring(0, 500),
      });
    }
  });

  const capturedResponses: Array<{url: string, status: number, contentType: string, location: string, bodySnippet: string}> = [];
  page.on("response", async (resp) => {
    const url = resp.url();
    if (url.includes("format") || url.includes("csv") || url.includes("export")) {
      let bodySnippet = "";
      try {
        const body = await resp.text();
        bodySnippet = body.substring(0, 300);
      } catch { /* */ }
      capturedResponses.push({
        url: url.substring(0, 300),
        status: resp.status(),
        contentType: resp.headers()["content-type"] || "",
        location: resp.headers()["location"] || "",
        bodySnippet,
      });
    }
  });

  // Open dropdown and click CSV
  const toggle = page.locator("button.dropdown-toggle-split").first();
  if (await toggle.isVisible().catch(() => false)) {
    await toggle.click();
    await page.waitForTimeout(500);
  }

  const csvBtn = page.locator('button[value="csv"]').first();
  if (await csvBtn.isVisible().catch(() => false)) {
    console.log("Clicking CSV button...");
    await csvBtn.click();
    await page.waitForTimeout(5000);
  } else {
    console.log("CSV button not visible, trying direct form approach...");
  }

  console.log("\nCaptured requests:");
  console.log(JSON.stringify(capturedRequests, null, 2));
  console.log("\nCaptured responses:");
  console.log(JSON.stringify(capturedResponses, null, 2));

  // 3. Try using route interception to prevent redirect and get CSV body
  console.log("\n3. Testing route interception for CSV...");

  await page.goto(testUrl, { waitUntil: "domcontentloaded", timeout: 30000 });
  await page.waitForTimeout(4000);

  // Try fetching with the session cookies, adding format=csv
  const csvUrl = testUrl + "&format=csv";
  console.log("Trying fetch() from page context: " + csvUrl);

  const fetchResult = await page.evaluate(async (url: string) => {
    try {
      const resp = await fetch(url, {
        method: "GET",
        credentials: "include",
        redirect: "follow",
      });
      const text = await resp.text();
      return {
        status: resp.status,
        contentType: resp.headers.get("content-type") || "",
        redirected: resp.redirected,
        url: resp.url.substring(0, 200),
        bodyLength: text.length,
        bodySnippet: text.substring(0, 500),
        isCSV: text.includes(",") && text.includes("\n") && !text.includes("<html"),
      };
    } catch (e) {
      return { error: String(e) };
    }
  }, csvUrl);

  console.log("Fetch result:");
  console.log(JSON.stringify(fetchResult, null, 2));

  // 4. Try POST with format=csv
  console.log("\n4. Testing POST with format=csv...");
  const postResult = await page.evaluate(async (url: string) => {
    try {
      const resp = await fetch(url, {
        method: "POST",
        credentials: "include",
        headers: { "Content-Type": "application/x-www-form-urlencoded" },
        body: "format=csv",
        redirect: "follow",
      });
      const text = await resp.text();
      return {
        status: resp.status,
        contentType: resp.headers.get("content-type") || "",
        bodyLength: text.length,
        bodySnippet: text.substring(0, 500),
        isCSV: text.includes(",") && text.includes("\n") && !text.includes("<html"),
      };
    } catch (e) {
      return { error: String(e) };
    }
  }, testUrl);

  console.log("POST result:");
  console.log(JSON.stringify(postResult, null, 2));

  // 5. Check if there's a JSON API
  console.log("\n5. Testing JSON API...");
  const jsonUrl = testUrl + "&format=json";
  const jsonResult = await page.evaluate(async (url: string) => {
    try {
      const resp = await fetch(url, {
        method: "GET",
        credentials: "include",
        headers: { "Accept": "application/json" },
      });
      const text = await resp.text();
      return {
        status: resp.status,
        contentType: resp.headers.get("content-type") || "",
        bodyLength: text.length,
        bodySnippet: text.substring(0, 500),
      };
    } catch (e) {
      return { error: String(e) };
    }
  }, jsonUrl);

  console.log("JSON result:");
  console.log(JSON.stringify(jsonResult, null, 2));

  // 6. Check Accept header approach
  console.log("\n6. Testing Accept: text/csv header...");
  const acceptResult = await page.evaluate(async (url: string) => {
    try {
      const resp = await fetch(url, {
        method: "GET",
        credentials: "include",
        headers: { "Accept": "text/csv" },
      });
      const text = await resp.text();
      return {
        status: resp.status,
        contentType: resp.headers.get("content-type") || "",
        bodyLength: text.length,
        bodySnippet: text.substring(0, 500),
        isCSV: text.includes(",") && text.includes("\n") && !text.includes("<html"),
      };
    } catch (e) {
      return { error: String(e) };
    }
  }, testUrl);

  console.log("Accept CSV result:");
  console.log(JSON.stringify(acceptResult, null, 2));

  await browser.close();
}

main().catch(console.error);
