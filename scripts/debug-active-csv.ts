/**
 * Debug: Test if subscriptions/growth?filter=active works with smaller date ranges.
 * Also test optimal date range size for the growth endpoint.
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

  // Load dashboard first
  await page.goto(BASE_URL + "/admin/orgs/" + ORG_SLUG + "/dashboard", {
    waitUntil: "domcontentloaded",
    timeout: 30000,
  });
  await page.waitForTimeout(3000);

  // Test 1: Active with 1-month range
  console.log("=== Test 1: growth?filter=active, 1-month range ===");
  const r1 = await page.evaluate(async () => {
    const start = Date.now();
    const resp = await fetch(
      "/admin/orgs/sky-ting/report/subscriptions/growth?filter=active&period=week&daterange=01%2F12%2F2026%20-%2002%2F12%2F2026&format=csv",
      { credentials: "include" }
    );
    const text = await resp.text();
    return {
      elapsed: Date.now() - start,
      status: resp.status,
      contentType: resp.headers.get("content-type") || "",
      lineCount: text.split("\n").length,
      size: text.length,
      isCSV: !text.includes("<html"),
      headers: text.split("\n")[0]?.substring(0, 200) || "",
    };
  });
  console.log(JSON.stringify(r1, null, 2));

  // Test 2: Active with 3-month range
  console.log("\n=== Test 2: growth?filter=active, 3-month range ===");
  const r2 = await page.evaluate(async () => {
    const start = Date.now();
    const resp = await fetch(
      "/admin/orgs/sky-ting/report/subscriptions/growth?filter=active&period=week&daterange=11%2F12%2F2025%20-%2002%2F12%2F2026&format=csv",
      { credentials: "include" }
    );
    const text = await resp.text();
    return {
      elapsed: Date.now() - start,
      status: resp.status,
      contentType: resp.headers.get("content-type") || "",
      lineCount: text.split("\n").length,
      size: text.length,
      isCSV: !text.includes("<html"),
    };
  });
  console.log(JSON.stringify(r2, null, 2));

  // Test 3: Active with 6-month range
  console.log("\n=== Test 3: growth?filter=active, 6-month range ===");
  const r3 = await page.evaluate(async () => {
    const start = Date.now();
    const resp = await fetch(
      "/admin/orgs/sky-ting/report/subscriptions/growth?filter=active&period=week&daterange=08%2F12%2F2025%20-%2002%2F12%2F2026&format=csv",
      { credentials: "include" }
    );
    const text = await resp.text();
    return {
      elapsed: Date.now() - start,
      status: resp.status,
      contentType: resp.headers.get("content-type") || "",
      lineCount: text.split("\n").length,
      size: text.length,
      isCSV: !text.includes("<html"),
    };
  });
  console.log(JSON.stringify(r3, null, 2));

  // Test 4: Active with 1-year range
  console.log("\n=== Test 4: growth?filter=active, 1-year range ===");
  const r4 = await page.evaluate(async () => {
    const start = Date.now();
    const resp = await fetch(
      "/admin/orgs/sky-ting/report/subscriptions/growth?filter=active&period=week&daterange=02%2F12%2F2025%20-%2002%2F12%2F2026&format=csv",
      { credentials: "include" }
    );
    const text = await resp.text();
    return {
      elapsed: Date.now() - start,
      status: resp.status,
      contentType: resp.headers.get("content-type") || "",
      lineCount: text.split("\n").length,
      size: text.length,
      isCSV: !text.includes("<html"),
    };
  });
  console.log(JSON.stringify(r4, null, 2));

  // Test 5: What does /subscriptions/list?status=active return?
  // Maybe it has a different CSV mechanism
  console.log("\n=== Test 5: subscriptions/list?status=active (different endpoint) ===");
  const r5 = await page.evaluate(async () => {
    const start = Date.now();
    const resp = await fetch(
      "/admin/orgs/sky-ting/report/subscriptions/list?status=active&format=csv",
      { credentials: "include" }
    );
    const text = await resp.text();
    // Check for flash message about email
    const hasEmailMsg = text.includes("emailed") || text.includes("export being created");
    return {
      elapsed: Date.now() - start,
      status: resp.status,
      contentType: resp.headers.get("content-type") || "",
      lineCount: text.split("\n").length,
      size: text.length,
      isCSV: !text.includes("<html"),
      hasEmailMsg,
    };
  });
  console.log(JSON.stringify(r5, null, 2));

  // Test 6: Canceled with smaller date range (to benchmark speed vs full range)
  console.log("\n=== Test 6: Canceled auto-renews, 1-month range (speed benchmark) ===");
  const r6 = await page.evaluate(async () => {
    const start = Date.now();
    const resp = await fetch(
      "/admin/orgs/sky-ting/report/subscriptions/growth?filter=cancelled&period=week&daterange=01%2F12%2F2026%20-%2002%2F12%2F2026&format=csv",
      { credentials: "include" }
    );
    const text = await resp.text();
    return {
      elapsed: Date.now() - start,
      status: resp.status,
      contentType: resp.headers.get("content-type") || "",
      lineCount: text.split("\n").length,
      size: text.length,
      isCSV: !text.includes("<html"),
    };
  });
  console.log(JSON.stringify(r6, null, 2));

  await browser.close();
}

main().catch(console.error);
