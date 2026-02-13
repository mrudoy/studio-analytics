/**
 * Debug: Check what the HTML response contains when format=csv returns HTML
 * (is it queuing an email export? showing an error? something else?)
 * Also test if subscription/growth endpoint works with format=csv for active/new
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

  // Test 1: Check what the newCustomers HTML response contains when given format=csv
  console.log("=== Test 1: newCustomers with format=csv (check HTML content) ===");
  const html1 = await page.evaluate(async () => {
    const resp = await fetch(
      "/admin/orgs/sky-ting/report/customers/created_within?period=week&daterange=01%2F12%2F2026%20-%2002%2F12%2F2026&format=csv",
      { credentials: "include" }
    );
    return await resp.text();
  });
  // Look for flash/alert messages
  const flashMatch = html1.match(/class="alert[^"]*"[^>]*>([\s\S]*?)<\/div>/);
  if (flashMatch) console.log("Flash message: " + flashMatch[1].trim());
  const emailMatch = html1.match(/email|emailed|export|queued/gi);
  if (emailMatch) console.log("Email-related keywords found: " + [...new Set(emailMatch)].join(", "));
  // Check if it's just the normal HTML page
  const titleMatch = html1.match(/<title>(.*?)<\/title>/);
  if (titleMatch) console.log("Page title: " + titleMatch[1]);

  // Test 2: Try the /subscriptions/growth endpoint with filter=active
  // (the canceled one uses /growth, active uses /list — maybe /growth supports CSV for all filters?)
  console.log("\n=== Test 2: subscriptions/growth?filter=active (not /list) ===");
  const result2 = await page.evaluate(async () => {
    const resp = await fetch(
      "/admin/orgs/sky-ting/report/subscriptions/growth?filter=active&period=week&daterange=08%2F31%2F2023%20-%2002%2F12%2F2026&format=csv",
      { credentials: "include" }
    );
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
  });
  console.log(JSON.stringify(result2, null, 2));

  // Test 3: Try newAutoRenews with a smaller date range (maybe 503 was from too much data)
  console.log("\n=== Test 3: newAutoRenews with small date range ===");
  const result3 = await page.evaluate(async () => {
    const resp = await fetch(
      "/admin/orgs/sky-ting/report/subscriptions/growth?filter=new&period=week&daterange=01%2F12%2F2026%20-%2002%2F12%2F2026&format=csv",
      { credentials: "include" }
    );
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
  });
  console.log(JSON.stringify(result3, null, 2));

  // Test 4: Try /memberships endpoint for customer data
  console.log("\n=== Test 4: /memberships with format=csv ===");
  const result4 = await page.evaluate(async () => {
    const resp = await fetch(
      "/admin/orgs/sky-ting/memberships?format=csv",
      { credentials: "include" }
    );
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
  });
  console.log(JSON.stringify(result4, null, 2));

  // Test 5: Try /customers endpoint
  console.log("\n=== Test 5: /customers with format=csv ===");
  const result5 = await page.evaluate(async () => {
    const resp = await fetch(
      "/admin/orgs/sky-ting/customers?format=csv",
      { credentials: "include" }
    );
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
  });
  console.log(JSON.stringify(result5, null, 2));

  // Test 6: Try registrations with format=csv on different sub-paths
  console.log("\n=== Test 6: registrations endpoints with format=csv ===");
  const regPaths = [
    "/report/registrations?format=csv",
    "/report/registrations/first_visit?daterange=01%2F12%2F2026%20-%2002%2F12%2F2026&format=csv",
    "/report/registrations/remaining?format=csv",
    "/registrations?format=csv",
  ];
  for (const rp of regPaths) {
    const result = await page.evaluate(async (path: string) => {
      const resp = await fetch("/admin/orgs/sky-ting" + path, { credentials: "include" });
      return {
        path,
        status: resp.status,
        contentType: resp.headers.get("content-type") || "",
        isCSV: !(await resp.text()).includes("<html"),
      };
    }, rp);
    console.log("  " + result.path + " → " + result.status + " " + result.contentType + " isCSV=" + result.isCSV);
  }

  await browser.close();
}

main().catch(console.error);
