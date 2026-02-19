/**
 * Debug script: Navigate to a few Union.fit report pages and screenshot them
 * to see what the "Download CSV" UI actually looks like.
 */

import * as dotenv from "dotenv";
dotenv.config();

import { UnionClient } from "../src/lib/scraper/union-client";
import { loadSettings } from "../src/lib/crypto/credentials";
import { REPORT_URLS, type ReportType } from "../src/lib/scraper/selectors";
import { join } from "path";

const BASE_URL = "https://www.union.fit/admin/orgs/sky-ting";

async function main() {
  const settings = loadSettings();
  if (!settings?.credentials) {
    console.error("No credentials");
    process.exit(1);
  }

  const client = new UnionClient({
    onProgress: (step) => console.log(`  ${step}`),
  });

  try {
    await client.initialize();
    await client.login(settings.credentials.email, settings.credentials.password);

    const page = client.getPage();
    if (!page) throw new Error("No page");

    // Check a few representative pages
    const pagesToCheck: ReportType[] = ["canceledAutoRenews", "newCustomers", "orders"];

    for (const reportType of pagesToCheck) {
      const url = `${BASE_URL}${REPORT_URLS[reportType]}`;
      console.log(`\n=== ${reportType} ===`);
      console.log(`URL: ${url}`);

      await page.goto(url, { waitUntil: "domcontentloaded", timeout: 30000 });
      await page.waitForLoadState("networkidle", { timeout: 10000 }).catch(() => {});

      // Screenshot
      const screenshotPath = join(process.cwd(), "data", `debug-${reportType}.png`);
      await page.screenshot({ path: screenshotPath, fullPage: false });
      console.log(`Screenshot: ${screenshotPath}`);

      // Dump all buttons and links on the page
      const buttons = await page.evaluate(() => {
        const els = [...document.querySelectorAll("button, a, input[type=submit]")];
        return els
          .filter((el) => {
            const text = (el as HTMLElement).innerText?.trim();
            const value = (el as HTMLInputElement).value;
            return text || value;
          })
          .map((el) => ({
            tag: el.tagName,
            text: (el as HTMLElement).innerText?.trim().slice(0, 80),
            value: (el as HTMLInputElement).value,
            href: (el as HTMLAnchorElement).href,
            classes: el.className?.slice(0, 80),
          }))
          .slice(0, 50); // Limit output
      });

      console.log(`\nButtons/links on page (${buttons.length}):`);
      for (const btn of buttons) {
        const parts = [btn.tag];
        if (btn.text) parts.push(`text="${btn.text}"`);
        if (btn.value) parts.push(`value="${btn.value}"`);
        if (btn.href && !btn.href.startsWith("javascript:")) parts.push(`href="${btn.href}"`);
        console.log(`  ${parts.join(" | ")}`);
      }

      // Also check for dropdown menus, specifically "View" or "Download"
      const viewElements = await page.evaluate(() => {
        const all = [...document.querySelectorAll("*")];
        return all
          .filter((el) => {
            const text = (el as HTMLElement).innerText?.trim().toLowerCase() || "";
            return (
              (text.includes("view") || text.includes("download") || text.includes("csv") || text.includes("export")) &&
              text.length < 50
            );
          })
          .map((el) => ({
            tag: el.tagName,
            text: (el as HTMLElement).innerText?.trim().slice(0, 80),
            classes: el.className?.slice(0, 100),
            id: el.id,
          }))
          .slice(0, 20);
      });

      console.log(`\nElements with view/download/csv/export text:`);
      for (const el of viewElements) {
        console.log(`  ${el.tag} | text="${el.text}" | class="${el.classes}" | id="${el.id}"`);
      }
    }
  } finally {
    await client.cleanup();
  }
}

main().catch((err) => {
  console.error("Fatal:", err.message);
  process.exit(1);
});
