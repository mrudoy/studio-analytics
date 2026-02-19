/**
 * Debug script: Check a single report page's HTML structure around the View/Download area.
 * Also takes a screenshot after clicking the dropdown toggle.
 */

import * as dotenv from "dotenv";
dotenv.config();

import { UnionClient } from "../src/lib/scraper/union-client";
import { loadSettings } from "../src/lib/crypto/credentials";
import { REPORT_URLS, type ReportType } from "../src/lib/scraper/selectors";
import { join } from "path";

const BASE_URL = "https://www.union.fit/admin/orgs/sky-ting";

async function main() {
  const reportType: ReportType = (process.argv[2] as ReportType) || "activeAutoRenews";

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

    const url = `${BASE_URL}${REPORT_URLS[reportType]}`;
    console.log(`\nNavigating to: ${url}`);
    await page.goto(url, { waitUntil: "domcontentloaded", timeout: 30000 });
    await page.waitForLoadState("networkidle", { timeout: 10000 }).catch(() => {});

    // Screenshot before click
    await page.screenshot({ path: join(process.cwd(), "data", `debug-${reportType}-before.png`) });

    // Dump the HTML of all btn-group and input-group elements
    const groups = await page.evaluate(() => {
      const els = [...document.querySelectorAll(".btn-group, .input-group")];
      return els.map((el, i) => ({
        index: i,
        classes: el.className,
        html: el.outerHTML.slice(0, 500),
        hasView: el.innerHTML.includes("View"),
        hasDownload: el.innerHTML.includes("Download"),
      }));
    });

    console.log(`\nbtn-group / input-group elements (${groups.length} total):`);
    for (const g of groups) {
      if (g.hasView || g.hasDownload) {
        console.log(`\n  [${g.index}] class="${g.classes}" hasView=${g.hasView} hasDownload=${g.hasDownload}`);
        console.log(`  HTML: ${g.html}`);
      }
    }

    // Try to find the dropdown toggle and click it
    const btnGroup = page.locator('.btn-group:has(button.form-control:has-text("View"))').first();
    const btnGroupVisible = await btnGroup.isVisible({ timeout: 2000 }).catch(() => false);
    console.log(`\nbtn-group with form-control View button visible: ${btnGroupVisible}`);

    if (!btnGroupVisible) {
      // Try alternate: input-group
      const inputGroup = page.locator('.input-group:has(button:has-text("View"))').first();
      const inputGroupVisible = await inputGroup.isVisible({ timeout: 2000 }).catch(() => false);
      console.log(`input-group with View button visible: ${inputGroupVisible}`);

      if (inputGroupVisible) {
        const toggleInInput = inputGroup.locator('.dropdown-toggle-split, .dropdown-toggle').first();
        const toggleVisible = await toggleInInput.isVisible({ timeout: 1000 }).catch(() => false);
        console.log(`  dropdown-toggle visible in input-group: ${toggleVisible}`);

        if (toggleVisible) {
          await toggleInInput.click();
          await page.waitForTimeout(500);
          await page.screenshot({ path: join(process.cwd(), "data", `debug-${reportType}-dropdown.png`) });
          console.log(`  Screenshot after dropdown click saved`);

          const downloadBtn = page.locator('button:has-text("Download CSV")').first();
          const downloadVisible = await downloadBtn.isVisible({ timeout: 2000 }).catch(() => false);
          console.log(`  Download CSV button visible: ${downloadVisible}`);
        }
      }
    } else {
      const toggle = btnGroup.locator('.dropdown-toggle-split, .dropdown-toggle').first();
      const toggleVisible = await toggle.isVisible({ timeout: 1000 }).catch(() => false);
      console.log(`  dropdown-toggle visible: ${toggleVisible}`);

      if (toggleVisible) {
        await toggle.click();
        await page.waitForTimeout(500);
        await page.screenshot({ path: join(process.cwd(), "data", `debug-${reportType}-dropdown.png`) });
        console.log(`  Screenshot after dropdown click saved`);

        const downloadBtn = btnGroup.locator('button:has-text("Download CSV")').first();
        const downloadVisible = await downloadBtn.isVisible({ timeout: 2000 }).catch(() => false);
        console.log(`  Download CSV button visible: ${downloadVisible}`);
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
