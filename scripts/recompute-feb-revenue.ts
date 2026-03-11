/**
 * Recompute Feb 2026 revenue by merging:
 *   1. The local historical export (through Feb 23/24)
 *   2. Daily exports from Union API covering Feb 24-28
 *
 * Downloads daily exports, extracts CSVs, merges with historical,
 * runs ZipTransformer.computeRevenueByCategory, and upserts the result.
 *
 * Usage:
 *   cd "/Users/mike.rudoy_old/Desktop/Claude App/studio-analytics"
 *   BATCH_IMPORT=1 npx tsx scripts/recompute-feb-revenue.ts
 */

import dotenv from "dotenv";
import { resolve, join } from "path";
import fs from "fs";
import { createDecipheriv } from "crypto";
import { execSync } from "child_process";

dotenv.config({ path: resolve(__dirname, "../.env.production.local") });
dotenv.config({ path: resolve(__dirname, "../.env.local") });
dotenv.config({ path: resolve(__dirname, "../.env") });

import { initDatabase, getPool } from "../src/lib/db/database";
import { runMigrations } from "../src/lib/db/migrations";
import { parseCSV } from "../src/lib/parser/csv-parser";
import { ZipTransformer } from "../src/lib/email/zip-transformer";
import { saveRevenueCategories } from "../src/lib/db/revenue-store";
import {
  RawMembershipSchema, RawPassSchema, RawEventSchema,
  RawPerformanceSchema, RawLocationSchema, RawPassTypeSchema,
  RawRevenueCategoryLookupSchema, RawOrderSchema, RawRefundSchema,
  RawTransferSchema,
  type RawOrder, type RawRefund, type RawTransfer,
} from "../src/lib/email/zip-schemas";

const UNION_API_URL = "https://www.union.fit/api/v1/data_exporters.json";
const WORK_DIR = "/tmp/feb-recompute";
const MERGED_DIR = join(WORK_DIR, "merged");
const PROJECT_ROOT = resolve(__dirname, "..");

const HISTORICAL_CSV_DIR = resolve(
  process.env.HOME || "~",
  "Downloads/union_data_export-sky-ting-20230830-20260223-zFoHME8GZ2ESfu8LzvDe27NU"
);

// ── Decrypt API key ──

function getKey(): Buffer {
  const key = process.env.ENCRYPTION_MASTER_KEY;
  if (!key) throw new Error("ENCRYPTION_MASTER_KEY not set");
  const keyBuffer = Buffer.from(key, "hex");
  const fullKey = Buffer.alloc(32);
  keyBuffer.copy(fullKey);
  return fullKey;
}

function getApiKey(): string {
  if (process.env.UNION_API_KEY) return process.env.UNION_API_KEY;
  const credFile = join(PROJECT_ROOT, "data", "credentials.enc");
  const blob = fs.readFileSync(credFile, "utf8").trim();
  const key = getKey();
  const parts = blob.split(":");
  const iv = Buffer.from(parts[0], "hex");
  const authTag = Buffer.from(parts[1], "hex");
  const decipher = createDecipheriv("aes-256-gcm", key, iv);
  decipher.setAuthTag(authTag);
  let decrypted = decipher.update(parts[2], "hex", "utf8");
  decrypted += decipher.final("utf8");
  const settings = JSON.parse(decrypted);
  if (!settings.unionApiKey) throw new Error("No unionApiKey in credentials");
  return settings.unionApiKey;
}

// ── Union API ──

interface DataExporter {
  org: string;
  created_at: string;
  data_updated_starts_at: string;
  data_updated_ends_at: string;
  download_url: string;
}

async function fetchExports(apiKey: string): Promise<DataExporter[]> {
  const resp = await fetch(UNION_API_URL, {
    headers: { Authorization: `Bearer ${apiKey}` },
  });
  if (!resp.ok) throw new Error(`API ${resp.status}: ${resp.statusText}`);
  const json = await resp.json();
  return json.data_exporters || [];
}

async function downloadZip(url: string, dest: string): Promise<void> {
  console.log(`  Downloading ${url.slice(0, 80)}...`);
  const resp = await fetch(url);
  if (!resp.ok) throw new Error(`Download failed: ${resp.status}`);
  const buf = Buffer.from(await resp.arrayBuffer());
  fs.writeFileSync(dest, buf);
  console.log(`  Saved ${(buf.length / 1024 / 1024).toFixed(1)}MB → ${dest}`);
}

// ── CSV merge helpers ──

// Lookup tables that are COMPLETE in the historical export.
// Daily exports have header-only versions of these.
// We just copy the historical version directly to avoid multiline CSV corruption.
const LOOKUP_FILES = ["pass_types", "revenue_categories", "events", "locations"];

// Transaction tables where daily exports ADD new records.
const TRANSACTION_FILES = ["memberships", "passes", "orders", "refunds", "transfers", "performances"];

/**
 * Parse CSV properly, handling quoted multiline fields.
 * Returns [header, Map<id, fullRow>] where fullRow is the complete CSV row
 * potentially spanning multiple lines.
 */
function parseCSVRows(content: string): [string, Map<string, string>] {
  const rows = new Map<string, string>();
  let header = "";
  let currentRow = "";
  let inQuotes = false;
  let isHeader = true;

  for (let i = 0; i < content.length; i++) {
    const ch = content[i];
    if (ch === '"') {
      inQuotes = !inQuotes;
      currentRow += ch;
    } else if (ch === '\n' && !inQuotes) {
      const trimmed = currentRow.trim();
      if (isHeader) {
        header = trimmed;
        isHeader = false;
      } else if (trimmed) {
        const commaIdx = trimmed.indexOf(",");
        const id = commaIdx > 0 ? trimmed.slice(0, commaIdx) : trimmed;
        rows.set(id, trimmed);
      }
      currentRow = "";
    } else {
      currentRow += ch;
    }
  }
  // Handle last row without trailing newline
  const trimmed = currentRow.trim();
  if (trimmed && !isHeader) {
    const commaIdx = trimmed.indexOf(",");
    const id = commaIdx > 0 ? trimmed.slice(0, commaIdx) : trimmed;
    rows.set(id, trimmed);
  }

  return [header, rows];
}

function mergeCSVFiles(historicalDir: string, dailyDirs: string[], outDir: string): void {
  fs.mkdirSync(outDir, { recursive: true });

  // For lookup files, just copy from historical (they're complete there)
  for (const name of LOOKUP_FILES) {
    const src = join(historicalDir, `${name}.csv`);
    const dest = join(outDir, `${name}.csv`);
    if (fs.existsSync(src)) {
      fs.copyFileSync(src, dest);
      const content = fs.readFileSync(src, "utf8");
      const [, rows] = parseCSVRows(content);
      console.log(`  ${name}: ${rows.size} rows (from historical, lookup-only)`);
    }
  }

  // For transaction files, merge all sources with proper dedup
  const allDirs = [historicalDir, ...dailyDirs];
  for (const name of TRANSACTION_FILES) {
    const fileName = `${name}.csv`;
    let header = "";
    const rowById = new Map<string, string>();

    for (const dir of allDirs) {
      const filePath = join(dir, fileName);
      if (!fs.existsSync(filePath)) continue;
      const content = fs.readFileSync(filePath, "utf8");
      const [hdr, rows] = parseCSVRows(content);
      if (!header && hdr) header = hdr;
      // Merge — later dirs override earlier (daily export updates win)
      for (const [id, row] of rows) {
        rowById.set(id, row);
      }
    }

    if (header) {
      const rows = Array.from(rowById.values());
      fs.writeFileSync(join(outDir, fileName), header + "\n" + rows.join("\n") + "\n");
      console.log(`  ${name}: ${rows.length} unique rows (merged + deduped)`);
    }
  }
}

// ── Main ──

function parse<T>(dir: string, name: string, schema: { parse: (v: unknown) => T }): T[] {
  const path = join(dir, `${name}.csv`);
  if (!fs.existsSync(path)) {
    console.log(`  ${name}: file not found, using empty`);
    return [];
  }
  const result = parseCSV<T>(path, schema as never);
  console.log(`  ${name}: ${result.data.length} rows`);
  return result.data;
}

async function main() {
  console.log("\n=== Recompute Feb 2026 Revenue (Historical + Daily Exports) ===\n");

  // 1. Check historical export exists
  if (!fs.existsSync(HISTORICAL_CSV_DIR)) {
    console.error(`Historical CSV directory not found: ${HISTORICAL_CSV_DIR}`);
    process.exit(1);
  }
  console.log(`Historical CSVs: ${HISTORICAL_CSV_DIR}`);

  // 2. Fetch exports from Union API
  const apiKey = getApiKey();
  console.log(`API key loaded.\n`);

  console.log("Fetching available exports from Union API...");
  const allExports = await fetchExports(apiKey);
  console.log(`Found ${allExports.length} exports\n`);

  // 3. Find exports that cover Feb 24-28
  const FEB24 = new Date("2026-02-24T00:00:00Z");
  const MAR01 = new Date("2026-03-01T23:59:59Z");

  const febExports = allExports.filter((exp) => {
    const start = new Date(exp.data_updated_starts_at);
    const end = new Date(exp.data_updated_ends_at);
    return start <= MAR01 && end >= FEB24;
  });

  console.log(`${febExports.length} exports cover Feb 24 - Mar 1:`);
  for (const exp of febExports) {
    console.log(`  ${exp.data_updated_starts_at} to ${exp.data_updated_ends_at} (created ${exp.created_at})`);
  }
  console.log();

  if (febExports.length === 0) {
    console.error("No exports cover Feb 24-28. They may have expired.");
    process.exit(1);
  }

  // 4. Download and extract daily exports
  fs.mkdirSync(WORK_DIR, { recursive: true });

  const dailyDirs: string[] = [];

  for (let i = 0; i < febExports.length; i++) {
    const exp = febExports[i];
    // Skip the historical export — we already have it locally
    const rangeStart = new Date(exp.data_updated_starts_at);
    if (rangeStart.getFullYear() < 2026) {
      console.log(`\nSkipping historical export (already have it locally)`);
      continue;
    }

    const zipPath = join(WORK_DIR, `export-${i}.zip`);
    const extractDir = join(WORK_DIR, `export-${i}`);

    console.log(`\nDownloading export ${i + 1}/${febExports.length}...`);
    console.log(`  Range: ${exp.data_updated_starts_at} to ${exp.data_updated_ends_at}`);
    await downloadZip(exp.download_url, zipPath);

    // Extract
    fs.mkdirSync(extractDir, { recursive: true });
    execSync(`unzip -o -q "${zipPath}" -d "${extractDir}"`);

    // Find the CSV directory (might be nested)
    const subdirs = fs.readdirSync(extractDir);
    let csvDir = extractDir;
    for (const sub of subdirs) {
      const subPath = join(extractDir, sub);
      if (fs.statSync(subPath).isDirectory() && fs.existsSync(join(subPath, "orders.csv"))) {
        csvDir = subPath;
        break;
      }
    }

    if (fs.existsSync(join(csvDir, "orders.csv"))) {
      dailyDirs.push(csvDir);
      console.log(`  Extracted to ${csvDir}`);
    } else {
      console.log(`  WARN: No orders.csv found in ${csvDir}`);
    }
  }

  // 5. Merge historical + daily exports
  // Lookup tables (pass_types, events, etc.) come from historical only.
  // Transaction tables (orders, passes, etc.) are merged across all.
  console.log(`\nMerging CSVs: 1 historical + ${dailyDirs.length} daily exports...`);
  mergeCSVFiles(HISTORICAL_CSV_DIR, dailyDirs, MERGED_DIR);

  // 6. Parse merged CSVs
  console.log("\nParsing merged CSVs...");
  const memberships = parse(MERGED_DIR, "memberships", RawMembershipSchema);
  const passes = parse(MERGED_DIR, "passes", RawPassSchema);
  const events = parse(MERGED_DIR, "events", RawEventSchema);
  const performances = parse(MERGED_DIR, "performances", RawPerformanceSchema);
  const locations = parse(MERGED_DIR, "locations", RawLocationSchema);
  const passTypes = parse(MERGED_DIR, "pass_types", RawPassTypeSchema);
  const revenueCategoryLookups = parse(MERGED_DIR, "revenue_categories", RawRevenueCategoryLookupSchema);
  const orders = parse(MERGED_DIR, "orders", RawOrderSchema);
  const refunds = parse(MERGED_DIR, "refunds", RawRefundSchema);
  const transfers = parse(MERGED_DIR, "transfers", RawTransferSchema);

  // 7. Build transformer and compute revenue
  console.log("\nBuilding transformer...");
  const transformer = new ZipTransformer({
    memberships, passes, events, performances, locations, passTypes, revenueCategoryLookups,
  });

  console.log("Computing revenue by category...");
  const monthlyRevenue = transformer.computeRevenueByCategory(
    orders as RawOrder[],
    refunds as RawRefund[],
    transfers as RawTransfer[],
  );

  console.log(`Found revenue for ${monthlyRevenue.size} months`);

  // 8. Save Feb 2026 only
  const FEB_KEY = "2026-02";
  const febCategories = monthlyRevenue.get(FEB_KEY);
  if (!febCategories) {
    console.error(`No revenue data found for ${FEB_KEY}`);
    process.exit(1);
  }

  const totalGross = febCategories.reduce((s, c) => s + (c.revenue ?? 0), 0);
  const totalNet = febCategories.reduce((s, c) => s + (c.netRevenue ?? 0), 0);
  console.log(`\nFeb 2026: ${febCategories.length} categories`);
  console.log(`  Gross: $${totalGross.toFixed(0)}`);
  console.log(`  Net:   $${totalNet.toFixed(0)}`);

  // Show top categories
  const sorted = [...febCategories].sort((a, b) => (b.revenue ?? 0) - (a.revenue ?? 0));
  console.log(`\nTop 10 categories:`);
  for (const cat of sorted.slice(0, 10)) {
    console.log(`  ${cat.revenueCategory}: $${(cat.revenue ?? 0).toFixed(0)}`);
  }

  // 9. Init DB and save
  await initDatabase();
  await runMigrations();

  console.log(`\nSaving Feb 2026 revenue (${febCategories.length} categories)...`);
  await saveRevenueCategories("2026-02-01", "2026-02-28", febCategories);

  // 10. Bump cache version
  const pool = getPool();
  await pool.query(
    "UPDATE data_version SET version = version + 1, updated_at = NOW() WHERE id = 1"
  );

  // 11. Verify
  const check = await pool.query(`
    WITH deduped AS (
      SELECT DISTINCT ON (category)
        category, revenue, net_revenue
      FROM revenue_categories
      WHERE TO_CHAR(period_start, 'YYYY-MM') = '2026-02'
        AND TO_CHAR(period_end, 'YYYY-MM') = '2026-02'
      ORDER BY category, period_end DESC
    )
    SELECT COUNT(*) as cnt, SUM(revenue)::numeric as gross, SUM(net_revenue)::numeric as net
    FROM deduped
  `);
  const dd = check.rows[0];
  console.log(`\nDB verification (deduped): ${dd.cnt} categories, gross=$${Number(dd.gross).toFixed(0)}, net=$${Number(dd.net).toFixed(0)}`);

  // Retreat deduction
  const retreat = await pool.query(`
    WITH deduped AS (
      SELECT DISTINCT ON (category)
        category, revenue, net_revenue
      FROM revenue_categories
      WHERE TO_CHAR(period_start, 'YYYY-MM') = '2026-02'
        AND TO_CHAR(period_end, 'YYYY-MM') = '2026-02'
      ORDER BY category, period_end DESC
    )
    SELECT SUM(revenue)::numeric as gross
    FROM deduped
    WHERE category ~* 'retreat' AND NOT category ~* 'retreat\\s*ting'
  `);
  const retreatGross = Number(retreat.rows[0]?.gross || 0);
  console.log(`Retreat gross: $${retreatGross.toFixed(0)}`);
  console.log(`Dashboard total (gross - retreat): $${(Number(dd.gross) - retreatGross).toFixed(0)}`);

  console.log("\nDone!");
  process.exit(0);
}

main().catch((err) => {
  console.error("Failed:", err);
  process.exit(1);
});
