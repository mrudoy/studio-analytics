/**
 * Seed the pass_type_lookups and revenue_category_lookups tables
 * from the full Union.fit data export.
 *
 * Usage:
 *   npx tsx scripts/seed-lookup-cache.ts [path-to-export-dir]
 *
 * Defaults to ~/Downloads/union_data_export-sky-ting-20230830-20260223-zFoHME8GZ2ESfu8LzvDe27NU
 */

import { config } from "dotenv";
import { resolve, join } from "path";
import { readFileSync } from "fs";

// Load env for DATABASE_URL
config({ path: resolve(process.cwd(), ".env.production.local") });
config({ path: resolve(process.cwd(), ".env.local") });

import { getPool } from "../src/lib/db/database";
import {
  ensureLookupTables,
  savePassTypeLookups,
  saveRevenueCategoryLookups,
} from "../src/lib/db/lookup-store";
import { runMigrations } from "../src/lib/db/migrations";

function parseCSVLine(line: string): string[] {
  const fields: string[] = [];
  let current = "";
  let inQuotes = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (inQuotes) {
      if (ch === '"' && line[i + 1] === '"') {
        current += '"';
        i++;
      } else if (ch === '"') {
        inQuotes = false;
      } else {
        current += ch;
      }
    } else {
      if (ch === '"') {
        inQuotes = true;
      } else if (ch === ",") {
        fields.push(current);
        current = "";
      } else {
        current += ch;
      }
    }
  }
  fields.push(current);
  return fields;
}

function parseCSV(filePath: string): Record<string, string>[] {
  const content = readFileSync(filePath, "utf-8");
  const lines = content.split("\n").filter((l) => l.trim());
  if (lines.length < 2) return [];
  const headers = parseCSVLine(lines[0]);
  return lines.slice(1).map((line) => {
    const values = parseCSVLine(line);
    const obj: Record<string, string> = {};
    headers.forEach((h, i) => {
      obj[h] = values[i] ?? "";
    });
    return obj;
  });
}

async function main() {
  const exportDir =
    process.argv[2] ||
    resolve(
      process.env.HOME || "~",
      "Downloads/union_data_export-sky-ting-20230830-20260223-zFoHME8GZ2ESfu8LzvDe27NU"
    );

  console.log(`Export dir: ${exportDir}`);

  // Run migrations first
  await runMigrations();
  await ensureLookupTables();

  // Parse pass_types.csv
  const passTypesPath = join(exportDir, "pass_types.csv");
  const passTypeRows = parseCSV(passTypesPath);
  console.log(`Parsed ${passTypeRows.length} pass types`);

  const ptEntries = passTypeRows.map((r) => ({
    id: r.id,
    name: r.name || null,
    revenueCategoryId: r.revenue_category_id || null,
    feesOutside: r.fees_outside === "true",
    createdAt: r.created_at || null,
  }));

  const ptCount = await savePassTypeLookups(ptEntries);
  console.log(`Saved ${ptCount} pass type lookups`);

  // Parse revenue_categories.csv
  const revCatsPath = join(exportDir, "revenue_categories.csv");
  const revCatRows = parseCSV(revCatsPath);
  console.log(`Parsed ${revCatRows.length} revenue categories`);

  const rcEntries = revCatRows.map((r) => ({
    id: r.id,
    name: r.name || "",
  }));

  const rcCount = await saveRevenueCategoryLookups(rcEntries);
  console.log(`Saved ${rcCount} revenue category lookups`);

  // Summary
  console.log("\nLookup cache seeded:");
  console.log(`  Pass types: ${ptCount}`);
  console.log(`  Revenue categories: ${rcCount}`);

  // Show category names
  console.log("\nRevenue categories:");
  for (const rc of rcEntries) {
    console.log(`  ${rc.id.slice(0, 8)}... → ${rc.name}`);
  }

  const pool = getPool();
  await pool.end();
}

main().catch((err) => {
  console.error("Failed:", err);
  process.exit(1);
});
