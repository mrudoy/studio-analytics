/**
 * Test the zip pipeline with local CSV files (no Gmail, no download).
 *
 * Usage:
 *   npx tsx scripts/test-zip-pipeline.ts [csv-directory]
 *
 * If no directory is specified, uses the default location:
 *   ~/Downloads/union_data_export-sky-ting-20230830-20260223-zFoHME8GZ2ESfu8LzvDe27NU/
 *
 * Requires DATABASE_URL to be set (uses .env.production.local or .env.local).
 */

import dotenv from "dotenv";
import { resolve } from "path";

// Load env files in priority order (same as Next.js)
dotenv.config({ path: resolve(__dirname, "../.env.production.local") });
dotenv.config({ path: resolve(__dirname, "../.env.local") });
dotenv.config({ path: resolve(__dirname, "../.env") });
import { existsSync } from "fs";
import { runZipLocalPipeline } from "../src/lib/email/zip-download-pipeline";
import { initDatabase } from "../src/lib/db/database";
import { runMigrations } from "../src/lib/db/migrations";

const DEFAULT_CSV_DIR = resolve(
  process.env.HOME || "~",
  "Downloads/union_data_export-sky-ting-20230830-20260223-zFoHME8GZ2ESfu8LzvDe27NU"
);

async function main() {
  const csvDir = process.argv[2] || DEFAULT_CSV_DIR;

  if (!existsSync(csvDir)) {
    console.error(`CSV directory not found: ${csvDir}`);
    console.error("Usage: npx tsx scripts/test-zip-pipeline.ts [csv-directory]");
    process.exit(1);
  }

  console.log(`\n=== Zip Pipeline Local Test ===`);
  console.log(`CSV directory: ${csvDir}`);
  console.log(`DATABASE_URL: ${process.env.DATABASE_URL ? "set" : "NOT SET"}\n`);

  if (!process.env.DATABASE_URL) {
    console.error("DATABASE_URL is not set. Check your .env files.");
    process.exit(1);
  }

  // Initialize DB and run migrations
  console.log("Initializing database...");
  await initDatabase();
  await runMigrations();
  console.log("Database ready.\n");

  // Run the local pipeline
  const result = await runZipLocalPipeline({
    csvDir,
    onProgress: (step, percent) => {
      console.log(`  [${String(percent).padStart(3)}%] ${step}`);
    },
  });

  console.log("\n=== Results ===");
  console.log(`Success: ${result.success}`);
  console.log(`Duration: ${result.duration}s`);
  console.log("Record counts:");
  for (const [key, value] of Object.entries(result.recordCounts)) {
    console.log(`  ${key}: ${typeof value === "number" ? value.toLocaleString() : value}`);
  }
  if (result.warnings.length > 0) {
    console.log(`Warnings: ${result.warnings.length}`);
    for (const w of result.warnings.slice(0, 10)) {
      console.log(`  - ${w}`);
    }
  }

  console.log("\nDone.");
  process.exit(0);
}

main().catch((err) => {
  console.error("Pipeline failed:", err);
  process.exit(1);
});
