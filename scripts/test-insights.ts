/**
 * Test insights engine against the production database.
 * Run: npx tsx scripts/test-insights.ts
 */
import dotenv from "dotenv";
dotenv.config({ path: ".env" });
dotenv.config({ path: ".env.local", override: true });
dotenv.config({ path: ".env.production.local", override: true });

import { getPool } from "../src/lib/db/database";
import { initDatabase } from "../src/lib/db/database";
import { computeInsights } from "../src/lib/analytics/insights";
import { saveInsights, getRecentInsights } from "../src/lib/db/insights-store";

async function main() {
  console.log("Initializing database...");
  await initDatabase();

  const pool = getPool();
  console.log("Running insight detectors...\n");

  const insights = await computeInsights(pool);

  console.log(`\n=== ${insights.length} Insights Detected ===\n`);
  for (const insight of insights) {
    const severityEmoji =
      insight.severity === "critical" ? "[!!]" :
      insight.severity === "warning" ? "[!]" :
      insight.severity === "positive" ? "[+]" : "[-]";
    console.log(`${severityEmoji} [${insight.category.toUpperCase()}] ${insight.headline}`);
    if (insight.explanation) {
      console.log(`    ${insight.explanation}`);
    }
    if (insight.metricContext) {
      console.log(`    Context: ${JSON.stringify(insight.metricContext)}`);
    }
    console.log();
  }

  // Save to DB
  if (insights.length > 0) {
    console.log("Saving insights to database...");
    await saveInsights(insights);

    // Read them back
    const recent = await getRecentInsights(10);
    console.log(`\nRead back ${recent.length} insights from DB:`);
    for (const r of recent) {
      console.log(`  [${r.severity}] ${r.headline} (detected ${r.detectedAt})`);
    }
  }

  await pool.end();
  console.log("\nDone.");
}

main().catch(console.error);
