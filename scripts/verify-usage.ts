/**
 * Verify usage data after backfill — uses period-aggregated tier logic.
 *
 * Run with: npx tsx -r dotenv/config scripts/verify-usage.ts
 */

import { getPool } from "../src/lib/db/database";
import {
  getUsageScorecard,
  getUsageSegments,
  getUsageMovement,
  getUsageMembers,
  getSky3TierDistribution,
  getTvEngagementDistribution,
  TIER_DISPLAY_LABELS,
} from "../src/lib/db/usage-store";

async function main() {
  const pool = getPool();

  console.log("=== Table counts ===");
  const { rows: visitCount } = await pool.query("SELECT COUNT(*) AS cnt FROM member_weekly_visits");
  const { rows: transCount } = await pool.query("SELECT COUNT(*) AS cnt FROM member_tier_transitions");
  console.log(`member_weekly_visits: ${visitCount[0].cnt} rows`);
  console.log(`member_tier_transitions: ${transCount[0].cnt} rows`);

  console.log("\n=== Scorecard (all segments, 4-week period) ===");
  const scorecard = await getUsageScorecard(4, "all");
  for (const card of scorecard) {
    const delta = card.delta >= 0 ? `+${card.delta}` : String(card.delta);
    console.log(`  ${card.label}: ${card.value} (${delta})`);
  }

  console.log("\n=== Segment table ===");
  const segments = await getUsageSegments(4);
  for (const seg of segments) {
    console.log(`  ${seg.segment}: ${seg.subscribed} subscribed, ${seg.healthMetric}% at target, net movement ${seg.netMovement >= 0 ? "+" : ""}${seg.netMovement}`);
  }

  console.log("\n=== Movement bar ===");
  const movement = await getUsageMovement(4, "all");
  console.log(`  ↑${movement.movedUp} →${movement.stayed} ↓${movement.slippedDown} (net: ${movement.net >= 0 ? "+" : ""}${movement.net})`);
  if (movement.topFlows.length > 0) {
    console.log("  Top flows:");
    for (const f of movement.topFlows.slice(0, 3)) {
      console.log(`    ${TIER_DISPLAY_LABELS[f.from] || f.from} → ${TIER_DISPLAY_LABELS[f.to] || f.to}: ${f.count}`);
    }
  }

  console.log("\n=== Sky3 revenue opportunity table ===");
  const sky3Tiers = await getSky3TierDistribution();
  for (const t of sky3Tiers) {
    console.log(`  ${TIER_DISPLAY_LABELS[t.tier]}: ${t.count} (${t.pct}%)`);
  }

  console.log("\n=== TV engagement distribution ===");
  const tvTiers = await getTvEngagementDistribution();
  for (const t of tvTiers) {
    console.log(`  ${TIER_DISPLAY_LABELS[t.tier]}: ${t.count} (${t.pct}%) — was ${t.priorCount}`);
  }

  console.log("\n=== Members action table (top 5 at risk) ===");
  const atRisk = await getUsageMembers({ segment: "members", filter: "at_risk", perPage: 5 });
  console.log(`  ${atRisk.total} members at risk`);
  for (const m of atRisk.members) {
    console.log(`    ${m.name}: ${TIER_DISPLAY_LABELS[m.priorTier]} → ${TIER_DISPLAY_LABELS[m.currentTier]} (${m.priorVisits} → ${m.currentVisits} visits)`);
  }

  console.log("\n=== Members action table (top 5 improving) ===");
  const improving = await getUsageMembers({ segment: "members", filter: "improving", perPage: 5 });
  console.log(`  ${improving.total} members improving`);
  for (const m of improving.members) {
    console.log(`    ${m.name}: ${TIER_DISPLAY_LABELS[m.priorTier]} → ${TIER_DISPLAY_LABELS[m.currentTier]} (${m.priorVisits} → ${m.currentVisits} visits)`);
  }

  console.log("\n=== Members scorecard ===");
  const memberScorecard = await getUsageScorecard(4, "members");
  for (const card of memberScorecard) {
    const delta = card.delta >= 0 ? `+${card.delta}` : String(card.delta);
    console.log(`  ${card.label}: ${card.value} (${delta})`);
  }

  console.log("\n=== Done ===");
  await pool.end();
}

main().catch(err => {
  console.error("Fatal:", err);
  process.exit(1);
});
