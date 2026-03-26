/**
 * One-off script: backfill usage data and spot-check tier assignments.
 *
 * Run with: npx tsx scripts/backfill-usage.ts
 */

import { getPool } from "../src/lib/db/database";
import { runMigrations } from "../src/lib/db/migrations";
import {
  backfillUsageData,
  assignTier,
  TIER_DISPLAY_LABELS,
  type Segment,
} from "../src/lib/db/usage-store";

async function main() {
  const pool = getPool();

  // 1. Run migrations (creates new tables if needed)
  console.log("\n=== Running migrations ===");
  await runMigrations();

  // 2. Backfill 12 weeks
  console.log("\n=== Backfilling 12 weeks of usage data ===");
  const start = Date.now();
  await backfillUsageData(12);
  const duration = Math.round((Date.now() - start) / 1000);
  console.log(`Backfill complete in ${duration}s`);

  // 3. Table counts
  console.log("\n=== Table counts ===");
  const { rows: visitCount } = await pool.query("SELECT COUNT(*) AS cnt FROM member_weekly_visits");
  const { rows: transCount } = await pool.query("SELECT COUNT(*) AS cnt FROM member_tier_transitions");
  console.log(`member_weekly_visits: ${visitCount[0].cnt} rows`);
  console.log(`member_tier_transitions: ${transCount[0].cnt} rows`);

  // 4. Tier distribution snapshot (latest week)
  console.log("\n=== Tier distribution (latest week) ===");
  for (const segment of ["members", "sky3", "tv"] as Segment[]) {
    const { rows } = await pool.query(`
      SELECT tier, COUNT(*) AS cnt
      FROM member_weekly_visits
      WHERE segment = $1
        AND week_start = (SELECT MAX(week_start) FROM member_weekly_visits WHERE segment = $1)
      GROUP BY tier
      ORDER BY cnt DESC
    `, [segment]);
    console.log(`\n  ${segment.toUpperCase()}:`);
    for (const r of rows) {
      console.log(`    ${TIER_DISPLAY_LABELS[r.tier] || r.tier}: ${r.cnt}`);
    }
  }

  // 5. Spot-check: sample members from each tier
  console.log("\n=== Spot-check: sample members by tier ===");
  for (const segment of ["members", "sky3", "tv"] as Segment[]) {
    console.log(`\n  ${segment.toUpperCase()} — 2 samples per tier:`);
    const { rows: tiers } = await pool.query(`
      SELECT DISTINCT tier FROM member_weekly_visits
      WHERE segment = $1
        AND week_start = (SELECT MAX(week_start) FROM member_weekly_visits WHERE segment = $1)
    `, [segment]);

    for (const { tier } of tiers) {
      const { rows: samples } = await pool.query(`
        SELECT m.member_email, m.visit_count, m.tier,
               a.customer_name
        FROM member_weekly_visits m
        LEFT JOIN auto_renews a ON LOWER(a.customer_email) = m.member_email
          AND a.plan_state NOT IN ('Canceled', 'Invalid')
        WHERE m.segment = $1
          AND m.tier = $2
          AND m.week_start = (SELECT MAX(week_start) FROM member_weekly_visits WHERE segment = $1)
        LIMIT 2
      `, [segment, tier]);
      for (const s of samples) {
        const expectedTier = assignTier(segment, s.visit_count);
        const match = expectedTier === s.tier ? "✓" : `✗ expected ${expectedTier}`;
        console.log(`    [${TIER_DISPLAY_LABELS[s.tier]}] ${s.customer_name || s.member_email} — ${s.visit_count} visits — ${match}`);
      }
    }
  }

  // 6. Movement summary
  console.log("\n=== Movement summary (latest period) ===");
  const { rows: movRows } = await pool.query(`
    SELECT segment,
      COUNT(*) FILTER (WHERE direction = 'up') AS up,
      COUNT(*) FILTER (WHERE direction = 'same') AS same,
      COUNT(*) FILTER (WHERE direction = 'down') AS down
    FROM member_tier_transitions
    WHERE subscribed_both = true
      AND period_start = (SELECT MAX(period_start) FROM member_tier_transitions)
    GROUP BY segment
  `);
  for (const r of movRows) {
    console.log(`  ${r.segment}: ↑${r.up} →${r.same} ↓${r.down} (net: ${Number(r.up) - Number(r.down)})`);
  }

  // 7. Quick scorecard preview
  console.log("\n=== Scorecard preview ===");
  const { rows: targetRows } = await pool.query(`
    SELECT segment,
      ROUND(COUNT(*) FILTER (WHERE tier IN ('target','strong','power_user','upgrade_candidate','ready_to_upgrade','light','active','engaged'))
        * 100.0 / NULLIF(COUNT(*), 0), 1) AS pct_at_target
    FROM member_weekly_visits
    WHERE week_start = (SELECT MAX(week_start) FROM member_weekly_visits)
    GROUP BY segment
  `);
  for (const r of targetRows) {
    console.log(`  ${r.segment}: ${r.pct_at_target}% at target`);
  }

  console.log("\n=== Done ===");
  await pool.end();
}

main().catch(err => {
  console.error("Fatal:", err);
  process.exit(1);
});
