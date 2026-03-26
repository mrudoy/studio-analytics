/**
 * Usage Redesign — Data Store
 *
 * All query functions and aggregation jobs for the new Usage section.
 * Tables: member_weekly_visits, member_tier_transitions, week_annotations
 */

import { getPool } from "./database";

// ── Tier Assignment ─────────────────────────────────────────

export type Segment = "members" | "sky3" | "tv";

const PLAN_CATEGORY_TO_SEGMENT: Record<string, Segment> = {
  MEMBER: "members",
  SKY3: "sky3",
  SKY_TING_TV: "tv",
};

const SEGMENT_TO_PLAN_CATEGORY: Record<Segment, string> = {
  members: "MEMBER",
  sky3: "SKY3",
  tv: "SKY_TING_TV",
};

export function assignTier(segment: Segment, visitCount: number): string {
  if (segment === "members") {
    if (visitCount === 0) return "dormant";
    if (visitCount <= 2) return "low";
    if (visitCount <= 4) return "target";
    if (visitCount <= 8) return "strong";
    return "power_user";
  }
  if (segment === "sky3") {
    if (visitCount === 0) return "not_using";
    if (visitCount === 1) return "barely_using";
    if (visitCount === 2) return "getting_there";
    if (visitCount === 3) return "full_use";
    return "wants_more";
  }
  // tv — based on sessions in trailing 14 days
  if (visitCount === 0) return "inactive";
  if (visitCount === 1) return "light";
  if (visitCount <= 3) return "active";
  return "engaged";
}

export const MEMBERS_TIER_ORDER = ["dormant", "low", "target", "strong", "power_user"];
export const SKY3_TIER_ORDER = ["not_using", "barely_using", "getting_there", "full_use", "wants_more"];
export const TV_TIER_ORDER = ["inactive", "light", "active", "engaged"];

function getTierOrder(segment: Segment): string[] {
  if (segment === "members") return MEMBERS_TIER_ORDER;
  if (segment === "sky3") return SKY3_TIER_ORDER;
  return TV_TIER_ORDER;
}

function computeDirection(segment: Segment, priorTier: string, currentTier: string): "up" | "down" | "same" {
  const order = getTierOrder(segment);
  const priorIdx = order.indexOf(priorTier);
  const currentIdx = order.indexOf(currentTier);
  if (currentIdx > priorIdx) return "up";
  if (currentIdx < priorIdx) return "down";
  return "same";
}

/** Tiers considered "at target" for % Hitting Target calculations */
const TARGET_TIERS: Record<Segment, string[]> = {
  members: ["target", "strong", "power_user"],
  sky3: ["full_use", "wants_more"],
  tv: ["light", "active", "engaged"],
};

export const TIER_DISPLAY_LABELS: Record<string, string> = {
  dormant: "Dormant",
  low: "Low",
  target: "Target",
  strong: "Strong",
  power_user: "Power User",
  // Sky3 (new bands)
  not_using: "Not Using",
  barely_using: "Barely Using",
  getting_there: "Getting There",
  full_use: "Using All 3 Classes",
  wants_more: "Wants More",
  // Legacy Sky3 keys (for transition compatibility)
  unused_pack: "Not Using",
  save_candidate: "Barely Using",
  building_habit: "Getting There",
  upgrade_candidate: "Using All 3 Classes",
  ready_to_upgrade: "Wants More",
  // TV
  inactive: "Inactive",
  light: "Light",
  active: "Active",
  engaged: "Engaged",
};

export const TIER_COLORS: Record<string, string> = {
  dormant: "#C0392B",
  low: "#E67E22",
  target: "#27AE60",
  strong: "#2ECC71",
  power_user: "#1ABC9C",
  // Sky3 (new warm-to-green gradient)
  not_using: "#E8D5D0",
  barely_using: "#F0DCC8",
  getting_there: "#F5EAB8",
  full_use: "#C8E6C9",
  wants_more: "#A5D6A7",
  // Legacy Sky3 keys
  unused_pack: "#E8D5D0",
  save_candidate: "#F0DCC8",
  building_habit: "#F5EAB8",
  upgrade_candidate: "#C8E6C9",
  ready_to_upgrade: "#A5D6A7",
  // TV
  inactive: "#C0392B",
  light: "#E67E22",
  active: "#27AE60",
  engaged: "#1ABC9C",
};

export const DIRECTION_LABELS: Record<string, string> = {
  up: "↑",
  down: "↓",
  same: "→",
};

export const DELTA_COLORS = {
  positive: "#27AE60",
  negative: "#C0392B",
  neutral: "#95A5A6",
};

export const REVENUE_OPPORTUNITY_COPY: Record<string, { means: string; action: string }> = {
  unused_pack: { means: "Will probably cancel", action: "Win-back sequence" },
  save_candidate: { means: "At risk of canceling", action: "Engagement check-in" },
  building_habit: { means: "Getting value, could go either way", action: "Nurture" },
  upgrade_candidate: { means: "Maxed out — want more", action: "Offer membership" },
  ready_to_upgrade: { means: "Already buying extra", action: "Priority upgrade outreach" },
};

// ── Helpers ─────────────────────────────────────────────────

/** Get Monday of the ISO week containing the given date */
function getWeekStart(date: Date): string {
  const d = new Date(date);
  const day = d.getUTCDay(); // 0=Sun, 1=Mon, ...
  const diff = day === 0 ? -6 : 1 - day; // shift to Monday
  d.setUTCDate(d.getUTCDate() + diff);
  return d.toISOString().slice(0, 10);
}

/** Get the Monday N weeks ago from now */
function weeksAgo(n: number): string {
  const d = new Date();
  d.setUTCDate(d.getUTCDate() - n * 7);
  return getWeekStart(d);
}

/** Get the current week's Monday */
function currentWeekStart(): string {
  return getWeekStart(new Date());
}

// ── Aggregation Jobs ────────────────────────────────────────

/**
 * Compute weekly visit snapshots for a given week.
 * For Members/Sky3: count registrations in that ISO week.
 * For TV: count registrations in trailing 14 days from week_start.
 */
export async function computeWeeklyVisits(weekStart: string): Promise<number> {
  const pool = getPool();

  // Get active subscribers per segment
  // Members and Sky3: count visits in the ISO week (Mon-Sun)
  const inStudioResult = await pool.query(`
    WITH active_subs AS (
      SELECT DISTINCT ON (LOWER(customer_email), plan_category)
        LOWER(customer_email) AS email,
        customer_name AS name,
        plan_category
      FROM auto_renews
      WHERE plan_state IN ('Valid Now', 'Paused')
        AND plan_category IN ('MEMBER', 'SKY3')
        AND customer_email IS NOT NULL
    ),
    visit_counts AS (
      SELECT
        LOWER(r.email) AS email,
        COUNT(*) AS visit_count
      FROM registrations r
      WHERE r.attended_at >= $1::date
        AND r.attended_at < ($1::date + INTERVAL '7 days')
        AND r.state IN ('redeemed', 'confirmed')
        AND r.email IS NOT NULL
      GROUP BY LOWER(r.email)
    )
    INSERT INTO member_weekly_visits (member_email, segment, week_start, visit_count, tier)
    SELECT
      s.email,
      CASE s.plan_category
        WHEN 'MEMBER' THEN 'members'
        WHEN 'SKY3' THEN 'sky3'
      END AS segment,
      $1::date AS week_start,
      COALESCE(v.visit_count, 0) AS visit_count,
      '' AS tier
    FROM active_subs s
    LEFT JOIN visit_counts v ON v.email = s.email
    ON CONFLICT (member_email, segment, week_start) DO UPDATE
      SET visit_count = EXCLUDED.visit_count
    RETURNING id
  `, [weekStart]);

  // TV: count registrations in trailing 14 days
  const tvResult = await pool.query(`
    WITH active_tv_subs AS (
      SELECT DISTINCT ON (LOWER(customer_email))
        LOWER(customer_email) AS email,
        customer_name AS name
      FROM auto_renews
      WHERE plan_state IN ('Valid Now', 'Paused')
        AND plan_category = 'SKY_TING_TV'
        AND customer_email IS NOT NULL
    ),
    session_counts AS (
      SELECT
        LOWER(r.email) AS email,
        COUNT(*) AS visit_count
      FROM registrations r
      WHERE r.attended_at >= ($1::date - INTERVAL '14 days')
        AND r.attended_at < $1::date
        AND r.state IN ('redeemed', 'confirmed')
        AND r.email IS NOT NULL
      GROUP BY LOWER(r.email)
    )
    INSERT INTO member_weekly_visits (member_email, segment, week_start, visit_count, tier)
    SELECT
      s.email,
      'tv' AS segment,
      $1::date AS week_start,
      COALESCE(sc.visit_count, 0) AS visit_count,
      '' AS tier
    FROM active_tv_subs s
    LEFT JOIN session_counts sc ON sc.email = s.email
    ON CONFLICT (member_email, segment, week_start) DO UPDATE
      SET visit_count = EXCLUDED.visit_count
    RETURNING id
  `, [weekStart]);

  const totalRows = (inStudioResult.rowCount ?? 0) + (tvResult.rowCount ?? 0);

  // For TV: assign tier directly (14-day trailing window = directly meaningful)
  // For Members/Sky3: store empty tier — real tiers computed from multi-week aggregates
  // in query functions and computeTierTransitions
  await pool.query(`
    UPDATE member_weekly_visits SET tier = CASE
      WHEN segment = 'tv' THEN CASE
        WHEN visit_count = 0 THEN 'inactive'
        WHEN visit_count = 1 THEN 'light'
        WHEN visit_count <= 3 THEN 'active'
        ELSE 'engaged' END
      ELSE ''
    END
    WHERE week_start = $1
  `, [weekStart]);

  return totalRows;
}

/**
 * Compute tier transitions between two periods.
 * periodWeeks = how many weeks per period (default 4).
 * currentPeriodStart = Monday of the current period's first week.
 */
export async function computeTierTransitions(currentPeriodStart: string, periodWeeks = 4): Promise<number> {
  const pool = getPool();

  // Aggregate visits per member over the period
  // Current period: currentPeriodStart to currentPeriodStart + periodWeeks weeks
  // Prior period: currentPeriodStart - periodWeeks weeks to currentPeriodStart
  const priorStart = new Date(currentPeriodStart);
  priorStart.setUTCDate(priorStart.getUTCDate() - periodWeeks * 7);
  const priorStartStr = priorStart.toISOString().slice(0, 10);
  const periodEnd = new Date(currentPeriodStart);
  periodEnd.setUTCDate(periodEnd.getUTCDate() + periodWeeks * 7);
  const periodEndStr = periodEnd.toISOString().slice(0, 10);

  // Get aggregated visit counts per member for current and prior periods
  const result = await pool.query(`
    WITH current_period AS (
      SELECT member_email, segment,
        SUM(visit_count) AS total_visits,
        MAX(tier) AS tier_placeholder
      FROM member_weekly_visits
      WHERE week_start >= $1::date AND week_start < $2::date
      GROUP BY member_email, segment
    ),
    prior_period AS (
      SELECT member_email, segment,
        SUM(visit_count) AS total_visits
      FROM member_weekly_visits
      WHERE week_start >= $3::date AND week_start < $1::date
      GROUP BY member_email, segment
    ),
    member_names AS (
      SELECT DISTINCT ON (LOWER(customer_email))
        LOWER(customer_email) AS email,
        customer_name AS name
      FROM auto_renews
      WHERE customer_email IS NOT NULL
      ORDER BY LOWER(customer_email), id DESC
    )
    INSERT INTO member_tier_transitions
      (member_email, member_name, segment, period_start, period_end,
       prior_tier, current_tier, direction, prior_visits, current_visits, subscribed_both)
    SELECT
      c.member_email,
      mn.name,
      c.segment,
      $1::date AS period_start,
      $2::date AS period_end,
      '' AS prior_tier,
      '' AS current_tier,
      '' AS direction,
      COALESCE(p.total_visits, 0) AS prior_visits,
      c.total_visits AS current_visits,
      (p.member_email IS NOT NULL) AS subscribed_both
    FROM current_period c
    LEFT JOIN prior_period p ON p.member_email = c.member_email AND p.segment = c.segment
    LEFT JOIN member_names mn ON mn.email = c.member_email
    ON CONFLICT (member_email, segment, period_start) DO UPDATE
      SET prior_visits = EXCLUDED.prior_visits,
          current_visits = EXCLUDED.current_visits,
          subscribed_both = EXCLUDED.subscribed_both,
          member_name = EXCLUDED.member_name
    RETURNING id, member_email, segment, prior_visits, current_visits, subscribed_both
  `, [currentPeriodStart, periodEndStr, priorStartStr]);

  // Bulk-update tiers and direction using SQL
  // months_in_period ≈ periodWeeks / 4.33
  const monthsInPeriod = periodWeeks / 4.33;

  // Helper: builds a tier CASE expression for a given visit expression
  const tierCase = (visitExpr: string, seg: string) => {
    if (seg === "members") {
      return `CASE
        WHEN ${visitExpr} = 0 THEN 'dormant'
        WHEN ${visitExpr} <= 2 THEN 'low'
        WHEN ${visitExpr} <= 4 THEN 'target'
        WHEN ${visitExpr} <= 8 THEN 'strong'
        ELSE 'power_user' END`;
    }
    if (seg === "sky3") {
      return `CASE
        WHEN ${visitExpr} = 0 THEN 'not_using'
        WHEN ${visitExpr} = 1 THEN 'barely_using'
        WHEN ${visitExpr} = 2 THEN 'getting_there'
        WHEN ${visitExpr} = 3 THEN 'full_use'
        ELSE 'wants_more' END`;
    }
    // tv
    return `CASE
      WHEN ${visitExpr} = 0 THEN 'inactive'
      WHEN ${visitExpr} = 1 THEN 'light'
      WHEN ${visitExpr} <= 3 THEN 'active'
      ELSE 'engaged' END`;
  };

  for (const seg of ["members", "sky3", "tv"] as Segment[]) {
    // For Members/Sky3: visits per month = ROUND(total_visits / monthsInPeriod)
    // For TV: average per week = ROUND(total_visits / periodWeeks)
    const currentVisitExpr = seg === "tv"
      ? `ROUND(current_visits::numeric / ${periodWeeks})`
      : `ROUND(current_visits::numeric / ${monthsInPeriod})`;
    const priorVisitExpr = seg === "tv"
      ? `ROUND(prior_visits::numeric / ${periodWeeks})`
      : `CASE WHEN subscribed_both THEN ROUND(prior_visits::numeric / ${monthsInPeriod}) ELSE 0 END`;

    const currentTierExpr = tierCase(currentVisitExpr, seg);
    const priorTierExpr = `CASE WHEN subscribed_both THEN ${tierCase(priorVisitExpr, seg)} ELSE ${currentTierExpr} END`;

    // Build tier order for direction comparison
    const tierOrder = getTierOrder(seg);
    const tierOrderCase = tierOrder.map((t, i) => `WHEN '${t}' THEN ${i}`).join(" ");
    const currentIdxExpr = `(CASE ${currentTierExpr} ${tierOrderCase} ELSE 0 END)`;
    const priorIdxExpr = `(CASE ${priorTierExpr} ${tierOrderCase} ELSE 0 END)`;

    await pool.query(`
      UPDATE member_tier_transitions SET
        current_tier = ${currentTierExpr},
        prior_tier = ${priorTierExpr},
        direction = CASE
          WHEN NOT subscribed_both THEN 'same'
          WHEN ${currentIdxExpr} > ${priorIdxExpr} THEN 'up'
          WHEN ${currentIdxExpr} < ${priorIdxExpr} THEN 'down'
          ELSE 'same'
        END
      WHERE segment = $1 AND period_start = $2::date
    `, [seg, currentPeriodStart]);
  }

  return result.rowCount ?? 0;
}

/**
 * Backfill usage data for the last N weeks.
 */
export async function backfillUsageData(weeks = 12): Promise<void> {
  console.log(`[usage] Backfilling ${weeks} weeks of usage data...`);

  // Compute weekly visits for each week
  for (let i = weeks; i >= 0; i--) {
    const ws = weeksAgo(i);
    const count = await computeWeeklyVisits(ws);
    console.log(`[usage] Week ${ws}: ${count} rows`);
  }

  // Compute transitions for each possible period start
  // With 4-week periods, we need at least 8 weeks of data for one transition
  for (let i = weeks - 4; i >= 0; i -= 4) {
    const ps = weeksAgo(i);
    const count = await computeTierTransitions(ps, 4);
    console.log(`[usage] Transitions from ${ps}: ${count} rows`);
  }

  console.log("[usage] Backfill complete");
}

// ── Period-Aggregated Tier Helper ────────────────────────────

/**
 * SQL CTE fragment that aggregates weekly visit data over a period
 * and assigns tiers based on the summed monthly-equivalent visit count.
 *
 * Returns rows: (member_email, segment, total_visits, tier)
 *
 * For Members/Sky3: sums visit_count over all weeks in [periodStart, periodEnd),
 *   then divides by months to get monthly rate, assigns tier.
 * For TV: uses the latest week's tier directly (14-day trailing = already meaningful).
 */
async function getPeriodTiers(
  pool: import("pg").Pool,
  segments: Segment[],
  periodStart: string,
  periodWeeks: number
): Promise<{ member_email: string; segment: string; total_visits: number; tier: string }[]> {
  const segList = segments.map(s => `'${s}'`).join(",");
  const periodEnd = new Date(periodStart);
  periodEnd.setUTCDate(periodEnd.getUTCDate() + periodWeeks * 7);
  const endStr = periodEnd.toISOString().slice(0, 10);
  const monthsInPeriod = periodWeeks / 4.33;

  const { rows } = await pool.query(`
    WITH agg AS (
      SELECT member_email, segment, SUM(visit_count) AS total_visits
      FROM member_weekly_visits
      WHERE segment IN (${segList})
        AND week_start >= $1::date AND week_start < $2::date
      GROUP BY member_email, segment
    )
    SELECT member_email, segment, total_visits,
      CASE
        WHEN segment IN ('members') THEN CASE
          WHEN total_visits = 0 THEN 'dormant'
          WHEN ROUND(total_visits / ${monthsInPeriod}) <= 2 THEN 'low'
          WHEN ROUND(total_visits / ${monthsInPeriod}) <= 4 THEN 'target'
          WHEN ROUND(total_visits / ${monthsInPeriod}) <= 8 THEN 'strong'
          ELSE 'power_user' END
        WHEN segment = 'sky3' THEN CASE
          WHEN total_visits = 0 THEN 'not_using'
          WHEN ROUND(total_visits / ${monthsInPeriod}) <= 1 THEN 'barely_using'
          WHEN ROUND(total_visits / ${monthsInPeriod}) <= 2 THEN 'getting_there'
          WHEN ROUND(total_visits / ${monthsInPeriod}) <= 3 THEN 'full_use'
          ELSE 'wants_more' END
        WHEN segment = 'tv' THEN CASE
          WHEN total_visits = 0 THEN 'inactive'
          WHEN total_visits = 1 THEN 'light'
          WHEN total_visits <= 3 THEN 'active'
          ELSE 'engaged' END
        ELSE ''
      END AS tier
    FROM agg
  `, [periodStart, endStr]);

  return rows as { member_email: string; segment: string; total_visits: number; tier: string }[];
}

/**
 * Convenience: get tier counts for given segments in a period.
 * Returns { total, atTarget } for % Hitting Target calculations.
 */
async function getPeriodTierCounts(
  pool: import("pg").Pool,
  segments: Segment[],
  periodStart: string,
  periodWeeks: number
): Promise<{ total: number; atTarget: number; tierCounts: Record<string, number> }> {
  const rows = await getPeriodTiers(pool, segments, periodStart, periodWeeks);
  const tierCounts: Record<string, number> = {};
  let atTarget = 0;
  for (const r of rows) {
    tierCounts[r.tier] = (tierCounts[r.tier] || 0) + 1;
    const seg = r.segment as Segment;
    if (TARGET_TIERS[seg]?.includes(r.tier)) atTarget++;
  }
  return { total: rows.length, atTarget, tierCounts };
}

// ── Query Functions (used by API endpoints) ─────────────────

export interface ScorecardCard {
  key: string;
  label: string;
  value: number;
  format: "pct" | "count" | "signed_count" | "decimal";
  sparkline: number[];
  delta: number;
  deltaType: "pct" | "count";
  invertDirection: boolean; // true for dormant (lower is better)
}

/**
 * Get scorecard metrics for the Overview or a specific segment.
 */
export async function getUsageScorecard(
  periodWeeks = 4,
  segment: Segment | "all" = "all"
): Promise<ScorecardCard[]> {
  const pool = getPool();
  const now = currentWeekStart();

  // Determine the periods
  const periodStart = weeksAgo(periodWeeks);
  const priorPeriodStart = weeksAgo(periodWeeks * 2);

  // Helper to get % hitting target for a set of segments in a period
  async function pctHittingTarget(segs: Segment[], start: string, weeks: number): Promise<number> {
    const { total, atTarget } = await getPeriodTierCounts(pool, segs, start, weeks);
    if (total === 0) return 0;
    return Math.round((atTarget / total) * 1000) / 10;
  }

  // Helper to get net movement for a period
  async function getNetMovement(segs: Segment[], start: string): Promise<{ up: number; stayed: number; down: number; net: number }> {
    const segList = segs.map(s => `'${s}'`).join(",");
    const { rows } = await pool.query(`
      SELECT
        COUNT(*) FILTER (WHERE direction = 'up') AS moved_up,
        COUNT(*) FILTER (WHERE direction = 'same') AS stayed,
        COUNT(*) FILTER (WHERE direction = 'down') AS slipped_down
      FROM member_tier_transitions
      WHERE segment IN (${segList})
        AND period_start = $1::date
        AND subscribed_both = true
    `, [start]);
    const r = rows[0] || { moved_up: 0, stayed: 0, slipped_down: 0 };
    const up = Number(r.moved_up);
    const down = Number(r.slipped_down);
    return { up, stayed: Number(r.stayed), down, net: up - down };
  }

  // Helper to get dormant/inactive count (uses period-aggregated tiers)
  async function getDormantCount(segs: Segment[], pStart: string, pWeeks: number): Promise<number> {
    const dormantTiers = new Set(["dormant", "inactive", "unused_pack"]);
    const { tierCounts } = await getPeriodTierCounts(pool, segs, pStart, pWeeks);
    let count = 0;
    for (const [tier, cnt] of Object.entries(tierCounts)) {
      if (dormantTiers.has(tier)) count += cnt;
    }
    return count;
  }

  // Helper for total subscribed
  async function getTotalSubscribed(segs: Segment[]): Promise<number> {
    const cats = segs.map(s => `'${SEGMENT_TO_PLAN_CATEGORY[s]}'`).join(",");
    const { rows } = await pool.query(`
      SELECT COUNT(DISTINCT LOWER(customer_email)) AS cnt
      FROM auto_renews
      WHERE plan_state IN ('Valid Now', 'Paused')
        AND plan_category IN (${cats})
        AND customer_email IS NOT NULL
    `);
    return Number(rows[0]?.cnt ?? 0);
  }

  // Determine segments to query
  const targetSegs: Segment[] = segment === "all"
    ? ["members", "sky3", "tv"]
    : [segment];
  const inStudioSegs: Segment[] = segment === "all"
    ? ["members", "sky3"]
    : segment === "tv" ? [] : [segment];
  const tvSegs: Segment[] = segment === "all" || segment === "tv"
    ? ["tv"]
    : [];

  // Sparkline helper: get 8 weekly values for a metric
  async function getSparkline(fn: (ws: string) => Promise<number>): Promise<number[]> {
    const points: number[] = [];
    for (let i = 7; i >= 0; i--) {
      const ws = weeksAgo(i);
      points.push(await fn(ws));
    }
    return points;
  }

  const cards: ScorecardCard[] = [];

  // Card 1: % Hitting Target (Members + Sky3, or segment-specific)
  if (inStudioSegs.length > 0) {
    const current = await pctHittingTarget(inStudioSegs, periodStart, periodWeeks);
    const prior = await pctHittingTarget(inStudioSegs, priorPeriodStart, periodWeeks);
    const sparkline = await getSparkline(async (ws) => {
      return pctHittingTarget(inStudioSegs, ws, periodWeeks);
    });
    cards.push({
      key: "pct_hitting_target",
      label: "% Hitting Target",
      value: current,
      format: "pct",
      sparkline,
      delta: Math.round((current - prior) * 10) / 10,
      deltaType: "pct",
      invertDirection: false,
    });
  }

  // Card 2: % Active 14-Day (TV) — only if we have TV in scope
  if (tvSegs.length > 0) {
    const tvActive = async (ws: string): Promise<number> => {
      const { rows } = await pool.query(`
        SELECT tier FROM member_weekly_visits
        WHERE segment = 'tv' AND week_start = $1::date
      `, [ws]);
      if (rows.length === 0) return 0;
      const active = rows.filter((r: { tier: string }) =>
        TARGET_TIERS.tv.includes(r.tier)
      ).length;
      return Math.round((active / rows.length) * 1000) / 10;
    };
    // Find the latest week with TV data
    const { rows: latestRows } = await pool.query(`
      SELECT MAX(week_start) AS ws FROM member_weekly_visits WHERE segment = 'tv'
    `);
    const latestWs = latestRows[0]?.ws;
    const current = latestWs ? await tvActive(latestWs) : 0;
    // Prior: 4 weeks before latest
    const priorWs = latestWs ? (() => {
      const d = new Date(latestWs);
      d.setUTCDate(d.getUTCDate() - periodWeeks * 7);
      return d.toISOString().slice(0, 10);
    })() : null;
    const prior = priorWs ? await tvActive(priorWs) : 0;
    const sparkline = await getSparkline(tvActive);
    cards.push({
      key: "pct_active_tv",
      label: "% Active 14-Day (TV)",
      value: current,
      format: "pct",
      sparkline,
      delta: Math.round((current - prior) * 10) / 10,
      deltaType: "pct",
      invertDirection: false,
    });
  }

  // Card 3: Net Movement
  {
    // Find the latest transition period
    const { rows: periodRows } = await pool.query(`
      SELECT DISTINCT period_start FROM member_tier_transitions
      WHERE segment IN (${targetSegs.map(s => `'${s}'`).join(",")})
      ORDER BY period_start DESC LIMIT 2
    `);
    const currentPeriod = periodRows[0]?.period_start;
    const priorPeriod = periodRows[1]?.period_start;

    const currentMov = currentPeriod ? await getNetMovement(targetSegs, currentPeriod) : { up: 0, stayed: 0, down: 0, net: 0 };
    const priorMov = priorPeriod ? await getNetMovement(targetSegs, priorPeriod) : { up: 0, stayed: 0, down: 0, net: 0 };

    // Sparkline: net movement per available period
    const sparkline: number[] = [];
    const { rows: allPeriods } = await pool.query(`
      SELECT DISTINCT period_start FROM member_tier_transitions
      WHERE segment IN (${targetSegs.map(s => `'${s}'`).join(",")})
      ORDER BY period_start DESC LIMIT 8
    `);
    for (const p of allPeriods.reverse()) {
      const m = await getNetMovement(targetSegs, p.period_start);
      sparkline.push(m.net);
    }

    cards.push({
      key: "net_movement",
      label: "Net Movement",
      value: currentMov.net,
      format: "signed_count",
      sparkline,
      delta: currentMov.net - priorMov.net,
      deltaType: "count",
      invertDirection: false,
    });
  }

  // Card 4: Dormant / Inactive Count
  {
    const current = await getDormantCount(targetSegs, periodStart, periodWeeks);
    const prior = await getDormantCount(targetSegs, priorPeriodStart, periodWeeks);

    const sparkline = await getSparkline(async (ws) => {
      return getDormantCount(targetSegs, ws, periodWeeks);
    });

    cards.push({
      key: "dormant_count",
      label: "Dormant / Inactive",
      value: current,
      format: "count",
      sparkline,
      delta: current - prior,
      deltaType: "count",
      invertDirection: true, // fewer is better
    });
  }

  // Card 5: Total Subscribed
  {
    const current = await getTotalSubscribed(targetSegs);
    // Prior subscribed count isn't directly available without historical snapshots
    // Use the count of members in the prior week's member_weekly_visits as a proxy
    const priorWs = weeksAgo(periodWeeks);
    const segList = targetSegs.map(s => `'${s}'`).join(",");
    const { rows: priorRows } = await pool.query(`
      SELECT COUNT(*) AS cnt FROM member_weekly_visits
      WHERE segment IN (${segList}) AND week_start = $1::date
    `, [priorWs]);
    const prior = Number(priorRows[0]?.cnt ?? 0);

    const sparkline = await getSparkline(async (ws) => {
      const { rows } = await pool.query(`
        SELECT COUNT(*) AS cnt FROM member_weekly_visits
        WHERE segment IN (${segList}) AND week_start = $1::date
      `, [ws]);
      return Number(rows[0]?.cnt ?? 0);
    });

    cards.push({
      key: "total_subscribed",
      label: "Total Subscribed",
      value: current,
      format: "count",
      sparkline,
      delta: current - prior,
      deltaType: "count",
      invertDirection: false,
    });
  }

  // For Sky3: replace default cards with Sky3-specific ones
  if (segment === "sky3") {
    const sky3Cards: ScorecardCard[] = [];
    const currentTiers = await getPeriodTierCounts(pool, ["sky3"], periodStart, periodWeeks);
    const priorTiers = await getPeriodTierCounts(pool, ["sky3"], priorPeriodStart, periodWeeks);

    // Card 1: % Using 3+ Classes (full_use + wants_more)
    const fullUseTiers = ["full_use", "wants_more"];
    const curFullUse = fullUseTiers.reduce((s, t) => s + (currentTiers.tierCounts[t] || 0), 0);
    const curFullUsePct = currentTiers.total > 0 ? Math.round((curFullUse / currentTiers.total) * 1000) / 10 : 0;
    const priFullUse = fullUseTiers.reduce((s, t) => s + (priorTiers.tierCounts[t] || 0), 0);
    const priFullUsePct = priorTiers.total > 0 ? Math.round((priFullUse / priorTiers.total) * 1000) / 10 : 0;
    sky3Cards.push({
      key: "pct_full_use", label: "% Using 3+ Classes", value: curFullUsePct, format: "pct",
      sparkline: [], delta: Math.round((curFullUsePct - priFullUsePct) * 10) / 10, deltaType: "pct", invertDirection: false,
    });

    // Card 2: % Not Using (0-1 visits) — inverted (lower is better)
    const notUsingTiers = ["not_using", "barely_using"];
    const curNotUsing = notUsingTiers.reduce((s, t) => s + (currentTiers.tierCounts[t] || 0), 0);
    const curNotUsingPct = currentTiers.total > 0 ? Math.round((curNotUsing / currentTiers.total) * 1000) / 10 : 0;
    const priNotUsing = notUsingTiers.reduce((s, t) => s + (priorTiers.tierCounts[t] || 0), 0);
    const priNotUsingPct = priorTiers.total > 0 ? Math.round((priNotUsing / priorTiers.total) * 1000) / 10 : 0;
    sky3Cards.push({
      key: "pct_not_using", label: "% Not Using (0\u20131)", value: curNotUsingPct, format: "pct",
      sparkline: [], delta: Math.round((curNotUsingPct - priNotUsingPct) * 10) / 10, deltaType: "pct", invertDirection: true,
    });

    // Card 3: Wants More (4+ visits — real upgrade signal)
    const wantsMore = currentTiers.tierCounts["wants_more"] || 0;
    const priWantsMore = priorTiers.tierCounts["wants_more"] || 0;
    sky3Cards.push({
      key: "wants_more_count", label: "Wants More (4+)", value: wantsMore, format: "count",
      sparkline: [], delta: wantsMore - priWantsMore, deltaType: "count", invertDirection: false,
    });

    // Card 4: Total Subscribed
    const totalSub = cards.find(c => c.key === "total_subscribed");
    if (totalSub) sky3Cards.push(totalSub);

    return sky3Cards;
  }

  // For segment-specific scorecards, add extra metrics
  if (segment === "members") {
    // Avg Visits/Mo — insert at position 3 (before Dormant)
    const segList = "'members'";
    const { rows } = await pool.query(`
      SELECT AVG(visit_count) AS avg_visits FROM member_weekly_visits
      WHERE segment = 'members'
        AND week_start = (SELECT MAX(week_start) FROM member_weekly_visits WHERE segment = 'members')
    `);
    const avgVisits = Math.round(Number(rows[0]?.avg_visits ?? 0) * 10) / 10;
    // Convert weekly avg to monthly (multiply by ~4.33)
    const monthlyAvg = Math.round(avgVisits * 4.33 * 10) / 10;

    const { rows: priorRows } = await pool.query(`
      SELECT AVG(visit_count) AS avg_visits FROM member_weekly_visits
      WHERE segment = 'members' AND week_start = $1::date
    `, [weeksAgo(periodWeeks)]);
    const priorAvg = Math.round(Number(priorRows[0]?.avg_visits ?? 0) * 4.33 * 10) / 10;

    cards.splice(3, 0, {
      key: "avg_visits_mo",
      label: "Avg Visits/Mo",
      value: monthlyAvg,
      format: "decimal",
      sparkline: [], // omit sparkline for simplicity
      delta: Math.round((monthlyAvg - priorAvg) * 10) / 10,
      deltaType: "count",
      invertDirection: false,
    });
  }

  return cards;
}

/**
 * Get trend line data: % hitting target per segment per week.
 */
export async function getUsageTrend(
  weeks = 12,
  segment: Segment | "all" = "all"
): Promise<{ segment: string; data: { week: string; value: number }[] }[]> {
  const pool = getPool();
  const startWeek = weeksAgo(weeks);

  const targetSegs: Segment[] = segment === "all"
    ? ["members", "sky3", "tv"]
    : [segment];

  const series: { segment: string; data: { week: string; value: number }[] }[] = [];

  for (const seg of targetSegs) {
    // For TV: weekly tier is already meaningful (14-day trailing)
    if (seg === "tv") {
      const { rows } = await pool.query(`
        SELECT week_start, tier
        FROM member_weekly_visits
        WHERE segment = 'tv' AND week_start >= $1::date
        ORDER BY week_start
      `, [startWeek]);

      const byWeek = new Map<string, { total: number; atTarget: number }>();
      for (const r of rows) {
        const ws = r.week_start as string;
        if (!byWeek.has(ws)) byWeek.set(ws, { total: 0, atTarget: 0 });
        const entry = byWeek.get(ws)!;
        entry.total++;
        if (TARGET_TIERS.tv.includes(r.tier)) entry.atTarget++;
      }

      const data = Array.from(byWeek.entries())
        .sort(([a], [b]) => a.localeCompare(b))
        .map(([week, { total, atTarget }]) => ({
          week,
          value: total > 0 ? Math.round((atTarget / total) * 1000) / 10 : 0,
        }));
      series.push({ segment: seg, data });
    } else {
      // Members/Sky3: compute period-aggregated tier for each week as a rolling 4-week window
      const { rows: weekRows } = await pool.query(`
        SELECT DISTINCT week_start FROM member_weekly_visits
        WHERE segment = $1 AND week_start >= $2::date
        ORDER BY week_start
      `, [seg, startWeek]);

      const data: { week: string; value: number }[] = [];
      for (const wr of weekRows) {
        const ws = wr.week_start as string;
        // Use 4-week window ending at this week
        const windowStart = new Date(ws);
        windowStart.setUTCDate(windowStart.getUTCDate() - 3 * 7); // 4 weeks back
        const { total, atTarget } = await getPeriodTierCounts(pool, [seg], windowStart.toISOString().slice(0, 10), 4);
        data.push({
          week: ws,
          value: total > 0 ? Math.round((atTarget / total) * 1000) / 10 : 0,
        });
      }
      series.push({ segment: seg, data });
    }
  }

  return series;
}

/**
 * Get segment comparison table data.
 */
export async function getUsageSegments(periodWeeks = 4): Promise<{
  segment: string;
  subscribed: number;
  healthMetric: number;
  delta: number;
  netMovement: number;
}[]> {
  const pool = getPool();
  const segments: Segment[] = ["members", "sky3", "tv"];
  const results = [];

  for (const seg of segments) {
    const cat = SEGMENT_TO_PLAN_CATEGORY[seg];

    // Total subscribed
    const { rows: subRows } = await pool.query(`
      SELECT COUNT(DISTINCT LOWER(customer_email)) AS cnt
      FROM auto_renews
      WHERE plan_state IN ('Valid Now', 'Paused')
        AND plan_category = $1
        AND customer_email IS NOT NULL
    `, [cat]);
    const subscribed = Number(subRows[0]?.cnt ?? 0);

    // Health metric: use period-aggregated tiers
    const latestPeriodStart = weeksAgo(periodWeeks);
    const priorPeriodStartSeg = weeksAgo(periodWeeks * 2);

    let healthMetric = 0;
    let priorHealth = 0;

    {
      const { total, atTarget } = await getPeriodTierCounts(pool, [seg], latestPeriodStart, periodWeeks);
      if (total > 0) {
        healthMetric = Math.round((atTarget / total) * 1000) / 10;
      }

      const { total: priorTotal, atTarget: priorAt } = await getPeriodTierCounts(pool, [seg], priorPeriodStartSeg, periodWeeks);
      if (priorTotal > 0) {
        priorHealth = Math.round((priorAt / priorTotal) * 1000) / 10;
      }
    }

    // Net movement
    const { rows: periodRows } = await pool.query(`
      SELECT period_start FROM member_tier_transitions
      WHERE segment = $1
      ORDER BY period_start DESC LIMIT 1
    `, [seg]);
    let netMovement = 0;
    if (periodRows[0]) {
      const { rows: movRows } = await pool.query(`
        SELECT
          COUNT(*) FILTER (WHERE direction = 'up') AS up,
          COUNT(*) FILTER (WHERE direction = 'down') AS down
        FROM member_tier_transitions
        WHERE segment = $1 AND period_start = $2::date AND subscribed_both = true
      `, [seg, periodRows[0].period_start]);
      netMovement = Number(movRows[0]?.up ?? 0) - Number(movRows[0]?.down ?? 0);
    }

    results.push({
      segment: seg,
      subscribed,
      healthMetric,
      delta: Math.round((healthMetric - priorHealth) * 10) / 10,
      netMovement,
    });
  }

  return results;
}

/**
 * Get movement data (up/stayed/down) for the movement bar.
 */
export async function getUsageMovement(
  periodWeeks = 4,
  segment: Segment | "all" = "all"
): Promise<{
  movedUp: number;
  stayed: number;
  slippedDown: number;
  net: number;
  topFlows: { from: string; to: string; count: number }[];
}> {
  const pool = getPool();
  const segs: Segment[] = segment === "all" ? ["members", "sky3", "tv"] : [segment];
  const segList = segs.map(s => `'${s}'`).join(",");

  // Get latest transition period
  const { rows: periodRows } = await pool.query(`
    SELECT period_start FROM member_tier_transitions
    WHERE segment IN (${segList})
    ORDER BY period_start DESC LIMIT 1
  `);

  if (!periodRows[0]) {
    return { movedUp: 0, stayed: 0, slippedDown: 0, net: 0, topFlows: [] };
  }

  const ps = periodRows[0].period_start;

  const { rows } = await pool.query(`
    SELECT
      COUNT(*) FILTER (WHERE direction = 'up') AS moved_up,
      COUNT(*) FILTER (WHERE direction = 'same') AS stayed,
      COUNT(*) FILTER (WHERE direction = 'down') AS slipped_down
    FROM member_tier_transitions
    WHERE segment IN (${segList})
      AND period_start = $1::date
      AND subscribed_both = true
  `, [ps]);

  const movedUp = Number(rows[0]?.moved_up ?? 0);
  const stayed = Number(rows[0]?.stayed ?? 0);
  const slippedDown = Number(rows[0]?.slipped_down ?? 0);

  // Top flows
  const { rows: flowRows } = await pool.query(`
    SELECT prior_tier, current_tier, COUNT(*) AS cnt
    FROM member_tier_transitions
    WHERE segment IN (${segList})
      AND period_start = $1::date
      AND subscribed_both = true
      AND direction != 'same'
    GROUP BY prior_tier, current_tier
    ORDER BY cnt DESC
    LIMIT 5
  `, [ps]);

  const topFlows = flowRows.map((r: { prior_tier: string; current_tier: string; cnt: string }) => ({
    from: r.prior_tier,
    to: r.current_tier,
    count: Number(r.cnt),
  }));

  return { movedUp, stayed, slippedDown, net: movedUp - slippedDown, topFlows };
}

/**
 * Get action table data — individual members with their tier info.
 */
export async function getUsageMembers(params: {
  segment: Segment;
  periodWeeks?: number;
  filter?: "at_risk" | "newly_on_target" | "dormant" | "improving" | null;
  sortBy?: string;
  sortDir?: "asc" | "desc";
  page?: number;
  perPage?: number;
}): Promise<{
  members: {
    email: string;
    name: string;
    currentTier: string;
    priorTier: string;
    direction: string;
    currentVisits: number;
    priorVisits: number;
  }[];
  total: number;
  page: number;
}> {
  const pool = getPool();
  const { segment, filter, page = 1, perPage = 25 } = params;

  // Get latest transition period for this segment
  const { rows: periodRows } = await pool.query(`
    SELECT period_start FROM member_tier_transitions
    WHERE segment = $1
    ORDER BY period_start DESC LIMIT 1
  `, [segment]);

  if (!periodRows[0]) {
    return { members: [], total: 0, page };
  }

  const ps = periodRows[0].period_start;
  const targetTiers = TARGET_TIERS[segment].map(t => `'${t}'`).join(",");

  // Build filter conditions
  let filterClause = "";
  if (filter === "at_risk") {
    filterClause = `AND prior_tier IN (${targetTiers}) AND current_tier NOT IN (${targetTiers})`;
  } else if (filter === "newly_on_target") {
    filterClause = `AND prior_tier NOT IN (${targetTiers}) AND current_tier IN (${targetTiers})`;
  } else if (filter === "dormant") {
    const dormantTiers = segment === "members" ? "'dormant'" : segment === "sky3" ? "'not_using'" : "'inactive'";
    filterClause = `AND current_tier IN (${dormantTiers})`;
  } else if (filter === "improving") {
    filterClause = `AND direction = 'up'`;
  }

  // Count total
  const { rows: countRows } = await pool.query(`
    SELECT COUNT(*) AS cnt
    FROM member_tier_transitions
    WHERE segment = $1 AND period_start = $2::date AND subscribed_both = true
    ${filterClause}
  `, [segment, ps]);
  const total = Number(countRows[0]?.cnt ?? 0);

  // Fetch page
  const offset = (page - 1) * perPage;
  const { rows } = await pool.query(`
    SELECT
      member_email AS email,
      member_name AS name,
      current_tier,
      prior_tier,
      direction,
      current_visits,
      prior_visits
    FROM member_tier_transitions
    WHERE segment = $1 AND period_start = $2::date AND subscribed_both = true
    ${filterClause}
    ORDER BY
      CASE direction WHEN 'down' THEN 0 WHEN 'same' THEN 1 WHEN 'up' THEN 2 END,
      current_visits DESC
    LIMIT $3 OFFSET $4
  `, [segment, ps, perPage, offset]);

  // Batch-fetch 8-week sparklines for all members in the page (avoid N+1)
  const emails = rows.map((r: { email: string }) => r.email);
  const sparklineMap = new Map<string, number[]>();

  if (emails.length > 0) {
    const eightWeeksAgo = weeksAgo(8);
    const { rows: sparkRows } = await pool.query(`
      SELECT member_email, week_start, visit_count
      FROM member_weekly_visits
      WHERE member_email = ANY($1)
        AND segment = $2
        AND week_start >= $3::date
      ORDER BY member_email, week_start
    `, [emails, segment, eightWeeksAgo]);

    // Group by member
    for (const sr of sparkRows) {
      const em = sr.member_email as string;
      if (!sparklineMap.has(em)) sparklineMap.set(em, []);
      sparklineMap.get(em)!.push(Number(sr.visit_count));
    }
  }

  return {
    members: rows.map((r: {
      email: string; name: string; current_tier: string;
      prior_tier: string; direction: string; current_visits: number; prior_visits: number;
    }) => ({
      email: r.email,
      name: r.name || r.email,
      currentTier: r.current_tier,
      priorTier: r.prior_tier,
      direction: r.direction,
      currentVisits: r.current_visits,
      priorVisits: r.prior_visits,
      sparkline: sparklineMap.get(r.email) ?? [],
    })),
    total,
    page,
  };
}

/**
 * Export members as CSV data.
 */
export async function exportUsageMembers(params: {
  segment: Segment;
  periodWeeks?: number;
  filter?: "at_risk" | "newly_on_target" | "dormant" | "improving" | null;
}): Promise<string> {
  const result = await getUsageMembers({ ...params, page: 1, perPage: 100000 });

  const header = "name,email,segment,current_tier,previous_tier,direction,visits_current,visits_previous";
  const rows = result.members.map(m =>
    [
      `"${(m.name || "").replace(/"/g, '""')}"`,
      m.email,
      params.segment,
      TIER_DISPLAY_LABELS[m.currentTier] || m.currentTier,
      TIER_DISPLAY_LABELS[m.priorTier] || m.priorTier,
      m.direction,
      m.currentVisits,
      m.priorVisits,
    ].join(",")
  );

  return [header, ...rows].join("\r\n");
}

/**
 * Sky3-specific: get tier distribution for revenue opportunity table.
 */
export async function getSky3TierDistribution(): Promise<{
  tier: string;
  count: number;
  pct: number;
}[]> {
  const pool = getPool();
  // Use period-aggregated tiers (4-week window)
  const periodStart = weeksAgo(4);
  const { tierCounts, total } = await getPeriodTierCounts(pool, ["sky3"], periodStart, 4);

  return SKY3_TIER_ORDER.map(tier => ({
    tier,
    count: tierCounts[tier] || 0,
    pct: total > 0 ? Math.round(((tierCounts[tier] || 0) / total) * 1000) / 10 : 0,
  }));
}

/**
 * TV-specific: get engagement distribution with prior period comparison.
 */
export async function getTvEngagementDistribution(): Promise<{
  tier: string;
  count: number;
  pct: number;
  priorCount: number;
}[]> {
  const pool = getPool();
  const { rows: latestRows } = await pool.query(`
    SELECT DISTINCT week_start FROM member_weekly_visits
    WHERE segment = 'tv'
    ORDER BY week_start DESC LIMIT 2
  `);
  if (latestRows.length === 0) return [];

  const currentWs = latestRows[0].week_start;
  const priorWs = latestRows[1]?.week_start;

  const { rows: currentRows } = await pool.query(`
    SELECT tier, COUNT(*) AS cnt
    FROM member_weekly_visits
    WHERE segment = 'tv' AND week_start = $1::date
    GROUP BY tier
  `, [currentWs]);

  const priorRows = priorWs ? (await pool.query(`
    SELECT tier, COUNT(*) AS cnt
    FROM member_weekly_visits
    WHERE segment = 'tv' AND week_start = $1::date
    GROUP BY tier
  `, [priorWs])).rows : [];

  const total = currentRows.reduce((s: number, r: { cnt: string }) => s + Number(r.cnt), 0);

  return TV_TIER_ORDER.map(tier => {
    const cur = currentRows.find((r: { tier: string }) => r.tier === tier);
    const pri = priorRows.find((r: { tier: string }) => r.tier === tier);
    const count = cur ? Number(cur.cnt) : 0;
    return {
      tier,
      count,
      pct: total > 0 ? Math.round((count / total) * 1000) / 10 : 0,
      priorCount: pri ? Number(pri.cnt) : 0,
    };
  });
}

/**
 * Get week annotations.
 */
export async function getAnnotations(weeks = 12): Promise<{ week: string; label: string }[]> {
  const pool = getPool();
  const startWeek = weeksAgo(weeks);
  const { rows } = await pool.query(`
    SELECT week_start AS week, label FROM week_annotations
    WHERE week_start >= $1::date
    ORDER BY week_start
  `, [startWeek]);
  return rows as { week: string; label: string }[];
}

/**
 * Create a week annotation.
 */
export async function createAnnotation(weekStart: string, label: string): Promise<{ id: number; weekStart: string; label: string }> {
  const pool = getPool();
  const { rows } = await pool.query(`
    INSERT INTO week_annotations (week_start, label)
    VALUES ($1::date, $2)
    ON CONFLICT (week_start) DO UPDATE SET label = EXCLUDED.label
    RETURNING id, week_start, label
  `, [weekStart, label]);
  return { id: rows[0].id, weekStart: rows[0].week_start, label: rows[0].label };
}


// ── Sky3 Distribution (redesigned page) ─────────────────────

export interface Sky3BandData {
  count: number;
  pct: number;
}

export interface Sky3CohortInfo {
  stable_count: number;
  excluded: { new_joins: number; paused: number; pending_cancel: number };
}

export interface Sky3DistributionResponse {
  periodDays: number;
  cohort: Sky3CohortInfo;
  current: Record<string, Sky3BandData>;
  takeaway: { trend: string; text: string };
  total: number;
}

/**
 * Get the stable Sky3 cohort — members subscribed in both periods,
 * excluding paused, pending-cancel, and new joins.
 */
async function getStableSky3Cohort(
  pool: import("pg").Pool,
  periodStart: string,
  periodWeeks: number
): Promise<{ stableEmails: Set<string>; cohort: Sky3CohortInfo }> {
  // All Sky3 emails with their state
  const { rows: activeRows } = await pool.query(`
    SELECT DISTINCT ON (LOWER(customer_email))
      LOWER(customer_email) AS email, plan_state, created_at
    FROM auto_renews
    WHERE plan_state IN ('Valid Now', 'Paused', 'Pending Cancel')
      AND plan_category = 'SKY3'
      AND customer_email IS NOT NULL
    ORDER BY LOWER(customer_email), id DESC
  `);

  const pausedEmails = new Set<string>();
  const pendingCancelEmails = new Set<string>();
  const allActiveEmails = new Set<string>();
  const newJoinEmails = new Set<string>();

  for (const r of activeRows) {
    const email = r.email as string;
    allActiveEmails.add(email);
    if (r.plan_state === "Paused") pausedEmails.add(email);
    if (r.plan_state === "Pending Cancel") pendingCancelEmails.add(email);
    if (r.created_at && new Date(r.created_at as string) >= new Date(periodStart)) {
      newJoinEmails.add(email);
    }
  }

  // Stable = active but not paused, not pending cancel, not new join
  const stableEmails = new Set<string>();
  for (const email of allActiveEmails) {
    if (!pausedEmails.has(email) && !pendingCancelEmails.has(email) && !newJoinEmails.has(email)) {
      stableEmails.add(email);
    }
  }

  return {
    stableEmails,
    cohort: {
      stable_count: stableEmails.size,
      excluded: {
        new_joins: newJoinEmails.size,
        paused: pausedEmails.size,
        pending_cancel: pendingCancelEmails.size,
      },
    },
  };
}

/**
 * Get Sky3 distribution data — cohort-filtered current bands and takeaway.
 * Movement data is separate (getSky3Movement).
 */
export async function getSky3Distribution(periodWeeks = 4): Promise<Sky3DistributionResponse> {
  const pool = getPool();
  const periodDays = periodWeeks * 7;
  const periodStart = weeksAgo(periodWeeks);

  // Get stable cohort
  const { stableEmails, cohort } = await getStableSky3Cohort(pool, periodStart, periodWeeks);

  // Get current period tiers for stable cohort only
  const allTiers = await getPeriodTiers(pool, ["sky3"], periodStart, periodWeeks);
  const stableTiers = allTiers.filter(r => stableEmails.has(r.member_email));
  const total = stableTiers.length;

  // Build band data
  const current: Record<string, Sky3BandData> = {};
  const tierCounts: Record<string, number> = {};
  for (const r of stableTiers) {
    tierCounts[r.tier] = (tierCounts[r.tier] || 0) + 1;
  }
  for (const band of SKY3_TIER_ORDER) {
    const count = tierCounts[band] || 0;
    current[band] = { count, pct: total > 0 ? Math.round((count / total) * 1000) / 10 : 0 };
  }

  // Takeaway from movement data
  const movement = await getSky3Movement(periodWeeks);
  const { improving, declining } = movement;

  // Takeaway scoring based on movement directions
  let score = 0;
  const leavingZero = improving.transitions.filter(t => t.from === "not_using").reduce((s, t) => s + t.count, 0);
  const enteringZero = declining.transitions.filter(t => t.to === "not_using").reduce((s, t) => s + t.count, 0);
  if (leavingZero > enteringZero) score += 3;
  else if (enteringZero > leavingZero) score -= 3;

  const leavingOne = improving.transitions.filter(t => t.from === "barely_using").reduce((s, t) => s + t.count, 0);
  const enteringOne = declining.transitions.filter(t => t.to === "barely_using").reduce((s, t) => s + t.count, 0);
  if (leavingOne > enteringOne) score += 1;
  else if (enteringOne > leavingOne) score -= 1;

  const entering2Plus = improving.transitions.filter(t => SKY3_TIER_ORDER.indexOf(t.to) >= 2).reduce((s, t) => s + t.count, 0);
  const leaving2Plus = declining.transitions.filter(t => SKY3_TIER_ORDER.indexOf(t.from) >= 2).reduce((s, t) => s + t.count, 0);
  if (entering2Plus > leaving2Plus) score += 2;
  else if (leaving2Plus > entering2Plus) score -= 2;

  let trend: string;
  let text: string;
  if (score >= 3) {
    trend = "improving";
    text = `Usage is improving: ${improving.count} members moved to a higher usage band, shrinking the 0-visit group.`;
  } else if (score > 0) {
    trend = "slightly_improving";
    text = `Usage is trending up: ${improving.count} members moved to a higher usage band.`;
  } else if (score <= -3) {
    trend = "declining";
    text = `Usage needs attention: ${declining.count} members dropped to a lower usage band.`;
  } else if (score < 0) {
    trend = "slightly_declining";
    text = `Usage dipped slightly: ${declining.count} members dropped to a lower usage band.`;
  } else {
    trend = "steady";
    text = "Usage is holding steady \u2014 no major shifts this period.";
  }

  return { periodDays, cohort, current, takeaway: { trend, text }, total };
}

// ── Sky3 Movement ──────────────────────────────────────────

export interface Sky3Transition {
  from: string;
  to: string;
  count: number;
}

export interface Sky3MovementResponse {
  period_days: number;
  improving: { count: number; transitions: Sky3Transition[] };
  stable: { count: number; by_band: Record<string, number> };
  declining: { count: number; transitions: Sky3Transition[] };
  cohort_size: number;
}

/**
 * Compare each stable cohort member's current band to their prior band.
 */
export async function getSky3Movement(periodWeeks = 4): Promise<Sky3MovementResponse> {
  const pool = getPool();
  const periodStart = weeksAgo(periodWeeks);
  const priorPeriodStart = weeksAgo(periodWeeks * 2);

  const { stableEmails } = await getStableSky3Cohort(pool, periodStart, periodWeeks);

  const currentTiers = await getPeriodTiers(pool, ["sky3"], periodStart, periodWeeks);
  const priorTiers = await getPeriodTiers(pool, ["sky3"], priorPeriodStart, periodWeeks);

  const currentMap = new Map<string, string>();
  const priorMap = new Map<string, string>();
  for (const r of currentTiers) {
    if (stableEmails.has(r.member_email)) currentMap.set(r.member_email, r.tier);
  }
  for (const r of priorTiers) {
    if (stableEmails.has(r.member_email)) priorMap.set(r.member_email, r.tier);
  }

  const improvingTransitions: Record<string, number> = {};
  const decliningTransitions: Record<string, number> = {};
  const stableBands: Record<string, number> = {};
  let improvingCount = 0;
  let stableCount = 0;
  let decliningCount = 0;

  for (const email of stableEmails) {
    const curBand = currentMap.get(email) || "not_using";
    const priBand = priorMap.get(email) || "not_using";
    const curIdx = SKY3_TIER_ORDER.indexOf(curBand);
    const priIdx = SKY3_TIER_ORDER.indexOf(priBand);

    if (curIdx > priIdx) {
      improvingCount++;
      const key = `${priBand}|${curBand}`;
      improvingTransitions[key] = (improvingTransitions[key] || 0) + 1;
    } else if (curIdx < priIdx) {
      decliningCount++;
      const key = `${priBand}|${curBand}`;
      decliningTransitions[key] = (decliningTransitions[key] || 0) + 1;
    } else {
      stableCount++;
      stableBands[curBand] = (stableBands[curBand] || 0) + 1;
    }
  }

  const toTransitions = (map: Record<string, number>): Sky3Transition[] =>
    Object.entries(map)
      .map(([key, count]) => {
        const [from, to] = key.split("|");
        return { from, to, count };
      })
      .sort((a, b) => b.count - a.count);

  return {
    period_days: periodWeeks * 7,
    improving: { count: improvingCount, transitions: toTransitions(improvingTransitions) },
    stable: { count: stableCount, by_band: stableBands },
    declining: { count: decliningCount, transitions: toTransitions(decliningTransitions) },
    cohort_size: stableEmails.size,
  };
}

/**
 * Get Sky3 movement members for detail slide-out.
 */
export async function getSky3MovementMembers(params: {
  direction: "improving" | "stable" | "declining";
  periodWeeks?: number;
  from?: string;
  to?: string;
  fieldsOnly?: "email";
  page?: number;
  perPage?: number;
}): Promise<{
  members: { name: string; email: string; prior_band: string; current_band: string; prior_visits: number; current_visits: number }[];
  total: number;
  page: number;
}> {
  const pool = getPool();
  const { direction, periodWeeks = 4, from, to, fieldsOnly, page = 1, perPage = 25 } = params;
  const periodStart = weeksAgo(periodWeeks);
  const priorPeriodStart = weeksAgo(periodWeeks * 2);

  const { stableEmails } = await getStableSky3Cohort(pool, periodStart, periodWeeks);
  const currentTiers = await getPeriodTiers(pool, ["sky3"], periodStart, periodWeeks);
  const priorTiers = await getPeriodTiers(pool, ["sky3"], priorPeriodStart, periodWeeks);

  const currentMap = new Map<string, { tier: string; visits: number }>();
  const priorMap = new Map<string, { tier: string; visits: number }>();
  for (const r of currentTiers) {
    if (stableEmails.has(r.member_email)) currentMap.set(r.member_email, { tier: r.tier, visits: r.total_visits });
  }
  for (const r of priorTiers) {
    if (stableEmails.has(r.member_email)) priorMap.set(r.member_email, { tier: r.tier, visits: r.total_visits });
  }

  const matched: { email: string; priorBand: string; currentBand: string; priorVisits: number; currentVisits: number }[] = [];
  for (const email of stableEmails) {
    const cur = currentMap.get(email) || { tier: "not_using", visits: 0 };
    const pri = priorMap.get(email) || { tier: "not_using", visits: 0 };
    const curIdx = SKY3_TIER_ORDER.indexOf(cur.tier);
    const priIdx = SKY3_TIER_ORDER.indexOf(pri.tier);

    let dir: string;
    if (curIdx > priIdx) dir = "improving";
    else if (curIdx < priIdx) dir = "declining";
    else dir = "stable";

    if (dir !== direction) continue;
    if (from && pri.tier !== from) continue;
    if (to && cur.tier !== to) continue;

    matched.push({ email, priorBand: pri.tier, currentBand: cur.tier, priorVisits: pri.visits, currentVisits: cur.visits });
  }

  const total = matched.length;

  if (fieldsOnly === "email") {
    return {
      members: matched.map(m => ({ name: "", email: m.email, prior_band: m.priorBand, current_band: m.currentBand, prior_visits: m.priorVisits, current_visits: m.currentVisits })),
      total,
      page: 1,
    };
  }

  const offset = (page - 1) * perPage;
  const pageItems = matched.slice(offset, offset + perPage);
  if (pageItems.length === 0) return { members: [], total, page };

  const emails = pageItems.map(m => m.email);
  const { rows: nameRows } = await pool.query(`
    SELECT DISTINCT ON (LOWER(customer_email))
      LOWER(customer_email) AS email,
      customer_name AS name
    FROM auto_renews
    WHERE LOWER(customer_email) = ANY($1)
    ORDER BY LOWER(customer_email), id DESC
  `, [emails]);

  const nameMap = new Map<string, string>();
  for (const nr of nameRows) nameMap.set(nr.email as string, nr.name as string);

  return {
    members: pageItems.map(m => ({
      name: nameMap.get(m.email) || m.email,
      email: m.email,
      prior_band: m.priorBand,
      current_band: m.currentBand,
      prior_visits: m.priorVisits,
      current_visits: m.currentVisits,
    })),
    total,
    page,
  };
}

/**
 * Get Sky3 members by band (for side panel).
 * Computes band from visit_count at query time.
 */
export async function getSky3MembersByBand(params: {
  band: string;
  periodWeeks?: number;
  fieldsOnly?: "email";
  page?: number;
  perPage?: number;
}): Promise<{
  members: { name: string; email: string }[];
  total: number;
  page: number;
}> {
  const pool = getPool();
  const { band, periodWeeks = 4, fieldsOnly, page = 1, perPage = 25 } = params;
  const periodStart = weeksAgo(periodWeeks);

  // Get all Sky3 members with their aggregated visits in the period
  const rows = await getPeriodTiers(pool, ["sky3"], periodStart, periodWeeks);

  // Filter by band
  const bandMembers = rows.filter(r => r.tier === band);
  const total = bandMembers.length;

  // Get names for paginated subset
  const offset = (page - 1) * perPage;
  const pageEmails = bandMembers.slice(offset, offset + perPage).map(r => r.member_email);

  if (fieldsOnly === "email") {
    // Return all emails (for Copy All Emails)
    return {
      members: bandMembers.map(r => ({ name: "", email: r.member_email })),
      total,
      page: 1,
    };
  }

  // Fetch names
  if (pageEmails.length === 0) return { members: [], total, page };

  const { rows: nameRows } = await pool.query(`
    SELECT DISTINCT ON (LOWER(customer_email))
      LOWER(customer_email) AS email,
      customer_name AS name
    FROM auto_renews
    WHERE LOWER(customer_email) = ANY($1)
    ORDER BY LOWER(customer_email), id DESC
  `, [pageEmails]);

  const nameMap = new Map<string, string>();
  for (const nr of nameRows) nameMap.set(nr.email as string, nr.name as string);

  return {
    members: pageEmails.map(em => ({
      name: nameMap.get(em) || em,
      email: em,
    })),
    total,
    page,
  };
}
