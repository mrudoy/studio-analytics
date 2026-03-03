import { NextResponse } from "next/server";
import { getPool } from "@/lib/db/database";

export async function GET() {
  try {
    const pool = getPool();

    // ── 1. Monthly revenue by sub-segment since Sept 2024 ──
    // Break In-Studio into: Membership, Sky3, Drop-In, Intro, Other In-Studio
    // Keep Digital separate
    const revenueRes = await pool.query(`
      WITH deduped AS (
        SELECT DISTINCT ON (category, LEFT(period_start, 7))
          category, revenue, net_revenue, period_start
        FROM revenue_categories
        WHERE LEFT(period_start, 7) = LEFT(period_end, 7)
          AND period_start >= '2024-09-01'
        ORDER BY category, LEFT(period_start, 7), period_end DESC
      )
      SELECT
        LEFT(period_start, 7) AS month,
        CASE
          -- Membership (Unlimited, All Access, etc.)
          WHEN category ~* 'sky\\s*unlimited|all\\s*access|10member|sky\\s*ting\\s*(monthly\\s*)?membership|ting\\s*fam|sky\\s*virgin|founding\\s*member|new\\s*member|back\\s*to\\s*school|secret\\s*membership|monthly\\s*membership'
            THEN 'Membership'
          -- Sky3 / 5-packs
          WHEN category ~* 'sky\\s*3|sky\\s*5|skyhigh|5[\\s-]*pack'
            THEN 'Sky3'
          -- Digital (TV)
          WHEN category ~* 'sky\\s*ting\\s*tv'
            OR category ~* '10skyting'
            OR category ~* 'digital\\s*all'
            OR category ~* 'a\\s*la\\s*carte\\s*sky\\s*ting'
            OR category ~* 'sky\\s*week\\s*tv'
            OR category ~* 'friends\\s*of\\s*sky\\s*ting'
            OR category ~* 'new\\s*subscriber\\s*special'
            OR category ~* 'limited\\s*edition\\s*sky\\s*ting'
            OR category ~* 'come\\s*back\\s*sky\\s*ting'
            OR category ~* 'retreat\\s*ting'
            THEN 'Digital'
          -- Drop-In
          WHEN category ~* 'drop[\\s-]*in|droplet'
            THEN 'DropIn'
          -- Intro
          WHEN category ~* 'intro\\s*week|unlimited\\s*week|sky\\s*virgin\\s*2\\s*week'
            THEN 'Intro'
          ELSE 'Other'
        END AS segment,
        SUM(revenue) AS gross,
        SUM(net_revenue) AS net
      FROM deduped
      GROUP BY month, segment
      ORDER BY month, segment
    `);

    // ── 2. Active subscriber snapshots ──
    // Current counts by plan type
    const subsRes = await pool.query(`
      SELECT
        CASE
          WHEN plan_name ILIKE '%sky3%' OR plan_name ILIKE '%sky5%'
            OR plan_name ILIKE '%5 pack%' OR plan_name ILIKE '%5-pack%'
            OR plan_name ILIKE '%skyhigh%'
            THEN 'Sky3'
          WHEN plan_name ILIKE '%unlimited%' OR plan_name ILIKE '%all access%'
            OR plan_name ILIKE '%membership%' OR plan_name ILIKE '%ting fam%'
            OR plan_name ILIKE '%founding%' OR plan_name ILIKE '%10member%'
            THEN 'Membership'
          WHEN plan_name ILIKE '%sky ting tv%' OR plan_name ILIKE '%digital%'
            OR plan_name ILIKE '%10skyting%' OR plan_name ILIKE '%a la carte%'
            OR plan_name ILIKE '%friends of sky%' OR plan_name ILIKE '%come back%'
            OR plan_name ILIKE '%retreat ting%' OR plan_name ILIKE '%sky week tv%'
            OR plan_name ILIKE '%new subscriber%' OR plan_name ILIKE '%limited edition%'
            THEN 'Digital'
          ELSE 'Other'
        END AS plan_type,
        plan_state,
        COUNT(*) AS cnt
      FROM auto_renews
      GROUP BY plan_type, plan_state
      ORDER BY plan_type, plan_state
    `);

    // ── 3. Monthly new subscriber acquisition since Sept 2024 ──
    // How many NEW subs started each month per plan type
    const newSubsRes = await pool.query(`
      WITH classified AS (
        SELECT
          LOWER(customer_email) AS email,
          CASE
            WHEN plan_name ILIKE '%sky3%' OR plan_name ILIKE '%sky5%'
              OR plan_name ILIKE '%5 pack%' OR plan_name ILIKE '%5-pack%'
              OR plan_name ILIKE '%skyhigh%'
              THEN 'Sky3'
            WHEN plan_name ILIKE '%unlimited%' OR plan_name ILIKE '%all access%'
              OR plan_name ILIKE '%membership%' OR plan_name ILIKE '%ting fam%'
              OR plan_name ILIKE '%founding%' OR plan_name ILIKE '%10member%'
              THEN 'Membership'
            WHEN plan_name ILIKE '%sky ting tv%' OR plan_name ILIKE '%digital%'
              OR plan_name ILIKE '%10skyting%' OR plan_name ILIKE '%a la carte%'
              OR plan_name ILIKE '%friends of sky%' OR plan_name ILIKE '%come back%'
              OR plan_name ILIKE '%retreat ting%' OR plan_name ILIKE '%sky week tv%'
              OR plan_name ILIKE '%new subscriber%' OR plan_name ILIKE '%limited edition%'
              THEN 'Digital'
            ELSE 'Other'
          END AS plan_type,
          created_at,
          plan_state
        FROM auto_renews
        WHERE created_at ~ '^\\d{4}-\\d{2}-\\d{2}'
          AND created_at >= '2024-09-01'
      )
      SELECT
        LEFT(created_at, 7) AS month,
        plan_type,
        COUNT(DISTINCT email) AS new_subs
      FROM classified
      WHERE plan_type IN ('Sky3', 'Membership', 'Digital')
      GROUP BY month, plan_type
      ORDER BY month, plan_type
    `);

    // ── 4. Monthly churns since Sept 2024 ──
    const churnsRes = await pool.query(`
      WITH classified AS (
        SELECT
          LOWER(customer_email) AS email,
          CASE
            WHEN plan_name ILIKE '%sky3%' OR plan_name ILIKE '%sky5%'
              OR plan_name ILIKE '%5 pack%' OR plan_name ILIKE '%5-pack%'
              OR plan_name ILIKE '%skyhigh%'
              THEN 'Sky3'
            WHEN plan_name ILIKE '%unlimited%' OR plan_name ILIKE '%all access%'
              OR plan_name ILIKE '%membership%' OR plan_name ILIKE '%ting fam%'
              OR plan_name ILIKE '%founding%' OR plan_name ILIKE '%10member%'
              THEN 'Membership'
            WHEN plan_name ILIKE '%sky ting tv%' OR plan_name ILIKE '%digital%'
              OR plan_name ILIKE '%10skyting%' OR plan_name ILIKE '%a la carte%'
              OR plan_name ILIKE '%friends of sky%' OR plan_name ILIKE '%come back%'
              OR plan_name ILIKE '%retreat ting%' OR plan_name ILIKE '%sky week tv%'
              OR plan_name ILIKE '%new subscriber%' OR plan_name ILIKE '%limited edition%'
              THEN 'Digital'
            ELSE 'Other'
          END AS plan_type,
          canceled_at
        FROM auto_renews
        WHERE plan_state IN ('Canceled', 'Invalid')
          AND canceled_at ~ '^\\d{4}-\\d{2}-\\d{2}'
          AND canceled_at >= '2024-09-01'
      )
      SELECT
        LEFT(canceled_at, 7) AS month,
        plan_type,
        COUNT(DISTINCT email) AS churns
      FROM classified
      WHERE plan_type IN ('Sky3', 'Membership', 'Digital')
      GROUP BY month, plan_type
      ORDER BY month, plan_type
    `);

    // ── 5. Average revenue per subscriber (ARPU) ──
    // Use latest full month revenue / active subscriber count
    const arpuRes = await pool.query(`
      WITH active_subs AS (
        SELECT
          CASE
            WHEN plan_name ILIKE '%sky3%' OR plan_name ILIKE '%sky5%'
              OR plan_name ILIKE '%5 pack%' OR plan_name ILIKE '%5-pack%'
              OR plan_name ILIKE '%skyhigh%'
              THEN 'Sky3'
            WHEN plan_name ILIKE '%unlimited%' OR plan_name ILIKE '%all access%'
              OR plan_name ILIKE '%membership%' OR plan_name ILIKE '%ting fam%'
              OR plan_name ILIKE '%founding%' OR plan_name ILIKE '%10member%'
              THEN 'Membership'
            WHEN plan_name ILIKE '%sky ting tv%' OR plan_name ILIKE '%digital%'
              OR plan_name ILIKE '%10skyting%' OR plan_name ILIKE '%a la carte%'
              OR plan_name ILIKE '%friends of sky%' OR plan_name ILIKE '%come back%'
              OR plan_name ILIKE '%retreat ting%' OR plan_name ILIKE '%sky week tv%'
              OR plan_name ILIKE '%new subscriber%' OR plan_name ILIKE '%limited edition%'
              THEN 'Digital'
            ELSE 'Other'
          END AS plan_type,
          COUNT(DISTINCT LOWER(customer_email)) AS active_count
        FROM auto_renews
        WHERE plan_state NOT IN ('Canceled', 'Invalid')
        GROUP BY plan_type
      )
      SELECT plan_type, active_count FROM active_subs
      WHERE plan_type IN ('Sky3', 'Membership', 'Digital')
    `);

    // ── 6. Full year 2025 revenue by these segments for baseline ──
    const baseline2025Res = await pool.query(`
      WITH deduped AS (
        SELECT DISTINCT ON (category, LEFT(period_start, 7))
          category, revenue, net_revenue, period_start
        FROM revenue_categories
        WHERE LEFT(period_start, 7) = LEFT(period_end, 7)
          AND period_start >= '2025-01-01' AND period_start < '2026-01-01'
        ORDER BY category, LEFT(period_start, 7), period_end DESC
      )
      SELECT
        CASE
          WHEN category ~* 'sky\\s*unlimited|all\\s*access|10member|sky\\s*ting\\s*(monthly\\s*)?membership|ting\\s*fam|sky\\s*virgin|founding\\s*member|new\\s*member|back\\s*to\\s*school|secret\\s*membership|monthly\\s*membership'
            THEN 'Membership'
          WHEN category ~* 'sky\\s*3|sky\\s*5|skyhigh|5[\\s-]*pack'
            THEN 'Sky3'
          WHEN category ~* 'sky\\s*ting\\s*tv'
            OR category ~* '10skyting'
            OR category ~* 'digital\\s*all'
            OR category ~* 'a\\s*la\\s*carte\\s*sky\\s*ting'
            OR category ~* 'sky\\s*week\\s*tv'
            OR category ~* 'friends\\s*of\\s*sky\\s*ting'
            OR category ~* 'new\\s*subscriber\\s*special'
            OR category ~* 'limited\\s*edition\\s*sky\\s*ting'
            OR category ~* 'come\\s*back\\s*sky\\s*ting'
            OR category ~* 'retreat\\s*ting'
            THEN 'Digital'
          ELSE 'Other'
        END AS segment,
        SUM(revenue) AS gross,
        SUM(net_revenue) AS net
      FROM deduped
      GROUP BY segment
      ORDER BY gross DESC
    `);

    // ── 7. Same for 2024 ──
    const baseline2024Res = await pool.query(`
      WITH deduped AS (
        SELECT DISTINCT ON (category, LEFT(period_start, 7))
          category, revenue, net_revenue, period_start
        FROM revenue_categories
        WHERE LEFT(period_start, 7) = LEFT(period_end, 7)
          AND period_start >= '2024-01-01' AND period_start < '2025-01-01'
        ORDER BY category, LEFT(period_start, 7), period_end DESC
      )
      SELECT
        CASE
          WHEN category ~* 'sky\\s*unlimited|all\\s*access|10member|sky\\s*ting\\s*(monthly\\s*)?membership|ting\\s*fam|sky\\s*virgin|founding\\s*member|new\\s*member|back\\s*to\\s*school|secret\\s*membership|monthly\\s*membership'
            THEN 'Membership'
          WHEN category ~* 'sky\\s*3|sky\\s*5|skyhigh|5[\\s-]*pack'
            THEN 'Sky3'
          WHEN category ~* 'sky\\s*ting\\s*tv'
            OR category ~* '10skyting'
            OR category ~* 'digital\\s*all'
            OR category ~* 'a\\s*la\\s*carte\\s*sky\\s*ting'
            OR category ~* 'sky\\s*week\\s*tv'
            OR category ~* 'friends\\s*of\\s*sky\\s*ting'
            OR category ~* 'new\\s*subscriber\\s*special'
            OR category ~* 'limited\\s*edition\\s*sky\\s*ting'
            OR category ~* 'come\\s*back\\s*sky\\s*ting'
            OR category ~* 'retreat\\s*ting'
            THEN 'Digital'
          ELSE 'Other'
        END AS segment,
        SUM(revenue) AS gross,
        SUM(net_revenue) AS net
      FROM deduped
      GROUP BY segment
      ORDER BY gross DESC
    `);

    return NextResponse.json({
      monthlyRevenue: revenueRes.rows,
      subscriberCounts: subsRes.rows,
      monthlyNewSubs: newSubsRes.rows,
      monthlyChurns: churnsRes.rows,
      arpu: arpuRes.rows,
      baseline2025: baseline2025Res.rows,
      baseline2024: baseline2024Res.rows,
    });
  } catch (err) {
    console.error("[growth-analysis]", err);
    return NextResponse.json({ error: String(err) }, { status: 500 });
  }
}
