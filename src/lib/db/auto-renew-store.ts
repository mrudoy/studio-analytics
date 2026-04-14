import { getPool } from "./database";
import { getCategory, isAnnualPlan } from "../analytics/categories";
import type { AutoRenewCategory } from "@/types/union-data";

// ── Types ────────────────────────────────────────────────────

export interface AutoRenewRow {
  planName: string;
  planState: string;
  planPrice: number;
  customerName: string;
  customerEmail: string;
  createdAt: string;
  orderId?: string;
  salesChannel?: string;
  canceledAt?: string;
  canceledBy?: string;
  admin?: string;
  currentState?: string;
  currentPlan?: string;
  /** Union.fit pass ID from raw export (for precise dedup) */
  unionPassId?: string;
}

export interface StoredAutoRenew {
  id: number;
  snapshotId: string | null;
  planName: string;
  planState: string;
  planPrice: number;
  customerName: string;
  customerEmail: string;
  createdAt: string;
  canceledAt: string | null;
  category: AutoRenewCategory;
  isAnnual: boolean;
  /** Monthly rate: price / 12 for annual plans, price for monthly */
  monthlyRate: number;
}

export interface AutoRenewStats {
  active: {
    member: number;
    sky3: number;
    skyTingTv: number;
    unknown: number;
    total: number;
  };
  mrr: {
    member: number;
    sky3: number;
    skyTingTv: number;
    unknown: number;
    total: number;
  };
  arpu: {
    member: number;
    sky3: number;
    skyTingTv: number;
    overall: number;
  };
}

// ── Write Operations ─────────────────────────────────────────

/**
 * Save a batch of auto-renews using UPSERT (INSERT ... ON CONFLICT DO UPDATE).
 *
 * Dedup key: (customer_email, plan_name, created_at) — from migration 003.
 * On conflict, mutable fields are updated (plan_state, plan_price, canceled_at, etc.)
 * so the database always reflects the latest known state while preserving history.
 *
 * This replaces the old DELETE + INSERT approach that destroyed historical records.
 */
export async function saveAutoRenews(
  snapshotId: string,
  rows: AutoRenewRow[]
): Promise<{ inserted: number; updated: number }> {
  const pool = getPool();
  const client = await pool.connect();
  let inserted = 0;
  let updated = 0;

  try {
    await client.query("BEGIN");

    for (const row of rows) {
      // Skip rows without email — they can't be deduped
      if (!row.customerEmail) continue;

      const values = [
        snapshotId,
        row.planName,
        row.planState,
        row.planPrice,
        row.customerName,
        row.customerEmail.toLowerCase(),
        row.createdAt || null,
        row.orderId || null,
        row.salesChannel || null,
        row.canceledAt || null,
        row.canceledBy || null,
        row.admin || null,
        row.currentState || null,
        row.currentPlan || null,
        row.unionPassId || null,
        getCategory(row.planName),
      ];

      // If union_pass_id is provided, try UPDATE-by-id first. This handles
      // the case where the same subscription arrives with a different business
      // key (email/plan_name/created_at drift), which would otherwise collide
      // on the partial unique index idx_ar_union_pass_id.
      //
      // This replaces the old DELETE-then-INSERT workaround, which violated
      // the permanent NEVER DELETE DATA rule. Same pattern as registration-store.ts.
      if (row.unionPassId) {
        const updated = await client.query(
          `UPDATE auto_renews SET
             snapshot_id = $1,
             plan_name = $2,
             plan_state = $3,
             plan_price = $4,
             customer_name = $5,
             customer_email = $6,
             created_at = COALESCE($7, created_at),
             order_id = COALESCE($8, order_id),
             sales_channel = COALESCE($9, sales_channel),
             canceled_at = COALESCE($10, canceled_at),
             canceled_by = COALESCE($11, canceled_by),
             admin = COALESCE($12, admin),
             current_state = COALESCE($13, current_state),
             current_plan = COALESCE($14, current_plan),
             plan_category = $16,
             imported_at = NOW()
           WHERE union_pass_id = $15`,
          values
        );
        if (updated.rowCount && updated.rowCount > 0) {
          inserted += updated.rowCount;
          continue;
        }
      }

      const result = await client.query(
        `INSERT INTO auto_renews (
          snapshot_id, plan_name, plan_state, plan_price,
          customer_name, customer_email, created_at, order_id, sales_channel,
          canceled_at, canceled_by, admin, current_state, current_plan,
          union_pass_id, plan_category
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
        ON CONFLICT (customer_email, plan_name, created_at)
        DO UPDATE SET
          snapshot_id = EXCLUDED.snapshot_id,
          plan_state = EXCLUDED.plan_state,
          plan_price = EXCLUDED.plan_price,
          customer_name = EXCLUDED.customer_name,
          order_id = COALESCE(EXCLUDED.order_id, auto_renews.order_id),
          sales_channel = COALESCE(EXCLUDED.sales_channel, auto_renews.sales_channel),
          canceled_at = COALESCE(EXCLUDED.canceled_at, auto_renews.canceled_at),
          canceled_by = COALESCE(EXCLUDED.canceled_by, auto_renews.canceled_by),
          admin = COALESCE(EXCLUDED.admin, auto_renews.admin),
          current_state = COALESCE(EXCLUDED.current_state, auto_renews.current_state),
          current_plan = COALESCE(EXCLUDED.current_plan, auto_renews.current_plan),
          union_pass_id = COALESCE(EXCLUDED.union_pass_id, auto_renews.union_pass_id),
          plan_category = EXCLUDED.plan_category,
          imported_at = NOW()`,
        values
      );

      if (result.rowCount === 1) {
        inserted++;
      }
    }

    await client.query("COMMIT");
  } catch (e) {
    await client.query("ROLLBACK");
    throw e;
  } finally {
    client.release();
  }

  console.log(
    `[auto-renew-store] Upserted ${rows.length} auto-renews (snapshot: ${snapshotId}, ` +
    `${inserted} rows affected)`
  );
  return { inserted, updated };
}

/**
 * Reconcile active auto-renews against a fresh export.
 *
 * Compares DB active subscribers against the emails present in the current
 * export. Anyone active in DB but missing from the export has gone stale
 * (canceled, changed plan, etc.) — update their plan_state to 'Canceled'.
 *
 * This fixes ghost subscribers whose plan_state was never updated by the
 * daily export. Data is never deleted — only plan_state is updated.
 */
export async function reconcileAutoRenews(
  exportEmails: Set<string>
): Promise<{ reconciled: number; details: { email: string; planName: string; oldState: string }[] }> {
  const pool = getPool();

  // Get all currently "active" rows from DB
  const { rows } = await pool.query(
    `SELECT id, customer_email, plan_name, plan_state
     FROM auto_renews
     WHERE plan_state IN ('Valid Now', 'Paused', 'In Trial', 'Invalid', 'Pending Cancel', 'Past Due')`
  );

  // Find rows whose email is NOT in the export
  const toReconcile: { id: number; email: string; planName: string; oldState: string }[] = [];
  for (const row of rows) {
    const email = (row.customer_email as string).toLowerCase();
    if (!exportEmails.has(email)) {
      toReconcile.push({
        id: row.id as number,
        email,
        planName: row.plan_name as string,
        oldState: row.plan_state as string,
      });
    }
  }

  if (toReconcile.length === 0) {
    console.log("[auto-renew-store] Reconciliation: all DB active subscribers found in export");
    return { reconciled: 0, details: [] };
  }

  // Update stale rows: set plan_state to 'Canceled' and canceled_at to now
  const ids = toReconcile.map((r) => r.id);
  await pool.query(
    `UPDATE auto_renews
     SET plan_state = 'Canceled',
         canceled_at = COALESCE(canceled_at, NOW())
     WHERE id = ANY($1)`,
    [ids]
  );

  console.log(
    `[auto-renew-store] Reconciliation: marked ${toReconcile.length} stale subscribers as Canceled`
  );
  for (const r of toReconcile) {
    console.log(`  ${r.email} | ${r.planName} | was ${r.oldState}`);
  }

  return {
    reconciled: toReconcile.length,
    details: toReconcile.map((r) => ({ email: r.email, planName: r.planName, oldState: r.oldState })),
  };
}

// ── Read Operations ──────────────────────────────────────────

interface RawAutoRenewRow {
  id: number;
  snapshot_id: string | null;
  plan_name: string;
  plan_state: string;
  plan_price: number;
  customer_name: string;
  customer_email: string;
  created_at: string;
  canceled_at: string | null;
}

function mapRow(raw: RawAutoRenewRow): StoredAutoRenew {
  const name = raw.plan_name || "";
  const cat = getCategory(name);
  const annual = isAnnualPlan(name);
  const price = parseFloat(String(raw.plan_price)) || 0;

  return {
    id: raw.id,
    snapshotId: raw.snapshot_id,
    planName: name,
    planState: raw.plan_state || "",
    planPrice: price,
    customerName: raw.customer_name || "",
    customerEmail: raw.customer_email || "",
    createdAt: raw.created_at || "",
    canceledAt: raw.canceled_at,
    category: cat,
    isAnnual: annual,
    monthlyRate: annual ? Math.round((price / 12) * 100) / 100 : price,
  };
}

/**
 * Get all active auto-renews.
 *
 * States included:
 *   - Valid Now: actively billing
 *   - Paused: on hold but committed
 *   - In Trial: trial period
 *   - Invalid: used all passes (e.g. SKY3 used 3/3), still a subscriber
 *   - Pending Cancel: canceling next cycle, currently active
 *   - Past Due: payment failed, not formally canceled
 *
 * NOTE: canceled_at is NOT used as a filter. For active subscribers,
 * Union.fit sets canceled_at to the next billing/renewal date, NOT the
 * cancellation date. Using it as a guard would filter out nearly everyone.
 * Instead, stale subscribers are cleaned up by reconcileAutoRenews() which
 * compares against each fresh export.
 *
 * Downstream callers (getActiveCounts, getAutoRenewStats) deduplicate by
 * customer_email so duplicate rows from multiple imports don't inflate counts.
 */
export async function getActiveAutoRenews(): Promise<StoredAutoRenew[]> {
  const pool = getPool();
  // A person is active on a plan if ANY of their rows has current_state = 'active'.
  // Daily deltas create rows with plan_state='Valid Now' but NULL current_state
  // for people who are actually canceled — so plan_state alone is unreliable.
  // Full exports and subscription-changes CSVs set current_state reliably.
  //
  // Strategy:
  //   1. Find all person+plan combos that have current_state='active' in any row
  //   2. For people with NO current_state data at all, fall back to plan_state
  //   3. Return one row per person+plan (latest by id)
  const { rows } = await pool.query(
    `WITH active_signals AS (
       -- Person+plan combos with at least one current_state='active' row
       SELECT DISTINCT LOWER(customer_email) AS email, plan_name
       FROM auto_renews
       WHERE current_state = 'active'
     ),
     has_any_cs AS (
       -- Person+plan combos with any current_state set (to distinguish "no data" from "canceled")
       SELECT DISTINCT LOWER(customer_email) AS email, plan_name
       FROM auto_renews
       WHERE current_state IS NOT NULL
     ),
     latest_rows AS (
       SELECT DISTINCT ON (LOWER(customer_email), plan_name)
              id, snapshot_id, plan_name, plan_state, plan_price,
              customer_name, customer_email, created_at, canceled_at
       FROM auto_renews
       ORDER BY LOWER(customer_email), plan_name, id DESC
     )
     SELECT lr.id, lr.snapshot_id, lr.plan_name, lr.plan_state, lr.plan_price,
            lr.customer_name, lr.customer_email, lr.created_at, lr.canceled_at
     FROM latest_rows lr
     WHERE (
       -- Has current_state='active' in any row → active
       EXISTS (SELECT 1 FROM active_signals a WHERE a.email = LOWER(lr.customer_email) AND a.plan_name = lr.plan_name)
       OR (
         -- No current_state data at all → fall back to plan_state
         NOT EXISTS (SELECT 1 FROM has_any_cs h WHERE h.email = LOWER(lr.customer_email) AND h.plan_name = lr.plan_name)
         AND lr.plan_state IN ('Valid Now', 'Paused', 'In Trial', 'Invalid', 'Pending Cancel')
       )
     )
     ORDER BY lr.plan_name`
  );

  return (rows as RawAutoRenewRow[]).map(mapRow);
}

/**
 * Get auto-renews created within a date range (new auto-renews).
 */
export async function getNewAutoRenews(startDate: string, endDate: string): Promise<StoredAutoRenew[]> {
  const pool = getPool();
  // Only count subscriptions that are currently active (not historical canceled rows
  // that happen to have created_at in this range from daily delta imports).
  const { rows } = await pool.query(
    `SELECT id, snapshot_id, plan_name, plan_state, plan_price,
            customer_name, customer_email, created_at, canceled_at
     FROM auto_renews
     WHERE created_at >= $1 AND created_at < $2
       AND (current_state = 'active' OR (current_state IS NULL
            AND plan_state IN ('Valid Now', 'Paused', 'In Trial', 'Invalid', 'Pending Cancel', 'Past Due')))
     ORDER BY created_at`,
    [startDate, endDate]
  );

  return (rows as RawAutoRenewRow[]).map(mapRow);
}

/**
 * Get auto-renews canceled within a date range.
 */
export async function getCanceledAutoRenews(startDate: string, endDate: string): Promise<StoredAutoRenew[]> {
  const pool = getPool();
  // Only count rows where plan_state is actually 'Canceled'.
  // For active subscribers, canceled_at is the next billing date — NOT a cancellation.
  // Daily deltas import active rows with canceled_at in recent ranges; filtering by
  // plan_state='Canceled' ensures we only count real cancellations.
  const { rows } = await pool.query(
    `SELECT id, snapshot_id, plan_name, plan_state, plan_price,
            customer_name, customer_email, created_at, canceled_at
     FROM auto_renews
     WHERE canceled_at IS NOT NULL AND canceled_at >= $1 AND canceled_at < $2
       AND plan_state = 'Canceled'
     ORDER BY canceled_at`,
    [startDate, endDate]
  );

  return (rows as RawAutoRenewRow[]).map(mapRow);
}

export interface DailyMovementRow {
  date: string; // YYYY-MM-DD
  newMembers: number;
  newSky3: number;
  newTv: number;
  churnedMembers: number;
  churnedSky3: number;
  churnedTv: number;
}

/**
 * Daily new + churned subscriber counts per category for the last `days` days.
 */
export async function getDailySubscriberMovement(days = 7): Promise<DailyMovementRow[]> {
  const pool = getPool();
  const { rows } = await pool.query(`
    WITH dates AS (
      SELECT generate_series(
        (CURRENT_DATE - ($1 || ' days')::interval)::date,
        CURRENT_DATE,
        '1 day'
      )::date AS d
    ),
    new_by_day AS (
      SELECT created_at AS d,
        COUNT(*) FILTER (WHERE plan_category = 'MEMBER') AS new_members,
        COUNT(*) FILTER (WHERE plan_category = 'SKY3') AS new_sky3,
        COUNT(*) FILTER (WHERE plan_category = 'SKY_TING_TV') AS new_tv
      FROM auto_renews
      WHERE created_at IS NOT NULL
        AND created_at >= CURRENT_DATE - ($1 || ' days')::interval
        AND (current_state = 'active' OR (current_state IS NULL
             AND plan_state IN ('Valid Now', 'Paused', 'In Trial', 'Invalid', 'Pending Cancel', 'Past Due')))
      GROUP BY created_at
    ),
    churned_by_day AS (
      SELECT canceled_at AS d,
        COUNT(*) FILTER (WHERE plan_category = 'MEMBER') AS churned_members,
        COUNT(*) FILTER (WHERE plan_category = 'SKY3') AS churned_sky3,
        COUNT(*) FILTER (WHERE plan_category = 'SKY_TING_TV') AS churned_tv
      FROM auto_renews
      WHERE canceled_at IS NOT NULL
        AND canceled_at >= CURRENT_DATE - ($1 || ' days')::interval
        AND plan_state = 'Canceled'
      GROUP BY canceled_at
    )
    SELECT
      dates.d::text AS date,
      COALESCE(n.new_members, 0)::int AS new_members,
      COALESCE(n.new_sky3, 0)::int AS new_sky3,
      COALESCE(n.new_tv, 0)::int AS new_tv,
      COALESCE(c.churned_members, 0)::int AS churned_members,
      COALESCE(c.churned_sky3, 0)::int AS churned_sky3,
      COALESCE(c.churned_tv, 0)::int AS churned_tv
    FROM dates
    LEFT JOIN new_by_day n ON n.d = dates.d
    LEFT JOIN churned_by_day c ON c.d = dates.d
    ORDER BY dates.d
  `, [days]);

  return rows.map((r: Record<string, unknown>) => ({
    date: String(r.date).slice(0, 10),
    newMembers: Number(r.new_members),
    newSky3: Number(r.new_sky3),
    newTv: Number(r.new_tv),
    churnedMembers: Number(r.churned_members),
    churnedSky3: Number(r.churned_sky3),
    churnedTv: Number(r.churned_tv),
  }));
}

/**
 * Canonical active subscriber counts by category — SINGLE SOURCE OF TRUTH.
 *
 * Returns unique people (distinct emails) per category. A person with
 * subscriptions in multiple categories appears in each. Every section
 * of the dashboard (Growth, Usage, Churn, Overview) MUST use this
 * function for "X Active" numbers.
 */
export interface ActiveCounts {
  member: number;
  sky3: number;
  skyTingTv: number;
  unknown: number;
  total: number;
}

export async function getActiveCounts(): Promise<ActiveCounts> {
  const active = await getActiveAutoRenews();

  const emailSets = {
    member: new Set<string>(),
    sky3: new Set<string>(),
    skyTingTv: new Set<string>(),
    unknown: new Set<string>(),
  };

  for (const ar of active) {
    const email = ar.customerEmail.toLowerCase();
    if (!email) continue;
    const key = ar.category === "MEMBER" ? "member"
      : ar.category === "SKY3" ? "sky3"
      : ar.category === "SKY_TING_TV" ? "skyTingTv"
      : "unknown";
    emailSets[key].add(email);
  }

  const counts: ActiveCounts = {
    member: emailSets.member.size,
    sky3: emailSets.sky3.size,
    skyTingTv: emailSets.skyTingTv.size,
    unknown: emailSets.unknown.size,
    total: 0,
  };
  // Total = union of all emails (a person in both Member + TV counts once in total)
  const allEmails = new Set<string>();
  for (const s of Object.values(emailSets)) {
    for (const e of s) allEmails.add(e);
  }
  counts.total = allEmails.size;

  return counts;
}

/**
 * Compute aggregate auto-renew stats: active counts, MRR, ARPU by category.
 * This is the primary function the dashboard uses.
 *
 * Active counts use getActiveCounts() — unique people, not subscription rows.
 * MRR sums all subscription rows (a person with 2 plans contributes 2x MRR).
 */
export async function getAutoRenewStats(): Promise<AutoRenewStats | null> {
  const active = await getActiveAutoRenews();
  if (active.length === 0) return null;

  const counts = await getActiveCounts();

  // MRR reflects revenue being recognized per ASC 606 — only Valid Now and
  // Pending Cancel subscribers are actively billing. Paused, Invalid (passes
  // used up), and In Trial are "active" in the subscriber sense but don't
  // contribute to recognized revenue this month:
  //   - Paused: no money flowing (user paused their subscription)
  //   - Invalid: passes used up, revenue already recognized at purchase/use
  //   - In Trial: plan_price shows full post-trial rate, not trial rate
  // Excluding these keeps MRR aligned with actual cash being collected.
  const BILLING_STATES = new Set(["Valid Now", "Pending Cancel"]);

  // Deduplicate MRR by email per category — keep highest monthlyRate per person
  const mrrByEmail: Record<string, Map<string, number>> = {
    member: new Map(),
    sky3: new Map(),
    skyTingTv: new Map(),
    unknown: new Map(),
  };

  for (const ar of active) {
    if (!BILLING_STATES.has(ar.planState)) continue;
    const key = ar.category === "MEMBER" ? "member"
      : ar.category === "SKY3" ? "sky3"
      : ar.category === "SKY_TING_TV" ? "skyTingTv"
      : "unknown";
    const email = ar.customerEmail.toLowerCase();
    const existing = mrrByEmail[key].get(email) ?? 0;
    if (ar.monthlyRate > existing) {
      mrrByEmail[key].set(email, ar.monthlyRate);
    }
  }

  const mrr = { member: 0, sky3: 0, skyTingTv: 0, unknown: 0 };
  for (const [cat, emailMap] of Object.entries(mrrByEmail)) {
    let total = 0;
    for (const rate of emailMap.values()) total += rate;
    mrr[cat as keyof typeof mrr] = Math.round(total * 100) / 100;
  }

  const totalMRR = mrr.member + mrr.sky3 + mrr.skyTingTv + mrr.unknown;

  return {
    active: {
      ...counts,
    },
    mrr: {
      ...mrr,
      total: Math.round(totalMRR * 100) / 100,
    },
    arpu: {
      member: counts.member > 0 ? Math.round((mrr.member / counts.member) * 100) / 100 : 0,
      sky3: counts.sky3 > 0 ? Math.round((mrr.sky3 / counts.sky3) * 100) / 100 : 0,
      skyTingTv: counts.skyTingTv > 0 ? Math.round((mrr.skyTingTv / counts.skyTingTv) * 100) / 100 : 0,
      overall: counts.total > 0 ? Math.round((totalMRR / counts.total) * 100) / 100 : 0,
    },
  };
}

/**
 * Check if auto-renew data exists in the database.
 */
export async function hasAutoRenewData(): Promise<boolean> {
  const pool = getPool();
  const { rows } = await pool.query(
    `SELECT COUNT(*) as count FROM auto_renews`
  );
  return Number(rows[0].count) > 0;
}
