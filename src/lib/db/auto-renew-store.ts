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

      const result = await client.query(
        `INSERT INTO auto_renews (
          snapshot_id, plan_name, plan_state, plan_price,
          customer_name, customer_email, created_at, order_id, sales_channel,
          canceled_at, canceled_by, admin, current_state, current_plan,
          union_pass_id
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
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
          imported_at = NOW()`,
        [
          snapshotId,
          row.planName,
          row.planState,
          row.planPrice,
          row.customerName,
          row.customerEmail,
          row.createdAt,
          row.orderId || null,
          row.salesChannel || null,
          row.canceledAt || null,
          row.canceledBy || null,
          row.admin || null,
          row.currentState || null,
          row.currentPlan || null,
          row.unionPassId || null,
        ]
      );

      // xmax = 0 means the row was inserted (not updated)
      // This is a PostgreSQL-specific way to tell INSERT from UPDATE in an UPSERT
      if (result.rowCount === 1) {
        // We can't easily distinguish insert vs update without a RETURNING trick,
        // but we can count total affected rows
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
  const price = raw.plan_price || 0;

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
 * Union.fit "Active" = Valid Now + Pending Cancel + Past Due + In Trial + Paused.
 * Excludes only: 'Canceled', 'Invalid'
 *
 * Paused members are included because Union.fit counts them as active subscribers
 * (they still have an active commitment and will resume). Excluding them caused
 * the dashboard to under-count members vs Union's own reports (~380 vs ~424).
 */
export async function getActiveAutoRenews(): Promise<StoredAutoRenew[]> {
  const pool = getPool();
  const { rows } = await pool.query(
    `SELECT id, snapshot_id, plan_name, plan_state, plan_price,
            customer_name, customer_email, created_at, canceled_at
     FROM auto_renews
     WHERE plan_state NOT IN ('Canceled', 'Invalid')
     ORDER BY plan_name`
  );

  return (rows as RawAutoRenewRow[]).map(mapRow);
}

/**
 * Get auto-renews created within a date range (new auto-renews).
 */
export async function getNewAutoRenews(startDate: string, endDate: string): Promise<StoredAutoRenew[]> {
  const pool = getPool();
  const { rows } = await pool.query(
    `SELECT id, snapshot_id, plan_name, plan_state, plan_price,
            customer_name, customer_email, created_at, canceled_at
     FROM auto_renews
     WHERE created_at >= $1 AND created_at < $2
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
  const { rows } = await pool.query(
    `SELECT id, snapshot_id, plan_name, plan_state, plan_price,
            customer_name, customer_email, created_at, canceled_at
     FROM auto_renews
     WHERE canceled_at IS NOT NULL AND canceled_at >= $1 AND canceled_at < $2
     ORDER BY canceled_at`,
    [startDate, endDate]
  );

  return (rows as RawAutoRenewRow[]).map(mapRow);
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

  const mrr = { member: 0, sky3: 0, skyTingTv: 0, unknown: 0 };

  for (const ar of active) {
    const key = ar.category === "MEMBER" ? "member"
      : ar.category === "SKY3" ? "sky3"
      : ar.category === "SKY_TING_TV" ? "skyTingTv"
      : "unknown";
    mrr[key] += ar.monthlyRate;
  }

  // Round MRR values
  mrr.member = Math.round(mrr.member * 100) / 100;
  mrr.sky3 = Math.round(mrr.sky3 * 100) / 100;
  mrr.skyTingTv = Math.round(mrr.skyTingTv * 100) / 100;
  mrr.unknown = Math.round(mrr.unknown * 100) / 100;

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
