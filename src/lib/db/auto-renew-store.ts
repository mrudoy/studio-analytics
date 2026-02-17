import { getDatabase } from "./database";
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
 * Save a batch of auto-renews from a CSV import.
 * Replaces all existing data (full snapshot).
 */
export function saveAutoRenews(
  snapshotId: string,
  rows: AutoRenewRow[]
): void {
  const db = getDatabase();

  const insert = db.prepare(`
    INSERT INTO auto_renews (
      snapshot_id, plan_name, plan_state, plan_price,
      customer_name, customer_email, created_at, order_id, sales_channel,
      canceled_at, canceled_by, admin, current_state, current_plan
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);

  const insertMany = db.transaction((items: AutoRenewRow[]) => {
    // Clear existing data for clean import
    db.exec("DELETE FROM auto_renews");

    for (const row of items) {
      insert.run(
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
        row.currentPlan || null
      );
    }
  });

  insertMany(rows);
  console.log(`[auto-renew-store] Saved ${rows.length} auto-renews (snapshot: ${snapshotId})`);
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
 * Get all active auto-renews (plan_state = 'Valid Now').
 */
export function getActiveAutoRenews(): StoredAutoRenew[] {
  const db = getDatabase();
  const rows = db.prepare(
    `SELECT id, snapshot_id, plan_name, plan_state, plan_price,
            customer_name, customer_email, created_at, canceled_at
     FROM auto_renews
     WHERE plan_state = 'Valid Now'
     ORDER BY plan_name`
  ).all() as RawAutoRenewRow[];

  return rows.map(mapRow);
}

/**
 * Get auto-renews created within a date range (new auto-renews).
 */
export function getNewAutoRenews(startDate: string, endDate: string): StoredAutoRenew[] {
  const db = getDatabase();
  const rows = db.prepare(
    `SELECT id, snapshot_id, plan_name, plan_state, plan_price,
            customer_name, customer_email, created_at, canceled_at
     FROM auto_renews
     WHERE created_at >= ? AND created_at < ?
     ORDER BY created_at`
  ).all(startDate, endDate) as RawAutoRenewRow[];

  return rows.map(mapRow);
}

/**
 * Get auto-renews canceled within a date range.
 */
export function getCanceledAutoRenews(startDate: string, endDate: string): StoredAutoRenew[] {
  const db = getDatabase();
  const rows = db.prepare(
    `SELECT id, snapshot_id, plan_name, plan_state, plan_price,
            customer_name, customer_email, created_at, canceled_at
     FROM auto_renews
     WHERE canceled_at IS NOT NULL AND canceled_at >= ? AND canceled_at < ?
     ORDER BY canceled_at`
  ).all(startDate, endDate) as RawAutoRenewRow[];

  return rows.map(mapRow);
}

/**
 * Get all auto-renews (any state).
 */
export function getAllAutoRenews(): StoredAutoRenew[] {
  const db = getDatabase();
  const rows = db.prepare(
    `SELECT id, snapshot_id, plan_name, plan_state, plan_price,
            customer_name, customer_email, created_at, canceled_at
     FROM auto_renews
     ORDER BY plan_name`
  ).all() as RawAutoRenewRow[];

  return rows.map(mapRow);
}

/**
 * Compute aggregate auto-renew stats: active counts, MRR, ARPU by category.
 * This is the primary function the dashboard uses.
 */
export function getAutoRenewStats(): AutoRenewStats | null {
  const active = getActiveAutoRenews();
  if (active.length === 0) return null;

  const counts = { member: 0, sky3: 0, skyTingTv: 0, unknown: 0 };
  const mrr = { member: 0, sky3: 0, skyTingTv: 0, unknown: 0 };

  for (const ar of active) {
    const key = ar.category === "MEMBER" ? "member"
      : ar.category === "SKY3" ? "sky3"
      : ar.category === "SKY_TING_TV" ? "skyTingTv"
      : "unknown";

    counts[key]++;
    mrr[key] += ar.monthlyRate;
  }

  // Round MRR values
  mrr.member = Math.round(mrr.member * 100) / 100;
  mrr.sky3 = Math.round(mrr.sky3 * 100) / 100;
  mrr.skyTingTv = Math.round(mrr.skyTingTv * 100) / 100;
  mrr.unknown = Math.round(mrr.unknown * 100) / 100;

  const totalActive = counts.member + counts.sky3 + counts.skyTingTv + counts.unknown;
  const totalMRR = mrr.member + mrr.sky3 + mrr.skyTingTv + mrr.unknown;

  return {
    active: {
      ...counts,
      total: totalActive,
    },
    mrr: {
      ...mrr,
      total: Math.round(totalMRR * 100) / 100,
    },
    arpu: {
      member: counts.member > 0 ? Math.round((mrr.member / counts.member) * 100) / 100 : 0,
      sky3: counts.sky3 > 0 ? Math.round((mrr.sky3 / counts.sky3) * 100) / 100 : 0,
      skyTingTv: counts.skyTingTv > 0 ? Math.round((mrr.skyTingTv / counts.skyTingTv) * 100) / 100 : 0,
      overall: totalActive > 0 ? Math.round((totalMRR / totalActive) * 100) / 100 : 0,
    },
  };
}

/**
 * Check if auto-renew data exists in the database.
 */
export function hasAutoRenewData(): boolean {
  const db = getDatabase();
  const row = db.prepare(
    `SELECT COUNT(*) as count FROM auto_renews`
  ).get() as { count: number };
  return row.count > 0;
}
