import { getDatabase } from "./database";
import { getCategory, isAnnualPlan } from "../analytics/categories";
import type { SubscriptionCategory } from "@/types/union-data";

// ── Types ────────────────────────────────────────────────────

export interface SubscriptionRow {
  subscriptionName: string;
  subscriptionState: string;
  subscriptionPrice: number;
  customerName: string;
  customerEmail: string;
  createdAt: string;
  orderId?: string;
  salesChannel?: string;
  canceledAt?: string;
  canceledBy?: string;
  admin?: string;
  currentState?: string;
  currentSubscription?: string;
}

export interface StoredSubscription {
  id: number;
  snapshotId: string | null;
  subscriptionName: string;
  subscriptionState: string;
  subscriptionPrice: number;
  customerName: string;
  customerEmail: string;
  createdAt: string;
  canceledAt: string | null;
  category: SubscriptionCategory;
  isAnnual: boolean;
  /** Monthly rate: price / 12 for annual plans, price for monthly */
  monthlyRate: number;
}

export interface SubscriptionStats {
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
 * Save a batch of subscriptions from a CSV import.
 * Replaces all existing data (full snapshot).
 */
export function saveSubscriptions(
  snapshotId: string,
  rows: SubscriptionRow[]
): void {
  const db = getDatabase();

  const insert = db.prepare(`
    INSERT INTO subscriptions (
      snapshot_id, subscription_name, subscription_state, subscription_price,
      customer_name, customer_email, created_at, order_id, sales_channel,
      canceled_at, canceled_by, admin, current_state, current_subscription
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);

  const insertMany = db.transaction((items: SubscriptionRow[]) => {
    // Clear existing data for clean import
    db.exec("DELETE FROM subscriptions");

    for (const row of items) {
      insert.run(
        snapshotId,
        row.subscriptionName,
        row.subscriptionState,
        row.subscriptionPrice,
        row.customerName,
        row.customerEmail,
        row.createdAt,
        row.orderId || null,
        row.salesChannel || null,
        row.canceledAt || null,
        row.canceledBy || null,
        row.admin || null,
        row.currentState || null,
        row.currentSubscription || null
      );
    }
  });

  insertMany(rows);
  console.log(`[subscription-store] Saved ${rows.length} subscriptions (snapshot: ${snapshotId})`);
}

// ── Read Operations ──────────────────────────────────────────

interface RawSubscriptionRow {
  id: number;
  snapshot_id: string | null;
  subscription_name: string;
  subscription_state: string;
  subscription_price: number;
  customer_name: string;
  customer_email: string;
  created_at: string;
  canceled_at: string | null;
}

function mapRow(raw: RawSubscriptionRow): StoredSubscription {
  const name = raw.subscription_name || "";
  const cat = getCategory(name);
  const annual = isAnnualPlan(name);
  const price = raw.subscription_price || 0;

  return {
    id: raw.id,
    snapshotId: raw.snapshot_id,
    subscriptionName: name,
    subscriptionState: raw.subscription_state || "",
    subscriptionPrice: price,
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
 * Get all active subscriptions (subscription_state = 'Valid Now').
 */
export function getActiveSubscriptions(): StoredSubscription[] {
  const db = getDatabase();
  const rows = db.prepare(
    `SELECT id, snapshot_id, subscription_name, subscription_state, subscription_price,
            customer_name, customer_email, created_at, canceled_at
     FROM subscriptions
     WHERE subscription_state = 'Valid Now'
     ORDER BY subscription_name`
  ).all() as RawSubscriptionRow[];

  return rows.map(mapRow);
}

/**
 * Get subscriptions created within a date range (new subscriptions).
 */
export function getNewSubscriptions(startDate: string, endDate: string): StoredSubscription[] {
  const db = getDatabase();
  const rows = db.prepare(
    `SELECT id, snapshot_id, subscription_name, subscription_state, subscription_price,
            customer_name, customer_email, created_at, canceled_at
     FROM subscriptions
     WHERE created_at >= ? AND created_at < ?
     ORDER BY created_at`
  ).all(startDate, endDate) as RawSubscriptionRow[];

  return rows.map(mapRow);
}

/**
 * Get subscriptions canceled within a date range.
 */
export function getCanceledSubscriptions(startDate: string, endDate: string): StoredSubscription[] {
  const db = getDatabase();
  const rows = db.prepare(
    `SELECT id, snapshot_id, subscription_name, subscription_state, subscription_price,
            customer_name, customer_email, created_at, canceled_at
     FROM subscriptions
     WHERE canceled_at IS NOT NULL AND canceled_at >= ? AND canceled_at < ?
     ORDER BY canceled_at`
  ).all(startDate, endDate) as RawSubscriptionRow[];

  return rows.map(mapRow);
}

/**
 * Get all subscriptions (any state).
 */
export function getAllSubscriptions(): StoredSubscription[] {
  const db = getDatabase();
  const rows = db.prepare(
    `SELECT id, snapshot_id, subscription_name, subscription_state, subscription_price,
            customer_name, customer_email, created_at, canceled_at
     FROM subscriptions
     ORDER BY subscription_name`
  ).all() as RawSubscriptionRow[];

  return rows.map(mapRow);
}

/**
 * Compute aggregate subscription stats: active counts, MRR, ARPU by category.
 * This is the primary function the dashboard uses.
 */
export function getSubscriptionStats(): SubscriptionStats | null {
  const active = getActiveSubscriptions();
  if (active.length === 0) return null;

  const counts = { member: 0, sky3: 0, skyTingTv: 0, unknown: 0 };
  const mrr = { member: 0, sky3: 0, skyTingTv: 0, unknown: 0 };

  for (const sub of active) {
    const key = sub.category === "MEMBER" ? "member"
      : sub.category === "SKY3" ? "sky3"
      : sub.category === "SKY_TING_TV" ? "skyTingTv"
      : "unknown";

    counts[key]++;
    mrr[key] += sub.monthlyRate;
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
 * Check if subscription data exists in the database.
 */
export function hasSubscriptionData(): boolean {
  const db = getDatabase();
  const row = db.prepare(
    `SELECT COUNT(*) as count FROM subscriptions`
  ).get() as { count: number };
  return row.count > 0;
}
