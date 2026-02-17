import { getDatabase } from "./database";

// ── Types ────────────────────────────────────────────────────

export interface OrderRow {
  created: string;
  code: string;
  customer: string;
  type: string;
  payment: string;
  total: number;
}

export interface StoredOrder {
  id: number;
  createdAt: string;
  code: string;
  customer: string;
  orderType: string;
  payment: string;
  total: number;
}

export interface OrderStats {
  /** Total orders in the database */
  totalOrders: number;
  /** Total revenue across all orders */
  totalRevenue: number;
  /** Revenue by order type */
  revenueByType: { type: string; revenue: number; count: number }[];
  /** Current month order count */
  currentMonthOrders: number;
  /** Current month revenue */
  currentMonthRevenue: number;
  /** Previous month revenue */
  previousMonthRevenue: number;
}

// ── Write Operations ─────────────────────────────────────────

/**
 * Save orders (additive — appends new rows, never deletes existing).
 */
export function saveOrders(rows: OrderRow[]): void {
  const db = getDatabase();
  const before = (db.prepare("SELECT COUNT(*) as count FROM orders").get() as { count: number }).count;

  const insert = db.prepare(`
    INSERT OR IGNORE INTO orders (created_at, code, customer, order_type, payment, total)
    VALUES (?, ?, ?, ?, ?, ?)
  `);

  const insertMany = db.transaction((items: OrderRow[]) => {
    for (const r of items) {
      insert.run(r.created, r.code, r.customer, r.type, r.payment, r.total);
    }
  });

  insertMany(rows);
  const after = (db.prepare("SELECT COUNT(*) as count FROM orders").get() as { count: number }).count;
  console.log(`[order-store] Orders: ${before} -> ${after} (+${after - before} new)`);
}

// ── Read Operations ──────────────────────────────────────────

/**
 * Get all orders within a date range.
 */
export function getOrders(startDate?: string, endDate?: string): StoredOrder[] {
  const db = getDatabase();
  let query = `
    SELECT id, created_at, code, customer, order_type, payment, total
    FROM orders
    WHERE created_at IS NOT NULL AND created_at != ''
  `;
  const params: string[] = [];

  if (startDate) {
    query += ` AND created_at >= ?`;
    params.push(startDate);
  }
  if (endDate) {
    query += ` AND created_at < ?`;
    params.push(endDate);
  }

  query += ` ORDER BY created_at DESC`;

  const rows = db.prepare(query).all(...params) as {
    id: number; created_at: string; code: string; customer: string;
    order_type: string; payment: string; total: number;
  }[];

  return rows.map((r) => ({
    id: r.id,
    createdAt: r.created_at,
    code: r.code,
    customer: r.customer,
    orderType: r.order_type,
    payment: r.payment,
    total: r.total,
  }));
}

/**
 * Get revenue grouped by order type.
 */
export function getRevenueByType(startDate?: string, endDate?: string): { type: string; revenue: number; count: number }[] {
  const db = getDatabase();
  let query = `
    SELECT order_type, SUM(total) as revenue, COUNT(*) as count
    FROM orders
    WHERE created_at IS NOT NULL AND created_at != ''
  `;
  const params: string[] = [];

  if (startDate) {
    query += ` AND created_at >= ?`;
    params.push(startDate);
  }
  if (endDate) {
    query += ` AND created_at < ?`;
    params.push(endDate);
  }

  query += ` GROUP BY order_type ORDER BY revenue DESC`;

  return db.prepare(query).all(...params) as { type: string; revenue: number; count: number }[];
}

/**
 * Get aggregate order stats.
 */
export function getOrderStats(): OrderStats | null {
  const db = getDatabase();
  const now = new Date();
  const currentMonthStart = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, "0")}-01`;
  const prevMonthDate = new Date(now.getFullYear(), now.getMonth() - 1, 1);
  const prevMonthStart = `${prevMonthDate.getFullYear()}-${String(prevMonthDate.getMonth() + 1).padStart(2, "0")}-01`;

  const totalRow = db.prepare(
    `SELECT COUNT(*) as count, SUM(total) as revenue FROM orders`
  ).get() as { count: number; revenue: number };

  if (totalRow.count === 0) return null;

  const currentRow = db.prepare(
    `SELECT COUNT(*) as count, COALESCE(SUM(total), 0) as revenue FROM orders WHERE created_at >= ?`
  ).get(currentMonthStart) as { count: number; revenue: number };

  const prevRow = db.prepare(
    `SELECT COALESCE(SUM(total), 0) as revenue FROM orders WHERE created_at >= ? AND created_at < ?`
  ).get(prevMonthStart, currentMonthStart) as { revenue: number };

  const revenueByType = getRevenueByType();

  return {
    totalOrders: totalRow.count,
    totalRevenue: Math.round((totalRow.revenue || 0) * 100) / 100,
    revenueByType,
    currentMonthOrders: currentRow.count,
    currentMonthRevenue: Math.round(currentRow.revenue * 100) / 100,
    previousMonthRevenue: Math.round(prevRow.revenue * 100) / 100,
  };
}

/**
 * Check if order data exists.
 */
export function hasOrderData(): boolean {
  const db = getDatabase();
  const row = db.prepare(`SELECT COUNT(*) as count FROM orders`).get() as { count: number };
  return row.count > 0;
}
