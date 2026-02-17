import { getPool } from "./database";

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
export async function saveOrders(rows: OrderRow[]): Promise<void> {
  const pool = getPool();

  const beforeResult = await pool.query("SELECT COUNT(*) as count FROM orders");
  const before = Number(beforeResult.rows[0].count);

  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    for (const r of rows) {
      await client.query(
        `INSERT INTO orders (created_at, code, customer, order_type, payment, total)
         VALUES ($1, $2, $3, $4, $5, $6)
         ON CONFLICT (code) DO NOTHING`,
        [r.created, r.code, r.customer, r.type, r.payment, r.total]
      );
    }
    await client.query("COMMIT");
  } catch (e) {
    await client.query("ROLLBACK");
    throw e;
  } finally {
    client.release();
  }

  const afterResult = await pool.query("SELECT COUNT(*) as count FROM orders");
  const after = Number(afterResult.rows[0].count);
  console.log(`[order-store] Orders: ${before} -> ${after} (+${after - before} new)`);
}

// ── Read Operations ──────────────────────────────────────────

/**
 * Get all orders within a date range.
 */
export async function getOrders(startDate?: string, endDate?: string): Promise<StoredOrder[]> {
  const pool = getPool();
  let query = `
    SELECT id, created_at, code, customer, order_type, payment, total
    FROM orders
    WHERE created_at IS NOT NULL AND created_at != ''
  `;
  const params: string[] = [];
  let paramIdx = 1;

  if (startDate) {
    query += ` AND created_at >= $${paramIdx++}`;
    params.push(startDate);
  }
  if (endDate) {
    query += ` AND created_at < $${paramIdx++}`;
    params.push(endDate);
  }

  query += ` ORDER BY created_at DESC`;

  const { rows } = await pool.query(query, params);

  return rows.map((r: Record<string, unknown>) => ({
    id: r.id as number,
    createdAt: r.created_at as string,
    code: r.code as string,
    customer: r.customer as string,
    orderType: r.order_type as string,
    payment: r.payment as string,
    total: r.total as number,
  }));
}

/**
 * Get revenue grouped by order type.
 */
export async function getRevenueByType(startDate?: string, endDate?: string): Promise<{ type: string; revenue: number; count: number }[]> {
  const pool = getPool();
  let query = `
    SELECT order_type, SUM(total) as revenue, COUNT(*) as count
    FROM orders
    WHERE created_at IS NOT NULL AND created_at != ''
  `;
  const params: string[] = [];
  let paramIdx = 1;

  if (startDate) {
    query += ` AND created_at >= $${paramIdx++}`;
    params.push(startDate);
  }
  if (endDate) {
    query += ` AND created_at < $${paramIdx++}`;
    params.push(endDate);
  }

  query += ` GROUP BY order_type ORDER BY revenue DESC`;

  const { rows } = await pool.query(query, params);

  return rows.map((r: Record<string, unknown>) => ({
    type: r.order_type as string,
    revenue: Number(r.revenue),
    count: Number(r.count),
  }));
}

/**
 * Get aggregate order stats.
 */
export async function getOrderStats(): Promise<OrderStats | null> {
  const pool = getPool();
  const now = new Date();
  const currentMonthStart = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, "0")}-01`;
  const prevMonthDate = new Date(now.getFullYear(), now.getMonth() - 1, 1);
  const prevMonthStart = `${prevMonthDate.getFullYear()}-${String(prevMonthDate.getMonth() + 1).padStart(2, "0")}-01`;

  const totalResult = await pool.query(
    `SELECT COUNT(*) as count, SUM(total) as revenue FROM orders`
  );
  const totalRow = totalResult.rows[0];

  if (Number(totalRow.count) === 0) return null;

  const currentResult = await pool.query(
    `SELECT COUNT(*) as count, COALESCE(SUM(total), 0) as revenue FROM orders WHERE created_at >= $1`,
    [currentMonthStart]
  );
  const currentRow = currentResult.rows[0];

  const prevResult = await pool.query(
    `SELECT COALESCE(SUM(total), 0) as revenue FROM orders WHERE created_at >= $1 AND created_at < $2`,
    [prevMonthStart, currentMonthStart]
  );
  const prevRow = prevResult.rows[0];

  const revenueByType = await getRevenueByType();

  return {
    totalOrders: Number(totalRow.count),
    totalRevenue: Math.round((Number(totalRow.revenue) || 0) * 100) / 100,
    revenueByType,
    currentMonthOrders: Number(currentRow.count),
    currentMonthRevenue: Math.round(Number(currentRow.revenue) * 100) / 100,
    previousMonthRevenue: Math.round(Number(prevRow.revenue) * 100) / 100,
  };
}

/**
 * Check if order data exists.
 */
export async function hasOrderData(): Promise<boolean> {
  const pool = getPool();
  const { rows } = await pool.query(`SELECT COUNT(*) as count FROM orders`);
  return Number(rows[0].count) > 0;
}
