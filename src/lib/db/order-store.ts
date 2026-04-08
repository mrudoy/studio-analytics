import { getPool } from "./database";

// ── Types ────────────────────────────────────────────────────

export interface OrderRow {
  created: string;
  code: string;
  customer: string;
  email: string;
  type: string;
  payment: string;
  total: number;
  /** Union.fit order ID from raw export (for precise dedup) */
  unionOrderId?: string;
  /** Order state: completed, refunded, etc. */
  state?: string;
  /** When the order was completed (may differ from created) */
  completedAt?: string;
  /** Union.fit platform fee */
  feeUnionTotal?: number;
  /** Payment processor fee (Stripe) */
  feePaymentTotal?: number;
  /** Whether fees are paid by the customer (not deducted from org revenue) */
  feesOutside?: boolean;
  /** Subscription pass ID for category resolution */
  subscriptionPassId?: string;
  /** Resolved revenue category name (e.g. "Members", "SKY3 / Packs") */
  revenueCategory?: string;
}

export interface StoredOrder {
  id: number;
  createdAt: string;
  code: string;
  customer: string;
  email: string;
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
 * Save orders (pure upsert — never deletes existing rows).
 *
 * Strategy:
 *  - If the row has a union_order_id: first try UPDATE WHERE union_order_id = $id.
 *    This handles the case where the same order arrives with a different `code`
 *    (Union.fit occasionally rewrites codes). If UPDATE hits 0 rows, fall
 *    through to INSERT.
 *  - INSERT path uses ON CONFLICT (code) DO UPDATE with COALESCE on every
 *    mutable field so existing non-NULL data is preserved.
 *
 * This removes the prior DELETE-then-INSERT pattern (when a row with the same
 * union_order_id but a different code existed), which violated the permanent
 * NEVER DELETE DATA rule.
 */
export async function saveOrders(rows: OrderRow[]): Promise<void> {
  const pool = getPool();

  const beforeResult = await pool.query("SELECT COUNT(*) as count FROM orders");
  const before = Number(beforeResult.rows[0].count);

  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    for (const r of rows) {
      const vals = [
        r.created || null,
        r.code,
        r.customer,
        (r.email || '').toLowerCase(),
        r.type,
        r.payment,
        r.total,
        r.unionOrderId || null,
        r.state || null,
        r.completedAt || null,
        r.feeUnionTotal ?? null,
        r.feePaymentTotal ?? null,
        r.feesOutside ?? null,
        r.subscriptionPassId || null,
        r.revenueCategory || null,
      ];

      // Step 1: if we have a union_order_id, try UPDATE by that id first.
      // This catches rows whose `code` changed since last seen — the partial
      // unique index idx_orders_union_id prevents a duplicate INSERT, and
      // UPDATE-in-place preserves the existing row id without a DELETE.
      if (r.unionOrderId) {
        const updated = await client.query(
          `UPDATE orders SET
             created_at = COALESCE($1, created_at),
             code = $2,
             customer = COALESCE($3, customer),
             email = COALESCE(NULLIF($4, ''), email),
             order_type = COALESCE($5, order_type),
             payment = COALESCE($6, payment),
             total = $7,
             state = COALESCE($9, state),
             completed_at = COALESCE($10, completed_at),
             fee_union_total = COALESCE($11, fee_union_total),
             fee_payment_total = COALESCE($12, fee_payment_total),
             fees_outside = COALESCE($13, fees_outside),
             subscription_pass_id = COALESCE($14, subscription_pass_id),
             revenue_category = COALESCE($15, revenue_category)
           WHERE union_order_id = $8`,
          vals
        );
        if (updated.rowCount && updated.rowCount > 0) continue;
      }

      // Step 2: no existing row for this union_order_id — insert, with a
      // final ON CONFLICT (code) safety net for rows without union_order_id.
      await client.query(
        `INSERT INTO orders (created_at, code, customer, email, order_type, payment, total, union_order_id,
           state, completed_at, fee_union_total, fee_payment_total, fees_outside, subscription_pass_id, revenue_category)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
         ON CONFLICT (code) DO UPDATE SET
           email = COALESCE(NULLIF(EXCLUDED.email, ''), orders.email),
           union_order_id = COALESCE(EXCLUDED.union_order_id, orders.union_order_id),
           state = COALESCE(EXCLUDED.state, orders.state),
           completed_at = COALESCE(EXCLUDED.completed_at, orders.completed_at),
           fee_union_total = COALESCE(EXCLUDED.fee_union_total, orders.fee_union_total),
           fee_payment_total = COALESCE(EXCLUDED.fee_payment_total, orders.fee_payment_total),
           fees_outside = COALESCE(EXCLUDED.fees_outside, orders.fees_outside),
           subscription_pass_id = COALESCE(EXCLUDED.subscription_pass_id, orders.subscription_pass_id),
           revenue_category = COALESCE(EXCLUDED.revenue_category, orders.revenue_category)`,
        vals
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
    SELECT id, created_at, code, customer, email, order_type, payment, total
    FROM orders
    WHERE created_at IS NOT NULL
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
    email: (r.email as string) || "",
    orderType: r.order_type as string,
    payment: r.payment as string,
    total: Number(r.total) || 0,
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
    WHERE created_at IS NOT NULL
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
