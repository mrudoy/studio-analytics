import { getDatabase } from "./database";

// ── Types ────────────────────────────────────────────────────

export interface CustomerRow {
  name: string;
  email: string;
  role: string;
  orders: number;
  created: string;
}

export interface StoredCustomer {
  id: number;
  name: string;
  email: string;
  role: string;
  orderCount: number;
  createdAt: string;
}

export interface CustomerStats {
  /** Total customers in the database */
  totalCustomers: number;
  /** Current month new customers */
  currentMonthNew: number;
  /** Previous month new customers */
  previousMonthNew: number;
  /** Current month paced (extrapolated) */
  currentMonthPaced: number;
  /** Customers by role breakdown */
  byRole: { role: string; count: number }[];
}

// ── Write Operations ─────────────────────────────────────────

/**
 * Save new customers (additive — appends new rows, never deletes existing).
 */
export function saveCustomers(rows: CustomerRow[]): void {
  const db = getDatabase();
  const before = (db.prepare("SELECT COUNT(*) as count FROM new_customers").get() as { count: number }).count;

  const insert = db.prepare(`
    INSERT OR IGNORE INTO new_customers (name, email, role, order_count, created_at)
    VALUES (?, ?, ?, ?, ?)
  `);

  const insertMany = db.transaction((items: CustomerRow[]) => {
    for (const r of items) {
      insert.run(r.name, r.email, r.role, r.orders, r.created);
    }
  });

  insertMany(rows);
  const after = (db.prepare("SELECT COUNT(*) as count FROM new_customers").get() as { count: number }).count;
  console.log(`[customer-store] Customers: ${before} -> ${after} (+${after - before} new)`);
}

// ── Read Operations ──────────────────────────────────────────

/**
 * Get customers created within a date range.
 */
export function getCustomers(startDate?: string, endDate?: string): StoredCustomer[] {
  const db = getDatabase();
  let query = `
    SELECT id, name, email, role, order_count, created_at
    FROM new_customers
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
    id: number; name: string; email: string; role: string;
    order_count: number; created_at: string;
  }[];

  return rows.map((r) => ({
    id: r.id,
    name: r.name,
    email: r.email,
    role: r.role,
    orderCount: r.order_count,
    createdAt: r.created_at,
  }));
}

/**
 * Get new customers grouped by week.
 */
export function getNewCustomersByWeek(startDate?: string, endDate?: string): { week: string; count: number }[] {
  const db = getDatabase();
  let query = `
    SELECT strftime('%Y-W%W', created_at) as week, COUNT(*) as count
    FROM new_customers
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

  query += ` GROUP BY week ORDER BY week`;

  return db.prepare(query).all(...params) as { week: string; count: number }[];
}

/**
 * Get aggregate customer stats.
 */
export function getCustomerStats(): CustomerStats | null {
  const db = getDatabase();
  const now = new Date();
  const currentMonthStart = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, "0")}-01`;
  const prevMonthDate = new Date(now.getFullYear(), now.getMonth() - 1, 1);
  const prevMonthStart = `${prevMonthDate.getFullYear()}-${String(prevMonthDate.getMonth() + 1).padStart(2, "0")}-01`;

  const totalRow = db.prepare(
    `SELECT COUNT(*) as count FROM new_customers`
  ).get() as { count: number };

  if (totalRow.count === 0) return null;

  const currentRow = db.prepare(
    `SELECT COUNT(*) as count FROM new_customers WHERE created_at >= ?`
  ).get(currentMonthStart) as { count: number };

  const prevRow = db.prepare(
    `SELECT COUNT(*) as count FROM new_customers WHERE created_at >= ? AND created_at < ?`
  ).get(prevMonthStart, currentMonthStart) as { count: number };

  const daysElapsed = now.getDate();
  const daysInMonth = new Date(now.getFullYear(), now.getMonth() + 1, 0).getDate();

  const byRole = db.prepare(
    `SELECT role, COUNT(*) as count FROM new_customers GROUP BY role ORDER BY count DESC`
  ).all() as { role: string; count: number }[];

  return {
    totalCustomers: totalRow.count,
    currentMonthNew: currentRow.count,
    previousMonthNew: prevRow.count,
    currentMonthPaced: daysElapsed > 0
      ? Math.round((currentRow.count / daysElapsed) * daysInMonth)
      : 0,
    byRole,
  };
}

/**
 * Check if customer data exists.
 */
export function hasCustomerData(): boolean {
  const db = getDatabase();
  const row = db.prepare(`SELECT COUNT(*) as count FROM new_customers`).get() as { count: number };
  return row.count > 0;
}
