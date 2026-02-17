import { getDatabase } from "./database";
import { getCategory, isAnnualPlan } from "../analytics/categories";

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

// ══════════════════════════════════════════════════════════════
// CRM: Full customer profiles from Union.fit customer export
// ══════════════════════════════════════════════════════════════

export interface FullCustomerRow {
  unionId: string;
  firstName: string;
  lastName: string;
  email: string;
  phone?: string;
  role?: string;
  totalSpent: number;
  ltv: number;
  orderCount: number;
  currentFreePass: boolean;
  currentFreeAutoRenew: boolean;
  currentPaidPass: boolean;
  currentPaidAutoRenew: boolean;
  currentPaymentPlan: boolean;
  livestreamRegistrations: number;
  inpersonRegistrations: number;
  replayRegistrations: number;
  livestreamRedeemed: number;
  inpersonRedeemed: number;
  replayRedeemed: number;
  instagram?: string;
  notes?: string;
  birthday?: string;
  howHeard?: string;
  goals?: string;
  neighborhood?: string;
  inspiration?: string;
  practiceFrequency?: string;
  createdAt: string;
}

export interface CustomerProfile {
  unionId: string;
  firstName: string;
  lastName: string;
  email: string;
  phone: string | null;
  role: string | null;
  totalSpent: number;
  ltv: number;
  orderCount: number;
  currentPaidAutoRenew: boolean;
  birthday: string | null;
  howHeard: string | null;
  neighborhood: string | null;
  instagram: string | null;
  inpersonRegistrations: number;
  livestreamRegistrations: number;
  replayRegistrations: number;
  createdAt: string;
  /** Auto-renew plans from auto_renews table */
  autoRenews: {
    planName: string;
    planState: string;
    planPrice: number;
    createdAt: string;
    canceledAt: string | null;
    category: string;
    isAnnual: boolean;
  }[];
  /** Recent registrations from registrations table */
  recentClasses: {
    eventName: string;
    attendedAt: string;
    pass: string;
  }[];
}

/**
 * Save full customer profiles from Union.fit customer export CSV.
 * Uses INSERT OR REPLACE on email (unique key).
 */
export function saveFullCustomers(rows: FullCustomerRow[]): void {
  const db = getDatabase();

  const upsert = db.prepare(`
    INSERT OR REPLACE INTO customers (
      union_id, first_name, last_name, email, phone, role,
      total_spent, ltv, order_count,
      current_free_pass, current_free_auto_renew,
      current_paid_pass, current_paid_auto_renew, current_payment_plan,
      livestream_registrations, inperson_registrations, replay_registrations,
      livestream_redeemed, inperson_redeemed, replay_redeemed,
      instagram, notes, birthday, how_heard, goals, neighborhood,
      inspiration, practice_frequency, created_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);

  const insertMany = db.transaction((items: FullCustomerRow[]) => {
    for (const row of items) {
      upsert.run(
        row.unionId,
        row.firstName,
        row.lastName,
        row.email,
        row.phone || null,
        row.role || null,
        row.totalSpent,
        row.ltv,
        row.orderCount,
        row.currentFreePass ? 1 : 0,
        row.currentFreeAutoRenew ? 1 : 0,
        row.currentPaidPass ? 1 : 0,
        row.currentPaidAutoRenew ? 1 : 0,
        row.currentPaymentPlan ? 1 : 0,
        row.livestreamRegistrations,
        row.inpersonRegistrations,
        row.replayRegistrations,
        row.livestreamRedeemed,
        row.inpersonRedeemed,
        row.replayRedeemed,
        row.instagram || null,
        row.notes || null,
        row.birthday || null,
        row.howHeard || null,
        row.goals || null,
        row.neighborhood || null,
        row.inspiration || null,
        row.practiceFrequency || null,
        row.createdAt,
      );
    }
  });

  insertMany(rows);
  console.log(`[customer-store] Saved ${rows.length} full customer profiles to CRM`);
}

/**
 * Look up a customer by email (the unique identifier).
 * Returns a full profile with auto-renew plans and recent classes.
 */
export function getCustomerByEmail(email: string): CustomerProfile | null {
  const db = getDatabase();

  const raw = db.prepare(`
    SELECT * FROM customers WHERE LOWER(email) = LOWER(?)
  `).get(email) as Record<string, unknown> | undefined;

  if (!raw) return null;

  // Get their auto-renew plans
  const arRows = db.prepare(`
    SELECT plan_name, plan_state, plan_price, created_at, canceled_at
    FROM auto_renews
    WHERE LOWER(customer_email) = LOWER(?)
    ORDER BY created_at DESC
  `).all(email) as { plan_name: string; plan_state: string; plan_price: number; created_at: string; canceled_at: string | null }[];

  // Get their recent classes (last 20)
  const classRows = db.prepare(`
    SELECT event_name, attended_at, pass
    FROM registrations
    WHERE LOWER(email) = LOWER(?)
    ORDER BY attended_at DESC
    LIMIT 20
  `).all(email) as { event_name: string; attended_at: string; pass: string }[];

  return {
    unionId: raw.union_id as string,
    firstName: raw.first_name as string,
    lastName: raw.last_name as string,
    email: raw.email as string,
    phone: raw.phone as string | null,
    role: raw.role as string | null,
    totalSpent: raw.total_spent as number,
    ltv: raw.ltv as number,
    orderCount: raw.order_count as number,
    currentPaidAutoRenew: !!(raw.current_paid_auto_renew as number),
    birthday: raw.birthday as string | null,
    howHeard: raw.how_heard as string | null,
    neighborhood: raw.neighborhood as string | null,
    instagram: raw.instagram as string | null,
    inpersonRegistrations: raw.inperson_registrations as number,
    livestreamRegistrations: raw.livestream_registrations as number,
    replayRegistrations: raw.replay_registrations as number,
    createdAt: raw.created_at as string,
    autoRenews: arRows.map((r) => ({
      planName: r.plan_name,
      planState: r.plan_state,
      planPrice: r.plan_price,
      createdAt: r.created_at,
      canceledAt: r.canceled_at,
      category: getCategory(r.plan_name),
      isAnnual: isAnnualPlan(r.plan_name),
    })),
    recentClasses: classRows.map((r) => ({
      eventName: r.event_name,
      attendedAt: r.attended_at,
      pass: r.pass,
    })),
  };
}

/**
 * Search customers by name or email (partial match).
 */
export function searchFullCustomers(query: string, limit = 20): { email: string; name: string; totalSpent: number; role: string }[] {
  const db = getDatabase();
  const q = `%${query}%`;
  const rows = db.prepare(`
    SELECT email, first_name || ' ' || last_name as name, total_spent, role
    FROM customers
    WHERE LOWER(email) LIKE LOWER(?) OR LOWER(first_name || ' ' || last_name) LIKE LOWER(?)
    ORDER BY total_spent DESC
    LIMIT ?
  `).all(q, q, limit) as { email: string; name: string; total_spent: number; role: string }[];

  return rows.map((r) => ({
    email: r.email,
    name: r.name,
    totalSpent: r.total_spent,
    role: r.role,
  }));
}

/**
 * Check if full customer profiles exist.
 */
export function hasFullCustomerData(): boolean {
  const db = getDatabase();
  const row = db.prepare(`SELECT COUNT(*) as count FROM customers`).get() as { count: number };
  return row.count > 0;
}

/**
 * Get total full customer count.
 */
export function getFullCustomerCount(): number {
  const db = getDatabase();
  const row = db.prepare(`SELECT COUNT(*) as count FROM customers`).get() as { count: number };
  return row.count;
}
