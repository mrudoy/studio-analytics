import { getPool } from "./database";
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
export async function saveCustomers(rows: CustomerRow[]): Promise<void> {
  const pool = getPool();
  const beforeResult = await pool.query("SELECT COUNT(*) as count FROM new_customers");
  const before = Number(beforeResult.rows[0].count);

  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    for (const r of rows) {
      await client.query(
        `INSERT INTO new_customers (name, email, role, order_count, created_at)
         VALUES ($1, $2, $3, $4, $5)
         ON CONFLICT (email) DO NOTHING`,
        [r.name, r.email, r.role, r.orders, r.created]
      );
    }
    await client.query("COMMIT");
  } catch (e) {
    await client.query("ROLLBACK");
    throw e;
  } finally {
    client.release();
  }

  const afterResult = await pool.query("SELECT COUNT(*) as count FROM new_customers");
  const after = Number(afterResult.rows[0].count);
  console.log(`[customer-store] Customers: ${before} -> ${after} (+${after - before} new)`);
}

// ── Read Operations ──────────────────────────────────────────

/**
 * Get customers created within a date range.
 */
export async function getCustomers(startDate?: string, endDate?: string): Promise<StoredCustomer[]> {
  const pool = getPool();
  let query = `
    SELECT id, name, email, role, order_count, created_at
    FROM new_customers
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
    name: r.name as string,
    email: r.email as string,
    role: r.role as string,
    orderCount: r.order_count as number,
    createdAt: r.created_at as string,
  }));
}

/**
 * Get new customers grouped by week.
 */
export async function getNewCustomersByWeek(startDate?: string, endDate?: string): Promise<{ week: string; count: number }[]> {
  const pool = getPool();
  let query = `
    SELECT TO_CHAR(created_at::date, 'IYYY-"W"IW') as week, COUNT(*) as count
    FROM new_customers
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

  query += ` GROUP BY week ORDER BY week`;

  const { rows } = await pool.query(query, params);
  return rows.map((r: Record<string, unknown>) => ({
    week: r.week as string,
    count: Number(r.count),
  }));
}

/**
 * Get aggregate customer stats.
 */
export async function getCustomerStats(): Promise<CustomerStats | null> {
  const pool = getPool();
  const now = new Date();
  const currentMonthStart = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, "0")}-01`;
  const prevMonthDate = new Date(now.getFullYear(), now.getMonth() - 1, 1);
  const prevMonthStart = `${prevMonthDate.getFullYear()}-${String(prevMonthDate.getMonth() + 1).padStart(2, "0")}-01`;

  const totalResult = await pool.query(
    `SELECT COUNT(*) as count FROM new_customers`
  );
  if (Number(totalResult.rows[0].count) === 0) return null;

  const currentResult = await pool.query(
    `SELECT COUNT(*) as count FROM new_customers WHERE created_at >= $1`,
    [currentMonthStart]
  );

  const prevResult = await pool.query(
    `SELECT COUNT(*) as count FROM new_customers WHERE created_at >= $1 AND created_at < $2`,
    [prevMonthStart, currentMonthStart]
  );

  const daysElapsed = now.getDate();
  const daysInMonth = new Date(now.getFullYear(), now.getMonth() + 1, 0).getDate();

  const roleResult = await pool.query(
    `SELECT role, COUNT(*) as count FROM new_customers GROUP BY role ORDER BY count DESC`
  );

  const currentCount = Number(currentResult.rows[0].count);

  return {
    totalCustomers: Number(totalResult.rows[0].count),
    currentMonthNew: currentCount,
    previousMonthNew: Number(prevResult.rows[0].count),
    currentMonthPaced: daysElapsed > 0
      ? Math.round((currentCount / daysElapsed) * daysInMonth)
      : 0,
    byRole: roleResult.rows.map((r: Record<string, unknown>) => ({
      role: r.role as string,
      count: Number(r.count),
    })),
  };
}

/**
 * Check if customer data exists.
 */
export async function hasCustomerData(): Promise<boolean> {
  const pool = getPool();
  const { rows } = await pool.query(`SELECT COUNT(*) as count FROM new_customers`);
  return Number(rows[0].count) > 0;
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
 * Uses INSERT ... ON CONFLICT (email) DO UPDATE SET for upsert.
 */
export async function saveFullCustomers(rows: FullCustomerRow[]): Promise<void> {
  const pool = getPool();
  const client = await pool.connect();

  try {
    await client.query("BEGIN");
    for (const row of rows) {
      await client.query(
        `INSERT INTO customers (
          union_id, first_name, last_name, email, phone, role,
          total_spent, ltv, order_count,
          current_free_pass, current_free_auto_renew,
          current_paid_pass, current_paid_auto_renew, current_payment_plan,
          livestream_registrations, inperson_registrations, replay_registrations,
          livestream_redeemed, inperson_redeemed, replay_redeemed,
          instagram, notes, birthday, how_heard, goals, neighborhood,
          inspiration, practice_frequency, created_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29)
        ON CONFLICT (email) DO UPDATE SET
          union_id = EXCLUDED.union_id,
          first_name = EXCLUDED.first_name,
          last_name = EXCLUDED.last_name,
          phone = EXCLUDED.phone,
          role = EXCLUDED.role,
          total_spent = EXCLUDED.total_spent,
          ltv = EXCLUDED.ltv,
          order_count = EXCLUDED.order_count,
          current_free_pass = EXCLUDED.current_free_pass,
          current_free_auto_renew = EXCLUDED.current_free_auto_renew,
          current_paid_pass = EXCLUDED.current_paid_pass,
          current_paid_auto_renew = EXCLUDED.current_paid_auto_renew,
          current_payment_plan = EXCLUDED.current_payment_plan,
          livestream_registrations = EXCLUDED.livestream_registrations,
          inperson_registrations = EXCLUDED.inperson_registrations,
          replay_registrations = EXCLUDED.replay_registrations,
          livestream_redeemed = EXCLUDED.livestream_redeemed,
          inperson_redeemed = EXCLUDED.inperson_redeemed,
          replay_redeemed = EXCLUDED.replay_redeemed,
          instagram = EXCLUDED.instagram,
          notes = EXCLUDED.notes,
          birthday = EXCLUDED.birthday,
          how_heard = EXCLUDED.how_heard,
          goals = EXCLUDED.goals,
          neighborhood = EXCLUDED.neighborhood,
          inspiration = EXCLUDED.inspiration,
          practice_frequency = EXCLUDED.practice_frequency,
          created_at = EXCLUDED.created_at`,
        [
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
        ]
      );
    }
    await client.query("COMMIT");
  } catch (e) {
    await client.query("ROLLBACK");
    throw e;
  } finally {
    client.release();
  }

  console.log(`[customer-store] Saved ${rows.length} full customer profiles to CRM`);
}

/**
 * Look up a customer by email (the unique identifier).
 * Returns a full profile with auto-renew plans and recent classes.
 */
export async function getCustomerByEmail(email: string): Promise<CustomerProfile | null> {
  const pool = getPool();

  const { rows: customerRows } = await pool.query(
    `SELECT * FROM customers WHERE LOWER(email) = LOWER($1)`,
    [email]
  );

  if (customerRows.length === 0) return null;
  const raw = customerRows[0] as Record<string, unknown>;

  // Get their auto-renew plans
  const { rows: arRows } = await pool.query(
    `SELECT plan_name, plan_state, plan_price, created_at, canceled_at
     FROM auto_renews
     WHERE LOWER(customer_email) = LOWER($1)
     ORDER BY created_at DESC`,
    [email]
  );

  // Get their recent classes (last 20)
  const { rows: classRows } = await pool.query(
    `SELECT event_name, attended_at, pass
     FROM registrations
     WHERE LOWER(email) = LOWER($1)
     ORDER BY attended_at DESC
     LIMIT 20`,
    [email]
  );

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
    autoRenews: arRows.map((r: Record<string, unknown>) => ({
      planName: r.plan_name as string,
      planState: r.plan_state as string,
      planPrice: r.plan_price as number,
      createdAt: r.created_at as string,
      canceledAt: r.canceled_at as string | null,
      category: getCategory(r.plan_name as string),
      isAnnual: isAnnualPlan(r.plan_name as string),
    })),
    recentClasses: classRows.map((r: Record<string, unknown>) => ({
      eventName: r.event_name as string,
      attendedAt: r.attended_at as string,
      pass: r.pass as string,
    })),
  };
}

/**
 * Search customers by name or email (partial match).
 */
export async function searchFullCustomers(query: string, limit = 20): Promise<{ email: string; name: string; totalSpent: number; role: string }[]> {
  const pool = getPool();
  const q = `%${query}%`;
  const { rows } = await pool.query(
    `SELECT email, first_name || ' ' || last_name as name, total_spent, role
     FROM customers
     WHERE email ILIKE $1 OR (first_name || ' ' || last_name) ILIKE $2
     ORDER BY total_spent DESC
     LIMIT $3`,
    [q, q, limit]
  );

  return rows.map((r: Record<string, unknown>) => ({
    email: r.email as string,
    name: r.name as string,
    totalSpent: r.total_spent as number,
    role: r.role as string,
  }));
}

/**
 * Check if full customer profiles exist.
 */
export async function hasFullCustomerData(): Promise<boolean> {
  const pool = getPool();
  const { rows } = await pool.query(`SELECT COUNT(*) as count FROM customers`);
  return Number(rows[0].count) > 0;
}

/**
 * Get total full customer count.
 */
export async function getFullCustomerCount(): Promise<number> {
  const pool = getPool();
  const { rows } = await pool.query(`SELECT COUNT(*) as count FROM customers`);
  return Number(rows[0].count);
}
