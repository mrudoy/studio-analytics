/**
 * Spa & Wellness data store — queries revenue_categories for spa-related services.
 *
 * Spa categories from Union.fit:
 *   - Infrared Sauna Suite
 *   - Contrast Suite
 *   - Spa Lounge
 *   - Treatment Room
 *   - CUPPING
 */

import { getPool } from "./database";

/** All spa-related revenue category names */
export const SPA_CATEGORIES = [
  "Infrared Sauna Suite",
  "Contrast Suite",
  "Spa Lounge",
  "Treatment Room",
  "CUPPING",
] as const;

export type SpaCategory = (typeof SPA_CATEGORIES)[number];

// ── Revenue by service ──────────────────────────────────────

export interface SpaServiceRevenue {
  category: SpaCategory;
  totalRevenue: number;
  totalNetRevenue: number;
  months: number;
}

/** Total revenue per spa service (all time). */
export async function getSpaServiceBreakdown(): Promise<SpaServiceRevenue[]> {
  const pool = getPool();
  const res = await pool.query(
    `WITH deduped AS (
       SELECT DISTINCT ON (category, LEFT(period_start, 7))
         category, revenue, net_revenue, period_start
       FROM revenue_categories
       WHERE LEFT(period_start, 7) = LEFT(period_end, 7)
       ORDER BY category, LEFT(period_start, 7), period_end DESC
     )
     SELECT
       category,
       SUM(revenue) AS total_revenue,
       SUM(net_revenue) AS total_net_revenue,
       COUNT(DISTINCT SUBSTR(period_start, 1, 7)) AS months
     FROM deduped
     WHERE category = ANY($1)
     GROUP BY category
     ORDER BY total_revenue DESC`,
    [SPA_CATEGORIES as unknown as string[]]
  );

  return res.rows.map((r: { category: string; total_revenue: string; total_net_revenue: string; months: string }) => ({
    category: r.category as SpaCategory,
    totalRevenue: parseFloat(r.total_revenue) || 0,
    totalNetRevenue: parseFloat(r.total_net_revenue) || 0,
    months: parseInt(r.months) || 0,
  }));
}

// ── Monthly revenue ─────────────────────────────────────────

export interface SpaMonthlyRevenue {
  month: string;        // YYYY-MM
  gross: number;
  net: number;
  byService: { category: SpaCategory; revenue: number }[];
}

/** Monthly spa revenue (all spa categories combined + per-service breakdown). */
export async function getSpaMonthlyRevenue(): Promise<SpaMonthlyRevenue[]> {
  const pool = getPool();
  const res = await pool.query(
    `WITH deduped AS (
       SELECT DISTINCT ON (category, LEFT(period_start, 7))
         category, revenue, net_revenue, period_start
       FROM revenue_categories
       WHERE LEFT(period_start, 7) = LEFT(period_end, 7)
       ORDER BY category, LEFT(period_start, 7), period_end DESC
     )
     SELECT
       SUBSTR(period_start, 1, 7) AS month,
       category,
       revenue,
       net_revenue
     FROM deduped
     WHERE category = ANY($1)
     ORDER BY month, category`,
    [SPA_CATEGORIES as unknown as string[]]
  );

  // Group by month
  const monthMap = new Map<string, { gross: number; net: number; byService: { category: SpaCategory; revenue: number }[] }>();
  for (const r of res.rows as { month: string; category: string; revenue: string; net_revenue: string }[]) {
    if (!monthMap.has(r.month)) {
      monthMap.set(r.month, { gross: 0, net: 0, byService: [] });
    }
    const entry = monthMap.get(r.month)!;
    const rev = parseFloat(r.revenue) || 0;
    const net = parseFloat(r.net_revenue) || 0;
    entry.gross += rev;
    entry.net += net;
    entry.byService.push({ category: r.category as SpaCategory, revenue: rev });
  }

  return Array.from(monthMap.entries())
    .map(([month, data]) => ({ month, ...data }))
    .sort((a, b) => a.month.localeCompare(b.month));
}

// ── MTD revenue ─────────────────────────────────────────────

/** Month-to-date spa revenue (current calendar month). */
export async function getSpaMTDRevenue(): Promise<number> {
  const pool = getPool();
  const now = new Date();
  const currentMonth = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, "0")}`;

  const res = await pool.query(
    `WITH deduped AS (
       SELECT DISTINCT ON (category, LEFT(period_start, 7))
         category, revenue, period_start
       FROM revenue_categories
       WHERE LEFT(period_start, 7) = LEFT(period_end, 7)
       ORDER BY category, LEFT(period_start, 7), period_end DESC
     )
     SELECT COALESCE(SUM(revenue), 0) AS mtd
     FROM deduped
     WHERE category = ANY($1)
       AND SUBSTR(period_start, 1, 7) = $2`,
    [SPA_CATEGORIES as unknown as string[], currentMonth]
  );

  return parseFloat(res.rows[0].mtd) || 0;
}

// ── Customer behavior (from registrations table) ─────────────

/** Spa bookings in the registrations table are at these locations */
const SPA_LOCATION_FILTER = `location_name IN ('SPA Lounge', 'SPA LOUNGE', 'TREATMENT ROOM')`;

export interface SpaVisitFrequency {
  bucket: string;
  customers: number;
}

export interface SpaCrossover {
  total: number;
  alsoTakeClasses: number;
  spaOnly: number;
}

export interface SpaSubscriberOverlap {
  total: number;
  areSubscribers: number;
  notSubscribers: number;
}

export interface SpaSubscriberPlan {
  planName: string;
  customers: number;
}

export interface SpaMonthlyVisits {
  month: string;
  visits: number;
  uniqueVisitors: number;
}

export interface SpaCustomerBehavior {
  uniqueCustomers: number;
  frequency: SpaVisitFrequency[];
  crossover: SpaCrossover;
  subscriberOverlap: SpaSubscriberOverlap;
  subscriberPlans: SpaSubscriberPlan[];
  monthlyVisits: SpaMonthlyVisits[];
}

/** Customer behavior analytics — who uses the spa? */
export async function getSpaCustomerBehavior(): Promise<SpaCustomerBehavior | null> {
  const pool = getPool();

  // 1. Get all unique spa customer emails
  const emailsRes = await pool.query(`
    SELECT DISTINCT email
    FROM registrations
    WHERE ${SPA_LOCATION_FILTER} AND email IS NOT NULL AND email != ''
  `);
  const spaEmails = emailsRes.rows.map((r: Record<string, unknown>) => r.email as string);

  if (spaEmails.length === 0) return null;

  // 2. Visit frequency buckets
  const freqRes = await pool.query(`
    SELECT bucket, COUNT(*) AS customers FROM (
      SELECT
        CASE
          WHEN COUNT(*) = 1 THEN '1 visit'
          WHEN COUNT(*) BETWEEN 2 AND 3 THEN '2-3 visits'
          WHEN COUNT(*) BETWEEN 4 AND 6 THEN '4-6 visits'
          WHEN COUNT(*) BETWEEN 7 AND 10 THEN '7-10 visits'
          ELSE '11+ visits'
        END AS bucket
      FROM registrations
      WHERE ${SPA_LOCATION_FILTER} AND email IS NOT NULL AND email != ''
      GROUP BY email
    ) sub
    GROUP BY bucket
    ORDER BY MIN(CASE bucket
      WHEN '1 visit' THEN 1 WHEN '2-3 visits' THEN 2
      WHEN '4-6 visits' THEN 3 WHEN '7-10 visits' THEN 4 ELSE 5 END)
  `);

  // 3. Crossover: how many also take classes?
  const classCheckRes = await pool.query(`
    SELECT DISTINCT email
    FROM registrations
    WHERE email = ANY($1)
      AND NOT (${SPA_LOCATION_FILTER})
  `, [spaEmails]);
  const classEmails = new Set(classCheckRes.rows.map((r: Record<string, unknown>) => r.email));

  // 4. Subscriber overlap
  const subRes = await pool.query(`
    SELECT COUNT(DISTINCT customer_email) AS cnt
    FROM auto_renews
    WHERE LOWER(customer_email) = ANY($1)
  `, [spaEmails.map(e => e.toLowerCase())]);
  const areSubscribers = Number(subRes.rows[0].cnt);

  // 5. Subscriber plan breakdown
  const planRes = await pool.query(`
    SELECT plan_name, COUNT(DISTINCT customer_email) AS customers
    FROM auto_renews
    WHERE LOWER(customer_email) = ANY($1)
    GROUP BY plan_name
    ORDER BY customers DESC
    LIMIT 15
  `, [spaEmails.map(e => e.toLowerCase())]);

  // 6. Monthly visits
  const monthlyRes = await pool.query(`
    SELECT
      SUBSTR(attended_at, 1, 7) AS month,
      COUNT(*) AS visits,
      COUNT(DISTINCT email) AS unique_visitors
    FROM registrations
    WHERE ${SPA_LOCATION_FILTER} AND attended_at IS NOT NULL
    GROUP BY SUBSTR(attended_at, 1, 7)
    ORDER BY month DESC
    LIMIT 18
  `);

  return {
    uniqueCustomers: spaEmails.length,
    frequency: freqRes.rows.map((r: Record<string, unknown>) => ({
      bucket: r.bucket as string,
      customers: Number(r.customers),
    })),
    crossover: {
      total: spaEmails.length,
      alsoTakeClasses: classEmails.size,
      spaOnly: spaEmails.length - classEmails.size,
    },
    subscriberOverlap: {
      total: spaEmails.length,
      areSubscribers,
      notSubscribers: spaEmails.length - areSubscribers,
    },
    subscriberPlans: planRes.rows.map((r: Record<string, unknown>) => ({
      planName: r.plan_name as string,
      customers: Number(r.customers),
    })),
    monthlyVisits: monthlyRes.rows.map((r: Record<string, unknown>) => ({
      month: r.month as string,
      visits: Number(r.visits),
      uniqueVisitors: Number(r.unique_visitors),
    })),
  };
}

// ── Aggregate stats ─────────────────────────────────────────

export interface SpaStats {
  mtdRevenue: number;
  avgMonthlyRevenue: number;
  monthlyRevenue: SpaMonthlyRevenue[];
  serviceBreakdown: SpaServiceRevenue[];
  totalRevenue: number;
  customerBehavior: SpaCustomerBehavior | null;
}

/** Full spa stats for the dashboard. */
export async function getSpaStats(): Promise<SpaStats> {
  const [mtd, monthly, services, behavior] = await Promise.all([
    getSpaMTDRevenue(),
    getSpaMonthlyRevenue(),
    getSpaServiceBreakdown(),
    getSpaCustomerBehavior().catch((err) => { console.error("[spa] getSpaCustomerBehavior error:", err); return null; }),
  ]);

  // Average from completed months only (exclude current)
  const now = new Date();
  const currentMonthKey = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, "0")}`;
  const completedMonths = monthly.filter((m) => m.month < currentMonthKey);
  const avgMonthlyRevenue = completedMonths.length > 0
    ? Math.round(completedMonths.reduce((sum, m) => sum + m.gross, 0) / completedMonths.length)
    : 0;

  const totalRevenue = services.reduce((sum, s) => sum + s.totalRevenue, 0);

  return {
    mtdRevenue: mtd,
    avgMonthlyRevenue,
    monthlyRevenue: monthly,
    serviceBreakdown: services,
    totalRevenue,
    customerBehavior: behavior,
  };
}
