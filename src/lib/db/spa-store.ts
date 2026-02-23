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
    `SELECT
       category,
       SUM(revenue) AS total_revenue,
       SUM(net_revenue) AS total_net_revenue,
       COUNT(DISTINCT SUBSTR(period_start, 1, 7)) AS months
     FROM revenue_categories
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
    `SELECT
       SUBSTR(period_start, 1, 7) AS month,
       category,
       SUM(revenue) AS revenue,
       SUM(net_revenue) AS net_revenue
     FROM revenue_categories
     WHERE category = ANY($1)
     GROUP BY SUBSTR(period_start, 1, 7), category
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
    `SELECT COALESCE(SUM(revenue), 0) AS mtd
     FROM revenue_categories
     WHERE category = ANY($1)
       AND SUBSTR(period_start, 1, 7) = $2`,
    [SPA_CATEGORIES as unknown as string[], currentMonth]
  );

  return parseFloat(res.rows[0].mtd) || 0;
}

// ── Aggregate stats ─────────────────────────────────────────

export interface SpaStats {
  mtdRevenue: number;
  avgMonthlyRevenue: number;
  monthlyRevenue: SpaMonthlyRevenue[];
  serviceBreakdown: SpaServiceRevenue[];
  totalRevenue: number;
}

/** Full spa stats for the dashboard. */
export async function getSpaStats(): Promise<SpaStats> {
  const [mtd, monthly, services] = await Promise.all([
    getSpaMTDRevenue(),
    getSpaMonthlyRevenue(),
    getSpaServiceBreakdown(),
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
  };
}
