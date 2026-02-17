/**
 * Compute DashboardStats from PostgreSQL data.
 *
 * Returns the same shape as readDashboardStats() from Sheets so the
 * dashboard components need zero changes. Returns null if the database doesn't
 * have enough data (caller falls back to Sheets).
 */

import { getAutoRenewStats, hasAutoRenewData } from "../db/auto-renew-store";
import { getLatestPeriod, getRevenueForPeriod } from "../db/revenue-store";
import type { DashboardStats } from "../sheets/read-dashboard";

/**
 * Attempt to build DashboardStats entirely from PostgreSQL.
 *
 * Requirements to return non-null:
 *   - Auto-renew data exists and has active auto-renews
 *
 * Revenue data is optional — if revenue_categories exist we include them,
 * otherwise we return 0 for revenue fields (better than blocking everything).
 */
export async function computeStatsFromDB(): Promise<DashboardStats | null> {
  // ── Guard: need auto-renew data ──────────────────────────
  if (!(await hasAutoRenewData())) {
    console.log("[db-stats] No auto-renew data — skipping");
    return null;
  }

  const subStats = await getAutoRenewStats();
  if (!subStats) {
    console.log("[db-stats] No active auto-renews — skipping");
    return null;
  }

  // ── Revenue from revenue_categories (optional) ────────────
  let currentMonthRevenue = 0;
  let previousMonthRevenue = 0;

  try {
    const latestPeriod = await getLatestPeriod();
    if (latestPeriod) {
      const rows = await getRevenueForPeriod(latestPeriod.periodStart, latestPeriod.periodEnd);
      const totalNet = rows.reduce((sum, r) => sum + r.netRevenue, 0);
      currentMonthRevenue = Math.round(totalNet * 100) / 100;

      // Try to find the previous period for previousMonthRevenue.
      // Revenue categories are stored per period, so look for an earlier period.
      const now = new Date();
      const prevMonthDate = new Date(now.getFullYear(), now.getMonth() - 1, 1);
      const prevStart = `${prevMonthDate.getFullYear()}-${String(prevMonthDate.getMonth() + 1).padStart(2, "0")}-01`;
      const prevEndDate = new Date(now.getFullYear(), now.getMonth(), 1);
      const prevEnd = `${prevEndDate.getFullYear()}-${String(prevEndDate.getMonth() + 1).padStart(2, "0")}-01`;

      const prevRows = await getRevenueForPeriod(prevStart, prevEnd);
      if (prevRows.length > 0) {
        previousMonthRevenue = Math.round(
          prevRows.reduce((sum, r) => sum + r.netRevenue, 0) * 100
        ) / 100;
      }
    }
  } catch (err) {
    console.warn("[db-stats] Failed to load revenue data:", err);
    // Non-fatal: we still have subscription stats
  }

  // ── Build DashboardStats ──────────────────────────────────
  const stats: DashboardStats = {
    lastUpdated: new Date().toISOString(),
    dateRange: null,
    mrr: subStats.mrr,
    activeSubscribers: subStats.active,
    arpu: subStats.arpu,
    currentMonthRevenue,
    previousMonthRevenue,
  };

  console.log(
    `[db-stats] Computed: ${subStats.active.total} subscribers, ` +
    `$${subStats.mrr.total} MRR, $${currentMonthRevenue} current month revenue`
  );

  return stats;
}
