/**
 * Compute DashboardStats from SQLite data.
 *
 * Returns the same shape as readDashboardStats() from Sheets so the
 * dashboard components need zero changes. Returns null if SQLite doesn't
 * have enough data (caller falls back to Sheets).
 */

import { getSubscriptionStats, hasSubscriptionData } from "../db/subscription-store";
import { getLatestPeriod, getRevenueForPeriod } from "../db/revenue-store";
import type { DashboardStats } from "../sheets/read-dashboard";

/**
 * Attempt to build DashboardStats entirely from SQLite.
 *
 * Requirements to return non-null:
 *   - Subscription data exists and has active subscriptions
 *
 * Revenue data is optional — if revenue_categories exist we include them,
 * otherwise we return 0 for revenue fields (better than blocking everything).
 */
export function computeStatsFromSQLite(): DashboardStats | null {
  // ── Guard: need subscription data ─────────────────────────
  if (!hasSubscriptionData()) {
    console.log("[sqlite-stats] No subscription data in SQLite — skipping");
    return null;
  }

  const subStats = getSubscriptionStats();
  if (!subStats) {
    console.log("[sqlite-stats] No active subscriptions — skipping");
    return null;
  }

  // ── Revenue from revenue_categories (optional) ────────────
  let currentMonthRevenue = 0;
  let previousMonthRevenue = 0;

  try {
    const latestPeriod = getLatestPeriod();
    if (latestPeriod) {
      const rows = getRevenueForPeriod(latestPeriod.periodStart, latestPeriod.periodEnd);
      const totalNet = rows.reduce((sum, r) => sum + r.netRevenue, 0);
      currentMonthRevenue = Math.round(totalNet * 100) / 100;

      // Try to find the previous period for previousMonthRevenue.
      // Revenue categories are stored per period, so look for an earlier period.
      const now = new Date();
      const prevMonthDate = new Date(now.getFullYear(), now.getMonth() - 1, 1);
      const prevStart = `${prevMonthDate.getFullYear()}-${String(prevMonthDate.getMonth() + 1).padStart(2, "0")}-01`;
      const prevEndDate = new Date(now.getFullYear(), now.getMonth(), 1);
      const prevEnd = `${prevEndDate.getFullYear()}-${String(prevEndDate.getMonth() + 1).padStart(2, "0")}-01`;

      const prevRows = getRevenueForPeriod(prevStart, prevEnd);
      if (prevRows.length > 0) {
        previousMonthRevenue = Math.round(
          prevRows.reduce((sum, r) => sum + r.netRevenue, 0) * 100
        ) / 100;
      }
    }
  } catch (err) {
    console.warn("[sqlite-stats] Failed to load revenue data:", err);
    // Non-fatal: we still have subscription stats
  }

  // ── Build DashboardStats ──────────────────────────────────
  const stats: DashboardStats = {
    lastUpdated: new Date().toISOString(),
    dateRange: null, // SQLite doesn't store a date range for subscriptions snapshots
    mrr: subStats.mrr,
    activeSubscribers: subStats.active,
    arpu: subStats.arpu,
    currentMonthRevenue,
    previousMonthRevenue,
  };

  console.log(
    `[sqlite-stats] Computed from SQLite: ${subStats.active.total} subscribers, ` +
    `$${subStats.mrr.total} MRR, $${currentMonthRevenue} current month revenue`
  );

  return stats;
}
