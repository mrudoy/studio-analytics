import { getDatabase } from "./database";
import type { RevenueCategory } from "@/types/union-data";

export function saveRevenueCategories(
  periodStart: string,
  periodEnd: string,
  rows: RevenueCategory[]
): void {
  const db = getDatabase();
  const upsert = db.prepare(`
    INSERT INTO revenue_categories (period_start, period_end, category, revenue, union_fees, stripe_fees, other_fees, transfers, refunded, union_fees_refunded, net_revenue)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(period_start, period_end, category)
    DO UPDATE SET
      revenue = excluded.revenue,
      union_fees = excluded.union_fees,
      stripe_fees = excluded.stripe_fees,
      other_fees = excluded.other_fees,
      transfers = excluded.transfers,
      refunded = excluded.refunded,
      union_fees_refunded = excluded.union_fees_refunded,
      net_revenue = excluded.net_revenue,
      created_at = datetime('now')
  `);

  const insertMany = db.transaction((items: RevenueCategory[]) => {
    for (const row of items) {
      upsert.run(
        periodStart,
        periodEnd,
        row.revenueCategory,
        row.revenue,
        row.unionFees,
        row.stripeFees,
        row.otherFees ?? 0,
        row.transfers ?? 0,
        row.refunded,
        row.unionFeesRefunded,
        row.netRevenue
      );
    }
  });

  insertMany(rows);
  console.log(`[revenue-store] Saved ${rows.length} revenue categories for ${periodStart} – ${periodEnd}`);
}

export function lockPeriod(periodStart: string, periodEnd: string): void {
  const db = getDatabase();
  db.prepare(`UPDATE revenue_categories SET locked = 1 WHERE period_start = ? AND period_end = ?`)
    .run(periodStart, periodEnd);
  console.log(`[revenue-store] Locked period ${periodStart} – ${periodEnd}`);
}

export function isLocked(periodStart: string, periodEnd: string): boolean {
  const db = getDatabase();
  const row = db.prepare(
    `SELECT locked FROM revenue_categories WHERE period_start = ? AND period_end = ? LIMIT 1`
  ).get(periodStart, periodEnd) as { locked: number } | undefined;
  return row?.locked === 1;
}

export interface StoredRevenueRow {
  category: string;
  revenue: number;
  unionFees: number;
  stripeFees: number;
  otherFees: number;
  transfers: number;
  refunded: number;
  unionFeesRefunded: number;
  netRevenue: number;
  periodStart: string;
  periodEnd: string;
  locked: boolean;
}

export function getRevenueForPeriod(periodStart: string, periodEnd: string): StoredRevenueRow[] {
  const db = getDatabase();
  const rows = db.prepare(
    `SELECT category, revenue, union_fees, stripe_fees, other_fees, transfers, refunded, union_fees_refunded, net_revenue, period_start, period_end, locked
     FROM revenue_categories
     WHERE period_start = ? AND period_end = ?
     ORDER BY revenue DESC`
  ).all(periodStart, periodEnd) as {
    category: string; revenue: number; union_fees: number; stripe_fees: number;
    other_fees: number; transfers: number; refunded: number; union_fees_refunded: number;
    net_revenue: number; period_start: string; period_end: string; locked: number;
  }[];

  return rows.map((r) => ({
    category: r.category,
    revenue: r.revenue,
    unionFees: r.union_fees,
    stripeFees: r.stripe_fees,
    otherFees: r.other_fees,
    transfers: r.transfers,
    refunded: r.refunded,
    unionFeesRefunded: r.union_fees_refunded,
    netRevenue: r.net_revenue,
    periodStart: r.period_start,
    periodEnd: r.period_end,
    locked: r.locked === 1,
  }));
}

export function getLatestPeriod(): { periodStart: string; periodEnd: string } | null {
  const db = getDatabase();
  const row = db.prepare(
    `SELECT period_start, period_end FROM revenue_categories ORDER BY period_end DESC LIMIT 1`
  ).get() as { period_start: string; period_end: string } | undefined;
  return row ? { periodStart: row.period_start, periodEnd: row.period_end } : null;
}

export function getAllPeriods(): { periodStart: string; periodEnd: string; locked: boolean; categoryCount: number; totalRevenue: number; totalNetRevenue: number }[] {
  const db = getDatabase();
  const rows = db.prepare(
    `SELECT period_start, period_end, MAX(locked) as locked, COUNT(*) as category_count,
            SUM(revenue) as total_revenue, SUM(net_revenue) as total_net_revenue
     FROM revenue_categories
     GROUP BY period_start, period_end
     ORDER BY period_end DESC`
  ).all() as {
    period_start: string; period_end: string; locked: number;
    category_count: number; total_revenue: number; total_net_revenue: number;
  }[];

  return rows.map((r) => ({
    periodStart: r.period_start,
    periodEnd: r.period_end,
    locked: r.locked === 1,
    categoryCount: r.category_count,
    totalRevenue: r.total_revenue,
    totalNetRevenue: r.total_net_revenue,
  }));
}

export function savePipelineRun(
  dateRangeStart: string,
  dateRangeEnd: string,
  recordCounts: Record<string, number>,
  durationMs: number
): void {
  const db = getDatabase();
  db.prepare(
    `INSERT INTO pipeline_runs (date_range_start, date_range_end, record_counts, duration_ms)
     VALUES (?, ?, ?, ?)`
  ).run(dateRangeStart, dateRangeEnd, JSON.stringify(recordCounts), durationMs);
}

export function saveUploadedData(
  filename: string,
  dataType: string,
  period: string | null,
  content: string
): number {
  const db = getDatabase();
  const result = db.prepare(
    `INSERT INTO uploaded_data (filename, data_type, period, content)
     VALUES (?, ?, ?, ?)`
  ).run(filename, dataType, period, content);
  return Number(result.lastInsertRowid);
}
