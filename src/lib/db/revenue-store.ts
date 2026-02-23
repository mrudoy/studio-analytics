import { getPool } from "./database";
import type { RevenueCategory } from "@/types/union-data";

export async function saveRevenueCategories(
  periodStart: string,
  periodEnd: string,
  rows: RevenueCategory[]
): Promise<void> {
  const pool = getPool();

  // Guard: reject multi-month periods if monthly data already exists for that year.
  // This prevents the double-counting bug where a full-year row ($2.2M) is summed
  // alongside 12 monthly rows ($2.2M) → $4.4M.
  const isMultiMonth = periodStart.slice(0, 7) !== periodEnd.slice(0, 7);
  if (isMultiMonth) {
    const year = periodStart.slice(0, 4);
    const { rows: existing } = await pool.query(
      `SELECT 1 FROM revenue_categories
       WHERE LEFT(period_start, 4) = $1 AND LEFT(period_start, 7) = LEFT(period_end, 7)
       LIMIT 1`,
      [year]
    );
    if (existing.length > 0) {
      console.warn(
        `[revenue-store] Skipping multi-month period ${periodStart}–${periodEnd}: ` +
        `monthly data already exists for ${year}. Would cause double-counting.`
      );
      return;
    }
  }

  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    for (const row of rows) {
      await client.query(
        `INSERT INTO revenue_categories (period_start, period_end, category, revenue, union_fees, stripe_fees, other_fees, transfers, refunded, union_fees_refunded, net_revenue)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
         ON CONFLICT(period_start, period_end, category)
         DO UPDATE SET
           revenue = EXCLUDED.revenue,
           union_fees = EXCLUDED.union_fees,
           stripe_fees = EXCLUDED.stripe_fees,
           other_fees = EXCLUDED.other_fees,
           transfers = EXCLUDED.transfers,
           refunded = EXCLUDED.refunded,
           union_fees_refunded = EXCLUDED.union_fees_refunded,
           net_revenue = EXCLUDED.net_revenue,
           created_at = NOW()`,
        [
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
          row.netRevenue,
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

  console.log(`[revenue-store] Saved ${rows.length} revenue categories for ${periodStart} – ${periodEnd}`);
}

export async function lockPeriod(periodStart: string, periodEnd: string): Promise<void> {
  const pool = getPool();
  await pool.query(
    `UPDATE revenue_categories SET locked = 1 WHERE period_start = $1 AND period_end = $2`,
    [periodStart, periodEnd]
  );
  console.log(`[revenue-store] Locked period ${periodStart} – ${periodEnd}`);
}

export async function isLocked(periodStart: string, periodEnd: string): Promise<boolean> {
  const pool = getPool();
  const { rows } = await pool.query(
    `SELECT locked FROM revenue_categories WHERE period_start = $1 AND period_end = $2 LIMIT 1`,
    [periodStart, periodEnd]
  );
  return rows[0]?.locked === 1;
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

export async function getRevenueForPeriod(periodStart: string, periodEnd: string): Promise<StoredRevenueRow[]> {
  const pool = getPool();
  const { rows } = await pool.query(
    `SELECT category, revenue, union_fees, stripe_fees, other_fees, transfers, refunded, union_fees_refunded, net_revenue, period_start, period_end, locked
     FROM revenue_categories
     WHERE period_start = $1 AND period_end = $2
     ORDER BY revenue DESC`,
    [periodStart, periodEnd]
  );

  return rows.map((r: Record<string, unknown>) => ({
    category: r.category as string,
    revenue: r.revenue as number,
    unionFees: r.union_fees as number,
    stripeFees: r.stripe_fees as number,
    otherFees: r.other_fees as number,
    transfers: r.transfers as number,
    refunded: r.refunded as number,
    unionFeesRefunded: r.union_fees_refunded as number,
    netRevenue: r.net_revenue as number,
    periodStart: r.period_start as string,
    periodEnd: r.period_end as string,
    locked: r.locked === 1,
  }));
}

export async function getLatestPeriod(): Promise<{ periodStart: string; periodEnd: string } | null> {
  const pool = getPool();
  const { rows } = await pool.query(
    `SELECT period_start, period_end FROM revenue_categories ORDER BY period_end DESC LIMIT 1`
  );
  if (rows.length === 0) return null;
  return { periodStart: rows[0].period_start, periodEnd: rows[0].period_end };
}

export async function getAllPeriods(): Promise<{ periodStart: string; periodEnd: string; locked: boolean; categoryCount: number; totalRevenue: number; totalNetRevenue: number }[]> {
  const pool = getPool();
  const { rows } = await pool.query(
    `SELECT period_start, period_end, MAX(locked) as locked, COUNT(*) as category_count,
            SUM(revenue) as total_revenue, SUM(net_revenue) as total_net_revenue
     FROM revenue_categories
     GROUP BY period_start, period_end
     ORDER BY period_end DESC`
  );

  return rows.map((r: Record<string, unknown>) => ({
    periodStart: r.period_start as string,
    periodEnd: r.period_end as string,
    locked: (r.locked as number) === 1,
    categoryCount: Number(r.category_count),
    totalRevenue: Number(r.total_revenue),
    totalNetRevenue: Number(r.total_net_revenue),
  }));
}

/**
 * Get revenue totals for a specific month (period_start = YYYY-MM-01, period_end = YYYY-MM-last).
 * Returns null if no data for that month.
 */
export async function getMonthlyRevenue(year: number, month: number): Promise<{
  periodStart: string;
  periodEnd: string;
  categoryCount: number;
  totalRevenue: number;
  totalNetRevenue: number;
  categories: StoredRevenueRow[];
} | null> {
  const pool = getPool();
  const startStr = `${year}-${String(month).padStart(2, "0")}-01`;

  // Find any period that starts with this month (period_end varies: 28/29/30/31)
  const { rows } = await pool.query(
    `SELECT period_start, period_end, category, revenue, union_fees, stripe_fees,
            other_fees, transfers, refunded, union_fees_refunded, net_revenue, locked
     FROM revenue_categories
     WHERE period_start = $1
       AND period_end LIKE $2
     ORDER BY revenue DESC`,
    [startStr, `${year}-${String(month).padStart(2, "0")}-%`]
  );

  if (rows.length === 0) return null;

  const categories = rows.map((r: Record<string, unknown>) => ({
    category: r.category as string,
    revenue: r.revenue as number,
    unionFees: r.union_fees as number,
    stripeFees: r.stripe_fees as number,
    otherFees: r.other_fees as number,
    transfers: r.transfers as number,
    refunded: r.refunded as number,
    unionFeesRefunded: r.union_fees_refunded as number,
    netRevenue: r.net_revenue as number,
    periodStart: r.period_start as string,
    periodEnd: r.period_end as string,
    locked: r.locked === 1,
  }));

  return {
    periodStart: categories[0].periodStart,
    periodEnd: categories[0].periodEnd,
    categoryCount: categories.length,
    totalRevenue: categories.reduce((s: number, c: StoredRevenueRow) => s + c.revenue, 0),
    totalNetRevenue: categories.reduce((s: number, c: StoredRevenueRow) => s + c.netRevenue, 0),
    categories,
  };
}

/**
 * Get all monthly revenue summaries (periods where start and end are within the same month).
 * Excludes annual/multi-month periods. Ordered chronologically.
 */
export async function getAllMonthlyRevenue(): Promise<{
  periodStart: string;
  periodEnd: string;
  categoryCount: number;
  totalRevenue: number;
  totalNetRevenue: number;
}[]> {
  const pool = getPool();
  const { rows } = await pool.query(
    `SELECT period_start, period_end, COUNT(*) as category_count,
            SUM(revenue) as total_revenue, SUM(net_revenue) as total_net_revenue
     FROM revenue_categories
     WHERE LEFT(period_start, 7) = LEFT(period_end, 7)
     GROUP BY period_start, period_end
     ORDER BY period_start ASC`
  );

  return rows.map((r: Record<string, unknown>) => ({
    periodStart: r.period_start as string,
    periodEnd: r.period_end as string,
    categoryCount: Number(r.category_count),
    totalRevenue: Number(r.total_revenue),
    totalNetRevenue: Number(r.total_net_revenue),
  }));
}

/**
 * Get per-month sum of Union.fit "Merch" + "Products" revenue categories.
 * Used for deduplication: when Shopify data exists for a month, we subtract
 * Union.fit's merch revenue from the overall total to avoid double-counting.
 */
export async function getMonthlyMerchRevenue(): Promise<Map<string, { gross: number; net: number }>> {
  const pool = getPool();
  const { rows } = await pool.query(`
    SELECT
      LEFT(period_start, 7) AS month,
      SUM(revenue) AS gross,
      SUM(net_revenue) AS net
    FROM revenue_categories
    WHERE category IN ('Merch', 'Products')
      AND LEFT(period_start, 7) = LEFT(period_end, 7)
    GROUP BY LEFT(period_start, 7)
    ORDER BY month
  `);

  const map = new Map<string, { gross: number; net: number }>();
  for (const r of rows) {
    map.set(r.month as string, {
      gross: Number(r.gross) || 0,
      net: Number(r.net) || 0,
    });
  }
  return map;
}

export async function savePipelineRun(
  dateRangeStart: string,
  dateRangeEnd: string,
  recordCounts: Record<string, number>,
  durationMs: number
): Promise<void> {
  const pool = getPool();
  await pool.query(
    `INSERT INTO pipeline_runs (date_range_start, date_range_end, record_counts, duration_ms)
     VALUES ($1, $2, $3, $4)`,
    [dateRangeStart, dateRangeEnd, JSON.stringify(recordCounts), durationMs]
  );
}

export async function saveUploadedData(
  filename: string,
  dataType: string,
  period: string | null,
  content: string
): Promise<number> {
  const pool = getPool();
  const { rows } = await pool.query(
    `INSERT INTO uploaded_data (filename, data_type, period, content)
     VALUES ($1, $2, $3, $4) RETURNING id`,
    [filename, dataType, period, content]
  );
  return rows[0].id as number;
}
