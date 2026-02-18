import { getPool } from "./database";

export interface Watermark {
  reportType: string;
  lastFetchedAt: Date | null;
  highWaterDate: string | null;
  recordCount: number;
  notes: string | null;
}

/**
 * Get the watermark for a report type.
 * Returns null if no watermark exists (first run = full backfill needed).
 */
export async function getWatermark(reportType: string): Promise<Watermark | null> {
  const pool = getPool();
  const { rows } = await pool.query(
    `SELECT report_type, last_fetched_at, high_water_date, record_count, notes
     FROM fetch_watermarks WHERE report_type = $1`,
    [reportType]
  );

  if (rows.length === 0) return null;

  const r = rows[0] as Record<string, unknown>;
  return {
    reportType: r.report_type as string,
    lastFetchedAt: r.last_fetched_at ? new Date(r.last_fetched_at as string) : null,
    highWaterDate: r.high_water_date as string | null,
    recordCount: Number(r.record_count),
    notes: r.notes as string | null,
  };
}

/**
 * Set or update a watermark after a successful fetch.
 */
export async function setWatermark(
  reportType: string,
  highWaterDate: string,
  recordCount: number,
  notes?: string
): Promise<void> {
  const pool = getPool();
  await pool.query(
    `INSERT INTO fetch_watermarks (report_type, last_fetched_at, high_water_date, record_count, notes)
     VALUES ($1, NOW(), $2, $3, $4)
     ON CONFLICT (report_type) DO UPDATE SET
       last_fetched_at = NOW(),
       high_water_date = EXCLUDED.high_water_date,
       record_count = EXCLUDED.record_count,
       notes = EXCLUDED.notes`,
    [reportType, highWaterDate, recordCount, notes || null]
  );
}

/**
 * Get all watermarks for a summary view.
 */
export async function getAllWatermarks(): Promise<Watermark[]> {
  const pool = getPool();
  const { rows } = await pool.query(
    `SELECT report_type, last_fetched_at, high_water_date, record_count, notes
     FROM fetch_watermarks ORDER BY report_type`
  );

  return rows.map((r: Record<string, unknown>) => ({
    reportType: r.report_type as string,
    lastFetchedAt: r.last_fetched_at ? new Date(r.last_fetched_at as string) : null,
    highWaterDate: r.high_water_date as string | null,
    recordCount: Number(r.record_count),
    notes: r.notes as string | null,
  }));
}

/**
 * Build date range string for a report based on its watermark.
 * If no watermark exists, returns a full historical range (back to 2024-01-01).
 * If watermark exists, returns from (high_water_date - 1 day) to today.
 * Format: "MM/DD/YYYY - MM/DD/YYYY" (Union.fit format).
 */
export function buildDateRangeForReport(watermark: Watermark | null): string {
  const now = new Date();
  const endStr = `${now.getMonth() + 1}/${now.getDate()}/${now.getFullYear()}`;

  if (!watermark || !watermark.highWaterDate) {
    // Full backfill: go back to Jan 1, 2024
    return `1/1/2024 - ${endStr}`;
  }

  // Incremental: start 1 day before high water mark for overlap safety
  const hwDate = new Date(watermark.highWaterDate);
  hwDate.setDate(hwDate.getDate() - 1);
  const startStr = `${hwDate.getMonth() + 1}/${hwDate.getDate()}/${hwDate.getFullYear()}`;

  return `${startStr} - ${endStr}`;
}
