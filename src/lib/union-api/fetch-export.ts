import { getWatermark, setWatermark } from "../db/watermark-store";
import { getPool } from "../db/database";

const UNION_API_URL = "https://www.union.fit/api/v1/data_exporters.json";
const WATERMARK_KEY = "unionApiExport";

export interface DataExporter {
  org: string;
  created_at: string;
  data_updated_starts_at: string;
  data_updated_ends_at: string;
  download_url: string;
}

export interface FetchExportResult {
  downloadUrl: string;
  createdAt: string;
  dataRange: { start: string; end: string };
}

/**
 * Call the Union Data Exporter API and return the latest export's download URL.
 * Always returns the latest export regardless of whether we've seen it before —
 * the DB upserts handle deduplication on insert.
 */
export async function fetchLatestExport(apiKey: string): Promise<FetchExportResult | null> {
  const all = await fetchAllExports(apiKey);
  return all.length > 0 ? all[0] : null;
}

/**
 * Fetch ALL available exports from the Union Data Exporter API.
 * Returns newest first. Daily exports are incremental (only recently changed
 * records), so processing all of them gives us the most complete dataset.
 * The DB upserts handle deduplication — processing the same record twice is safe.
 */
export async function fetchAllExports(apiKey: string): Promise<FetchExportResult[]> {
  const response = await fetch(UNION_API_URL, {
    headers: { Authorization: `Bearer ${apiKey}` },
  });

  if (!response.ok) {
    throw new Error(`Union API returned ${response.status}: ${response.statusText}`);
  }

  const json = await response.json();
  const exporters: DataExporter[] = json.data_exporters;

  if (!exporters || exporters.length === 0) {
    console.log("[union-api] No exports available");
    return [];
  }

  console.log(`[union-api] Found ${exporters.length} exports available`);

  // API returns newest first
  return exporters.map((exp) => ({
    downloadUrl: exp.download_url,
    createdAt: exp.created_at,
    dataRange: {
      start: exp.data_updated_starts_at,
      end: exp.data_updated_ends_at,
    },
  }));
}

/**
 * Mark an API export as processed (set watermark).
 */
export async function markExportProcessed(createdAt: string, recordCount: number): Promise<void> {
  await setWatermark(WATERMARK_KEY, createdAt, recordCount, "Union Data Exporter API");
}

/**
 * Log a processed export to the export_log table for freshness tracking.
 */
export async function logExport(
  exp: FetchExportResult,
  recordCount: number,
  index: number,
  totalExports: number,
): Promise<void> {
  try {
    const pool = getPool();
    await pool.query(
      `INSERT INTO export_log (export_created_at, data_range_start, data_range_end, record_count, export_index, total_exports)
       VALUES ($1, $2, $3, $4, $5, $6)`,
      [exp.createdAt, exp.dataRange.start, exp.dataRange.end, recordCount, index, totalExports],
    );
  } catch (err) {
    // Non-fatal — don't break the pipeline over logging
    console.warn("[union-api] Failed to log export:", err instanceof Error ? err.message : err);
  }
}

/**
 * Data freshness check — returns null if data is fresh (within 2 days),
 * or the date string since which data has been incomplete.
 *
 * Looks at the most recent export's data_range_end to determine the last
 * date Union.fit actually covered. If that's more than 2 calendar days
 * behind today (ET), data is incomplete.
 */
export interface DataFreshness {
  /** True if data is current (within acceptable lag) */
  isFresh: boolean;
  /** The latest date Union data covers (ISO date string) */
  latestDataDate: string;
  /** How many days behind today the data is */
  daysStale: number;
  /** When we last processed an export */
  lastProcessedAt: string;
  /** Number of exports processed in last run */
  exportsProcessed: number;
}

export async function getDataFreshness(): Promise<DataFreshness | null> {
  try {
    const pool = getPool();

    // Get the most recent export log entry (newest data_range_end)
    const { rows } = await pool.query(
      `SELECT data_range_end, created_at, total_exports
       FROM export_log
       WHERE data_range_end IS NOT NULL AND data_range_end != ''
       ORDER BY data_range_end DESC
       LIMIT 1`,
    );

    if (rows.length === 0) return null;

    const row = rows[0] as { data_range_end: string; created_at: string; total_exports: number };
    const latestEnd = row.data_range_end;

    // Parse the end date — could be ISO datetime like "2026-03-08T07:00:44Z"
    const endDate = new Date(latestEnd);
    const now = new Date();

    // Compare in ET
    const endDay = new Date(endDate.toLocaleDateString("en-CA", { timeZone: "America/New_York" }));
    const todayET = new Date(now.toLocaleDateString("en-CA", { timeZone: "America/New_York" }));
    const daysStale = Math.floor((todayET.getTime() - endDay.getTime()) / (24 * 60 * 60 * 1000));

    return {
      isFresh: daysStale <= 2, // Allow 2 days lag (export generated day-of covers through yesterday)
      latestDataDate: endDay.toISOString().slice(0, 10),
      daysStale,
      lastProcessedAt: row.created_at,
      exportsProcessed: row.total_exports,
    };
  } catch (err) {
    console.warn("[union-api] Freshness check failed:", err instanceof Error ? err.message : err);
    return null;
  }
}
