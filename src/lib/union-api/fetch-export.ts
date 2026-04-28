import { getWatermark, setWatermark } from "../db/watermark-store";
import type { Watermark } from "../db/watermark-store";
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
 * Filter exports to only those newer than the watermark.
 * If the watermark is stale (>24h) and no new exports exist, forces the latest.
 * Always returns a fresh array — never aliases the input.
 */
export function filterNewExports(
  allExports: FetchExportResult[],
  wm: Watermark | null,
  logPrefix: string,
): FetchExportResult[] {
  const lastProcessedAt = wm?.highWaterDate ?? null;
  let result = lastProcessedAt
    ? allExports.filter((e) => new Date(e.createdAt).getTime() > new Date(lastProcessedAt).getTime())
    : allExports.slice();

  if (result.length === 0 && allExports.length > 0 && wm?.lastFetchedAt) {
    const hoursSince = (Date.now() - wm.lastFetchedAt.getTime()) / (1000 * 60 * 60);
    if (hoursSince > 24) {
      console.warn(`[${logPrefix}] Watermark stale (${hoursSince.toFixed(1)}h) — force-processing latest export`);
      result = [allExports[0]];
    }
  }
  return result;
}

/**
 * Mark an API export as processed (set watermark).
 */
export async function markExportProcessed(createdAt: string, recordCount: number): Promise<void> {
  await setWatermark(WATERMARK_KEY, createdAt, recordCount, "Union Data Exporter API");
}

/**
 * Track the newest successfully-processed export across a pipeline run.
 *
 * Use this instead of gating `markExportProcessed` on `i === 0` of the loop:
 * if the newest Union export fails (expired URL, transient parse error, etc.)
 * but an older one succeeds, the watermark must still advance to that older
 * one. Otherwise, on every subsequent run, the same newest export keeps
 * failing and the watermark stays pinned forever.
 *
 * Compare on `new Date(...).getTime()` — Union's createdAt is an ISO string,
 * but this is robust against any sort-order quirks if format ever changes.
 */
export class WatermarkTracker {
  private newest: { createdAt: string; recordCount: number } | null = null;

  observe(createdAt: string, recordCount: number): void {
    if (!createdAt) return;
    const t = new Date(createdAt).getTime();
    if (!Number.isFinite(t)) return;
    if (!this.newest || t > new Date(this.newest.createdAt).getTime()) {
      this.newest = { createdAt, recordCount };
    }
  }

  async commit(): Promise<{ createdAt: string; recordCount: number } | null> {
    if (!this.newest) return null;
    await markExportProcessed(this.newest.createdAt, this.newest.recordCount);
    return this.newest;
  }
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
 * Uses the unionApiExport watermark's high_water_date as the primary freshness
 * signal. The watermark is updated via UPSERT by markExportProcessed() on every
 * successful pipeline run, so it is reliable even if logExport() fails silently.
 * export_log is consulted only for the display date (data_range_end coverage).
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

    // Run both queries in parallel
    const [wmResult, logResult] = await Promise.all([
      // Primary signal: when did the pipeline last successfully process a Union export?
      // markExportProcessed() uses an UPSERT, so this is always current even if
      // logExport()'s plain INSERT fails silently.
      pool.query(
        `SELECT high_water_date, last_fetched_at
         FROM fetch_watermarks
         WHERE report_type = 'unionApiExport'`,
      ),
      // For display only: take the MAX coverage end date Union has ever reported.
      // data_range_end is non-monotonic — different exports report different
      // coverage windows, so picking "the latest row" gives misleading results.
      // MAX(data_range_end) reflects the furthest point in time our data covers.
      pool.query(
        `SELECT MAX(data_range_end) AS max_data_range_end,
                MAX(created_at) AS last_logged_at,
                COUNT(*) AS total_rows
         FROM export_log
         WHERE data_range_end IS NOT NULL AND data_range_end != ''`,
      ),
    ]);

    const wm = wmResult.rows[0] as { high_water_date: string | null; last_fetched_at: string } | undefined;
    const logAgg = logResult.rows[0] as { max_data_range_end: string | null; last_logged_at: string | null; total_rows: string } | undefined;

    // Need at least one signal to report freshness
    if (!wm?.high_water_date && !logAgg?.max_data_range_end) return null;

    const now = new Date();
    const todayET = new Date(now.toLocaleDateString("en-CA", { timeZone: "America/New_York" }));

    // Staleness = how long since the pipeline last successfully processed a Union
    // export (watermark.high_water_date is the createdAt of that newest export).
    // Fall back to MAX(data_range_end) only if no watermark exists yet.
    const freshnessDateStr = wm?.high_water_date ?? logAgg?.max_data_range_end ?? "";
    if (!freshnessDateStr) return null;

    const freshnessDate = new Date(freshnessDateStr);
    const freshnessDay = new Date(freshnessDate.toLocaleDateString("en-CA", { timeZone: "America/New_York" }));
    const daysStale = Math.floor((todayET.getTime() - freshnessDay.getTime()) / (24 * 60 * 60 * 1000));

    // Display date for the "covers through X" line: the furthest data point we
    // have. Prefer MAX(data_range_end); fall back to the watermark date.
    const displayDateStr = logAgg?.max_data_range_end ?? freshnessDateStr;
    const displayDate = new Date(displayDateStr);
    const displayDay = new Date(displayDate.toLocaleDateString("en-CA", { timeZone: "America/New_York" }));

    return {
      isFresh: daysStale <= 2,
      latestDataDate: displayDay.toISOString().slice(0, 10),
      daysStale,
      lastProcessedAt: logAgg?.last_logged_at ?? wm?.last_fetched_at ?? "",
      exportsProcessed: logAgg?.total_rows ? Number(logAgg.total_rows) : 0,
    };
  } catch (err) {
    console.warn("[union-api] Freshness check failed:", err instanceof Error ? err.message : err);
    return null;
  }
}
