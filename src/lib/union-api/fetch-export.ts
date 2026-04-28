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
 * Track + advance the watermark as exports succeed within a pipeline run.
 *
 * Why this exists: all three pipeline entry points used to gate
 * `markExportProcessed` on `i === 0`. Loops are newest-first, so if Union's
 * newest export failed (expired URL, parse error, etc.) but an older one
 * succeeded, data landed in the DB but the watermark never advanced. Every
 * subsequent run hit the same wall and the watermark stayed pinned forever.
 *
 * Behavior:
 * - `observe(createdAt, n)` advances the in-memory + DB watermark
 *   IMMEDIATELY if `createdAt` is newer than what we've already committed
 *   in this run. This way, even if the loop is killed mid-flight (zombie
 *   cleanup, OOM, container restart), every successfully-processed export
 *   is still durably reflected in the watermark.
 * - The newer-than guard prevents regression if observations arrive out of
 *   order.
 *
 * `await observe(...)` after each successful export. No `commit()` step needed.
 */
export class WatermarkTracker {
  private committedCreatedAt: string | null = null;

  async observe(createdAt: string, recordCount: number): Promise<void> {
    if (!createdAt) return;
    const t = new Date(createdAt).getTime();
    if (!Number.isFinite(t)) return;
    if (this.committedCreatedAt && t <= new Date(this.committedCreatedAt).getTime()) {
      // Older than what we already committed — don't regress.
      return;
    }
    await markExportProcessed(createdAt, recordCount);
    this.committedCreatedAt = createdAt;
  }

  /** Returns the newest createdAt this tracker has committed, or null. */
  get committed(): string | null {
    return this.committedCreatedAt;
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
 * Data freshness check — returns null if no signal, otherwise reports
 * how many days behind today the most-recently-processed Union export is.
 *
 * Uses ONE signal end-to-end: the unionApiExport watermark's high_water_date,
 * which is set to Union's `created_at` of the newest export this pipeline
 * has successfully processed. Both `daysStale` and `latestDataDate` derive
 * from it, so the banner can't say contradictory things ("April 7 / 7 days
 * behind"). data_range_end was tried as the freshness signal previously and
 * abandoned — Union reports it non-monotonically across exports, so it's
 * unreliable as a freshness indicator even though it sounds like one.
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

    const [wmResult, logResult] = await Promise.all([
      pool.query(
        `SELECT high_water_date, last_fetched_at
         FROM fetch_watermarks
         WHERE report_type = 'unionApiExport'`,
      ),
      // export_log is informational only — we use it for the row count and the
      // last-logged timestamp, but it does NOT drive isFresh / daysStale.
      pool.query(
        `SELECT MAX(created_at) AS last_logged_at, COUNT(*) AS total_rows
         FROM export_log`,
      ),
    ]);

    const wm = wmResult.rows[0] as { high_water_date: string | null; last_fetched_at: string } | undefined;
    const logAgg = logResult.rows[0] as { last_logged_at: string | null; total_rows: string } | undefined;

    // No watermark = no signal. Return null so the banner doesn't render.
    if (!wm?.high_water_date) return null;

    const now = new Date();
    const todayET = new Date(now.toLocaleDateString("en-CA", { timeZone: "America/New_York" }));

    const freshnessDate = new Date(wm.high_water_date);
    const freshnessDay = new Date(freshnessDate.toLocaleDateString("en-CA", { timeZone: "America/New_York" }));
    const daysStale = Math.floor((todayET.getTime() - freshnessDay.getTime()) / (24 * 60 * 60 * 1000));

    return {
      isFresh: daysStale <= 2,
      latestDataDate: freshnessDay.toISOString().slice(0, 10),
      daysStale,
      lastProcessedAt: logAgg?.last_logged_at ?? wm.last_fetched_at ?? "",
      exportsProcessed: logAgg?.total_rows ? Number(logAgg.total_rows) : 0,
    };
  } catch (err) {
    console.warn("[union-api] Freshness check failed:", err instanceof Error ? err.message : err);
    return null;
  }
}
