import { getWatermark, setWatermark } from "../db/watermark-store";

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
