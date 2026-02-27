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
    return null;
  }

  // API returns newest first
  const latest = exporters[0];

  console.log(`[union-api] Fetching export: created_at=${latest.created_at}, org=${latest.org}`);
  return {
    downloadUrl: latest.download_url,
    createdAt: latest.created_at,
    dataRange: {
      start: latest.data_updated_starts_at,
      end: latest.data_updated_ends_at,
    },
  };
}

/**
 * Mark an API export as processed (set watermark).
 */
export async function markExportProcessed(createdAt: string, recordCount: number): Promise<void> {
  await setWatermark(WATERMARK_KEY, createdAt, recordCount, "Union Data Exporter API");
}
