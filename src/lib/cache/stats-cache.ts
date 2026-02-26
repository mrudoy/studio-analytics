/**
 * In-memory response cache for /api/stats.
 *
 * Data only changes when the pipeline runs (~once/day), so caching the full
 * JSON response avoids re-running expensive DB queries on every page load.
 *
 * TTL defaults to 15 minutes as a safety net — the primary invalidation
 * mechanism is an explicit `invalidateStatsCache()` call from the pipeline
 * worker on completion.
 */

const DEFAULT_TTL_MS = 15 * 60 * 1000; // 15 minutes

interface CacheEntry {
  data: unknown;
  createdAt: number;
  ttlMs: number;
}

let cached: CacheEntry | null = null;

/** Return cached response or null if expired/missing. */
export function getStatsCache(): unknown | null {
  if (!cached) return null;
  if (Date.now() - cached.createdAt > cached.ttlMs) {
    cached = null;
    return null;
  }
  return cached.data;
}

/** Store a response in the cache. */
export function setStatsCache(data: unknown, ttlMs = DEFAULT_TTL_MS): void {
  cached = { data, createdAt: Date.now(), ttlMs };
}

/** Clear the cache (called after pipeline completion). */
export function invalidateStatsCache(): void {
  cached = null;
  console.log("[stats-cache] Cache invalidated");
}

/** Return age in seconds (for freshness header), or null if no cache. */
export function getStatsCacheAge(): number | null {
  if (!cached) return null;
  return Math.round((Date.now() - cached.createdAt) / 1000);
}
