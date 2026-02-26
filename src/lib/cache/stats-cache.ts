/**
 * In-memory response cache for /api/stats.
 *
 * Data only changes when the pipeline runs (~once/day), so caching the full
 * JSON response avoids re-running expensive DB queries on every page load.
 *
 * TTL defaults to 15 minutes as a safety net — the primary invalidation
 * mechanism is an explicit `invalidateStatsCache()` call from the pipeline
 * worker on completion.
 *
 * Uses `globalThis` instead of module-level variables because Next.js may
 * create separate module instances for different request contexts.
 */

const DEFAULT_TTL_MS = 15 * 60 * 1000; // 15 minutes

const CACHE_KEY = "__stats_cache__" as const;

interface CacheEntry {
  data: unknown;
  createdAt: number;
  ttlMs: number;
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
const g = globalThis as any;

function getEntry(): CacheEntry | null {
  return g[CACHE_KEY] ?? null;
}

function setEntry(entry: CacheEntry | null): void {
  g[CACHE_KEY] = entry;
}

/** Return cached response or null if expired/missing. */
export function getStatsCache(): unknown | null {
  const cached = getEntry();
  if (!cached) return null;
  if (Date.now() - cached.createdAt > cached.ttlMs) {
    setEntry(null);
    return null;
  }
  return cached.data;
}

/** Store a response in the cache. */
export function setStatsCache(data: unknown, ttlMs = DEFAULT_TTL_MS): void {
  setEntry({ data, createdAt: Date.now(), ttlMs });
  console.log("[stats-cache] Cache populated");
}

/** Clear the cache (called after pipeline completion). */
export function invalidateStatsCache(): void {
  setEntry(null);
  console.log("[stats-cache] Cache invalidated");
}

/** Return age in seconds (for freshness header), or null if no cache. */
export function getStatsCacheAge(): number | null {
  const cached = getEntry();
  if (!cached) return null;
  return Math.round((Date.now() - cached.createdAt) / 1000);
}
