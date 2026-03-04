/**
 * In-memory response cache for /api/stats.
 *
 * Data only changes when the pipeline runs (~once/day), so caching the full
 * JSON response avoids re-running expensive DB queries on every page load.
 *
 * Uses a DB-based version stamp (`data_version` table) for automatic
 * invalidation. ANY process that writes to the DB — pipeline worker, seed
 * scripts, manual SQL — calls `bumpDataVersion()` and the cache self-
 * invalidates on the next request. No in-process signaling needed.
 *
 * TTL is a safety net; primary invalidation is version-based.
 *
 * Uses `globalThis` instead of module-level variables because Next.js may
 * create separate module instances for different request contexts.
 */

import { getPool } from "../db/database";

const DEFAULT_TTL_MS = 15 * 60 * 1000; // 15 minutes

const CACHE_KEY = "__stats_cache__" as const;

interface CacheEntry {
  data: unknown;
  createdAt: number;
  ttlMs: number;
  dataVersion: number; // DB version stamp at cache time
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
const g = globalThis as any;

function getEntry(): CacheEntry | null {
  return g[CACHE_KEY] ?? null;
}

function setEntry(entry: CacheEntry | null): void {
  g[CACHE_KEY] = entry;
}

// ── DB version stamp ──────────────────────────────────────

/** Ensure the data_version table exists (single row). */
export async function ensureDataVersionTable(): Promise<void> {
  const pool = getPool();
  await pool.query(`
    CREATE TABLE IF NOT EXISTS data_version (
      id INTEGER PRIMARY KEY DEFAULT 1 CHECK (id = 1),
      version INTEGER NOT NULL DEFAULT 1,
      updated_at TIMESTAMPTZ DEFAULT NOW()
    )
  `);
  await pool.query(`
    INSERT INTO data_version (id, version) VALUES (1, 1)
    ON CONFLICT (id) DO NOTHING
  `);
}

/**
 * Bump the DB data version. Call this after ANY data write operation
 * (pipeline import, backfill, seed script, manual SQL).
 * The next cache check will see the new version and invalidate.
 */
export async function bumpDataVersion(): Promise<number> {
  const pool = getPool();
  const { rows } = await pool.query(`
    UPDATE data_version SET version = version + 1, updated_at = NOW()
    WHERE id = 1 RETURNING version
  `);
  const v = rows[0]?.version ?? 0;
  console.log(`[stats-cache] Data version bumped to ${v}`);
  return v;
}

/** Read current DB version (lightweight single-row query). */
async function getDbVersion(): Promise<number> {
  try {
    const pool = getPool();
    const { rows } = await pool.query(`SELECT version FROM data_version WHERE id = 1`);
    return rows[0]?.version ?? 0;
  } catch {
    // Table may not exist yet — treat as version 0
    return 0;
  }
}

// ── Cache API ─────────────────────────────────────────────

/**
 * Return cached response or null if expired/stale.
 * Checks the DB version stamp — if it changed since caching, invalidate.
 */
export async function getStatsCache(): Promise<unknown | null> {
  const cached = getEntry();
  if (!cached) return null;

  // TTL check
  if (Date.now() - cached.createdAt > cached.ttlMs) {
    setEntry(null);
    return null;
  }

  // Version check — one tiny query per cache hit
  const dbVersion = await getDbVersion();
  if (dbVersion !== cached.dataVersion) {
    console.log(`[stats-cache] Version mismatch (cached=${cached.dataVersion}, db=${dbVersion}) — invalidating`);
    setEntry(null);
    return null;
  }

  return cached.data;
}

/** Store a response in the cache with the current DB version. */
export async function setStatsCache(data: unknown, ttlMs = DEFAULT_TTL_MS): Promise<void> {
  const dbVersion = await getDbVersion();
  setEntry({ data, createdAt: Date.now(), ttlMs, dataVersion: dbVersion });
  console.log(`[stats-cache] Cache populated (version=${dbVersion})`);
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
