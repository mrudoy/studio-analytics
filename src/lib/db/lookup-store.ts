/**
 * Persistent cache for Union.fit lookup tables (pass_types, revenue_categories).
 *
 * Full data exports include populated pass_types.csv and revenue_categories.csv,
 * but daily exports have these as header-only. This store caches the lookup data
 * from full exports so daily pipeline runs can resolve revenue categories.
 */

import { getPool } from "./database";

// ── Table creation ─────────────────────────────────────────────

export async function ensureLookupTables(): Promise<void> {
  const pool = getPool();
  await pool.query(`
    CREATE TABLE IF NOT EXISTS pass_type_lookups (
      id TEXT PRIMARY KEY,
      name TEXT,
      revenue_category_id TEXT,
      fees_outside BOOLEAN DEFAULT FALSE,
      created_at TEXT,
      cached_at TIMESTAMPTZ DEFAULT NOW()
    );
    CREATE TABLE IF NOT EXISTS revenue_category_lookups (
      id TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      cached_at TIMESTAMPTZ DEFAULT NOW()
    );
  `);
}

// ── Save functions ─────────────────────────────────────────────

export async function savePassTypeLookups(
  entries: { id: string; name: string | null; revenueCategoryId: string | null; feesOutside: boolean; createdAt: string | null }[]
): Promise<number> {
  if (entries.length === 0) return 0;
  const pool = getPool();
  const client = await pool.connect();
  let upserted = 0;
  try {
    await client.query("BEGIN");
    for (const e of entries) {
      await client.query(
        `INSERT INTO pass_type_lookups (id, name, revenue_category_id, fees_outside, created_at)
         VALUES ($1, $2, $3, $4, $5)
         ON CONFLICT (id) DO UPDATE SET
           name = EXCLUDED.name,
           revenue_category_id = EXCLUDED.revenue_category_id,
           fees_outside = EXCLUDED.fees_outside,
           cached_at = NOW()`,
        [e.id, e.name, e.revenueCategoryId, e.feesOutside, e.createdAt]
      );
      upserted++;
    }
    await client.query("COMMIT");
  } catch (err) {
    await client.query("ROLLBACK");
    throw err;
  } finally {
    client.release();
  }
  console.log(`[lookup-store] Pass type lookups: ${upserted} entries upserted`);
  return upserted;
}

export async function saveRevenueCategoryLookups(
  entries: { id: string; name: string }[]
): Promise<number> {
  if (entries.length === 0) return 0;
  const pool = getPool();
  const client = await pool.connect();
  let upserted = 0;
  try {
    await client.query("BEGIN");
    for (const e of entries) {
      await client.query(
        `INSERT INTO revenue_category_lookups (id, name)
         VALUES ($1, $2)
         ON CONFLICT (id) DO UPDATE SET
           name = EXCLUDED.name,
           cached_at = NOW()`,
        [e.id, e.name]
      );
      upserted++;
    }
    await client.query("COMMIT");
  } catch (err) {
    await client.query("ROLLBACK");
    throw err;
  } finally {
    client.release();
  }
  console.log(`[lookup-store] Revenue category lookups: ${upserted} entries upserted`);
  return upserted;
}

// ── Load functions ─────────────────────────────────────────────

export interface CachedPassType {
  id: string;
  name: string | null;
  revenueCategoryId: string | null;
  feesOutside: boolean;
}

export interface CachedRevenueCategory {
  id: string;
  name: string;
}

export async function loadPassTypeLookups(): Promise<Map<string, CachedPassType>> {
  const pool = getPool();
  const { rows } = await pool.query(
    `SELECT id, name, revenue_category_id, fees_outside FROM pass_type_lookups`
  );
  const map = new Map<string, CachedPassType>();
  for (const r of rows) {
    map.set(r.id, {
      id: r.id,
      name: r.name,
      revenueCategoryId: r.revenue_category_id,
      feesOutside: r.fees_outside ?? false,
    });
  }
  console.log(`[lookup-store] Loaded ${map.size} pass type lookups from cache`);
  return map;
}

export async function loadRevenueCategoryLookups(): Promise<Map<string, CachedRevenueCategory>> {
  const pool = getPool();
  const { rows } = await pool.query(
    `SELECT id, name FROM revenue_category_lookups`
  );
  const map = new Map<string, CachedRevenueCategory>();
  for (const r of rows) {
    map.set(r.id, { id: r.id, name: r.name });
  }
  console.log(`[lookup-store] Loaded ${map.size} revenue category lookups from cache`);
  return map;
}
