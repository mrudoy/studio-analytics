/**
 * Seed the pass_email_cache table from a full Union.fit historical export.
 *
 * The daily API export only includes ~346 active passes, but the full export
 * has 60K+ passes and 28K memberships. This script joins passes → memberships
 * to build pass_id → email mappings, then bulk-upserts them into the cache.
 *
 * After seeding, it runs the backfill to resolve ghost registrations.
 *
 * Usage:
 *   npx tsx scripts/seed-pass-email-cache.ts [export-dir]
 *
 * Defaults to the known full export path if no argument is provided.
 */

import { Pool } from "pg";
import { createReadStream } from "fs";
import { createInterface } from "readline";
import path from "path";

const DEFAULT_EXPORT_DIR =
  "/Users/mike.rudoy_old/Downloads/union_data_export-sky-ting-20230830-20260223-zFoHME8GZ2ESfu8LzvDe27NU";

const exportDir = process.argv[2] || DEFAULT_EXPORT_DIR;

// ── CSV parser (simple, handles quoted fields) ──

function parseCSVLine(line: string): string[] {
  const fields: string[] = [];
  let current = "";
  let inQuotes = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (inQuotes) {
      if (ch === '"') {
        if (i + 1 < line.length && line[i + 1] === '"') {
          current += '"';
          i++;
        } else {
          inQuotes = false;
        }
      } else {
        current += ch;
      }
    } else {
      if (ch === '"') {
        inQuotes = true;
      } else if (ch === ",") {
        fields.push(current);
        current = "";
      } else {
        current += ch;
      }
    }
  }
  fields.push(current);
  return fields;
}

async function readCSV(filePath: string): Promise<{ headers: string[]; rows: string[][] }> {
  const rl = createInterface({ input: createReadStream(filePath, "utf-8"), crlfDelay: Infinity });
  let headers: string[] = [];
  const rows: string[][] = [];
  let isFirst = true;
  let partial = "";

  for await (const rawLine of rl) {
    // Handle multi-line quoted fields
    const line = partial + rawLine;
    const quoteCount = (line.match(/"/g) || []).length;
    if (quoteCount % 2 !== 0) {
      partial = line + "\n";
      continue;
    }
    partial = "";

    if (isFirst) {
      headers = parseCSVLine(line);
      isFirst = false;
      continue;
    }
    rows.push(parseCSVLine(line));
  }
  return { headers, rows };
}

function colIndex(headers: string[], name: string): number {
  const idx = headers.indexOf(name);
  if (idx === -1) throw new Error(`Column "${name}" not found in headers: ${headers.join(", ")}`);
  return idx;
}

async function main() {
  console.log(`[seed] Export dir: ${exportDir}`);

  // ── 1. Read memberships.csv → build membershipId → {email, firstName, lastName} ──
  console.log("[seed] Reading memberships.csv...");
  const memberships = await readCSV(path.join(exportDir, "memberships.csv"));
  const mIdIdx = colIndex(memberships.headers, "id");
  const mEmailIdx = colIndex(memberships.headers, "email");
  const mFirstIdx = colIndex(memberships.headers, "first_name");
  const mLastIdx = colIndex(memberships.headers, "last_name");

  const memberMap = new Map<string, { email: string; firstName: string; lastName: string }>();
  for (const row of memberships.rows) {
    const id = row[mIdIdx];
    const email = row[mEmailIdx]?.toLowerCase().trim();
    if (!id || !email) continue;
    memberMap.set(id, {
      email,
      firstName: row[mFirstIdx] || "",
      lastName: row[mLastIdx] || "",
    });
  }
  console.log(`[seed] ${memberMap.size} memberships with email`);

  // ── 2. Read passes.csv → join with memberships ──
  console.log("[seed] Reading passes.csv...");
  const passes = await readCSV(path.join(exportDir, "passes.csv"));
  const pIdIdx = colIndex(passes.headers, "id");
  const pMemberIdx = colIndex(passes.headers, "membership_id");
  const pNameIdx = colIndex(passes.headers, "name");

  type CacheEntry = {
    passId: string;
    membershipId: string;
    email: string;
    firstName: string;
    lastName: string;
    passName: string;
  };

  const entries: CacheEntry[] = [];
  let noMembership = 0;
  let noEmail = 0;

  for (const row of passes.rows) {
    const passId = row[pIdIdx];
    const membershipId = row[pMemberIdx];
    const passName = row[pNameIdx] || "";

    if (!passId) continue;
    if (!membershipId) {
      noMembership++;
      continue;
    }

    const member = memberMap.get(membershipId);
    if (!member || !member.email) {
      noEmail++;
      continue;
    }

    entries.push({
      passId,
      membershipId,
      email: member.email,
      firstName: member.firstName,
      lastName: member.lastName,
      passName,
    });
  }

  console.log(`[seed] ${entries.length} passes with email (${noMembership} no membership_id, ${noEmail} no email)`);

  // ── 3. Connect to DB and bulk upsert ──
  const dbUrl = process.env.DATABASE_URL;
  if (!dbUrl) throw new Error("DATABASE_URL not set");

  const isRailway = dbUrl.includes("railway");
  const pool = new Pool({
    connectionString: dbUrl,
    max: 4,
    ssl: isRailway ? { rejectUnauthorized: false } : undefined,
    statement_timeout: 120_000, // 2 min for large batches
  });

  // Ensure table exists
  await pool.query(`
    CREATE TABLE IF NOT EXISTS pass_email_cache (
      pass_id TEXT PRIMARY KEY,
      membership_id TEXT NOT NULL DEFAULT '',
      email TEXT NOT NULL,
      first_name TEXT DEFAULT '',
      last_name TEXT DEFAULT '',
      pass_name TEXT DEFAULT '',
      cached_at TIMESTAMPTZ DEFAULT NOW()
    )
  `);

  // Batch upsert in chunks of 500
  const BATCH_SIZE = 500;
  let upserted = 0;
  const totalBatches = Math.ceil(entries.length / BATCH_SIZE);

  for (let i = 0; i < entries.length; i += BATCH_SIZE) {
    const batch = entries.slice(i, i + BATCH_SIZE);
    const batchNum = Math.floor(i / BATCH_SIZE) + 1;

    // Build a multi-row VALUES clause
    const values: unknown[] = [];
    const placeholders: string[] = [];
    for (let j = 0; j < batch.length; j++) {
      const e = batch[j];
      const offset = j * 6;
      placeholders.push(`($${offset + 1}, $${offset + 2}, $${offset + 3}, $${offset + 4}, $${offset + 5}, $${offset + 6})`);
      values.push(e.passId, e.membershipId, e.email, e.firstName, e.lastName, e.passName);
    }

    await pool.query(
      `INSERT INTO pass_email_cache (pass_id, membership_id, email, first_name, last_name, pass_name)
       VALUES ${placeholders.join(", ")}
       ON CONFLICT (pass_id) DO UPDATE SET
         email = EXCLUDED.email,
         first_name = EXCLUDED.first_name,
         last_name = EXCLUDED.last_name,
         pass_name = EXCLUDED.pass_name,
         cached_at = NOW()`,
      values
    );
    upserted += batch.length;

    if (batchNum % 20 === 0 || batchNum === totalBatches) {
      console.log(`[seed] Batch ${batchNum}/${totalBatches} — ${upserted}/${entries.length} upserted`);
    }
  }

  console.log(`[seed] Cache seeded: ${upserted} pass→email mappings`);

  // ── 4. Verify cache size ──
  const { rows: countRows } = await pool.query(`SELECT COUNT(*) as cnt FROM pass_email_cache`);
  console.log(`[seed] Total cache entries: ${countRows[0].cnt}`);

  // ── 5. Run backfill on ghost registrations ──
  console.log("[seed] Running backfill on empty-email registrations...");

  // Strategy 1: pass_id → cache lookup
  const cacheResult = await pool.query(`
    UPDATE registrations r
    SET email = c.email,
        first_name = COALESCE(NULLIF(r.first_name, ''), c.first_name),
        last_name = COALESCE(NULLIF(r.last_name, ''), c.last_name),
        pass = COALESCE(NULLIF(r.pass, ''), c.pass_name)
    FROM pass_email_cache c
    WHERE r.email = ''
      AND r.pass_id IS NOT NULL AND r.pass_id <> ''
      AND r.pass_id = c.pass_id
      AND c.email <> ''
      AND NOT EXISTS (
        SELECT 1 FROM registrations dup
        WHERE dup.email = c.email AND dup.attended_at = r.attended_at AND dup.id <> r.id
      )
  `);
  const cacheFixed = cacheResult.rowCount ?? 0;
  console.log(`[seed] Backfill via cache: ${cacheFixed} registrations resolved`);

  // Cleanup: remove empty-email duplicates that now have a real counterpart
  const cleanupResult = await pool.query(`
    DELETE FROM registrations r
    WHERE r.email = ''
      AND r.pass_id IS NOT NULL AND r.pass_id <> ''
      AND EXISTS (
        SELECT 1 FROM pass_email_cache c
        WHERE c.pass_id = r.pass_id AND c.email <> ''
          AND EXISTS (
            SELECT 1 FROM registrations dup
            WHERE dup.email = c.email AND dup.attended_at = r.attended_at AND dup.id <> r.id
          )
      )
  `);
  const cleaned = cleanupResult.rowCount ?? 0;
  console.log(`[seed] Cleanup: removed ${cleaned} empty-email duplicates`);

  // Strategy 2: timestamp match
  const tsResult = await pool.query(`
    UPDATE registrations bad
    SET email = good.email,
        first_name = COALESCE(NULLIF(bad.first_name, ''), good.first_name),
        last_name = COALESCE(NULLIF(bad.last_name, ''), good.last_name)
    FROM registrations good
    WHERE bad.email = ''
      AND good.email <> ''
      AND good.attended_at = bad.attended_at
      AND good.event_name = bad.event_name
      AND good.id <> bad.id
      AND NOT EXISTS (
        SELECT 1 FROM registrations dup
        WHERE dup.email = good.email AND dup.attended_at = bad.attended_at AND dup.id <> bad.id
      )
  `);
  const tsFixed = tsResult.rowCount ?? 0;
  console.log(`[seed] Backfill via timestamp: ${tsFixed} registrations resolved`);

  // Cleanup after timestamp match
  const tsCleanup = await pool.query(`
    DELETE FROM registrations r
    WHERE r.email = ''
      AND EXISTS (
        SELECT 1 FROM registrations good
        WHERE good.email <> '' AND good.attended_at = r.attended_at
          AND good.event_name = r.event_name AND good.id <> r.id
      )
  `);
  const tsCleaned = tsCleanup.rowCount ?? 0;
  console.log(`[seed] Timestamp cleanup: removed ${tsCleaned} empty-email duplicates`);

  // ── 6. Final stats ──
  const { rows: stats } = await pool.query(`
    SELECT
      COUNT(*) as total,
      COUNT(*) FILTER (WHERE email <> '') as with_email,
      COUNT(*) FILTER (WHERE email = '') as without_email
    FROM registrations
  `);
  const s = stats[0];
  const pct = ((s.with_email / s.total) * 100).toFixed(1);
  console.log(`[seed] Final: ${s.with_email}/${s.total} registrations with email (${pct}%)`);
  console.log(`[seed] Remaining without email: ${s.without_email}`);

  // Check Zoe specifically
  const { rows: zoeRows } = await pool.query(`
    SELECT COUNT(*) as cnt, MAX(attended_at) as latest
    FROM registrations WHERE email = 'zoealtus@gmail.com'
  `);
  console.log(`[seed] Zoe Altus: ${zoeRows[0].cnt} registrations, latest: ${zoeRows[0].latest}`);

  // ── 7. Bump data version so the dashboard cache auto-invalidates ──
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
  const { rows: vRows } = await pool.query(`
    UPDATE data_version SET version = version + 1, updated_at = NOW()
    WHERE id = 1 RETURNING version
  `);
  console.log(`[seed] Data version bumped to ${vRows[0]?.version} — dashboard cache will auto-invalidate`);

  await pool.end();
  console.log("[seed] Done.");
}

main().catch((err) => {
  console.error("[seed] Fatal:", err);
  process.exit(1);
});
