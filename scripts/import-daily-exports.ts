/**
 * Fast batch import of daily Union.fit zip exports.
 * Uses multi-row INSERTs (200 rows/query) instead of row-by-row.
 *
 * Usage:
 *   npx tsx scripts/import-daily-exports.ts export1.zip [export2.zip ...]
 */

import dotenv from "dotenv";
import { resolve, join } from "path";

dotenv.config({ path: resolve(__dirname, "../.env.production.local") });
dotenv.config({ path: resolve(__dirname, "../.env.local") });
dotenv.config({ path: resolve(__dirname, "../.env") });

import { existsSync, readdirSync } from "fs";
import { execSync } from "child_process";
import { tmpdir } from "os";
import { mkdtempSync } from "fs";
import { Pool } from "pg";
import { ZipTransformer } from "../src/lib/email/zip-transformer";
import { parseCSV } from "../src/lib/parser/csv-parser";
import {
  RawMembershipSchema, RawPassSchema, RawEventSchema,
  RawPerformanceSchema, RawLocationSchema, RawOrderSchema,
  RawRegistrationSchema, RawRefundSchema, RawTransferSchema,
} from "../src/lib/email/zip-schemas";
import { saveAutoRenews } from "../src/lib/db/auto-renew-store";
import { saveRevenueCategories, isMonthLocked } from "../src/lib/db/revenue-store";
import { bumpDataVersion } from "../src/lib/cache/stats-cache";
import { initDatabase, getPool } from "../src/lib/db/database";
import { runMigrations } from "../src/lib/db/migrations";

const BATCH_SIZE = 200;

/* ── Batch registration insert ──────────────────────────────── */

async function batchUpsertRegistrations(pool: Pool, rows: any[]): Promise<number> {
  if (rows.length === 0) return 0;
  let total = 0;

  // Pure upsert — no DELETE. For rows with a union_registration_id, try
  // UPDATE-by-id first (handles (email, attended_at) drift across runs).
  // If UPDATE hits 0 rows, fall through to INSERT ... ON CONFLICT.
  // This preserves NEVER DELETE DATA (no rows ever removed).
  const client = await pool.connect();
  try {
    await client.query(`SET statement_timeout = '120s'`);

    for (const r of rows) {
      const email = (r.email || "").toLowerCase();
      const vals = [
        r.eventName, r.eventId || null, r.performanceId || null, r.performanceStartsAt || null,
        r.locationName, r.videoName || null, r.videoId || null, r.teacherName,
        r.firstName, r.lastName, email, r.phone || null, r.role || null,
        r.registeredAt || null, r.canceledAt || null, r.attendedAt || null,
        r.registrationType, r.state, r.pass, r.subscription, r.revenueState || null,
        r.revenue, r.unionRegistrationId || null, r.passId || null,
      ];

      if (r.unionRegistrationId) {
        const updated = await client.query(
          `UPDATE registrations SET
             event_name = COALESCE($1, event_name),
             event_id = COALESCE($2, event_id),
             performance_id = COALESCE($3, performance_id),
             performance_starts_at = COALESCE($4, performance_starts_at),
             location_name = COALESCE($5, location_name),
             video_name = COALESCE($6, video_name),
             video_id = COALESCE($7, video_id),
             teacher_name = COALESCE($8, teacher_name),
             first_name = COALESCE($9, first_name),
             last_name = COALESCE($10, last_name),
             email = COALESCE($11, email),
             phone = COALESCE($12, phone),
             role = COALESCE($13, role),
             registered_at = COALESCE($14, registered_at),
             canceled_at = COALESCE($15, canceled_at),
             attended_at = COALESCE($16, attended_at),
             registration_type = COALESCE($17, registration_type),
             state = COALESCE($18, state),
             pass = COALESCE($19, pass),
             subscription = COALESCE($20, subscription),
             revenue_state = COALESCE($21, revenue_state),
             revenue = COALESCE($22, revenue),
             pass_id = COALESCE(NULLIF($24, ''), pass_id)
           WHERE union_registration_id = $23`,
          vals
        );
        if (updated.rowCount && updated.rowCount > 0) {
          total += updated.rowCount;
          continue;
        }
      }

      const result = await client.query(
        `INSERT INTO registrations (
          event_name, event_id, performance_id, performance_starts_at,
          location_name, video_name, video_id, teacher_name,
          first_name, last_name, email, phone, role,
          registered_at, canceled_at, attended_at,
          registration_type, state, pass, subscription, revenue_state, revenue,
          union_registration_id, pass_id
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24)
        ON CONFLICT (email, attended_at) DO UPDATE SET
          event_name = COALESCE(EXCLUDED.event_name, registrations.event_name),
          event_id = COALESCE(EXCLUDED.event_id, registrations.event_id),
          performance_id = COALESCE(EXCLUDED.performance_id, registrations.performance_id),
          performance_starts_at = COALESCE(EXCLUDED.performance_starts_at, registrations.performance_starts_at),
          location_name = COALESCE(EXCLUDED.location_name, registrations.location_name),
          video_name = COALESCE(EXCLUDED.video_name, registrations.video_name),
          video_id = COALESCE(EXCLUDED.video_id, registrations.video_id),
          phone = COALESCE(EXCLUDED.phone, registrations.phone),
          role = COALESCE(EXCLUDED.role, registrations.role),
          canceled_at = COALESCE(EXCLUDED.canceled_at, registrations.canceled_at),
          revenue_state = COALESCE(EXCLUDED.revenue_state, registrations.revenue_state),
          revenue = COALESCE(EXCLUDED.revenue, registrations.revenue),
          union_registration_id = COALESCE(EXCLUDED.union_registration_id, registrations.union_registration_id),
          pass_id = COALESCE(NULLIF(EXCLUDED.pass_id, ''), registrations.pass_id)`,
        vals
      );
      total += result.rowCount ?? 0;
    }
  } finally {
    client.release();
  }

  return total;
}

/* ── Batch order insert ─────────────────────────────────────── */

async function batchUpsertOrders(pool: Pool, rows: any[]): Promise<number> {
  if (rows.length === 0) return 0;
  let total = 0;

  for (let i = 0; i < rows.length; i += BATCH_SIZE) {
    const batch = rows.slice(i, i + BATCH_SIZE);
    const values: any[] = [];
    const placeholders: string[] = [];

    for (let j = 0; j < batch.length; j++) {
      const r = batch[j];
      const o = j * 14;
      placeholders.push(`($${o+1},$${o+2},$${o+3},$${o+4},$${o+5},$${o+6},$${o+7},$${o+8},$${o+9},$${o+10},$${o+11},$${o+12},$${o+13},$${o+14})`);
      values.push(
        r.unionOrderId, r.completedAt || null, r.email?.toLowerCase() || null,
        r.firstName || null, r.lastName || null, r.eventName || null,
        r.passName || null, r.total ?? 0, r.payment || null,
        r.state || null, r.subscriptionPassId || null,
        r.locationName || null, r.salesChannel || null, r.isFirstVisit ?? false,
      );
    }

    const client = await pool.connect();
    try {
      await client.query(`SET statement_timeout = '60s'`);
      const result = await client.query(
        `INSERT INTO orders (
          union_order_id, completed_at, email, first_name, last_name, event_name,
          pass_name, total, payment, state, subscription_pass_id,
          location_name, sales_channel, is_first_visit
        ) VALUES ${placeholders.join(",")}
        ON CONFLICT (union_order_id) DO UPDATE SET
          completed_at = COALESCE(EXCLUDED.completed_at, orders.completed_at),
          email = COALESCE(EXCLUDED.email, orders.email),
          total = EXCLUDED.total, state = EXCLUDED.state`,
        values
      );
      total += result.rowCount ?? 0;
    } finally {
      client.release();
    }
  }

  return total;
}

/* ── Process one export ─────────────────────────────────────── */

async function processExport(zipPath: string): Promise<void> {
  const name = zipPath.split("/").pop()!;
  console.log(`\nProcessing: ${name}`);
  const t0 = Date.now();

  // Extract
  const tmpDir = mkdtempSync(join(tmpdir(), "union-"));
  execSync(`unzip -o "${zipPath}" -d "${tmpDir}"`, { stdio: "pipe" });
  const files = readdirSync(tmpDir).filter((f) => f.endsWith(".csv"));
  const fileMap = new Map<string, string>();
  for (const f of files) fileMap.set(f.toLowerCase().replace(/\.csv$/, ""), join(tmpDir, f));
  console.log(`  ${files.length} CSVs`);

  const pool = getPool();

  // Parse lookup CSVs
  const parse = <T>(key: string, schema: any) => fileMap.has(key) ? parseCSV(fileMap.get(key)!, schema).data : [];
  const memberships = parse("memberships", RawMembershipSchema);
  const passes = parse("passes", RawPassSchema);
  const events = parse("events", RawEventSchema);
  const performances = parse("performances", RawPerformanceSchema);
  const locations = parse("locations", RawLocationSchema);

  const transformer = new ZipTransformer({ memberships, passes, events, performances, locations });

  // Load pass-email cache
  const { rows: cacheRows } = await pool.query("SELECT pass_id, email FROM pass_email_cache");
  transformer.setPassEmailCache(new Map(cacheRows.map((r: any) => [r.pass_id, r.email])));
  console.log(`  Lookups loaded (${memberships.length} members, ${passes.length} passes, ${cacheRows.length} cache)`);

  // Orders
  const rawOrders = parse("orders", RawOrderSchema);
  if (rawOrders.length > 0) {
    const orderRows = transformer.transformOrdersBatch(rawOrders);
    const n = await batchUpsertOrders(pool, orderRows);
    console.log(`  Orders: ${orderRows.length} parsed, ${n} upserted`);
  }

  // Registrations
  const rawRegs = parse("registrations", RawRegistrationSchema);
  if (rawRegs.length > 0) {
    const { rows: regRows } = transformer.transformRegistrationsBatch(rawRegs);
    const n = await batchUpsertRegistrations(pool, regRows);
    console.log(`  Registrations: ${regRows.length} parsed, ${n} upserted`);
  }

  // Auto-renews (small, use existing)
  const autoRenews = transformer.extractAutoRenews();
  if (autoRenews.length > 0) {
    await saveAutoRenews(autoRenews, `batch-${Date.now()}`);
    console.log(`  Auto-renews: ${autoRenews.length}`);
  }

  // Revenue (only save current month from daily exports)
  const rawRefunds = parse("refunds", RawRefundSchema);
  const rawTransfers = parse("transfers", RawTransferSchema);
  const monthlyRevenue = transformer.computeRevenueByCategory(rawOrders, rawRefunds, rawTransfers);
  for (const [month, categories] of monthlyRevenue.entries()) {
    if (!month.startsWith("2026-03")) continue;
    const locked = await isMonthLocked(2026, 3);
    if (locked) { console.log(`  Revenue ${month}: locked`); continue; }
    const lastDay = new Date(2026, 3, 0).getDate();
    await saveRevenueCategories(`${month}-01`, `${month}-${String(lastDay).padStart(2, "0")}`, categories);
    console.log(`  Revenue ${month}: ${categories.length} categories`);
  }

  execSync(`rm -rf "${tmpDir}"`);
  console.log(`  Done in ${((Date.now() - t0) / 1000).toFixed(1)}s`);
}

/* ── Main ───────────────────────────────────────────────────── */

async function main() {
  const zipPaths = process.argv.slice(2);
  if (zipPaths.length === 0) {
    console.error("Usage: npx tsx scripts/import-daily-exports.ts export1.zip [export2.zip ...]");
    process.exit(1);
  }
  for (const p of zipPaths) {
    if (!existsSync(p)) { console.error(`Not found: ${p}`); process.exit(1); }
  }

  console.log(`=== Batch Import: ${zipPaths.length} exports ===`);
  await initDatabase();
  await runMigrations();

  for (const p of zipPaths) await processExport(p);

  await bumpDataVersion();
  console.log("\nDone. Cache invalidated.");
  process.exit(0);
}

main().catch((err) => { console.error("Failed:", err); process.exit(1); });
