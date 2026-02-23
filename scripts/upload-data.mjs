#!/usr/bin/env node
/**
 * Direct data upload script — reads CSV files and bulk inserts into PostgreSQL.
 * Uses temp tables + INSERT...SELECT...ON CONFLICT for speed.
 * Usage: node scripts/upload-data.mjs
 */
import pg from "pg";
import { readFileSync } from "fs";

const DATABASE_URL = process.env.DATABASE_URL;
if (!DATABASE_URL) throw new Error("DATABASE_URL env var is required");

const pool = new pg.Pool({
  connectionString: DATABASE_URL,
  ssl: false,
  statement_timeout: 300000,      // 5 min statement timeout
  query_timeout: 300000,           // 5 min query timeout
  connectionTimeoutMillis: 30000,  // 30s to connect
  idle_in_transaction_session_timeout: 300000,
});

// ── CSV Parser ────────────────────────────────────────────────
function snakeToCamel(s) {
  return s.replace(/_([a-z])/g, (_, c) => c.toUpperCase());
}

function parseCSVLine(line) {
  const fields = [];
  let current = "";
  let inQuotes = false;

  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (ch === '"') {
      if (inQuotes && i + 1 < line.length && line[i + 1] === '"') {
        current += '"';
        i++;
      } else {
        inQuotes = !inQuotes;
      }
    } else if (ch === "," && !inQuotes) {
      fields.push(current);
      current = "";
    } else {
      current += ch;
    }
  }
  fields.push(current);
  return fields;
}

function parseCSVFile(filePath) {
  const content = readFileSync(filePath, "utf8");
  const lines = content.split("\n");
  const rows = [];
  let headers = null;

  for (const rawLine of lines) {
    const line = rawLine.trim();
    if (!line) continue;

    const fields = parseCSVLine(line);

    if (!headers) {
      headers = fields.map((h) => snakeToCamel(h.trim().replace(/^"|"$/g, "")));
      continue;
    }

    const obj = {};
    for (let i = 0; i < headers.length; i++) {
      obj[headers[i]] = (fields[i] || "").replace(/^"|"$/g, "").trim();
    }
    rows.push(obj);
  }

  return rows;
}

function parseMoney(val) {
  if (typeof val === "number") return val;
  if (!val) return 0;
  const cleaned = String(val).replace(/[$,]/g, "").trim();
  const num = parseFloat(cleaned);
  return isNaN(num) ? 0 : num;
}

function parseBool(val) {
  if (typeof val === "boolean") return val;
  if (!val) return false;
  return String(val).trim().toLowerCase() === "true";
}

// Escape a value for tab-delimited COPY format
function escapeCopy(val) {
  if (val === null || val === undefined) return "\\N";
  const s = String(val);
  return s.replace(/\\/g, "\\\\").replace(/\t/g, "\\t").replace(/\n/g, "\\n").replace(/\r/g, "\\r");
}

// ── Upload Registrations (bulk) ───────────────────────────────
async function uploadRegistrations(filePath) {
  console.log(`\n📂 Parsing registrations from: ${filePath}`);
  const allRows = parseCSVFile(filePath);
  console.log(`  Parsed ${allRows.length} rows`);

  const valid = allRows.filter((r) => r.email && r.attendedAt);
  console.log(`  Valid rows (have email + attendedAt): ${valid.length}`);

  const client = await pool.connect();
  try {
    // Create temp table
    await client.query(`
      CREATE TEMP TABLE IF NOT EXISTS tmp_registrations (
        event_name TEXT,
        event_id TEXT,
        performance_id TEXT,
        performance_starts_at TEXT,
        location_name TEXT,
        video_name TEXT,
        video_id TEXT,
        teacher_name TEXT,
        first_name TEXT,
        last_name TEXT,
        email TEXT,
        phone TEXT,
        role TEXT,
        registered_at TEXT,
        canceled_at TEXT,
        attended_at TEXT,
        registration_type TEXT,
        state TEXT,
        pass TEXT,
        subscription TEXT,
        revenue_state TEXT,
        revenue NUMERIC
      )
    `);
    await client.query("TRUNCATE tmp_registrations");

    // Bulk insert via multi-value INSERT in batches
    const BATCH = 500;
    for (let i = 0; i < valid.length; i += BATCH) {
      const slice = valid.slice(i, i + BATCH);
      const values = [];
      const params = [];
      let paramIdx = 1;

      for (const r of slice) {
        const placeholders = [];
        for (let p = 0; p < 22; p++) {
          placeholders.push(`$${paramIdx++}`);
        }
        values.push(`(${placeholders.join(",")})`);
        params.push(
          r.eventName || "",
          r.eventId || null,
          r.performanceId || null,
          r.performanceStartsAt || r.attendedAt,
          r.locationName || "",
          r.videoName || null,
          r.videoId || null,
          r.teacherName || "",
          r.firstName || "",
          r.lastName || "",
          r.email,
          r.phoneNumber || null,
          r.role || null,
          r.registeredAt || null,
          r.canceledAt || null,
          r.attendedAt,
          r.registrationType || "",
          r.state || "",
          r.pass || "",
          String(r.subscription || "false"),
          r.revenueState || null,
          parseMoney(r.revenue),
        );
      }

      await client.query(
        `INSERT INTO tmp_registrations (
          event_name, event_id, performance_id, performance_starts_at,
          location_name, video_name, video_id, teacher_name,
          first_name, last_name, email, phone, role,
          registered_at, canceled_at, attended_at,
          registration_type, state, pass, subscription,
          revenue_state, revenue
        ) VALUES ${values.join(",")}`,
        params
      );

      if ((i + BATCH) % 10000 < BATCH) {
        console.log(`  ... loaded ${Math.min(i + BATCH, valid.length)}/${valid.length} into temp table`);
      }
    }

    console.log(`  Deduplicating temp table...`);

    // Deduplicate: keep last row per (email, attended_at) — use ctid as tie-breaker
    await client.query(`
      DELETE FROM tmp_registrations t1
      USING tmp_registrations t2
      WHERE t1.email = t2.email
        AND t1.attended_at = t2.attended_at
        AND t1.ctid < t2.ctid
    `);

    const dedupCount = await client.query("SELECT COUNT(*) as count FROM tmp_registrations");
    console.log(`  After dedup: ${dedupCount.rows[0].count} unique rows`);

    console.log(`  Merging into registrations table...`);

    // Merge from temp into real table
    const mergeResult = await client.query(`
      INSERT INTO registrations (
        event_name, event_id, performance_id, performance_starts_at,
        location_name, video_name, video_id, teacher_name,
        first_name, last_name, email, phone, role,
        registered_at, canceled_at, attended_at,
        registration_type, state, pass, subscription,
        revenue_state, revenue
      )
      SELECT
        event_name, event_id, performance_id, performance_starts_at,
        location_name, video_name, video_id, teacher_name,
        first_name, last_name, email, phone, role,
        registered_at, canceled_at, attended_at,
        registration_type, state, pass, subscription,
        revenue_state, revenue
      FROM tmp_registrations
      ON CONFLICT (email, attended_at) DO UPDATE SET
        event_name = EXCLUDED.event_name,
        event_id = EXCLUDED.event_id,
        performance_id = EXCLUDED.performance_id,
        performance_starts_at = EXCLUDED.performance_starts_at,
        location_name = EXCLUDED.location_name,
        video_name = EXCLUDED.video_name,
        video_id = EXCLUDED.video_id,
        teacher_name = EXCLUDED.teacher_name,
        first_name = EXCLUDED.first_name,
        last_name = EXCLUDED.last_name,
        phone = EXCLUDED.phone,
        role = EXCLUDED.role,
        registered_at = EXCLUDED.registered_at,
        canceled_at = EXCLUDED.canceled_at,
        registration_type = EXCLUDED.registration_type,
        state = EXCLUDED.state,
        pass = EXCLUDED.pass,
        subscription = EXCLUDED.subscription,
        revenue_state = EXCLUDED.revenue_state,
        revenue = EXCLUDED.revenue
    `);

    console.log(`  ✅ Merge complete: ${mergeResult.rowCount} rows affected`);
  } finally {
    await client.query("DROP TABLE IF EXISTS tmp_registrations");
    client.release();
  }

  return valid.length;
}

// ── Upload Customers (bulk) ───────────────────────────────────
async function uploadCustomers(filePath) {
  console.log(`\n📂 Parsing customers from: ${filePath}`);
  const allRows = parseCSVFile(filePath);
  console.log(`  Parsed ${allRows.length} rows`);

  const valid = allRows.filter((r) => r.email);
  console.log(`  Valid rows (have email): ${valid.length}`);

  const client = await pool.connect();
  try {
    await client.query(`
      CREATE TEMP TABLE IF NOT EXISTS tmp_customers (
        union_id TEXT,
        first_name TEXT,
        last_name TEXT,
        email TEXT,
        phone TEXT,
        role TEXT,
        total_spent NUMERIC,
        ltv NUMERIC,
        order_count INT,
        current_free_pass INT,
        current_free_auto_renew INT,
        current_paid_pass INT,
        current_paid_auto_renew INT,
        current_payment_plan INT,
        livestream_registrations INT,
        inperson_registrations INT,
        replay_registrations INT,
        livestream_redeemed INT,
        inperson_redeemed INT,
        replay_redeemed INT,
        twitter TEXT,
        instagram TEXT,
        facebook TEXT,
        notes TEXT,
        birthday TEXT,
        how_heard TEXT,
        goals TEXT,
        neighborhood TEXT,
        inspiration TEXT,
        practice_frequency TEXT,
        created_at TEXT
      )
    `);
    await client.query("TRUNCATE tmp_customers");

    const BATCH = 200; // 31 params * 200 = 6200 params per batch
    for (let i = 0; i < valid.length; i += BATCH) {
      const slice = valid.slice(i, i + BATCH);
      const values = [];
      const params = [];
      let paramIdx = 1;

      for (const r of slice) {
        const placeholders = [];
        for (let p = 0; p < 31; p++) {
          placeholders.push(`$${paramIdx++}`);
        }
        values.push(`(${placeholders.join(",")})`);
        params.push(
          r.id || "",
          r.firstName || "",
          r.lastName || "",
          r.email,
          r.phone || null,
          r.role || null,
          parseMoney(r.totalSpent),
          parseMoney(r.ltv),
          parseInt(r.orders) || 0,
          parseBool(r.currentFreeNonAutoRenewPass) ? 1 : 0,
          parseBool(r.currentFreeAutoRenewPass) ? 1 : 0,
          parseBool(r.currentPaidNonAutoRenewPass) ? 1 : 0,
          parseBool(r.currentPaidAutoRenewPass) ? 1 : 0,
          parseBool(r.currentPaymentPlan) ? 1 : 0,
          parseInt(r.livestreamRegistrations) || 0,
          parseInt(r.inpersonRegistrations) || 0,
          parseInt(r.replayRegistrations) || 0,
          parseInt(r.livestreamRegistrationsRedeemed) || 0,
          parseInt(r.inpersonRegistrationsRedeemed) || 0,
          parseInt(r.replayRegistrationsRedeemed) || 0,
          r.twitter || null,
          r.instagram || null,
          r.facebook || null,
          r.notes || null,
          r.birthday || null,
          r.howDidYouHearAboutUs || null,
          r.whatAreYourGoalsForJoiningSkyTing || null,
          r.whatNeighborhoodDoYouLiveIn || null,
          r.whatInspiredYouToJoinSkyTing || null,
          r.howManyTimesPerWeekDoYouWantToPractice || null,
          r.created || "",
        );
      }

      await client.query(
        `INSERT INTO tmp_customers VALUES ${values.join(",")}`,
        params
      );

      if ((i + BATCH) % 2000 < BATCH) {
        console.log(`  ... loaded ${Math.min(i + BATCH, valid.length)}/${valid.length} into temp table`);
      }
    }

    console.log(`  Merging ${valid.length} rows into customers table...`);

    const mergeResult = await client.query(`
      INSERT INTO customers (
        union_id, first_name, last_name, email, phone, role,
        total_spent, ltv, order_count,
        current_free_pass, current_free_auto_renew,
        current_paid_pass, current_paid_auto_renew, current_payment_plan,
        livestream_registrations, inperson_registrations, replay_registrations,
        livestream_redeemed, inperson_redeemed, replay_redeemed,
        twitter, instagram, facebook, notes, birthday,
        how_heard, goals, neighborhood, inspiration, practice_frequency,
        created_at
      )
      SELECT
        union_id, first_name, last_name, email, phone, role,
        total_spent, ltv, order_count,
        current_free_pass, current_free_auto_renew,
        current_paid_pass, current_paid_auto_renew, current_payment_plan,
        livestream_registrations, inperson_registrations, replay_registrations,
        livestream_redeemed, inperson_redeemed, replay_redeemed,
        twitter, instagram, facebook, notes, birthday,
        how_heard, goals, neighborhood, inspiration, practice_frequency,
        created_at
      FROM tmp_customers
      ON CONFLICT (email) DO UPDATE SET
        union_id = EXCLUDED.union_id,
        first_name = EXCLUDED.first_name,
        last_name = EXCLUDED.last_name,
        phone = EXCLUDED.phone,
        role = EXCLUDED.role,
        total_spent = EXCLUDED.total_spent,
        ltv = EXCLUDED.ltv,
        order_count = EXCLUDED.order_count,
        current_free_pass = EXCLUDED.current_free_pass,
        current_free_auto_renew = EXCLUDED.current_free_auto_renew,
        current_paid_pass = EXCLUDED.current_paid_pass,
        current_paid_auto_renew = EXCLUDED.current_paid_auto_renew,
        current_payment_plan = EXCLUDED.current_payment_plan,
        livestream_registrations = EXCLUDED.livestream_registrations,
        inperson_registrations = EXCLUDED.inperson_registrations,
        replay_registrations = EXCLUDED.replay_registrations,
        livestream_redeemed = EXCLUDED.livestream_redeemed,
        inperson_redeemed = EXCLUDED.inperson_redeemed,
        replay_redeemed = EXCLUDED.replay_redeemed,
        twitter = EXCLUDED.twitter,
        instagram = EXCLUDED.instagram,
        facebook = EXCLUDED.facebook,
        notes = EXCLUDED.notes,
        birthday = EXCLUDED.birthday,
        how_heard = EXCLUDED.how_heard,
        goals = EXCLUDED.goals,
        neighborhood = EXCLUDED.neighborhood,
        inspiration = EXCLUDED.inspiration,
        practice_frequency = EXCLUDED.practice_frequency,
        created_at = EXCLUDED.created_at
    `);

    console.log(`  ✅ Merge complete: ${mergeResult.rowCount} rows affected`);
  } finally {
    await client.query("DROP TABLE IF EXISTS tmp_customers");
    client.release();
  }

  return valid.length;
}

// ── Main ──────────────────────────────────────────────────────
async function main() {
  console.log("🚀 Starting bulk data upload...\n");

  // Check current counts
  const regCount = await pool.query("SELECT COUNT(*) as count FROM registrations");
  const custCount = await pool.query("SELECT COUNT(*) as count FROM customers");
  console.log(`Current DB: ${regCount.rows[0].count} registrations, ${custCount.rows[0].count} customers`);

  // Upload registrations (oldest first so newer data overwrites on conflict)
  // Note: 2024 file already uploaded in previous run
  const regFiles = [
    // "/Users/mrudoy/Downloads/20260223-1353-union-registration-export.csv",    // 2024 data — ALREADY DONE
    "/Users/mrudoy/Downloads/20260223-1342-union-registration-export 2.csv",  // 2025 data
    "/Users/mrudoy/Downloads/20260223-1342-union-registration-export.csv",    // 2026 data
  ];

  let totalRegs = 0;
  for (const f of regFiles) {
    totalRegs += await uploadRegistrations(f);
  }
  console.log(`\n📊 Total registration rows processed: ${totalRegs}`);

  // Upload customers
  const custFile = "/Users/mrudoy/Downloads/20260223-1337-union-customer-export.csv";
  const totalCust = await uploadCustomers(custFile);
  console.log(`\n📊 Total customer rows processed: ${totalCust}`);

  // Final counts
  const regCountAfter = await pool.query("SELECT COUNT(*) as count FROM registrations");
  const custCountAfter = await pool.query("SELECT COUNT(*) as count FROM customers");
  console.log(`\n✅ Final DB: ${regCountAfter.rows[0].count} registrations, ${custCountAfter.rows[0].count} customers`);

  await pool.end();
  console.log("\n🏁 Upload complete!");
}

main().catch((e) => {
  console.error("Fatal error:", e);
  process.exit(1);
});
