#!/usr/bin/env node
/**
 * Chunked upload — inserts directly in small batches without temp tables.
 * Avoids disk space issues on the remote DB.
 */
import pg from "pg";
import { readFileSync } from "fs";

const DATABASE_URL = process.env.DATABASE_URL;
if (!DATABASE_URL) throw new Error("DATABASE_URL env var is required");

const pool = new pg.Pool({
  connectionString: DATABASE_URL,
  ssl: false,
  statement_timeout: 120000,
  query_timeout: 120000,
  connectionTimeoutMillis: 30000,
});

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

// ── Upload Registrations in small batches ─────────────────────
async function uploadRegistrations(filePath) {
  console.log(`\n📂 Parsing registrations from: ${filePath}`);
  const allRows = parseCSVFile(filePath);
  console.log(`  Parsed ${allRows.length} rows`);

  const valid = allRows.filter((r) => r.email && r.attendedAt);
  console.log(`  Valid rows: ${valid.length}`);

  // Deduplicate in memory: keep last occurrence per (email, attendedAt)
  const deduped = new Map();
  for (const r of valid) {
    const key = `${r.email.toLowerCase()}|${r.attendedAt}`;
    deduped.set(key, r);
  }
  const uniqueRows = [...deduped.values()];
  console.log(`  After in-memory dedup: ${uniqueRows.length} unique rows`);

  // Insert in batches of 50 rows using multi-value INSERT
  const BATCH = 50;
  let processed = 0;
  let errors = 0;

  for (let i = 0; i < uniqueRows.length; i += BATCH) {
    const slice = uniqueRows.slice(i, i + BATCH);
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

    try {
      await pool.query(
        `INSERT INTO registrations (
          event_name, event_id, performance_id, performance_starts_at,
          location_name, video_name, video_id, teacher_name,
          first_name, last_name, email, phone, role,
          registered_at, canceled_at, attended_at,
          registration_type, state, pass, subscription,
          revenue_state, revenue
        ) VALUES ${values.join(",")}
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
          revenue = EXCLUDED.revenue`,
        params
      );
      processed += slice.length;
    } catch (e) {
      errors++;
      if (errors <= 5) {
        console.log(`  ⚠️ Batch error at ${i}: ${e.message.slice(0, 120)}`);
      }
    }

    if ((i + BATCH) % 5000 < BATCH) {
      console.log(`  ... ${Math.min(i + BATCH, uniqueRows.length)}/${uniqueRows.length} (${errors} errors)`);
    }
  }

  console.log(`  ✅ Done: ${processed} inserted/updated, ${errors} batch errors`);
  return processed;
}

// ── Upload Customers in small batches ─────────────────────────
async function uploadCustomers(filePath) {
  console.log(`\n📂 Parsing customers from: ${filePath}`);
  const allRows = parseCSVFile(filePath);
  console.log(`  Parsed ${allRows.length} rows`);

  const valid = allRows.filter((r) => r.email);
  console.log(`  Valid rows: ${valid.length}`);

  const BATCH = 30; // 31 params * 30 = 930 params
  let processed = 0;
  let errors = 0;

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

    try {
      await pool.query(
        `INSERT INTO customers (
          union_id, first_name, last_name, email, phone, role,
          total_spent, ltv, order_count,
          current_free_pass, current_free_auto_renew,
          current_paid_pass, current_paid_auto_renew, current_payment_plan,
          livestream_registrations, inperson_registrations, replay_registrations,
          livestream_redeemed, inperson_redeemed, replay_redeemed,
          twitter, instagram, facebook, notes, birthday,
          how_heard, goals, neighborhood, inspiration, practice_frequency,
          created_at
        ) VALUES ${values.join(",")}
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
          created_at = EXCLUDED.created_at`,
        params
      );
      processed += slice.length;
    } catch (e) {
      errors++;
      if (errors <= 5) {
        console.log(`  ⚠️ Batch error at ${i}: ${e.message.slice(0, 120)}`);
      }
    }

    if ((i + BATCH) % 2000 < BATCH) {
      console.log(`  ... ${Math.min(i + BATCH, valid.length)}/${valid.length} (${errors} errors)`);
    }
  }

  console.log(`  ✅ Done: ${processed} inserted/updated, ${errors} batch errors`);
  return processed;
}

// ── Main ──────────────────────────────────────────────────────
async function main() {
  console.log("🚀 Starting chunked data upload...\n");

  const regCount = await pool.query("SELECT COUNT(*) as count FROM registrations");
  const custCount = await pool.query("SELECT COUNT(*) as count FROM customers");
  console.log(`Current DB: ${regCount.rows[0].count} registrations, ${custCount.rows[0].count} customers`);

  // Upload ALL registration files to fresh Postgres-6X12
  const regFiles = [
    "/Users/mrudoy/Downloads/20260223-1353-union-registration-export.csv",    // 2024 data (~117K rows)
    "/Users/mrudoy/Downloads/20260223-1342-union-registration-export 2.csv",  // 2025 data (~130K rows)
    "/Users/mrudoy/Downloads/20260223-1342-union-registration-export.csv",    // 2026 data (~22K rows)
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
