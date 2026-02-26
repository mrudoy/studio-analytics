const Papa = require('papaparse');
const fs = require('fs');
const { Pool } = require('pg');

const DATABASE_URL = process.env.DATABASE_URL || 'postgresql://postgres:GeCueHexSTjgwiqiifkjloYceGQTNIcr@trolley.proxy.rlwy.net:52977/railway';
const csvPath = process.argv[2];
const tableName = process.argv[3] || 'registrations'; // 'registrations' or 'first_visits'

if (!csvPath) {
  console.error('Usage: node scripts/upload-registrations.js <csv-path> [registrations|first_visits]');
  process.exit(1);
}

const pool = new Pool({ connectionString: DATABASE_URL, ssl: false });

async function main() {
  const csv = fs.readFileSync(csvPath, 'utf8');
  const parsed = Papa.parse(csv.replace(/^\uFEFF/, ''), { header: true, skipEmptyLines: true });
  console.log(`Parsed rows: ${parsed.data.length}`);
  console.log(`Target table: ${tableName}`);

  const before = await pool.query(`SELECT COUNT(*) as count FROM ${tableName}`);
  console.log('Before:', before.rows[0].count);

  const client = await pool.connect();
  let inserted = 0;
  let skipped = 0;
  const BATCH_SIZE = 500;

  try {
    await client.query('BEGIN');

    for (let i = 0; i < parsed.data.length; i++) {
      const row = parsed.data[i];
      if (!row.email || !row.attended_at) {
        skipped++;
        continue;
      }
      await client.query(
        `INSERT INTO ${tableName} (
          event_name, performance_starts_at, location_name, video_name, teacher_name,
          first_name, last_name, email, registered_at, attended_at,
          registration_type, state, pass, subscription, revenue
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
        ON CONFLICT (email, attended_at) DO NOTHING`,
        [
          row.event_name || '', row.performance_starts_at || '', row.location_name || '',
          row.video_name || null, row.teacher_name || '',
          row.first_name || '', row.last_name || '', row.email,
          row.registered_at || null, row.attended_at,
          row.registration_type || '', row.state || '', row.pass || '',
          row.subscription || 'false',
          parseFloat((row.revenue || '0').replace(/[$,]/g, '')) || 0
        ]
      );
      inserted++;

      // Progress every 5000 rows
      if (inserted % 5000 === 0) {
        console.log(`  ...${inserted} rows inserted so far`);
      }
    }
    await client.query('COMMIT');
  } catch (e) {
    await client.query('ROLLBACK');
    throw e;
  } finally {
    client.release();
  }

  const after = await pool.query(`SELECT COUNT(*) as count FROM ${tableName}`);
  console.log('After:', after.rows[0].count);
  console.log(`Inserted: ${inserted}, Skipped (no email/attended_at): ${skipped}`);

  await pool.end();
}
main().catch(e => { console.error(e); process.exit(1); });
