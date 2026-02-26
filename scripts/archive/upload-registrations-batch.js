const Papa = require('papaparse');
const fs = require('fs');
const { Pool } = require('pg');

const DATABASE_URL = process.env.DATABASE_URL || 'postgresql://postgres:GeCueHexSTjgwiqiifkjloYceGQTNIcr@trolley.proxy.rlwy.net:52977/railway';
const csvPath = process.argv[2];
const tableName = process.argv[3] || 'registrations';

if (!csvPath) {
  console.error('Usage: node scripts/upload-registrations-batch.js <csv-path> [registrations|first_visits]');
  process.exit(1);
}

const pool = new Pool({ connectionString: DATABASE_URL, ssl: false });
const BATCH_SIZE = 200; // rows per INSERT statement
const COMMIT_SIZE = 2000; // rows per transaction

async function main() {
  const csv = fs.readFileSync(csvPath, 'utf8');
  const parsed = Papa.parse(csv.replace(/^\uFEFF/, ''), { header: true, skipEmptyLines: true });
  console.log(`Parsed rows: ${parsed.data.length}`);
  console.log(`Target table: ${tableName}`);

  const before = await pool.query(`SELECT COUNT(*) as count FROM ${tableName}`);
  console.log('Before:', before.rows[0].count);

  // Filter valid rows
  const validRows = parsed.data.filter(row => row.email && row.attended_at);
  console.log(`Valid rows (have email + attended_at): ${validRows.length}`);
  const skipped = parsed.data.length - validRows.length;

  let totalInserted = 0;
  const startTime = Date.now();

  for (let chunkStart = 0; chunkStart < validRows.length; chunkStart += COMMIT_SIZE) {
    const chunk = validRows.slice(chunkStart, chunkStart + COMMIT_SIZE);
    const client = await pool.connect();
    try {
      await client.query('BEGIN');

      for (let batchStart = 0; batchStart < chunk.length; batchStart += BATCH_SIZE) {
        const batch = chunk.slice(batchStart, batchStart + BATCH_SIZE);
        const values = [];
        const params = [];
        let paramIdx = 1;

        for (const row of batch) {
          values.push(`($${paramIdx},$${paramIdx+1},$${paramIdx+2},$${paramIdx+3},$${paramIdx+4},$${paramIdx+5},$${paramIdx+6},$${paramIdx+7},$${paramIdx+8},$${paramIdx+9},$${paramIdx+10},$${paramIdx+11},$${paramIdx+12},$${paramIdx+13},$${paramIdx+14})`);
          params.push(
            row.event_name || '', row.performance_starts_at || '', row.location_name || '',
            row.video_name || null, row.teacher_name || '',
            row.first_name || '', row.last_name || '', row.email,
            row.registered_at || null, row.attended_at,
            row.registration_type || '', row.state || '', row.pass || '',
            row.subscription || 'false',
            parseFloat((row.revenue || '0').replace(/[$,]/g, '')) || 0
          );
          paramIdx += 15;
        }

        await client.query(
          `INSERT INTO ${tableName} (
            event_name, performance_starts_at, location_name, video_name, teacher_name,
            first_name, last_name, email, registered_at, attended_at,
            registration_type, state, pass, subscription, revenue
          ) VALUES ${values.join(',')}
          ON CONFLICT (email, attended_at) DO NOTHING`,
          params
        );
      }

      await client.query('COMMIT');
      totalInserted += chunk.length;
      const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
      const rate = (totalInserted / ((Date.now() - startTime) / 1000)).toFixed(0);
      console.log(`  ${totalInserted}/${validRows.length} rows (${elapsed}s, ~${rate} rows/s)`);
    } catch (e) {
      await client.query('ROLLBACK');
      console.error(`Error at chunk starting ${chunkStart}:`, e.message);
    } finally {
      client.release();
    }
  }

  const after = await pool.query(`SELECT COUNT(*) as count FROM ${tableName}`);
  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  console.log(`\nDone in ${elapsed}s`);
  console.log('After:', after.rows[0].count);
  console.log(`Processed: ${totalInserted}, Skipped (no email/attended_at): ${skipped}`);

  await pool.end();
}
main().catch(e => { console.error(e); process.exit(1); });
