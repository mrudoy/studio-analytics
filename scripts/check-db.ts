import { Pool } from "pg";

async function main() {
  const pool = new Pool({ connectionString: process.env.DATABASE_URL, ssl: { rejectUnauthorized: false } });

  const migrations = await pool.query('SELECT * FROM _migrations ORDER BY id');
  console.log('=== Applied Migrations ===');
  for (const row of migrations.rows) {
    console.log('  ' + row.name + ' (' + row.applied_at + ')');
  }

  const cols = await pool.query("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'revenue_categories' ORDER BY ordinal_position");
  console.log('\n=== revenue_categories column types ===');
  for (const row of cols.rows) {
    console.log('  ' + row.column_name + ': ' + row.data_type);
  }

  const arCols = await pool.query("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'auto_renews' AND column_name IN ('created_at', 'canceled_at', 'plan_category', 'plan_price') ORDER BY ordinal_position");
  console.log('\n=== auto_renews key column types ===');
  for (const row of arCols.rows) {
    console.log('  ' + row.column_name + ': ' + row.data_type);
  }

  await pool.end();
}
main().catch(e => { console.error(e.message); process.exit(1); });
