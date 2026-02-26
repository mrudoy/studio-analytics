/**
 * Download the latest Transactions CSV from Gmail and import to DB with email field.
 * This uses the new OrderSchema that captures customer_email.
 */
import 'dotenv/config';
import { GmailClient } from '../src/lib/email/gmail-client';
import { parseCSV } from '../src/lib/parser/csv-parser';
import { OrderSchema } from '../src/lib/parser/schemas';
import { Pool } from 'pg';

async function main() {
  const gmail = new GmailClient({ robotEmail: 'robot@skyting.com' });
  const since = new Date('2026-02-24T00:00:00Z');
  const emails = await gmail.findReportEmails(since);

  // Find the largest transactions CSV (most complete data)
  const txEmails = emails.filter(e => e.subject.includes('Transactions'));
  console.log(`Found ${txEmails.length} transaction emails`);
  for (const e of txEmails) {
    console.log(`  ${e.subject} - ${e.attachments[0]?.filename} (${e.attachments[0]?.size} bytes)`);
  }

  // Get the biggest one
  const txEmail = txEmails.reduce((best, e) => {
    const size = e.attachments[0]?.size || 0;
    const bestSize = best?.attachments[0]?.size || 0;
    return size > bestSize ? e : best;
  }, txEmails[0]);

  if (!txEmail) {
    console.log('No transactions CSV found');
    return;
  }

  const att = txEmail.attachments[0];
  console.log(`\nDownloading: ${att.filename} (${att.size} bytes)`);
  const filePath = await gmail.downloadAttachment(txEmail.id, att.attachmentId, att.filename);
  console.log(`Saved to: ${filePath}`);

  // Parse with OrderSchema (now includes email)
  const result = parseCSV(filePath, OrderSchema);
  console.log(`\nParsed ${result.data.length} orders`);
  if (result.warnings.length > 0) {
    console.log(`Warnings: ${result.warnings.slice(0, 5).join('\n  ')}`);
  }

  // Check email coverage
  const withEmail = result.data.filter((r: any) => r.email && r.email.trim());
  console.log(`With email: ${withEmail.length} / ${result.data.length} (${Math.round(withEmail.length / result.data.length * 100)}%)`);

  // Show first 5 rows
  console.log('\nSample rows:');
  for (const row of result.data.slice(0, 5)) {
    console.log(JSON.stringify({ code: row.code, customer: row.customer, email: row.email, type: row.type, total: row.total }));
  }

  // Import to Railway DB
  const connStr = process.env.DATABASE_URL;
  if (!connStr) {
    // Try production local
    require('dotenv').config({ path: '.env.production.local', override: true });
  }

  const dbUrl = process.env.DATABASE_URL;
  if (!dbUrl) {
    console.log('\nNo DATABASE_URL found — skipping DB import');
    return;
  }

  console.log(`\nConnecting to DB...`);
  const pool = new Pool({
    connectionString: dbUrl,
    ssl: dbUrl.includes('railway') ? { rejectUnauthorized: false } : undefined
  });

  // Ensure email column exists
  await pool.query('ALTER TABLE orders ADD COLUMN IF NOT EXISTS email TEXT');

  const before = await pool.query('SELECT COUNT(*) as c FROM orders');
  console.log(`Orders before: ${before.rows[0].c}`);

  let imported = 0;
  let updated = 0;
  for (const row of result.data) {
    const res = await pool.query(
      `INSERT INTO orders (created_at, code, customer, email, order_type, payment, total)
       VALUES ($1, $2, $3, $4, $5, $6, $7)
       ON CONFLICT (code) DO UPDATE SET email = EXCLUDED.email
         WHERE orders.email IS NULL OR orders.email = ''`,
      [row.created, row.code, row.customer, row.email || '', row.type, row.payment, row.total]
    );
    if (res.rowCount && res.rowCount > 0) {
      // Check if it was an insert or update
      imported++;
    }
  }

  const after = await pool.query('SELECT COUNT(*) as c FROM orders');
  const afterEmail = await pool.query("SELECT COUNT(*) as c FROM orders WHERE email IS NOT NULL AND email != ''");
  console.log(`\nOrders after: ${after.rows[0].c} (+${Number(after.rows[0].c) - Number(before.rows[0].c)} new)`);
  console.log(`Orders with email: ${afterEmail.rows[0].c}`);

  await pool.end();
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
