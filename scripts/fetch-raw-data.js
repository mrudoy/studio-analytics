/**
 * Fetch raw data from the studio analytics API and save locally for analysis.
 * Tries multiple endpoints to get the most granular subscriber data available.
 */
const fs = require('fs');
const path = require('path');

const BASE = 'https://studio-analytics-production.up.railway.app';

async function fetchJSON(url) {
  const resp = await fetch(url);
  if (!resp.ok) {
    console.error(`  [${resp.status}] ${url}`);
    return null;
  }
  return resp.json();
}

async function main() {
  const outDir = path.join(__dirname, 'data');
  if (!fs.existsSync(outDir)) fs.mkdirSync(outDir, { recursive: true });

  // 1. Main stats endpoint
  console.log('Fetching /api/stats ...');
  const stats = await fetchJSON(`${BASE}/api/stats`);
  if (stats) {
    fs.writeFileSync(path.join(outDir, 'stats.json'), JSON.stringify(stats, null, 2));
    console.log('  Saved stats.json');

    // Show top-level keys
    console.log('  Top-level keys:', Object.keys(stats));

    // Check trends
    if (stats.trends) {
      console.log('  Trends keys:', Object.keys(stats.trends));
      if (stats.trends.churnRates) {
        console.log('  ChurnRates keys:', Object.keys(stats.trends.churnRates));
        if (stats.trends.churnRates.byCategory) {
          for (const [cat, data] of Object.entries(stats.trends.churnRates.byCategory)) {
            console.log(`    ${cat}: keys =`, Object.keys(data));
            if (data.monthly) {
              console.log(`    ${cat}.monthly[0]:`, JSON.stringify(data.monthly[0]).slice(0, 200));
            }
            if (data.tenureMetrics) {
              console.log(`    ${cat}.tenureMetrics:`, JSON.stringify(data.tenureMetrics).slice(0, 300));
            }
          }
        }
        if (stats.trends.churnRates.atRiskByState) {
          const ars = stats.trends.churnRates.atRiskByState;
          console.log('  atRiskByState: pastDue=', ars.pastDue?.length,
                      'invalid=', ars.invalid?.length,
                      'pendingCancel=', ars.pendingCancel?.length);
          // Show sample
          if (ars.pastDue?.length > 0) {
            console.log('    pastDue sample:', JSON.stringify(ars.pastDue[0]).slice(0, 200));
          }
          if (ars.pendingCancel?.length > 0) {
            console.log('    pendingCancel sample:', JSON.stringify(ars.pendingCancel[0]).slice(0, 200));
          }
        }
        if (stats.trends.churnRates.memberAlerts) {
          const ma = stats.trends.churnRates.memberAlerts;
          console.log('  memberAlerts: renewalApproaching=', ma.renewalApproaching?.length,
                      'tenureMilestones=', ma.tenureMilestones?.length);
        }
      }
    }
  }

  // 2. Try health endpoint
  console.log('\nFetching /api/health ...');
  const health = await fetchJSON(`${BASE}/api/health`);
  if (health) {
    fs.writeFileSync(path.join(outDir, 'health.json'), JSON.stringify(health, null, 2));
    console.log('  Saved health.json, keys:', Object.keys(health));
  }

  // 3. Try customer endpoint (summary)
  console.log('\nFetching /api/customer ...');
  const customer = await fetchJSON(`${BASE}/api/customer`);
  if (customer) {
    fs.writeFileSync(path.join(outDir, 'customer.json'), JSON.stringify(customer, null, 2));
    console.log('  Customer:', JSON.stringify(customer));
  }

  // 4. Try backup download - this should have all raw table data
  console.log('\nFetching /api/backup?action=download ...');
  const backup = await fetchJSON(`${BASE}/api/backup?action=download`);
  if (backup) {
    // This is potentially very large, check structure first
    console.log('  Backup metadata:', JSON.stringify(backup.metadata || {}).slice(0, 500));
    if (backup.tables) {
      console.log('  Tables:', Object.keys(backup.tables));
      for (const [table, data] of Object.entries(backup.tables)) {
        const rows = Array.isArray(data) ? data : (data?.rows || []);
        console.log(`    ${table}: ${rows.length} rows`);
        if (rows.length > 0) {
          console.log(`      columns: ${Object.keys(rows[0]).join(', ')}`);
        }
      }
    }

    // Save auto_renews data specifically
    if (backup.tables?.auto_renews) {
      const arData = Array.isArray(backup.tables.auto_renews)
        ? backup.tables.auto_renews
        : backup.tables.auto_renews.rows || [];
      fs.writeFileSync(path.join(outDir, 'auto_renews.json'), JSON.stringify(arData, null, 2));
      console.log(`  Saved auto_renews.json (${arData.length} rows)`);

      // Show sample
      if (arData.length > 0) {
        console.log('  Sample row:', JSON.stringify(arData[0]).slice(0, 300));
      }
    }

    // Save full backup
    fs.writeFileSync(path.join(outDir, 'backup.json'), JSON.stringify(backup, null, 2));
    console.log('  Saved full backup.json');
  }
}

main().catch(console.error);
