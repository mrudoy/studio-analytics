/**
 * Comprehensive cancellation pattern analysis for Sky Ting yoga studio.
 *
 * Analyzes ALL cancelled members across:
 *   1. Tenure distribution (how long before cancelling)
 *   2. Seasonal patterns (monthly, day-of-week, day-of-month)
 *   3. Plan type patterns (which plans churn most)
 *   4. Price sensitivity
 *   5. Cohort analysis (join month vs churn speed)
 *   6. Reactivation (same email re-subscribing)
 *
 * Data source: /api/backup?action=download → auto_renews table
 */
const fs = require('fs');
const path = require('path');

// ── Load data ──────────────────────────────────────────────
const dataDir = path.join(__dirname, 'data');
const raw = JSON.parse(fs.readFileSync(path.join(dataDir, 'auto_renews.json'), 'utf8'));

console.log(`\nLoaded ${raw.length} auto_renew records\n`);

// ── Parse dates ────────────────────────────────────────────
function parseDate(str) {
  if (!str || str.trim() === '') return null;
  // Handle "2024-01-28 22:46:51 -0500" format
  const isoMatch = str.match(/^(\d{4})-(\d{2})-(\d{2})/);
  if (isoMatch) return new Date(isoMatch[0] + 'T00:00:00');
  // Handle "1/28/2024" format
  const usMatch = str.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})/);
  if (usMatch) return new Date(`${usMatch[3]}-${usMatch[1].padStart(2,'0')}-${usMatch[2].padStart(2,'0')}T00:00:00`);
  return new Date(str);
}

function isValidDate(d) {
  return d instanceof Date && !isNaN(d.getTime());
}

// Categorize plan names (matching the app's logic)
function getCategory(planName) {
  if (!planName) return 'UNKNOWN';
  const n = planName.toLowerCase();
  if (n.includes('skyting') || n.includes('sky ting tv') || n.includes('10skyting')) {
    if (n.includes('membership') || n.includes('unlimited') || n.includes('in-studio')) return 'MEMBER';
    return 'SKY_TING_TV';
  }
  if (n.includes('sky3') || n.includes('sky 3') || n.includes('class pack') || n.includes('3-class') || n.includes('3 class')) return 'SKY3';
  if (n.includes('member') || n.includes('unlimited') || n.includes('in-studio') || n.includes('in studio')) return 'MEMBER';
  if (n.includes('monthly') && (n.includes('199') || n.includes('249') || n.includes('219') || n.includes('179'))) return 'MEMBER';
  return 'UNKNOWN';
}

function isAnnual(planName) {
  if (!planName) return false;
  const n = planName.toLowerCase();
  return n.includes('annual') || n.includes('yearly') || n.includes('/yr') || n.includes('year');
}

// ── Enrich rows ────────────────────────────────────────────
const rows = raw.map(r => {
  const created = parseDate(r.created_at);
  const canceled = parseDate(r.canceled_at);
  const cat = getCategory(r.plan_name);
  const annual = isAnnual(r.plan_name);
  const price = parseFloat(r.plan_price) || 0;
  const monthlyRate = annual ? Math.round((price / 12) * 100) / 100 : price;

  let tenureMonths = null;
  if (isValidDate(created) && isValidDate(canceled)) {
    tenureMonths = (canceled.getTime() - created.getTime()) / (30.44 * 24 * 60 * 60 * 1000);
    if (tenureMonths < 0) tenureMonths = null; // bad data
  }

  return {
    ...r,
    _created: created,
    _canceled: canceled,
    _category: cat,
    _isAnnual: annual,
    _price: price,
    _monthlyRate: monthlyRate,
    _tenureMonths: tenureMonths,
    _email: (r.customer_email || '').toLowerCase().trim(),
  };
});

// Split into cancelled vs active
const cancelled = rows.filter(r => r.plan_state === 'Canceled' || (r.canceled_at && r.canceled_at.trim() !== ''));
const active = rows.filter(r => ['Valid Now', 'Pending Cancel', 'Past Due', 'In Trial', 'Paused'].includes(r.plan_state));

console.log(`Total records: ${rows.length}`);
console.log(`Cancelled: ${cancelled.length}`);
console.log(`Active (incl paused): ${active.length}`);
console.log(`Other: ${rows.length - cancelled.length - active.length}`);

// Cancelled with valid dates
const cancelledWithDates = cancelled.filter(r => isValidDate(r._created) && isValidDate(r._canceled) && r._tenureMonths !== null && r._tenureMonths >= 0);
console.log(`Cancelled with valid date pair: ${cancelledWithDates.length}\n`);

// ════════════════════════════════════════════════════════════
// 1. TENURE DISTRIBUTION
// ════════════════════════════════════════════════════════════
console.log('═══════════════════════════════════════════════════');
console.log('1. TENURE DISTRIBUTION — How long do people stay before cancelling?');
console.log('═══════════════════════════════════════════════════\n');

const tenures = cancelledWithDates.map(r => r._tenureMonths);
tenures.sort((a, b) => a - b);

const median = tenures[Math.floor(tenures.length / 2)];
const mean = tenures.reduce((a, b) => a + b, 0) / tenures.length;
const p25 = tenures[Math.floor(tenures.length * 0.25)];
const p75 = tenures[Math.floor(tenures.length * 0.75)];
const p90 = tenures[Math.floor(tenures.length * 0.90)];

console.log('Overall tenure of cancelled members:');
console.log(`  Median: ${median.toFixed(1)} months`);
console.log(`  Mean:   ${mean.toFixed(1)} months`);
console.log(`  25th percentile: ${p25.toFixed(1)} months`);
console.log(`  75th percentile: ${p75.toFixed(1)} months`);
console.log(`  90th percentile: ${p90.toFixed(1)} months`);

// Bucket distribution
const buckets = [
  { label: '< 1 month', min: 0, max: 1 },
  { label: '1-2 months', min: 1, max: 2 },
  { label: '2-3 months', min: 2, max: 3 },
  { label: '3-4 months', min: 3, max: 4 },
  { label: '4-6 months', min: 4, max: 6 },
  { label: '6-9 months', min: 6, max: 9 },
  { label: '9-12 months', min: 9, max: 12 },
  { label: '12-18 months', min: 12, max: 18 },
  { label: '18-24 months', min: 18, max: 24 },
  { label: '24+ months', min: 24, max: 999 },
];

console.log('\nTenure bucket distribution:');
const totalCancelled = cancelledWithDates.length;
let cumulative = 0;
for (const b of buckets) {
  const count = cancelledWithDates.filter(r => r._tenureMonths >= b.min && r._tenureMonths < b.max).length;
  cumulative += count;
  const pct = (count / totalCancelled * 100).toFixed(1);
  const cumPct = (cumulative / totalCancelled * 100).toFixed(1);
  const bar = '#'.repeat(Math.round(count / totalCancelled * 60));
  console.log(`  ${b.label.padEnd(16)} ${String(count).padStart(5)}  (${pct.padStart(5)}%)  cum: ${cumPct.padStart(5)}%  ${bar}`);
}

// By category
console.log('\nTenure by category:');
for (const cat of ['MEMBER', 'SKY3', 'SKY_TING_TV']) {
  const catRows = cancelledWithDates.filter(r => r._category === cat);
  if (catRows.length === 0) continue;
  const catTenures = catRows.map(r => r._tenureMonths).sort((a, b) => a - b);
  const catMedian = catTenures[Math.floor(catTenures.length / 2)];
  const catMean = catTenures.reduce((a, b) => a + b, 0) / catTenures.length;
  const cat25 = catTenures[Math.floor(catTenures.length * 0.25)];
  const cat75 = catTenures[Math.floor(catTenures.length * 0.75)];
  const under3mo = catRows.filter(r => r._tenureMonths < 3).length;
  console.log(`  ${cat.padEnd(15)} n=${String(catRows.length).padStart(5)}  median=${catMedian.toFixed(1)}mo  mean=${catMean.toFixed(1)}mo  p25=${cat25.toFixed(1)}  p75=${cat75.toFixed(1)}  under3mo=${under3mo} (${(under3mo/catRows.length*100).toFixed(1)}%)`);
}


// ════════════════════════════════════════════════════════════
// 2. SEASONAL PATTERNS
// ════════════════════════════════════════════════════════════
console.log('\n═══════════════════════════════════════════════════');
console.log('2. SEASONAL PATTERNS — When do people cancel?');
console.log('═══════════════════════════════════════════════════\n');

// By month of cancellation
const cancelsByMonth = {};
const cancelledWithCancelDate = cancelled.filter(r => isValidDate(r._canceled));

for (const r of cancelledWithCancelDate) {
  const month = r._canceled.getMonth(); // 0-11
  cancelsByMonth[month] = (cancelsByMonth[month] || 0) + 1;
}

const monthNames = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
console.log('Cancellations by month (all years combined):');
for (let m = 0; m < 12; m++) {
  const count = cancelsByMonth[m] || 0;
  const pct = (count / cancelledWithCancelDate.length * 100).toFixed(1);
  const bar = '#'.repeat(Math.round(count / cancelledWithCancelDate.length * 50));
  console.log(`  ${monthNames[m].padEnd(4)} ${String(count).padStart(5)}  (${pct.padStart(5)}%)  ${bar}`);
}

// By year-month (timeline)
const cancelsByYearMonth = {};
for (const r of cancelledWithCancelDate) {
  const ym = `${r._canceled.getFullYear()}-${String(r._canceled.getMonth() + 1).padStart(2, '0')}`;
  cancelsByYearMonth[ym] = (cancelsByYearMonth[ym] || 0) + 1;
}

console.log('\nCancellations by year-month (timeline):');
const ymKeys = Object.keys(cancelsByYearMonth).sort();
for (const ym of ymKeys) {
  const count = cancelsByYearMonth[ym];
  const bar = '#'.repeat(Math.round(count / 10));
  console.log(`  ${ym}  ${String(count).padStart(5)}  ${bar}`);
}

// By day of week
const cancelsByDow = {};
const dowNames = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
for (const r of cancelledWithCancelDate) {
  const dow = r._canceled.getDay();
  cancelsByDow[dow] = (cancelsByDow[dow] || 0) + 1;
}

console.log('\nCancellations by day of week:');
for (let d = 0; d < 7; d++) {
  const count = cancelsByDow[d] || 0;
  const pct = (count / cancelledWithCancelDate.length * 100).toFixed(1);
  const bar = '#'.repeat(Math.round(count / cancelledWithCancelDate.length * 50));
  console.log(`  ${dowNames[d].padEnd(4)} ${String(count).padStart(5)}  (${pct.padStart(5)}%)  ${bar}`);
}

// By day of month
const cancelsByDom = {};
for (const r of cancelledWithCancelDate) {
  const dom = r._canceled.getDate();
  cancelsByDom[dom] = (cancelsByDom[dom] || 0) + 1;
}

console.log('\nCancellations by day of month (top 15):');
const domEntries = Object.entries(cancelsByDom).map(([d, c]) => ({ day: parseInt(d), count: c }));
domEntries.sort((a, b) => b.count - a.count);
for (const e of domEntries.slice(0, 15)) {
  const pct = (e.count / cancelledWithCancelDate.length * 100).toFixed(1);
  const bar = '#'.repeat(Math.round(e.count / cancelledWithCancelDate.length * 40));
  console.log(`  Day ${String(e.day).padStart(2)}  ${String(e.count).padStart(5)}  (${pct.padStart(5)}%)  ${bar}`);
}


// ════════════════════════════════════════════════════════════
// 3. PLAN TYPE PATTERNS
// ════════════════════════════════════════════════════════════
console.log('\n═══════════════════════════════════════════════════');
console.log('3. PLAN TYPE PATTERNS — Which plans churn most?');
console.log('═══════════════════════════════════════════════════\n');

// By plan name
const planStats = {};
for (const r of rows) {
  const plan = r.plan_name || 'Unknown';
  if (!planStats[plan]) {
    planStats[plan] = { total: 0, cancelled: 0, active: 0, prices: [], tenures: [] };
  }
  planStats[plan].total++;
  if (r.plan_state === 'Canceled' || (r.canceled_at && r.canceled_at.trim() !== '')) {
    planStats[plan].cancelled++;
    if (r._tenureMonths !== null && r._tenureMonths >= 0) {
      planStats[plan].tenures.push(r._tenureMonths);
    }
  }
  if (['Valid Now', 'Pending Cancel', 'Past Due', 'In Trial'].includes(r.plan_state)) {
    planStats[plan].active++;
  }
  planStats[plan].prices.push(r._price);
}

console.log('Plan-level churn rates (sorted by churn rate, min 20 total):');
console.log(`${'Plan Name'.padEnd(50)} ${'Total'.padStart(6)} ${'Cxl'.padStart(5)} ${'Active'.padStart(6)} ${'Churn%'.padStart(7)} ${'Med Tnr'.padStart(8)} ${'Price'.padStart(7)}`);
console.log('-'.repeat(95));

const planEntries = Object.entries(planStats)
  .filter(([_, s]) => s.total >= 20)
  .sort((a, b) => (b[1].cancelled / b[1].total) - (a[1].cancelled / a[1].total));

for (const [plan, s] of planEntries) {
  const churnRate = (s.cancelled / s.total * 100).toFixed(1);
  const medTenure = s.tenures.length > 0
    ? s.tenures.sort((a, b) => a - b)[Math.floor(s.tenures.length / 2)].toFixed(1)
    : 'N/A';
  const avgPrice = s.prices.length > 0
    ? (s.prices.reduce((a, b) => a + b, 0) / s.prices.length).toFixed(0)
    : '0';
  console.log(`${plan.slice(0, 49).padEnd(50)} ${String(s.total).padStart(6)} ${String(s.cancelled).padStart(5)} ${String(s.active).padStart(6)} ${churnRate.padStart(6)}% ${(medTenure + 'mo').padStart(8)} ${('$' + avgPrice).padStart(7)}`);
}


// ════════════════════════════════════════════════════════════
// 4. PRICE SENSITIVITY
// ════════════════════════════════════════════════════════════
console.log('\n═══════════════════════════════════════════════════');
console.log('4. PRICE SENSITIVITY — Do more expensive plans churn more?');
console.log('═══════════════════════════════════════════════════\n');

// Group by price point
const priceGroups = {};
for (const r of rows) {
  const price = r._price;
  if (!priceGroups[price]) {
    priceGroups[price] = { total: 0, cancelled: 0, planNames: new Set(), tenures: [] };
  }
  priceGroups[price].total++;
  priceGroups[price].planNames.add(r.plan_name);
  if (r.plan_state === 'Canceled' || (r.canceled_at && r.canceled_at.trim() !== '')) {
    priceGroups[price].cancelled++;
    if (r._tenureMonths !== null && r._tenureMonths >= 0) {
      priceGroups[price].tenures.push(r._tenureMonths);
    }
  }
}

console.log('Churn rate by price point (min 20 records):');
console.log(`${'Price'.padStart(8)} ${'Total'.padStart(6)} ${'Cxl'.padStart(5)} ${'Churn%'.padStart(7)} ${'Med Tenure'.padStart(11)} ${'Plans'}`);
console.log('-'.repeat(95));

const priceEntries = Object.entries(priceGroups)
  .filter(([_, s]) => s.total >= 20)
  .sort((a, b) => parseFloat(a[0]) - parseFloat(b[0]));

for (const [price, s] of priceEntries) {
  const churnRate = (s.cancelled / s.total * 100).toFixed(1);
  const medTenure = s.tenures.length > 0
    ? s.tenures.sort((a, b) => a - b)[Math.floor(s.tenures.length / 2)].toFixed(1) + 'mo'
    : 'N/A';
  const plans = [...s.planNames].slice(0, 3).join(', ');
  console.log(`${'$' + parseFloat(price).toFixed(0).padStart(6)} ${String(s.total).padStart(6)} ${String(s.cancelled).padStart(5)} ${churnRate.padStart(6)}% ${medTenure.padStart(11)}  ${plans.slice(0, 50)}`);
}

// Monthly rate analysis (normalizing annual to monthly)
console.log('\nChurn rate by monthly-rate bucket:');
const mrrBuckets = [
  { label: '$0-20', min: 0, max: 20 },
  { label: '$20-40', min: 20, max: 40 },
  { label: '$40-80', min: 40, max: 80 },
  { label: '$80-120', min: 80, max: 120 },
  { label: '$120-180', min: 120, max: 180 },
  { label: '$180-250', min: 180, max: 250 },
  { label: '$250+', min: 250, max: 99999 },
];

for (const b of mrrBuckets) {
  const bRows = rows.filter(r => r._monthlyRate >= b.min && r._monthlyRate < b.max);
  const bCancelled = bRows.filter(r => r.plan_state === 'Canceled' || (r.canceled_at && r.canceled_at.trim() !== ''));
  if (bRows.length < 10) continue;
  const churnRate = (bCancelled.length / bRows.length * 100).toFixed(1);
  const tenures = bCancelled.filter(r => r._tenureMonths !== null && r._tenureMonths >= 0).map(r => r._tenureMonths);
  const medTenure = tenures.length > 0
    ? tenures.sort((a, b) => a - b)[Math.floor(tenures.length / 2)].toFixed(1)
    : 'N/A';
  console.log(`  ${b.label.padEnd(10)} n=${String(bRows.length).padStart(5)}  cxl=${String(bCancelled.length).padStart(5)}  churn=${churnRate.padStart(5)}%  medTenure=${medTenure}mo`);
}


// ════════════════════════════════════════════════════════════
// 5. COHORT ANALYSIS
// ════════════════════════════════════════════════════════════
console.log('\n═══════════════════════════════════════════════════');
console.log('5. COHORT ANALYSIS — Do certain join months churn faster?');
console.log('═══════════════════════════════════════════════════\n');

// Group by join year-month
const cohorts = {};
for (const r of rows) {
  if (!isValidDate(r._created)) continue;
  const ym = `${r._created.getFullYear()}-${String(r._created.getMonth() + 1).padStart(2, '0')}`;
  if (!cohorts[ym]) {
    cohorts[ym] = { total: 0, cancelled: 0, cancelledIn3mo: 0, cancelledIn6mo: 0, tenures: [] };
  }
  cohorts[ym].total++;
  if (r.plan_state === 'Canceled' || (r.canceled_at && r.canceled_at.trim() !== '')) {
    cohorts[ym].cancelled++;
    if (r._tenureMonths !== null && r._tenureMonths >= 0) {
      cohorts[ym].tenures.push(r._tenureMonths);
      if (r._tenureMonths < 3) cohorts[ym].cancelledIn3mo++;
      if (r._tenureMonths < 6) cohorts[ym].cancelledIn6mo++;
    }
  }
}

console.log('Cohort analysis (by join month):');
console.log(`${'Cohort'.padEnd(10)} ${'Joined'.padStart(7)} ${'Cxl'.padStart(5)} ${'Churn%'.padStart(7)} ${'<3mo'.padStart(6)} ${'<6mo'.padStart(6)} ${'Med Tnr'.padStart(8)}`);
console.log('-'.repeat(60));

const cohortKeys = Object.keys(cohorts).sort();
for (const ym of cohortKeys) {
  const c = cohorts[ym];
  if (c.total < 5) continue;
  const churnRate = (c.cancelled / c.total * 100).toFixed(1);
  const earlyChurn = c.total > 0 ? (c.cancelledIn3mo / c.total * 100).toFixed(1) : '0.0';
  const midChurn = c.total > 0 ? (c.cancelledIn6mo / c.total * 100).toFixed(1) : '0.0';
  const medTenure = c.tenures.length > 0
    ? c.tenures.sort((a, b) => a - b)[Math.floor(c.tenures.length / 2)].toFixed(1)
    : 'N/A';
  console.log(`${ym.padEnd(10)} ${String(c.total).padStart(7)} ${String(c.cancelled).padStart(5)} ${churnRate.padStart(6)}% ${(earlyChurn + '%').padStart(6)} ${(midChurn + '%').padStart(6)} ${(medTenure + 'mo').padStart(8)}`);
}

// By join month (all years)
console.log('\nChurn by join month (all years combined):');
const joinMonths = {};
for (const r of rows) {
  if (!isValidDate(r._created)) continue;
  const m = r._created.getMonth();
  if (!joinMonths[m]) joinMonths[m] = { total: 0, cancelled: 0, tenures: [] };
  joinMonths[m].total++;
  if (r.plan_state === 'Canceled' || (r.canceled_at && r.canceled_at.trim() !== '')) {
    joinMonths[m].cancelled++;
    if (r._tenureMonths !== null && r._tenureMonths >= 0) {
      joinMonths[m].tenures.push(r._tenureMonths);
    }
  }
}

for (let m = 0; m < 12; m++) {
  const jm = joinMonths[m];
  if (!jm || jm.total < 5) continue;
  const churnRate = (jm.cancelled / jm.total * 100).toFixed(1);
  const medTenure = jm.tenures.length > 0
    ? jm.tenures.sort((a, b) => a - b)[Math.floor(jm.tenures.length / 2)].toFixed(1)
    : 'N/A';
  const bar = '#'.repeat(Math.round(parseFloat(churnRate) / 2));
  console.log(`  ${monthNames[m].padEnd(4)} joined=${String(jm.total).padStart(5)}  cxl=${String(jm.cancelled).padStart(5)}  churn=${churnRate.padStart(5)}%  medTenure=${medTenure.padStart(5)}mo  ${bar}`);
}


// ════════════════════════════════════════════════════════════
// 6. REACTIVATION ANALYSIS
// ════════════════════════════════════════════════════════════
console.log('\n═══════════════════════════════════════════════════');
console.log('6. REACTIVATION — Do cancelled members come back?');
console.log('═══════════════════════════════════════════════════\n');

// Group by email
const emailMap = {};
for (const r of rows) {
  if (!r._email) continue;
  if (!emailMap[r._email]) emailMap[r._email] = [];
  emailMap[r._email].push(r);
}

const totalUniqueEmails = Object.keys(emailMap).length;
const multiSub = Object.entries(emailMap).filter(([_, subs]) => subs.length > 1);
const multiSubEmails = multiSub.length;

console.log(`Unique emails: ${totalUniqueEmails}`);
console.log(`Emails with multiple subscriptions: ${multiSubEmails} (${(multiSubEmails/totalUniqueEmails*100).toFixed(1)}%)`);

// Find reactivations: cancelled then re-subscribed (new created_at after canceled_at)
let reactivations = 0;
let reactivationDetails = [];

for (const [email, subs] of Object.entries(emailMap)) {
  if (subs.length < 2) continue;

  // Sort by created date
  const sorted = subs
    .filter(s => isValidDate(s._created))
    .sort((a, b) => a._created.getTime() - b._created.getTime());

  for (let i = 0; i < sorted.length - 1; i++) {
    const prev = sorted[i];
    const next = sorted[i + 1];

    // Previous subscription was cancelled AND next one was created after cancellation
    if (isValidDate(prev._canceled) && isValidDate(next._created) && next._created > prev._canceled) {
      reactivations++;
      const gapDays = (next._created.getTime() - prev._canceled.getTime()) / (24 * 60 * 60 * 1000);
      reactivationDetails.push({
        email,
        prevPlan: prev.plan_name,
        nextPlan: next.plan_name,
        prevPrice: prev._price,
        nextPrice: next._price,
        gapDays: Math.round(gapDays),
        prevTenure: prev._tenureMonths,
        prevCategory: prev._category,
        nextCategory: next._category,
      });
    }
  }
}

console.log(`\nReactivation events (cancelled then re-subscribed): ${reactivations}`);
console.log(`Unique people who reactivated: ${new Set(reactivationDetails.map(r => r.email)).size}`);

if (reactivationDetails.length > 0) {
  const gaps = reactivationDetails.map(r => r.gapDays).sort((a, b) => a - b);
  console.log(`\nGap between cancellation and re-subscription:`);
  console.log(`  Median: ${gaps[Math.floor(gaps.length / 2)]} days`);
  console.log(`  Mean: ${Math.round(gaps.reduce((a, b) => a + b, 0) / gaps.length)} days`);
  console.log(`  Min: ${gaps[0]} days, Max: ${gaps[gaps.length - 1]} days`);

  // Gap distribution
  const gapBuckets = [
    { label: '< 7 days', min: 0, max: 7 },
    { label: '7-30 days', min: 7, max: 30 },
    { label: '1-3 months', min: 30, max: 90 },
    { label: '3-6 months', min: 90, max: 180 },
    { label: '6-12 months', min: 180, max: 365 },
    { label: '12+ months', min: 365, max: 99999 },
  ];

  console.log('\nGap distribution:');
  for (const b of gapBuckets) {
    const count = reactivationDetails.filter(r => r.gapDays >= b.min && r.gapDays < b.max).length;
    const pct = (count / reactivationDetails.length * 100).toFixed(1);
    console.log(`  ${b.label.padEnd(14)} ${String(count).padStart(5)}  (${pct.padStart(5)}%)`);
  }

  // Plan switching patterns
  const samePlan = reactivationDetails.filter(r => r.prevPlan === r.nextPlan).length;
  const sameCat = reactivationDetails.filter(r => r.prevCategory === r.nextCategory).length;
  const upgraded = reactivationDetails.filter(r => r.nextPrice > r.prevPrice).length;
  const downgraded = reactivationDetails.filter(r => r.nextPrice < r.prevPrice).length;
  const samePrice = reactivationDetails.filter(r => r.nextPrice === r.prevPrice).length;

  console.log(`\nReactivation plan changes:`);
  console.log(`  Same plan:     ${samePlan} (${(samePlan/reactivationDetails.length*100).toFixed(1)}%)`);
  console.log(`  Same category: ${sameCat} (${(sameCat/reactivationDetails.length*100).toFixed(1)}%)`);
  console.log(`  Upgraded $:    ${upgraded} (${(upgraded/reactivationDetails.length*100).toFixed(1)}%)`);
  console.log(`  Downgraded $:  ${downgraded} (${(downgraded/reactivationDetails.length*100).toFixed(1)}%)`);
  console.log(`  Same price:    ${samePrice} (${(samePrice/reactivationDetails.length*100).toFixed(1)}%)`);

  // Category switches
  const catSwitches = {};
  for (const r of reactivationDetails) {
    const key = `${r.prevCategory} -> ${r.nextCategory}`;
    catSwitches[key] = (catSwitches[key] || 0) + 1;
  }
  console.log('\nCategory transitions on reactivation (top 10):');
  Object.entries(catSwitches)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 10)
    .forEach(([key, count]) => {
      console.log(`  ${key.padEnd(35)} ${count}`);
    });
}


// ════════════════════════════════════════════════════════════
// 7. EXTRA: CANCELLATION REASON / PATTERN ANALYSIS
// ════════════════════════════════════════════════════════════
console.log('\n═══════════════════════════════════════════════════');
console.log('7. ADDITIONAL PATTERNS');
console.log('═══════════════════════════════════════════════════\n');

// Canceled_by analysis
const canceledByStats = {};
for (const r of cancelled) {
  const by = r.canceled_by || 'unknown';
  canceledByStats[by] = (canceledByStats[by] || 0) + 1;
}

console.log('Cancelled by (who initiated):');
Object.entries(canceledByStats)
  .sort((a, b) => b[1] - a[1])
  .slice(0, 15)
  .forEach(([by, count]) => {
    const pct = (count / cancelled.length * 100).toFixed(1);
    console.log(`  ${by.slice(0, 40).padEnd(42)} ${String(count).padStart(5)}  (${pct.padStart(5)}%)`);
  });

// Plan state distribution
const stateStats = {};
for (const r of rows) {
  stateStats[r.plan_state] = (stateStats[r.plan_state] || 0) + 1;
}
console.log('\nPlan state distribution (all records):');
Object.entries(stateStats)
  .sort((a, b) => b[1] - a[1])
  .forEach(([state, count]) => {
    const pct = (count / rows.length * 100).toFixed(1);
    console.log(`  ${state.padEnd(20)} ${String(count).padStart(6)}  (${pct.padStart(5)}%)`);
  });

// Tenure by annual vs monthly (MEMBER only)
console.log('\nMEMBER tenure: Annual vs Monthly billing:');
const memberCancelled = cancelledWithDates.filter(r => r._category === 'MEMBER');
const annualCxl = memberCancelled.filter(r => r._isAnnual);
const monthlyCxl = memberCancelled.filter(r => !r._isAnnual);

if (annualCxl.length > 0) {
  const aTenures = annualCxl.map(r => r._tenureMonths).sort((a, b) => a - b);
  console.log(`  Annual:  n=${annualCxl.length}  median=${aTenures[Math.floor(aTenures.length/2)].toFixed(1)}mo  mean=${(aTenures.reduce((a,b)=>a+b,0)/aTenures.length).toFixed(1)}mo`);
}
if (monthlyCxl.length > 0) {
  const mTenures = monthlyCxl.map(r => r._tenureMonths).sort((a, b) => a - b);
  console.log(`  Monthly: n=${monthlyCxl.length}  median=${mTenures[Math.floor(mTenures.length/2)].toFixed(1)}mo  mean=${(mTenures.reduce((a,b)=>a+b,0)/mTenures.length).toFixed(1)}mo`);
}

// "First month dropoff" - what percentage cancel in first 30 days?
const firstMonthDropoff = cancelledWithDates.filter(r => r._tenureMonths < 1).length;
const firstThreeMonths = cancelledWithDates.filter(r => r._tenureMonths < 3).length;
console.log(`\nEarly dropoff rates (all categories):`);
console.log(`  < 1 month:  ${firstMonthDropoff} of ${cancelledWithDates.length} (${(firstMonthDropoff/cancelledWithDates.length*100).toFixed(1)}%)`);
console.log(`  < 3 months: ${firstThreeMonths} of ${cancelledWithDates.length} (${(firstThreeMonths/cancelledWithDates.length*100).toFixed(1)}%)`);

// Revenue impact of cancellations
const totalCxlMRR = cancelled.reduce((s, r) => s + r._monthlyRate, 0);
const avgCxlMRR = totalCxlMRR / cancelled.length;
console.log(`\nRevenue impact of all cancellations:`);
console.log(`  Total MRR lost: $${totalCxlMRR.toFixed(0)}`);
console.log(`  Avg MRR per cancelled: $${avgCxlMRR.toFixed(2)}`);

// By category
for (const cat of ['MEMBER', 'SKY3', 'SKY_TING_TV']) {
  const catCxl = cancelled.filter(r => r._category === cat);
  if (catCxl.length === 0) continue;
  const catMRR = catCxl.reduce((s, r) => s + r._monthlyRate, 0);
  console.log(`  ${cat.padEnd(15)} cxl=${String(catCxl.length).padStart(5)}  MRR lost=$${catMRR.toFixed(0).padStart(8)}  avg=$${(catMRR/catCxl.length).toFixed(2)}`);
}

console.log('\n--- Analysis complete ---\n');
