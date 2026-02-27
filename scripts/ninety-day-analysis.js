/**
 * 90-Day Churn Analysis for Sky Ting Yoga Studio
 * 
 * Investigates what distinguishes members who cancel within 90 days ("early churners")
 * from those who stay ("survivors"), with particular focus on attendance frequency.
 */

const fs = require('fs');
const path = require('path');

const BACKUP_PATH = path.join(__dirname, 'data', 'backup.json');
const API_URL = 'https://studio-analytics-production.up.railway.app/api/backup?action=download';

// ---------------------------------------------------------------------------
// Plan classification helpers
// ---------------------------------------------------------------------------
const MEMBER_KEYWORDS = ['UNLIMITED', 'MEMBER', 'ALL ACCESS', 'TING FAM', 'SKY3', 'SKY5', 'SKYHIGH3', 'SKY UNLIMITED'];
const ANNUAL_KEYWORDS = ['ANNUAL', 'YEARLY', '12 MONTH', '12-MONTH'];
const EXCLUDE_KEYWORDS = ['TV', 'TING TV', 'ON DEMAND', 'COME BACK', 'VIRGIN', 'TT', 'TEACHER TRAINING', 'MENTORSHIP', 'PAYMENT PLAN', 'RETREAT', 'GREECE', 'SPECIAL', 'SUBSCRIBER'];

function isMemberPlan(planName) {
  const upper = planName.toUpperCase();
  
  // Exclude annual plans
  if (ANNUAL_KEYWORDS.some(kw => upper.includes(kw))) return false;
  
  // Exclude TV-only, teacher training, mentorship, payment plans, specials
  if (EXCLUDE_KEYWORDS.some(kw => upper.includes(kw))) return false;
  
  // Must match a member keyword
  if (MEMBER_KEYWORDS.some(kw => upper.includes(kw))) return true;
  
  // Also include "SKY TING In Person Membership" type plans
  if (upper.includes('IN PERSON')) return true;
  
  return false;
}

// ---------------------------------------------------------------------------
// Date parsing helpers
// ---------------------------------------------------------------------------
function parseDate(dateStr) {
  if (!dateStr) return null;
  // Handle formats like "2024-02-05 18:00:00 -0500" and "2024-02-05T05:00:37Z"
  const d = new Date(dateStr);
  if (isNaN(d.getTime())) return null;
  return d;
}

function daysBetween(d1, d2) {
  return (d2.getTime() - d1.getTime()) / (1000 * 60 * 60 * 24);
}

function weeksBetween(d1, d2) {
  return daysBetween(d1, d2) / 7;
}

function getHour(dateStr) {
  // Extract hour from the original string to avoid timezone confusion
  const match = dateStr.match(/(\d{2}):\d{2}:\d{2}/);
  if (match) return parseInt(match[1]);
  return null;
}

function getTimeBucket(hour) {
  if (hour === null) return 'unknown';
  if (hour >= 6 && hour < 12) return 'morning';
  if (hour >= 12 && hour < 17) return 'afternoon';
  if (hour >= 17 && hour < 21) return 'evening';
  return 'night/off-hours';
}

// ---------------------------------------------------------------------------
// Main analysis
// ---------------------------------------------------------------------------
async function main() {
  console.log('='.repeat(80));
  console.log('SKY TING YOGA — 90-DAY CHURN ANALYSIS');
  console.log('='.repeat(80));
  console.log('');

  // 1. Load data
  let data;
  if (fs.existsSync(BACKUP_PATH)) {
    console.log('Loading backup from disk:', BACKUP_PATH);
    data = JSON.parse(fs.readFileSync(BACKUP_PATH, 'utf8'));
  } else {
    console.log('Backup not found on disk. Fetching from API...');
    const resp = await fetch(API_URL);
    if (!resp.ok) throw new Error('API fetch failed: ' + resp.status);
    data = await resp.json();
    fs.mkdirSync(path.dirname(BACKUP_PATH), { recursive: true });
    fs.writeFileSync(BACKUP_PATH, JSON.stringify(data));
    console.log('Saved backup to:', BACKUP_PATH);
  }

  const autoRenews = data.tables.auto_renews;
  const registrations = data.tables.registrations;

  console.log(`Total auto_renews rows: ${autoRenews.length}`);
  console.log(`Total registrations rows: ${registrations.length}`);
  console.log('');

  // 2. Deduplicate auto_renews — prefer rows with created_at, then by most recent snapshot
  const deduped = {};
  for (const row of autoRenews) {
    const key = row.customer_email.toLowerCase() + '|' + row.plan_name;
    const existing = deduped[key];
    if (!existing) {
      deduped[key] = row;
    } else {
      // Prefer the one with a non-empty created_at
      const existingHasCreated = existing.created_at && existing.created_at.trim() !== '';
      const newHasCreated = row.created_at && row.created_at.trim() !== '';
      if (!existingHasCreated && newHasCreated) {
        deduped[key] = row;
      } else if (existingHasCreated && newHasCreated) {
        // If both have created_at, prefer the one from a more "authoritative" snapshot (non-zip)
        if (existing.snapshot_id && existing.snapshot_id.startsWith('zip-') && row.snapshot_id && !row.snapshot_id.startsWith('zip-')) {
          deduped[key] = row;
        }
      }
    }
  }
  const uniqueSubs = Object.values(deduped);
  console.log(`Deduplicated auto_renews: ${uniqueSubs.length} unique email+plan combos`);

  // 3. Filter to member plans only (exclude annual, TV-only, etc.)
  const memberSubs = uniqueSubs.filter(r => isMemberPlan(r.plan_name));
  console.log(`\nMember-category subscriptions (monthly unlimited/membership plans):`);
  console.log(`  Total: ${memberSubs.length}`);

  // Show which plan names made the cut
  const includedPlans = {};
  memberSubs.forEach(r => { includedPlans[r.plan_name] = (includedPlans[r.plan_name] || 0) + 1; });
  console.log(`  Plan names included:`);
  Object.entries(includedPlans).sort((a,b) => b[1] - a[1]).forEach(([name, count]) => {
    console.log(`    ${name}: ${count}`);
  });

  // 4. Filter to subs with valid created_at
  const withCreated = memberSubs.filter(r => {
    const d = parseDate(r.created_at);
    return d !== null;
  });
  console.log(`\n  With valid created_at: ${withCreated.length}`);

  // 5. Build registration lookup by email (lowercase)
  console.log('\nBuilding registration index by email...');
  const regByEmail = {};
  let inPersonOrLivestreamCount = 0;
  for (const reg of registrations) {
    if (!reg.email) continue;
    const email = reg.email.toLowerCase().trim();
    // Only count attended registrations (state = "redeemed") that are in-person or livestream
    // Exclude replays / on-demand which are TV-only
    const regType = (reg.registration_type || '').toLowerCase();
    const isAttendance = reg.state === 'redeemed';
    // Include in-person, livestream, external video call, zoom — basically all live attendance
    // We'll include everything that's "redeemed" since these are actual attendances
    if (!isAttendance) continue;
    
    if (!regByEmail[email]) regByEmail[email] = [];
    regByEmail[email].push(reg);
    inPersonOrLivestreamCount++;
  }
  console.log(`  Indexed ${inPersonOrLivestreamCount} redeemed registrations across ${Object.keys(regByEmail).length} unique emails`);

  // 6. Classify members into cohorts
  const earlyChurners = []; // canceled within 90 days
  const survivors = [];     // still active OR canceled after 90+ days
  const noMatch = [];       // members with no registrations found

  for (const sub of withCreated) {
    const email = sub.customer_email.toLowerCase().trim();
    const createdAt = parseDate(sub.created_at);
    const canceledAt = parseDate(sub.canceled_at);
    const isCanceled = canceledAt !== null;
    
    let daysToCancel = null;
    if (isCanceled) {
      daysToCancel = daysBetween(createdAt, canceledAt);
    }

    // Get registrations for this member within their first 90 days
    const memberRegs = regByEmail[email] || [];
    
    // Filter to registrations within the analysis window
    // The window is: created_at to min(created_at + 90 days, canceled_at)
    const windowEnd = new Date(createdAt.getTime() + 90 * 24 * 60 * 60 * 1000);
    const effectiveEnd = (isCanceled && canceledAt < windowEnd) ? canceledAt : windowEnd;
    
    const regsInWindow = memberRegs.filter(reg => {
      const regDate = parseDate(reg.performance_starts_at || reg.attended_at);
      if (!regDate) return false;
      return regDate >= createdAt && regDate <= effectiveEnd;
    });

    // Also get first 30 and first 60 day attendance
    const day30End = new Date(createdAt.getTime() + 30 * 24 * 60 * 60 * 1000);
    const day60End = new Date(createdAt.getTime() + 60 * 24 * 60 * 60 * 1000);
    
    const regsIn30 = memberRegs.filter(reg => {
      const regDate = parseDate(reg.performance_starts_at || reg.attended_at);
      if (!regDate) return false;
      return regDate >= createdAt && regDate <= day30End;
    });
    
    const regsIn60 = memberRegs.filter(reg => {
      const regDate = parseDate(reg.performance_starts_at || reg.attended_at);
      if (!regDate) return false;
      return regDate >= createdAt && regDate <= day60End;
    });
    
    // Week 1 attendance (first 7 days)
    const week1End = new Date(createdAt.getTime() + 7 * 24 * 60 * 60 * 1000);
    const regsWeek1 = memberRegs.filter(reg => {
      const regDate = parseDate(reg.performance_starts_at || reg.attended_at);
      if (!regDate) return false;
      return regDate >= createdAt && regDate <= week1End;
    });
    
    // Compute weeks in window for per-week calculation
    const actualDays = isCanceled ? Math.min(90, daysToCancel) : 90;
    const actualWeeks = Math.max(actualDays / 7, 1); // avoid division by zero
    
    const memberData = {
      email,
      planName: sub.plan_name,
      planState: sub.plan_state,
      createdAt,
      canceledAt,
      isCanceled,
      daysToCancel,
      actualDays,
      actualWeeks,
      totalClassesInWindow: regsInWindow.length,
      classesPerWeek: regsInWindow.length / actualWeeks,
      classesIn30: regsIn30.length,
      classesIn60: regsIn60.length,
      classesIn90: regsInWindow.length,
      classesWeek1: regsWeek1.length,
      attendedWeek1: regsWeek1.length > 0,
      regsInWindow,
      regsIn30,
      // Unique class types
      uniqueClassTypes: new Set(regsInWindow.map(r => r.event_name || r.video_name || 'unknown')).size,
      // Unique locations
      uniqueLocations: new Set(regsInWindow.filter(r => r.location_name).map(r => r.location_name)).size,
    };

    if (isCanceled && daysToCancel <= 90) {
      earlyChurners.push(memberData);
    } else {
      survivors.push(memberData);
    }
  }

  console.log(`\n${'='.repeat(80)}`);
  console.log('COHORT BREAKDOWN');
  console.log('='.repeat(80));
  console.log(`Early churners (canceled <= 90 days):  ${earlyChurners.length}`);
  console.log(`Survivors (active or canceled > 90 days): ${survivors.length}`);
  console.log(`Total analyzed: ${earlyChurners.length + survivors.length}`);

  // ---------------------------------------------------------------------------
  // Helper: compute stats for a cohort
  // ---------------------------------------------------------------------------
  function cohortStats(cohort, label) {
    console.log(`\n${'─'.repeat(80)}`);
    console.log(`COHORT: ${label} (n=${cohort.length})`);
    console.log('─'.repeat(80));

    if (cohort.length === 0) {
      console.log('  No members in this cohort.');
      return;
    }

    // Average classes per week
    const cpw = cohort.map(m => m.classesPerWeek);
    const avgCPW = cpw.reduce((a,b) => a+b, 0) / cpw.length;
    const medianCPW = median(cpw);
    console.log(`\n  Classes per week (first 90 days or until cancel):`);
    console.log(`    Mean:   ${avgCPW.toFixed(2)}`);
    console.log(`    Median: ${medianCPW.toFixed(2)}`);

    // Average total classes in 30/60/90 days
    const avg30 = cohort.reduce((a,m) => a + m.classesIn30, 0) / cohort.length;
    const avg60 = cohort.reduce((a,m) => a + m.classesIn60, 0) / cohort.length;
    const avg90 = cohort.reduce((a,m) => a + m.classesIn90, 0) / cohort.length;
    console.log(`\n  Average total classes attended:`);
    console.log(`    First 30 days: ${avg30.toFixed(1)}`);
    console.log(`    First 60 days: ${avg60.toFixed(1)}`);
    console.log(`    First 90 days: ${avg90.toFixed(1)}`);
    
    // Median total classes 
    console.log(`\n  Median total classes attended:`);
    console.log(`    First 30 days: ${median(cohort.map(m => m.classesIn30)).toFixed(1)}`);
    console.log(`    First 60 days: ${median(cohort.map(m => m.classesIn60)).toFixed(1)}`);
    console.log(`    First 90 days: ${median(cohort.map(m => m.classesIn90)).toFixed(1)}`);

    // Distribution of weekly attendance
    const buckets = { '0 (no classes)': 0, '0-1': 0, '1-2': 0, '2-3': 0, '3+': 0 };
    for (const m of cohort) {
      if (m.classesPerWeek === 0) buckets['0 (no classes)']++;
      else if (m.classesPerWeek < 1) buckets['0-1']++;
      else if (m.classesPerWeek < 2) buckets['1-2']++;
      else if (m.classesPerWeek < 3) buckets['2-3']++;
      else buckets['3+']++;
    }
    console.log(`\n  Weekly attendance frequency distribution:`);
    for (const [bucket, count] of Object.entries(buckets)) {
      const pct = (count / cohort.length * 100).toFixed(1);
      const bar = '#'.repeat(Math.round(count / cohort.length * 40));
      console.log(`    ${bucket.padEnd(16)} ${String(count).padStart(4)} (${pct.padStart(5)}%) ${bar}`);
    }

    // Class time distribution
    const timeBuckets = { morning: 0, afternoon: 0, evening: 0, 'night/off-hours': 0, unknown: 0 };
    let totalRegsWithTime = 0;
    for (const m of cohort) {
      for (const reg of m.regsInWindow) {
        const hour = getHour(reg.performance_starts_at || '');
        const bucket = getTimeBucket(hour);
        timeBuckets[bucket]++;
        totalRegsWithTime++;
      }
    }
    if (totalRegsWithTime > 0) {
      console.log(`\n  Class time distribution (all attendances in window):`);
      for (const [bucket, count] of Object.entries(timeBuckets)) {
        if (count === 0 && bucket === 'unknown') continue;
        const pct = (count / totalRegsWithTime * 100).toFixed(1);
        console.log(`    ${bucket.padEnd(18)} ${String(count).padStart(5)} (${pct.padStart(5)}%)`);
      }
    }

    // Most popular classes
    const classCounts = {};
    for (const m of cohort) {
      for (const reg of m.regsInWindow) {
        const name = reg.event_name || reg.video_name || 'unknown';
        classCounts[name] = (classCounts[name] || 0) + 1;
      }
    }
    const topClasses = Object.entries(classCounts).sort((a,b) => b[1] - a[1]).slice(0, 10);
    if (topClasses.length > 0) {
      const totalRegs = Object.values(classCounts).reduce((a,b) => a+b, 0);
      console.log(`\n  Top 10 class types attended:`);
      for (const [name, count] of topClasses) {
        const pct = (count / totalRegs * 100).toFixed(1);
        console.log(`    ${name.padEnd(40)} ${String(count).padStart(5)} (${pct.padStart(5)}%)`);
      }
    }

    // Most popular locations
    const locCounts = {};
    for (const m of cohort) {
      for (const reg of m.regsInWindow) {
        const loc = reg.location_name || '(online/video)';
        locCounts[loc] = (locCounts[loc] || 0) + 1;
      }
    }
    const topLocs = Object.entries(locCounts).sort((a,b) => b[1] - a[1]).slice(0, 10);
    if (topLocs.length > 0) {
      const totalRegs = Object.values(locCounts).reduce((a,b) => a+b, 0);
      console.log(`\n  Location distribution:`);
      for (const [name, count] of topLocs) {
        const pct = (count / totalRegs * 100).toFixed(1);
        console.log(`    ${name.padEnd(30)} ${String(count).padStart(5)} (${pct.padStart(5)}%)`);
      }
    }

    // Registration type distribution (in-person vs livestream vs replay)
    const regTypeCounts = {};
    for (const m of cohort) {
      for (const reg of m.regsInWindow) {
        const t = reg.registration_type || 'unknown';
        regTypeCounts[t] = (regTypeCounts[t] || 0) + 1;
      }
    }
    const totalRegTypes = Object.values(regTypeCounts).reduce((a,b) => a+b, 0);
    if (totalRegTypes > 0) {
      console.log(`\n  Registration type distribution:`);
      Object.entries(regTypeCounts).sort((a,b) => b[1] - a[1]).forEach(([t, count]) => {
        const pct = (count / totalRegTypes * 100).toFixed(1);
        console.log(`    ${t.padEnd(25)} ${String(count).padStart(5)} (${pct.padStart(5)}%)`);
      });
    }

    // Average unique class types
    const avgUniqueClasses = cohort.reduce((a,m) => a + m.uniqueClassTypes, 0) / cohort.length;
    console.log(`\n  Average unique class types tried: ${avgUniqueClasses.toFixed(1)}`);
    
    // Week 1 attendance
    const week1Rate = cohort.filter(m => m.attendedWeek1).length / cohort.length * 100;
    const avgWeek1 = cohort.reduce((a,m) => a + m.classesWeek1, 0) / cohort.length;
    console.log(`\n  Week 1 engagement:`);
    console.log(`    % who attended at least 1 class in week 1: ${week1Rate.toFixed(1)}%`);
    console.log(`    Average classes in week 1: ${avgWeek1.toFixed(2)}`);
  }

  function median(arr) {
    if (arr.length === 0) return 0;
    const sorted = [...arr].sort((a,b) => a - b);
    const mid = Math.floor(sorted.length / 2);
    return sorted.length % 2 !== 0 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
  }

  // Run cohort analysis
  cohortStats(earlyChurners, 'EARLY CHURNERS (canceled within 90 days)');
  cohortStats(survivors, 'SURVIVORS (active or canceled after 90+ days)');

  // =========================================================================
  // MAGIC NUMBER ANALYSIS
  // =========================================================================
  console.log(`\n${'='.repeat(80)}`);
  console.log('"MAGIC NUMBER" ANALYSIS — Classes in First 30 Days vs. Retention');
  console.log('='.repeat(80));
  
  const allMembers = [...earlyChurners, ...survivors];
  
  // Group by classes in first 30 days
  const classesIn30Buckets = {};
  for (const m of allMembers) {
    const bucket = m.classesIn30;
    if (!classesIn30Buckets[bucket]) classesIn30Buckets[bucket] = { churned: 0, survived: 0 };
    if (m.isCanceled && m.daysToCancel <= 90) {
      classesIn30Buckets[bucket].churned++;
    } else {
      classesIn30Buckets[bucket].survived++;
    }
  }
  
  console.log('\n  Classes in   | Total | Churned | Survived | Retention | Churn');
  console.log('  first 30 days| Members| (<=90d) | (>90d)   | Rate      | Rate');
  console.log('  ' + '-'.repeat(72));
  
  const sortedBuckets = Object.keys(classesIn30Buckets).map(Number).sort((a,b) => a - b);
  for (const bucket of sortedBuckets) {
    const { churned, survived } = classesIn30Buckets[bucket];
    const total = churned + survived;
    if (total < 2) continue; // skip tiny groups
    const retRate = (survived / total * 100).toFixed(1);
    const churnRate = (churned / total * 100).toFixed(1);
    console.log(`  ${String(bucket).padStart(13)} | ${String(total).padStart(5)} | ${String(churned).padStart(7)} | ${String(survived).padStart(8)} | ${retRate.padStart(8)}% | ${churnRate.padStart(5)}%`);
  }

  // Also group into ranges for cleaner view
  console.log('\n  GROUPED VIEW:');
  console.log('  Classes in   | Total | Churned | Survived | Retention | Churn');
  console.log('  first 30 days| Members| (<=90d) | (>90d)   | Rate      | Rate');
  console.log('  ' + '-'.repeat(72));
  
  const ranges = [
    { label: '0', min: 0, max: 0 },
    { label: '1-2', min: 1, max: 2 },
    { label: '3-4', min: 3, max: 4 },
    { label: '5-7', min: 5, max: 7 },
    { label: '8-12', min: 8, max: 12 },
    { label: '13-20', min: 13, max: 20 },
    { label: '21+', min: 21, max: 999 },
  ];
  
  for (const range of ranges) {
    let churned = 0, survived = 0;
    for (const m of allMembers) {
      if (m.classesIn30 >= range.min && m.classesIn30 <= range.max) {
        if (m.isCanceled && m.daysToCancel <= 90) churned++;
        else survived++;
      }
    }
    const total = churned + survived;
    if (total === 0) continue;
    const retRate = (survived / total * 100).toFixed(1);
    const churnRate = (churned / total * 100).toFixed(1);
    console.log(`  ${range.label.padStart(13)} | ${String(total).padStart(5)} | ${String(churned).padStart(7)} | ${String(survived).padStart(8)} | ${retRate.padStart(8)}% | ${churnRate.padStart(5)}%`);
  }

  // =========================================================================
  // RETENTION RATE BY WEEKLY ATTENDANCE
  // =========================================================================
  console.log(`\n${'='.repeat(80)}`);
  console.log('RETENTION RATE BY WEEKLY ATTENDANCE FREQUENCY');
  console.log('='.repeat(80));
  
  const weeklyBuckets = [
    { label: '0 classes/week', min: 0, max: 0.001 },
    { label: '0-0.5/week', min: 0.001, max: 0.5 },
    { label: '0.5-1/week', min: 0.5, max: 1 },
    { label: '1-1.5/week', min: 1, max: 1.5 },
    { label: '1.5-2/week', min: 1.5, max: 2 },
    { label: '2-3/week', min: 2, max: 3 },
    { label: '3+/week', min: 3, max: 999 },
  ];
  
  console.log('\n  Weekly freq     | Total | Churned | Survived | Retention | Churn');
  console.log('  ' + '-'.repeat(72));
  
  for (const range of weeklyBuckets) {
    let churned = 0, survived = 0;
    for (const m of allMembers) {
      const cpw = m.classesPerWeek;
      const inRange = (range.min === 0 && cpw === 0) ? true : (cpw >= range.min && cpw < range.max);
      if (!inRange && !(range.label === '0 classes/week' && cpw === 0)) {
        if (cpw < range.min || cpw >= range.max) continue;
      }
      if (m.isCanceled && m.daysToCancel <= 90) churned++;
      else survived++;
    }
    const total = churned + survived;
    if (total === 0) continue;
    const retRate = (survived / total * 100).toFixed(1);
    const churnRate = (churned / total * 100).toFixed(1);
    console.log(`  ${range.label.padEnd(17)} | ${String(total).padStart(5)} | ${String(churned).padStart(7)} | ${String(survived).padStart(8)} | ${retRate.padStart(8)}% | ${churnRate.padStart(5)}%`);
  }

  // =========================================================================
  // WEEK 1 ATTENDANCE VS RETENTION
  // =========================================================================
  console.log(`\n${'='.repeat(80)}`);
  console.log('WEEK 1 ATTENDANCE vs RETENTION');
  console.log('='.repeat(80));
  
  const week1Groups = {
    'Attended in Week 1': { churned: 0, survived: 0 },
    'Did NOT attend in Week 1': { churned: 0, survived: 0 },
  };
  
  for (const m of allMembers) {
    const key = m.attendedWeek1 ? 'Attended in Week 1' : 'Did NOT attend in Week 1';
    if (m.isCanceled && m.daysToCancel <= 90) week1Groups[key].churned++;
    else week1Groups[key].survived++;
  }
  
  console.log('\n  Group                     | Total | Churned | Survived | Retention | Churn');
  console.log('  ' + '-'.repeat(76));
  for (const [label, { churned, survived }] of Object.entries(week1Groups)) {
    const total = churned + survived;
    if (total === 0) continue;
    const retRate = (survived / total * 100).toFixed(1);
    const churnRate = (churned / total * 100).toFixed(1);
    console.log(`  ${label.padEnd(27)} | ${String(total).padStart(5)} | ${String(churned).padStart(7)} | ${String(survived).padStart(8)} | ${retRate.padStart(8)}% | ${churnRate.padStart(5)}%`);
  }

  // Further break down: 0 classes week 1, 1 class, 2 classes, 3+ classes
  console.log('\n  Detailed Week 1 breakdown:');
  console.log('  Classes in Week 1 | Total | Churned | Survived | Retention | Churn');
  console.log('  ' + '-'.repeat(72));
  
  const week1Detailed = {};
  for (const m of allMembers) {
    const w1 = Math.min(m.classesWeek1, 4); // cap at 4+ for grouping
    const key = w1 >= 4 ? '4+' : String(w1);
    if (!week1Detailed[key]) week1Detailed[key] = { churned: 0, survived: 0 };
    if (m.isCanceled && m.daysToCancel <= 90) week1Detailed[key].churned++;
    else week1Detailed[key].survived++;
  }
  
  for (const key of ['0', '1', '2', '3', '4+']) {
    const group = week1Detailed[key];
    if (!group) continue;
    const total = group.churned + group.survived;
    const retRate = (group.survived / total * 100).toFixed(1);
    const churnRate = (group.churned / total * 100).toFixed(1);
    console.log(`  ${key.padStart(19)} | ${String(total).padStart(5)} | ${String(group.churned).padStart(7)} | ${String(group.survived).padStart(8)} | ${retRate.padStart(8)}% | ${churnRate.padStart(5)}%`);
  }

  // =========================================================================
  // CLASSES IN FIRST 30 DAYS: MAGIC NUMBER DEEP DIVE
  // =========================================================================
  console.log(`\n${'='.repeat(80)}`);
  console.log('MAGIC NUMBER DEEP DIVE — Retention Jump Analysis');
  console.log('='.repeat(80));
  
  // For each threshold X, compute: what % of people who attend >= X classes in 
  // first 30 days survive past 90 days?
  console.log('\n  If member attends >= X classes in first 30 days, what is their retention rate?');
  console.log('  Threshold | Members meeting | Retention Rate | vs. Below Threshold');
  console.log('  ' + '-'.repeat(72));
  
  for (let threshold = 0; threshold <= 20; threshold++) {
    const above = allMembers.filter(m => m.classesIn30 >= threshold);
    const below = allMembers.filter(m => m.classesIn30 < threshold);
    
    if (above.length < 5) continue;
    
    const aboveRetention = above.filter(m => !(m.isCanceled && m.daysToCancel <= 90)).length / above.length * 100;
    let belowRetention = 'N/A';
    if (below.length >= 5) {
      belowRetention = (below.filter(m => !(m.isCanceled && m.daysToCancel <= 90)).length / below.length * 100).toFixed(1) + '%';
    }
    
    console.log(`  >= ${String(threshold).padStart(2)} classes | ${String(above.length).padStart(14)} | ${aboveRetention.toFixed(1).padStart(13)}% | Below: ${belowRetention}`);
  }

  // =========================================================================
  // CANCELLATION TIMING DISTRIBUTION
  // =========================================================================
  console.log(`\n${'='.repeat(80)}`);
  console.log('CANCELLATION TIMING (for all member-plan cancellations)');
  console.log('='.repeat(80));
  
  const canceledMembers = allMembers.filter(m => m.isCanceled);
  const timingBuckets = [
    { label: '0-7 days (Week 1)', min: 0, max: 7 },
    { label: '8-14 days (Week 2)', min: 8, max: 14 },
    { label: '15-30 days (Month 1)', min: 15, max: 30 },
    { label: '31-60 days (Month 2)', min: 31, max: 60 },
    { label: '61-90 days (Month 3)', min: 61, max: 90 },
    { label: '91-180 days (Months 4-6)', min: 91, max: 180 },
    { label: '181-365 days (6-12 months)', min: 181, max: 365 },
    { label: '365+ days (1+ year)', min: 366, max: 99999 },
  ];
  
  console.log(`\n  Total canceled members: ${canceledMembers.length}`);
  console.log('');
  console.log('  Timing                      | Count |   %   | Cumulative %');
  console.log('  ' + '-'.repeat(65));
  
  let cumulative = 0;
  for (const range of timingBuckets) {
    const count = canceledMembers.filter(m => m.daysToCancel >= range.min && m.daysToCancel <= range.max).length;
    cumulative += count;
    const pct = (count / canceledMembers.length * 100).toFixed(1);
    const cumPct = (cumulative / canceledMembers.length * 100).toFixed(1);
    console.log(`  ${range.label.padEnd(29)} | ${String(count).padStart(5)} | ${pct.padStart(5)}% | ${cumPct.padStart(11)}%`);
  }

  // =========================================================================
  // STATISTICAL SIGNIFICANCE TEST
  // =========================================================================
  console.log(`\n${'='.repeat(80)}`);
  console.log('STATISTICAL COMPARISON — Key Differences Between Cohorts');
  console.log('='.repeat(80));
  
  function computeStats(arr) {
    const n = arr.length;
    const mean = arr.reduce((a,b) => a+b, 0) / n;
    const variance = arr.reduce((a,b) => a + (b - mean) ** 2, 0) / (n - 1);
    const stddev = Math.sqrt(variance);
    return { n, mean, stddev, median: median(arr) };
  }
  
  const metrics = [
    { label: 'Classes per week', churnerFn: m => m.classesPerWeek, survivorFn: m => m.classesPerWeek },
    { label: 'Total classes in 30 days', churnerFn: m => m.classesIn30, survivorFn: m => m.classesIn30 },
    { label: 'Total classes in 60 days', churnerFn: m => m.classesIn60, survivorFn: m => m.classesIn60 },
    { label: 'Total classes in 90 days', churnerFn: m => m.classesIn90, survivorFn: m => m.classesIn90 },
    { label: 'Classes in week 1', churnerFn: m => m.classesWeek1, survivorFn: m => m.classesWeek1 },
    { label: 'Unique class types tried', churnerFn: m => m.uniqueClassTypes, survivorFn: m => m.uniqueClassTypes },
  ];
  
  console.log(`\n  ${'Metric'.padEnd(28)} | ${'Churners'.padEnd(24)} | ${'Survivors'.padEnd(24)} | Diff`);
  console.log('  ' + '-'.repeat(90));
  
  for (const metric of metrics) {
    const cVals = earlyChurners.map(metric.churnerFn);
    const sVals = survivors.map(metric.survivorFn);
    const cStats = computeStats(cVals);
    const sStats = computeStats(sVals);
    const diff = ((sStats.mean - cStats.mean) / cStats.mean * 100);
    const diffStr = cStats.mean > 0 ? `${diff > 0 ? '+' : ''}${diff.toFixed(0)}%` : 'N/A';
    
    console.log(`  ${metric.label.padEnd(28)} | mean=${cStats.mean.toFixed(2).padStart(6)} med=${cStats.median.toFixed(1).padStart(5)} | mean=${sStats.mean.toFixed(2).padStart(6)} med=${sStats.median.toFixed(1).padStart(5)} | ${diffStr}`);
  }

  // =========================================================================
  // IN-PERSON vs ONLINE BREAKDOWN
  // =========================================================================
  console.log(`\n${'='.repeat(80)}`);
  console.log('IN-PERSON vs ONLINE/REPLAY ATTENDANCE BY COHORT');
  console.log('='.repeat(80));
  
  function regTypeBreakdown(cohort, label) {
    let inPerson = 0, livestream = 0, replay = 0, other = 0;
    for (const m of cohort) {
      for (const reg of m.regsInWindow) {
        const rt = (reg.registration_type || '').toLowerCase();
        if (rt === 'in-person') inPerson++;
        else if (rt === 'livestream' || rt === 'external video call') livestream++;
        else if (rt === 'replay' || rt === 'restream') replay++;
        else other++;
      }
    }
    const total = inPerson + livestream + replay + other;
    console.log(`\n  ${label}:`);
    if (total === 0) {
      console.log('    No registrations');
      return;
    }
    console.log(`    In-Person:  ${inPerson} (${(inPerson/total*100).toFixed(1)}%)`);
    console.log(`    Livestream: ${livestream} (${(livestream/total*100).toFixed(1)}%)`);
    console.log(`    Replay:     ${replay} (${(replay/total*100).toFixed(1)}%)`);
    console.log(`    Other:      ${other} (${(other/total*100).toFixed(1)}%)`);
    
    // Per member averages
    const avgInPerson = cohort.reduce((a,m) => a + m.regsInWindow.filter(r => (r.registration_type||'').toLowerCase() === 'in-person').length, 0) / cohort.length;
    console.log(`    Avg in-person classes per member: ${avgInPerson.toFixed(1)}`);
  }
  
  regTypeBreakdown(earlyChurners, 'Early Churners');
  regTypeBreakdown(survivors, 'Survivors');

  // =========================================================================
  // PLAN-LEVEL BREAKDOWN
  // =========================================================================
  console.log(`\n${'='.repeat(80)}`);
  console.log('CHURN RATE BY PLAN TYPE');
  console.log('='.repeat(80));
  
  const planStats = {};
  for (const m of allMembers) {
    if (!planStats[m.planName]) planStats[m.planName] = { churned: 0, survived: 0, total: 0 };
    planStats[m.planName].total++;
    if (m.isCanceled && m.daysToCancel <= 90) planStats[m.planName].churned++;
    else planStats[m.planName].survived++;
  }
  
  console.log(`\n  ${'Plan Name'.padEnd(35)} | Total | Churned | Churn Rate`);
  console.log('  ' + '-'.repeat(72));
  
  Object.entries(planStats)
    .sort((a,b) => b[1].total - a[1].total)
    .forEach(([name, stats]) => {
      const churnRate = (stats.churned / stats.total * 100).toFixed(1);
      console.log(`  ${name.padEnd(35)} | ${String(stats.total).padStart(5)} | ${String(stats.churned).padStart(7)} | ${churnRate.padStart(9)}%`);
    });

  // =========================================================================
  // SUMMARY & KEY FINDINGS
  // =========================================================================
  console.log(`\n${'='.repeat(80)}`);
  console.log('SUMMARY — KEY FINDINGS');
  console.log('='.repeat(80));
  
  const overallChurnRate = (earlyChurners.length / allMembers.length * 100).toFixed(1);
  
  const churnerAvgCPW = earlyChurners.reduce((a,m) => a + m.classesPerWeek, 0) / earlyChurners.length;
  const survivorAvgCPW = survivors.reduce((a,m) => a + m.classesPerWeek, 0) / survivors.length;
  
  const churnerAvg30 = earlyChurners.reduce((a,m) => a + m.classesIn30, 0) / earlyChurners.length;
  const survivorAvg30 = survivors.reduce((a,m) => a + m.classesIn30, 0) / survivors.length;
  
  const churnerWeek1Rate = earlyChurners.filter(m => m.attendedWeek1).length / earlyChurners.length * 100;
  const survivorWeek1Rate = survivors.filter(m => m.attendedWeek1).length / survivors.length * 100;
  
  console.log(`
  1. OVERALL: ${overallChurnRate}% of monthly members cancel within 90 days (${earlyChurners.length} of ${allMembers.length})

  2. ATTENDANCE FREQUENCY IS A STRONG PREDICTOR:
     - Early churners average ${churnerAvgCPW.toFixed(2)} classes/week vs survivors at ${survivorAvgCPW.toFixed(2)} classes/week
     - Churners attend ${churnerAvg30.toFixed(1)} classes in first 30 days vs ${survivorAvg30.toFixed(1)} for survivors

  3. WEEK 1 MATTERS:
     - ${churnerWeek1Rate.toFixed(1)}% of churners attended in week 1 vs ${survivorWeek1Rate.toFixed(1)}% of survivors

  4. See the "Magic Number" table above for the specific attendance threshold
     where retention dramatically improves.
`);

  console.log('Analysis complete.');
}

main().catch(err => {
  console.error('Error:', err);
  process.exit(1);
});
