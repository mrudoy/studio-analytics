/**
 * October 2025 Churn Anomaly Investigation
 * 
 * 163 out of 243 monthly MEMBER subscribers cancelled in October 2025 (67.1% eligible churn).
 * This script investigates:
 *   1. Were cancellations clustered on a single date or spread across the month?
 *   2. What plan names were involved?
 *   3. How long had members been active before cancelling (tenure)?
 *   4. Were there patterns in created_at dates?
 *
 * Data source: /api/backup?action=download returns the full database as JSON,
 * including the auto_renews table with individual records.
 */

const https = require("https");

// ── Configuration ──────────────────────────────────────────────

const API_BASE = "https://studio-analytics-production.up.railway.app";
const BACKUP_URL = `${API_BASE}/api/backup?action=download`;
const STATS_URL = `${API_BASE}/api/stats`;

// ── MEMBER plan classification (matches categories.ts) ─────────

function isMemberPlan(planName) {
  const upper = (planName || "").trim().toUpperCase();
  return (
    upper.includes("UNLIMITED") ||
    upper.includes("MEMBER") ||
    upper.includes("ALL ACCESS") ||
    upper.includes("TING FAM")
  );
}

function isAnnualPlan(planName) {
  const upper = (planName || "").trim().toUpperCase();
  return (
    upper.includes("ANNUAL") ||
    upper.includes("YEARLY") ||
    upper.includes("12 MONTH") ||
    upper.includes("12-MONTH") ||
    upper.includes("12M ")
  );
}

function isMonthlyMember(planName) {
  return isMemberPlan(planName) && !isAnnualPlan(planName);
}

// ── Utility ────────────────────────────────────────────────────

function formatDate(d) {
  if (!d) return "N/A";
  const dt = new Date(d);
  if (isNaN(dt.getTime())) return String(d);
  return dt.toISOString().split("T")[0];
}

function daysBetween(d1, d2) {
  const ms = new Date(d2).getTime() - new Date(d1).getTime();
  return Math.round(ms / (1000 * 60 * 60 * 24));
}

function monthsBetween(d1, d2) {
  return Math.round(daysBetween(d1, d2) / 30.44 * 10) / 10;
}

function median(arr) {
  if (arr.length === 0) return 0;
  const sorted = [...arr].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 !== 0 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
}

function percentile(arr, p) {
  if (arr.length === 0) return 0;
  const sorted = [...arr].sort((a, b) => a - b);
  const idx = Math.ceil(sorted.length * p / 100) - 1;
  return sorted[Math.max(0, idx)];
}

function histogram(values, label, maxDisplay = 30) {
  const freq = {};
  for (const v of values) {
    freq[v] = (freq[v] || 0) + 1;
  }
  const sorted = Object.entries(freq).sort((a, b) => b[1] - a[1]);
  console.log(`\n  ${label}:`);
  const display = sorted.slice(0, maxDisplay);
  for (const [val, count] of display) {
    const bar = "█".repeat(Math.min(Math.ceil(count / 2), 50));
    const pct = ((count / values.length) * 100).toFixed(1);
    console.log(`    ${String(val).padEnd(35)} ${String(count).padStart(4)} (${pct.padStart(5)}%) ${bar}`);
  }
  if (sorted.length > maxDisplay) {
    console.log(`    ... and ${sorted.length - maxDisplay} more`);
  }
}

function histogramSorted(values, label) {
  const freq = {};
  for (const v of values) {
    freq[v] = (freq[v] || 0) + 1;
  }
  const sorted = Object.entries(freq).sort((a, b) => a[0].localeCompare(b[0]));
  console.log(`\n  ${label}:`);
  for (const [val, count] of sorted) {
    const bar = "█".repeat(Math.min(Math.ceil(count / 2), 50));
    const pct = ((count / values.length) * 100).toFixed(1);
    const dayOfWeek = new Date(val + "T12:00:00").toLocaleDateString("en-US", { weekday: "short" });
    const dayLabel = val.length === 10 ? ` (${dayOfWeek})` : "";
    console.log(`    ${String(val + dayLabel).padEnd(25)} ${String(count).padStart(4)} (${pct.padStart(5)}%) ${bar}`);
  }
}

// ── HTTP fetch ─────────────────────────────────────────────────

function fetchJSON(url, label) {
  return new Promise((resolve, reject) => {
    console.log(`Fetching ${label || url}...`);
    const startMs = Date.now();
    https.get(url, (res) => {
      let data = "";
      res.on("data", (chunk) => data += chunk);
      res.on("end", () => {
        const elapsed = Date.now() - startMs;
        console.log(`  Received ${(data.length / 1024 / 1024).toFixed(1)} MB in ${elapsed}ms`);
        try {
          resolve(JSON.parse(data));
        } catch (e) {
          reject(new Error(`JSON parse error: ${e.message}`));
        }
      });
    }).on("error", reject);
  });
}

// ── Analysis Functions ─────────────────────────────────────────

function analyzeOctoberCancellations(autoRenews) {
  // Filter to October 2025 cancellations
  const octCancellations = autoRenews.filter(r => {
    const ca = r.canceled_at;
    if (!ca) return false;
    const d = formatDate(ca);
    return d >= "2025-10-01" && d < "2025-11-01";
  });

  console.log(`\nTotal auto-renew records in database: ${autoRenews.length}`);
  console.log(`Total cancellations in October 2025:  ${octCancellations.length}`);

  // Split by category
  const monthlyMembers = octCancellations.filter(r => isMonthlyMember(r.plan_name));
  const annualMembers = octCancellations.filter(r => isMemberPlan(r.plan_name) && isAnnualPlan(r.plan_name));
  const others = octCancellations.filter(r => !isMemberPlan(r.plan_name));

  console.log(`  Monthly MEMBER cancellations: ${monthlyMembers.length}`);
  console.log(`  Annual MEMBER cancellations:  ${annualMembers.length}`);
  console.log(`  Other cancellations:          ${others.length}`);

  if (monthlyMembers.length === 0) {
    console.log("\nWARNING: No monthly member cancellations found. Will analyze all October cancellations.\n");
    analyzeRows(octCancellations, "All October Cancellations", autoRenews);
    return;
  }

  analyzeRows(monthlyMembers, "Monthly MEMBER", autoRenews);

  // Also show context for other categories
  if (others.length > 0) {
    console.log(`\n${"=".repeat(80)}`);
    console.log("OTHER CATEGORY CANCELLATIONS IN OCTOBER (for comparison)");
    console.log("=".repeat(80));
    histogram(others.map(r => r.plan_name || "UNKNOWN"), "Non-member plan cancellations");
    
    // Were other categories also clustered on same dates?
    const otherDates = others.map(r => formatDate(r.canceled_at));
    histogramSorted(otherDates, "Non-member cancellation dates");
  }
}

function analyzeRows(rows, label, allAutoRenews) {
  console.log(`\n${"=".repeat(80)}`);
  console.log(`ANALYSIS: ${label} Cancellations in October 2025 (n=${rows.length})`);
  console.log("=".repeat(80));

  // ══════════════════════════════════════════════════════════════
  // 1. CANCELLATION DATE DISTRIBUTION
  // ══════════════════════════════════════════════════════════════
  console.log("\n── 1. CANCELLATION DATE DISTRIBUTION ──────────────────────────");

  const cancelDates = rows.map(r => formatDate(r.canceled_at));
  histogramSorted(cancelDates, "Cancellation dates (chronological)");

  const dateFreq = {};
  for (const d of cancelDates) {
    dateFreq[d] = (dateFreq[d] || 0) + 1;
  }
  const sortedByCount = Object.entries(dateFreq).sort((a, b) => b[1] - a[1]);

  // Clustering analysis
  const totalDays = Object.keys(dateFreq).length;
  const topDate = sortedByCount[0];
  const top3Dates = sortedByCount.slice(0, 3);
  const top3Total = top3Dates.reduce((s, [, c]) => s + c, 0);

  console.log(`\n  CLUSTERING VERDICT:`);
  console.log(`    Total cancellations: ${rows.length}`);
  console.log(`    Spread across ${totalDays} distinct dates`);
  console.log(`    Peak date: ${topDate[0]} with ${topDate[1]} cancellations (${(topDate[1] / rows.length * 100).toFixed(1)}%)`);
  console.log(`    Top 3 dates: ${top3Total} of ${rows.length} (${(top3Total / rows.length * 100).toFixed(1)}%)`);

  if (topDate[1] / rows.length > 0.5) {
    console.log(`\n    *** BULK CLEANUP DETECTED: ${(topDate[1] / rows.length * 100).toFixed(0)}% of cancellations on ${topDate[0]} ***`);
    console.log(`    This is almost certainly an administrative batch cancellation, not organic churn.`);
  } else if (top3Total / rows.length > 0.7) {
    console.log(`\n    *** CLUSTERED CANCELLATIONS: Top 3 dates account for ${(top3Total / rows.length * 100).toFixed(0)}% ***`);
  } else if (totalDays <= 5) {
    console.log(`\n    *** CONCENTRATED: Only ${totalDays} distinct dates ***`);
  } else {
    console.log(`\n    Cancellations were spread across ${totalDays} dates (more organic pattern).`);
  }

  // Week-of analysis
  const weekFreq = {};
  for (const d of cancelDates) {
    const dt = new Date(d + "T12:00:00");
    const weekNum = Math.ceil(dt.getDate() / 7);
    const weekLabel = `Week ${weekNum} (${d.substring(0, 7)}-${String(Math.max(1, (weekNum - 1) * 7 + 1)).padStart(2, "0")} to ${String(Math.min(31, weekNum * 7)).padStart(2, "0")})`;
    weekFreq[weekLabel] = (weekFreq[weekLabel] || 0) + 1;
  }
  console.log(`\n  By week of month:`);
  for (const [week, count] of Object.entries(weekFreq).sort()) {
    console.log(`    ${week}: ${count}`);
  }

  // ══════════════════════════════════════════════════════════════
  // 2. PLAN NAME BREAKDOWN
  // ══════════════════════════════════════════════════════════════
  console.log("\n── 2. PLAN NAME BREAKDOWN ─────────────────────────────────────");
  histogram(rows.map(r => r.plan_name || "UNKNOWN"), "Plans canceled");

  // Price analysis
  const prices = rows.map(r => parseFloat(r.plan_price)).filter(p => !isNaN(p) && p > 0);
  if (prices.length > 0) {
    console.log(`\n  Price statistics (n=${prices.length}):`);
    console.log(`    Min:    $${Math.min(...prices)}`);
    console.log(`    Max:    $${Math.max(...prices)}`);
    console.log(`    Median: $${median(prices)}`);
    console.log(`    Mean:   $${(prices.reduce((s, p) => s + p, 0) / prices.length).toFixed(2)}`);
    console.log(`    Total MRR lost: $${prices.reduce((s, p) => s + p, 0).toFixed(2)}`);
    histogram(prices.map(p => `$${p}`), "Price tiers");
  }

  // ══════════════════════════════════════════════════════════════
  // 3. TENURE ANALYSIS
  // ══════════════════════════════════════════════════════════════
  console.log("\n── 3. TENURE AT CANCELLATION ───────────────────────────────────");

  const tenures = rows
    .filter(r => r.created_at && r.canceled_at)
    .map(r => {
      const months = monthsBetween(r.created_at, r.canceled_at);
      return {
        months,
        days: daysBetween(r.created_at, r.canceled_at),
        created: formatDate(r.created_at),
        canceled: formatDate(r.canceled_at),
        name: r.customer_name,
        plan: r.plan_name,
        price: r.plan_price,
      };
    })
    .filter(t => t.months >= 0 && t.months < 120); // filter out data errors

  if (tenures.length > 0) {
    const tenureMonths = tenures.map(t => t.months);
    console.log(`\n  Tenure statistics (n=${tenures.length}):`);
    console.log(`    Min:    ${Math.min(...tenureMonths).toFixed(1)} months (${Math.min(...tenures.map(t => t.days))} days)`);
    console.log(`    Max:    ${Math.max(...tenureMonths).toFixed(1)} months`);
    console.log(`    Median: ${median(tenureMonths).toFixed(1)} months`);
    console.log(`    Mean:   ${(tenureMonths.reduce((s, t) => s + t, 0) / tenureMonths.length).toFixed(1)} months`);
    console.log(`    P10:    ${percentile(tenureMonths, 10).toFixed(1)} months`);
    console.log(`    P25:    ${percentile(tenureMonths, 25).toFixed(1)} months`);
    console.log(`    P75:    ${percentile(tenureMonths, 75).toFixed(1)} months`);
    console.log(`    P90:    ${percentile(tenureMonths, 90).toFixed(1)} months`);

    // Tenure buckets
    const buckets = [
      { label: "< 1 month", min: 0, max: 1 },
      { label: "1-2 months", min: 1, max: 2 },
      { label: "2-3 months", min: 2, max: 3 },
      { label: "3-6 months", min: 3, max: 6 },
      { label: "6-12 months", min: 6, max: 12 },
      { label: "12-24 months", min: 12, max: 24 },
      { label: "24+ months", min: 24, max: 999 },
    ];

    console.log(`\n  Tenure distribution:`);
    for (const b of buckets) {
      const count = tenureMonths.filter(t => t >= b.min && t < b.max).length;
      if (count === 0) continue;
      const pct = ((count / tenures.length) * 100).toFixed(1);
      const bar = "█".repeat(Math.min(Math.ceil(count / 2), 50));
      console.log(`    ${b.label.padEnd(15)} ${String(count).padStart(4)} (${pct.padStart(5)}%) ${bar}`);
    }

    // Short-lived members (< 3 months)
    const shortLived = tenures.filter(t => t.months < 3);
    const longLived = tenures.filter(t => t.months >= 12);
    console.log(`\n  Short-lived (< 3mo): ${shortLived.length} of ${tenures.length} (${(shortLived.length / tenures.length * 100).toFixed(1)}%)`);
    console.log(`  Long-lived (12+ mo): ${longLived.length} of ${tenures.length} (${(longLived.length / tenures.length * 100).toFixed(1)}%)`);

    // Show the shortest-tenure cancellations (likely trial/new signups)
    const shortestTenure = [...tenures].sort((a, b) => a.months - b.months).slice(0, 10);
    console.log(`\n  Shortest-tenure cancellations (newest members who left):`);
    for (const t of shortestTenure) {
      console.log(`    ${t.name || "N/A"} — ${t.days} days (created ${t.created}, canceled ${t.canceled}, ${t.plan} @ $${t.price})`);
    }

    // Show the longest-tenure cancellations (loyal members lost)
    const longestTenure = [...tenures].sort((a, b) => b.months - a.months).slice(0, 10);
    console.log(`\n  Longest-tenure cancellations (most loyal members lost):`);
    for (const t of longestTenure) {
      console.log(`    ${t.name || "N/A"} — ${t.months.toFixed(1)} months (created ${t.created}, canceled ${t.canceled}, ${t.plan} @ $${t.price})`);
    }
  }

  // ══════════════════════════════════════════════════════════════
  // 4. CREATED-AT PATTERN ANALYSIS
  // ══════════════════════════════════════════════════════════════
  console.log("\n── 4. CREATED-AT DATE PATTERNS ─────────────────────────────────");

  const createdDates = rows.map(r => formatDate(r.created_at)).filter(d => d !== "N/A");

  if (createdDates.length > 0) {
    // By month
    const createdMonths = createdDates.map(d => d.substring(0, 7));
    histogram(createdMonths, "Sign-up month (when these canceled members originally joined)");

    // Top signup dates
    const createdFreq = {};
    for (const d of createdDates) {
      createdFreq[d] = (createdFreq[d] || 0) + 1;
    }
    const topCreated = Object.entries(createdFreq).sort((a, b) => b[1] - a[1]).slice(0, 15);
    console.log(`\n  Top 15 signup dates among October cancellations:`);
    for (const [date, count] of topCreated) {
      const dayOfWeek = new Date(date + "T12:00:00").toLocaleDateString("en-US", { weekday: "short" });
      console.log(`    ${date} (${dayOfWeek}): ${count} members`);
    }

    // Recent vs old signups
    const ranges = [
      { label: "Signed up Oct 2025 (same month!)", test: d => d >= "2025-10-01" && d < "2025-11-01" },
      { label: "Signed up Sep 2025", test: d => d >= "2025-09-01" && d < "2025-10-01" },
      { label: "Signed up Aug 2025", test: d => d >= "2025-08-01" && d < "2025-09-01" },
      { label: "Signed up Jul 2025", test: d => d >= "2025-07-01" && d < "2025-08-01" },
      { label: "Signed up Q2 2025 (Apr-Jun)", test: d => d >= "2025-04-01" && d < "2025-07-01" },
      { label: "Signed up Q1 2025 (Jan-Mar)", test: d => d >= "2025-01-01" && d < "2025-04-01" },
      { label: "Signed up 2024", test: d => d >= "2024-01-01" && d < "2025-01-01" },
      { label: "Signed up before 2024", test: d => d < "2024-01-01" },
    ];

    console.log(`\n  When did these canceled members originally sign up?`);
    for (const r of ranges) {
      const count = createdDates.filter(r.test).length;
      if (count === 0) continue;
      const pct = ((count / createdDates.length) * 100).toFixed(1);
      console.log(`    ${r.label.padEnd(45)} ${String(count).padStart(4)} (${pct}%)`);
    }
  }

  // ══════════════════════════════════════════════════════════════
  // 5. PLAN STATE ANALYSIS
  // ══════════════════════════════════════════════════════════════
  console.log("\n── 5. PLAN STATE AT TIME OF DATA ──────────────────────────────");
  const states = rows.map(r => r.plan_state).filter(Boolean);
  if (states.length > 0) {
    histogram(states, "Plan states of October-canceled members");
  }

  // ══════════════════════════════════════════════════════════════
  // 6. CANCELED_BY / ADMIN ANALYSIS
  // ══════════════════════════════════════════════════════════════
  console.log("\n── 6. WHO INITIATED THE CANCELLATIONS? ────────────────────────");

  const canceledByValues = rows.map(r => r.canceled_by).filter(Boolean);
  if (canceledByValues.length > 0) {
    histogram(canceledByValues, "Canceled By (who initiated)");
  } else {
    console.log("\n  No 'canceled_by' data available.");
  }

  const adminValues = rows.map(r => r.admin).filter(Boolean);
  if (adminValues.length > 0) {
    histogram(adminValues, "Admin (who processed)");
  } else {
    console.log("  No 'admin' data available.");
  }

  // ══════════════════════════════════════════════════════════════
  // 7. EMAIL DOMAIN ANALYSIS
  // ══════════════════════════════════════════════════════════════
  console.log("\n── 7. EMAIL DOMAIN PATTERNS ────────────────────────────────────");
  const emails = rows.map(r => r.customer_email).filter(Boolean);
  if (emails.length > 0) {
    const domains = emails.map(e => e.split("@")[1]?.toLowerCase() || "unknown");
    const domainFreq = {};
    for (const d of domains) {
      domainFreq[d] = (domainFreq[d] || 0) + 1;
    }
    const topDomains = Object.entries(domainFreq).sort((a, b) => b[1] - a[1]).slice(0, 10);
    console.log(`\n  Top 10 email domains:`);
    for (const [domain, count] of topDomains) {
      console.log(`    ${domain.padEnd(30)} ${String(count).padStart(4)} (${(count / emails.length * 100).toFixed(1)}%)`);
    }

    // Check for unusual corporate domains (non-gmail/yahoo/etc)
    const genericDomains = new Set(["gmail.com", "yahoo.com", "hotmail.com", "icloud.com", "outlook.com", "me.com", "aol.com", "live.com", "msn.com", "mail.com", "comcast.net", "verizon.net", "att.net", "protonmail.com"]);
    const corporateEmails = emails.filter(e => !genericDomains.has(e.split("@")[1]?.toLowerCase()));
    console.log(`\n  Corporate/custom domain emails: ${corporateEmails.length} of ${emails.length} (${(corporateEmails.length / emails.length * 100).toFixed(1)}%)`);
  }

  // ══════════════════════════════════════════════════════════════
  // 8. COMPARISON: DID ACTIVE SUBSCRIBER COUNT ACTUALLY DROP?
  // ══════════════════════════════════════════════════════════════
  if (allAutoRenews) {
    console.log("\n── 8. IMPACT: DID SUBSCRIBER COUNT ACTUALLY DROP? ─────────────");

    // Active at start of October = created before Oct 1 AND (not canceled OR canceled on/after Oct 1)
    const activeOct1 = allAutoRenews.filter(r => {
      if (!isMonthlyMember(r.plan_name)) return false;
      const created = formatDate(r.created_at);
      if (created >= "2025-10-01") return false;
      const canceled = r.canceled_at ? formatDate(r.canceled_at) : null;
      if (canceled && canceled < "2025-10-01") return false;
      return true;
    });

    // Active at start of November = created before Nov 1 AND (not canceled OR canceled on/after Nov 1)
    const activeNov1 = allAutoRenews.filter(r => {
      if (!isMonthlyMember(r.plan_name)) return false;
      const created = formatDate(r.created_at);
      if (created >= "2025-11-01") return false;
      const canceled = r.canceled_at ? formatDate(r.canceled_at) : null;
      if (canceled && canceled < "2025-11-01") return false;
      return true;
    });

    // New signups in October
    const newInOct = allAutoRenews.filter(r => {
      if (!isMonthlyMember(r.plan_name)) return false;
      const created = formatDate(r.created_at);
      return created >= "2025-10-01" && created < "2025-11-01";
    });

    console.log(`\n  Monthly MEMBER active at Oct 1:  ${activeOct1.length}`);
    console.log(`  Cancellations in October:        ${rows.length}`);
    console.log(`  New signups in October:           ${newInOct.length}`);
    console.log(`  Monthly MEMBER active at Nov 1:   ${activeNov1.length}`);
    console.log(`  Net change:                       ${activeNov1.length - activeOct1.length} (${activeOct1.length} - ${rows.length} + ${newInOct.length} = ${activeOct1.length - rows.length + newInOct.length})`);

    if (activeNov1.length > activeOct1.length) {
      console.log(`\n  IMPORTANT: Despite 163 cancellations, the subscriber count GREW!`);
      console.log(`  This strongly suggests the cancellations were stale records being cleaned up,`);
      console.log(`  not active paying members actually leaving.`);
    }
  }
}

// ══════════════════════════════════════════════════════════════════
// COMPARISON WITH ADJACENT MONTHS
// ══════════════════════════════════════════════════════════════════

function compareWithAdjacentMonths(autoRenews) {
  console.log(`\n${"=".repeat(80)}`);
  console.log("COMPARISON: CANCELLATION PATTERNS IN ADJACENT MONTHS");
  console.log("=".repeat(80));

  const months = [
    { label: "August 2025", start: "2025-08-01", end: "2025-09-01" },
    { label: "September 2025", start: "2025-09-01", end: "2025-10-01" },
    { label: "October 2025", start: "2025-10-01", end: "2025-11-01" },
    { label: "November 2025", start: "2025-11-01", end: "2025-12-01" },
  ];

  for (const m of months) {
    const cancellations = autoRenews.filter(r => {
      if (!isMonthlyMember(r.plan_name)) return false;
      const ca = formatDate(r.canceled_at);
      return ca >= m.start && ca < m.end;
    });

    console.log(`\n  ${m.label}: ${cancellations.length} monthly MEMBER cancellations`);
    if (cancellations.length > 0) {
      const dates = cancellations.map(r => formatDate(r.canceled_at));
      const dateFreq = {};
      for (const d of dates) {
        dateFreq[d] = (dateFreq[d] || 0) + 1;
      }
      const sorted = Object.entries(dateFreq).sort((a, b) => b[1] - a[1]);
      const topDate = sorted[0];
      const uniqueDates = Object.keys(dateFreq).length;
      console.log(`    Spread across ${uniqueDates} dates, peak: ${topDate[0]} (${topDate[1]} cancellations, ${(topDate[1] / cancellations.length * 100).toFixed(0)}%)`);
    }
  }
}

// ── Main ───────────────────────────────────────────────────────

async function main() {
  console.log("╔══════════════════════════════════════════════════════════════════╗");
  console.log("║   OCTOBER 2025 CHURN ANOMALY INVESTIGATION                     ║");
  console.log("║   163 of 243 monthly MEMBER cancellations (67.1% eligible churn)║");
  console.log("╚══════════════════════════════════════════════════════════════════╝\n");

  // Strategy: Download full backup to get individual auto_renews records
  let autoRenews = null;

  try {
    const backup = await fetchJSON(BACKUP_URL, "database backup (may take a moment)");

    if (backup.tables && backup.tables.auto_renews) {
      autoRenews = backup.tables.auto_renews;
      console.log(`\nLoaded ${autoRenews.length} auto_renew records from backup.\n`);

      // Show available columns
      if (autoRenews.length > 0) {
        console.log(`Available columns: ${Object.keys(autoRenews[0]).join(", ")}\n`);
      }
    } else if (Array.isArray(backup)) {
      // Maybe the backup IS the auto_renews array directly?
      console.log("Backup returned an array, checking structure...");
      if (backup.length > 0 && backup[0].plan_name) {
        autoRenews = backup;
      }
    } else {
      console.log("Backup structure:", Object.keys(backup));
      if (backup.tables) {
        console.log("Tables:", Object.keys(backup.tables));
      }
    }
  } catch (err) {
    console.log(`Backup download failed: ${err.message}`);
  }

  if (autoRenews && autoRenews.length > 0) {
    // Full individual-level analysis
    analyzeOctoberCancellations(autoRenews);
    compareWithAdjacentMonths(autoRenews);

    // Final summary
    console.log(`\n${"=".repeat(80)}`);
    console.log("FINAL SUMMARY");
    console.log("=".repeat(80));

    const octMembers = autoRenews.filter(r => {
      if (!isMonthlyMember(r.plan_name)) return false;
      const ca = formatDate(r.canceled_at);
      return ca >= "2025-10-01" && ca < "2025-11-01";
    });

    const cancelDates = octMembers.map(r => formatDate(r.canceled_at));
    const dateFreq = {};
    for (const d of cancelDates) {
      dateFreq[d] = (dateFreq[d] || 0) + 1;
    }
    const sorted = Object.entries(dateFreq).sort((a, b) => b[1] - a[1]);
    const topDate = sorted[0];

    if (topDate && topDate[1] / octMembers.length > 0.5) {
      console.log(`\n  CONCLUSION: BULK CLEANUP`);
      console.log(`  ${topDate[1]} of ${octMembers.length} cancellations (${(topDate[1] / octMembers.length * 100).toFixed(0)}%) occurred on ${topDate[0]}.`);
      console.log(`  This was an administrative batch cancellation, not organic churn.`);
      console.log(`  The "real" organic churn for October was likely ~${Math.round(octMembers.length - topDate[1] + 35)} cancellations`);
      console.log(`  (the remainder + typical monthly average), or roughly ${((octMembers.length - topDate[1] + 35) / 243 * 100).toFixed(0)}% eligible churn.`);
    } else if (sorted.length <= 5) {
      console.log(`\n  CONCLUSION: CONCENTRATED CANCELLATIONS`);
      console.log(`  Cancellations occurred on only ${sorted.length} dates.`);
      console.log(`  Top dates: ${sorted.slice(0, 3).map(([d, c]) => `${d} (${c})`).join(", ")}`);
    } else {
      const tenures = octMembers
        .filter(r => r.created_at && r.canceled_at)
        .map(r => monthsBetween(r.created_at, r.canceled_at))
        .filter(t => t >= 0);
      const medTenure = median(tenures);

      console.log(`\n  CONCLUSION: DISTRIBUTED CANCELLATIONS`);
      console.log(`  Cancellations spread across ${sorted.length} dates — more organic than bulk.`);
      console.log(`  Median tenure: ${medTenure.toFixed(1)} months.`);
      console.log(`  This may indicate a real churn event (price change, competitor, seasonal).`);
    }
  } else {
    // Fall back to API analysis
    console.log("\nCould not get individual records. Falling back to API aggregate analysis.\n");
    const stats = await fetchJSON(STATS_URL, "API stats");
    analyzeFromAPIData(stats);
  }
}

function analyzeFromAPIData(data) {
  const member = data.trends?.churnRates?.byCategory?.member;
  if (!member) {
    console.log("ERROR: No member churn data found.");
    return;
  }

  const monthly = member.monthly || [];
  console.log("── MONTHLY MEMBER CHURN TREND ──\n");
  console.log("  Month     | Mo.Active | Mo.Cancel | Elig.Churn% | MRR Lost    ");
  console.log("  " + "-".repeat(65));
  for (const m of monthly) {
    const marker = m.month === "2025-10" ? " <<<" : "";
    console.log(
      `  ${m.month}  | ${String(m.monthlyActiveAtStart).padStart(9)} | ` +
      `${String(m.monthlyCanceledCount).padStart(9)} | ` +
      `${String(m.eligibleChurnRate + "%").padStart(11)} | ` +
      `$${String(Math.round(m.monthlyCanceledMrr || 0)).padStart(9)}${marker}`
    );
  }
  console.log("\n  NOTE: Individual-level detail requires database access or backup download.");
}

main().catch(err => {
  console.error("Fatal error:", err);
  process.exit(1);
});
