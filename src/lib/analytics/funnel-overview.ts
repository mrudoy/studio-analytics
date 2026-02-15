/**
 * Funnel Overview — computes the stacked funnel visualization data:
 *
 *   New Customers          2,801
 *   First Visit (5-wk)       907   41.5% conversion
 *   Auto Renew (10-wk)        76    8.2% conversion
 *
 * Logic:
 *   Stage 1 — New Customers: all customer sign-ups in the period
 *   Stage 2 — First Visit:   customers who had a first visit within 5 weeks of sign-up
 *   Stage 3 — Auto Renew:    customers who started any auto-renew within 10 weeks of first visit
 *
 * The funnel is computed for:
 *   - All time
 *   - Each month
 *   - Rolling 30 / 60 / 90 day windows
 */

import { differenceInDays, subDays } from "date-fns";
import type { NewCustomer, FirstVisit, AutoRenew } from "@/types/union-data";
import { parseDate, getMonthKey } from "./date-utils";

/** Time windows (in days) for each funnel step */
const FIRST_VISIT_WINDOW_DAYS = 35; // 5 weeks
const AUTO_RENEW_WINDOW_DAYS = 70; // 10 weeks

export interface FunnelOverviewRow {
  period: string; // "All Time", "2025-01", "Rolling 30d", etc.
  newCustomers: number;
  firstVisits: number;
  firstVisitConversion: number; // percentage
  autoRenews: number;
  autoRenewConversion: number; // percentage (from first-visit stage, not from new-customers)
}

export interface FunnelOverviewResults {
  allTime: FunnelOverviewRow;
  monthly: FunnelOverviewRow[];
  rolling30: FunnelOverviewRow;
  rolling60: FunnelOverviewRow;
  rolling90: FunnelOverviewRow;
}

/**
 * Normalize "Last, First" → "first last" lowercase for cross-report matching.
 */
function normalizeName(name: string): string {
  let n = name.trim().toLowerCase();
  if (n.includes(",")) {
    const parts = n.split(",").map((p) => p.trim());
    if (parts.length === 2 && parts[0] && parts[1]) {
      n = parts[1] + " " + parts[0];
    }
  }
  return n.replace(/\s+/g, " ");
}

/**
 * Build a name→email lookup from NewCustomers for email-based matching.
 */
function buildNameToEmailMap(newCustomers: NewCustomer[]): Map<string, string> {
  const map = new Map<string, string>();
  for (const nc of newCustomers) {
    if (!nc.name || !nc.email) continue;
    const key = normalizeName(nc.name);
    if (!map.has(key)) {
      map.set(key, nc.email.trim().toLowerCase());
    }
  }
  return map;
}

/**
 * Resolve a person to their best matching key (email preferred, name fallback).
 */
function resolveKey(
  name: string,
  email: string | undefined,
  nameToEmail: Map<string, string>
): string {
  if (email && email.trim()) return email.trim().toLowerCase();
  const normalName = normalizeName(name);
  const resolved = nameToEmail.get(normalName);
  if (resolved) return resolved;
  return `name:${normalName}`;
}

/**
 * Build a lookup: customer key (email preferred) → earliest first-visit date.
 */
function buildFirstVisitMap(firstVisits: FirstVisit[], nameToEmail: Map<string, string>): Map<string, Date> {
  const map = new Map<string, Date>();
  for (const fv of firstVisits) {
    if (!fv.attendee) continue;
    const date = parseDate(fv.redeemedAt);
    if (!date) continue;
    const key = resolveKey(fv.attendee, undefined, nameToEmail);
    const existing = map.get(key);
    if (!existing || date < existing) {
      map.set(key, date);
    }
  }
  return map;
}

/**
 * Build a lookup: customer key (email preferred) → earliest auto-renew created date
 */
function buildAutoRenewMap(newAutoRenews: AutoRenew[], nameToEmail: Map<string, string>): Map<string, Date> {
  const map = new Map<string, Date>();
  for (const ar of newAutoRenews) {
    if (!ar.customer) continue;
    const date = parseDate(ar.created || "");
    if (!date) continue;
    const key = resolveKey(ar.customer, ar.email, nameToEmail);
    const existing = map.get(key);
    if (!existing || date < existing) {
      map.set(key, date);
    }
  }
  return map;
}

interface CustomerRecord {
  signUpDate: Date;
  firstVisitDate: Date | null;
  autoRenewDate: Date | null;
  /** Did they visit within the window? */
  visitedInWindow: boolean;
  /** Did they auto-renew within the window of their first visit? */
  renewedInWindow: boolean;
}

function buildCustomerRecords(
  newCustomers: NewCustomer[],
  firstVisitMap: Map<string, Date>,
  autoRenewMap: Map<string, Date>,
  nameToEmail: Map<string, string>
): CustomerRecord[] {
  const records: CustomerRecord[] = [];

  for (const nc of newCustomers) {
    if (!nc.name) continue;
    const signUpDate = parseDate(nc.created);
    if (!signUpDate) continue;

    const key = resolveKey(nc.name, nc.email, nameToEmail);
    const firstVisitDate = firstVisitMap.get(key) || null;
    const autoRenewDate = autoRenewMap.get(key) || null;

    // Did they have a first visit within the window of sign-up?
    // Allow visit up to 7 days BEFORE sign-up (same-day timing / pre-registration)
    // or up to FIRST_VISIT_WINDOW_DAYS after sign-up.
    let visitedInWindow = false;
    if (firstVisitDate) {
      const daysDiff = differenceInDays(firstVisitDate, signUpDate);
      visitedInWindow = daysDiff >= -7 && daysDiff <= FIRST_VISIT_WINDOW_DAYS;
    }

    // Did they auto-renew within the window of first visit (or sign-up if no visit)?
    let renewedInWindow = false;
    if (visitedInWindow && firstVisitDate && autoRenewDate) {
      const daysSinceVisit = differenceInDays(autoRenewDate, firstVisitDate);
      renewedInWindow = daysSinceVisit >= -7 && daysSinceVisit <= AUTO_RENEW_WINDOW_DAYS;
    }

    records.push({
      signUpDate,
      firstVisitDate,
      autoRenewDate,
      visitedInWindow,
      renewedInWindow,
    });
  }

  return records;
}

function computeRow(period: string, records: CustomerRecord[]): FunnelOverviewRow {
  const newCustomers = records.length;
  const firstVisits = records.filter((r) => r.visitedInWindow).length;
  const autoRenews = records.filter((r) => r.renewedInWindow).length;

  return {
    period,
    newCustomers,
    firstVisits,
    firstVisitConversion: newCustomers > 0 ? Math.round((firstVisits / newCustomers) * 1000) / 10 : 0,
    autoRenews,
    autoRenewConversion: firstVisits > 0 ? Math.round((autoRenews / firstVisits) * 1000) / 10 : 0,
  };
}

export function analyzeFunnelOverview(
  newCustomers: NewCustomer[],
  firstVisits: FirstVisit[],
  newAutoRenews: AutoRenew[]
): FunnelOverviewResults {
  const nameToEmail = buildNameToEmailMap(newCustomers);
  const firstVisitMap = buildFirstVisitMap(firstVisits, nameToEmail);
  const autoRenewMap = buildAutoRenewMap(newAutoRenews, nameToEmail);
  const records = buildCustomerRecords(newCustomers, firstVisitMap, autoRenewMap, nameToEmail);

  // Debug: show sample first visit records to diagnose matching issues
  if (firstVisits.length > 0) {
    console.log(`[funnel-overview] Sample first visits (first 3):`);
    for (const fv of firstVisits.slice(0, 3)) {
      console.log(`  attendee="${fv.attendee}" pass="${fv.pass}" redeemedAt="${fv.redeemedAt}" -> parsed: ${parseDate(fv.redeemedAt)}`);
    }
  }
  console.log(`[funnel-overview] ${newCustomers.length} new customers, ${firstVisitMap.size} unique first-visit names, ${autoRenewMap.size} unique auto-renew names`);
  console.log(`[funnel-overview] ${records.length} customer records built, ${records.filter(r => r.firstVisitDate).length} matched a first visit, ${records.filter(r => r.visitedInWindow).length} visited in window`);

  // All time
  const allTime = computeRow("All Time", records);

  // Monthly
  const monthlyBuckets = new Map<string, CustomerRecord[]>();
  for (const rec of records) {
    const mo = getMonthKey(rec.signUpDate);
    if (!monthlyBuckets.has(mo)) monthlyBuckets.set(mo, []);
    monthlyBuckets.get(mo)!.push(rec);
  }
  const monthly = Array.from(monthlyBuckets.entries())
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([period, recs]) => computeRow(period, recs));

  // Rolling windows
  const now = new Date();
  function rollingRecords(days: number): CustomerRecord[] {
    const cutoff = subDays(now, days);
    return records.filter((r) => r.signUpDate >= cutoff);
  }

  return {
    allTime,
    monthly,
    rolling30: computeRow("Rolling 30d", rollingRecords(30)),
    rolling60: computeRow("Rolling 60d", rollingRecords(60)),
    rolling90: computeRow("Rolling 90d", rollingRecords(90)),
  };
}
