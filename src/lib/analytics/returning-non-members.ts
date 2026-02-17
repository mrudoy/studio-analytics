import type { FullRegistration, FirstVisit } from "@/types/union-data";
import type { FirstVisitSegment } from "./trends";
import { parseDate, getWeekKey } from "./date-utils";

// ─── Types ───────────────────────────────────────────────────

export interface ReturningNonMemberWeek {
  week: string;
  count: number; // unique emails that week
  segments: Record<FirstVisitSegment, number>;
}

export interface ReturningNonMemberStats {
  currentWeekTotal: number;
  currentWeekSegments: Record<FirstVisitSegment, number>;
  completedWeeks: ReturningNonMemberWeek[]; // last 4 completed weeks
  aggregateSegments: Record<FirstVisitSegment, number>; // unique people by most-recent source across full window
}

// ─── Helpers ─────────────────────────────────────────────────

const emptySegments = (): Record<FirstVisitSegment, number> =>
  ({ introWeek: 0, dropIn: 0, guest: 0, other: 0 });

/**
 * Normalize a name to "first last" lowercase for cross-report matching.
 * First Visits use "Last, First" format; full registrations have separate first/last.
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
 * Classify a pass name into a segment for returning non-members.
 * Intro Week is routed to "other" (it's a one-time pass, shouldn't appear for returners).
 */
function classifyPass(pass: string): FirstVisitSegment {
  const upper = pass.trim().toUpperCase();
  // Intro → route to "other" for returning non-members (defensive — shouldn't appear often)
  if (upper.includes("INTRO")) return "other";
  if (upper.includes("GUEST")) return "guest";
  if (
    upper.includes("DROP-IN") || upper.includes("DROP IN") || upper.includes("DROPIN") ||
    upper.includes("SINGLE CLASS") || upper.includes("WELLHUB") ||
    upper.includes("5 PACK") || upper.includes("5-PACK") ||
    upper.includes("COMMUNITY DAY") || upper.includes("POKER CHIP") ||
    upper.includes("ON RUNNING")
  ) return "dropIn";
  return "other";
}

// ─── Main Analysis ───────────────────────────────────────────

/**
 * Analyze returning non-members: people who visit the studio without an auto-renewing
 * subscription and who are NOT first-time visitors.
 *
 * Logic:
 * 1. Build name→email map from full registrations (firstName + lastName → email)
 * 2. Build first-visit email exclusion set by matching first visit attendee names to emails
 * 3. Filter: subscription === false AND state === "redeemed" AND has email
 * 4. Exclude emails in the first-visit set
 * 5. Group by (email, week) → count unique emails per week
 * 6. Classify passes for segment breakdown
 */
export function analyzeReturningNonMembers(
  fullRegistrations: FullRegistration[],
  firstVisits: FirstVisit[]
): ReturningNonMemberStats | null {
  // Check if we have the rich data (email field populated)
  const regsWithEmail = fullRegistrations.filter((r) => r.email);
  if (regsWithEmail.length === 0) {
    console.log("[returning-non-members] No emails in full registrations data — skipping analysis");
    return null;
  }

  // Step 1: Build name→email map from full registrations
  const nameToEmail = new Map<string, string>();
  for (const reg of fullRegistrations) {
    if (!reg.email || !reg.firstName) continue;
    const key = `${reg.firstName} ${reg.lastName}`.trim().toLowerCase().replace(/\s+/g, " ");
    if (key && !nameToEmail.has(key)) {
      nameToEmail.set(key, reg.email.toLowerCase());
    }
  }

  // Step 2: Build first-visit email exclusion set
  const firstVisitEmails = new Set<string>();
  for (const fv of firstVisits) {
    if (!fv.attendee) continue;
    const name = normalizeName(fv.attendee);
    const email = nameToEmail.get(name);
    if (email) firstVisitEmails.add(email);
  }

  console.log(
    `[returning-non-members] Name→email map: ${nameToEmail.size} entries, ` +
    `first-visit exclusion set: ${firstVisitEmails.size} emails`
  );

  // Step 3: Filter non-subscribers with state=redeemed and email
  const nonSubRedeemed = fullRegistrations.filter(
    (r) => !r.subscription && r.state.toLowerCase() === "redeemed" && r.email
  );

  // Step 4: Exclude first-visit emails
  const returners = nonSubRedeemed.filter(
    (r) => !firstVisitEmails.has(r.email.toLowerCase())
  );

  console.log(
    `[returning-non-members] Non-subscriber redeemed: ${nonSubRedeemed.length}, ` +
    `after excluding first visits: ${returners.length}`
  );

  // Step 5: Group by (email, week) → count unique emails per week
  const weekEmailSets = new Map<string, Set<string>>();
  const weekSegments = new Map<string, Record<FirstVisitSegment, number>>();
  // Track raw pass names that map to "other" for debugging
  const otherPassCounts = new Map<string, number>();
  // Track most-recent visit per email for aggregate source breakdown
  const emailMostRecent = new Map<string, { date: Date; pass: string }>();

  for (const reg of returners) {
    const date = parseDate(reg.attendedAt || "");
    if (!date) continue;
    const wk = getWeekKey(date);
    const email = reg.email.toLowerCase();
    const seg = classifyPass(reg.pass);

    // Track "other" reasons for debug
    if (seg === "other") {
      const passKey = reg.pass.trim() || "(empty)";
      otherPassCounts.set(passKey, (otherPassCounts.get(passKey) || 0) + 1);
    }

    // Track most-recent visit per email (for aggregate)
    const prev = emailMostRecent.get(email);
    if (!prev || date > prev.date) {
      emailMostRecent.set(email, { date, pass: reg.pass });
    }

    // Per-week dedup
    if (!weekEmailSets.has(wk)) weekEmailSets.set(wk, new Set());
    if (!weekSegments.has(wk)) weekSegments.set(wk, emptySegments());

    const emailSet = weekEmailSets.get(wk)!;
    if (!emailSet.has(email)) {
      emailSet.add(email);
      weekSegments.get(wk)![seg]++;
    }
  }

  // Step 6: Build output
  const now = new Date();
  const currentWeekKey = getWeekKey(now);
  const allWeeks = Array.from(weekEmailSets.keys()).sort();
  const completedWeeks = allWeeks.filter((w) => w < currentWeekKey);
  const last4 = completedWeeks.slice(-4);

  // Aggregate: unique people in the display window, attributed by most-recent source
  const displayWeeks = new Set([...last4, currentWeekKey]);
  const aggregateSegments = emptySegments();
  const aggregateCounted = new Set<string>();
  for (const wk of displayWeeks) {
    const emails = weekEmailSets.get(wk);
    if (!emails) continue;
    for (const email of emails) {
      if (!aggregateCounted.has(email)) {
        aggregateCounted.add(email);
        const recent = emailMostRecent.get(email);
        const seg = recent ? classifyPass(recent.pass) : "other";
        aggregateSegments[seg]++;
      }
    }
  }

  const result: ReturningNonMemberStats = {
    currentWeekTotal: weekEmailSets.get(currentWeekKey)?.size ?? 0,
    currentWeekSegments: weekSegments.get(currentWeekKey) ?? emptySegments(),
    completedWeeks: last4.map((wk) => ({
      week: wk,
      count: weekEmailSets.get(wk)?.size ?? 0,
      segments: weekSegments.get(wk) ?? emptySegments(),
    })),
    aggregateSegments,
  };

  // Debug: log top 5 pass types that rolled into "Other"
  if (otherPassCounts.size > 0) {
    const top5 = Array.from(otherPassCounts.entries())
      .sort((a, b) => b[1] - a[1])
      .slice(0, 5)
      .map(([pass, count]) => `${pass} (${count})`)
      .join(", ");
    console.log(`[returning-non-members] Other reasons: ${top5}`);
  }

  console.log(
    `[returning-non-members] This week: ${result.currentWeekTotal}, ` +
    `last 4 weeks: [${result.completedWeeks.map((w) => w.count).join(", ")}], ` +
    `aggregate: dropIn=${aggregateSegments.dropIn} guest=${aggregateSegments.guest} other=${aggregateSegments.other}`
  );

  return result;
}
