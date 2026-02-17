import { getDatabase } from "./database";
import { isDropInOrIntro } from "../analytics/categories";

// ── Types ────────────────────────────────────────────────────

export interface RegistrationRow {
  eventName: string;
  performanceStartsAt: string;
  locationName: string;
  videoName?: string;
  teacherName: string;
  firstName: string;
  lastName: string;
  email: string;
  registeredAt?: string;
  attendedAt: string;
  registrationType: string;
  state: string;
  pass: string;
  subscription: string; // "true" or "false"
  revenue: number;
}

export interface WeeklyCount {
  week: string; // ISO week key, e.g. "2025-W03"
  count: number;
}

export interface WeeklySegmentedCount {
  week: string;
  count: number;
  segments: {
    introWeek: number;
    dropIn: number;
    guest: number;
    other: number;
  };
}

export interface AttendanceStats {
  /** Current month total registrations */
  currentMonthTotal: number;
  /** Days elapsed in current month */
  currentMonthDaysElapsed: number;
  /** Total days in current month */
  currentMonthDaysInMonth: number;
  /** Projected pace for full month */
  currentMonthPaced: number;
  /** Previous month total */
  previousMonthTotal: number;
  /** 6-week weekly average */
  weeklyAvg6w: number;
}

// ── Write Operations ─────────────────────────────────────────

/**
 * Save registrations (additive — appends new rows, never deletes existing).
 */
export function saveRegistrations(rows: RegistrationRow[]): void {
  const db = getDatabase();
  const before = (db.prepare("SELECT COUNT(*) as count FROM registrations").get() as { count: number }).count;

  const insert = db.prepare(`
    INSERT OR IGNORE INTO registrations (
      event_name, performance_starts_at, location_name, video_name, teacher_name,
      first_name, last_name, email, registered_at, attended_at,
      registration_type, state, pass, subscription, revenue
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);

  const insertMany = db.transaction((items: RegistrationRow[]) => {
    for (const r of items) {
      insert.run(
        r.eventName, r.performanceStartsAt, r.locationName, r.videoName || null,
        r.teacherName, r.firstName, r.lastName, r.email,
        r.registeredAt || null, r.attendedAt,
        r.registrationType, r.state, r.pass, r.subscription,
        r.revenue
      );
    }
  });

  insertMany(rows);
  const after = (db.prepare("SELECT COUNT(*) as count FROM registrations").get() as { count: number }).count;
  console.log(`[registration-store] Registrations: ${before} -> ${after} (+${after - before} new)`);
}

/**
 * Save first visits (additive — appends new rows, never deletes existing).
 */
export function saveFirstVisits(rows: RegistrationRow[]): void {
  const db = getDatabase();
  const before = (db.prepare("SELECT COUNT(*) as count FROM first_visits").get() as { count: number }).count;

  const insert = db.prepare(`
    INSERT OR IGNORE INTO first_visits (
      event_name, performance_starts_at, location_name, video_name, teacher_name,
      first_name, last_name, email, registered_at, attended_at,
      registration_type, state, pass, subscription, revenue
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);

  const insertMany = db.transaction((items: RegistrationRow[]) => {
    for (const r of items) {
      insert.run(
        r.eventName, r.performanceStartsAt, r.locationName, r.videoName || null,
        r.teacherName, r.firstName, r.lastName, r.email,
        r.registeredAt || null, r.attendedAt,
        r.registrationType, r.state, r.pass, r.subscription,
        r.revenue
      );
    }
  });

  insertMany(rows);
  const after = (db.prepare("SELECT COUNT(*) as count FROM first_visits").get() as { count: number }).count;
  console.log(`[registration-store] First visits: ${before} -> ${after} (+${after - before} new)`);
}

// ── Read Operations ──────────────────────────────────────────

/**
 * Get drop-in (non-subscriber) visits grouped by ISO week.
 */
export function getDropInsByWeek(startDate?: string, endDate?: string): WeeklyCount[] {
  const db = getDatabase();
  let query = `
    SELECT strftime('%Y-W%W', attended_at) as week, COUNT(*) as count
    FROM registrations
    WHERE attended_at IS NOT NULL AND attended_at != ''
      AND (subscription = 'false' OR subscription IS NULL)
  `;
  const params: string[] = [];

  if (startDate) {
    query += ` AND attended_at >= ?`;
    params.push(startDate);
  }
  if (endDate) {
    query += ` AND attended_at < ?`;
    params.push(endDate);
  }

  query += ` GROUP BY week ORDER BY week`;

  return db.prepare(query).all(...params) as WeeklyCount[];
}

/**
 * Get first visits grouped by ISO week with segment breakdown.
 * Segments: introWeek (Intro passes), dropIn (drop-in/5-pack), guest (guest passes), other
 */
export function getFirstVisitsByWeek(startDate?: string, endDate?: string): WeeklySegmentedCount[] {
  const db = getDatabase();
  let query = `
    SELECT strftime('%Y-W%W', attended_at) as week, pass, COUNT(*) as count
    FROM first_visits
    WHERE attended_at IS NOT NULL AND attended_at != ''
  `;
  const params: string[] = [];

  if (startDate) {
    query += ` AND attended_at >= ?`;
    params.push(startDate);
  }
  if (endDate) {
    query += ` AND attended_at < ?`;
    params.push(endDate);
  }

  query += ` GROUP BY week, pass ORDER BY week`;

  const rawRows = db.prepare(query).all(...params) as { week: string; pass: string; count: number }[];

  // Aggregate by week with segment classification
  const weekMap = new Map<string, WeeklySegmentedCount>();

  for (const row of rawRows) {
    if (!weekMap.has(row.week)) {
      weekMap.set(row.week, {
        week: row.week,
        count: 0,
        segments: { introWeek: 0, dropIn: 0, guest: 0, other: 0 },
      });
    }
    const entry = weekMap.get(row.week)!;
    entry.count += row.count;

    const passUpper = (row.pass || "").toUpperCase();
    if (passUpper.includes("INTRO")) {
      entry.segments.introWeek += row.count;
    } else if (passUpper.includes("GUEST") || passUpper.includes("COMMUNITY")) {
      entry.segments.guest += row.count;
    } else if (isDropInOrIntro(row.pass || "")) {
      entry.segments.dropIn += row.count;
    } else {
      entry.segments.other += row.count;
    }
  }

  return Array.from(weekMap.values());
}

/**
 * Get total registrations grouped by ISO week.
 */
export function getRegistrationsByWeek(startDate?: string, endDate?: string): WeeklyCount[] {
  const db = getDatabase();
  let query = `
    SELECT strftime('%Y-W%W', attended_at) as week, COUNT(*) as count
    FROM registrations
    WHERE attended_at IS NOT NULL AND attended_at != ''
  `;
  const params: string[] = [];

  if (startDate) {
    query += ` AND attended_at >= ?`;
    params.push(startDate);
  }
  if (endDate) {
    query += ` AND attended_at < ?`;
    params.push(endDate);
  }

  query += ` GROUP BY week ORDER BY week`;

  return db.prepare(query).all(...params) as WeeklyCount[];
}

/**
 * Get drop-in attendance stats (MTD, previous month, weekly average).
 */
export function getDropInStats(): AttendanceStats | null {
  const db = getDatabase();
  const now = new Date();
  const currentMonthStart = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, "0")}-01`;
  const prevMonthDate = new Date(now.getFullYear(), now.getMonth() - 1, 1);
  const prevMonthStart = `${prevMonthDate.getFullYear()}-${String(prevMonthDate.getMonth() + 1).padStart(2, "0")}-01`;

  // Current month total
  const currentRow = db.prepare(`
    SELECT COUNT(*) as count FROM registrations
    WHERE attended_at >= ? AND (subscription = 'false' OR subscription IS NULL)
      AND attended_at IS NOT NULL AND attended_at != ''
  `).get(currentMonthStart) as { count: number };

  if (currentRow.count === 0) {
    // Check if we have any registration data at all
    const anyData = db.prepare(`SELECT COUNT(*) as count FROM registrations`).get() as { count: number };
    if (anyData.count === 0) return null;
  }

  // Previous month total
  const prevRow = db.prepare(`
    SELECT COUNT(*) as count FROM registrations
    WHERE attended_at >= ? AND attended_at < ?
      AND (subscription = 'false' OR subscription IS NULL)
      AND attended_at IS NOT NULL AND attended_at != ''
  `).get(prevMonthStart, currentMonthStart) as { count: number };

  // Days in month calculation
  const daysElapsed = now.getDate();
  const daysInMonth = new Date(now.getFullYear(), now.getMonth() + 1, 0).getDate();

  // 6-week weekly average
  const sixWeeksAgo = new Date(now);
  sixWeeksAgo.setDate(sixWeeksAgo.getDate() - 42);
  const sixWeeksAgoStr = sixWeeksAgo.toISOString().split("T")[0];

  const weeklyRows = db.prepare(`
    SELECT strftime('%Y-W%W', attended_at) as week, COUNT(*) as count
    FROM registrations
    WHERE attended_at >= ? AND (subscription = 'false' OR subscription IS NULL)
      AND attended_at IS NOT NULL AND attended_at != ''
    GROUP BY week
    ORDER BY week
  `).all(sixWeeksAgoStr) as WeeklyCount[];

  const weeklyAvg = weeklyRows.length > 0
    ? Math.round(weeklyRows.reduce((sum, w) => sum + w.count, 0) / weeklyRows.length)
    : 0;

  return {
    currentMonthTotal: currentRow.count,
    currentMonthDaysElapsed: daysElapsed,
    currentMonthDaysInMonth: daysInMonth,
    currentMonthPaced: daysElapsed > 0
      ? Math.round((currentRow.count / daysElapsed) * daysInMonth)
      : 0,
    previousMonthTotal: prevRow.count,
    weeklyAvg6w: weeklyAvg,
  };
}

/**
 * Check if registration data exists.
 */
export function hasRegistrationData(): boolean {
  const db = getDatabase();
  const row = db.prepare(`SELECT COUNT(*) as count FROM registrations`).get() as { count: number };
  return row.count > 0;
}

/**
 * Check if first visit data exists.
 */
export function hasFirstVisitData(): boolean {
  const db = getDatabase();
  const row = db.prepare(`SELECT COUNT(*) as count FROM first_visits`).get() as { count: number };
  return row.count > 0;
}

// ── Unique Visitor Queries ──────────────────────────────────

export interface UniqueVisitorWeek {
  weekStart: string; // Monday date, e.g. "2025-01-13"
  uniqueVisitors: number;
}

export interface SourceBreakdown {
  introWeek: number;
  dropIn: number;
  guest: number;
  other: number;
  otherBreakdownTop5: { passName: string; count: number }[];
}

/**
 * Classify pass/count rows into source segments.
 * When includeIntroWeek is false, intro passes are counted as "other".
 */
function classifySourceRows(
  rows: { pass: string; cnt: number }[],
  includeIntroWeek: boolean
): SourceBreakdown {
  const result: SourceBreakdown = {
    introWeek: 0,
    dropIn: 0,
    guest: 0,
    other: 0,
    otherBreakdownTop5: [],
  };
  const otherPasses = new Map<string, number>();

  for (const row of rows) {
    const passUpper = (row.pass || "").toUpperCase();
    if (includeIntroWeek && passUpper.includes("INTRO")) {
      result.introWeek += row.cnt;
    } else if (passUpper.includes("GUEST") || passUpper.includes("COMMUNITY")) {
      result.guest += row.cnt;
    } else if (isDropInOrIntro(row.pass || "")) {
      result.dropIn += row.cnt;
    } else {
      result.other += row.cnt;
      const key = row.pass || "(empty)";
      otherPasses.set(key, (otherPasses.get(key) || 0) + row.cnt);
    }
  }

  result.otherBreakdownTop5 = Array.from(otherPasses.entries())
    .sort((a, b) => b[1] - a[1])
    .slice(0, 5)
    .map(([passName, count]) => ({ passName, count }));

  return result;
}

/**
 * Get unique first-time visitors per week (Monday-based weeks).
 * Returns COUNT(DISTINCT email) grouped by week start date.
 */
export function getFirstTimeUniqueVisitorsByWeek(
  startDate?: string,
  endDate?: string
): UniqueVisitorWeek[] {
  const db = getDatabase();
  const params: string[] = [];
  let dateFilter = "";

  if (startDate) {
    dateFilter += ` AND attended_at >= ?`;
    params.push(startDate);
  }
  if (endDate) {
    dateFilter += ` AND attended_at < ?`;
    params.push(endDate);
  }

  const query = `
    SELECT date(substr(attended_at, 1, 19), 'weekday 0', '-6 days') as weekStart,
           COUNT(DISTINCT email) as uniqueVisitors
    FROM first_visits
    WHERE attended_at IS NOT NULL AND attended_at != ''
      AND email IS NOT NULL AND email != ''
      ${dateFilter}
    GROUP BY weekStart
    ORDER BY weekStart
  `;

  return db.prepare(query).all(...params) as UniqueVisitorWeek[];
}

/**
 * Get source breakdown for first-time visitors (unique people).
 * Each person is attributed to one source based on their FIRST visit's pass type.
 */
export function getFirstTimeSourceBreakdown(
  startDate?: string,
  endDate?: string
): SourceBreakdown {
  const db = getDatabase();
  const params: string[] = [];
  let dateFilter = "";

  if (startDate) {
    dateFilter += ` AND attended_at >= ?`;
    params.push(startDate);
  }
  if (endDate) {
    dateFilter += ` AND attended_at < ?`;
    params.push(endDate);
  }

  // For each email, find their earliest visit in the window, get that visit's pass
  const query = `
    WITH first_per_email AS (
      SELECT email, MIN(attended_at) as first_at
      FROM first_visits
      WHERE attended_at IS NOT NULL AND attended_at != ''
        AND email IS NOT NULL AND email != ''
        ${dateFilter}
      GROUP BY email
    )
    SELECT fv.pass, COUNT(*) as cnt
    FROM first_per_email fpe
    JOIN first_visits fv
      ON fv.email = fpe.email
      AND fv.attended_at = fpe.first_at
    GROUP BY fv.pass
  `;

  const rows = db.prepare(query).all(...params) as { pass: string; cnt: number }[];
  return classifySourceRows(rows, true);
}

/**
 * Get unique returning non-member visitors per week (Monday-based weeks).
 * Returning = non-subscriber emails that are NOT in the first_visits table for the same window.
 */
export function getReturningUniqueVisitorsByWeek(
  startDate?: string,
  endDate?: string
): UniqueVisitorWeek[] {
  const db = getDatabase();
  const params: string[] = [];
  let fvDateFilter = "";
  let regDateFilter = "";

  if (startDate) {
    fvDateFilter += ` AND attended_at >= ?`;
    params.push(startDate);
    regDateFilter += ` AND r.attended_at >= ?`;
    params.push(startDate);
  }
  if (endDate) {
    regDateFilter += ` AND r.attended_at < ?`;
    params.push(endDate);
  }

  const query = `
    SELECT date(substr(r.attended_at, 1, 19), 'weekday 0', '-6 days') as weekStart,
           COUNT(DISTINCT r.email) as uniqueVisitors
    FROM registrations r
    WHERE r.attended_at IS NOT NULL AND r.attended_at != ''
      AND r.email IS NOT NULL AND r.email != ''
      AND (r.subscription = 'false' OR r.subscription IS NULL)
      AND r.email NOT IN (
        SELECT DISTINCT email FROM first_visits
        WHERE attended_at IS NOT NULL AND attended_at != ''
          AND email IS NOT NULL AND email != ''
          ${fvDateFilter}
      )
      ${regDateFilter}
    GROUP BY weekStart
    ORDER BY weekStart
  `;

  return db.prepare(query).all(...params) as UniqueVisitorWeek[];
}

/**
 * Get source breakdown for returning non-members (unique people).
 * Each person is attributed to one source based on their MOST RECENT visit's pass type.
 */
export function getReturningSourceBreakdown(
  startDate?: string,
  endDate?: string
): SourceBreakdown {
  const db = getDatabase();
  const params: string[] = [];
  let fvDateFilter = "";
  let regDateFilter = "";

  if (startDate) {
    fvDateFilter += ` AND attended_at >= ?`;
    params.push(startDate);
    regDateFilter += ` AND r.attended_at >= ?`;
    params.push(startDate);
  }
  if (endDate) {
    regDateFilter += ` AND r.attended_at < ?`;
    params.push(endDate);
  }

  const query = `
    WITH qualifying AS (
      SELECT r.email, r.attended_at, r.pass
      FROM registrations r
      WHERE r.attended_at IS NOT NULL AND r.attended_at != ''
        AND r.email IS NOT NULL AND r.email != ''
        AND (r.subscription = 'false' OR r.subscription IS NULL)
        AND r.email NOT IN (
          SELECT DISTINCT email FROM first_visits
          WHERE attended_at IS NOT NULL AND attended_at != ''
            AND email IS NOT NULL AND email != ''
            ${fvDateFilter}
        )
        ${regDateFilter}
    ),
    most_recent AS (
      SELECT email, MAX(attended_at) as last_at
      FROM qualifying
      GROUP BY email
    )
    SELECT q.pass, COUNT(*) as cnt
    FROM most_recent mr
    JOIN qualifying q ON q.email = mr.email AND q.attended_at = mr.last_at
    GROUP BY q.pass
  `;

  const rows = db.prepare(query).all(...params) as { pass: string; cnt: number }[];
  return classifySourceRows(rows, false);
}
