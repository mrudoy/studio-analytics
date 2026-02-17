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
 * Save registrations (full roster). Replaces all existing data.
 */
export function saveRegistrations(rows: RegistrationRow[]): void {
  const db = getDatabase();
  const insert = db.prepare(`
    INSERT INTO registrations (
      event_name, performance_starts_at, location_name, video_name, teacher_name,
      first_name, last_name, email, registered_at, attended_at,
      registration_type, state, pass, subscription, revenue
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);

  const insertMany = db.transaction((items: RegistrationRow[]) => {
    db.exec("DELETE FROM registrations");
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
  console.log(`[registration-store] Saved ${rows.length} registrations`);
}

/**
 * Save first visits. Replaces all existing data.
 */
export function saveFirstVisits(rows: RegistrationRow[]): void {
  const db = getDatabase();
  const insert = db.prepare(`
    INSERT INTO first_visits (
      event_name, performance_starts_at, location_name, video_name, teacher_name,
      first_name, last_name, email, registered_at, attended_at,
      registration_type, state, pass, subscription, revenue
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);

  const insertMany = db.transaction((items: RegistrationRow[]) => {
    db.exec("DELETE FROM first_visits");
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
  console.log(`[registration-store] Saved ${rows.length} first visits`);
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
