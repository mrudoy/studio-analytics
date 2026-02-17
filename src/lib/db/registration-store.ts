import { getPool } from "./database";
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
export async function saveRegistrations(rows: RegistrationRow[]): Promise<void> {
  const pool = getPool();
  const beforeResult = await pool.query("SELECT COUNT(*) as count FROM registrations");
  const before = Number(beforeResult.rows[0].count);

  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    for (const r of rows) {
      await client.query(
        `INSERT INTO registrations (
          event_name, performance_starts_at, location_name, video_name, teacher_name,
          first_name, last_name, email, registered_at, attended_at,
          registration_type, state, pass, subscription, revenue
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
        ON CONFLICT (email, attended_at) DO NOTHING`,
        [
          r.eventName, r.performanceStartsAt, r.locationName, r.videoName || null,
          r.teacherName, r.firstName, r.lastName, r.email,
          r.registeredAt || null, r.attendedAt,
          r.registrationType, r.state, r.pass, r.subscription,
          r.revenue,
        ]
      );
    }
    await client.query("COMMIT");
  } catch (e) {
    await client.query("ROLLBACK");
    throw e;
  } finally {
    client.release();
  }

  const afterResult = await pool.query("SELECT COUNT(*) as count FROM registrations");
  const after = Number(afterResult.rows[0].count);
  console.log(`[registration-store] Registrations: ${before} -> ${after} (+${after - before} new)`);
}

/**
 * Save first visits (additive — appends new rows, never deletes existing).
 */
export async function saveFirstVisits(rows: RegistrationRow[]): Promise<void> {
  const pool = getPool();
  const beforeResult = await pool.query("SELECT COUNT(*) as count FROM first_visits");
  const before = Number(beforeResult.rows[0].count);

  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    for (const r of rows) {
      await client.query(
        `INSERT INTO first_visits (
          event_name, performance_starts_at, location_name, video_name, teacher_name,
          first_name, last_name, email, registered_at, attended_at,
          registration_type, state, pass, subscription, revenue
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
        ON CONFLICT (email, attended_at) DO NOTHING`,
        [
          r.eventName, r.performanceStartsAt, r.locationName, r.videoName || null,
          r.teacherName, r.firstName, r.lastName, r.email,
          r.registeredAt || null, r.attendedAt,
          r.registrationType, r.state, r.pass, r.subscription,
          r.revenue,
        ]
      );
    }
    await client.query("COMMIT");
  } catch (e) {
    await client.query("ROLLBACK");
    throw e;
  } finally {
    client.release();
  }

  const afterResult = await pool.query("SELECT COUNT(*) as count FROM first_visits");
  const after = Number(afterResult.rows[0].count);
  console.log(`[registration-store] First visits: ${before} -> ${after} (+${after - before} new)`);
}

// ── Read Operations ──────────────────────────────────────────

/**
 * Get drop-in (non-subscriber) visits grouped by ISO week.
 */
export async function getDropInsByWeek(startDate?: string, endDate?: string): Promise<WeeklyCount[]> {
  const pool = getPool();
  let query = `
    SELECT TO_CHAR(attended_at::date, 'IYYY-"W"IW') as week, COUNT(*) as count
    FROM registrations
    WHERE attended_at IS NOT NULL AND attended_at != ''
      AND (subscription = 'false' OR subscription IS NULL)
  `;
  const params: string[] = [];
  let paramIdx = 1;

  if (startDate) {
    query += ` AND attended_at >= $${paramIdx++}`;
    params.push(startDate);
  }
  if (endDate) {
    query += ` AND attended_at < $${paramIdx++}`;
    params.push(endDate);
  }

  query += ` GROUP BY week ORDER BY week`;

  const { rows } = await pool.query(query, params);
  return rows.map((r: Record<string, unknown>) => ({
    week: r.week as string,
    count: Number(r.count),
  }));
}

/**
 * Get first visits grouped by ISO week with segment breakdown.
 * Segments: introWeek (Intro passes), dropIn (drop-in/5-pack), guest (guest passes), other
 */
export async function getFirstVisitsByWeek(startDate?: string, endDate?: string): Promise<WeeklySegmentedCount[]> {
  const pool = getPool();
  let query = `
    SELECT TO_CHAR(attended_at::date, 'IYYY-"W"IW') as week, pass, COUNT(*) as count
    FROM first_visits
    WHERE attended_at IS NOT NULL AND attended_at != ''
  `;
  const params: string[] = [];
  let paramIdx = 1;

  if (startDate) {
    query += ` AND attended_at >= $${paramIdx++}`;
    params.push(startDate);
  }
  if (endDate) {
    query += ` AND attended_at < $${paramIdx++}`;
    params.push(endDate);
  }

  query += ` GROUP BY week, pass ORDER BY week`;

  const { rows: rawRows } = await pool.query(query, params);

  // Aggregate by week with segment classification
  const weekMap = new Map<string, WeeklySegmentedCount>();

  for (const row of rawRows as { week: string; pass: string; count: string | number }[]) {
    if (!weekMap.has(row.week)) {
      weekMap.set(row.week, {
        week: row.week,
        count: 0,
        segments: { introWeek: 0, dropIn: 0, guest: 0, other: 0 },
      });
    }
    const entry = weekMap.get(row.week)!;
    const cnt = Number(row.count);
    entry.count += cnt;

    const passUpper = (row.pass || "").toUpperCase();
    if (passUpper.includes("INTRO")) {
      entry.segments.introWeek += cnt;
    } else if (passUpper.includes("GUEST") || passUpper.includes("COMMUNITY")) {
      entry.segments.guest += cnt;
    } else if (isDropInOrIntro(row.pass || "")) {
      entry.segments.dropIn += cnt;
    } else {
      entry.segments.other += cnt;
    }
  }

  return Array.from(weekMap.values());
}

/**
 * Get total registrations grouped by ISO week.
 */
export async function getRegistrationsByWeek(startDate?: string, endDate?: string): Promise<WeeklyCount[]> {
  const pool = getPool();
  let query = `
    SELECT TO_CHAR(attended_at::date, 'IYYY-"W"IW') as week, COUNT(*) as count
    FROM registrations
    WHERE attended_at IS NOT NULL AND attended_at != ''
  `;
  const params: string[] = [];
  let paramIdx = 1;

  if (startDate) {
    query += ` AND attended_at >= $${paramIdx++}`;
    params.push(startDate);
  }
  if (endDate) {
    query += ` AND attended_at < $${paramIdx++}`;
    params.push(endDate);
  }

  query += ` GROUP BY week ORDER BY week`;

  const { rows } = await pool.query(query, params);
  return rows.map((r: Record<string, unknown>) => ({
    week: r.week as string,
    count: Number(r.count),
  }));
}

/**
 * Get drop-in attendance stats (MTD, previous month, weekly average).
 */
export async function getDropInStats(): Promise<AttendanceStats | null> {
  const pool = getPool();
  const now = new Date();
  const currentMonthStart = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, "0")}-01`;
  const prevMonthDate = new Date(now.getFullYear(), now.getMonth() - 1, 1);
  const prevMonthStart = `${prevMonthDate.getFullYear()}-${String(prevMonthDate.getMonth() + 1).padStart(2, "0")}-01`;

  // Current month total
  const currentResult = await pool.query(
    `SELECT COUNT(*) as count FROM registrations
     WHERE attended_at >= $1 AND (subscription = 'false' OR subscription IS NULL)
       AND attended_at IS NOT NULL AND attended_at != ''`,
    [currentMonthStart]
  );
  const currentCount = Number(currentResult.rows[0].count);

  if (currentCount === 0) {
    // Check if we have any registration data at all
    const anyResult = await pool.query(`SELECT COUNT(*) as count FROM registrations`);
    if (Number(anyResult.rows[0].count) === 0) return null;
  }

  // Previous month total
  const prevResult = await pool.query(
    `SELECT COUNT(*) as count FROM registrations
     WHERE attended_at >= $1 AND attended_at < $2
       AND (subscription = 'false' OR subscription IS NULL)
       AND attended_at IS NOT NULL AND attended_at != ''`,
    [prevMonthStart, currentMonthStart]
  );
  const prevCount = Number(prevResult.rows[0].count);

  // Days in month calculation
  const daysElapsed = now.getDate();
  const daysInMonth = new Date(now.getFullYear(), now.getMonth() + 1, 0).getDate();

  // 6-week weekly average
  const sixWeeksAgo = new Date(now);
  sixWeeksAgo.setDate(sixWeeksAgo.getDate() - 42);
  const sixWeeksAgoStr = sixWeeksAgo.toISOString().split("T")[0];

  const weeklyResult = await pool.query(
    `SELECT TO_CHAR(attended_at::date, 'IYYY-"W"IW') as week, COUNT(*) as count
     FROM registrations
     WHERE attended_at >= $1 AND (subscription = 'false' OR subscription IS NULL)
       AND attended_at IS NOT NULL AND attended_at != ''
     GROUP BY week
     ORDER BY week`,
    [sixWeeksAgoStr]
  );
  const weeklyRows = weeklyResult.rows as { week: string; count: string | number }[];

  const weeklyAvg = weeklyRows.length > 0
    ? Math.round(weeklyRows.reduce((sum, w) => sum + Number(w.count), 0) / weeklyRows.length)
    : 0;

  return {
    currentMonthTotal: currentCount,
    currentMonthDaysElapsed: daysElapsed,
    currentMonthDaysInMonth: daysInMonth,
    currentMonthPaced: daysElapsed > 0
      ? Math.round((currentCount / daysElapsed) * daysInMonth)
      : 0,
    previousMonthTotal: prevCount,
    weeklyAvg6w: weeklyAvg,
  };
}

/**
 * Check if registration data exists.
 */
export async function hasRegistrationData(): Promise<boolean> {
  const pool = getPool();
  const { rows } = await pool.query(`SELECT COUNT(*) as count FROM registrations`);
  return Number(rows[0].count) > 0;
}

/**
 * Check if first visit data exists.
 */
export async function hasFirstVisitData(): Promise<boolean> {
  const pool = getPool();
  const { rows } = await pool.query(`SELECT COUNT(*) as count FROM first_visits`);
  return Number(rows[0].count) > 0;
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
export async function getFirstTimeUniqueVisitorsByWeek(
  startDate?: string,
  endDate?: string
): Promise<UniqueVisitorWeek[]> {
  const pool = getPool();
  const params: string[] = [];
  let paramIdx = 1;
  let dateFilter = "";

  if (startDate) {
    dateFilter += ` AND attended_at >= $${paramIdx++}`;
    params.push(startDate);
  }
  if (endDate) {
    dateFilter += ` AND attended_at < $${paramIdx++}`;
    params.push(endDate);
  }

  const query = `
    SELECT DATE_TRUNC('week', attended_at::date)::date::text as "weekStart",
           COUNT(DISTINCT email) as "uniqueVisitors"
    FROM first_visits
    WHERE attended_at IS NOT NULL AND attended_at != ''
      AND email IS NOT NULL AND email != ''
      ${dateFilter}
    GROUP BY "weekStart"
    ORDER BY "weekStart"
  `;

  const { rows } = await pool.query(query, params);
  return rows.map((r: Record<string, unknown>) => ({
    weekStart: r.weekStart as string,
    uniqueVisitors: Number(r.uniqueVisitors),
  }));
}

/**
 * Get source breakdown for first-time visitors (unique people).
 * Each person is attributed to one source based on their FIRST visit's pass type.
 */
export async function getFirstTimeSourceBreakdown(
  startDate?: string,
  endDate?: string
): Promise<SourceBreakdown> {
  const pool = getPool();
  const params: string[] = [];
  let paramIdx = 1;
  let dateFilter = "";

  if (startDate) {
    dateFilter += ` AND attended_at >= $${paramIdx++}`;
    params.push(startDate);
  }
  if (endDate) {
    dateFilter += ` AND attended_at < $${paramIdx++}`;
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

  const { rows } = await pool.query(query, params);
  return classifySourceRows(
    rows.map((r: Record<string, unknown>) => ({ pass: r.pass as string, cnt: Number(r.cnt) })),
    true
  );
}

/**
 * Get unique returning non-member visitors per week (Monday-based weeks).
 * Returning = non-subscriber emails that are NOT in the first_visits table for the same window.
 */
export async function getReturningUniqueVisitorsByWeek(
  startDate?: string,
  endDate?: string
): Promise<UniqueVisitorWeek[]> {
  const pool = getPool();
  const params: string[] = [];
  let paramIdx = 1;
  let fvDateFilter = "";
  let regDateFilter = "";

  if (startDate) {
    fvDateFilter += ` AND attended_at >= $${paramIdx++}`;
    params.push(startDate);
    regDateFilter += ` AND r.attended_at >= $${paramIdx++}`;
    params.push(startDate);
  }
  if (endDate) {
    regDateFilter += ` AND r.attended_at < $${paramIdx++}`;
    params.push(endDate);
  }

  const query = `
    SELECT DATE_TRUNC('week', r.attended_at::date)::date::text as "weekStart",
           COUNT(DISTINCT r.email) as "uniqueVisitors"
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
    GROUP BY "weekStart"
    ORDER BY "weekStart"
  `;

  const { rows } = await pool.query(query, params);
  return rows.map((r: Record<string, unknown>) => ({
    weekStart: r.weekStart as string,
    uniqueVisitors: Number(r.uniqueVisitors),
  }));
}

/**
 * Get source breakdown for returning non-members (unique people).
 * Each person is attributed to one source based on their MOST RECENT visit's pass type.
 */
export async function getReturningSourceBreakdown(
  startDate?: string,
  endDate?: string
): Promise<SourceBreakdown> {
  const pool = getPool();
  const params: string[] = [];
  let paramIdx = 1;
  let fvDateFilter = "";
  let regDateFilter = "";

  if (startDate) {
    fvDateFilter += ` AND attended_at >= $${paramIdx++}`;
    params.push(startDate);
    regDateFilter += ` AND r.attended_at >= $${paramIdx++}`;
    params.push(startDate);
  }
  if (endDate) {
    regDateFilter += ` AND r.attended_at < $${paramIdx++}`;
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

  const { rows } = await pool.query(query, params);
  return classifySourceRows(
    rows.map((r: Record<string, unknown>) => ({ pass: r.pass as string, cnt: Number(r.cnt) })),
    false
  );
}
