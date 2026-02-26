import { getPool } from "./database";
import { isDropInOrIntro } from "../analytics/categories";

// ── Types ────────────────────────────────────────────────────

export interface RegistrationRow {
  eventName: string;
  eventId?: string;
  performanceId?: string;
  performanceStartsAt: string;
  locationName: string;
  videoName?: string;
  videoId?: string;
  teacherName: string;
  firstName: string;
  lastName: string;
  email: string;
  phone?: string;
  role?: string;
  registeredAt?: string;
  canceledAt?: string;
  attendedAt: string;
  registrationType: string;
  state: string;
  pass: string;
  subscription: string; // "true" or "false"
  revenueState?: string;
  revenue: number;
  /** Union.fit registration ID from raw export (for precise dedup) */
  unionRegistrationId?: string;
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
          event_name, event_id, performance_id, performance_starts_at,
          location_name, video_name, video_id, teacher_name,
          first_name, last_name, email, phone, role,
          registered_at, canceled_at, attended_at,
          registration_type, state, pass, subscription, revenue_state, revenue,
          union_registration_id
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23)
        ON CONFLICT (email, attended_at) DO UPDATE SET
          event_name = COALESCE(EXCLUDED.event_name, registrations.event_name),
          event_id = COALESCE(EXCLUDED.event_id, registrations.event_id),
          performance_id = COALESCE(EXCLUDED.performance_id, registrations.performance_id),
          performance_starts_at = COALESCE(EXCLUDED.performance_starts_at, registrations.performance_starts_at),
          location_name = COALESCE(EXCLUDED.location_name, registrations.location_name),
          video_name = COALESCE(EXCLUDED.video_name, registrations.video_name),
          video_id = COALESCE(EXCLUDED.video_id, registrations.video_id),
          phone = COALESCE(EXCLUDED.phone, registrations.phone),
          role = COALESCE(EXCLUDED.role, registrations.role),
          canceled_at = COALESCE(EXCLUDED.canceled_at, registrations.canceled_at),
          revenue_state = COALESCE(EXCLUDED.revenue_state, registrations.revenue_state),
          revenue = COALESCE(EXCLUDED.revenue, registrations.revenue),
          union_registration_id = COALESCE(EXCLUDED.union_registration_id, registrations.union_registration_id)`,
        [
          r.eventName, r.eventId || null, r.performanceId || null, r.performanceStartsAt,
          r.locationName, r.videoName || null, r.videoId || null, r.teacherName,
          r.firstName, r.lastName, r.email, r.phone || null, r.role || null,
          r.registeredAt || null, r.canceledAt || null, r.attendedAt,
          r.registrationType, r.state, r.pass, r.subscription, r.revenueState || null,
          r.revenue, r.unionRegistrationId || null,
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
      ${dropInPassFilter()}
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
 *
 * Uses `registrations` table (fresh data with emails) instead of `first_visits`
 * (which is stale and lacks email/name fields from old HTML scrape).
 * Filters to non-subscriber registrations to approximate first-visit behavior.
 */
export async function getFirstVisitsByWeek(startDate?: string, endDate?: string): Promise<WeeklySegmentedCount[]> {
  const pool = getPool();
  let query = `
    SELECT TO_CHAR(attended_at::date, 'IYYY-"W"IW') as week, pass, COUNT(*) as count
    FROM registrations
    WHERE attended_at IS NOT NULL AND attended_at != ''
      AND (subscription = 'false' OR subscription IS NULL)
      ${dropInPassFilter()}
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
 * Count drop-in (non-subscriber) registrations in a date range.
 */
export async function getDropInCountForRange(
  startDate: string,
  endDate: string,
): Promise<number> {
  const pool = getPool();
  const res = await pool.query(
    `SELECT COUNT(*) AS cnt
     FROM registrations
     WHERE attended_at IS NOT NULL AND attended_at != ''
       AND attended_at >= $1 AND attended_at < $2
       AND (subscription = 'false' OR subscription IS NULL)
       ${dropInPassFilter()}`,
    [startDate, endDate]
  );
  return Number(res.rows[0].cnt);
}

/**
 * Count guest/community pass visits in a date range.
 * Guest passes include GUEST and COMMUNITY passes.
 * Uses registrations table (non-subscriber only) for fresh data.
 */
export async function getGuestCountForRange(
  startDate: string,
  endDate: string,
): Promise<number> {
  const pool = getPool();
  const res = await pool.query(
    `SELECT COUNT(*) AS cnt
     FROM registrations
     WHERE attended_at IS NOT NULL AND attended_at != ''
       AND attended_at >= $1 AND attended_at < $2
       AND (subscription = 'false' OR subscription IS NULL)
       AND (UPPER(pass) LIKE '%GUEST%' OR UPPER(pass) LIKE '%COMMUNITY%')`,
    [startDate, endDate]
  );
  return Number(res.rows[0].cnt);
}

/**
 * Count intro week passes used in a date range.
 * Uses registrations table for fresh data (first_visits is stale).
 */
export async function getIntroWeekCountForRange(
  startDate: string,
  endDate: string,
): Promise<number> {
  const pool = getPool();
  const res = await pool.query(
    `SELECT COUNT(*) AS cnt
     FROM registrations
     WHERE attended_at IS NOT NULL AND attended_at != ''
       AND attended_at >= $1 AND attended_at < $2
       AND (subscription = 'false' OR subscription IS NULL)
       AND UPPER(pass) LIKE '%INTRO WEEK%'`,
    [startDate, endDate]
  );
  return Number(res.rows[0].cnt);
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
       AND attended_at IS NOT NULL AND attended_at != ''
       ${dropInPassFilter()}`,
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
       AND attended_at IS NOT NULL AND attended_at != ''
       ${dropInPassFilter()}`,
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
       ${dropInPassFilter()}
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
 * Get unique non-subscriber visitors per week (Monday-based weeks).
 * Returns COUNT(DISTINCT email) grouped by week start date.
 * Uses registrations table (non-subscriber) for fresh data.
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
    FROM registrations
    WHERE attended_at IS NOT NULL AND attended_at != ''
      AND email IS NOT NULL AND email != ''
      AND (subscription = 'false' OR subscription IS NULL)
      ${dropInPassFilter()}
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
 * Get source breakdown for non-subscriber visitors (unique people).
 * Each person is attributed to one source based on their FIRST visit's pass type.
 * Uses registrations table for fresh data.
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
      FROM registrations
      WHERE attended_at IS NOT NULL AND attended_at != ''
        AND email IS NOT NULL AND email != ''
        AND (subscription = 'false' OR subscription IS NULL)
        ${dropInPassFilter()}
        ${dateFilter}
      GROUP BY email
    )
    SELECT r.pass, COUNT(*) as cnt
    FROM first_per_email fpe
    JOIN registrations r
      ON r.email = fpe.email
      AND r.attended_at = fpe.first_at
    GROUP BY r.pass
  `;

  const { rows } = await pool.query(query, params);
  return classifySourceRows(
    rows.map((r: Record<string, unknown>) => ({ pass: r.pass as string, cnt: Number(r.cnt) })),
    true
  );
}

/**
 * Get unique INTRO WEEK customers per week (Monday-based weeks).
 * Counts distinct emails from registrations where pass contains 'INTRO'.
 * Uses registrations table for fresh data (first_visits is stale).
 */
export async function getIntroWeekCustomersByWeek(
  startDate?: string,
): Promise<{ weekStart: string; customers: number }[]> {
  const pool = getPool();
  const params: string[] = [];
  let dateFilter = "";

  if (startDate) {
    dateFilter = ` AND attended_at >= $1`;
    params.push(startDate);
  }

  const query = `
    SELECT DATE_TRUNC('week', attended_at::date)::date::text as "weekStart",
           COUNT(DISTINCT email)::int as "customers"
    FROM registrations
    WHERE attended_at IS NOT NULL AND attended_at != ''
      AND email IS NOT NULL AND email != ''
      AND (subscription = 'false' OR subscription IS NULL)
      AND UPPER(pass) LIKE '%INTRO WEEK%'
      ${dateFilter}
    GROUP BY "weekStart"
    ORDER BY "weekStart"
  `;

  const { rows } = await pool.query(query, params);
  return rows.map((r: Record<string, unknown>) => ({
    weekStart: r.weekStart as string,
    customers: Number(r.customers),
  }));
}

// ── Active Intro Week customers (for CSV export) ────────────

export interface ActiveIntroWeekRow {
  name: string;
  email: string;
  startDate: string;   // YYYY-MM-DD  (first class registration)
  endDate: string;     // YYYY-MM-DD  (startDate + 7 days)
  daysLeft: number;
  classesAttended: number;
}

/**
 * Get all people currently within their 7-day Intro Week window.
 *
 * Logic: find each email's first attended_at with INTRO WEEK pass.
 * Their window is [first_attended, first_attended + 7 days].
 * If today is within that window (or slightly past), they appear.
 *
 * `lookbackDays` controls how far back to search (default 14 = includes
 * recently expired intro weeks too).
 */
export async function getActiveIntroWeekCustomers(
  lookbackDays = 14,
): Promise<ActiveIntroWeekRow[]> {
  const pool = getPool();

  const query = `
    WITH intro_visits AS (
      SELECT
        email,
        CONCAT(first_name, ' ', last_name) AS name,
        attended_at::date AS visit_date
      FROM registrations
      WHERE attended_at IS NOT NULL AND attended_at != ''
        AND email IS NOT NULL AND email != ''
        AND UPPER(pass) LIKE '%INTRO WEEK%'
        AND state IN ('redeemed', 'confirmed')
    ),
    first_visit AS (
      SELECT
        email,
        MIN(name) AS name,
        MIN(visit_date) AS start_date
      FROM intro_visits
      GROUP BY email
      HAVING MIN(visit_date) >= CURRENT_DATE - $1::int
    )
    SELECT
      fv.name,
      fv.email,
      fv.start_date::text AS "startDate",
      (fv.start_date + INTERVAL '7 days')::date::text AS "endDate",
      GREATEST(0, (fv.start_date + INTERVAL '7 days')::date - CURRENT_DATE)::int AS "daysLeft",
      COUNT(iv.visit_date)::int AS "classesAttended"
    FROM first_visit fv
    JOIN intro_visits iv ON iv.email = fv.email
    GROUP BY fv.name, fv.email, fv.start_date
    ORDER BY fv.start_date DESC, fv.name
  `;

  const { rows } = await pool.query(query, [lookbackDays]);
  return rows as ActiveIntroWeekRow[];
}

// ── Intro Week Conversion Funnel ─────────────────────────────

export interface IntroWeekConversionRow {
  name: string;
  email: string;
  introStart: string;
  introEnd: string;
  classesAttended: number;
  converted: boolean;
}

/**
 * Get expired intro week customers from the last 14 days and determine
 * which ones converted to an in-studio auto-renew subscription.
 *
 * "Expired" = their 7-day intro window ended between 0 and 14 days ago.
 * "Converted" = they have ANY active in-studio auto-renew right now
 * (plan_state not in Canceled/Invalid/Paused, plan matches IN_STUDIO_PLAN_FILTER).
 *
 * Excludes people currently in their intro week (window hasn't ended yet).
 */
export async function getIntroWeekConversionData(): Promise<IntroWeekConversionRow[]> {
  const pool = getPool();

  const query = `
    WITH intro_visits AS (
      SELECT
        LOWER(email) AS email,
        CONCAT(first_name, ' ', last_name) AS name,
        attended_at::date AS visit_date
      FROM registrations
      WHERE attended_at IS NOT NULL AND attended_at != ''
        AND email IS NOT NULL AND email != ''
        AND UPPER(pass) LIKE '%INTRO WEEK%'
        AND state IN ('redeemed', 'confirmed')
    ),
    intro_customers AS (
      SELECT
        email,
        MIN(name) AS name,
        MIN(visit_date) AS start_date,
        (MIN(visit_date) + INTERVAL '7 days')::date AS end_date,
        COUNT(visit_date)::int AS classes_attended
      FROM intro_visits
      GROUP BY email
      -- Expired in the last 14 days: end_date between (today - 14) and today
      HAVING (MIN(visit_date) + INTERVAL '7 days')::date <= CURRENT_DATE
        AND (MIN(visit_date) + INTERVAL '7 days')::date >= CURRENT_DATE - 14
    ),
    active_instudio AS (
      SELECT DISTINCT LOWER(customer_email) AS email
      FROM auto_renews
      WHERE plan_state NOT IN ('Canceled', 'Invalid', 'Paused')
        ${IN_STUDIO_PLAN_FILTER}
    )
    SELECT
      ic.name,
      ic.email,
      ic.start_date::text AS "introStart",
      ic.end_date::text AS "introEnd",
      ic.classes_attended AS "classesAttended",
      CASE WHEN ais.email IS NOT NULL THEN true ELSE false END AS converted
    FROM intro_customers ic
    LEFT JOIN active_instudio ais ON ais.email = ic.email
    ORDER BY ic.end_date DESC, ic.name
  `;

  const { rows } = await pool.query(query);
  return rows as IntroWeekConversionRow[];
}

/**
 * Get intro week customers whose 7-day trial is expiring within 2 days.
 *
 * Logic:
 * - Intro week start = first attended class date (not purchase date)
 * - Intro week end = start + 6 days (7-day window)
 * - "Expiring" = end_date - today is 1 or 2 (within 2 days of end, not same day)
 * - Only looks at intro weeks started in the last 14 days
 * - Classes attended = count of registrations within the 7-day window
 */
export async function getExpiringIntroWeekCustomers(): Promise<{
  firstName: string;
  lastName: string;
  email: string;
  introStartDate: string;
  introEndDate: string;
  classesAttended: number;
  daysUntilExpiry: number;
}[]> {
  const pool = getPool();

  const query = `
    WITH intro_starts AS (
      SELECT
        LOWER(email) as email,
        MIN(first_name) as first_name,
        MIN(last_name) as last_name,
        MIN(attended_at::date) as intro_start
      FROM registrations
      WHERE attended_at IS NOT NULL AND attended_at != ''
        AND email IS NOT NULL AND email != ''
        AND (subscription = 'false' OR subscription IS NULL)
        AND UPPER(pass) LIKE '%INTRO WEEK%'
        AND attended_at::date >= (CURRENT_DATE - INTERVAL '14 days')
      GROUP BY LOWER(email)
    ),
    intro_windows AS (
      SELECT
        email,
        first_name,
        last_name,
        intro_start,
        (intro_start + INTERVAL '6 days')::date as intro_end,
        ((intro_start + INTERVAL '6 days')::date - CURRENT_DATE)::int as days_until_expiry
      FROM intro_starts
    ),
    class_counts AS (
      SELECT
        LOWER(r.email) as email,
        COUNT(*)::int as classes_attended
      FROM registrations r
      JOIN intro_windows iw ON LOWER(r.email) = iw.email
      WHERE r.attended_at IS NOT NULL AND r.attended_at != ''
        AND r.attended_at::date >= iw.intro_start
        AND r.attended_at::date <= iw.intro_end
        AND (r.subscription = 'false' OR r.subscription IS NULL)
      GROUP BY LOWER(r.email)
    )
    SELECT
      iw.first_name as "firstName",
      iw.last_name as "lastName",
      iw.email,
      iw.intro_start::text as "introStartDate",
      iw.intro_end::text as "introEndDate",
      COALESCE(cc.classes_attended, 0)::int as "classesAttended",
      iw.days_until_expiry as "daysUntilExpiry"
    FROM intro_windows iw
    LEFT JOIN class_counts cc ON cc.email = iw.email
    WHERE iw.days_until_expiry >= 1
      AND iw.days_until_expiry <= 2
    ORDER BY iw.days_until_expiry ASC, iw.last_name ASC
  `;

  const { rows } = await pool.query(query);
  return rows.map((r: Record<string, unknown>) => ({
    firstName: (r.firstName as string) || "",
    lastName: (r.lastName as string) || "",
    email: r.email as string,
    introStartDate: r.introStartDate as string,
    introEndDate: r.introEndDate as string,
    classesAttended: Number(r.classesAttended),
    daysUntilExpiry: Number(r.daysUntilExpiry),
  }));
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
      ${dropInPassFilter("r.pass")}
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
        ${dropInPassFilter("r.pass")}
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

// ── Drop-In Weekly Detail Queries ────────────────────────────

export interface DropInWeekDetailRow {
  weekStart: string;
  weekEnd: string;
  visits: number;
  uniqueCustomers: number;
  firstTime: number;
  repeatCustomers: number;
}

/**
 * Get drop-in weekly detail: visits, unique customers, first-time vs repeat.
 * "First-time" = customer's all-time first non-subscriber visit falls in that week.
 * Only returns complete weeks (excludes the current partial week).
 */
export async function getDropInWeeklyDetail(weeksBack = 16): Promise<DropInWeekDetailRow[]> {
  const pool = getPool();

  const query = `
    WITH first_ever AS (
      -- Each customer's all-time first non-subscriber visit date
      SELECT LOWER(email) as email, MIN(attended_at::date) as first_drop_in_date
      FROM registrations
      WHERE attended_at IS NOT NULL AND attended_at != ''
        AND email IS NOT NULL AND email != ''
        AND (subscription = 'false' OR subscription IS NULL)
        ${dropInPassFilter()}
      GROUP BY LOWER(email)
    ),
    drop_in_visits AS (
      SELECT r.email, r.attended_at::date as visit_date,
             DATE_TRUNC('week', r.attended_at::date)::date as week_start
      FROM registrations r
      WHERE r.attended_at IS NOT NULL AND r.attended_at != ''
        AND r.email IS NOT NULL AND r.email != ''
        AND (r.subscription = 'false' OR r.subscription IS NULL)
        ${dropInPassFilter("r.pass")}
        AND r.attended_at::date >= (DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '${weeksBack} weeks')::date
        AND r.attended_at::date < DATE_TRUNC('week', CURRENT_DATE)::date
    )
    SELECT
      d.week_start::text as "weekStart",
      (d.week_start + INTERVAL '6 days')::date::text as "weekEnd",
      COUNT(*) as visits,
      COUNT(DISTINCT LOWER(d.email)) as "uniqueCustomers",
      COUNT(DISTINCT CASE
        WHEN fe.first_drop_in_date >= d.week_start
         AND fe.first_drop_in_date < d.week_start + INTERVAL '7 days'
        THEN LOWER(d.email) END) as "firstTime",
      COUNT(DISTINCT CASE
        WHEN fe.first_drop_in_date < d.week_start
        THEN LOWER(d.email) END) as "repeatCustomers"
    FROM drop_in_visits d
    LEFT JOIN first_ever fe ON LOWER(d.email) = fe.email
    GROUP BY d.week_start
    ORDER BY d.week_start
  `;

  const { rows } = await pool.query(query);
  return rows.map((r: Record<string, unknown>) => ({
    weekStart: r.weekStart as string,
    weekEnd: r.weekEnd as string,
    visits: Number(r.visits),
    uniqueCustomers: Number(r.uniqueCustomers),
    firstTime: Number(r.firstTime),
    repeatCustomers: Number(r.repeatCustomers),
  }));
}

/**
 * Get drop-in WTD (week-to-date) stats for the current partial week.
 */
export async function getDropInWTD(): Promise<DropInWeekDetailRow & { daysLeft: number } | null> {
  const pool = getPool();

  const query = `
    WITH first_ever AS (
      SELECT LOWER(email) as email, MIN(attended_at::date) as first_drop_in_date
      FROM registrations
      WHERE attended_at IS NOT NULL AND attended_at != ''
        AND email IS NOT NULL AND email != ''
        AND (subscription = 'false' OR subscription IS NULL)
        ${dropInPassFilter()}
      GROUP BY LOWER(email)
    ),
    wtd_visits AS (
      SELECT r.email, r.attended_at::date as visit_date
      From registrations r
      WHERE r.attended_at IS NOT NULL AND r.attended_at != ''
        AND r.email IS NOT NULL AND r.email != ''
        AND (r.subscription = 'false' OR r.subscription IS NULL)
        ${dropInPassFilter("r.pass")}
        AND r.attended_at::date >= DATE_TRUNC('week', CURRENT_DATE)::date
        AND r.attended_at::date <= CURRENT_DATE
    )
    SELECT
      DATE_TRUNC('week', CURRENT_DATE)::date::text as "weekStart",
      (DATE_TRUNC('week', CURRENT_DATE) + INTERVAL '6 days')::date::text as "weekEnd",
      COUNT(*) as visits,
      COUNT(DISTINCT LOWER(w.email)) as "uniqueCustomers",
      COUNT(DISTINCT CASE
        WHEN fe.first_drop_in_date >= DATE_TRUNC('week', CURRENT_DATE)::date
        THEN LOWER(w.email) END) as "firstTime",
      COUNT(DISTINCT CASE
        WHEN fe.first_drop_in_date < DATE_TRUNC('week', CURRENT_DATE)::date
        THEN LOWER(w.email) END) as "repeatCustomers",
      (DATE_TRUNC('week', CURRENT_DATE)::date + 6 - CURRENT_DATE) as "daysLeft"
    FROM wtd_visits w
    LEFT JOIN first_ever fe ON LOWER(w.email) = fe.email
  `;

  const { rows } = await pool.query(query);
  if (rows.length === 0) return null;
  const r = rows[0] as Record<string, unknown>;
  return {
    weekStart: r.weekStart as string,
    weekEnd: r.weekEnd as string,
    visits: Number(r.visits),
    uniqueCustomers: Number(r.uniqueCustomers),
    firstTime: Number(r.firstTime),
    repeatCustomers: Number(r.repeatCustomers),
    daysLeft: Number(r.daysLeft),
  };
}

/**
 * Get last week's drop-in visits through the same weekday as today.
 * Used for apples-to-apples WTD comparison ("vs last week, same day cut").
 */
export async function getDropInLastWeekWTD(): Promise<number> {
  const pool = getPool();

  const query = `
    SELECT COUNT(*) as visits
    FROM registrations r
    WHERE r.attended_at IS NOT NULL AND r.attended_at != ''
      AND r.email IS NOT NULL AND r.email != ''
      AND (r.subscription = 'false' OR r.subscription IS NULL)
      ${dropInPassFilter("r.pass")}
      AND r.attended_at::date >= (DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '1 week')::date
      AND r.attended_at::date <= (CURRENT_DATE - INTERVAL '7 days')::date
  `;

  const { rows } = await pool.query(query);
  return Number(rows[0]?.visits ?? 0);
}

/**
 * Get drop-in frequency distribution over the last 90 days.
 * Buckets: 1 visit, 2-4 visits, 5-10 visits, 11+ visits.
 */
export async function getDropInFrequencyDistribution(): Promise<{
  bucket1: number;
  bucket2to4: number;
  bucket5to10: number;
  bucket11plus: number;
  totalCustomers: number;
}> {
  const pool = getPool();

  const query = `
    WITH visit_counts AS (
      SELECT LOWER(email) as email, COUNT(*) as visits
      FROM registrations
      WHERE attended_at IS NOT NULL AND attended_at != ''
        AND email IS NOT NULL AND email != ''
        AND (subscription = 'false' OR subscription IS NULL)
        ${dropInPassFilter()}
        AND attended_at::date >= (CURRENT_DATE - INTERVAL '90 days')
      GROUP BY LOWER(email)
    )
    SELECT
      COUNT(*) FILTER (WHERE visits = 1) as bucket1,
      COUNT(*) FILTER (WHERE visits BETWEEN 2 AND 4) as "bucket2to4",
      COUNT(*) FILTER (WHERE visits BETWEEN 5 AND 10) as "bucket5to10",
      COUNT(*) FILTER (WHERE visits >= 11) as "bucket11plus",
      COUNT(*) as "totalCustomers"
    FROM visit_counts
  `;

  const { rows } = await pool.query(query);
  const r = rows[0] as Record<string, unknown>;
  return {
    bucket1: Number(r.bucket1),
    bucket2to4: Number(r.bucket2to4),
    bucket5to10: Number(r.bucket5to10),
    bucket11plus: Number(r.bucket11plus),
    totalCustomers: Number(r.totalCustomers),
  };
}

// ── New Customer Queries ────────────────────────────────────

export interface NewCustomerWeekRow {
  weekStart: string;  // Monday YYYY-MM-DD
  weekEnd: string;    // Sunday YYYY-MM-DD
  count: number;
}

export interface NewCustomerCohortRow {
  cohortStart: string;
  cohortEnd: string;
  newCustomers: number;
  week1: number;
  week2: number;
  week3: number;
  total3Week: number;
}

// SQL fragment: in-studio intro/drop-in passes only (mirrors isDropInOrIntro).
// Excludes staff, teacher, demo, teacher-training, and TV/replay/livestream visits.
// Use col param for queries with a table alias (e.g. 'r.pass').
function dropInPassFilter(col = "pass"): string {
  return `
  AND (
    UPPER(${col}) LIKE '%DROP-IN%' OR UPPER(${col}) LIKE '%DROP IN%' OR UPPER(${col}) LIKE '%DROPIN%'
    OR UPPER(${col}) LIKE '%DROPLET%'
    OR UPPER(${col}) LIKE '%INTRO WEEK%'
    OR UPPER(${col}) LIKE '%TRIAL%'
    OR UPPER(${col}) LIKE '%FIRST%'
    OR UPPER(${col}) LIKE '%SINGLE CLASS%'
    OR UPPER(${col}) LIKE '%WELLHUB%'
    OR UPPER(${col}) LIKE '%GUEST%'
    OR UPPER(${col}) LIKE '%COMMUNITY DAY%'
    OR UPPER(${col}) LIKE '%POKER CHIP%'
    OR UPPER(${col}) LIKE '%ON RUNNING%'
  )`;
}
// Backward-compat alias
const IN_STUDIO_PASS_FILTER = dropInPassFilter();

// SQL fragment: only SKY3 + MEMBER auto-renew plans (excludes SKY TING TV, UNKNOWN).
const IN_STUDIO_PLAN_FILTER = `
  AND (
    UPPER(plan_name) LIKE '%SKY3%' OR UPPER(plan_name) LIKE '%SKY5%'
    OR UPPER(plan_name) LIKE '%SKYHIGH%' OR UPPER(plan_name) LIKE '%5 PACK%'
    OR UPPER(plan_name) LIKE '%5-PACK%'
    OR UPPER(plan_name) LIKE '%UNLIMITED%' OR UPPER(plan_name) LIKE '%MEMBER%'
    OR UPPER(plan_name) LIKE '%ALL ACCESS%' OR UPPER(plan_name) LIKE '%TING FAM%'
    OR UPPER(plan_name) LIKE '%WELCOME SKY3%'
  )
  AND UPPER(plan_name) NOT LIKE '%SKY TING TV%'
  AND UPPER(plan_name) NOT LIKE '%SKYTING TV%'`;

/**
 * Get new customer volume by week.
 * A "new customer" = person whose first in-studio visit (intro week / drop-in)
 * falls in the week. Excludes TV/replay/livestream first visits.
 * Returns ~6 weeks of data (Monday-based weeks).
 */
export async function getNewCustomerVolumeByWeek(): Promise<NewCustomerWeekRow[]> {
  const pool = getPool();

  const query = `
    WITH first_date_per_email AS (
      SELECT LOWER(email) as email, MIN(attended_at::date) as first_date
      FROM first_visits
      WHERE attended_at IS NOT NULL AND attended_at != ''
        AND email IS NOT NULL AND email != ''
        ${IN_STUDIO_PASS_FILTER}
      GROUP BY LOWER(email)
    ),
    weekly AS (
      SELECT DATE_TRUNC('week', first_date)::date as week_start,
             COUNT(*) as count
      FROM first_date_per_email
      GROUP BY week_start
      ORDER BY week_start DESC
      LIMIT 7
    )
    SELECT week_start::text as "weekStart",
           (week_start + INTERVAL '6 days')::date::text as "weekEnd",
           count
    FROM weekly
    ORDER BY week_start
  `;

  const { rows } = await pool.query(query);
  return rows.map((r: Record<string, unknown>) => ({
    weekStart: r.weekStart as string,
    weekEnd: r.weekEnd as string,
    count: Number(r.count),
  }));
}

/**
 * Get new customer cohort conversion data (in-studio only).
 *
 * Acquisition: first in-studio visit (intro week / drop-in pass).
 * Conversion: earliest SKY3 or MEMBER auto-renew within 3 weeks.
 * Excludes Sky Ting TV on both sides.
 *
 * For each weekly cohort (by first visit date), counts how many converted
 * to auto-renew subscriptions within week 1 (days 0-6), week 2 (7-13),
 * week 3 (14-20), and total.
 */
export async function getNewCustomerCohorts(): Promise<NewCustomerCohortRow[]> {
  const pool = getPool();

  const query = `
    WITH new_custs AS (
      SELECT LOWER(email) as email,
             MIN(attended_at::date) as first_date,
             DATE_TRUNC('week', MIN(attended_at::date))::date as cohort_start
      FROM first_visits
      WHERE attended_at IS NOT NULL AND attended_at != ''
        AND email IS NOT NULL AND email != ''
        ${IN_STUDIO_PASS_FILTER}
      GROUP BY LOWER(email)
    ),
    conversions AS (
      SELECT LOWER(customer_email) as email,
             MIN(created_at::date) as earliest_sub
      FROM auto_renews
      WHERE customer_email IS NOT NULL AND customer_email != ''
        AND created_at IS NOT NULL AND created_at != ''
        ${IN_STUDIO_PLAN_FILTER}
      GROUP BY LOWER(customer_email)
    ),
    cohort_data AS (
      SELECT nc.cohort_start,
             (nc.cohort_start + INTERVAL '6 days')::date as cohort_end,
             COUNT(*) as new_customers,
             COUNT(CASE WHEN c.earliest_sub IS NOT NULL
                        AND c.earliest_sub - nc.first_date BETWEEN 0 AND 6
                   THEN 1 END) as week1,
             COUNT(CASE WHEN c.earliest_sub IS NOT NULL
                        AND c.earliest_sub - nc.first_date BETWEEN 7 AND 13
                   THEN 1 END) as week2,
             COUNT(CASE WHEN c.earliest_sub IS NOT NULL
                        AND c.earliest_sub - nc.first_date BETWEEN 14 AND 20
                   THEN 1 END) as week3,
             COUNT(CASE WHEN c.earliest_sub IS NOT NULL
                        AND c.earliest_sub - nc.first_date BETWEEN 0 AND 20
                   THEN 1 END) as total_3week
      FROM new_custs nc
      LEFT JOIN conversions c ON c.email = nc.email
      GROUP BY nc.cohort_start
      ORDER BY nc.cohort_start DESC
      LIMIT 8
    )
    SELECT cohort_start::text as "cohortStart",
           cohort_end::text as "cohortEnd",
           new_customers as "newCustomers",
           week1,
           week2,
           week3,
           total_3week as "total3Week"
    FROM cohort_data
    ORDER BY cohort_start
  `;

  const { rows } = await pool.query(query);
  return rows.map((r: Record<string, unknown>) => ({
    cohortStart: r.cohortStart as string,
    cohortEnd: r.cohortEnd as string,
    newCustomers: Number(r.newCustomers),
    week1: Number(r.week1),
    week2: Number(r.week2),
    week3: Number(r.week3),
    total3Week: Number(r.total3Week),
  }));
}

// ── Conversion Pool Queries ──────────────────────────────────

// Pool slice SQL fragments: filter which non-subscriber visits count
export type PoolSliceKey = "all" | "drop-ins" | "intro-week" | "class-packs" | "high-intent";

const POOL_SLICE_FILTERS: Record<PoolSliceKey, string> = {
  "all": "",
  "drop-ins": `AND (UPPER(r.pass) LIKE '%DROP-IN%' OR UPPER(r.pass) LIKE '%DROP IN%' OR UPPER(r.pass) LIKE '%DROPIN%' OR UPPER(r.pass) LIKE '%DROPLET%')`,
  "intro-week": `AND (UPPER(r.pass) LIKE '%INTRO WEEK%' OR UPPER(r.pass) LIKE '%TRIAL%' OR UPPER(r.pass) LIKE '%FIRST%')`,
  "class-packs": `AND (UPPER(r.pass) LIKE '%PACK%' OR UPPER(r.pass) LIKE '%SINGLE CLASS%')`,
  "high-intent": "", // handled via subquery wrapping (≥2 visits in 30 days)
};

// For high-intent, we wrap the pool CTE with an additional filter
function getHighIntentPoolCTE(baseDateFilter: string): string {
  return `
    SELECT week_start, COUNT(DISTINCT email) as pool_size
    FROM (
      SELECT LOWER(r.email) as email,
             DATE_TRUNC('week', r.attended_at::date)::date as week_start,
             COUNT(*) OVER (
               PARTITION BY LOWER(r.email)
               ORDER BY r.attended_at::date
               RANGE BETWEEN INTERVAL '30 days' PRECEDING AND CURRENT ROW
             ) as rolling_30d_visits
      FROM registrations r
      WHERE r.attended_at IS NOT NULL AND r.attended_at != ''
        AND r.email IS NOT NULL AND r.email != ''
        AND (r.subscription = 'false' OR r.subscription IS NULL)
        ${dropInPassFilter("r.pass")}
        ${baseDateFilter}
    ) sub
    WHERE rolling_30d_visits >= 2
    GROUP BY week_start`;
}

export interface ConversionPoolWeekRow {
  weekStart: string;
  weekEnd: string;
  activePool7d: number;
  converts: number;
}

/**
 * Get weekly conversion pool data for complete weeks.
 *
 * Pool = unique emails with a non-subscriber registration that week
 *        (subscription = 'false') who were NOT on any active auto-renew at time of visit.
 * Converts = pool members whose FIRST in-studio auto-renew (MEMBER/SKY3) started that week,
 *            AND who had at least one prior non-subscriber visit.
 */
export async function getConversionPoolWeekly(weeksBack = 16, slice: PoolSliceKey = "all"): Promise<ConversionPoolWeekRow[]> {
  const pool = getPool();
  const sliceFilter = POOL_SLICE_FILTERS[slice];
  const baseDateFilter = `AND r.attended_at::date >= (DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '${weeksBack} weeks')::date
        AND r.attended_at::date < DATE_TRUNC('week', CURRENT_DATE)::date`;

  const poolCTE = slice === "high-intent"
    ? `non_auto_pool AS (${getHighIntentPoolCTE(baseDateFilter)})`
    : `non_auto_pool AS (
      SELECT DATE_TRUNC('week', r.attended_at::date)::date as week_start,
             COUNT(DISTINCT LOWER(r.email)) as pool_size
      FROM registrations r
      WHERE r.attended_at IS NOT NULL AND r.attended_at != ''
        AND r.email IS NOT NULL AND r.email != ''
        AND (r.subscription = 'false' OR r.subscription IS NULL)
        ${dropInPassFilter("r.pass")}
        ${baseDateFilter}
        ${sliceFilter}
      GROUP BY DATE_TRUNC('week', r.attended_at::date)::date
    )`;

  const query = `
    WITH week_series AS (
      SELECT generate_series(
        (DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '${weeksBack} weeks')::date,
        (DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '1 week')::date,
        '1 week'::interval
      )::date as week_start
    ),
    ${poolCTE},
    -- Each person's first in-studio auto-renew start date
    first_in_studio_sub AS (
      SELECT LOWER(customer_email) as email,
             MIN(created_at::date) as first_sub_date
      FROM auto_renews
      WHERE customer_email IS NOT NULL AND customer_email != ''
        AND created_at IS NOT NULL AND created_at != ''
        ${IN_STUDIO_PLAN_FILTER}
      GROUP BY LOWER(customer_email)
    ),
    -- Only count converters who had at least one prior non-sub visit
    converters AS (
      SELECT f.email, f.first_sub_date,
             DATE_TRUNC('week', f.first_sub_date)::date as convert_week
      FROM first_in_studio_sub f
      WHERE EXISTS (
        SELECT 1 FROM registrations r
        WHERE LOWER(r.email) = f.email
          AND r.attended_at IS NOT NULL AND r.attended_at != ''
          AND (r.subscription = 'false' OR r.subscription IS NULL)
          ${dropInPassFilter("r.pass")}
          AND r.attended_at::date < f.first_sub_date
          ${sliceFilter}
      )
    ),
    weekly_converts AS (
      SELECT convert_week as week_start,
             COUNT(*) as converts
      FROM converters
      WHERE convert_week >= (DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '${weeksBack} weeks')::date
        AND convert_week < DATE_TRUNC('week', CURRENT_DATE)::date
      GROUP BY convert_week
    )
    SELECT ws.week_start::text as "weekStart",
           (ws.week_start + INTERVAL '6 days')::date::text as "weekEnd",
           COALESCE(p.pool_size, 0) as "activePool7d",
           COALESCE(wc.converts, 0) as converts
    FROM week_series ws
    LEFT JOIN non_auto_pool p ON p.week_start = ws.week_start
    LEFT JOIN weekly_converts wc ON wc.week_start = ws.week_start
    ORDER BY ws.week_start
  `;

  const { rows } = await pool.query(query);
  return rows.map((r: Record<string, unknown>) => ({
    weekStart: r.weekStart as string,
    weekEnd: r.weekEnd as string,
    activePool7d: Number(r.activePool7d),
    converts: Number(r.converts),
  }));
}

/**
 * Get conversion pool week-to-date stats for the current partial week.
 * Includes both 7d pool (this week's visitors) and 30d pool (last 30 days).
 */
export async function getConversionPoolWTD(slice: PoolSliceKey = "all"): Promise<{
  weekStart: string;
  weekEnd: string;
  activePool7d: number;
  activePool30d: number;
  converts: number;
  daysLeft: number;
} | null> {
  const pool = getPool();
  const sliceFilter = POOL_SLICE_FILTERS[slice];

  // For high-intent, we need a different pool CTE
  const pool7dCTE = slice === "high-intent" ? `
    pool_7d AS (
      SELECT COUNT(DISTINCT email) as cnt FROM (
        SELECT LOWER(r.email) as email,
               COUNT(*) OVER (PARTITION BY LOWER(r.email) ORDER BY r.attended_at::date RANGE BETWEEN INTERVAL '30 days' PRECEDING AND CURRENT ROW) as rolling
        FROM registrations r
        WHERE r.attended_at IS NOT NULL AND r.attended_at != ''
          AND r.email IS NOT NULL AND r.email != ''
          AND (r.subscription = 'false' OR r.subscription IS NULL)
          ${dropInPassFilter("r.pass")}
          AND r.attended_at::date >= DATE_TRUNC('week', CURRENT_DATE)::date
          AND r.attended_at::date <= CURRENT_DATE
      ) sub WHERE rolling >= 2
    )` : `
    pool_7d AS (
      SELECT COUNT(DISTINCT LOWER(r.email)) as cnt
      FROM registrations r
      WHERE r.attended_at IS NOT NULL AND r.attended_at != ''
        AND r.email IS NOT NULL AND r.email != ''
        AND (r.subscription = 'false' OR r.subscription IS NULL)
        ${dropInPassFilter("r.pass")}
        AND r.attended_at::date >= DATE_TRUNC('week', CURRENT_DATE)::date
        AND r.attended_at::date <= CURRENT_DATE
        ${sliceFilter}
    )`;

  const pool30dCTE = slice === "high-intent" ? `
    pool_30d AS (
      SELECT COUNT(DISTINCT email) as cnt FROM (
        SELECT LOWER(r.email) as email,
               COUNT(*) OVER (PARTITION BY LOWER(r.email) ORDER BY r.attended_at::date RANGE BETWEEN INTERVAL '30 days' PRECEDING AND CURRENT ROW) as rolling
        FROM registrations r
        WHERE r.attended_at IS NOT NULL AND r.attended_at != ''
          AND r.email IS NOT NULL AND r.email != ''
          AND (r.subscription = 'false' OR r.subscription IS NULL)
          ${dropInPassFilter("r.pass")}
          AND r.attended_at::date >= (CURRENT_DATE - INTERVAL '30 days')::date
          AND r.attended_at::date <= CURRENT_DATE
      ) sub WHERE rolling >= 2
    )` : `
    pool_30d AS (
      SELECT COUNT(DISTINCT LOWER(r.email)) as cnt
      FROM registrations r
      WHERE r.attended_at IS NOT NULL AND r.attended_at != ''
        AND r.email IS NOT NULL AND r.email != ''
        AND (r.subscription = 'false' OR r.subscription IS NULL)
        ${dropInPassFilter("r.pass")}
        AND r.attended_at::date >= (CURRENT_DATE - INTERVAL '30 days')::date
        AND r.attended_at::date <= CURRENT_DATE
        ${sliceFilter}
    )`;

  const query = `
    WITH ${pool7dCTE},
    ${pool30dCTE},
    first_in_studio_sub AS (
      SELECT LOWER(customer_email) as email,
             MIN(created_at::date) as first_sub_date
      FROM auto_renews
      WHERE customer_email IS NOT NULL AND customer_email != ''
        AND created_at IS NOT NULL AND created_at != ''
        ${IN_STUDIO_PLAN_FILTER}
      GROUP BY LOWER(customer_email)
    ),
    wtd_converts AS (
      SELECT COUNT(*) as cnt
      FROM first_in_studio_sub f
      WHERE f.first_sub_date >= DATE_TRUNC('week', CURRENT_DATE)::date
        AND f.first_sub_date <= CURRENT_DATE
        AND EXISTS (
          SELECT 1 FROM registrations r
          WHERE LOWER(r.email) = f.email
            AND r.attended_at IS NOT NULL AND r.attended_at != ''
            AND (r.subscription = 'false' OR r.subscription IS NULL)
            ${dropInPassFilter("r.pass")}
            AND r.attended_at::date < f.first_sub_date
            ${sliceFilter}
        )
    )
    SELECT
      DATE_TRUNC('week', CURRENT_DATE)::date::text as "weekStart",
      (DATE_TRUNC('week', CURRENT_DATE) + INTERVAL '6 days')::date::text as "weekEnd",
      (SELECT cnt FROM pool_7d) as "activePool7d",
      (SELECT cnt FROM pool_30d) as "activePool30d",
      (SELECT cnt FROM wtd_converts) as converts,
      (7 - EXTRACT(ISODOW FROM CURRENT_DATE)::int) as "daysLeft"
  `;

  const { rows } = await pool.query(query);
  if (!rows.length) return null;
  const r = rows[0] as Record<string, unknown>;
  return {
    weekStart: r.weekStart as string,
    weekEnd: r.weekEnd as string,
    activePool7d: Number(r.activePool7d),
    activePool30d: Number(r.activePool30d),
    converts: Number(r.converts),
    daysLeft: Number(r.daysLeft),
  };
}

/**
 * Get conversion lag stats: median time-to-convert, avg visits before convert,
 * plus bucket distributions for both time and visits.
 *
 * Returns stats for both the current week's converters and a 12-week historical window.
 * Falls back to historical if current week has no converters.
 */
export async function getConversionPoolLagStats(slice: PoolSliceKey = "all"): Promise<{
  medianTimeToConvert: number | null;
  avgVisitsBeforeConvert: number | null;
  timeBucket0to30: number;
  timeBucket31to90: number;
  timeBucket91to180: number;
  timeBucket180plus: number;
  visitBucket1to2: number;
  visitBucket3to5: number;
  visitBucket6to10: number;
  visitBucket11plus: number;
  totalConvertersInBuckets: number;
  historicalMedianTimeToConvert: number | null;
  historicalAvgVisitsBeforeConvert: number | null;
} | null> {
  const pool = getPool();
  const sliceFilter = POOL_SLICE_FILTERS[slice];

  const query = `
    WITH first_in_studio_sub AS (
      SELECT LOWER(customer_email) as email,
             MIN(created_at::date) as first_sub_date
      FROM auto_renews
      WHERE customer_email IS NOT NULL AND customer_email != ''
        AND created_at IS NOT NULL AND created_at != ''
        ${IN_STUDIO_PLAN_FILTER}
      GROUP BY LOWER(customer_email)
    ),
    -- All converters who had prior non-sub visits (last 12 complete weeks + current)
    converter_detail AS (
      SELECT f.email,
             f.first_sub_date,
             DATE_TRUNC('week', f.first_sub_date)::date as convert_week,
             MIN(r.attended_at::date) as first_non_auto_visit,
             COUNT(DISTINCT r.attended_at::date) as visit_count,
             f.first_sub_date - MIN(r.attended_at::date) as days_to_convert
      FROM first_in_studio_sub f
      JOIN registrations r ON LOWER(r.email) = f.email
        AND r.attended_at IS NOT NULL AND r.attended_at != ''
        AND (r.subscription = 'false' OR r.subscription IS NULL)
        ${dropInPassFilter("r.pass")}
        AND r.attended_at::date < f.first_sub_date
        ${sliceFilter}
      WHERE f.first_sub_date >= (DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '12 weeks')::date
      GROUP BY f.email, f.first_sub_date
    ),
    -- Current week converters
    current_week AS (
      SELECT * FROM converter_detail
      WHERE convert_week = DATE_TRUNC('week', CURRENT_DATE)::date
    ),
    -- Historical: last 12 complete weeks
    historical AS (
      SELECT * FROM converter_detail
      WHERE convert_week >= (DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '12 weeks')::date
        AND convert_week < DATE_TRUNC('week', CURRENT_DATE)::date
    )
    SELECT
      -- Current week stats
      (SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY days_to_convert)
       FROM current_week) as "medianTimeToConvert",
      (SELECT AVG(visit_count)
       FROM current_week) as "avgVisitsBeforeConvert",
      -- Time-to-convert buckets (12-week historical — more meaningful than current week alone)
      (SELECT COUNT(*) FILTER (WHERE days_to_convert BETWEEN 0 AND 30) FROM historical) as "timeBucket0to30",
      (SELECT COUNT(*) FILTER (WHERE days_to_convert BETWEEN 31 AND 90) FROM historical) as "timeBucket31to90",
      (SELECT COUNT(*) FILTER (WHERE days_to_convert BETWEEN 91 AND 180) FROM historical) as "timeBucket91to180",
      (SELECT COUNT(*) FILTER (WHERE days_to_convert > 180) FROM historical) as "timeBucket180plus",
      -- Visits-before-convert buckets (12-week historical)
      (SELECT COUNT(*) FILTER (WHERE visit_count BETWEEN 1 AND 2) FROM historical) as "visitBucket1to2",
      (SELECT COUNT(*) FILTER (WHERE visit_count BETWEEN 3 AND 5) FROM historical) as "visitBucket3to5",
      (SELECT COUNT(*) FILTER (WHERE visit_count BETWEEN 6 AND 10) FROM historical) as "visitBucket6to10",
      (SELECT COUNT(*) FILTER (WHERE visit_count > 10) FROM historical) as "visitBucket11plus",
      (SELECT COUNT(*) FROM historical) as "totalConvertersInBuckets",
      -- Historical aggregates (12 complete weeks)
      (SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY days_to_convert)
       FROM historical) as "historicalMedianTimeToConvert",
      (SELECT AVG(visit_count)
       FROM historical) as "historicalAvgVisitsBeforeConvert"
  `;

  const { rows } = await pool.query(query);
  if (!rows.length) return null;
  const r = rows[0] as Record<string, unknown>;
  return {
    medianTimeToConvert: r.medianTimeToConvert != null ? Number(r.medianTimeToConvert) : null,
    avgVisitsBeforeConvert: r.avgVisitsBeforeConvert != null ? Number(r.avgVisitsBeforeConvert) : null,
    timeBucket0to30: Number(r.timeBucket0to30 ?? 0),
    timeBucket31to90: Number(r.timeBucket31to90 ?? 0),
    timeBucket91to180: Number(r.timeBucket91to180 ?? 0),
    timeBucket180plus: Number(r.timeBucket180plus ?? 0),
    visitBucket1to2: Number(r.visitBucket1to2 ?? 0),
    visitBucket3to5: Number(r.visitBucket3to5 ?? 0),
    visitBucket6to10: Number(r.visitBucket6to10 ?? 0),
    visitBucket11plus: Number(r.visitBucket11plus ?? 0),
    totalConvertersInBuckets: Number(r.totalConvertersInBuckets ?? 0),
    historicalMedianTimeToConvert: r.historicalMedianTimeToConvert != null ? Number(r.historicalMedianTimeToConvert) : null,
    historicalAvgVisitsBeforeConvert: r.historicalAvgVisitsBeforeConvert != null ? Number(r.historicalAvgVisitsBeforeConvert) : null,
  };
}

// ── Usage Frequency by Category ──────────────────────────────

import type { UsageCategoryData, UsageSegment, UsageData } from "@/types/dashboard";

// ── Segment color palette ────────────────────────────────
// Red/yellow/green semantic: red = disengaged, yellow = developing, green = healthy
// Blue = neutral/informational (e.g. Sky3 members who may be in the wrong plan)
const SEG_RED      = "#C4605A";
const SEG_RED_LITE = "#D4817B";
const SEG_YELLOW   = "#CDA63A";
const SEG_GREEN_LT = "#8BB574";
const SEG_GREEN    = "#5E9A4B";
const SEG_GREEN_DK = "#3D6B48";
const SEG_BLUE     = "#5B7FA5";

interface SegmentDef {
  name: string;
  rangeLabel: string;  // human-readable range, e.g. "0/mo", "1-2/mo"
  color: string;       // explicit semantic color
  min: number;         // exclusive lower bound (visits > min)
  max: number;         // inclusive upper bound (visits <= max), Infinity for unbounded
}

// Members: red → yellow → shades of green
const MEMBER_SEGMENTS: SegmentDef[] = [
  { name: "Dormant",     rangeLabel: "0/mo",    color: SEG_RED,      min: -1,  max: 0 },
  { name: "Casual",      rangeLabel: "1-2/mo",  color: SEG_RED_LITE, min: 0,   max: 2 },
  { name: "Developing",  rangeLabel: "2-4/mo",  color: SEG_YELLOW,   min: 2,   max: 4 },
  { name: "Established", rangeLabel: "4-8/mo",  color: SEG_GREEN_LT, min: 4,   max: 8 },
  { name: "Committed",   rangeLabel: "8-12/mo", color: SEG_GREEN,    min: 8,   max: 12 },
  { name: "Core",        rangeLabel: "12+/mo",  color: SEG_GREEN_DK, min: 12,  max: Infinity },
];

// Sky3: red → green → blue (over-users = wrong plan, not bad)
const SKY3_SEGMENTS: SegmentDef[] = [
  { name: "Dormant",     rangeLabel: "0/mo",   color: SEG_RED,      min: -1,  max: 0 },
  { name: "Under-Using", rangeLabel: "<1/mo",  color: SEG_RED_LITE, min: 0,   max: 1 },
  { name: "On Pace",     rangeLabel: "1-3/mo", color: SEG_GREEN,    min: 1,   max: 3 },
  { name: "Over-Using",  rangeLabel: "3+/mo",  color: SEG_BLUE,     min: 3,   max: Infinity },
];

// Sky Ting TV: red → yellow → shades of green
const TV_SEGMENTS: SegmentDef[] = [
  { name: "Dormant",  rangeLabel: "0/mo",   color: SEG_RED,      min: -1,  max: 0 },
  { name: "Casual",   rangeLabel: "1-2/mo", color: SEG_YELLOW,   min: 0,   max: 2 },
  { name: "Moderate", rangeLabel: "2-4/mo", color: SEG_GREEN_LT, min: 2,   max: 4 },
  { name: "Regular",  rangeLabel: "4-8/mo", color: SEG_GREEN,    min: 4,   max: 8 },
  { name: "Active",   rangeLabel: "8+/mo",  color: SEG_GREEN_DK, min: 8,   max: Infinity },
];

const CATEGORY_CONFIG: Record<string, {
  label: string;
  segments: SegmentDef[];
}> = {
  MEMBER:      { label: "Members",     segments: MEMBER_SEGMENTS },
  SKY3:        { label: "Sky3",        segments: SKY3_SEGMENTS },
  SKY_TING_TV: { label: "Sky Ting TV", segments: TV_SEGMENTS },
};

/**
 * Get usage frequency data segmented by plan category.
 *
 * Uses getActiveCounts() + getActiveAutoRenews() from auto-renew-store
 * as the SINGLE SOURCE OF TRUTH for "X Active" numbers. The counts
 * shown here will exactly match Growth, Overview, and every other section.
 *
 * Then joins visit counts from registrations in last 90 days.
 */
export async function getUsageFrequencyByCategory(): Promise<UsageData> {
  const { getActiveAutoRenews, getActiveCounts } = await import("./auto-renew-store");
  const pool = getPool();

  // 1. Get canonical active counts (single source of truth)
  const canonicalCounts = await getActiveCounts();

  // 2. Get active subs to build email→category mapping
  const activeSubs = await getActiveAutoRenews();

  // Build map: category → Set of unique emails
  const catEmails = new Map<string, Set<string>>();
  for (const sub of activeSubs) {
    if (sub.category === "UNKNOWN") continue;
    const email = sub.customerEmail.toLowerCase();
    if (!email) continue;
    if (!catEmails.has(sub.category)) catEmails.set(sub.category, new Set());
    catEmails.get(sub.category)!.add(email);
  }

  // 3. Get visit counts from registrations (last 90 days)
  const visitQuery = `
    SELECT
      LOWER(email) as email,
      COUNT(*) as visits
    FROM registrations
    WHERE attended_at IS NOT NULL AND attended_at != ''
      AND email IS NOT NULL AND email != ''
      AND state IN ('redeemed', 'confirmed')
      AND attended_at::date >= (CURRENT_DATE - INTERVAL '90 days')
    GROUP BY LOWER(email)
  `;
  const { rows: visitRows } = await pool.query(visitQuery);
  const visitMap = new Map<string, number>();
  for (const r of visitRows) {
    visitMap.set(r.email as string, Number(r.visits));
  }

  // 4. Build per-category visit arrays
  const byCategory = new Map<string, number[]>();
  for (const [cat, emails] of catEmails) {
    const visits: number[] = [];
    for (const email of emails) {
      visits.push(visitMap.get(email) ?? 0);
    }
    byCategory.set(cat, visits);
  }

  // Canonical count map for display (must match Growth exactly)
  const canonicalCountMap: Record<string, number> = {
    MEMBER: canonicalCounts.member,
    SKY3: canonicalCounts.sky3,
    SKY_TING_TV: canonicalCounts.skyTingTv,
  };

  const monthsInWindow = 3; // 90 days ≈ 3 months
  const categories: UsageCategoryData[] = [];
  let upgradeOpportunities = 0;

  for (const [cat, config] of Object.entries(CATEGORY_CONFIG)) {
    const visitsList = byCategory.get(cat) || [];
    // Use canonical count from getActiveCounts() — single source of truth
    const totalActive = canonicalCountMap[cat] ?? visitsList.length;
    if (totalActive === 0) continue;

    // Convert total visits to avg visits per month
    const avgPerMonth = visitsList.map(v => v / monthsInWindow);

    // Sort for median
    const sorted = [...avgPerMonth].sort((a, b) => a - b);
    const median = sorted.length > 0
      ? sorted.length % 2 === 0
        ? (sorted[sorted.length / 2 - 1] + sorted[sorted.length / 2]) / 2
        : sorted[Math.floor(sorted.length / 2)]
      : 0;
    const mean = sorted.length > 0
      ? sorted.reduce((s, v) => s + v, 0) / sorted.length
      : 0;

    // Count dormant (0 visits) and with visits
    const dormant = visitsList.filter(v => v === 0).length;
    const withVisits = totalActive - dormant;

    // Build segments
    const segments: UsageSegment[] = config.segments.map((seg) => {
      const count = avgPerMonth.filter(v => {
        if (seg.max === 0) return v === 0; // dormant = exactly 0
        return v > seg.min && v <= seg.max;
      }).length;
      return {
        name: seg.name,
        rangeLabel: seg.rangeLabel,
        count,
        percent: totalActive > 0 ? Math.round((count / totalActive) * 1000) / 10 : 0,
        color: seg.color,
      };
    });

    categories.push({
      category: cat,
      label: config.label,
      totalActive,
      withVisits,
      dormant,
      segments,
      median: Math.round(median * 10) / 10,
      mean: Math.round(mean * 10) / 10,
    });

    // Upgrade opportunities: Sky3 members averaging 3+/mo
    if (cat === "SKY3") {
      upgradeOpportunities = avgPerMonth.filter(v => v >= 3).length;
    }
  }

  return { categories, upgradeOpportunities };
}

// ── Per-Person Usage Detail (for CSV export) ────────────────

export interface UsageDetailRow {
  email: string;
  name: string;
  planName: string;
  segment: string;
  totalVisits: number;
  avgPerMonth: number;
}

/**
 * Get per-person usage detail for a given category, optionally filtered to a
 * single segment. Uses the exact same data sources and bucket logic as
 * getUsageFrequencyByCategory() above — just returns individual rows instead
 * of aggregated counts.
 */
export async function getUsageDetailByCategory(
  category: string,
  segment?: string,
): Promise<UsageDetailRow[]> {
  const { getActiveAutoRenews } = await import("./auto-renew-store");
  const pool = getPool();

  const config = CATEGORY_CONFIG[category];
  if (!config) return [];

  // 1. Get active subs, filter to this category, deduplicate by email
  const activeSubs = await getActiveAutoRenews();
  const personMap = new Map<string, { name: string; planName: string }>();
  for (const sub of activeSubs) {
    if (sub.category !== category) continue;
    const email = sub.customerEmail.toLowerCase();
    if (!email) continue;
    if (!personMap.has(email)) {
      personMap.set(email, { name: sub.customerName, planName: sub.planName });
    }
  }

  if (personMap.size === 0) return [];

  // 2. Get visit counts (same query as getUsageFrequencyByCategory)
  const visitQuery = `
    SELECT
      LOWER(email) as email,
      COUNT(*) as visits
    FROM registrations
    WHERE attended_at IS NOT NULL AND attended_at != ''
      AND email IS NOT NULL AND email != ''
      AND state IN ('redeemed', 'confirmed')
      AND attended_at::date >= (CURRENT_DATE - INTERVAL '90 days')
    GROUP BY LOWER(email)
  `;
  const { rows: visitRows } = await pool.query(visitQuery);
  const visitMap = new Map<string, number>();
  for (const r of visitRows) {
    visitMap.set(r.email as string, Number(r.visits));
  }

  const monthsInWindow = 3;

  // 3. Assign each person to their segment
  const rows: UsageDetailRow[] = [];
  for (const [email, person] of personMap) {
    const totalVisits = visitMap.get(email) ?? 0;
    const avg = totalVisits / monthsInWindow;

    // Find matching segment (same bucket logic as above)
    let segName = "Unknown";
    for (const seg of config.segments) {
      const match = seg.max === 0 ? avg === 0 : (avg > seg.min && avg <= seg.max);
      if (match) {
        segName = seg.name;
        break;
      }
    }

    if (segment && segName !== segment) continue;

    rows.push({
      email,
      name: person.name,
      planName: person.planName,
      segment: segName,
      totalVisits,
      avgPerMonth: Math.round(avg * 10) / 10,
    });
  }

  // 4. Sort by segment order, then by name within segment
  const segOrder = new Map(config.segments.map((s, i) => [s.name, i]));
  rows.sort((a, b) => {
    const oa = segOrder.get(a.segment) ?? 999;
    const ob = segOrder.get(b.segment) ?? 999;
    if (oa !== ob) return oa - ob;
    return a.name.localeCompare(b.name);
  });

  return rows;
}
