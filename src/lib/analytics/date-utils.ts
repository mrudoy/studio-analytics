import { startOfWeek, parseISO, isValid } from "date-fns";

/**
 * Parse dates from various Union.fit formats:
 *
 * HTML scraped:  "Thu, 2/12/26 2:00 PM EST"  or  "2/12/26 4:23 PM"
 * Direct CSV:    "2024-01-28 22:46:51 -0500"
 * ISO:           "2024-01-28T22:46:51Z"
 */
export function parseDate(dateStr: string): Date | null {
  if (!dateStr) return null;

  const cleaned = dateStr
    .replace(/^[A-Za-z]+,\s*/, "") // Remove day name prefix
    .replace(/\s*(EST|EDT|CST|CDT|MST|MDT|PST|PDT)\s*$/i, "") // Remove timezone abbrev
    .trim();

  // Format 1: MM/DD/YY(YY) HH:MM (AM/PM) — HTML scraped dates
  const mmddMatch = cleaned.match(
    /(\d{1,2})\/(\d{1,2})\/(\d{2,4})\s+(\d{1,2}):(\d{2})\s*(AM|PM)?/i
  );
  if (mmddMatch) {
    let year = parseInt(mmddMatch[3]);
    if (year < 100) year += 2000;
    const month = parseInt(mmddMatch[1]) - 1;
    const day = parseInt(mmddMatch[2]);
    let hour = parseInt(mmddMatch[4]);
    const min = parseInt(mmddMatch[5]);
    const ampm = mmddMatch[6]?.toUpperCase();
    if (ampm === "PM" && hour < 12) hour += 12;
    if (ampm === "AM" && hour === 12) hour = 0;
    const d = new Date(year, month, day, hour, min);
    if (isValid(d)) return d;
  }

  // Format 2: YYYY-MM-DD HH:MM:SS ±HHMM — Direct CSV dates
  // Convert to ISO 8601 by replacing space with T and formatting timezone
  const csvMatch = cleaned.match(
    /(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2})\s*([+-]\d{4})?/
  );
  if (csvMatch) {
    const datePart = csvMatch[1];
    const timePart = csvMatch[2];
    const tz = csvMatch[3];
    // Format timezone: -0500 → -05:00
    const tzFormatted = tz
      ? `${tz.slice(0, 3)}:${tz.slice(3)}`
      : "";
    const isoStr = `${datePart}T${timePart}${tzFormatted}`;
    const d = parseISO(isoStr);
    if (isValid(d)) return d;
  }

  // Format 3: Standard ISO 8601
  const iso = parseISO(dateStr);
  if (isValid(iso)) return iso;

  // Format 4: MM/DD/YYYY with no time
  const dateOnlyMatch = cleaned.match(/(\d{1,2})\/(\d{1,2})\/(\d{2,4})/);
  if (dateOnlyMatch) {
    let year = parseInt(dateOnlyMatch[3]);
    if (year < 100) year += 2000;
    const d = new Date(year, parseInt(dateOnlyMatch[1]) - 1, parseInt(dateOnlyMatch[2]));
    if (isValid(d)) return d;
  }

  return null;
}

export function getWeekKey(date: Date): string {
  const weekStart = startOfWeek(date, { weekStartsOn: 1 });
  return `${weekStart.getFullYear()}-${String(weekStart.getMonth() + 1).padStart(2, "0")}-${String(weekStart.getDate()).padStart(2, "0")}`;
}

export function getMonthKey(date: Date): string {
  return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, "0")}`;
}
