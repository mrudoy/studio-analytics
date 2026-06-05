/**
 * Convert a Union.fit timestamp to its **America/New_York calendar date** (YYYY-MM-DD).
 *
 * WHY THIS EXISTS: `auto_renews.created_at` (and canceled_at / pending_canceled_at)
 * are Postgres DATE columns. Union emits Eastern timestamps with an offset, e.g.
 * `2026-01-16 23:30:00 -0500`. When such a string is handed to a DATE column in a
 * UTC session, Postgres converts to UTC first (`2026-01-17 04:30 UTC`) and truncates
 * → DATE `2026-01-17` — a systematic **+1 day** skew for evening-ET timestamps. That
 * skew silently broke dedup (renewals spawned duplicate rows) and broke any reconcile
 * keyed on `created_date`. Storing the Eastern calendar date up front makes our dates
 * match Union's exactly.
 *
 * Guarantees:
 *  - A bare `YYYY-MM-DD` (already a calendar date, no time/offset) is returned AS-IS.
 *    (Re-interpreting it through a timezone would shift it the other way.)
 *  - A timestamp WITH a time/offset is converted to the ET calendar date.
 *  - null/empty/unparseable → falls back to the leading YYYY-MM-DD if present, else null.
 */

const ET_DATE_FMT = new Intl.DateTimeFormat("en-CA", {
  timeZone: "America/New_York",
  year: "numeric",
  month: "2-digit",
  day: "2-digit",
});

export function toEasternDate(ts: string | null | undefined): string | null {
  if (ts == null) return null;
  const s = String(ts).trim();
  if (!s) return null;

  // Already a bare calendar date (no time component) → trust it, do NOT re-tz it.
  if (/^\d{4}-\d{2}-\d{2}$/.test(s)) return s;

  const ms = Date.parse(s);
  if (Number.isNaN(ms)) {
    // Unparseable: salvage a leading YYYY-MM-DD if the string starts with one.
    const lead = s.match(/^(\d{4}-\d{2}-\d{2})/);
    return lead ? lead[1] : null;
  }
  // en-CA formats as YYYY-MM-DD; timeZone pins it to the Eastern calendar day.
  return ET_DATE_FMT.format(new Date(ms));
}
