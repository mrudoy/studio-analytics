/**
 * Shared revenue category inference utility.
 *
 * Used by both zip-transformer.ts (CSV-based computation) and
 * db-revenue.ts (DB-based 7-step algorithm) to resolve revenue
 * categories from pass/order names when lookup table resolution fails.
 */

/**
 * Infer a revenue category from a pass or order name using regex patterns.
 * Returns null if no pattern matches (caller should default to "Uncategorized").
 *
 * Patterns are ordered most-specific-first to avoid false matches
 * (e.g., "SKY3" before generic "member").
 */
export function inferCategoryFromName(name: string): string | null {
  const n = name.toLowerCase();

  // Specific patterns first (more specific before less specific)
  if (/sky\s*3|sky\s*three|3.?pack/i.test(n)) return "SKY3 / Packs";
  if (/sky\s*ting\s*tv|sttv|retreat\s*ting/i.test(n)) return "SKY TING TV";
  if (/intro|trial/i.test(n)) return "Intro / Trial";
  if (/member/i.test(n) && !/sky\s*3|sky\s*ting\s*tv/i.test(n)) return "Members";
  if (/drop.?in|single\s*class/i.test(n)) return "Drop-Ins";
  if (/workshop/i.test(n)) return "Workshops";
  if (/spa|wellness|massage|facial/i.test(n)) return "Wellness / Spa";
  if (/teacher\s*training|tt\b|training/i.test(n)) return "Teacher Training";
  if (/retail|merch|merchandise|shop/i.test(n)) return "Retail / Merch";
  if (/private|1.on.1|one.on.one/i.test(n)) return "Privates";
  if (/donat/i.test(n)) return "Donations";
  if (/rental|rent/i.test(n)) return "Rentals";
  if (/retreat/i.test(n)) return "Retreats";
  if (/community/i.test(n)) return "Community";

  return null;
}
