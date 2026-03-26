/**
 * Quarterly churn reduction targets from the 2026 Goals deck.
 * Stored as configuration so they can be updated without code changes.
 */

export type ChurnTier = "member" | "sky3" | "tv";

export interface QuarterlyChurnGoal {
  year: number;
  quarter: 1 | 2 | 3 | 4;
  member: number; // monthly % target (eligibleChurnRate for members)
  sky3: number; // monthly % target (userChurnRate)
  tv: number; // monthly % target (userChurnRate)
}

/**
 * Quarterly targets — corrected March 2026 baseline.
 * Q1 is the corrected baseline (no reduction). Q2-Q4 step down.
 */
export const CHURN_GOALS: QuarterlyChurnGoal[] = [
  { year: 2026, quarter: 1, member: 6.7, sky3: 21.1, tv: 6.9 },
  { year: 2026, quarter: 2, member: 6.0, sky3: 17.7, tv: 6.0 },
  { year: 2026, quarter: 3, member: 5.4, sky3: 14.9, tv: 5.3 },
  { year: 2026, quarter: 4, member: 4.9, sky3: 12.5, tv: 4.6 },
];

/** Map month (1-12) to quarter (1-4). */
function monthToQuarter(month: number): 1 | 2 | 3 | 4 {
  return Math.ceil(month / 3) as 1 | 2 | 3 | 4;
}

/**
 * Get the monthly churn target for a given YYYY-MM string and tier.
 * Returns null if no goal is configured for that quarter.
 */
export function getMonthlyGoal(monthStr: string, tier: ChurnTier): number | null {
  const [yearStr, moStr] = monthStr.split("-");
  const year = parseInt(yearStr);
  const quarter = monthToQuarter(parseInt(moStr));
  const entry = CHURN_GOALS.find((g) => g.year === year && g.quarter === quarter);
  return entry ? entry[tier] : null;
}

/**
 * Convert a monthly churn target to weekly equivalent.
 * Formula: weekly = (1 - (1 - monthly/100)^(7/30)) * 100
 */
export function getWeeklyGoal(monthStr: string, tier: ChurnTier): number | null {
  const monthly = getMonthlyGoal(monthStr, tier);
  if (monthly === null) return null;
  const weekly = (1 - Math.pow(1 - monthly / 100, 7 / 30)) * 100;
  return parseFloat(weekly.toFixed(1));
}

/**
 * Get the current quarter's goal for a tier (based on today's date in ET).
 */
export function getCurrentQuarterGoal(tier: ChurnTier): number | null {
  const now = new Date();
  const etStr = now.toLocaleDateString("en-CA", { timeZone: "America/New_York" });
  return getMonthlyGoal(etStr.slice(0, 7), tier);
}

/**
 * Get quarter label for a YYYY-MM string, e.g. "Q1" or "Q2 2026".
 */
export function getQuarterLabel(monthStr: string, includeYear = false): string {
  const [yearStr, moStr] = monthStr.split("-");
  const q = monthToQuarter(parseInt(moStr));
  return includeYear ? `Q${q} ${yearStr}` : `Q${q}`;
}

/**
 * Days remaining in the quarter containing the given YYYY-MM string.
 * Uses today's date for the current quarter, or 0 for past quarters.
 */
export function daysRemainingInQuarter(monthStr: string): number {
  const [yearStr, moStr] = monthStr.split("-");
  const year = parseInt(yearStr);
  const quarter = monthToQuarter(parseInt(moStr));
  // Quarter end: last day of month 3, 6, 9, or 12
  const endMonth = quarter * 3;
  const quarterEnd = new Date(year, endMonth, 0); // last day of end month

  const now = new Date();
  const todayET = new Date(now.toLocaleDateString("en-CA", { timeZone: "America/New_York" }));

  const diff = Math.ceil((quarterEnd.getTime() - todayET.getTime()) / (24 * 60 * 60 * 1000));
  return Math.max(0, diff);
}
