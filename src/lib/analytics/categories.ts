import type { AutoRenewCategory } from "@/types/union-data";

/**
 * Mapping of Union.fit auto-renew plan names to categories.
 * Source: uploaded CSV "union auto-renews by category"
 */
const MEMBER_PLANS = [
  "SKY UNLIMITED",
  "SKY UNLIMITED ANNUAL",
  "10MEMBER",
  "SKY UNLIMITED - NEW",
  "ALL ACCESS MONTHLY",
  "ALL ACCESS YEARLY",
  "BACK TO SCHOOL SPECIAL",
  "Monthly Membership Special",
  "NEW MEMBER FALL SPECIAL",
  "NEW MEMBER SUMMER SPECIAL",
  "Secret membership",
  "SKY TING In Person Membership",
  "SKY VIRGIN MEMBERSHIP",
  "TING FAM",
  "SKY TING Monthly Membership",
  "Founding Member Annual",
];

const SKY_TING_TV_PLANS = [
  "SKY TING TV",
  "SKY TING TV NEW",
  "SKY TING TV ANNUAL",
  "10SKYTING",
  "SUNLIFE x SKY TING",
  "FRIENDS OF SKY TING TV",
  "COME BACK SKY TING TV",
  "Founding Member Annual SKY TING TV",
  "Limited Edition SKY TING TV",
  "NEW SUBSCRIBER SPECIAL",
  "SKY TING TV - Unlimited Monthly",
  "SKY TING TV (VIRGIN)",
  "SKY TING TV On Demand",
  "SKY TING TV Unlimited Yearly",
  "WE LOVE LA - SKY TING TV",
  "RETREAT TING",
  "SKY WEEK TV",
];

const SKY3_PLANS = [
  "SKY3",
  "SKY3 NEW",
  "SKY3 - ARCHIVED",
  "SKY3 VIRGIN",
  "Welcome SKY3",
  "SKYHIGH3",
  "SKY5",
  "SKY5 NEW",
  "5 Pack",
  "5-Pack",
];

// Build a lookup map for O(1) lookups (case-insensitive)
const planCategoryMap = new Map<string, AutoRenewCategory>();

for (const plan of MEMBER_PLANS) {
  planCategoryMap.set(plan.toUpperCase(), "MEMBER");
}
for (const plan of SKY_TING_TV_PLANS) {
  planCategoryMap.set(plan.toUpperCase(), "SKY_TING_TV");
}
for (const plan of SKY3_PLANS) {
  planCategoryMap.set(plan.toUpperCase(), "SKY3");
}

/**
 * Get the auto-renew category for a given plan name.
 * Uses case-insensitive matching with fuzzy fallback.
 */
export function getCategory(planName: string): AutoRenewCategory {
  const upper = planName.trim().toUpperCase();

  // Exact match
  const exact = planCategoryMap.get(upper);
  if (exact) return exact;

  // Fuzzy match: check if the plan name contains a known category keyword
  if (upper.includes("SKY3") || upper.includes("SKY5") || upper.includes("SKYHIGH") || upper.includes("5 PACK") || upper.includes("5-PACK")) return "SKY3";
  if (upper.includes("SKY TING TV") || upper.includes("SKYTING TV")) return "SKY_TING_TV";
  if (
    upper.includes("UNLIMITED") ||
    upper.includes("MEMBER") ||
    upper.includes("ALL ACCESS") ||
    upper.includes("TING FAM")
  )
    return "MEMBER";

  return "UNKNOWN";
}

/**
 * Detect whether an auto-renew plan bills annually (vs monthly).
 * Annual plans store the full yearly price, so MRR = price / 12.
 *
 * Known annual plans from the SKY TING pricing page:
 *   - SKY UNLIMITED ANNUAL   → $2,300/yr
 *   - ALL ACCESS YEARLY       → $2,300/yr
 *   - SKY TING TV ANNUAL      → $390/yr
 *   - Founding Member Annual SKY TING TV
 *   - SKY TING TV Unlimited Yearly
 */
export function isAnnualPlan(planName: string): boolean {
  const upper = planName.trim().toUpperCase();
  return (
    upper.includes("ANNUAL") ||
    upper.includes("YEARLY") ||
    upper.includes("12 MONTH") ||
    upper.includes("12-MONTH") ||
    upper.includes("12M ")
  );
}

/**
 * Check if a pass/plan name represents a non-membership visit type
 * (drop-in, droplet, intro, guest, community pass, etc.).
 *
 * Based on the marketing team's conversion funnel methodology:
 *   Intro Week, Exclusive Intro Week, Guest pass, Member guest,
 *   Community Day, On Running Guest, Poker chip, Teacher's guest,
 *   Droplet, drop-in
 *
 * Note: 5 Packs are classified as SKY3 (not drop-in).
 */
export function isDropInOrIntro(passName: string): boolean {
  const upper = passName.trim().toUpperCase();
  return (
    upper.includes("DROP-IN") ||
    upper.includes("DROP IN") ||
    upper.includes("DROPIN") ||
    upper.includes("DROPLET") ||
    upper.includes("INTRO") ||
    upper.includes("TRIAL") ||
    upper.includes("FIRST") ||
    upper.includes("SINGLE CLASS") ||
    upper.includes("WELLHUB") ||
    upper.includes("GUEST") ||
    upper.includes("COMMUNITY DAY") ||
    upper.includes("POKER CHIP") ||
    upper.includes("ON RUNNING")
  );
}
