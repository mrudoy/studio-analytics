import type { SubscriptionCategory } from "@/types/union-data";

/**
 * Mapping of Union.fit auto-renew plan names to subscription categories.
 * Source: uploaded CSV "union subscriptions by category"
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
];

// Build a lookup map for O(1) lookups (case-insensitive)
const planCategoryMap = new Map<string, SubscriptionCategory>();

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
 * Get the subscription category for a given plan name.
 * Uses case-insensitive matching with fuzzy fallback.
 */
export function getCategory(planName: string): SubscriptionCategory {
  const upper = planName.trim().toUpperCase();

  // Exact match
  const exact = planCategoryMap.get(upper);
  if (exact) return exact;

  // Fuzzy match: check if the plan name contains a known category keyword
  if (upper.includes("SKY3") || upper.includes("SKY5") || upper.includes("SKYHIGH")) return "SKY3";
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
 * Check if a pass/plan name represents a drop-in or intro type visit.
 */
export function isDropInOrIntro(passName: string): boolean {
  const upper = passName.trim().toUpperCase();
  return (
    upper.includes("DROP-IN") ||
    upper.includes("DROP IN") ||
    upper.includes("DROPIN") ||
    upper.includes("INTRO") ||
    upper.includes("TRIAL") ||
    upper.includes("FIRST") ||
    upper.includes("SINGLE CLASS") ||
    upper.includes("WELLHUB")
  );
}
