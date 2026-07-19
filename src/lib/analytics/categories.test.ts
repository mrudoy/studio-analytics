import { describe, it, expect } from "vitest";
import { getCategory, isNonSubscriptionPlan } from "./categories";

describe("isNonSubscriptionPlan", () => {
  // Every non-membership installment plan seen in 26 months of Union data.
  const NON_SUB_PLANS = [
    "TT 4 MONTH PAYMENT PLAN",
    "TT 5 MONTH PAYMENT PLAN",
    "6M Payment Plan TT",
    "6M Payment Plan TT 2025",
    "3M Payment Plan TT",
    "3M Partial Payment Plan 200 Hour TT",
    "TT $700/month",
    "TT $775 Payment Plan",
    "TT for Audrey",
    "TT For Erin",
    "KODY TT 2.0",
    "Mentorship Payment Plan",
    "Early Bird 3 Month Payment Plan",
    "EARLY BIRD 6 MONTH PAYMENT PLAN",
    "GREECE 2026 2 PAYMENT PLAN",
    "GREECE 2026 3 PAYMENT PLAN",
  ];

  for (const plan of NON_SUB_PLANS) {
    it(`flags "${plan}" as a non-subscription installment plan`, () => {
      expect(isNonSubscriptionPlan(plan)).toBe(true);
    });
  }

  // Real memberships must NEVER be flagged — that would drop live subscribers.
  const MEMBERSHIPS = [
    "SKY UNLIMITED",
    "SKY UNLIMITED ANNUAL",
    "ALL ACCESS MONTHLY",
    "SKY TING Monthly Membership",
    "TING FAM",
    "SKY3",
    "SKYHIGH3",
    "SKY5",
    "5 Pack",
    "SKY TING TV",
    "SKY TING TV ANNUAL",
    "FABxSKYTING",
    "FRIENDS OF SKY TING TV",
  ];

  for (const plan of MEMBERSHIPS) {
    it(`does NOT flag membership "${plan}"`, () => {
      expect(isNonSubscriptionPlan(plan)).toBe(false);
    });
  }

  it("does not match 'TT' embedded inside another token", () => {
    // Word-boundary guard: no false positive from a substring 'tt'.
    expect(isNonSubscriptionPlan("SETTLE UNLIMITED")).toBe(false);
    expect(isNonSubscriptionPlan("SHUTTLE PASS")).toBe(false);
  });

  it("is case- and whitespace-insensitive", () => {
    expect(isNonSubscriptionPlan("  tt 4 month payment plan  ")).toBe(true);
  });

  it("the flagged non-subscription plans all categorize to UNKNOWN today", () => {
    // Confirms the two mechanisms are aligned: these plans match no membership
    // keyword, so excluding them removes exactly the UNKNOWN-bucket leak.
    for (const plan of NON_SUB_PLANS) {
      expect(getCategory(plan)).toBe("UNKNOWN");
    }
  });
});
