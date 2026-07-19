import { describe, it, expect } from "vitest";
import { getCategory, isNonSubscriptionPlan, nonSubscriptionPlanSql } from "./categories";

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

describe("isNonSubscriptionPlan", () => {
  for (const plan of NON_SUB_PLANS) {
    it(`flags "${plan}" as a non-subscription installment plan`, () => {
      expect(isNonSubscriptionPlan(plan)).toBe(true);
    });
  }

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

describe("nonSubscriptionPlanSql (SQL twin of isNonSubscriptionPlan)", () => {
  // getMonthlySubscriptionBilling() must equal getAutoRenewStats() MRR to the
  // penny, so the SQL predicate and the TS predicate must classify identically.
  // This emulates the Postgres semantics of the generated SQL in JS: UPPER(TRIM(x))
  // then the same four branches (\y is Postgres's \b).
  function evalSql(planName: string): boolean {
    const sql = nonSubscriptionPlanSql("plan_name");
    const upper = planName.trim().toUpperCase();
    const likes = [...sql.matchAll(/LIKE '%([^%]+)%'/g)].map((m) => m[1]);
    // Postgres `\y` word boundary → JS `\b`. The SQL string holds a literal
    // backslash-y, so match a literal backslash here.
    const regexes = [...sql.matchAll(/~ '\\y([A-Z]+)\\y'/g)].map((m) => m[1]);
    // Guard: if the SQL shape changes so neither branch parses, fail loudly
    // rather than silently passing every case.
    expect(likes.length).toBeGreaterThan(0);
    expect(regexes.length).toBeGreaterThan(0);
    return (
      likes.some((l) => upper.includes(l)) ||
      regexes.some((r) => new RegExp(`\\b${r}\\b`).test(upper))
    );
  }

  const ALL_CASES = [
    ...NON_SUB_PLANS,
    ...MEMBERSHIPS,
    "SETTLE UNLIMITED",
    "SHUTTLE PASS",
    "  tt 4 month payment plan  ",
  ];

  for (const plan of ALL_CASES) {
    it(`agrees with the TS predicate for "${plan}"`, () => {
      expect(evalSql(plan)).toBe(isNonSubscriptionPlan(plan));
    });
  }

  it("references the column expression it is given", () => {
    expect(nonSubscriptionPlanSql("ar.plan_name")).toContain("ar.plan_name");
  });
});
