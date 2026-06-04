import { describe, it, expect } from "vitest";
import {
  summarizeExport,
  runPreflight,
  computeReconcileDiff,
  type CategoryCounts,
  type ReconcileDiff,
} from "./reconcile";
import type { AutoRenewRow } from "./auto-renew-store";

function row(o: Partial<AutoRenewRow>): AutoRenewRow {
  return {
    planName: "SKY UNLIMITED",
    planState: "Valid Now",
    planPrice: 0,
    customerName: "",
    customerEmail: "a@b.com",
    createdAt: "2026-01-01",
    ...o,
  };
}

function counts(o: Partial<CategoryCounts> = {}): CategoryCounts {
  return { member: 0, sky3: 0, skyTingTv: 0, unknown: 0, total: 0, ...o };
}

function diff(o: Partial<ReconcileDiff> = {}): ReconcileDiff {
  return {
    dbCounts: counts(),
    exportCounts: counts(),
    candidates: [],
    candidateCounts: counts(),
    exportOnlyActiveCount: 0,
    protectedCount: 0,
    unknownActiveCount: 0,
    matchRate: 1,
    ...o,
  };
}

// Build a fake Queryable returning canned DB-active rows.
function fakeQ(rows: Record<string, unknown>[]) {
  return { query: async () => ({ rows }) } as never;
}

describe("summarizeExport", () => {
  it("buckets active rows by category and builds tuples", () => {
    const exp = summarizeExport([
      row({ planName: "SKY UNLIMITED", customerEmail: "A@B.com", createdAt: "2026-01-01" }),
      row({ planName: "SKY3", customerEmail: "c@d.com", createdAt: "2026-02-02" }),
      row({ planName: "SKY TING TV", customerEmail: "e@f.com", createdAt: "2026-03-03" }),
    ]);
    expect(exp.counts.member).toBe(1);
    expect(exp.counts.sky3).toBe(1);
    expect(exp.counts.skyTingTv).toBe(1);
    // tuple is lowercased + trimmed
    expect(exp.activeTuples.has("a@b.com|SKY UNLIMITED|2026-01-01")).toBe(true);
  });

  it("excludes Canceled and non-active current_state; includes Past Due", () => {
    const exp = summarizeExport([
      row({ planState: "Canceled" }),
      row({ planState: "Valid Now", currentState: "canceled" }),
      row({ planState: "Past Due", customerEmail: "pd@x.com" }),
    ]);
    expect(exp.counts.total).toBe(1); // only Past Due survives
  });

  it("flags unmapped plans as unknown", () => {
    const exp = summarizeExport([row({ planName: "Mentorship: Foo", customerEmail: "x@y.com" })]);
    expect(exp.counts.unknown).toBe(1);
    expect(exp.unknownActive).toHaveLength(1);
  });

  it("idempotent: summarizing twice yields identical counts", () => {
    const rows = [row({ customerEmail: "a@b.com" }), row({ planName: "SKY3", customerEmail: "c@d.com" })];
    const a = summarizeExport(rows);
    const b = summarizeExport(rows);
    expect(b.counts).toEqual(a.counts);
    expect([...b.activeTuples].sort()).toEqual([...a.activeTuples].sort());
  });
});

describe("computeReconcileDiff", () => {
  it("classifies absent rows as cancel candidates and computes match rate", async () => {
    const exp = summarizeExport([row({ customerEmail: "keep@x.com", createdAt: "2026-01-01" })]);
    const d = await computeReconcileDiff(
      exp,
      Date.parse("2026-06-01"),
      fakeQ([
        // matches export → kept
        { id: 1, email: "keep@x.com", plan_name: "SKY UNLIMITED", plan_category: "MEMBER", created_date: "2026-01-01", created_at: "2026-01-01", imported_at: "2026-01-01" },
        // absent from export, created before cutoff → cancel candidate
        { id: 2, email: "ghost@x.com", plan_name: "SKY UNLIMITED", plan_category: "MEMBER", created_date: "2024-01-01", created_at: "2024-01-01", imported_at: "2024-01-01" },
      ]),
    );
    expect(d.candidates.map((c) => c.id)).toEqual([2]);
    expect(d.matchRate).toBeCloseTo(0.5);
  });

  it("protects rows created/imported after the report cutoff", async () => {
    const exp = summarizeExport([]); // empty export → everything would otherwise be a candidate
    const d = await computeReconcileDiff(
      exp,
      Date.parse("2026-06-01"),
      fakeQ([
        { id: 9, email: "new@x.com", plan_name: "SKY3", plan_category: "SKY3", created_date: "2026-06-10", created_at: "2026-06-10", imported_at: "2026-06-10" },
      ]),
    );
    expect(d.candidates).toHaveLength(0);
    expect(d.protectedCount).toBe(1);
  });
});

describe("runPreflight gates", () => {
  const exp = summarizeExport([row({ customerEmail: "a@b.com" })]);

  it("passes when everything is clean", () => {
    const r = runPreflight(exp, diff({ matchRate: 1, candidates: [] }), {
      expectedCounts: { member: 1, sky3: 0, skyTingTv: 0 },
    });
    expect(r.ok).toBe(true);
  });

  it("aborts on per-category count mismatch (e.g. export omits Invalid)", () => {
    const r = runPreflight(exp, diff(), { expectedCounts: { member: 506, sky3: 382, skyTingTv: 2043 } });
    expect(r.ok).toBe(false);
    expect(r.failures.join(" ")).toMatch(/per-category/i);
  });

  it("aborts on low match rate", () => {
    const r = runPreflight(exp, diff({ matchRate: 0.5 }), {});
    expect(r.ok).toBe(false);
    expect(r.failures.join(" ")).toMatch(/match/i);
  });

  it("aborts when candidates exceed threshold without --force, passes with it", () => {
    const many = diff({ candidates: Array.from({ length: 50 }, (_, i) => ({ id: i, email: "x", planName: "p", category: "MEMBER" })) });
    expect(runPreflight(exp, many, { maxCandidates: 10 }).ok).toBe(false);
    expect(runPreflight(exp, many, { maxCandidates: 10, force: true }).ok).toBe(true);
  });

  it("aborts on parse failures and on a zero-active export", () => {
    expect(runPreflight(exp, diff(), { parseFailures: 3 }).ok).toBe(false);
    expect(runPreflight(summarizeExport([]), diff(), {}).ok).toBe(false);
  });

  it("aborts when too many active rows map to UNKNOWN (new unmapped plans)", () => {
    const unknownExp = summarizeExport(
      Array.from({ length: 12 }, (_, i) => row({ planName: `New Mystery Plan ${i}`, customerEmail: `u${i}@x.com` })),
    );
    const r = runPreflight(unknownExp, diff(), { maxUnknownActive: 10 });
    expect(r.ok).toBe(false);
    expect(r.failures.join(" ")).toMatch(/UNKNOWN/);
  });
});
