import { describe, it, expect } from "vitest";
import {
  deriveAlerts, countZeroChurnCompletedMonths,
  type DriftMetrics, type MonthlyChurnSlice, DEFAULT_DRIFT_THRESHOLDS,
} from "./drift-check";

function metrics(o: Partial<DriftMetrics> = {}): DriftMetrics {
  return {
    activeMember: 507, activeSky3: 382, activeTv: 2043, activeTotal: 2932,
    dupActiveIdentities: 0, futureDatedRows: 0,
    unknownActivePlans: 0, zeroChurnCompletedMonths: 0, churnSourceOk: true,
    revenueNetExceedsGross: 0,
    lastFullSyncAgeDays: 1,
    ...o,
  };
}

// Builder for a monthly churn slice; healthy by default (every category churns).
function mo(period: string, o: Partial<Record<"member" | "sky3" | "skyTingTv", MonthlyChurnSlice["member"]>> = {}): MonthlyChurnSlice {
  const def = { activeAtStart: 500, canceled: 10 };
  return { period, member: { ...def, ...o.member }, sky3: { ...def, ...o.sky3 }, skyTingTv: { ...def, ...o.skyTingTv } };
}

describe("deriveAlerts", () => {
  it("ok when all invariants hold and total is stable", () => {
    const r = deriveAlerts(metrics(), 2930);
    expect(r.status).toBe("ok");
    expect(r.alerts).toHaveLength(0);
  });

  it("ALERTs on duplicate active identities (inflation recurrence)", () => {
    const r = deriveAlerts(metrics({ dupActiveIdentities: 12 }), 2932);
    expect(r.status).toBe("alert");
    expect(r.alerts.join(" ")).toMatch(/duplicate active/i);
  });

  it("ALERTs on future-dated rows (TZ skew tripwire)", () => {
    const r = deriveAlerts(metrics({ futureDatedRows: 3 }), 2932);
    expect(r.status).toBe("alert");
    expect(r.alerts.join(" ")).toMatch(/future/i);
  });

  it("ALERTs when an active plan maps to UNKNOWN (rename dropping from counts)", () => {
    const r = deriveAlerts(metrics({ unknownActivePlans: 7 }), 2932);
    expect(r.status).toBe("alert");
    expect(r.alerts.join(" ")).toMatch(/UNKNOWN category/i);
  });

  it("ALERTs when a completed month renders zero churn (history regression)", () => {
    const r = deriveAlerts(metrics({ zeroChurnCompletedMonths: 2 }), 2932);
    expect(r.status).toBe("alert");
    expect(r.alerts.join(" ")).toMatch(/0 churn/i);
  });

  it("WARNs (not silently passes) when the churn source can't be evaluated", () => {
    const r = deriveAlerts(metrics({ churnSourceOk: false }), 2932);
    expect(r.status).toBe("warning");
    expect(r.alerts.join(" ")).toMatch(/could not be evaluated/i);
  });

  it("ALERTs when revenue net exceeds gross", () => {
    const r = deriveAlerts(metrics({ revenueNetExceedsGross: 1 }), 2932);
    expect(r.status).toBe("alert");
    expect(r.alerts.join(" ")).toMatch(/net > gross/i);
  });

  it("WARNs when the full reconcile is overdue", () => {
    const r = deriveAlerts(metrics({ lastFullSyncAgeDays: 14 }), 2932);
    expect(r.status).toBe("warning");
    expect(r.alerts.join(" ")).toMatch(/reconcile was 14 days/i);
  });

  it("WARNs on a large active-total jump", () => {
    const r = deriveAlerts(metrics({ activeTotal: 3300 }), 2932); // +368 > max(300, 293)
    expect(r.status).toBe("warning");
    expect(r.alerts.join(" ")).toMatch(/Active total moved/);
  });

  it("does not warn on a normal day-over-day change or the first run", () => {
    expect(deriveAlerts(metrics({ activeTotal: 2950 }), 2932).status).toBe("ok"); // +18
    expect(deriveAlerts(metrics(), null).status).toBe("ok"); // first run, no prev
  });

  it("hard alert wins over a soft warning", () => {
    const r = deriveAlerts(metrics({ dupActiveIdentities: 1, lastFullSyncAgeDays: 30 }), 2932);
    expect(r.status).toBe("alert");
    expect(r.alerts.length).toBe(2);
  });
});

describe("countZeroChurnCompletedMonths", () => {
  it("returns 0 when every completed month has churn in every category", () => {
    const series = [mo("2026-01"), mo("2026-02"), mo("2026-03"), mo("2026-04") /* current/partial */];
    expect(countZeroChurnCompletedMonths(series)).toBe(0);
  });

  it("counts a completed month where ONE category renders 0 churn", () => {
    const series = [
      mo("2026-01"),
      mo("2026-02", { sky3: { activeAtStart: 380, canceled: 0 } }), // sky3 zero → broken
      mo("2026-03"),
      mo("2026-04"),
    ];
    expect(countZeroChurnCompletedMonths(series)).toBe(1);
  });

  it("ignores the partial current month (last element) even if it has 0 churn", () => {
    const series = [mo("2026-01"), mo("2026-02"), mo("2026-03"),
      mo("2026-04", { member: { activeAtStart: 500, canceled: 0 }, sky3: { activeAtStart: 380, canceled: 0 }, skyTingTv: { activeAtStart: 2000, canceled: 0 } })];
    expect(countZeroChurnCompletedMonths(series)).toBe(0);
  });

  it("excludes the 2025-10 bulk-cleanup month", () => {
    const series = [
      mo("2025-10", { member: { activeAtStart: 500, canceled: 0 } }), // would be flagged, but excluded
      mo("2025-11"), mo("2025-12"), mo("2026-01"),
    ];
    expect(countZeroChurnCompletedMonths(series)).toBe(0);
  });

  it("does not flag a category with zero active subscribers", () => {
    const series = [
      mo("2026-01", { skyTingTv: { activeAtStart: 0, canceled: 0 } }), // 0 active → not a regression
      mo("2026-02"), mo("2026-03"),
    ];
    expect(countZeroChurnCompletedMonths(series)).toBe(0);
  });

  it("handles short arrays without throwing", () => {
    expect(countZeroChurnCompletedMonths([])).toBe(0);
    expect(countZeroChurnCompletedMonths([mo("2026-06")])).toBe(0); // only the partial month
  });
});
