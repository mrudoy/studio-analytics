import { describe, it, expect } from "vitest";
import { deriveAlerts, type DriftMetrics, DEFAULT_DRIFT_THRESHOLDS } from "./drift-check";

function metrics(o: Partial<DriftMetrics> = {}): DriftMetrics {
  return {
    activeMember: 507, activeSky3: 382, activeTv: 2043, activeTotal: 2932,
    dupActiveIdentities: 0, futureDatedRows: 0,
    unknownActivePlans: 0, monthsMissingChurnEvents: 0, revenueNetExceedsGross: 0,
    lastFullSyncAgeDays: 1,
    ...o,
  };
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

  it("ALERTs when a completed month has cancels but no churn events", () => {
    const r = deriveAlerts(metrics({ monthsMissingChurnEvents: 2 }), 2932);
    expect(r.status).toBe("alert");
    expect(r.alerts.join(" ")).toMatch(/no churn events/i);
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
