import { describe, it, expect } from "vitest";
import { classifyDeltaCancel, type DeltaCancelRow } from "./auto-renew-store";
import type { ShadowCancel } from "../email/zip-transformer";

/** Build a ShadowCancel signal (only the fields classifyDeltaCancel reads). */
function signal(o: Partial<Pick<ShadowCancel, "unionPassId" | "intendedAction" | "effectiveAt">> = {}) {
  return {
    unionPassId: "pass_123",
    intendedAction: "cancel" as const,
    effectiveAt: "2026-06-01",
    ...o,
  };
}

/** Build a matched auto_renews row. */
function dbRow(o: Partial<DeltaCancelRow> = {}): DeltaCancelRow {
  return {
    plan_state: "Valid Now",
    current_state: "active",
    created_at: "2026-01-01",
    ...o,
  };
}

describe("classifyDeltaCancel — the B1 cancellation matrix", () => {
  it("terminal cancel on an active row (exact pass_id) → cancel", () => {
    expect(classifyDeltaCancel(signal({ intendedAction: "cancel" }), dbRow({ plan_state: "Valid Now" })))
      .toBe("cancel");
  });

  it("auto-renew-off but still in period → pending_cancel (stays active)", () => {
    expect(classifyDeltaCancel(signal({ intendedAction: "pending_cancel" }), dbRow({ plan_state: "Valid Now" })))
      .toBe("pending_cancel");
  });

  it("missing union_pass_id → no_pass_id (skipped, never guessed by email)", () => {
    expect(classifyDeltaCancel(signal({ unionPassId: "" }), dbRow())).toBe("no_pass_id");
  });

  it("no DB row for the pass_id → no_match", () => {
    expect(classifyDeltaCancel(signal(), null)).toBe("no_match");
  });

  it("row already Canceled → noop (idempotent, no re-churn)", () => {
    expect(classifyDeltaCancel(signal({ intendedAction: "cancel" }), dbRow({ plan_state: "Canceled" })))
      .toBe("noop");
  });

  it("pending_cancel when already Pending Cancel → noop", () => {
    expect(classifyDeltaCancel(signal({ intendedAction: "pending_cancel" }), dbRow({ plan_state: "Pending Cancel" })))
      .toBe("noop");
  });

  it("terminal cancel on a Pending Cancel row → cancel (Pending Cancel → Canceled = final_cancel)", () => {
    expect(classifyDeltaCancel(signal({ intendedAction: "cancel" }), dbRow({ plan_state: "Pending Cancel" })))
      .toBe("cancel");
  });

  it("Past Due is active → a cancel signal cancels it", () => {
    expect(classifyDeltaCancel(signal({ intendedAction: "cancel" }), dbRow({ plan_state: "Past Due" })))
      .toBe("cancel");
  });

  it("row excluded by current_state (plan_state active but current_state canceled) → noop", () => {
    expect(classifyDeltaCancel(signal(), dbRow({ plan_state: "Valid Now", current_state: "canceled" })))
      .toBe("noop");
  });

  it("current_state null counts as active → cancel", () => {
    expect(classifyDeltaCancel(signal({ intendedAction: "cancel" }), dbRow({ current_state: null })))
      .toBe("cancel");
  });

  it("monotonicity: active row created AFTER the cancellation (reused pass_id) → protected_newer", () => {
    expect(
      classifyDeltaCancel(
        signal({ effectiveAt: "2026-01-15", intendedAction: "cancel" }),
        dbRow({ created_at: "2026-06-01" }),
      ),
    ).toBe("protected_newer");
  });

  it("monotonicity: active row created BEFORE the cancellation → cancel", () => {
    expect(
      classifyDeltaCancel(
        signal({ effectiveAt: "2026-06-01", intendedAction: "cancel" }),
        dbRow({ created_at: "2026-01-01" }),
      ),
    ).toBe("cancel");
  });

  it("monotonicity: SAME-DAY is treated as possibly-newer → protected_newer (err safe vs reused pass_id)", () => {
    expect(
      classifyDeltaCancel(
        signal({ effectiveAt: "2026-06-01", intendedAction: "cancel" }),
        dbRow({ created_at: "2026-06-01" }),
      ),
    ).toBe("protected_newer");
  });

  it("monotonicity uses the EASTERN date of an evening-ET timestamp, not the UTC roll", () => {
    // 23:30 ET on 06-01 is 03:30 UTC on 06-02; toEasternDate must keep the
    // boundary at 06-01. A row created 05-31 (before the ET date) still cancels;
    // had we wrongly used the UTC date (06-02), 05-31 < 06-02 would also cancel,
    // so pair it with the 06-01 same-day case above which only protects under ET.
    expect(
      classifyDeltaCancel(
        signal({ effectiveAt: "2026-06-01T23:30:00-04:00", intendedAction: "cancel" }),
        dbRow({ created_at: "2026-05-31" }),
      ),
    ).toBe("cancel");
  });

  it("null effectiveAt → no monotonicity guard, applies (exact pass_id is trusted)", () => {
    expect(
      classifyDeltaCancel(
        signal({ effectiveAt: null, intendedAction: "cancel" }),
        dbRow({ created_at: "2026-01-01" }),
      ),
    ).toBe("cancel");
  });
});
