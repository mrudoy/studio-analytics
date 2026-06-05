import { describe, it, expect } from "vitest";
import { toEasternDate } from "./eastern-date";

describe("toEasternDate", () => {
  it("keeps an evening-ET timestamp on the same ET day (the +1-day bug case)", () => {
    // 23:30 ET = 04:30 UTC next day. The DATE column bug stored 2026-01-17; correct is 2026-01-16.
    expect(toEasternDate("2026-01-16 23:30:00 -0500")).toBe("2026-01-16");
    expect(toEasternDate("2026-01-16T23:30:00-05:00")).toBe("2026-01-16");
  });

  it("handles a UTC 'Z' instant by converting to the ET calendar day", () => {
    // 2026-01-17 04:30 UTC == 2026-01-16 23:30 ET
    expect(toEasternDate("2026-01-17T04:30:00Z")).toBe("2026-01-16");
  });

  it("handles a morning-ET timestamp", () => {
    expect(toEasternDate("2026-01-01 10:49:12 -0500")).toBe("2026-01-01");
  });

  it("passes a bare YYYY-MM-DD through unchanged (no re-timezoning)", () => {
    expect(toEasternDate("2026-01-16")).toBe("2026-01-16");
    expect(toEasternDate("2024-12-31")).toBe("2024-12-31");
  });

  it("handles a summer (EDT, -0400) timestamp at the day boundary", () => {
    // 23:30 EDT = 03:30 UTC next day; ET day is still the 15th.
    expect(toEasternDate("2026-07-15 23:30:00 -0400")).toBe("2026-07-15");
  });

  it("salvages a leading date from an unparseable string", () => {
    expect(toEasternDate("2026-03-09 weird")).toBe("2026-03-09");
  });

  it("returns null for empty/nullish", () => {
    expect(toEasternDate(null)).toBeNull();
    expect(toEasternDate(undefined)).toBeNull();
    expect(toEasternDate("")).toBeNull();
    expect(toEasternDate("   ")).toBeNull();
  });
});
