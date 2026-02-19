"use client";

import { useState, useEffect, useRef, Fragment } from "react";
import type {
  DashboardStats,
  TrendRowData,
  PacingData,
  ProjectionData,
  DropInData,
  FirstVisitSegment,
  FirstVisitData,
  ReturningNonMemberData,
  CategoryChurnData,
  ChurnRateData,
  TrendsData,
  MonthOverMonthData,
  NewCustomerVolumeData,
  NewCustomerCohortData,
} from "@/types/dashboard";

// ─── Client-only types ───────────────────────────────────────

type DashboardLoadState =
  | { state: "loading" }
  | { state: "loaded"; data: DashboardStats }
  | { state: "not-configured" }
  | { state: "error"; message: string };

type JobStatus =
  | { state: "idle" }
  | { state: "running"; jobId: string; step: string; percent: number; startedAt?: number }
  | { state: "complete"; sheetUrl: string; duration: number; recordCounts?: Record<string, number>; validation?: { passed: boolean; checks: { name: string; count: number; status: string }[] }; warnings?: string[] }
  | { state: "error"; message: string };

type AppMode = "loading" | "pipeline" | "dashboard";

// ─── Font constants ─────────────────────────────────────────

const FONT_SANS = "'Helvetica Neue', Helvetica, Arial, sans-serif";
const FONT_BRAND = "'Cormorant Garamond', 'Times New Roman', serif";

// ─── Design System Tokens ────────────────────────────────────
// Strict type scale — only these sizes are allowed
const DS = {
  // Typography scale (5 sizes only)
  text: {
    xs: "0.75rem",     // uppercase labels, captions, fine print
    sm: "0.9rem",      // secondary text, sublabels, table content
    md: "1.05rem",     // body text, card titles, category names
    lg: "1.75rem",     // metric values, card hero numbers
    xl: "2.5rem",      // page-level hero KPIs (only in KPIHeroStrip and CategoryDetail headers)
  },
  // Font weights (3 only)
  weight: {
    normal: 400,
    medium: 500,
    bold: 600,
  },
  // Spacing scale
  space: {
    xs: "0.25rem",     // tight gaps
    sm: "0.5rem",      // small inner gaps
    md: "1rem",        // standard inner padding/gaps
    lg: "1.5rem",      // card padding (universal)
    xl: "2rem",        // section spacing
  },
  // Card padding — one value everywhere
  cardPad: "0.75rem",
  // Gap between card header (label row) and content below
  cardHeaderMb: "0.5rem",
  // Uppercase label style
  label: {
    fontSize: "0.75rem",
    fontWeight: 600,
    letterSpacing: "0.05em",
    textTransform: "uppercase" as const,
    color: "var(--st-text-secondary)",
  },
} as const;

// ─── Section & Category Labels ──────────────────────────────
// RULE: Use honest data labels that match Union.fit terminology.
// Never rename these to marketing-friendly alternatives.
// "Auto-Renews" not "Subscriptions". "Sky3" not "Memberships".
const LABELS = {
  autoRenews: "Auto-Renews",
  members: "Members",
  sky3: "Sky3",
  tv: "Sky Ting TV",
  nonMembers: "Non Members",
  firstVisits: "First Visits",
  dropIns: "Drop-Ins",
  returningNonMembers: "Returning Non-Members",
  newCustomers: "New Customers",
  newCustomerFunnel: "New Customer Funnel",
  revenue: "Revenue",
  mrr: "Monthly Recurring Revenue",
  yoy: "Year over Year",
} as const;

// ─── Color palette for categories ──────────────────────────

const COLORS = {
  member: "#4A7C59",     // forest green
  sky3: "#5B7FA5",       // steel blue
  tv: "#8B6FA5",         // muted purple
  accent: "#413A3A",
  accentLight: "rgba(65, 58, 58, 0.08)",
  success: "#4A7C59",
  error: "#A04040",
  warning: "#8B7340",
  teal: "#3A8A8A",       // teal for first visits
  copper: "#B87333",     // copper for returning non-members
  newCustomer: "#5A6B7A", // slate for new customer funnel
};

// ─── Formatting helpers ──────────────────────────────────────

function churnBenchmarkColor(rate: number): string {
  if (rate <= 7) return COLORS.success;
  if (rate <= 12) return COLORS.warning;
  return COLORS.error;
}

function formatNumber(n: number): string {
  return n.toLocaleString("en-US");
}

function formatCurrency(n: number): string {
  const abs = Math.abs(n);
  const sign = n < 0 ? "-" : "";
  // Millions: $2.2M (one decimal, drop trailing .0)
  if (abs >= 1_000_000) {
    const m = (abs / 1_000_000);
    const s = m.toFixed(1);
    return sign + "$" + (s.endsWith(".0") ? s.slice(0, -2) : s) + "M";
  }
  return n.toLocaleString("en-US", {
    style: "currency",
    currency: "USD",
    minimumFractionDigits: 0,
    maximumFractionDigits: 0,
  });
}

function formatCompactCurrency(n: number): string {
  const abs = Math.abs(n);
  const sign = n < 0 ? "-" : "";
  // Millions: $2.2M
  if (abs >= 1_000_000) {
    const m = (abs / 1_000_000);
    const s = m.toFixed(1);
    return sign + "$" + (s.endsWith(".0") ? s.slice(0, -2) : s) + "M";
  }
  // Thousands: $234k
  if (abs >= 1000) {
    return sign + "$" + (abs / 1000).toFixed(abs % 1000 === 0 ? 0 : 1) + "k";
  }
  return formatCurrency(n);
}

function formatRelativeTime(iso: string): string {
  const date = new Date(iso);
  if (isNaN(date.getTime())) return iso;
  const now = new Date();
  const diffMs = now.getTime() - date.getTime();
  const diffMins = Math.floor(diffMs / 60000);
  if (diffMins < 1) return "just now";
  if (diffMins < 60) return `${diffMins}m ago`;
  const diffHours = Math.floor(diffMins / 60);
  if (diffHours < 24) return `${diffHours}h ago`;
  const diffDays = Math.floor(diffHours / 24);
  return `${diffDays}d ago`;
}

function formatDateTime(iso: string): string {
  const date = new Date(iso);
  if (isNaN(date.getTime())) return iso;
  return date.toLocaleString("en-US", {
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
    hour12: true,
  });
}

function formatDelta(n: number | null): string {
  if (n == null) return "";
  const abs = Math.abs(n).toLocaleString("en-US");
  if (n > 0) return `+${abs}`;
  if (n < 0) return `-${abs}`;
  return "0";
}

function formatDeltaCurrency(n: number | null): string {
  if (n == null) return "";
  const formatted = Math.abs(Math.round(n)).toLocaleString("en-US");
  if (n > 0) return `+$${formatted}`;
  if (n < 0) return `-$${formatted}`;
  return "$0";
}

function formatDeltaPercent(n: number | null): string {
  if (n == null) return "";
  if (n > 0) return `+${n}%`;
  return `${n}%`;
}

/** Convert ISO week string "2025-W03" to a Date (Monday of that week) */
function isoWeekToDate(isoWeek: string): Date | null {
  const m = isoWeek.match(/^(\d{4})-W(\d{2})$/);
  if (!m) return null;
  const year = parseInt(m[1]);
  const week = parseInt(m[2]);
  // Jan 4 is always in ISO week 1
  const jan4 = new Date(year, 0, 4);
  const dayOfWeek = jan4.getDay() || 7; // Mon=1...Sun=7
  const mondayW1 = new Date(jan4);
  mondayW1.setDate(jan4.getDate() - dayOfWeek + 1);
  const result = new Date(mondayW1);
  result.setDate(mondayW1.getDate() + (week - 1) * 7);
  return result;
}

/** Format a period (date string or ISO week) as "M/D" */
/** Format a Date as MM/DD/YY */
function formatDateShort(d: Date): string {
  const mm = String(d.getMonth() + 1).padStart(2, "0");
  const dd = String(d.getDate()).padStart(2, "0");
  const yy = String(d.getFullYear()).slice(-2);
  return `${mm}/${dd}/${yy}`;
}

/** Format a period (date string or ISO week) as MM/DD/YY for bar chart labels */
function formatWeekShort(period: string): string {
  const isoDate = isoWeekToDate(period);
  if (isoDate) return formatDateShort(isoDate);
  const d = new Date(period + "T00:00:00");
  if (!isNaN(d.getTime())) return formatDateShort(d);
  return period;
}

/** Format week range start date as MM/DD/YY for bar chart and table labels */
function formatWeekRange(start: string, _end: string): string {
  const s = new Date(start + "T00:00:00");
  if (isNaN(s.getTime())) return start;
  return formatDateShort(s);
}

/** Format cohort start as short label "Jan 26" for bar chart x-axis */
function formatCohortShort(start: string): string {
  const s = new Date(start + "T00:00:00");
  if (isNaN(s.getTime())) return start;
  return `${s.toLocaleDateString("en-US", { month: "short" })} ${s.getDate()}`;
}

/** Format week range as "Jan 19-25" or "Jan 26-Feb 1" for cohort labels */
function formatWeekRangeLabel(start: string, end: string): string {
  const s = new Date(start + "T00:00:00");
  const e = new Date(end + "T00:00:00");
  if (isNaN(s.getTime()) || isNaN(e.getTime())) return start;
  const sMonth = s.toLocaleDateString("en-US", { month: "short" });
  const eMonth = e.toLocaleDateString("en-US", { month: "short" });
  if (sMonth === eMonth) {
    return `${sMonth} ${s.getDate()}-${e.getDate()}`;
  }
  return `${sMonth} ${s.getDate()}-${eMonth} ${e.getDate()}`;
}

function formatMonthLabel(period: string): string {
  const [year, month] = period.split("-");
  const d = new Date(parseInt(year), parseInt(month) - 1);
  if (isNaN(d.getTime())) return period;
  return d.toLocaleDateString("en-US", { month: "short", year: "numeric" });
}

// ─── Shared Components ──────────────────────────────────────

function SkyTingLogo() {
  return (
    <span
      style={{
        fontFamily: FONT_BRAND,
        fontSize: "1.1rem",
        fontWeight: 400,
        letterSpacing: "0.35em",
        textTransform: "uppercase" as const,
        color: "var(--st-text-primary)",
      }}
    >
      SKY TING
    </span>
  );
}

function NavLink({ href, children }: { href: string; children: React.ReactNode }) {
  return (
    <a
      href={href}
      className="hover:underline transition-colors"
      style={{ color: "var(--st-text-secondary)", fontWeight: DS.weight.normal }}
      onMouseOver={(e) =>
        (e.currentTarget.style.color = "var(--st-text-primary)")
      }
      onMouseOut={(e) =>
        (e.currentTarget.style.color = "var(--st-text-secondary)")
      }
    >
      {children}
    </a>
  );
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
//  PIPELINE VIEW (local)
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

function formatEta(ms: number): string {
  if (ms <= 0) return "";
  const totalSec = Math.round(ms / 1000);
  if (totalSec < 60) return `~${totalSec}s remaining`;
  const min = Math.floor(totalSec / 60);
  const sec = totalSec % 60;
  return sec > 0 ? `~${min}m ${sec}s remaining` : `~${min}m remaining`;
}

function PipelineView() {
  const [status, setStatus] = useState<JobStatus>({ state: "idle" });
  const [hasCredentials, setHasCredentials] = useState<boolean | null>(null);
  const [now, setNow] = useState(Date.now());
  const eventSourceRef = useRef<EventSource | null>(null);

  // ETA ticker: updates every second while running so the countdown is smooth
  useEffect(() => {
    if (status.state !== "running") return;
    const timer = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(timer);
  }, [status.state]);

  useEffect(() => {
    fetch("/api/settings")
      .then((res) => res.json())
      .then((data) => setHasCredentials(data.hasCredentials))
      .catch(() => setHasCredentials(false));

    return () => {
      eventSourceRef.current?.close();
    };
  }, []);

  async function resetPipeline() {
    try {
      await fetch("/api/pipeline", { method: "DELETE" });
      eventSourceRef.current?.close();
      setStatus({ state: "idle" });
    } catch {
      setStatus({ state: "error", message: "Failed to reset queue" });
    }
  }

  async function runPipeline() {
    setStatus({ state: "running", jobId: "", step: "Starting...", percent: 0 });

    try {
      const res = await fetch("/api/pipeline", { method: "POST" });
      if (!res.ok) {
        const err = await res.json();
        setStatus({ state: "error", message: err.error || "Failed to start pipeline" });
        return;
      }

      const { jobId } = await res.json();
      setStatus({ state: "running", jobId, step: "Queued...", percent: 0 });

      eventSourceRef.current?.close();
      const es = new EventSource(`/api/status?jobId=${jobId}`);
      eventSourceRef.current = es;

      es.addEventListener("progress", (e) => {
        const data = JSON.parse(e.data);
        setStatus({ state: "running", jobId, step: data.step, percent: data.percent, startedAt: data.startedAt });
      });

      es.addEventListener("complete", (e) => {
        const data = JSON.parse(e.data);
        setStatus({ state: "complete", sheetUrl: data.sheetUrl, duration: data.duration, recordCounts: data.recordCounts, validation: data.validation, warnings: data.warnings });
        es.close();
      });

      es.addEventListener("error", (e) => {
        try {
          const data = JSON.parse((e as MessageEvent).data);
          setStatus({ state: "error", message: data.message });
        } catch {
          setStatus({ state: "error", message: "Connection lost" });
        }
        es.close();
      });
    } catch {
      setStatus({ state: "error", message: "Network error" });
    }
  }

  return (
    <div className="min-h-screen flex flex-col items-center justify-center p-8">
      <div className="max-w-xl w-full space-y-10">
        <div className="text-center space-y-4">
          <div className="flex justify-center">
            <SkyTingLogo />
          </div>
          <h1
            style={{
              color: "var(--st-text-primary)",
                           fontWeight: 700,
              fontSize: "2.4rem",
              letterSpacing: "-0.03em",
            }}
          >
            Studio Analytics
          </h1>
          <p style={{ color: "var(--st-text-secondary)", fontSize: "0.95rem" }}>
            Pull data from Union.fit, run analytics, export to Google Sheets
          </p>
        </div>

        {hasCredentials === false && (
          <div
            className="rounded-2xl p-4 text-center text-sm"
            style={{
              backgroundColor: "var(--st-bg-section)",
              border: "1px solid var(--st-border)",
              color: "var(--st-warning)",
                         }}
          >
            No credentials configured.{" "}
            <a href="/settings" className="underline font-medium" style={{ color: "var(--st-text-primary)" }}>
              Go to Settings
            </a>{" "}
            to set up Union.fit and Google Sheets.
          </div>
        )}

        <div
          className="rounded-2xl p-8 space-y-6"
          style={{
            backgroundColor: "var(--st-bg-card)",
            border: "1px solid var(--st-border)",
          }}
        >
          <button
            onClick={runPipeline}
            disabled={status.state === "running" || !hasCredentials}
            className="w-full px-6 py-4 text-base font-medium tracking-wide uppercase transition-all disabled:opacity-40 disabled:cursor-not-allowed"
            style={{
              backgroundColor: "var(--st-accent)",
              color: "var(--st-text-light)",
              borderRadius: "var(--st-radius-pill)",
                           fontWeight: 700,
              letterSpacing: "0.08em",
            }}
            onMouseOver={(e) => {
              if (!(e.currentTarget as HTMLButtonElement).disabled)
                e.currentTarget.style.backgroundColor = "var(--st-accent-hover)";
            }}
            onMouseOut={(e) => {
              e.currentTarget.style.backgroundColor = "var(--st-accent)";
            }}
          >
            {status.state === "running" ? "Pipeline Running..." : "Run Analytics Pipeline"}
          </button>

          {status.state === "running" && (() => {
            const elapsed = status.startedAt ? now - status.startedAt : 0;
            const etaMs = status.percent > 5 && elapsed > 0
              ? (elapsed / status.percent) * (100 - status.percent)
              : 0;
            return (
              <div className="space-y-3">
                <div className="flex justify-between items-baseline">
                  <span className="text-sm" style={{ color: "var(--st-text-secondary)" }}>{status.step}</span>
                  <span className="text-lg font-bold" style={{ color: "var(--st-text-primary)" }}>{status.percent}%</span>
                </div>
                <div
                  className="h-2 rounded-full overflow-hidden"
                  style={{ backgroundColor: "var(--st-border)" }}
                >
                  <div
                    className="h-full rounded-full transition-all duration-500"
                    style={{
                      width: `${status.percent}%`,
                      backgroundColor: "var(--st-accent)",
                    }}
                  />
                </div>
                {etaMs > 3000 && (
                  <p className="text-xs text-center" style={{ color: "var(--st-text-secondary)", opacity: 0.7 }}>
                    {formatEta(etaMs)}
                  </p>
                )}
                <button
                  onClick={resetPipeline}
                  className="text-xs underline opacity-60 hover:opacity-100 transition-opacity"
                  style={{ color: "var(--st-text-secondary)" }}
                >
                  Reset if stuck
                </button>
              </div>
            );
          })()}

          {status.state === "complete" && (
            <div
              className="rounded-2xl p-5 space-y-4"
              style={{
                backgroundColor: status.validation && !status.validation.passed ? "#FDF6EC" : "#EFF5F0",
                border: `1px solid ${status.validation && !status.validation.passed ? "rgba(139, 115, 64, 0.2)" : "rgba(74, 124, 89, 0.2)"}`,
              }}
            >
              <div className="text-center space-y-1">
                <p className="font-medium" style={{ color: "var(--st-success)" }}>
                  Pipeline complete
                </p>
                <p className="text-sm" style={{ color: "var(--st-success)", opacity: 0.8 }}>
                  Finished in {Math.round(status.duration / 1000)}s
                </p>
              </div>

              {/* Validation warning */}
              {status.validation && !status.validation.passed && (
                <p className="text-xs text-center font-medium" style={{ color: COLORS.warning }}>
                  Some data may be missing or incomplete
                </p>
              )}

              {/* Record counts table */}
              {status.validation?.checks && (
                <div className="rounded-lg overflow-hidden" style={{ border: "1px solid rgba(0,0,0,0.06)" }}>
                  {status.validation.checks.map((check, i) => (
                    <div
                      key={check.name}
                      className="flex items-center justify-between px-3 py-1.5 text-xs"
                      style={{
                                               backgroundColor: i % 2 === 0 ? "rgba(255,255,255,0.6)" : "rgba(255,255,255,0.3)",
                      }}
                    >
                      <span style={{ color: "var(--st-text-secondary)" }}>{check.name}</span>
                      <div className="flex items-center gap-2">
                        <span style={{ fontWeight: 600, color: "var(--st-text-primary)", fontVariantNumeric: "tabular-nums" }}>
                          {check.count.toLocaleString()}
                        </span>
                        <span style={{
                          color: check.status === "ok" ? COLORS.success : check.status === "warn" ? COLORS.warning : COLORS.error,
                          fontSize: "0.7rem",
                        }}>
                          {check.status === "ok" ? "\u2713" : check.status === "warn" ? "\u26A0" : "\u2717"}
                        </span>
                      </div>
                    </div>
                  ))}
                </div>
              )}

              {/* Warnings */}
              {status.warnings && status.warnings.length > 0 && (
                <details className="text-xs" style={{color: COLORS.warning }}>
                  <summary className="cursor-pointer font-medium">{status.warnings.length} warning{status.warnings.length > 1 ? "s" : ""}</summary>
                  <ul className="mt-1 ml-4 space-y-0.5 list-disc" style={{ color: "var(--st-text-secondary)" }}>
                    {status.warnings.slice(0, 10).map((w, i) => <li key={i}>{w}</li>)}
                  </ul>
                </details>
              )}

              <div className="text-center">
                <a
                  href={status.sheetUrl}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="inline-block px-5 py-2 text-sm font-medium uppercase tracking-wider transition-colors"
                  style={{
                    backgroundColor: "var(--st-success)",
                    color: "#fff",
                    borderRadius: "var(--st-radius-pill)",
                                       letterSpacing: "0.06em",
                  }}
                >
                  Open Google Sheet
                </a>
              </div>
            </div>
          )}

          {status.state === "error" && (
            <div
              className="rounded-2xl p-5 text-center"
              style={{
                backgroundColor: "#F5EFEF",
                border: "1px solid rgba(160, 64, 64, 0.2)",
              }}
            >
              <p className="font-medium" style={{ color: "var(--st-error)" }}>Error</p>
              <p className="text-sm mt-1" style={{ color: "var(--st-error)", opacity: 0.85 }}>
                {status.message}
              </p>
              <button
                onClick={() => setStatus({ state: "idle" })}
                className="mt-3 text-sm underline"
                style={{ color: "var(--st-error)", opacity: 0.7 }}
              >
                Dismiss
              </button>
            </div>
          )}
        </div>

        <div className="flex justify-center gap-8 text-sm" style={{ color: "var(--st-text-secondary)" }}>
          <NavLink href="/settings">Settings</NavLink>
          <NavLink href="/results">Results</NavLink>
        </div>
      </div>
    </div>
  );
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
//  DASHBOARD VIEW — Visual redesign
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

// ─── Section Header ──────────────────────────────────────────

function SectionHeader({ children, subtitle }: { children: React.ReactNode; subtitle?: string }) {
  return (
    <div>
      <h2
        style={{
          color: "var(--st-text-primary)",
          fontFamily: FONT_SANS,
          fontWeight: DS.weight.bold,
          fontSize: DS.text.md,
          letterSpacing: "0.02em",
        }}
      >
        {children}
      </h2>
      {subtitle && (
        <p style={{ color: "var(--st-text-secondary)", fontSize: DS.text.xs, marginTop: "2px", opacity: 0.7 }}>
          {subtitle}
        </p>
      )}
    </div>
  );
}

// ─── Delta Badge ─────────────────────────────────────────────

function DeltaBadge({ delta, deltaPercent, isPositiveGood = true, isCurrency = false, compact = false }: {
  delta: number | null;
  deltaPercent: number | null;
  isPositiveGood?: boolean;
  isCurrency?: boolean;
  compact?: boolean;
}) {
  if (delta == null) return null;

  const isPositive = delta > 0;
  const isGood = isPositiveGood ? isPositive : !isPositive;
  const color = delta === 0 ? "var(--st-text-secondary)" : isGood ? "var(--st-success)" : "var(--st-error)";
  const arrow = delta > 0 ? "▲" : delta < 0 ? "▼" : "";

  if (compact) {
    const compactBg = delta === 0
      ? "rgba(128, 128, 128, 0.1)"
      : isGood
        ? "rgba(74, 124, 89, 0.1)"
        : "rgba(160, 64, 64, 0.1)";

    return (
      <span
        className="inline-flex items-center rounded-full px-2 py-0.5"
        style={{ color, fontWeight: DS.weight.medium, fontSize: DS.text.xs, whiteSpace: "nowrap", backgroundColor: compactBg, gap: "2px" }}
      >
        <span style={{ fontSize: "0.55rem" }}>{arrow}</span>
        {isCurrency ? formatDeltaCurrency(delta) : formatDelta(delta)}
        {deltaPercent != null && <span style={{ opacity: 0.75 }}>({formatDeltaPercent(deltaPercent)})</span>}
      </span>
    );
  }

  const bgColor = delta === 0
    ? "rgba(128, 128, 128, 0.1)"
    : isGood
      ? "rgba(74, 124, 89, 0.1)"
      : "rgba(160, 64, 64, 0.1)";

  return (
    <div
      className="inline-flex items-center rounded-full px-2.5 py-0.5 mt-1.5"
      style={{ color, backgroundColor: bgColor, gap: "3px" }}
    >
      <span style={{ fontSize: DS.text.xs, fontWeight: DS.weight.bold }}>{arrow}</span>
      <span style={{ fontWeight: DS.weight.bold, fontSize: DS.text.md, letterSpacing: "-0.01em" }}>
        {isCurrency ? formatDeltaCurrency(delta) : formatDelta(delta)}
      </span>
      {deltaPercent != null && (
        <span style={{ fontWeight: DS.weight.medium, fontSize: DS.text.sm, opacity: 0.8 }}>
          ({formatDeltaPercent(deltaPercent)})
        </span>
      )}
    </div>
  );
}

// ─── Freshness Badge ─────────────────────────────────────────

function useNextRunCountdown() {
  const [nextRun, setNextRun] = useState<number | null>(null);
  const [countdown, setCountdown] = useState<string>("");

  useEffect(() => {
    fetch("/api/schedule").then((r) => r.json()).then((data) => {
      if (data.nextRun) setNextRun(data.nextRun);
    }).catch(() => {});
  }, []);

  useEffect(() => {
    if (!nextRun) return;
    function tick() {
      const diff = nextRun! - Date.now();
      if (diff <= 0) { setCountdown("running soon"); return; }
      const h = Math.floor(diff / 3_600_000);
      const m = Math.floor((diff % 3_600_000) / 60_000);
      setCountdown(h > 0 ? `${h}h ${m}m` : `${m}m`);
    }
    tick();
    const id = setInterval(tick, 60_000);
    return () => clearInterval(id);
  }, [nextRun]);

  return countdown;
}

function FreshnessBadge({ lastUpdated, spreadsheetUrl, dataSource }: { lastUpdated: string | null; spreadsheetUrl?: string; dataSource?: "database" | "sheets" | "hybrid" }) {
  const [refreshState, setRefreshState] = useState<"idle" | "running" | "done" | "error">("idle");
  const [pipelineStep, setPipelineStep] = useState("");
  const [pipelinePercent, setPipelinePercent] = useState(0);
  const [pipelineStartedAt, setPipelineStartedAt] = useState(0);
  const [now, setNow] = useState(Date.now());
  const eventSourceRef = useRef<EventSource | null>(null);
  const countdown = useNextRunCountdown();

  // ETA ticker — update every second while running
  useEffect(() => {
    if (refreshState !== "running") return;
    const timer = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(timer);
  }, [refreshState]);

  // Cleanup EventSource on unmount
  useEffect(() => {
    return () => { eventSourceRef.current?.close(); };
  }, []);

  async function triggerRefresh() {
    setRefreshState("running");
    setPipelineStep("Starting...");
    setPipelinePercent(0);
    setPipelineStartedAt(0);
    try {
      const res = await fetch("/api/pipeline", { method: "POST", headers: { "Content-Type": "application/json" }, body: "{}" });
      if (res.status === 409) {
        setRefreshState("running"); // already running
        return;
      }
      if (!res.ok) throw new Error("Failed");
      const { jobId } = await res.json();

      // Connect to SSE for live progress
      eventSourceRef.current?.close();
      const es = new EventSource(`/api/status?jobId=${jobId}`);
      eventSourceRef.current = es;

      es.addEventListener("progress", (e) => {
        const data = JSON.parse(e.data);
        setPipelineStep(data.step || "Processing...");
        setPipelinePercent(data.percent || 0);
        if (data.startedAt) setPipelineStartedAt(data.startedAt);
      });

      es.addEventListener("complete", () => {
        es.close();
        setRefreshState("done");
        setPipelineStep("");
        setPipelinePercent(100);
        // Reload dashboard data after short delay
        setTimeout(() => window.location.reload(), 2000);
      });

      es.addEventListener("error", (e) => {
        try {
          const data = JSON.parse((e as MessageEvent).data);
          setPipelineStep(data.message || "Error");
        } catch { /* SSE error */ }
        es.close();
        setRefreshState("error");
        setTimeout(() => setRefreshState("idle"), 8_000);
      });
    } catch {
      setRefreshState("error");
      setTimeout(() => setRefreshState("idle"), 5_000);
    }
  }

  const elapsed = pipelineStartedAt ? now - pipelineStartedAt : 0;
  const etaMs = pipelinePercent > 5 && elapsed > 0
    ? (elapsed / pipelinePercent) * (100 - pipelinePercent)
    : 0;

  if (!lastUpdated) return null;

  const date = new Date(lastUpdated);
  const isStale = Date.now() - date.getTime() > 24 * 60 * 60 * 1000;

  return (
    <div className="flex flex-col items-center gap-2.5" style={{ fontSize: "0.85rem" }}>
      {/* Freshness pills row */}
      <div className="inline-flex items-center gap-2">
        {/* Updated pill */}
        <div
          className="inline-flex items-center gap-2 rounded-full px-3.5 py-1.5"
          style={{
            backgroundColor: isStale ? "rgba(160, 64, 64, 0.08)" : "rgba(74, 124, 89, 0.08)",
            border: `1px solid ${isStale ? "rgba(160, 64, 64, 0.2)" : "rgba(74, 124, 89, 0.2)"}`,
                       fontWeight: 600,
          }}
        >
          <span
            className="inline-block rounded-full"
            style={{ width: "7px", height: "7px", backgroundColor: isStale ? "var(--st-error)" : "var(--st-success)" }}
          />
          <span style={{ color: isStale ? "var(--st-error)" : "var(--st-success)" }}>
            Updated {formatRelativeTime(lastUpdated)}
          </span>
          <span style={{ color: "var(--st-text-secondary)", fontWeight: 400 }}>
            {formatDateTime(lastUpdated)}
          </span>
        </div>
        {/* Next run pill */}
        {countdown && refreshState !== "running" && (
          <div
            className="inline-flex items-center gap-1.5 rounded-full px-3 py-1.5"
            style={{
              backgroundColor: "rgba(128, 128, 128, 0.06)",
              border: "1px solid var(--st-border)",
                           fontWeight: 500,
              color: "var(--st-text-secondary)",
            }}
          >
            Next in {countdown}
          </div>
        )}
      </div>

      <div className="inline-flex items-center gap-3">
        {spreadsheetUrl && (
          <a
            href={spreadsheetUrl}
            target="_blank"
            rel="noopener noreferrer"
            className="inline-flex items-center gap-1.5 rounded-full px-3 py-1 transition-colors"
            style={{
              color: "var(--st-text-secondary)",
              border: "1px solid var(--st-border)",
                           fontWeight: 500,
              fontSize: "0.8rem",
            }}
            onMouseOver={(e) => {
              e.currentTarget.style.borderColor = "var(--st-border-hover)";
              e.currentTarget.style.color = "var(--st-text-primary)";
            }}
            onMouseOut={(e) => {
              e.currentTarget.style.borderColor = "var(--st-border)";
              e.currentTarget.style.color = "var(--st-text-secondary)";
            }}
          >
            <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
              <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z" />
              <polyline points="14 2 14 8 20 8" />
              <line x1="16" y1="13" x2="8" y2="13" />
              <line x1="16" y1="17" x2="8" y2="17" />
              <polyline points="10 9 9 9 8 9" />
            </svg>
            Google Sheet
          </a>
        )}

        <button
          onClick={triggerRefresh}
          disabled={refreshState === "running"}
          className="inline-flex items-center gap-1.5 rounded-full px-3 py-1 transition-colors"
          style={{
            color: refreshState === "running" ? "var(--st-text-secondary)" : refreshState === "done" ? "var(--st-success)" : refreshState === "error" ? "var(--st-error)" : "var(--st-text-secondary)",
            border: `1px solid ${refreshState === "done" ? "rgba(74, 124, 89, 0.3)" : "var(--st-border)"}`,
                       fontWeight: 500,
            fontSize: "0.8rem",
            cursor: refreshState === "running" ? "wait" : "pointer",
            opacity: refreshState === "running" ? 0.6 : 1,
            background: "transparent",
          }}
          onMouseOver={(e) => {
            if (refreshState === "idle") {
              e.currentTarget.style.borderColor = "var(--st-border-hover)";
              e.currentTarget.style.color = "var(--st-text-primary)";
            }
          }}
          onMouseOut={(e) => {
            if (refreshState === "idle") {
              e.currentTarget.style.borderColor = "var(--st-border)";
              e.currentTarget.style.color = "var(--st-text-secondary)";
            }
          }}
        >
          <svg
            width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"
            style={{ animation: refreshState === "running" ? "spin 1s linear infinite" : "none" }}
          >
            <polyline points="23 4 23 10 17 10" />
            <polyline points="1 20 1 14 7 14" />
            <path d="M3.51 9a9 9 0 0 1 14.85-3.36L23 10M1 14l4.64 4.36A9 9 0 0 0 20.49 15" />
          </svg>
          {refreshState === "running" ? "Refreshing..." : refreshState === "done" ? "Queued" : refreshState === "error" ? "Failed" : "Refresh Data"}
        </button>
      </div>

      {refreshState === "running" && (
        <div style={{ marginTop: "0.5rem", maxWidth: "340px", width: "100%" }}>
          <div className="flex justify-between items-baseline" style={{fontSize: "0.72rem", marginBottom: "4px" }}>
            <span style={{ color: "var(--st-text-secondary)" }}>{pipelineStep}</span>
            <span style={{ color: "var(--st-text-primary)", fontWeight: 700 }}>{pipelinePercent}%</span>
          </div>
          <div className="rounded-full overflow-hidden" style={{ height: "6px", backgroundColor: "var(--st-border)" }}>
            <div
              className="rounded-full transition-all duration-500"
              style={{ height: "100%", width: `${pipelinePercent}%`, backgroundColor: "var(--st-accent)" }}
            />
          </div>
          {etaMs > 3000 && (
            <p style={{fontSize: "0.68rem", color: "var(--st-text-secondary)", opacity: 0.7, marginTop: "3px", textAlign: "center" }}>
              {formatEta(etaMs)}
            </p>
          )}
        </div>
      )}
    </div>
  );
}

// ─── SVG Donut Chart ─────────────────────────────────────────

// ─── Mini Bar Chart (reusable) ───────────────────────────────

interface BarChartData {
  label: string;
  value: number;
  color?: string;
}

/**
 * Bar chart rule: each bar gets a fixed-width slot (BAR_SLOT_PX).
 * Fluid flex-based layout — bars stretch to fill available width.
 */

function MiniBarChart({ data, height = 80, showValues = true, formatValue }: {
  data: BarChartData[];
  height?: number;
  showValues?: boolean;
  formatValue?: (v: number) => string;
}) {
  if (data.length === 0) return null;
  const max = Math.max(...data.map((d) => Math.abs(d.value)), 1);
  const fmt = formatValue || ((v: number) => String(v));
  const barHeight = height;

  return (
    <div style={{ width: "100%" }}>
      {/* Bar area */}
      <div style={{
        display: "flex",
        alignItems: "flex-end",
        gap: "2px",
        height: `${barHeight}px`,
        borderBottom: "1px solid var(--st-border)",
        paddingBottom: "1px",
      }}>
        {data.map((d, i) => {
          const fraction = max > 0 ? Math.abs(d.value) / max : 0;
          const h = Math.max(Math.round(fraction * (barHeight - (showValues ? 18 : 4))), 3);
          return (
            <div key={i} style={{ flex: 1, minWidth: 0, display: "flex", flexDirection: "column", alignItems: "center", justifyContent: "flex-end" }}>
              {showValues && (
                <span style={{
                                   fontSize: "0.65rem",
                  fontWeight: DS.weight.medium,
                  color: "var(--st-text-primary)",
                  marginBottom: "2px",
                  whiteSpace: "nowrap",
                  textAlign: "center",
                }}>
                  {fmt(d.value)}
                </span>
              )}
              <div style={{
                width: "70%",
                maxWidth: "28px",
                height: `${h}px`,
                borderRadius: "2px 2px 0 0",
                backgroundColor: d.color || "var(--st-accent)",
                opacity: 0.8,
              }} />
            </div>
          );
        })}
      </div>
      {/* X-axis labels */}
      <div style={{
        display: "flex",
        gap: "2px",
        marginTop: "4px",
      }}>
        {data.map((d, i) => (
          <div key={i} style={{
            flex: 1,
            minWidth: 0,
            textAlign: "center",
                       fontSize: "0.65rem",
            fontWeight: DS.weight.normal,
            color: "var(--st-text-secondary)",
            overflow: "hidden",
            textOverflow: "ellipsis",
            whiteSpace: "nowrap",
          }}>
            {d.label}
          </div>
        ))}
      </div>
    </div>
  );
}

// ─── Area Chart (responsive, fills container) ────────────────

/**
 * Full-width area chart with gradient fill.
 * Renders inside a container div and uses viewBox for responsive scaling.
 * Inspired by modern dashboard UIs (Square, Stripe, etc.)
 */
function AreaChart({ data, height = 200, formatValue, color = COLORS.member, showGrid = true }: {
  data: { label: string; value: number }[];
  height?: number;
  formatValue?: (v: number) => string;
  color?: string;
  showGrid?: boolean;
}) {
  if (data.length < 2) return null;
  const fmt = formatValue || ((v: number) => String(v));
  const values = data.map(d => d.value);
  const dataMax = Math.max(...values);
  const dataMin = Math.min(...values);
  // Add 10% padding above max for breathing room
  const max = dataMax + (dataMax - dataMin) * 0.1;
  const min = dataMin - (dataMax - dataMin) * 0.05;
  const range = max - min || 1;

  // Internal SVG coordinate space
  const svgW = 800;
  const padLeft = 52;   // Y-axis labels
  const padRight = 16;
  const padTop = 12;
  const padBot = 28;    // X-axis labels
  const plotW = svgW - padLeft - padRight;
  const plotH = height - padTop - padBot;

  const gradId = `area-${data.length}-${Math.round(dataMax)}`;

  // Build points
  const points = data.map((d, i) => {
    const x = padLeft + (i / (data.length - 1)) * plotW;
    const yFrac = 1 - (d.value - min) / range;
    const y = padTop + yFrac * plotH;
    return { x, y, value: d.value, label: d.label };
  });

  // Smooth curve (cardinal spline approximation)
  const linePath = points.map((p, i) => `${i === 0 ? "M" : "L"}${p.x.toFixed(1)},${p.y.toFixed(1)}`).join(" ");
  const areaPath = `${linePath} L${points[points.length - 1].x.toFixed(1)},${padTop + plotH} L${points[0].x.toFixed(1)},${padTop + plotH} Z`;

  // Grid lines (4 horizontal)
  const gridLines = [0.25, 0.5, 0.75, 1.0].map(frac => {
    const val = min + frac * range;
    const y = padTop + (1 - frac) * plotH;
    return { y, val };
  });

  return (
    <div style={{ width: "100%", position: "relative" }}>
      <svg
        viewBox={`0 0 ${svgW} ${height}`}
        preserveAspectRatio="none"
        style={{ width: "100%", height: `${height}px`, display: "block" }}
      >
        <defs>
          <linearGradient id={gradId} x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor={color} stopOpacity="0.2" />
            <stop offset="100%" stopColor={color} stopOpacity="0.02" />
          </linearGradient>
        </defs>

        {/* Grid lines */}
        {showGrid && gridLines.map((g, i) => (
          <g key={i}>
            <line
              x1={padLeft}
              y1={g.y}
              x2={svgW - padRight}
              y2={g.y}
              stroke="var(--st-border)"
              strokeWidth="0.5"
              strokeDasharray="4 4"
            />
            <text
              x={padLeft - 8}
              y={g.y + 3}
              textAnchor="end"
              style={{fontSize: "9px", fontWeight: 400, fill: "var(--st-text-secondary)" }}
            >
              {fmt(g.val)}
            </text>
          </g>
        ))}

        {/* Area fill */}
        <path d={areaPath} fill={`url(#${gradId})`} />

        {/* Line */}
        <path d={linePath} fill="none" stroke={color} strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />

        {/* Dots — only first, last, and every other for spacing */}
        {points.map((p, i) => {
          const show = i === 0 || i === points.length - 1 || (data.length <= 8) || (i % Math.ceil(data.length / 8) === 0);
          if (!show) return null;
          return (
            <circle key={i} cx={p.x} cy={p.y} r="3" fill="var(--st-bg-card)" stroke={color} strokeWidth="1.5" />
          );
        })}

        {/* X-axis labels — show subset if many points */}
        {points.map((p, i) => {
          const step = Math.max(1, Math.ceil(data.length / 8));
          const show = i === 0 || i === points.length - 1 || i % step === 0;
          if (!show) return null;
          return (
            <text
              key={`x-${i}`}
              x={p.x}
              y={height - 4}
              textAnchor="middle"
              style={{fontSize: "9px", fontWeight: 400, fill: "var(--st-text-secondary)" }}
            >
              {p.label}
            </text>
          );
        })}

        {/* Value on last point */}
        {(() => {
          const last = points[points.length - 1];
          return (
            <text
              x={last.x}
              y={last.y - 10}
              textAnchor="end"
              style={{fontSize: "11px", fontWeight: 700, fill: "var(--st-text-primary)" }}
            >
              {fmt(last.value)}
            </text>
          );
        })()}
      </svg>
    </div>
  );
}

// ─── Card wrapper (used selectively) ─────────────────────────

function Card({ children, padding = DS.cardPad }: { children: React.ReactNode; padding?: string }) {
  return (
    <div
      className="rounded-2xl"
      style={{
        backgroundColor: "var(--st-bg-card)",
        border: "1px solid var(--st-border)",
        padding,
        overflow: "hidden",
        minWidth: 0,
        fontFamily: FONT_SANS,
      }}
    >
      {children}
    </div>
  );
}

function NoData({ label }: { label: string }) {
  return (
    <Card>
      <p style={{fontWeight: DS.weight.normal, fontSize: DS.text.sm, color: "var(--st-text-secondary)" }}>
        {label}: <span style={{ opacity: 0.6 }}>No data available</span>
      </p>
    </Card>
  );
}

// ─── Trend Row (compact, inline) ─────────────────────────────

function TrendRow({ label, value, delta, deltaPercent, isPositiveGood = true, isCurrency = false, sublabel, isLast = false }: {
  label: string;
  value: string;
  delta: number | null;
  deltaPercent: number | null;
  isPositiveGood?: boolean;
  isCurrency?: boolean;
  sublabel?: string;
  isLast?: boolean;
}) {
  return (
    <div style={{ padding: "0.5rem 0", borderBottom: isLast ? "none" : "1px solid var(--st-border)" }}>
      <div style={{ display: "flex", alignItems: "center", gap: DS.space.lg }}>
        <span style={{fontWeight: DS.weight.medium, fontSize: DS.text.sm, color: "var(--st-text-secondary)" }}>
          {label}
        </span>
        <div className="flex items-center gap-3">
          <span style={{fontWeight: DS.weight.bold, fontSize: DS.text.md, color: "var(--st-text-primary)" }}>
            {value}
          </span>
          <DeltaBadge delta={delta} deltaPercent={deltaPercent} isPositiveGood={isPositiveGood} isCurrency={isCurrency} compact />
        </div>
      </div>
      {sublabel && (
        <p style={{fontWeight: DS.weight.normal, fontSize: DS.text.xs, color: "var(--st-text-secondary)", marginTop: "2px", fontStyle: "italic" }}>
          {sublabel}
        </p>
      )}
    </div>
  );
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
//  NEW LAYOUT — Layer 1, 2, 3 components
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

// ─── Layer 1: KPI Hero Strip ────────────────────────────────

interface HeroTile {
  label: string;
  value: string;
  delta?: number | null;
  deltaPercent?: number | null;
  isPositiveGood?: boolean;
  isCurrency?: boolean;
  sublabel?: string;
}

function KPIHeroStrip({ tiles }: { tiles: HeroTile[] }) {
  return (
    <div className="grid gap-3 grid-cols-1 sm:grid-cols-3">
      {tiles.map((tile, i) => (
        <Card key={i}>
          <p style={{...DS.label, marginBottom: DS.cardHeaderMb }}>
            {tile.label}
          </p>
          <p style={{fontWeight: DS.weight.bold, fontSize: DS.text.xl, color: "var(--st-text-primary)", letterSpacing: "-0.03em", lineHeight: 1.0 }}>
            {tile.value}
          </p>
          {(tile.sublabel || tile.delta != null) && (
            <div className="flex items-center gap-2" style={{ marginTop: "8px" }}>
              {tile.delta != null && (
                <DeltaBadge delta={tile.delta} deltaPercent={tile.deltaPercent ?? null} isPositiveGood={tile.isPositiveGood} isCurrency={tile.isCurrency} compact />
              )}
              {tile.sublabel && (
                <span style={{fontWeight: DS.weight.normal, fontSize: DS.text.xs, color: "var(--st-text-secondary)" }}>
                  {tile.sublabel}
                </span>
              )}
            </div>
          )}
        </Card>
      ))}
    </div>
  );
}

// ─── Revenue Section (weekly + monthly, hard separation) ─────

function formatShortMonth(period: string): string {
  const [year, month] = period.split("-");
  const d = new Date(parseInt(year), parseInt(month) - 1);
  if (isNaN(d.getTime())) return period;
  const mon = d.toLocaleDateString("en-US", { month: "short" });
  const yr = year.slice(2); // "25", "26"
  // Always show abbreviated year to avoid confusion across year boundaries
  return `${mon} '${yr}`;
}

function RevenueSection({ data, trends }: { data: DashboardStats; trends?: TrendsData | null }) {
  // Only show completed months (exclude current calendar month)
  const nowDate = new Date();
  const currentMonthKey = `${nowDate.getFullYear()}-${String(nowDate.getMonth() + 1).padStart(2, "0")}`;
  const monthlyRevenue = (data.monthlyRevenue || []).filter((m) => m.month < currentMonthKey);

  // Area chart data (last 12 months)
  const revenueAreaData = monthlyRevenue.slice(-12).map((m) => ({
    label: formatShortMonth(m.month),
    value: m.gross,
  }));

  // MoM delta from the last two months
  const lastTwo = monthlyRevenue.slice(-2);
  const currentMonth = lastTwo.length >= 1 ? lastTwo[lastTwo.length - 1] : null;
  const prevMonth = lastTwo.length >= 2 ? lastTwo[0] : null;
  const momDelta = currentMonth && prevMonth && prevMonth.gross > 0
    ? currentMonth.gross - prevMonth.gross
    : null;
  const momDeltaPct = currentMonth && prevMonth && prevMonth.gross > 0
    ? Math.round((currentMonth.gross - prevMonth.gross) / prevMonth.gross * 1000) / 10
    : null;

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "1rem" }}>
      <SectionHeader>{LABELS.revenue}</SectionHeader>

      {/* Full-width area chart */}
      {revenueAreaData.length > 1 && (
        <Card>
          <div style={{ display: "flex", alignItems: "baseline", justifyContent: "space-between", marginBottom: DS.cardHeaderMb }}>
            <p style={{...DS.label }}>Monthly Gross Revenue</p>
            {currentMonth && (
              <div style={{ display: "flex", alignItems: "baseline", gap: "0.5rem" }}>
                <span style={{fontWeight: DS.weight.bold, fontSize: DS.text.lg, color: "var(--st-text-primary)", letterSpacing: "-0.02em" }}>
                  {formatCurrency(currentMonth.gross)}
                </span>
                {momDelta != null && (
                  <DeltaBadge delta={momDelta} deltaPercent={momDeltaPct} isCurrency compact />
                )}
              </div>
            )}
          </div>
          <AreaChart data={revenueAreaData} height={200} formatValue={formatCompactCurrency} color={COLORS.member} />
        </Card>
      )}

      {/* Breakdown — simple list */}
      {currentMonth && (
        <Card>
          <p style={{...DS.label, marginBottom: DS.cardHeaderMb }}>
            {formatMonthLabel(currentMonth.month)} Breakdown
          </p>
          <div style={{ display: "flex", flexDirection: "column" }}>
            {[
              { label: "Gross Revenue", value: formatCurrency(currentMonth.gross), bold: true },
              { label: "Net Revenue", value: formatCurrency(currentMonth.net) },
              { label: "Fees + Refunds", value: `-${formatCurrency(currentMonth.gross - currentMonth.net)}`, color: "var(--st-error)" },
            ].map((row, i) => (
              <div key={i} style={{
                display: "flex", justifyContent: "space-between", alignItems: "center",
                padding: "0.6rem 0",
                borderBottom: i < 2 ? "1px solid var(--st-border)" : "none",
                fontSize: DS.text.sm,
              }}>
                <span style={{ color: "var(--st-text-secondary)" }}>{row.label}</span>
                <span style={{ fontWeight: row.bold ? DS.weight.bold : DS.weight.medium, color: row.color || "var(--st-text-primary)" }}>
                  {row.value}
                </span>
              </div>
            ))}
          </div>
        </Card>
      )}
    </div>
  );
}

// ─── MRR Breakdown (standalone) ──────────────────────────────

function MRRBreakdown({ data }: { data: DashboardStats }) {
  const segments = [
    { label: LABELS.members, value: data.mrr.member, color: COLORS.member },
    { label: LABELS.sky3, value: data.mrr.sky3, color: COLORS.sky3 },
    { label: LABELS.tv, value: data.mrr.skyTingTv, color: COLORS.tv },
    ...(data.mrr.unknown > 0 ? [{ label: "Other", value: data.mrr.unknown, color: "#999" }] : []),
  ].filter(s => s.value > 0);

  return (
    <Card>
      <div style={{ display: "flex", alignItems: "baseline", justifyContent: "space-between", marginBottom: DS.cardHeaderMb }}>
        <p style={{...DS.label }}>
          {LABELS.mrr}
        </p>
        <p style={{fontWeight: DS.weight.bold, fontSize: DS.text.lg, color: "var(--st-text-primary)", letterSpacing: "-0.02em" }}>
          {formatCurrency(data.mrr.total)}
        </p>
      </div>
      <div style={{ display: "flex", flexDirection: "column" }}>
        {segments.map((seg, i) => (
          <div key={i} style={{
            display: "flex", justifyContent: "space-between", alignItems: "center",
            padding: "0.6rem 0",
            borderBottom: i < segments.length - 1 ? "1px solid var(--st-border)" : "none",
          }}>
            <div style={{ display: "flex", alignItems: "center", gap: "0.5rem" }}>
              <span style={{ width: "8px", height: "8px", borderRadius: "50%", backgroundColor: seg.color, opacity: 0.85, flexShrink: 0 }} />
              <span style={{fontSize: DS.text.sm, color: "var(--st-text-secondary)" }}>
                {seg.label}
              </span>
            </div>
            <span style={{fontSize: DS.text.sm, fontWeight: DS.weight.bold, color: "var(--st-text-primary)", fontVariantNumeric: "tabular-nums" }}>
              {formatCurrency(seg.value)}
            </span>
          </div>
        ))}
      </div>
    </Card>
  );
}

// ─── Layer 3: First Visits Card ─────────────────────────────

const SEGMENT_LABELS: Record<FirstVisitSegment, string> = {
  introWeek: "Intro Week",
  dropIn: "Drop-In",
  guest: "Guest Pass",
  other: "Other",
};

const SEGMENT_COLORS: Record<FirstVisitSegment, string> = {
  introWeek: "#3A8A8A",
  dropIn: "#8B7340",
  guest: "#5B7FA5",
  other: "#999999",
};

function FirstVisitsCard({ firstVisits }: { firstVisits: FirstVisitData }) {
  const segments: FirstVisitSegment[] = ["introWeek", "dropIn", "guest", "other"];
  const agg = firstVisits.aggregateSegments;
  const aggTotal = agg.introWeek + agg.dropIn + agg.guest + agg.other;

  return (
    <Card>
      <div style={{ display: "flex", alignItems: "baseline", justifyContent: "space-between", marginBottom: DS.cardHeaderMb }}>
        <div style={{ display: "flex", alignItems: "center", gap: "0.5rem" }}>
          <span style={{ width: "8px", height: "8px", borderRadius: "50%", backgroundColor: COLORS.teal, opacity: 0.85 }} />
          <span style={{...DS.label }}>{LABELS.firstVisits}</span>
        </div>
        <span style={{fontWeight: DS.weight.bold, fontSize: DS.text.lg, color: "var(--st-text-primary)", letterSpacing: "-0.02em" }}>
          {formatNumber(firstVisits.currentWeekTotal)}
          <span style={{...DS.label, marginLeft: "0.4rem", fontSize: DS.text.xs }}>this week</span>
        </span>
      </div>
      <p style={{fontSize: DS.text.xs, color: "var(--st-text-secondary)", marginBottom: "0.4rem" }}>Last 5 weeks by source</p>
      <div style={{ display: "flex", flexDirection: "column", gap: "0.3rem" }}>
        {segments.map((seg) => {
          const count = agg[seg] || 0;
          if (count === 0 && aggTotal === 0) return null;
          return (
            <div key={seg} style={{ display: "flex", alignItems: "center", justifyContent: "space-between" }}>
              <div style={{ display: "flex", alignItems: "center", gap: "0.4rem" }}>
                <span style={{ width: "6px", height: "6px", borderRadius: "50%", backgroundColor: SEGMENT_COLORS[seg] }} />
                <span style={{fontSize: DS.text.sm, color: "var(--st-text-secondary)" }}>{SEGMENT_LABELS[seg]}</span>
              </div>
              <span style={{fontWeight: DS.weight.bold, fontSize: DS.text.sm, color: "var(--st-text-primary)", fontVariantNumeric: "tabular-nums" }}>
                {formatNumber(count)}
                {aggTotal > 0 && <span style={{ fontWeight: DS.weight.normal, fontSize: DS.text.xs, color: "var(--st-text-secondary)", marginLeft: "0.3rem" }}>{Math.round((count / aggTotal) * 100)}%</span>}
              </span>
            </div>
          );
        })}
      </div>
    </Card>
  );
}

// ─── Layer 3: Returning Non-Members Card ────────────────────

const RNM_SEGMENT_COLORS: Record<string, string> = {
  dropIn: "#B87333",
  guest: "#5B7FA5",
  other: "#999999",
};

const RNM_SEGMENT_LABELS: Record<string, string> = {
  dropIn: "Drop-In",
  guest: "Guest Pass",
  other: "Other",
};

function ReturningNonMembersCard({ returningNonMembers }: { returningNonMembers: ReturningNonMemberData }) {
  const rnmSegments = ["dropIn", "guest", "other"] as const;
  const agg = returningNonMembers.aggregateSegments;
  const aggDisplay = {
    dropIn: agg.dropIn,
    guest: agg.guest,
    other: agg.other + agg.introWeek,
  };
  const rnmAggTotal = aggDisplay.dropIn + aggDisplay.guest + aggDisplay.other;

  return (
    <Card>
      <div style={{ display: "flex", alignItems: "baseline", justifyContent: "space-between", marginBottom: DS.cardHeaderMb }}>
        <div style={{ display: "flex", alignItems: "center", gap: "0.5rem" }}>
          <span style={{ width: "8px", height: "8px", borderRadius: "50%", backgroundColor: COLORS.copper, opacity: 0.85 }} />
          <span style={{...DS.label }}>{LABELS.returningNonMembers}</span>
        </div>
        <span style={{fontWeight: DS.weight.bold, fontSize: DS.text.lg, color: "var(--st-text-primary)", letterSpacing: "-0.02em" }}>
          {formatNumber(returningNonMembers.currentWeekTotal)}
          <span style={{...DS.label, marginLeft: "0.4rem", fontSize: DS.text.xs }}>this week</span>
        </span>
      </div>
      <p style={{fontSize: DS.text.xs, color: "var(--st-text-secondary)", marginBottom: "0.4rem" }}>Last 5 weeks by source</p>
      <div style={{ display: "flex", flexDirection: "column", gap: "0.3rem" }}>
        {rnmSegments.map((seg) => (
          <div key={seg} style={{ display: "flex", alignItems: "center", justifyContent: "space-between" }}>
            <div style={{ display: "flex", alignItems: "center", gap: "0.4rem" }}>
              <span style={{ width: "6px", height: "6px", borderRadius: "50%", backgroundColor: RNM_SEGMENT_COLORS[seg] }} />
              <span style={{fontSize: DS.text.sm, color: "var(--st-text-secondary)" }}>{RNM_SEGMENT_LABELS[seg]}</span>
            </div>
            <span style={{fontWeight: DS.weight.bold, fontSize: DS.text.sm, color: "var(--st-text-primary)", fontVariantNumeric: "tabular-nums" }}>
              {formatNumber(aggDisplay[seg] || 0)}
              {rnmAggTotal > 0 && <span style={{ fontWeight: DS.weight.normal, fontSize: DS.text.xs, color: "var(--st-text-secondary)", marginLeft: "0.3rem" }}>{Math.round(((aggDisplay[seg] || 0) / rnmAggTotal) * 100)}%</span>}
            </span>
          </div>
        ))}
      </div>
    </Card>
  );
}

// ─── Layer 3: New Customer Volume Card ──────────────────────

function NewCustomerFunnelModule({ volume, cohorts }: {
  volume: NewCustomerVolumeData | null;
  cohorts: NewCustomerCohortData | null;
}) {
  if (!volume && !cohorts) return null;

  const [activeTab, setActiveTab] = useState<"complete" | "inProgress">("complete");
  const [expandedRow, setExpandedRow] = useState<string | null>(null);
  const [hoveredCohort, setHoveredCohort] = useState<string | null>(null);
  const [showChart, setShowChart] = useState(false);

  const today = new Date();
  today.setHours(0, 0, 0, 0);

  function daysElapsed(cohortStart: string): number {
    const start = new Date(cohortStart + "T00:00:00");
    return Math.floor((today.getTime() - start.getTime()) / (1000 * 60 * 60 * 24));
  }

  // Partition cohorts
  const allCohorts = cohorts?.cohorts ?? [];
  const completeCohorts = allCohorts.filter((c) => daysElapsed(c.cohortStart) >= 21);
  const incompleteCohorts = allCohorts.filter((c) => daysElapsed(c.cohortStart) < 21);
  const displayComplete = completeCohorts.slice(-5);

  // Chart data: bars only (new customers per cohort, single purpose)
  const chartData = [...displayComplete, ...incompleteCohorts].map((c) => ({
    cohortStart: c.cohortStart,
    cohortEnd: c.cohortEnd,
    newCustomers: c.newCustomers,
    complete: daysElapsed(c.cohortStart) >= 21,
  }));
  const barMax = Math.max(...chartData.map((d) => d.newCustomers), 1);
  const BAR_H = 80;

  // KPI values
  const avgRate = cohorts?.avgConversionRate ?? null;
  const currentCohortCount = volume?.currentWeekCount ?? (incompleteCohorts.length > 0 ? incompleteCohorts[incompleteCohorts.length - 1].newCustomers : null);
  const expectedAutoRenews = (currentCohortCount !== null && currentCohortCount !== undefined && avgRate !== null)
    ? Math.round(currentCohortCount * avgRate / 100)
    : null;

  // Insight for tooltip
  const insightCohorts = displayComplete.filter((c) => c.total3Week > 0);
  const totalWk1 = insightCohorts.reduce((s, c) => s + c.week1, 0);
  const totalConv = insightCohorts.reduce((s, c) => s + c.total3Week, 0);
  const sameWeekPct = totalConv > 0 ? Math.round((totalWk1 / totalConv) * 100) : null;

  // Tooltips
  const convTooltip = avgRate !== null
    ? `Weighted avg across last ${Math.min(completeCohorts.length, 5)} complete cohorts.${sameWeekPct ? ` ${sameWeekPct}% of conversions happen in the same week.` : ""}`
    : "Needs 3+ complete cohorts";
  const projTooltip = expectedAutoRenews !== null
    ? `${formatNumber(currentCohortCount ?? 0)} new \u00d7 ${avgRate?.toFixed(1)}% conversion. Based on last ${Math.min(completeCohorts.length, 5)} complete cohorts.`
    : "";

  // Table styles — right-align numeric columns flush to right edge, tabular numerals
  const thStyle: React.CSSProperties = { textAlign: "right", padding: "0.3rem 0.5rem", fontSize: "0.6rem", fontWeight: DS.weight.normal, letterSpacing: "0.05em", textTransform: "uppercase", color: "rgba(65, 58, 58, 0.45)" };
  const tdBase: React.CSSProperties = { textAlign: "right", padding: "0.3rem 0.5rem", fontVariantNumeric: "tabular-nums", fontFamily: FONT_SANS };
  const mostRecentComplete = displayComplete.length > 0 ? displayComplete[displayComplete.length - 1].cohortStart : null;

  // Timing bar segment colors — 3 distinct lightness steps on one neutral hue
  // Charcoal → mid → light so segments are clearly distinguishable
  const timingColors = ["rgba(65, 58, 58, 0.55)", "rgba(65, 58, 58, 0.30)", "rgba(65, 58, 58, 0.14)"];

  // Tab style — underline sits above the hairline divider (1px gap via marginBottom)
  const tabStyle = (active: boolean): React.CSSProperties => ({
    padding: "0.25rem 0.6rem 0.3rem",
    marginBottom: "-1px",          // overlap with the container hairline
    fontSize: "0.7rem",
    fontWeight: active ? DS.weight.bold : DS.weight.normal,
    color: active ? "var(--st-text-primary)" : "var(--st-text-secondary)",
    backgroundColor: "transparent",
    border: "none",
    borderBottom: active ? "2px solid var(--st-text-primary)" : "2px solid transparent",
    borderRadius: "0",
    cursor: "pointer",
    transition: "color 0.15s, border-color 0.15s",
    letterSpacing: "0.02em",
    outline: "none",
  });

  // Info icon (24x24 hit area)
  const InfoIcon = ({ tooltip }: { tooltip: string }) => (
    <span
      title={tooltip}
      style={{
        display: "inline-flex", alignItems: "center", justifyContent: "center",
        width: "24px", height: "24px", cursor: "help",
        fontSize: "0.75rem", color: "var(--st-text-secondary)", opacity: 0.5,
      }}
    >
      &#9432;
    </span>
  );

  return (
    <Card>
      {/* ── Module header ── */}
      <div style={{ display: "flex", alignItems: "center", gap: "0.5rem", marginBottom: "0.4rem" }}>
        <span style={{ width: "7px", height: "7px", borderRadius: "50%", backgroundColor: COLORS.newCustomer, opacity: 0.85 }} />
        <span style={{ ...DS.label }}>{LABELS.newCustomerFunnel}</span>
      </div>

      {/* ── KPI Row: 3 blocks, ultra-subtle hairlines, tabular numerals ── */}
      <div style={{
        display: "grid",
        gridTemplateColumns: "1fr 1px 1fr 1px auto",
        alignItems: "stretch",
        padding: "0.4rem 0",
        marginBottom: "0.5rem",
        fontVariantNumeric: "tabular-nums",
      }}>
        {/* Block 1: New customers */}
        <div style={{ padding: "0 0.5rem", display: "flex", flexDirection: "column", justifyContent: "center" }}>
          <div style={{ fontWeight: DS.weight.bold, fontSize: DS.text.lg, fontFamily: FONT_SANS, color: "var(--st-text-primary)", letterSpacing: "-0.02em", lineHeight: 1.1, fontVariantNumeric: "tabular-nums" }}>
            {formatNumber(currentCohortCount ?? 0)}
          </div>
          <div style={{ fontSize: DS.text.xs, color: "var(--st-text-secondary)", marginTop: "0.1rem" }}>
            New customers (this week)
          </div>
        </div>

        {/* Hairline */}
        <div style={{ width: "1px", backgroundColor: "var(--st-border)", opacity: 0.5 }} />

        {/* Block 2: 3-week conversion */}
        <div style={{ padding: "0 0.5rem", display: "flex", flexDirection: "column", justifyContent: "center" }}>
          <div style={{ display: "flex", alignItems: "center", gap: "0.15rem" }}>
            <span style={{ fontWeight: DS.weight.bold, fontSize: DS.text.lg, fontFamily: FONT_SANS, color: "var(--st-text-primary)", letterSpacing: "-0.02em", lineHeight: 1.1, fontVariantNumeric: "tabular-nums" }}>
              {avgRate !== null ? `${avgRate.toFixed(1)}%` : "\u2014"}
            </span>
            <InfoIcon tooltip={convTooltip} />
          </div>
          <div style={{ fontSize: DS.text.xs, color: "var(--st-text-secondary)", marginTop: "0.1rem" }}>
            3-wk conversion
          </div>
        </div>

        {/* Hairline */}
        <div style={{ width: "1px", backgroundColor: "var(--st-border)", opacity: 0.5 }} />

        {/* Block 3: Projected converts */}
        <div style={{ padding: "0 0.5rem", display: "flex", flexDirection: "column", justifyContent: "center" }}>
          <div style={{ display: "flex", alignItems: "center", gap: "0.2rem" }}>
            <span style={{ fontWeight: DS.weight.bold, fontSize: DS.text.lg, fontFamily: FONT_SANS, color: "var(--st-text-primary)", letterSpacing: "-0.02em", lineHeight: 1.1, fontVariantNumeric: "tabular-nums" }}>
              {expectedAutoRenews !== null ? `\u2248${expectedAutoRenews}` : "\u2014"}
            </span>
            {expectedAutoRenews !== null && (
              <span style={{
                fontSize: "0.55rem", fontWeight: DS.weight.medium,
                color: "var(--st-text-secondary)", backgroundColor: "rgba(65, 58, 58, 0.06)",
                padding: "1px 4px", borderRadius: "3px", textTransform: "uppercase", letterSpacing: "0.03em",
              }}>
                Proj
              </span>
            )}
            {expectedAutoRenews !== null && <InfoIcon tooltip={projTooltip} />}
          </div>
          <div style={{ fontSize: DS.text.xs, color: "var(--st-text-secondary)", marginTop: "0.1rem" }}>
            Expected converts
          </div>
        </div>
      </div>

      {/* ── Chart toggle + chart (collapsed by default) ── */}
      {chartData.length > 0 && (
        <div style={{ marginBottom: "1.1rem" }}>
          <button
            type="button"
            onClick={() => setShowChart(!showChart)}
            style={{
              background: "none", border: "none", padding: "0.15rem 0", cursor: "pointer",
              fontSize: DS.text.xs, color: "var(--st-text-secondary)",
              display: "flex", alignItems: "center", gap: "0.3rem",
            }}
          >
            <span style={{ fontSize: "0.6rem", transition: "transform 0.15s", transform: showChart ? "rotate(90deg)" : "rotate(0)" }}>&#9654;</span>
            Show trend
          </button>

          {showChart && (
            <div style={{ marginTop: "0.3rem" }}>
              {/* Bars */}
              <div style={{
                display: "flex", alignItems: "flex-end", gap: "3px",
                height: `${BAR_H}px`, borderBottom: "1px solid var(--st-border)", paddingBottom: "1px",
              }}>
                {chartData.map((d) => {
                  const fraction = barMax > 0 ? d.newCustomers / barMax : 0;
                  const h = Math.max(Math.round(fraction * (BAR_H - 4)), 3);
                  const isHovered = hoveredCohort === d.cohortStart;
                  return (
                    <div
                      key={d.cohortStart}
                      style={{
                        flex: 1, minWidth: 0, display: "flex", flexDirection: "column",
                        alignItems: "center", justifyContent: "flex-end", cursor: "default",
                      }}
                      onMouseEnter={() => setHoveredCohort(d.cohortStart)}
                      onMouseLeave={() => setHoveredCohort(null)}
                      title={`${formatWeekRangeLabel(d.cohortStart, d.cohortEnd)}: ${d.newCustomers} new${d.complete ? "" : " (in progress)"}`}
                    >
                      <div style={{
                        width: "70%", maxWidth: "28px", height: `${h}px`, borderRadius: "2px 2px 0 0",
                        ...(d.complete
                          ? { backgroundColor: COLORS.newCustomer, opacity: isHovered ? 0.85 : 0.65 }
                          : { backgroundColor: "rgba(90, 107, 122, 0.06)", border: `1.5px dashed ${COLORS.newCustomer}`, opacity: isHovered ? 0.5 : 0.35 }
                        ),
                        transition: "opacity 0.15s",
                      }} />
                    </div>
                  );
                })}
              </div>
              {/* X-axis */}
              <div style={{ display: "flex", gap: "3px", marginTop: "2px" }}>
                {chartData.map((d) => (
                  <div key={d.cohortStart} style={{
                    flex: 1, minWidth: 0, textAlign: "center", fontSize: "0.55rem",
                    color: hoveredCohort === d.cohortStart ? "var(--st-text-primary)" : "var(--st-text-secondary)",
                    overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap",
                  }}>
                    {formatCohortShort(d.cohortStart)}
                  </div>
                ))}
              </div>
              {/* Legend — single muted line */}
              <div style={{ display: "flex", gap: "0.6rem", marginTop: "0.15rem" }}>
                <div style={{ display: "flex", alignItems: "center", gap: "0.15rem" }}>
                  <span style={{ width: "6px", height: "4px", borderRadius: "1px", backgroundColor: COLORS.newCustomer, opacity: 0.65, display: "inline-block" }} />
                  <span style={{ fontSize: "0.5rem", color: "var(--st-text-secondary)" }}>Complete</span>
                </div>
                {incompleteCohorts.length > 0 && (
                  <div style={{ display: "flex", alignItems: "center", gap: "0.15rem" }}>
                    <span style={{ width: "6px", height: "4px", borderRadius: "1px", border: `1px dashed ${COLORS.newCustomer}`, opacity: 0.35, display: "inline-block" }} />
                    <span style={{ fontSize: "0.5rem", color: "var(--st-text-secondary)" }}>In progress</span>
                  </div>
                )}
              </div>
            </div>
          )}
        </div>
      )}

      {/* ── Tabs — underline sits above a lighter hairline ── */}
      <div style={{ display: "flex", gap: "0", borderBottom: "1px solid rgba(65, 58, 58, 0.08)", marginBottom: "0" }}>
        <button type="button" className="funnel-tab" style={tabStyle(activeTab === "complete")} onClick={() => setActiveTab("complete")}>
          Complete ({displayComplete.length})
        </button>
        {incompleteCohorts.length > 0 && (
          <button type="button" className="funnel-tab" style={tabStyle(activeTab === "inProgress")} onClick={() => setActiveTab("inProgress")}>
            In progress ({incompleteCohorts.length})
          </button>
        )}
      </div>

      {/* ── Complete table ── */}
      {cohorts && activeTab === "complete" && (
        <div style={{ overflowX: "auto" }}>
          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: DS.text.sm, fontVariantNumeric: "tabular-nums" }}>
            <thead>
              <tr>
                <th style={{ ...thStyle, textAlign: "left" }}>Cohort</th>
                <th style={{ ...thStyle, width: "3.2rem" }}>New</th>
                <th style={{ ...thStyle, width: "3.8rem" }}>Converts</th>
                <th style={{ ...thStyle, width: "3.2rem" }}>Rate</th>
                <th style={{ ...thStyle, textAlign: "center", width: "5rem" }}>Timing</th>
              </tr>
            </thead>
            <tbody>
              {displayComplete.map((c) => {
                const rate = c.newCustomers > 0 ? (c.total3Week / c.newCustomers * 100).toFixed(1) : "0.0";
                const isHovered = hoveredCohort === c.cohortStart;
                const isExpanded = expandedRow === c.cohortStart;
                const isNewest = c.cohortStart === mostRecentComplete;
                return (
                  <Fragment key={c.cohortStart}>
                    <tr
                      style={{
                        borderBottom: isExpanded ? "none" : "1px solid rgba(65, 58, 58, 0.06)",
                        borderLeft: isHovered ? `2px solid ${COLORS.newCustomer}` : "2px solid transparent",
                        backgroundColor: isNewest && !isHovered ? "rgba(65, 58, 58, 0.015)" : "transparent",
                        transition: "border-color 0.15s, background-color 0.15s",
                        cursor: "pointer",
                      }}
                      onMouseEnter={() => setHoveredCohort(c.cohortStart)}
                      onMouseLeave={() => setHoveredCohort(null)}
                      onClick={() => setExpandedRow(isExpanded ? null : c.cohortStart)}
                    >
                      <td style={{ padding: "0.3rem 0.5rem", fontVariantNumeric: "tabular-nums", whiteSpace: "nowrap", fontWeight: DS.weight.medium }}>
                        {formatWeekRangeLabel(c.cohortStart, c.cohortEnd)}
                      </td>
                      <td style={{ ...tdBase, fontWeight: DS.weight.bold, color: "var(--st-text-primary)" }}>
                        {formatNumber(c.newCustomers)}
                      </td>
                      <td style={{ ...tdBase, fontWeight: DS.weight.bold }}>
                        {c.total3Week}
                      </td>
                      <td style={{ ...tdBase, fontWeight: DS.weight.medium, color: "var(--st-text-primary)" }}>
                        {rate}%
                      </td>
                      <td style={{ ...tdBase, textAlign: "center" }}>
                        {/* Mini 3-segment bar — 3 lightness steps, 1px separators, 8px tall */}
                        <div
                          title={`Week 0: ${c.week1} \u00b7 Week 1: ${c.week2} \u00b7 Week 2: ${c.week3}`}
                          style={{ display: "flex", height: "8px", borderRadius: "3px", overflow: "hidden", gap: "1px", minWidth: "3rem", maxWidth: "5rem", margin: "0 auto", backgroundColor: "rgba(65, 58, 58, 0.06)" }}
                        >
                          {[c.week1, c.week2, c.week3].map((w, i) => (
                            <div key={i} style={{
                              flex: Math.max(w, 0.2),
                              backgroundColor: timingColors[i],
                              borderRadius: "1px",
                            }} />
                          ))}
                        </div>
                      </td>
                    </tr>
                    {/* Expanded timing detail */}
                    {isExpanded && (
                      <tr style={{ borderBottom: "1px solid rgba(65, 58, 58, 0.06)", borderLeft: `2px solid ${COLORS.newCustomer}` }}>
                        <td colSpan={5} style={{ padding: "0.15rem 0.5rem 0.25rem 1.5rem", backgroundColor: "rgba(65, 58, 58, 0.015)" }}>
                          <div style={{ display: "flex", gap: "1rem", fontSize: DS.text.xs, color: "var(--st-text-secondary)", fontVariantNumeric: "tabular-nums" }}>
                            <span>Same week: <strong style={{ color: "var(--st-text-primary)", fontWeight: DS.weight.medium }}>{c.week1}</strong></span>
                            <span>+1 week: <strong style={{ color: "var(--st-text-primary)", fontWeight: DS.weight.medium }}>{c.week2}</strong></span>
                            <span>+2 weeks: <strong style={{ color: "var(--st-text-primary)", fontWeight: DS.weight.medium }}>{c.week3}</strong></span>
                          </div>
                        </td>
                      </tr>
                    )}
                  </Fragment>
                );
              })}
            </tbody>
          </table>
        </div>
      )}

      {/* ── In-progress table — COHORT | NEW | CONVERTS | DAYS LEFT ── */}
      {cohorts && activeTab === "inProgress" && (
        <div style={{ overflowX: "auto" }}>
          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: DS.text.sm, fontVariantNumeric: "tabular-nums", tableLayout: "fixed" }}>
            <colgroup>
              <col style={{ width: "42%" }} />
              <col style={{ width: "14%" }} />
              <col style={{ width: "22%" }} />
              <col style={{ width: "22%" }} />
            </colgroup>
            <thead>
              <tr>
                <th style={{ ...thStyle, textAlign: "left" }}>Cohort</th>
                <th style={thStyle}>New</th>
                <th style={thStyle} title="Converts so far (cohort still accumulating)">
                  <span style={{ whiteSpace: "nowrap" }}>Converts</span>
                </th>
                <th style={thStyle}>
                  <span style={{ whiteSpace: "nowrap" }}>Days left</span>
                </th>
              </tr>
            </thead>
            <tbody>
              {incompleteCohorts.map((c) => {
                const days = daysElapsed(c.cohortStart);
                const wk2Possible = days >= 7;
                const isHovered = hoveredCohort === c.cohortStart;
                const isExpanded = expandedRow === c.cohortStart;
                const daysRemaining = Math.max(21 - days, 0);
                const convertsSoFar = c.week1 + (wk2Possible ? c.week2 : 0);
                return (
                  <Fragment key={c.cohortStart}>
                    <tr
                      style={{
                        borderBottom: isExpanded ? "none" : "1px solid rgba(65, 58, 58, 0.06)",
                        borderLeft: isHovered ? `2px solid ${COLORS.newCustomer}` : "2px solid transparent",
                        transition: "border-color 0.15s",
                        cursor: "pointer",
                      }}
                      onMouseEnter={() => setHoveredCohort(c.cohortStart)}
                      onMouseLeave={() => setHoveredCohort(null)}
                      onClick={() => setExpandedRow(isExpanded ? null : c.cohortStart)}
                    >
                      <td style={{ padding: "0.3rem 0.5rem", fontVariantNumeric: "tabular-nums", whiteSpace: "nowrap", fontWeight: DS.weight.medium, overflow: "hidden", textOverflow: "ellipsis" }}>
                        {formatWeekRangeLabel(c.cohortStart, c.cohortEnd)}
                      </td>
                      <td style={{ ...tdBase, fontWeight: DS.weight.bold, color: "var(--st-text-primary)" }}>
                        {formatNumber(c.newCustomers)}
                      </td>
                      <td style={{ ...tdBase, fontWeight: DS.weight.bold, color: "var(--st-text-primary)" }}>
                        {convertsSoFar}
                      </td>
                      <td style={{ ...tdBase, color: "var(--st-text-secondary)" }}>
                        {daysRemaining} {daysRemaining === 1 ? "day" : "days"}
                      </td>
                    </tr>
                    {isExpanded && (
                      <tr style={{ borderBottom: "1px solid rgba(65, 58, 58, 0.06)", borderLeft: `2px solid ${COLORS.newCustomer}` }}>
                        <td colSpan={4} style={{ padding: "0.15rem 0.5rem 0.25rem 1.5rem", backgroundColor: "rgba(65, 58, 58, 0.015)" }}>
                          <div style={{ display: "flex", gap: "1rem", fontSize: DS.text.xs, color: "var(--st-text-secondary)", fontVariantNumeric: "tabular-nums" }}>
                            <span>Same week: <strong style={{ color: "var(--st-text-primary)", fontWeight: DS.weight.medium }}>{c.week1}</strong></span>
                            <span>+1 week: <strong style={{ color: "var(--st-text-primary)", fontWeight: DS.weight.medium }}>{wk2Possible ? c.week2 : "\u2014"}</strong></span>
                            <span>+2 weeks: <strong style={{ color: "var(--st-text-primary)", fontWeight: DS.weight.medium }}>{"\u2014"}</strong></span>
                            <span style={{ color: "var(--st-text-secondary)", opacity: 0.7 }}>{daysRemaining} {daysRemaining === 1 ? "day" : "days"} until complete</span>
                          </div>
                        </td>
                      </tr>
                    )}
                  </Fragment>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
    </Card>
  );
}

// ─── Churn Section ──────────────────────────────────────────

// ─── Non Members Section (First Visits + Returning Non-Members + Drop-Ins) ──

function NonMembersSection({ firstVisits, returningNonMembers, dropIns, newCustomerVolume, newCustomerCohorts }: {
  firstVisits: FirstVisitData | null;
  returningNonMembers: ReturningNonMemberData | null;
  dropIns: DropInData | null;
  newCustomerVolume: NewCustomerVolumeData | null;
  newCustomerCohorts: NewCustomerCohortData | null;
}) {
  if (!firstVisits && !returningNonMembers && !dropIns && !newCustomerVolume && !newCustomerCohorts) return null;

  return (
    <div className="space-y-3">
      <SectionHeader subtitle="Drop-ins, guests, and first-time visitors">{LABELS.nonMembers}</SectionHeader>

      {dropIns && <DropInCardNew dropIns={dropIns} />}
      {(firstVisits || returningNonMembers) && (
        <div className={`grid gap-3 ${firstVisits && returningNonMembers ? "grid-cols-1 sm:grid-cols-2" : "grid-cols-1"}`}>
          {firstVisits && <FirstVisitsCard firstVisits={firstVisits} />}
          {returningNonMembers && <ReturningNonMembersCard returningNonMembers={returningNonMembers} />}
        </div>
      )}
      {(newCustomerVolume || newCustomerCohorts) && (
        <NewCustomerFunnelModule volume={newCustomerVolume} cohorts={newCustomerCohorts} />
      )}
    </div>
  );
}

// ─── Layer 3: Drop-In Card (visits-focused, distinct from CategoryDetail) ──

function DropInCardNew({ dropIns }: { dropIns: DropInData }) {
  const isPacing = dropIns.currentMonthDaysElapsed < dropIns.currentMonthDaysInMonth;
  const prevDelta = dropIns.previousMonthTotal > 0
    ? dropIns.currentMonthTotal - dropIns.previousMonthTotal
    : null;

  // Drop current partial week — only show completed weeks
  const allWeeks = dropIns.weeklyBreakdown;
  const completedDropInWeeks = allWeeks.length > 0 ? allWeeks.slice(0, -1) : [];
  const weeklyBars: BarChartData[] = completedDropInWeeks.map((w) => {
    return { label: formatWeekShort(w.week), value: w.count, color: COLORS.warning };
  });

  return (
    <Card>
      {/* Header */}
      <div className="flex items-center justify-between" style={{ marginBottom: DS.cardHeaderMb }}>
        <div className="flex items-center gap-2">
          <span className="rounded-full" style={{ width: "8px", height: "8px", backgroundColor: COLORS.warning, opacity: 0.85 }} />
          <span style={{fontWeight: DS.weight.medium, fontSize: DS.text.sm, color: "var(--st-text-secondary)", textTransform: "uppercase", letterSpacing: "0.05em" }}>
            {LABELS.dropIns}
          </span>
          <span style={{fontWeight: DS.weight.normal, fontSize: DS.text.xs, color: "var(--st-text-secondary)", fontStyle: "italic" }}>
            Visits
          </span>
        </div>
        <div className="flex flex-col items-end">
          <span style={{fontWeight: DS.weight.bold, fontSize: DS.text.lg, color: "var(--st-text-primary)", letterSpacing: "-0.02em", lineHeight: 1 }}>
            {formatNumber(dropIns.currentMonthTotal)}
          </span>
          <span style={{...DS.label, marginTop: "0.25rem" }}>
            Month to Date
          </span>
        </div>
      </div>

      <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
        {/* Left: Weekly visits chart */}
        <div>
          <p style={{...DS.label, marginBottom: DS.cardHeaderMb }}>
            Weekly Visits
          </p>
          {weeklyBars.length > 0 ? (
            <MiniBarChart data={weeklyBars} height={70} />
          ) : (
            <p style={{ color: "var(--st-text-secondary)", fontSize: DS.text.sm }}>—</p>
          )}
        </div>

        {/* Right: Key metrics */}
        <div style={{ borderLeft: "1px solid var(--st-border)", paddingLeft: "1rem" }}>
          <TrendRow
            label="Month to Date"
            value={formatNumber(dropIns.currentMonthTotal)}
            delta={prevDelta}
            deltaPercent={null}
            sublabel={isPacing ? `Projected month-end: ${formatNumber(dropIns.currentMonthPaced)}` : undefined}
          />
          <TrendRow
            label="Last Month"
            value={formatNumber(dropIns.previousMonthTotal)}
            delta={null}
            deltaPercent={null}
          />
          <TrendRow
            label="Weekly Avg"
            value={formatNumber(dropIns.weeklyAvg6w)}
            delta={null}
            deltaPercent={null}
            sublabel="6-week rolling"
            isLast
          />
        </div>
      </div>
    </Card>
  );
}

// ─── Category Detail Card (Members / SKY3 / TV) ─────────────
// Clean card: big count, simple metric rows, no chart clutter

function CategoryDetail({ title, color, count, weekly, monthly, pacing, weeklyKeyNew, weeklyKeyChurn, weeklyKeyNet, pacingNew, pacingChurn, churnData }: {
  title: string;
  color: string;
  count: number;
  weekly: TrendRowData[];
  monthly: TrendRowData[];
  pacing: PacingData | null;
  weeklyKeyNew: (r: TrendRowData) => number;
  weeklyKeyChurn: (r: TrendRowData) => number;
  weeklyKeyNet: (r: TrendRowData) => number;
  pacingNew?: (p: PacingData) => { actual: number; paced: number };
  pacingChurn?: (p: PacingData) => { actual: number; paced: number };
  churnData?: CategoryChurnData;
}) {
  const completedWeekly = weekly.length > 1 ? weekly.slice(0, -1) : weekly;
  const latestW = completedWeekly.length >= 1 ? completedWeekly[completedWeekly.length - 1] : null;
  const prevW = completedWeekly.length >= 2 ? completedWeekly[completedWeekly.length - 2] : null;
  const isPacing = pacing && pacing.daysElapsed < pacing.daysInMonth;

  // Build metric rows
  const metrics: { label: string; value: string; delta?: number | null; deltaPercent?: number | null; isPositiveGood?: boolean; color?: string }[] = [];

  if (latestW) {
    const newVal = weeklyKeyNew(latestW);
    const newDelta = prevW ? newVal - weeklyKeyNew(prevW) : null;
    const newDeltaPct = prevW && weeklyKeyNew(prevW) > 0
      ? Math.round((newDelta! / weeklyKeyNew(prevW)) * 100)
      : null;
    metrics.push({ label: "New this week", value: String(newVal), delta: newDelta, deltaPercent: newDeltaPct, isPositiveGood: true });

    const churnVal = weeklyKeyChurn(latestW);
    const churnDelta = prevW ? -(churnVal - weeklyKeyChurn(prevW)) : null;
    metrics.push({ label: "Churned this week", value: String(churnVal), delta: churnDelta, isPositiveGood: true });

    const netVal = weeklyKeyNet(latestW);
    metrics.push({
      label: "Net change",
      value: formatDelta(netVal) || "0",
      color: netVal > 0 ? COLORS.success : netVal < 0 ? COLORS.error : undefined,
    });
  }

  if (churnData) {
    metrics.push({
      label: "User churn rate (avg/mo)",
      value: `${churnData.avgUserChurnRate.toFixed(1)}%`,
      color: churnBenchmarkColor(churnData.avgUserChurnRate),
    });
    metrics.push({
      label: "MRR churn rate (avg/mo)",
      value: `${churnData.avgMrrChurnRate.toFixed(1)}%`,
      color: churnBenchmarkColor(churnData.avgMrrChurnRate),
    });
    if (churnData.atRiskCount > 0) {
      metrics.push({
        label: "At risk",
        value: String(churnData.atRiskCount),
        color: COLORS.warning,
      });
    }
  }

  return (
    <Card>
      {/* Header: title + big count */}
      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: DS.cardHeaderMb }}>
        <div style={{ display: "flex", alignItems: "center", gap: "0.5rem" }}>
          <span style={{ width: "10px", height: "10px", borderRadius: "50%", backgroundColor: color, opacity: 0.85, flexShrink: 0 }} />
          <span style={{fontWeight: DS.weight.medium, fontSize: DS.text.sm, color: "var(--st-text-secondary)", textTransform: "uppercase", letterSpacing: "0.05em" }}>
            {title}
          </span>
        </div>
        <span style={{fontWeight: DS.weight.bold, fontSize: DS.text.xl, color: "var(--st-text-primary)", letterSpacing: "-0.03em" }}>
          {formatNumber(count)}
        </span>
      </div>

      {/* Metric rows — clean table style */}
      <div style={{ display: "flex", flexDirection: "column" }}>
        {metrics.map((m, i) => (
          <div key={i} style={{
            display: "flex", justifyContent: "space-between", alignItems: "center",
            padding: "0.5rem 0",
            borderBottom: i < metrics.length - 1 ? "1px solid var(--st-border)" : "none",
          }}>
            <span style={{fontSize: DS.text.sm, color: "var(--st-text-secondary)" }}>
              {m.label}
            </span>
            <div style={{ display: "flex", alignItems: "center", gap: "0.5rem" }}>
              <span style={{
                fontSize: DS.text.md, fontWeight: DS.weight.bold,
                color: m.color || "var(--st-text-primary)",
                fontVariantNumeric: "tabular-nums",
              }}>
                {m.value}
              </span>
              {m.delta != null && (
                <DeltaBadge delta={m.delta} deltaPercent={m.deltaPercent ?? null} isPositiveGood={m.isPositiveGood} compact />
              )}
            </div>
          </div>
        ))}
      </div>

      {/* Pacing (if mid-month) */}
      {isPacing && pacingNew && (
        <div style={{
          marginTop: "0.75rem", paddingTop: "0.75rem",
          borderTop: "1px solid var(--st-border)",
          fontSize: DS.text.xs, color: "var(--st-text-secondary)",
          display: "flex", gap: "1rem", flexWrap: "wrap",
        }}>
          <span style={{ textTransform: "uppercase", letterSpacing: "0.04em", fontWeight: DS.weight.medium, color: "var(--st-accent)" }}>
            Pacing ({pacing!.daysElapsed}/{pacing!.daysInMonth}d)
          </span>
          <span><b style={{ color: "var(--st-text-primary)" }}>{pacingNew(pacing!).actual}</b> new / {pacingNew(pacing!).paced} proj</span>
          {pacingChurn && (
            <span><b style={{ color: "var(--st-text-primary)" }}>{pacingChurn(pacing!).actual}</b> churn / {pacingChurn(pacing!).paced} proj</span>
          )}
        </div>
      )}

      {/* MEMBER annual/monthly breakdown */}
      {churnData?.category === "MEMBER" && (() => {
        const lastCompleted = churnData.monthly.length >= 2 ? churnData.monthly[churnData.monthly.length - 2] : null;
        if (!lastCompleted || !lastCompleted.annualActiveAtStart) return null;
        return (
          <p style={{fontSize: DS.text.xs, color: "var(--st-text-secondary)", fontStyle: "italic", marginTop: "0.5rem" }}>
            Annual: {lastCompleted.annualCanceledCount}/{lastCompleted.annualActiveAtStart} churned | Monthly: {lastCompleted.monthlyCanceledCount}/{lastCompleted.monthlyActiveAtStart} churned
          </p>
        );
      })()}
    </Card>
  );
}

// ─── Annual Revenue Comparison (single card with vertical bars) ──────────

function AnnualRevenueCard({ monthlyRevenue, projection }: {
  monthlyRevenue: { month: string; gross: number; net: number }[];
  projection?: ProjectionData | null;
}) {
  const currentYear = new Date().getFullYear();
  const priorYear = currentYear - 1;
  const twoYearsAgo = currentYear - 2;

  const priorData = monthlyRevenue.filter((m) => m.month.startsWith(String(priorYear)));
  const olderData = monthlyRevenue.filter((m) => m.month.startsWith(String(twoYearsAgo)));

  if (olderData.length === 0 && priorData.length === 0) return null;

  const olderTotal = olderData.reduce((s, m) => s + m.net, 0);
  const priorTotal = priorData.reduce((s, m) => s + m.net, 0);
  const yoyDelta = olderTotal > 0 ? ((priorTotal - olderTotal) / olderTotal * 100) : 0;
  const maxVal = Math.max(olderTotal, priorTotal, 1);
  const BAR_HEIGHT = 100;

  const bars = [
    { year: twoYearsAgo, value: olderTotal, color: "var(--st-text-secondary)" },
    { year: priorYear, value: priorTotal, color: COLORS.member },
  ].filter((b) => b.value > 0);

  return (
    <Card>
      <div style={{ display: "flex", alignItems: "baseline", justifyContent: "space-between", marginBottom: DS.cardHeaderMb }}>
        <p style={{...DS.label }}>Annual Net Revenue</p>
        {olderTotal > 0 && priorTotal > 0 && (
          <DeltaBadge delta={priorTotal - olderTotal} deltaPercent={Math.round(yoyDelta * 10) / 10} isCurrency compact />
        )}
      </div>

      {/* Vertical bar chart */}
      <div style={{ display: "flex", alignItems: "flex-end", justifyContent: "center", gap: "2rem", height: `${BAR_HEIGHT}px`, marginBottom: "0.5rem" }}>
        {bars.map((b) => {
          const fraction = b.value / maxVal;
          const h = Math.max(Math.round(fraction * (BAR_HEIGHT - 20)), 8);
          return (
            <div key={b.year} style={{ display: "flex", flexDirection: "column", alignItems: "center", justifyContent: "flex-end", flex: 1, maxWidth: "120px" }}>
              <span style={{fontWeight: DS.weight.bold, fontSize: DS.text.sm, color: "var(--st-text-primary)", marginBottom: "4px" }}>
                {formatCompactCurrency(b.value)}
              </span>
              <div style={{
                width: "100%",
                height: `${h}px`,
                borderRadius: "4px 4px 0 0",
                backgroundColor: b.color,
                opacity: 0.85,
              }} />
            </div>
          );
        })}
      </div>

      {/* Year labels */}
      <div style={{ display: "flex", justifyContent: "center", gap: "2rem" }}>
        {bars.map((b) => (
          <div key={b.year} style={{ flex: 1, maxWidth: "120px", textAlign: "center" }}>
            <span style={{fontSize: DS.text.sm, fontWeight: DS.weight.medium, color: "var(--st-text-secondary)" }}>
              {b.year}
            </span>
          </div>
        ))}
      </div>

      {/* MRR note if available */}
      {projection && projection.currentMRR > 0 && Math.abs(projection.monthlyGrowthRate) < 50 && (
        <div style={{ borderTop: "1px solid var(--st-border)", marginTop: "0.75rem", paddingTop: "0.75rem" }}>
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "baseline" }}>
            <span style={{fontSize: DS.text.sm, color: "var(--st-text-secondary)" }}>
              {projection.year} Subscription MRR
            </span>
            <span style={{fontWeight: DS.weight.bold, fontSize: DS.text.sm, color: "var(--st-text-primary)" }}>
              {formatCurrency(projection.currentMRR)}
            </span>
          </div>
          <p style={{fontSize: DS.text.xs, color: "var(--st-text-secondary)", marginTop: "2px" }}>
            {projection.monthlyGrowthRate > 0 ? "+" : ""}{projection.monthlyGrowthRate}% monthly growth
          </p>
        </div>
      )}
    </Card>
  );
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
//  DASHBOARD VIEW
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

function DashboardView() {
  const [loadState, setLoadState] = useState<DashboardLoadState>({ state: "loading" });

  useEffect(() => {
    fetch("/api/stats")
      .then(async (res) => {
        if (res.status === 503) {
          setLoadState({ state: "not-configured" });
          return;
        }
        if (!res.ok) {
          const err = await res.json();
          setLoadState({
            state: "error",
            message: err.error || "Failed to load stats",
          });
          return;
        }
        const data = await res.json();
        setLoadState({ state: "loaded", data });
      })
      .catch(() => {
        setLoadState({ state: "error", message: "Network error" });
      });
  }, []);

  if (loadState.state === "loading") {
    return (
      <div className="min-h-screen flex items-center justify-center">
        <p style={{ color: "var(--st-text-secondary)", fontWeight: DS.weight.normal }}>
          Loading...
        </p>
      </div>
    );
  }

  if (loadState.state === "not-configured") {
    return (
      <div className="min-h-screen flex flex-col items-center justify-center p-8">
        <div className="max-w-md text-center space-y-4">
          <SkyTingLogo />
          <h1 style={{ color: "var(--st-text-primary)", fontWeight: DS.weight.bold, fontSize: DS.text.xl }}>
            Studio Dashboard
          </h1>
          <div className="rounded-2xl p-6" style={{ backgroundColor: "var(--st-bg-card)", border: "1px solid var(--st-border)" }}>
            <p style={{ color: "var(--st-text-secondary)" }}>
              No analytics data yet. Run the pipeline to see stats here.
            </p>
          </div>
        </div>
      </div>
    );
  }

  if (loadState.state === "error") {
    return (
      <div className="min-h-screen flex flex-col items-center justify-center p-8">
        <div className="max-w-md text-center space-y-4">
          <SkyTingLogo />
          <h1 style={{ color: "var(--st-text-primary)", fontWeight: DS.weight.bold, fontSize: DS.text.xl }}>
            Studio Dashboard
          </h1>
          <div className="rounded-2xl p-5 text-center" style={{ backgroundColor: "#F5EFEF", border: "1px solid rgba(160, 64, 64, 0.2)" }}>
            <p className="font-medium" style={{ color: "var(--st-error)" }}>
              Unable to load stats
            </p>
            <p className="text-sm mt-1" style={{ color: "var(--st-error)", opacity: 0.85 }}>
              {loadState.message}
            </p>
          </div>
        </div>
      </div>
    );
  }

  const { data } = loadState;
  const trends = data.trends;
  const weekly = trends?.weekly || [];
  const monthly = trends?.monthly || [];
  const pacing = trends?.pacing || null;

  return (
    <div className="min-h-screen p-4 sm:p-6 lg:p-8 pb-16" style={{ backgroundColor: "var(--st-bg-section)", fontFamily: FONT_SANS }}>
      {/* ── Header ─────────────────────────────────── */}
      <div style={{ textAlign: "center", paddingTop: "1rem", marginBottom: "2rem" }}>
        <div style={{ display: "flex", justifyContent: "center", marginBottom: "0.5rem" }}>
          <SkyTingLogo />
        </div>
        <h1
          style={{
            color: "var(--st-text-primary)",
                       fontWeight: DS.weight.bold,
            fontSize: DS.text.lg,
            letterSpacing: "-0.01em",
            marginBottom: "1rem",
          }}
        >
          Studio Dashboard
        </h1>
        <FreshnessBadge lastUpdated={data.lastUpdated} spreadsheetUrl={data.spreadsheetUrl} dataSource={data.dataSource} />
      </div>

      {/* ━━ Single column — centered, spacious ━━━━━━━━ */}
      <div style={{ maxWidth: "960px", width: "100%", margin: "0 auto", padding: "0 1rem", display: "flex", flexDirection: "column", gap: "2rem", boxSizing: "border-box" }}>

        {/* ── KPI Hero Strip ── */}
        <KPIHeroStrip tiles={(() => {
          const latestW = weekly.length >= 1 ? weekly[weekly.length - 1] : null;
          const inStudioCount = data.activeSubscribers.member + data.activeSubscribers.sky3;
          const inStudioNet = latestW
            ? latestW.netMemberGrowth + latestW.netSky3Growth
            : null;
          const digitalNet = latestW
            ? latestW.newSkyTingTv - latestW.skyTingTvChurn
            : null;

          const nowDate = new Date();
          const currentMonthKey = `${nowDate.getFullYear()}-${String(nowDate.getMonth() + 1).padStart(2, "0")}`;
          const mr = (data.monthlyRevenue || []).filter((m) => m.month < currentMonthKey);
          const latestMonthly = mr.length > 0 ? mr[mr.length - 1] : null;
          const prevMonthly = mr.length > 1 ? mr[mr.length - 2] : null;
          const heroLabel = latestMonthly
            ? `${formatMonthLabel(latestMonthly.month)} Revenue`
            : "Revenue MTD";
          const heroValue = latestMonthly
            ? latestMonthly.gross
            : data.currentMonthRevenue;
          const heroSublabel = prevMonthly
            ? `${formatMonthLabel(prevMonthly.month)}: ${formatCurrency(prevMonthly.gross)}`
            : data.previousMonthRevenue > 0
              ? `Last month: ${formatCurrency(data.previousMonthRevenue)}`
              : undefined;

          const tiles: HeroTile[] = [
            {
              label: heroLabel,
              value: formatCurrency(heroValue),
              sublabel: heroSublabel,
            },
            {
              label: "In-Studio Subscribers",
              value: formatNumber(inStudioCount),
              delta: inStudioNet,
              sublabel: inStudioNet != null ? "net this week" : undefined,
            },
            {
              label: LABELS.tv,
              value: formatNumber(data.activeSubscribers.skyTingTv),
              delta: digitalNet,
              sublabel: digitalNet != null ? "net this week" : undefined,
            },
          ];
          return tiles;
        })()} />

        {/* ── Revenue ── */}
        <RevenueSection data={data} trends={trends} />

        {/* ── MRR + Year-over-Year side by side ── */}
        <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
          <MRRBreakdown data={data} />
          {data.monthOverMonth ? (
            <Card>
              <div style={{ display: "flex", alignItems: "baseline", justifyContent: "space-between", marginBottom: DS.cardHeaderMb }}>
                <p style={{...DS.label }}>{LABELS.yoy}</p>
                <p style={{fontWeight: DS.weight.bold, fontSize: DS.text.lg, color: "var(--st-text-primary)", letterSpacing: "-0.02em" }}>
                  {formatCurrency(data.monthOverMonth.current?.gross ?? 0)}
                </p>
              </div>
              <div style={{ display: "flex", flexDirection: "column" }}>
                <div style={{ display: "flex", justifyContent: "space-between", padding: "0.6rem 0", borderBottom: "1px solid var(--st-border)" }}>
                  <span style={{fontSize: DS.text.sm, color: "var(--st-text-secondary)" }}>
                    {data.monthOverMonth.monthName} {data.monthOverMonth.priorYear?.year}
                  </span>
                  <span style={{fontSize: DS.text.sm, fontWeight: DS.weight.bold, color: "var(--st-text-secondary)", fontVariantNumeric: "tabular-nums" }}>
                    {formatCurrency(data.monthOverMonth.priorYear?.gross ?? 0)}
                  </span>
                </div>
                <div style={{ display: "flex", justifyContent: "space-between", padding: "0.6rem 0", borderBottom: "1px solid var(--st-border)" }}>
                  <span style={{fontSize: DS.text.sm, color: "var(--st-text-secondary)" }}>
                    {data.monthOverMonth.monthName} {data.monthOverMonth.current?.year}
                  </span>
                  <span style={{fontSize: DS.text.sm, fontWeight: DS.weight.bold, color: "var(--st-text-primary)", fontVariantNumeric: "tabular-nums" }}>
                    {formatCurrency(data.monthOverMonth.current?.gross ?? 0)}
                  </span>
                </div>
                {data.monthOverMonth.yoyGrossPct !== null && (
                  <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", padding: "0.6rem 0" }}>
                    <span style={{fontSize: DS.text.sm, color: "var(--st-text-secondary)" }}>Change</span>
                    <DeltaBadge delta={data.monthOverMonth.yoyGrossChange ?? null} deltaPercent={data.monthOverMonth.yoyGrossPct} isCurrency compact />
                  </div>
                )}
              </div>
            </Card>
          ) : (
            <Card>
              <p style={{...DS.label, marginBottom: DS.cardHeaderMb }}>Year over Year</p>
              <p style={{fontSize: DS.text.sm, color: "var(--st-text-secondary)", opacity: 0.6 }}>No data available</p>
            </Card>
          )}
        </div>

        {/* ── Auto-Renews ── */}
        <div style={{ display: "flex", flexDirection: "column", gap: "1rem" }}>
          <SectionHeader>{LABELS.autoRenews}</SectionHeader>

          <CategoryDetail
            title={LABELS.members}
            color={COLORS.member}
            count={data.activeSubscribers.member}
            weekly={weekly}
            monthly={monthly}
            pacing={pacing}
            weeklyKeyNew={(r) => r.newMembers}
            weeklyKeyChurn={(r) => r.memberChurn}
            weeklyKeyNet={(r) => r.netMemberGrowth}
            pacingNew={(p) => ({ actual: p.newMembersActual, paced: p.newMembersPaced })}
            pacingChurn={(p) => ({ actual: p.memberCancellationsActual, paced: p.memberCancellationsPaced })}
            churnData={trends?.churnRates?.byCategory?.member}
          />

          <CategoryDetail
            title={LABELS.sky3}
            color={COLORS.sky3}
            count={data.activeSubscribers.sky3}
            weekly={weekly}
            monthly={monthly}
            pacing={pacing}
            weeklyKeyNew={(r) => r.newSky3}
            weeklyKeyChurn={(r) => r.sky3Churn}
            weeklyKeyNet={(r) => r.netSky3Growth}
            pacingNew={(p) => ({ actual: p.newSky3Actual, paced: p.newSky3Paced })}
            pacingChurn={(p) => ({ actual: p.sky3CancellationsActual, paced: p.sky3CancellationsPaced })}
            churnData={trends?.churnRates?.byCategory?.sky3}
          />

          <CategoryDetail
            title={LABELS.tv}
            color={COLORS.tv}
            count={data.activeSubscribers.skyTingTv}
            weekly={weekly}
            monthly={monthly}
            pacing={pacing}
            weeklyKeyNew={(r) => r.newSkyTingTv}
            weeklyKeyChurn={(r) => r.skyTingTvChurn}
            weeklyKeyNet={(r) => r.newSkyTingTv - r.skyTingTvChurn}
            churnData={trends?.churnRates?.byCategory?.skyTingTv}
          />
        </div>

        {/* ── Non Members ── */}
        {(trends?.firstVisits || trends?.returningNonMembers || trends?.dropIns || trends?.newCustomerVolume || trends?.newCustomerCohorts) ? (
          <NonMembersSection firstVisits={trends?.firstVisits ?? null} returningNonMembers={trends?.returningNonMembers ?? null} dropIns={trends?.dropIns ?? null} newCustomerVolume={trends?.newCustomerVolume ?? null} newCustomerCohorts={trends?.newCustomerCohorts ?? null} />
        ) : (
          <NoData label="Non-Members (First Visits, Drop-Ins)" />
        )}

        {/* ── Annual Revenue Comparison ── */}
        {data.monthlyRevenue && data.monthlyRevenue.length > 0 && (
          <div className="grid gap-3 grid-cols-1 sm:grid-cols-2">
            <AnnualRevenueCard monthlyRevenue={data.monthlyRevenue} projection={trends?.projection} />
          </div>
        )}

        {/* ── Footer ── */}
        <div style={{ textAlign: "center", paddingTop: "2rem", paddingBottom: "1rem" }}>
          <div style={{ display: "flex", justifyContent: "center", gap: "2rem" }}>
            <NavLink href="/settings">Settings</NavLink>
            <NavLink href="/results">Results</NavLink>
          </div>
        </div>
      </div>
    </div>
  );
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
//  MAIN — auto-detects local vs production
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

export default function Home() {
  const [mode, setMode] = useState<AppMode>("loading");

  useEffect(() => {
    const qs = new URLSearchParams(window.location.search);
    const modeParam = qs.get("mode");
    const url = modeParam ? `/api/mode?mode=${modeParam}` : "/api/mode";
    fetch(url)
      .then((res) => res.json())
      .then((data) => setMode(data.mode))
      .catch(() => setMode("pipeline"));
  }, []);

  if (mode === "loading") {
    return (
      <div className="min-h-screen flex items-center justify-center">
        <p style={{ color: "var(--st-text-secondary)", fontWeight: DS.weight.normal }}>
          Loading...
        </p>
      </div>
    );
  }

  if (mode === "pipeline") {
    return <PipelineView />;
  }

  return <DashboardView />;
}
