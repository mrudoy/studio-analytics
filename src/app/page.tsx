"use client";

import { useState, useEffect, useRef } from "react";

// ─── Types ───────────────────────────────────────────────────

interface DashboardStats {
  lastUpdated: string | null;
  dateRange: string | null;
  spreadsheetUrl?: string;
  dataSource?: "database" | "sheets" | "hybrid";
  mrr: {
    member: number;
    sky3: number;
    skyTingTv: number;
    unknown: number;
    total: number;
  };
  activeSubscribers: {
    member: number;
    sky3: number;
    skyTingTv: number;
    unknown: number;
    total: number;
  };
  arpu: {
    member: number;
    sky3: number;
    skyTingTv: number;
    overall: number;
  };
  currentMonthRevenue: number;
  previousMonthRevenue: number;
  trends?: TrendsData | null;
  revenueCategories?: RevenueCategoryData | null;
  monthOverMonth?: MonthOverMonthData | null;
  monthlyRevenue?: { month: string; gross: number; net: number }[];
}

interface RevenueCategoryData {
  periodStart: string;
  periodEnd: string;
  categories: { category: string; revenue: number; netRevenue: number; fees: number; refunded: number }[];
  totalRevenue: number;
  totalNetRevenue: number;
  totalFees: number;
  totalRefunded: number;
  dropInRevenue: number;
  dropInNetRevenue: number;
  autoRenewRevenue: number;
  autoRenewNetRevenue: number;
  workshopRevenue: number;
  otherRevenue: number;
}

interface TrendRowData {
  period: string;
  type: string;
  newMembers: number;
  newSky3: number;
  newSkyTingTv: number;
  memberChurn: number;
  sky3Churn: number;
  skyTingTvChurn: number;
  netMemberGrowth: number;
  netSky3Growth: number;
  revenueAdded: number;
  revenueLost: number;
  deltaNewMembers: number | null;
  deltaNewSky3: number | null;
  deltaRevenue: number | null;
  deltaPctNewMembers: number | null;
  deltaPctNewSky3: number | null;
  deltaPctRevenue: number | null;
}

interface PacingData {
  month: string;
  daysElapsed: number;
  daysInMonth: number;
  newMembersActual: number;
  newMembersPaced: number;
  newSky3Actual: number;
  newSky3Paced: number;
  revenueActual: number;
  revenuePaced: number;
  memberCancellationsActual: number;
  memberCancellationsPaced: number;
  sky3CancellationsActual: number;
  sky3CancellationsPaced: number;
}

interface ProjectionData {
  year: number;
  projectedAnnualRevenue: number;
  currentMRR: number;
  projectedYearEndMRR: number;
  monthlyGrowthRate: number;
  priorYearRevenue: number;
  priorYearActualRevenue: number | null;
}

interface DropInData {
  currentMonthTotal: number;
  currentMonthDaysElapsed: number;
  currentMonthDaysInMonth: number;
  currentMonthPaced: number;
  previousMonthTotal: number;
  weeklyAvg6w: number;
  weeklyBreakdown: { week: string; count: number }[];
}

type FirstVisitSegment = "introWeek" | "dropIn" | "guest" | "other";

interface FirstVisitData {
  currentWeekTotal: number;
  currentWeekSegments: Record<FirstVisitSegment, number>;
  completedWeeks: { week: string; uniqueVisitors: number; segments: Record<FirstVisitSegment, number> }[];
  aggregateSegments: Record<FirstVisitSegment, number>;
  otherBreakdownTop5?: { passName: string; count: number }[];
}

interface ReturningNonMemberData {
  currentWeekTotal: number;
  currentWeekSegments: Record<FirstVisitSegment, number>;
  completedWeeks: { week: string; uniqueVisitors: number; segments: Record<FirstVisitSegment, number> }[];
  aggregateSegments: Record<FirstVisitSegment, number>;
  otherBreakdownTop5?: { passName: string; count: number }[];
}

interface CategoryMonthlyChurn {
  month: string;
  userChurnRate: number;
  mrrChurnRate: number;
  activeAtStart: number;
  activeMrrAtStart: number;
  canceledCount: number;
  canceledMrr: number;
  annualCanceledCount?: number;
  annualActiveAtStart?: number;
  monthlyCanceledCount?: number;
  monthlyActiveAtStart?: number;
}

interface CategoryChurnData {
  category: "MEMBER" | "SKY3" | "SKY_TING_TV";
  monthly: CategoryMonthlyChurn[];
  avgUserChurnRate: number;
  avgMrrChurnRate: number;
  atRiskCount: number;
}

interface ChurnRateData {
  byCategory: {
    member: CategoryChurnData;
    sky3: CategoryChurnData;
    skyTingTv: CategoryChurnData;
  };
  totalAtRisk: number;
  monthly: {
    month: string;
    memberRate: number;
    sky3Rate: number;
    memberActiveStart: number;
    sky3ActiveStart: number;
    memberCanceled: number;
    sky3Canceled: number;
  }[];
  avgMemberRate: number;
  avgSky3Rate: number;
  atRisk: number;
}

interface MonthOverMonthPeriod {
  year: number;
  periodStart: string;
  periodEnd: string;
  gross: number;
  net: number;
  categoryCount: number;
}

interface MonthOverMonthData {
  month: number;
  monthName: string;
  current: MonthOverMonthPeriod | null;
  priorYear: MonthOverMonthPeriod | null;
  yoyGrossChange: number | null;
  yoyNetChange: number | null;
  yoyGrossPct: number | null;
  yoyNetPct: number | null;
}

interface TrendsData {
  weekly: TrendRowData[];
  monthly: TrendRowData[];
  pacing: PacingData | null;
  projection: ProjectionData | null;
  dropIns: DropInData | null;
  firstVisits: FirstVisitData | null;
  returningNonMembers: ReturningNonMemberData | null;
  churnRates: ChurnRateData | null;
}

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
    xs: "0.7rem",      // uppercase labels, captions, fine print
    sm: "0.85rem",     // secondary text, sublabels, table content
    md: "1rem",        // body text, card titles, category names
    lg: "1.5rem",      // metric values, card hero numbers
    xl: "2.2rem",      // page-level hero KPIs (only in KPIHeroStrip and CategoryDetail headers)
  },
  // Font weights (3 only)
  weight: {
    normal: 500,
    medium: 600,
    bold: 700,
  },
  // Spacing scale
  space: {
    xs: "0.25rem",     // tight gaps
    sm: "0.5rem",      // small inner gaps
    md: "1rem",        // standard inner padding/gaps
    lg: "1.25rem",     // card padding (universal)
    xl: "2rem",        // section spacing
  },
  // Card padding — one value everywhere
  cardPad: "1.25rem",
  // Uppercase label style
  label: {
    fontSize: "0.7rem",
    fontWeight: 600,
    letterSpacing: "0.05em",
    textTransform: "uppercase" as const,
    color: "var(--st-text-secondary)",
  },
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
  return n.toLocaleString("en-US", {
    style: "currency",
    currency: "USD",
    minimumFractionDigits: 0,
    maximumFractionDigits: 0,
  });
}

function formatCurrencyDecimal(n: number): string {
  return n.toLocaleString("en-US", {
    style: "currency",
    currency: "USD",
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  });
}

function formatCompactCurrency(n: number): string {
  if (n >= 1000) {
    return "$" + (n / 1000).toFixed(n % 1000 === 0 ? 0 : 1) + "k";
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
  if (n > 0) return `+${n}`;
  return String(n);
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

function formatWeekLabel(period: string): string {
  const d = new Date(period + "T00:00:00");
  if (isNaN(d.getTime())) return period;
  return d.toLocaleDateString("en-US", { month: "short", day: "numeric" });
}

function formatWeekRange(period: string): string {
  const start = new Date(period + "T00:00:00");
  if (isNaN(start.getTime())) return period;
  const end = new Date(start);
  end.setDate(end.getDate() + 6);
  const fmt = (d: Date) => d.toLocaleDateString("en-US", { month: "short", day: "numeric" });
  return `${fmt(start)} \u2013 ${fmt(end)}`;
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
      style={{ color: "var(--st-text-secondary)", fontFamily: FONT_SANS, fontWeight: DS.weight.normal }}
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
              fontFamily: FONT_SANS,
              fontWeight: 700,
              fontSize: "2.4rem",
              letterSpacing: "-0.03em",
            }}
          >
            Studio Analytics
          </h1>
          <p style={{ color: "var(--st-text-secondary)", fontFamily: FONT_SANS, fontSize: "0.95rem" }}>
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
              fontFamily: FONT_SANS,
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
              fontFamily: FONT_SANS,
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
                <div className="flex justify-between items-baseline" style={{ fontFamily: FONT_SANS }}>
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
                  <p className="text-xs text-center" style={{ color: "var(--st-text-secondary)", fontFamily: FONT_SANS, opacity: 0.7 }}>
                    {formatEta(etaMs)}
                  </p>
                )}
                <button
                  onClick={resetPipeline}
                  className="text-xs underline opacity-60 hover:opacity-100 transition-opacity"
                  style={{ color: "var(--st-text-secondary)", fontFamily: FONT_SANS }}
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
                <p className="font-medium" style={{ color: "var(--st-success)", fontFamily: FONT_SANS }}>
                  Pipeline complete
                </p>
                <p className="text-sm" style={{ color: "var(--st-success)", opacity: 0.8, fontFamily: FONT_SANS }}>
                  Finished in {Math.round(status.duration / 1000)}s
                </p>
              </div>

              {/* Validation warning */}
              {status.validation && !status.validation.passed && (
                <p className="text-xs text-center font-medium" style={{ color: COLORS.warning, fontFamily: FONT_SANS }}>
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
                        fontFamily: FONT_SANS,
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
                <details className="text-xs" style={{ fontFamily: FONT_SANS, color: COLORS.warning }}>
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
                    fontFamily: FONT_SANS,
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
              <p className="font-medium" style={{ color: "var(--st-error)", fontFamily: FONT_SANS }}>Error</p>
              <p className="text-sm mt-1" style={{ color: "var(--st-error)", opacity: 0.85, fontFamily: FONT_SANS }}>
                {status.message}
              </p>
              <button
                onClick={() => setStatus({ state: "idle" })}
                className="mt-3 text-sm underline"
                style={{ color: "var(--st-error)", opacity: 0.7, fontFamily: FONT_SANS }}
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
          fontSize: DS.text.lg,
          letterSpacing: "-0.01em",
          textTransform: "uppercase" as const,
        }}
      >
        {children}
      </h2>
      {subtitle && (
        <p style={{ color: "var(--st-text-secondary)", fontFamily: FONT_SANS, fontSize: DS.text.sm, marginTop: "2px" }}>
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
        style={{ color, fontFamily: FONT_SANS, fontWeight: DS.weight.medium, fontSize: DS.text.xs, whiteSpace: "nowrap", backgroundColor: compactBg, gap: "2px" }}
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
      style={{ color, fontFamily: FONT_SANS, backgroundColor: bgColor, gap: "3px" }}
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
            fontFamily: FONT_SANS,
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
              fontFamily: FONT_SANS,
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
              fontFamily: FONT_SANS,
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
            fontFamily: FONT_SANS,
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
          <div className="flex justify-between items-baseline" style={{ fontFamily: FONT_SANS, fontSize: "0.72rem", marginBottom: "4px" }}>
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
            <p style={{ fontFamily: FONT_SANS, fontSize: "0.68rem", color: "var(--st-text-secondary)", opacity: 0.7, marginTop: "3px", textAlign: "center" }}>
              {formatEta(etaMs)}
            </p>
          )}
        </div>
      )}
    </div>
  );
}

// ─── SVG Donut Chart ─────────────────────────────────────────

interface DonutSegment {
  label: string;
  value: number;
  color: string;
}

function DonutChart({ segments, size = 160 }: { segments: DonutSegment[]; size?: number }) {
  const total = segments.reduce((s, seg) => s + seg.value, 0);
  if (total === 0) return null;

  const radius = 52;
  const strokeWidth = 20;
  const circumference = 2 * Math.PI * radius;
  let cumulativeOffset = 0;

  return (
    <div className="flex items-center gap-6">
      <svg width={size} height={size} viewBox="0 0 128 128">
        {segments.map((seg, i) => {
          const fraction = seg.value / total;
          const dashLength = fraction * circumference;
          const dashOffset = -cumulativeOffset;
          cumulativeOffset += dashLength;
          return (
            <circle
              key={i}
              cx="64"
              cy="64"
              r={radius}
              fill="none"
              stroke={seg.color}
              strokeWidth={strokeWidth}
              strokeDasharray={`${dashLength} ${circumference - dashLength}`}
              strokeDashoffset={dashOffset}
              transform="rotate(-90 64 64)"
              strokeLinecap="butt"
              style={{ opacity: 0.85 }}
            />
          );
        })}
        <text x="64" y="58" textAnchor="middle" style={{ fill: "var(--st-text-primary)", fontFamily: FONT_SANS, fontWeight: DS.weight.bold, fontSize: "26px" }}>
          {formatNumber(total)}
        </text>
        <text x="64" y="78" textAnchor="middle" style={{ fill: "var(--st-text-secondary)", fontFamily: FONT_SANS, fontWeight: DS.weight.normal, fontSize: "11px", letterSpacing: "0.05em" }}>
          TOTAL
        </text>
      </svg>
      <div className="flex flex-col gap-2">
        {segments.map((seg, i) => (
          <div key={i} className="flex items-center gap-2.5">
            <span className="rounded-full" style={{ width: "10px", height: "10px", backgroundColor: seg.color, flexShrink: 0, opacity: 0.85 }} />
            <div>
              <span style={{ fontFamily: FONT_SANS, fontWeight: DS.weight.bold, fontSize: DS.text.md, color: "var(--st-text-primary)" }}>
                {formatNumber(seg.value)}
              </span>
              <span style={{ fontFamily: FONT_SANS, fontWeight: DS.weight.normal, fontSize: DS.text.sm, color: "var(--st-text-secondary)", marginLeft: "6px" }}>
                {seg.label}
              </span>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

// ─── Mini Bar Chart (reusable) ───────────────────────────────

interface BarChartData {
  label: string;
  value: number;
  color?: string;
}

function MiniBarChart({ data, height = 80, showValues = true, formatValue }: {
  data: BarChartData[];
  height?: number;
  showValues?: boolean;
  formatValue?: (v: number) => string;
}) {
  if (data.length === 0) return null;
  const max = Math.max(...data.map((d) => Math.abs(d.value)), 1);
  const fmt = formatValue || ((v: number) => String(v));

  // For short charts (< 120px), use compact mode: no Y-axis, no gridlines
  const compact = height < 120;

  const marginLeft = compact ? 4 : 52;
  const marginRight = compact ? 4 : 12;
  const marginTop = compact ? (showValues ? 14 : 4) : (showValues ? 16 : 6);
  const marginBottom = compact ? 16 : 22;
  const chartHeight = height;
  const barAreaHeight = Math.max(chartHeight - marginTop - marginBottom, 20);
  const barGap = data.length > 8 ? 3 : 6;

  // Generate nice Y-axis gridlines (only for tall charts)
  const niceMax = (() => {
    const raw = max;
    if (raw === 0) return 1;
    const magnitude = Math.pow(10, Math.floor(Math.log10(raw)));
    const normalized = raw / magnitude;
    if (normalized <= 1.5) return 1.5 * magnitude;
    if (normalized <= 2) return 2 * magnitude;
    if (normalized <= 3) return 3 * magnitude;
    if (normalized <= 5) return 5 * magnitude;
    return 10 * magnitude;
  })();
  const gridCount = compact ? 0 : 3;
  const gridLines = compact ? [] : Array.from({ length: gridCount + 1 }, (_, i) => (niceMax / gridCount) * i);

  // For gridline labels, use the formatValue function if provided, otherwise format as number
  const fmtGrid = formatValue || ((v: number) => v >= 1000 ? `${(v / 1000).toFixed(v % 1000 === 0 ? 0 : 1)}k` : String(Math.round(v)));

  // Scale factor: use niceMax for tall charts with gridlines, raw max for compact
  const scaleMax = compact ? max : niceMax;

  return (
    <div style={{ width: "100%", position: "relative" }}>
      <svg
        viewBox={`0 0 500 ${chartHeight}`}
        preserveAspectRatio="xMidYMid meet"
        style={{ width: "100%", height: "auto", display: "block" }}
      >
        {/* Y-axis gridlines and labels (tall charts only) */}
        {gridLines.map((val, i) => {
          const y = marginTop + barAreaHeight - (val / scaleMax) * barAreaHeight;
          return (
            <g key={`grid-${i}`}>
              <line
                x1={marginLeft}
                x2={500 - marginRight}
                y1={y}
                y2={y}
                stroke="var(--st-border)"
                strokeWidth={i === 0 ? 1.2 : 0.8}
                strokeDasharray={i === 0 ? "none" : "3,3"}
              />
              <text
                x={marginLeft - 6}
                y={y + 1}
                textAnchor="end"
                dominantBaseline="middle"
                fill="var(--st-text-secondary)"
                fontFamily={FONT_SANS}
                fontSize="10"
                fontWeight="500"
              >
                {fmtGrid(val)}
              </text>
            </g>
          );
        })}

        {/* Baseline for compact charts */}
        {compact && (
          <line
            x1={marginLeft}
            x2={500 - marginRight}
            y1={marginTop + barAreaHeight}
            y2={marginTop + barAreaHeight}
            stroke="var(--st-border)"
            strokeWidth={1}
          />
        )}

        {/* Bars */}
        {data.map((d, i) => {
          const barWidth = (500 - marginLeft - marginRight - barGap * (data.length - 1)) / data.length;
          const x = marginLeft + i * (barWidth + barGap);
          const fraction = scaleMax > 0 ? Math.abs(d.value) / scaleMax : 0;
          const barH = Math.max(Math.round(fraction * barAreaHeight), 3);
          const y = marginTop + barAreaHeight - barH;
          return (
            <g key={i}>
              <rect
                x={x}
                y={y}
                width={barWidth}
                height={barH}
                rx={2}
                fill={d.color || "var(--st-accent)"}
                opacity={0.8}
              />
              {/* Value label above bar */}
              {showValues && (
                <text
                  x={x + barWidth / 2}
                  y={y - 3}
                  textAnchor="middle"
                  fill="var(--st-text-primary)"
                  fontFamily={FONT_SANS}
                  fontSize={compact ? "10" : data.length > 10 ? "7.5" : data.length > 6 ? "8" : "9.5"}
                  fontWeight="600"
                >
                  {fmt(d.value)}
                </text>
              )}
              {/* Label below */}
              <text
                x={x + barWidth / 2}
                y={chartHeight - marginBottom + 12}
                textAnchor="middle"
                fill="var(--st-text-secondary)"
                fontFamily={FONT_SANS}
                fontSize={compact ? "9.5" : data.length > 10 ? "7.5" : data.length > 6 ? "8" : "9.5"}
                fontWeight="500"
              >
                {d.label}
              </text>
            </g>
          );
        })}
      </svg>
    </div>
  );
}

// ─── Stacked Bar Chart ───────────────────────────────────────

interface StackedBarData {
  label: string;
  segments: { value: number; color: string; label: string }[];
}

function StackedBarChart({ data, height = 28 }: { data: StackedBarData[]; height?: number }) {
  if (data.length === 0) return null;

  return (
    <div className="flex flex-col gap-3">
      {data.map((bar, i) => {
        const total = bar.segments.reduce((s, seg) => s + seg.value, 0);
        if (total === 0) return null;
        return (
          <div key={i}>
            <div className="flex items-center justify-between mb-1">
              <span style={{ fontFamily: FONT_SANS, fontSize: DS.text.sm, fontWeight: DS.weight.medium, color: "var(--st-text-secondary)", letterSpacing: "0.05em", textTransform: "uppercase" as const }}>
                {bar.label}
              </span>
              <span style={{ fontFamily: FONT_SANS, fontSize: DS.text.md, fontWeight: DS.weight.bold, color: "var(--st-text-primary)" }}>
                {formatCurrency(total)}
              </span>
            </div>
            <div className="flex rounded-full overflow-hidden" style={{ height: `${height}px`, backgroundColor: "var(--st-border)" }}>
              {bar.segments.map((seg, j) => {
                const pct = (seg.value / total) * 100;
                if (pct < 0.5) return null;
                return (
                  <div
                    key={j}
                    title={`${seg.label}: ${formatCurrency(seg.value)} (${pct.toFixed(0)}%)`}
                    style={{
                      width: `${pct}%`,
                      backgroundColor: seg.color,
                      opacity: 0.8,
                      transition: "width 0.4s ease",
                    }}
                  />
                );
              })}
            </div>
          </div>
        );
      })}
    </div>
  );
}

// ─── KPI Row (open layout — no box) ─────────────────────────

function KPIMetric({ label, value, sublabel }: { label: string; value: string; sublabel?: string }) {
  return (
    <div style={{ padding: "0.5rem 0" }}>
      <p style={{ fontFamily: FONT_SANS, ...DS.label }}>
        {label}
      </p>
      <p style={{ fontFamily: FONT_SANS, fontWeight: DS.weight.bold, fontSize: DS.text.lg, color: "var(--st-text-primary)", letterSpacing: "-0.02em", lineHeight: 1.2, marginTop: "2px" }}>
        {value}
      </p>
      {sublabel && (
        <p style={{ fontFamily: FONT_SANS, fontWeight: DS.weight.normal, fontSize: DS.text.sm, color: "var(--st-text-secondary)", marginTop: "1px" }}>
          {sublabel}
        </p>
      )}
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
      }}
    >
      {children}
    </div>
  );
}

function NoData({ label }: { label: string }) {
  return (
    <Card>
      <p style={{ fontFamily: FONT_SANS, fontWeight: DS.weight.normal, fontSize: DS.text.sm, color: "var(--st-text-secondary)" }}>
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
    <div style={{ padding: "0.65rem 0", borderBottom: isLast ? "none" : "1px solid var(--st-border)" }}>
      <div className="flex items-center justify-between">
        <span style={{ fontFamily: FONT_SANS, fontWeight: DS.weight.medium, fontSize: DS.text.sm, color: "var(--st-text-secondary)" }}>
          {label}
        </span>
        <div className="flex items-center gap-3">
          <span style={{ fontFamily: FONT_SANS, fontWeight: DS.weight.bold, fontSize: DS.text.md, color: "var(--st-text-primary)" }}>
            {value}
          </span>
          <DeltaBadge delta={delta} deltaPercent={deltaPercent} isPositiveGood={isPositiveGood} isCurrency={isCurrency} compact />
        </div>
      </div>
      {sublabel && (
        <p style={{ fontFamily: FONT_SANS, fontWeight: DS.weight.normal, fontSize: DS.text.xs, color: "var(--st-text-secondary)", marginTop: "2px", fontStyle: "italic" }}>
          {sublabel}
        </p>
      )}
    </div>
  );
}

// ─── Forecast Metric ─────────────────────────────────────────

function ForecastMetric({ label, value, color }: { label: string; value: string; color?: string }) {
  return (
    <div className="flex items-center justify-between" style={{ padding: "0.7rem 0", borderBottom: "1px solid var(--st-border)" }}>
      <p style={{ fontFamily: FONT_SANS, ...DS.label }}>
        {label}
      </p>
      <p style={{ color: color || "var(--st-text-primary)", fontFamily: FONT_SANS, fontWeight: DS.weight.bold, fontSize: DS.text.lg }}>
        {value}
      </p>
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
    <Card>
      <div className="flex flex-wrap gap-x-6 gap-y-5">
        {tiles.map((tile, i) => (
          <div key={i} style={{ flex: "1 1 140px", minWidth: "140px", borderLeft: i > 0 ? "1px solid var(--st-border)" : "none", paddingLeft: i > 0 ? "0.75rem" : 0 }}>
            <p style={{ fontFamily: FONT_SANS, ...DS.label, marginBottom: "4px" }}>
              {tile.label}
            </p>
            <p style={{ fontFamily: FONT_SANS, fontWeight: DS.weight.bold, fontSize: DS.text.lg, color: "var(--st-text-primary)", letterSpacing: "-0.02em", lineHeight: 1.1, whiteSpace: "nowrap" }}>
              {tile.value}
            </p>
            {(tile.sublabel || tile.delta != null) && (
              <div className="flex items-center gap-1.5" style={{ marginTop: "3px" }}>
                {tile.delta != null && (
                  <DeltaBadge delta={tile.delta} deltaPercent={tile.deltaPercent ?? null} isPositiveGood={tile.isPositiveGood} isCurrency={tile.isCurrency} compact />
                )}
                {tile.sublabel && (
                  <span style={{ fontFamily: FONT_SANS, fontWeight: DS.weight.normal, fontSize: DS.text.sm, color: "var(--st-text-secondary)" }}>
                    {tile.sublabel}
                  </span>
                )}
              </div>
            )}
          </div>
        ))}
      </div>
    </Card>
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

  // Use actual monthly revenue data for the bar chart (last 12 months)
  const revenueMonthlyBars: BarChartData[] = monthlyRevenue.slice(-12).map((m) => ({
    label: formatShortMonth(m.month),
    value: m.gross,
    color: COLORS.member,
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
    <div className="space-y-5">
      <SectionHeader>Revenue</SectionHeader>

      {/* Summary cards — 2 column */}
      <div className="grid grid-cols-1 sm:grid-cols-2 gap-5">
        {/* Left: Latest completed month */}
        <Card>
          <p style={{ fontFamily: FONT_SANS, ...DS.label, marginBottom: "2px" }}>
            {currentMonth ? formatMonthLabel(currentMonth.month) : "Latest Month"} Gross Revenue
          </p>

          {currentMonth ? (
            <>
              <p style={{ fontFamily: FONT_SANS, fontWeight: DS.weight.bold, fontSize: DS.text.lg, color: "var(--st-text-primary)", letterSpacing: "-0.02em", lineHeight: 1.1, marginTop: "8px" }}>
                {formatCurrency(currentMonth.gross)}
              </p>
              <p style={{ fontFamily: FONT_SANS, fontSize: DS.text.sm, color: "var(--st-text-secondary)", marginTop: "4px" }}>
                Net: {formatCurrency(currentMonth.net)}
              </p>
              {momDelta != null && prevMonth && (
                <p style={{ marginTop: "8px" }}>
                  <span style={{ fontFamily: FONT_SANS, fontSize: DS.text.sm, color: "var(--st-text-secondary)" }}>vs {formatMonthLabel(prevMonth.month)}: </span>
                  <DeltaBadge delta={momDelta} deltaPercent={momDeltaPct} isCurrency compact />
                </p>
              )}
            </>
          ) : (
            <p style={{ color: "var(--st-text-secondary)", fontFamily: FONT_SANS, marginTop: "8px" }}>No monthly data yet</p>
          )}
        </Card>

        {/* Right: Gross vs Net for latest month */}
        {currentMonth && (
          <Card>
            <p style={{ fontFamily: FONT_SANS, ...DS.label, marginBottom: "2px" }}>
              {formatMonthLabel(currentMonth.month)} Breakdown
            </p>
            <div style={{ marginTop: "12px" }}>
              <div className="flex justify-between" style={{ fontFamily: FONT_SANS, fontSize: DS.text.sm, padding: "6px 0", borderBottom: "1px solid var(--st-border)" }}>
                <span style={{ color: "var(--st-text-secondary)", fontWeight: DS.weight.normal }}>Gross</span>
                <span style={{ color: "var(--st-text-primary)", fontWeight: DS.weight.bold }}>{formatCurrency(currentMonth.gross)}</span>
              </div>
              <div className="flex justify-between" style={{ fontFamily: FONT_SANS, fontSize: DS.text.sm, padding: "6px 0", borderBottom: "1px solid var(--st-border)" }}>
                <span style={{ color: "var(--st-text-secondary)", fontWeight: DS.weight.normal }}>Net</span>
                <span style={{ color: "var(--st-text-primary)", fontWeight: DS.weight.bold }}>{formatCurrency(currentMonth.net)}</span>
              </div>
              <div className="flex justify-between" style={{ fontFamily: FONT_SANS, fontSize: DS.text.sm, padding: "6px 0" }}>
                <span style={{ color: "var(--st-text-secondary)", fontWeight: DS.weight.normal }}>Fees + Refunds</span>
                <span style={{ color: "var(--st-error)", fontWeight: DS.weight.medium }}>-{formatCurrency(currentMonth.gross - currentMonth.net)}</span>
              </div>
            </div>
          </Card>
        )}
      </div>

      {/* Full-width monthly trend chart */}
      {revenueMonthlyBars.length > 0 && (
        <Card>
          <p className="mb-3" style={{ fontFamily: FONT_SANS, ...DS.label }}>
            Monthly Revenue Trend (Gross)
          </p>
          <MiniBarChart data={revenueMonthlyBars} height={200} formatValue={formatCompactCurrency} />
        </Card>
      )}
    </div>
  );
}

// ─── Month-over-Month YoY Comparison ─────────────────────────

function MonthOverMonthSection({ data }: { data: MonthOverMonthData }) {
  const hasCurrentData = data.current !== null;
  const hasPriorData = data.priorYear !== null;

  if (!hasCurrentData && !hasPriorData) return null;

  const priorGross = data.priorYear?.gross ?? 0;
  const currentGross = data.current?.gross ?? 0;
  const priorNet = data.priorYear?.net ?? 0;
  const currentNet = data.current?.net ?? 0;
  const maxVal = Math.max(priorGross, currentGross, 1);

  const chartHeight = 260;
  const marginTop = 40;
  const marginBottom = 50;
  const barAreaHeight = chartHeight - marginTop - marginBottom;
  const barWidth = 120;
  const gap = 60;
  const totalWidth = barWidth * 2 + gap;
  const startX = (500 - totalWidth) / 2;

  const priorH = maxVal > 0 ? Math.max((priorGross / maxVal) * barAreaHeight, 4) : 0;
  const currentH = maxVal > 0 ? Math.max((currentGross / maxVal) * barAreaHeight, 4) : 0;

  return (
    <div className="space-y-5">
      <SectionHeader subtitle={`${data.monthName} ${data.priorYear?.year ?? (data.current ? data.current.year - 1 : "")} → ${data.monthName} ${data.current?.year ?? ""}`}>
        Year-over-Year
      </SectionHeader>

      <Card>
        <div style={{ width: "100%", position: "relative" }}>
          <svg viewBox={`0 0 500 ${chartHeight}`} preserveAspectRatio="xMidYMid meet" style={{ width: "100%", height: "auto", display: "block" }}>
            {/* Baseline */}
            <line x1={startX - 20} x2={startX + totalWidth + 20} y1={marginTop + barAreaHeight} y2={marginTop + barAreaHeight}
              stroke="var(--st-border)" strokeWidth={1.2} />

            {/* Prior year bar */}
            <rect x={startX} y={marginTop + barAreaHeight - priorH} width={barWidth} height={priorH}
              rx={4} fill="var(--st-text-secondary)" opacity={0.3} />
            {/* Prior year gross label */}
            <text x={startX + barWidth / 2} y={marginTop + barAreaHeight - priorH - 14}
              textAnchor="middle" fill="var(--st-text-primary)" fontFamily={FONT_SANS} fontSize="16" fontWeight="700">
              {formatCurrency(priorGross)}
            </text>
            {/* Prior year net label */}
            <text x={startX + barWidth / 2} y={marginTop + barAreaHeight - priorH - 1}
              textAnchor="middle" fill="var(--st-text-secondary)" fontFamily={FONT_SANS} fontSize="10" fontWeight="500">
              Net {formatCurrency(priorNet)}
            </text>
            {/* Prior year month label */}
            <text x={startX + barWidth / 2} y={marginTop + barAreaHeight + 20}
              textAnchor="middle" fill="var(--st-text-secondary)" fontFamily={FONT_SANS} fontSize="13" fontWeight="600">
              {data.monthName} {data.priorYear?.year ?? ""}
            </text>

            {/* Current year bar */}
            <rect x={startX + barWidth + gap} y={marginTop + barAreaHeight - currentH} width={barWidth} height={currentH}
              rx={4} fill="var(--st-accent)" opacity={0.8} />
            {/* Current year gross label */}
            <text x={startX + barWidth + gap + barWidth / 2} y={marginTop + barAreaHeight - currentH - 14}
              textAnchor="middle" fill="var(--st-text-primary)" fontFamily={FONT_SANS} fontSize="16" fontWeight="700">
              {formatCurrency(currentGross)}
            </text>
            {/* Current year net label */}
            <text x={startX + barWidth + gap + barWidth / 2} y={marginTop + barAreaHeight - currentH - 1}
              textAnchor="middle" fill="var(--st-text-secondary)" fontFamily={FONT_SANS} fontSize="10" fontWeight="500">
              Net {formatCurrency(currentNet)}
            </text>
            {/* Current year month label */}
            <text x={startX + barWidth + gap + barWidth / 2} y={marginTop + barAreaHeight + 20}
              textAnchor="middle" fill="var(--st-text-secondary)" fontFamily={FONT_SANS} fontSize="13" fontWeight="600">
              {data.monthName} {data.current?.year ?? ""}
            </text>

            {/* YoY delta badge */}
            {data.yoyGrossPct !== null && (
              <text x={250} y={16}
                textAnchor="middle" fill={data.yoyGrossChange !== null && data.yoyGrossChange >= 0 ? "var(--st-success)" : "var(--st-error)"} fontFamily={FONT_SANS} fontSize="14" fontWeight="700">
                {data.yoyGrossPct >= 0 ? "+" : ""}{data.yoyGrossPct}% YoY
              </text>
            )}
          </svg>
        </div>
      </Card>
    </div>
  );
}

// ─── MRR Breakdown (standalone) ──────────────────────────────

function MRRBreakdown({ data }: { data: DashboardStats }) {
  const mrrData: StackedBarData[] = [{
    label: "MRR",
    segments: [
      { value: data.mrr.member, color: COLORS.member, label: "Member" },
      { value: data.mrr.sky3, color: COLORS.sky3, label: "SKY3" },
      { value: data.mrr.skyTingTv, color: COLORS.tv, label: "TV" },
      ...(data.mrr.unknown > 0 ? [{ value: data.mrr.unknown, color: "#999", label: "Other" }] : []),
    ],
  }];

  return (
    <Card>
      <div className="flex items-baseline justify-between mb-3">
        <p style={{ fontFamily: FONT_SANS, ...DS.label }}>
          Recurring Revenue (MRR)
        </p>
        <p style={{ fontFamily: FONT_SANS, fontWeight: DS.weight.bold, fontSize: DS.text.lg, color: "var(--st-text-primary)", letterSpacing: "-0.02em" }}>
          {formatCurrency(data.mrr.total)}
        </p>
      </div>
      <StackedBarChart data={mrrData} />
      <div className="flex gap-4 mt-3 flex-wrap">
        {mrrData[0].segments.filter(s => s.value > 0).map((seg, i) => (
          <div key={i} className="flex items-center gap-1.5">
            <span className="rounded-full" style={{ width: "8px", height: "8px", backgroundColor: seg.color, opacity: 0.8 }} />
            <span style={{ fontFamily: FONT_SANS, fontSize: DS.text.sm, color: "var(--st-text-secondary)" }}>
              {seg.label}: {formatCurrency(seg.value)}
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
  const weeklyBars: BarChartData[] = firstVisits.completedWeeks.map((w) => {
    return { label: formatWeekRange(w.week), value: w.uniqueVisitors, color: COLORS.teal };
  });

  const segments: FirstVisitSegment[] = ["introWeek", "dropIn", "guest", "other"];
  const agg = firstVisits.aggregateSegments;
  const aggTotal = agg.introWeek + agg.dropIn + agg.guest + agg.other;
  const otherTop5 = firstVisits.otherBreakdownTop5 || [];

  return (
    <Card>
      {/* Header */}
      <div className="flex items-start justify-between mb-4">
        <div className="flex items-center gap-2.5">
          <span className="rounded-full" style={{ width: "10px", height: "10px", backgroundColor: COLORS.teal, opacity: 0.85 }} />
          <span style={{ fontFamily: FONT_SANS, fontWeight: DS.weight.bold, fontSize: DS.text.md, color: "var(--st-text-primary)", textTransform: "uppercase", letterSpacing: "0.05em" }}>
            First Visits
          </span>
        </div>
        <div style={{ textAlign: "right" }}>
          <span style={{ fontFamily: FONT_SANS, fontWeight: DS.weight.bold, fontSize: DS.text.xl, color: "var(--st-text-primary)", letterSpacing: "-0.02em", lineHeight: 1 }}>
            {formatNumber(firstVisits.currentWeekTotal)}
          </span>
          <p style={{ fontFamily: FONT_SANS, ...DS.label, marginTop: "2px" }}>
            Unique This Week
          </p>
        </div>
      </div>

      <div className="grid grid-cols-1 sm:grid-cols-2 gap-5">
        {/* Left: Weekly bar chart — last 4 completed weeks */}
        <div>
          <p className="mb-2" style={{ fontFamily: FONT_SANS, ...DS.label }}>
            Unique Visitors — Last 4 Weeks
          </p>
          {weeklyBars.length > 0 ? (
            <MiniBarChart data={weeklyBars} height={100} />
          ) : (
            <p style={{ color: "var(--st-text-secondary)", fontFamily: FONT_SANS, fontSize: DS.text.sm }}>--</p>
          )}
        </div>

        {/* Right: Source Mix (aggregate across full window) */}
        <div style={{ borderLeft: "1px solid var(--st-border)", paddingLeft: "1rem" }}>
          <p className="mb-2" style={{ fontFamily: FONT_SANS, ...DS.label }}>
            Source Mix
          </p>
          <div className="flex flex-col gap-1.5">
            {segments.map((seg) => {
              const count = agg[seg] || 0;
              if (count === 0 && aggTotal === 0) return null;
              return (
                <div key={seg} className="flex items-center justify-between" style={{ padding: "0.3rem 0" }}>
                  <div className="flex items-center gap-2">
                    <span className="rounded-full" style={{ width: "7px", height: "7px", backgroundColor: SEGMENT_COLORS[seg] }} />
                    <span style={{ fontFamily: FONT_SANS, fontWeight: DS.weight.normal, fontSize: DS.text.sm, color: "var(--st-text-secondary)" }}>
                      {SEGMENT_LABELS[seg]}
                    </span>
                  </div>
                  <span style={{ fontFamily: FONT_SANS, fontWeight: DS.weight.bold, fontSize: DS.text.md, color: "var(--st-text-primary)" }}>
                    {count}
                    {aggTotal > 0 && (
                      <span style={{ fontWeight: DS.weight.normal, fontSize: DS.text.xs, color: "var(--st-text-secondary)", marginLeft: "0.35rem" }}>
                        {Math.round((count / aggTotal) * 100)}%
                      </span>
                    )}
                  </span>
                </div>
              );
            })}
          </div>
          {otherTop5.length > 0 && (
            <div style={{ marginTop: "0.5rem", paddingLeft: "1rem" }}>
              <p style={{ fontFamily: FONT_SANS, fontWeight: DS.weight.normal, fontSize: DS.text.xs, color: "var(--st-text-secondary)", marginBottom: "0.3rem" }}>
                Other includes:
              </p>
              {otherTop5.map((item, i) => (
                <div key={i} className="flex items-center justify-between" style={{ padding: "0.15rem 0" }}>
                  <span style={{ fontFamily: FONT_SANS, fontSize: DS.text.xs, color: "var(--st-text-secondary)" }}>
                    {item.passName}
                  </span>
                  <span style={{ fontFamily: FONT_SANS, fontWeight: DS.weight.medium, fontSize: DS.text.xs, color: "var(--st-text-secondary)" }}>
                    {item.count}
                  </span>
                </div>
              ))}
            </div>
          )}
        </div>
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
  const weeklyBars: BarChartData[] = returningNonMembers.completedWeeks.map((w) => {
    return { label: formatWeekRange(w.week), value: w.uniqueVisitors, color: COLORS.copper };
  });

  // 3 buckets only (no Intro Week — it's merged into Other in the analytics)
  const rnmSegments = ["dropIn", "guest", "other"] as const;
  const agg = returningNonMembers.aggregateSegments;
  // Merge any introWeek into other for display (defensive)
  const aggDisplay = {
    dropIn: agg.dropIn,
    guest: agg.guest,
    other: agg.other + agg.introWeek,
  };
  const rnmAggTotal = aggDisplay.dropIn + aggDisplay.guest + aggDisplay.other;
  const otherTop5 = returningNonMembers.otherBreakdownTop5 || [];

  return (
    <Card>
      {/* Header */}
      <div className="flex items-start justify-between mb-4">
        <div className="flex items-center gap-2.5">
          <span className="rounded-full" style={{ width: "10px", height: "10px", backgroundColor: COLORS.copper, opacity: 0.85 }} />
          <span style={{ fontFamily: FONT_SANS, fontWeight: DS.weight.bold, fontSize: DS.text.md, color: "var(--st-text-primary)", textTransform: "uppercase", letterSpacing: "0.05em" }}>
            Returning Non-Members
          </span>
        </div>
        <div style={{ textAlign: "right" }}>
          <span style={{ fontFamily: FONT_SANS, fontWeight: DS.weight.bold, fontSize: DS.text.xl, color: "var(--st-text-primary)", letterSpacing: "-0.02em", lineHeight: 1 }}>
            {formatNumber(returningNonMembers.currentWeekTotal)}
          </span>
          <p style={{ fontFamily: FONT_SANS, ...DS.label, marginTop: "2px" }}>
            Unique This Week
          </p>
        </div>
      </div>

      <div className="grid grid-cols-1 sm:grid-cols-2 gap-5">
        {/* Left: Weekly bar chart */}
        <div>
          <p className="mb-2" style={{ fontFamily: FONT_SANS, ...DS.label }}>
            Unique Visitors — Last 4 Weeks
          </p>
          {weeklyBars.length > 0 ? (
            <MiniBarChart data={weeklyBars} height={100} />
          ) : (
            <p style={{ color: "var(--st-text-secondary)", fontFamily: FONT_SANS, fontSize: DS.text.sm }}>--</p>
          )}
        </div>

        {/* Right: Source Mix (aggregate, 3 buckets) */}
        <div style={{ borderLeft: "1px solid var(--st-border)", paddingLeft: "1rem" }}>
          <p className="mb-2" style={{ fontFamily: FONT_SANS, ...DS.label }}>
            Source Mix
          </p>
          <div className="flex flex-col gap-1.5">
            {rnmSegments.map((seg) => (
              <div key={seg} className="flex items-center justify-between" style={{ padding: "0.3rem 0" }}>
                <div className="flex items-center gap-2">
                  <span className="rounded-full" style={{ width: "7px", height: "7px", backgroundColor: RNM_SEGMENT_COLORS[seg] }} />
                  <span style={{ fontFamily: FONT_SANS, fontWeight: DS.weight.normal, fontSize: DS.text.sm, color: "var(--st-text-secondary)" }}>
                    {RNM_SEGMENT_LABELS[seg]}
                  </span>
                </div>
                <span style={{ fontFamily: FONT_SANS, fontWeight: DS.weight.bold, fontSize: DS.text.md, color: "var(--st-text-primary)" }}>
                  {aggDisplay[seg] || 0}
                  {rnmAggTotal > 0 && (
                    <span style={{ fontWeight: DS.weight.normal, fontSize: DS.text.xs, color: "var(--st-text-secondary)", marginLeft: "0.35rem" }}>
                      {Math.round(((aggDisplay[seg] || 0) / rnmAggTotal) * 100)}%
                    </span>
                  )}
                </span>
              </div>
            ))}
          </div>
          {otherTop5.length > 0 && (
            <div style={{ marginTop: "0.5rem", paddingLeft: "1rem" }}>
              <p style={{ fontFamily: FONT_SANS, fontWeight: DS.weight.normal, fontSize: DS.text.xs, color: "var(--st-text-secondary)", marginBottom: "0.3rem" }}>
                Other includes:
              </p>
              {otherTop5.map((item, i) => (
                <div key={i} className="flex items-center justify-between" style={{ padding: "0.15rem 0" }}>
                  <span style={{ fontFamily: FONT_SANS, fontSize: DS.text.xs, color: "var(--st-text-secondary)" }}>
                    {item.passName}
                  </span>
                  <span style={{ fontFamily: FONT_SANS, fontWeight: DS.weight.medium, fontSize: DS.text.xs, color: "var(--st-text-secondary)" }}>
                    {item.count}
                  </span>
                </div>
              ))}
            </div>
          )}
        </div>
      </div>
    </Card>
  );
}

// ─── Churn Section ──────────────────────────────────────────

function ChurnSection({ churnRates }: { churnRates: ChurnRateData }) {
  const categories = [
    { key: "member" as const, label: "Members", color: COLORS.member, data: churnRates.byCategory.member },
    { key: "sky3" as const, label: "SKY3", color: COLORS.sky3, data: churnRates.byCategory.sky3 },
    { key: "tv" as const, label: "SKY TING TV", color: COLORS.tv, data: churnRates.byCategory.skyTingTv },
  ];

  // Get completed months from the first category (they all share the same months)
  const firstCat = churnRates.byCategory.member;
  const completedMonths = firstCat?.monthly?.slice(0, -1).slice(-6) ?? [];

  // Max rate across ALL categories for bar scaling
  const maxRate = Math.max(
    ...categories.flatMap((c) =>
      (c.data?.monthly?.slice(0, -1).slice(-6) ?? []).map((m) => m.userChurnRate)
    ),
    1
  );

  // Total at-risk across all categories
  const totalAtRisk = categories.reduce((sum, c) => sum + (c.data?.atRiskCount ?? 0), 0);

  return (
    <div className="space-y-5">
      <SectionHeader subtitle={totalAtRisk > 0 ? `${totalAtRisk} at-risk subscribers` : undefined}>
        Churn
      </SectionHeader>

      {/* ── Overview: 3 tiles ── */}
      <div className="grid grid-cols-3 gap-3">
        {categories.map(({ label, color, data }) => {
          if (!data) return <div key={label} />;
          return (
            <div
              key={label}
              className="rounded-xl p-4"
              style={{
                backgroundColor: "var(--st-surface)",
                border: "1px solid var(--st-border)",
                fontFamily: FONT_SANS,
              }}
            >
              {/* Category label */}
              <div className="flex items-center gap-2 mb-3">
                <div style={{ width: 10, height: 10, borderRadius: 3, backgroundColor: color }} />
                <span style={{ fontSize: DS.text.sm, fontWeight: DS.weight.medium, color: "var(--st-text)" }}>
                  {label}
                </span>
              </div>

              {/* User churn rate (primary metric) */}
              <div style={{ marginBottom: "0.5rem" }}>
                <div style={{ fontSize: DS.text.xs, color: "var(--st-text-secondary)", fontWeight: DS.weight.normal, letterSpacing: "0.05em", textTransform: "uppercase" as const, marginBottom: 2 }}>
                  User Churn (avg/mo)
                </div>
                <span style={{ fontSize: DS.text.lg, fontWeight: DS.weight.bold, color: churnBenchmarkColor(data.avgUserChurnRate) }}>
                  {data.avgUserChurnRate.toFixed(1)}%
                </span>
              </div>

              {/* MRR churn + at-risk row */}
              <div className="flex items-center gap-3">
                <div>
                  <div style={{ fontSize: DS.text.xs, color: "var(--st-text-secondary)", fontWeight: DS.weight.normal, letterSpacing: "0.05em", textTransform: "uppercase" as const, marginBottom: 1 }}>
                    MRR Churn
                  </div>
                  <span style={{ fontSize: DS.text.md, fontWeight: DS.weight.medium, color: churnBenchmarkColor(data.avgMrrChurnRate) }}>
                    {data.avgMrrChurnRate.toFixed(1)}%
                  </span>
                </div>
                {data.atRiskCount > 0 && (
                  <div style={{ marginLeft: "auto" }}>
                    <span
                      className="rounded-full px-2 py-0.5"
                      style={{
                        fontSize: DS.text.xs,
                        fontWeight: DS.weight.medium,
                        color: COLORS.warning,
                        backgroundColor: COLORS.warning + "15",
                      }}
                    >
                      {data.atRiskCount} at-risk
                    </span>
                  </div>
                )}
              </div>
            </div>
          );
        })}
      </div>

      {/* ── Monthly Trend Chart ── */}
      <div
        className="rounded-xl p-5"
        style={{
          backgroundColor: "var(--st-surface)",
          border: "1px solid var(--st-border)",
          fontFamily: FONT_SANS,
        }}
      >
        <div className="flex items-center justify-between mb-4">
          <div style={{ fontFamily: FONT_SANS, ...DS.label }}>
            Monthly User Churn Rate
          </div>
          <span style={{ fontSize: DS.text.xs, color: "var(--st-text-secondary)", fontStyle: "italic" }}>
            Healthy: 5-7%
          </span>
        </div>

        {/* Grouped bar chart — one row per month, bars for each category */}
        <div className="space-y-3">
          {completedMonths.map((m) => {
            const memberM = churnRates.byCategory.member?.monthly?.find((x) => x.month === m.month);
            const sky3M = churnRates.byCategory.sky3?.monthly?.find((x) => x.month === m.month);
            const tvM = churnRates.byCategory.skyTingTv?.monthly?.find((x) => x.month === m.month);

            return (
              <div key={m.month}>
                <div style={{ fontSize: DS.text.xs, color: "var(--st-text-secondary)", fontWeight: DS.weight.normal, marginBottom: 4 }}>
                  {formatMonthLabel(m.month)}
                </div>
                <div className="flex flex-col gap-1">
                  {/* Members bar */}
                  {memberM && (
                    <div className="flex items-center gap-2">
                      <div style={{ width: 48, fontSize: DS.text.xs, color: COLORS.member, fontWeight: DS.weight.normal, flexShrink: 0 }}>Members</div>
                      <div className="flex-1 flex items-center gap-1.5">
                        <div style={{
                          height: 8,
                          width: `${Math.max((memberM.userChurnRate / maxRate) * 100, memberM.userChurnRate > 0 ? 2 : 0)}%`,
                          backgroundColor: COLORS.member,
                          borderRadius: 4,
                          opacity: 0.75,
                        }} />
                        <span style={{ fontSize: DS.text.xs, color: COLORS.member, fontWeight: DS.weight.medium, flexShrink: 0 }}>
                          {memberM.userChurnRate.toFixed(1)}%
                        </span>
                      </div>
                      <span style={{ fontSize: DS.text.xs, color: "var(--st-text-secondary)", flexShrink: 0 }}>
                        {memberM.canceledCount}/{memberM.activeAtStart}
                      </span>
                    </div>
                  )}
                  {/* SKY3 bar */}
                  {sky3M && (
                    <div className="flex items-center gap-2">
                      <div style={{ width: 48, fontSize: DS.text.xs, color: COLORS.sky3, fontWeight: DS.weight.normal, flexShrink: 0 }}>SKY3</div>
                      <div className="flex-1 flex items-center gap-1.5">
                        <div style={{
                          height: 8,
                          width: `${Math.max((sky3M.userChurnRate / maxRate) * 100, sky3M.userChurnRate > 0 ? 2 : 0)}%`,
                          backgroundColor: COLORS.sky3,
                          borderRadius: 4,
                          opacity: 0.75,
                        }} />
                        <span style={{ fontSize: DS.text.xs, color: COLORS.sky3, fontWeight: DS.weight.medium, flexShrink: 0 }}>
                          {sky3M.userChurnRate.toFixed(1)}%
                        </span>
                      </div>
                      <span style={{ fontSize: DS.text.xs, color: "var(--st-text-secondary)", flexShrink: 0 }}>
                        {sky3M.canceledCount}/{sky3M.activeAtStart}
                      </span>
                    </div>
                  )}
                  {/* TV bar */}
                  {tvM && (
                    <div className="flex items-center gap-2">
                      <div style={{ width: 48, fontSize: DS.text.xs, color: COLORS.tv, fontWeight: DS.weight.normal, flexShrink: 0 }}>TV</div>
                      <div className="flex-1 flex items-center gap-1.5">
                        <div style={{
                          height: 8,
                          width: `${Math.max((tvM.userChurnRate / maxRate) * 100, tvM.userChurnRate > 0 ? 2 : 0)}%`,
                          backgroundColor: COLORS.tv,
                          borderRadius: 4,
                          opacity: 0.75,
                        }} />
                        <span style={{ fontSize: DS.text.xs, color: COLORS.tv, fontWeight: DS.weight.medium, flexShrink: 0 }}>
                          {tvM.userChurnRate.toFixed(1)}%
                        </span>
                      </div>
                      <span style={{ fontSize: DS.text.xs, color: "var(--st-text-secondary)", flexShrink: 0 }}>
                        {tvM.canceledCount}/{tvM.activeAtStart}
                      </span>
                    </div>
                  )}
                </div>
              </div>
            );
          })}
        </div>

        {/* MEMBER annual vs monthly breakdown for last completed month */}
        {(() => {
          const memberMonthly = churnRates.byCategory.member?.monthly ?? [];
          const lastCompleted = memberMonthly.length >= 2 ? memberMonthly[memberMonthly.length - 2] : null;
          if (!lastCompleted?.annualActiveAtStart) return null;
          return (
            <div style={{ marginTop: 12, paddingTop: 10, borderTop: "1px solid var(--st-border)" }}>
              <div style={{ fontSize: DS.text.xs, color: "var(--st-text-secondary)", fontStyle: "italic" }}>
                Members last month: Annual {lastCompleted.annualCanceledCount}/{lastCompleted.annualActiveAtStart} churned, Monthly {lastCompleted.monthlyCanceledCount}/{lastCompleted.monthlyActiveAtStart} churned
              </div>
            </div>
          );
        })()}
      </div>
    </div>
  );
}

// ─── Non Members Section (First Visits + Returning Non-Members + Drop-Ins) ──

function NonMembersSection({ firstVisits, returningNonMembers, dropIns }: { firstVisits: FirstVisitData | null; returningNonMembers: ReturningNonMemberData | null; dropIns: DropInData | null }) {
  if (!firstVisits && !returningNonMembers && !dropIns) return null;

  return (
    <div className="space-y-5">
      <SectionHeader subtitle="Drop-ins, guests, and first-time visitors">Non Members</SectionHeader>

      {firstVisits && <FirstVisitsCard firstVisits={firstVisits} />}
      {returningNonMembers && <ReturningNonMembersCard returningNonMembers={returningNonMembers} />}
      {dropIns && <DropInCardNew dropIns={dropIns} />}
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
    const d = new Date(w.week + "T00:00:00");
    const label = isNaN(d.getTime()) ? w.week : d.toLocaleDateString("en-US", { month: "short", day: "numeric" });
    return { label, value: w.count, color: COLORS.warning };
  });

  return (
    <Card>
      {/* Header */}
      <div className="flex items-start justify-between mb-4">
        <div className="flex items-center gap-2.5">
          <span className="rounded-full" style={{ width: "10px", height: "10px", backgroundColor: COLORS.warning, opacity: 0.85 }} />
          <span style={{ fontFamily: FONT_SANS, fontWeight: DS.weight.bold, fontSize: DS.text.md, color: "var(--st-text-primary)", textTransform: "uppercase", letterSpacing: "0.05em" }}>
            Drop-Ins
          </span>
          <span style={{ fontFamily: FONT_SANS, fontWeight: DS.weight.normal, fontSize: DS.text.sm, color: "var(--st-text-secondary)", fontStyle: "italic" }}>
            Visits
          </span>
        </div>
        <div className="flex flex-col items-end">
          <span style={{ fontFamily: FONT_SANS, fontWeight: DS.weight.bold, fontSize: DS.text.xl, color: "var(--st-text-primary)", letterSpacing: "-0.02em", lineHeight: 1 }}>
            {formatNumber(dropIns.currentMonthTotal)}
          </span>
          <span style={{ fontFamily: FONT_SANS, ...DS.label, marginTop: "0.25rem" }}>
            Month to Date
          </span>
        </div>
      </div>

      <div className="grid grid-cols-1 sm:grid-cols-2 gap-5">
        {/* Left: Weekly visits chart */}
        <div>
          <p className="mb-2" style={{ fontFamily: FONT_SANS, ...DS.label }}>
            Weekly Visits
          </p>
          {weeklyBars.length > 0 ? (
            <MiniBarChart data={weeklyBars} height={100} />
          ) : (
            <p style={{ color: "var(--st-text-secondary)", fontFamily: FONT_SANS, fontSize: DS.text.sm }}>—</p>
          )}
        </div>

        {/* Right: Key metrics */}
        <div style={{ borderLeft: "1px solid var(--st-border)", paddingLeft: "1rem" }}>
          <TrendRow
            label="Month to Date"
            value={String(dropIns.currentMonthTotal)}
            delta={prevDelta}
            deltaPercent={null}
            sublabel={isPacing ? `Projected month-end: ${dropIns.currentMonthPaced}` : undefined}
          />
          <TrendRow
            label="Last Month"
            value={String(dropIns.previousMonthTotal)}
            delta={null}
            deltaPercent={null}
          />
          <TrendRow
            label="Weekly Avg"
            value={String(dropIns.weeklyAvg6w)}
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

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
//  LEGACY SECTIONS (kept for revert — not rendered in new layout)
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

// ─── Category Detail Section (Members / SKY3 / TV) ──────────

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
  // Drop current partial week — use only completed weeks for all weekly metrics
  const completedWeekly = weekly.length > 1 ? weekly.slice(0, -1) : weekly;
  const latestW = completedWeekly.length >= 1 ? completedWeekly[completedWeekly.length - 1] : null;
  const prevW = completedWeekly.length >= 2 ? completedWeekly[completedWeekly.length - 2] : null;
  const latestM = monthly.length >= 1 ? monthly[monthly.length - 1] : null;
  const isPacing = pacing && pacing.daysElapsed < pacing.daysInMonth;
  const weeklyNewBars: BarChartData[] = completedWeekly.slice(-6).map((w) => ({
    label: formatWeekLabel(w.period),
    value: weeklyKeyNew(w),
    color,
  }));

  return (
    <Card>
      {/* Header row */}
      <div className="flex items-start justify-between mb-4">
        <div className="flex items-center gap-2.5">
          <span className="rounded-full" style={{ width: "10px", height: "10px", backgroundColor: color, opacity: 0.85 }} />
          <span style={{ fontFamily: FONT_SANS, fontWeight: DS.weight.bold, fontSize: DS.text.md, color: "var(--st-text-primary)", textTransform: "uppercase", letterSpacing: "0.05em" }}>
            {title}
          </span>
        </div>
        <span style={{ fontFamily: FONT_SANS, fontWeight: DS.weight.bold, fontSize: DS.text.xl, color: "var(--st-text-primary)", letterSpacing: "-0.02em", lineHeight: 1 }}>
          {formatNumber(count)}
        </span>
      </div>

      <div className="grid grid-cols-1 sm:grid-cols-2 gap-5">
        {/* Left: Weekly new sign-ups chart */}
        <div>
          <p className="mb-2" style={{ fontFamily: FONT_SANS, ...DS.label }}>
            New Sign-ups — Weekly
          </p>
          {weeklyNewBars.length > 0 ? (
            <MiniBarChart data={weeklyNewBars} height={100} />
          ) : (
            <p style={{ color: "var(--st-text-secondary)", fontFamily: FONT_SANS, fontSize: DS.text.sm }}>—</p>
          )}
        </div>

        {/* Right: Key metrics */}
        <div>
          {latestW && (
            <>
              <TrendRow
                label="New (WoW)"
                value={String(weeklyKeyNew(latestW))}
                delta={prevW ? weeklyKeyNew(latestW) - weeklyKeyNew(prevW) : null}
                deltaPercent={null}
              />
              <TrendRow
                label="Churn (WoW)"
                value={String(weeklyKeyChurn(latestW))}
                delta={prevW ? -(weeklyKeyChurn(latestW) - weeklyKeyChurn(prevW)) : null}
                deltaPercent={null}
                isPositiveGood={false}
              />
              <TrendRow
                label="Net Growth"
                value={formatDelta(weeklyKeyNet(latestW)) || "0"}
                delta={null}
                deltaPercent={null}
                isLast
              />
            </>
          )}
          {latestM && isPacing && pacingNew && (
            <div style={{ marginTop: "0.5rem", padding: "0.5rem 0", borderTop: "1px solid var(--st-border)" }}>
              <p style={{ fontFamily: FONT_SANS, ...DS.label, color: "var(--st-accent)", marginBottom: "4px" }}>
                Month Pacing ({pacing!.daysElapsed}/{pacing!.daysInMonth}d)
              </p>
              <div className="flex gap-4">
                <span style={{ fontFamily: FONT_SANS, fontSize: DS.text.sm, color: "var(--st-text-primary)" }}>
                  <b>{pacingNew(pacing!).actual}</b> new <span style={{ color: "var(--st-text-secondary)", fontSize: DS.text.xs, fontStyle: "italic" }}>/ {pacingNew(pacing!).paced} projected</span>
                </span>
                {pacingChurn && (
                  <span style={{ fontFamily: FONT_SANS, fontSize: DS.text.sm, color: "var(--st-text-primary)" }}>
                    <b>{pacingChurn(pacing!).actual}</b> churn <span style={{ color: "var(--st-text-secondary)", fontSize: DS.text.xs, fontStyle: "italic" }}>/ {pacingChurn(pacing!).paced} projected</span>
                  </span>
                )}
              </div>
            </div>
          )}
        </div>
      </div>

      {/* ─── Churn Section ─── */}
      {churnData && (
        <div style={{ marginTop: "1rem", paddingTop: "1rem", borderTop: "1px solid var(--st-border)" }}>
          <p className="mb-3" style={{ fontFamily: FONT_SANS, ...DS.label }}>
            Churn
          </p>

          {/* Metric pills */}
          <div className="flex gap-3 mb-3">
            <div className="flex-1 rounded-lg" style={{ padding: "0.6rem 0.75rem", backgroundColor: "var(--st-bg-elevated, rgba(0,0,0,0.02))", border: "1px solid var(--st-border)" }}>
              <div style={{ fontSize: DS.text.xs, color: "var(--st-text-secondary)", fontWeight: DS.weight.normal, letterSpacing: "0.05em", textTransform: "uppercase" as const, marginBottom: "2px" }}>
                User Churn (avg/mo)
              </div>
              <span style={{ fontFamily: FONT_SANS, fontSize: DS.text.lg, fontWeight: DS.weight.bold, color: churnBenchmarkColor(churnData.avgUserChurnRate) }}>
                {churnData.avgUserChurnRate.toFixed(1)}%
              </span>
            </div>

            <div className="flex-1 rounded-lg" style={{ padding: "0.6rem 0.75rem", backgroundColor: "var(--st-bg-elevated, rgba(0,0,0,0.02))", border: "1px solid var(--st-border)" }}>
              <div style={{ fontSize: DS.text.xs, color: "var(--st-text-secondary)", fontWeight: DS.weight.normal, letterSpacing: "0.05em", textTransform: "uppercase" as const, marginBottom: "2px" }}>
                MRR Churn (avg/mo)
              </div>
              <span style={{ fontFamily: FONT_SANS, fontSize: DS.text.lg, fontWeight: DS.weight.bold, color: churnBenchmarkColor(churnData.avgMrrChurnRate) }}>
                {churnData.avgMrrChurnRate.toFixed(1)}%
              </span>
            </div>

            {churnData.atRiskCount > 0 && (
              <div className="rounded-lg" style={{ padding: "0.6rem 0.75rem", backgroundColor: "var(--st-bg-elevated, rgba(0,0,0,0.02))", border: "1px solid var(--st-border)", minWidth: "60px", textAlign: "center" }}>
                <div style={{ fontSize: DS.text.xs, color: "var(--st-text-secondary)", fontWeight: DS.weight.normal, letterSpacing: "0.05em", textTransform: "uppercase" as const, marginBottom: "2px" }}>
                  At Risk
                </div>
                <span style={{ fontFamily: FONT_SANS, fontSize: DS.text.lg, fontWeight: DS.weight.bold, color: COLORS.warning }}>
                  {churnData.atRiskCount}
                </span>
              </div>
            )}
          </div>

          {/* Monthly trend bars (last 6 completed months) */}
          {(() => {
            const completed = churnData.monthly.slice(0, -1).slice(-6);
            const maxRate = Math.max(...completed.map((m) => m.userChurnRate), 1);
            return (
              <div className="space-y-1">
                {completed.map((m) => (
                  <div key={m.month} className="flex items-center gap-2" style={{ fontSize: DS.text.xs }}>
                    <span style={{ width: "38px", color: "var(--st-text-secondary)", fontFamily: FONT_SANS, fontWeight: DS.weight.normal, flexShrink: 0 }}>
                      {formatMonthLabel(m.month)}
                    </span>
                    <div className="flex-1 flex items-center gap-1.5">
                      <div style={{
                        height: "6px",
                        width: `${Math.max((m.userChurnRate / maxRate) * 100, m.userChurnRate > 0 ? 3 : 0)}%`,
                        backgroundColor: color,
                        borderRadius: "3px",
                        opacity: 0.7,
                      }} />
                      <span style={{ fontSize: DS.text.xs, color, fontFamily: FONT_SANS, fontWeight: DS.weight.medium, flexShrink: 0 }}>
                        {m.userChurnRate.toFixed(1)}%
                      </span>
                    </div>
                    <span style={{ fontSize: DS.text.xs, color: "var(--st-text-secondary)", fontFamily: FONT_SANS, fontWeight: DS.weight.normal, flexShrink: 0 }}>
                      {m.canceledCount}/{m.activeAtStart}
                    </span>
                  </div>
                ))}
              </div>
            );
          })()}

          {/* MEMBER-only: annual vs monthly breakdown */}
          {churnData.category === "MEMBER" && (() => {
            const lastCompleted = churnData.monthly.length >= 2 ? churnData.monthly[churnData.monthly.length - 2] : null;
            if (!lastCompleted || !lastCompleted.annualActiveAtStart) return null;
            return (
              <p style={{ fontFamily: FONT_SANS, fontSize: DS.text.xs, color: "var(--st-text-secondary)", fontStyle: "italic", marginTop: "6px" }}>
                Annual: {lastCompleted.annualCanceledCount}/{lastCompleted.annualActiveAtStart} churned | Monthly: {lastCompleted.monthlyCanceledCount}/{lastCompleted.monthlyActiveAtStart} churned (last completed month)
              </p>
            );
          })()}
        </div>
      )}
    </Card>
  );
}

// ─── Year-over-Year Revenue Comparison ──────────────────────

function YoYRevenueSection({ monthlyRevenue }: { monthlyRevenue: { month: string; gross: number; net: number }[] }) {
  const currentYear = new Date().getFullYear();
  const priorYear = currentYear - 1;
  const twoYearsAgo = currentYear - 2;

  const priorData = monthlyRevenue.filter((m) => m.month.startsWith(String(priorYear)));
  const olderData = monthlyRevenue.filter((m) => m.month.startsWith(String(twoYearsAgo)));

  if (olderData.length === 0 && priorData.length === 0) return null;

  // Annual totals (net)
  const olderTotal = olderData.reduce((s, m) => s + m.net, 0);
  const priorTotal = priorData.reduce((s, m) => s + m.net, 0);
  const yoyDelta = olderTotal > 0 ? ((priorTotal - olderTotal) / olderTotal * 100) : 0;

  // Two-bar chart
  const maxVal = Math.max(olderTotal, priorTotal, 1);
  const chartHeight = 260;
  const marginTop = 28;
  const marginBottom = 32;
  const barAreaHeight = chartHeight - marginTop - marginBottom;
  const barWidth = 120;
  const gap = 60;
  const totalWidth = barWidth * 2 + gap;
  const startX = (500 - totalWidth) / 2;

  const olderH = maxVal > 0 ? Math.max((olderTotal / maxVal) * barAreaHeight, 4) : 0;
  const priorH = maxVal > 0 ? Math.max((priorTotal / maxVal) * barAreaHeight, 4) : 0;

  return (
    <div className="space-y-5">
      <SectionHeader>{twoYearsAgo} vs {priorYear} Revenue</SectionHeader>

      <Card>
        <div style={{ width: "100%", position: "relative" }}>
          <svg viewBox={`0 0 500 ${chartHeight}`} preserveAspectRatio="xMidYMid meet" style={{ width: "100%", height: "auto", display: "block" }}>
            {/* Baseline */}
            <line x1={startX - 20} x2={startX + totalWidth + 20} y1={marginTop + barAreaHeight} y2={marginTop + barAreaHeight}
              stroke="var(--st-border)" strokeWidth={1.2} />

            {/* Older year bar */}
            <rect x={startX} y={marginTop + barAreaHeight - olderH} width={barWidth} height={olderH}
              rx={4} fill="var(--st-text-secondary)" opacity={0.3} />
            {/* Older year value */}
            <text x={startX + barWidth / 2} y={marginTop + barAreaHeight - olderH - 8}
              textAnchor="middle" fill="var(--st-text-primary)" fontFamily={FONT_SANS} fontSize="16" fontWeight="700">
              {formatCurrency(olderTotal)}
            </text>
            {/* Older year label */}
            <text x={startX + barWidth / 2} y={marginTop + barAreaHeight + 22}
              textAnchor="middle" fill="var(--st-text-secondary)" fontFamily={FONT_SANS} fontSize="14" fontWeight="600">
              {twoYearsAgo}
            </text>

            {/* Prior year bar */}
            <rect x={startX + barWidth + gap} y={marginTop + barAreaHeight - priorH} width={barWidth} height={priorH}
              rx={4} fill="var(--st-accent)" opacity={0.8} />
            {/* Prior year value */}
            <text x={startX + barWidth + gap + barWidth / 2} y={marginTop + barAreaHeight - priorH - 8}
              textAnchor="middle" fill="var(--st-text-primary)" fontFamily={FONT_SANS} fontSize="16" fontWeight="700">
              {formatCurrency(priorTotal)}
            </text>
            {/* Prior year label */}
            <text x={startX + barWidth + gap + barWidth / 2} y={marginTop + barAreaHeight + 22}
              textAnchor="middle" fill="var(--st-text-secondary)" fontFamily={FONT_SANS} fontSize="14" fontWeight="600">
              {priorYear}
            </text>

            {/* YoY delta badge between bars */}
            {olderTotal > 0 && priorTotal > 0 && (
              <text x={250} y={marginTop + 10}
                textAnchor="middle" fill={yoyDelta >= 0 ? "var(--st-success)" : "var(--st-error)"} fontFamily={FONT_SANS} fontSize="14" fontWeight="700">
                {yoyDelta >= 0 ? "+" : ""}{yoyDelta.toFixed(1)}% ({yoyDelta >= 0 ? "+" : ""}{formatCurrency(priorTotal - olderTotal)})
              </text>
            )}
          </svg>
        </div>
      </Card>
    </div>
  );
}

// ─── Revenue Section ─────────────────────────────────────────

function RevenueProjectionSection({ projection }: { projection: ProjectionData }) {
  // Use actual prior year revenue if available
  const priorYearRev = projection.priorYearActualRevenue || projection.priorYearRevenue;
  const isActualPrior = !!projection.priorYearActualRevenue;

  // Only show MRR metrics if they look sane (non-zero MRR, growth rate under 50%)
  const mrrSane = projection.currentMRR > 0
    && Math.abs(projection.monthlyGrowthRate) < 50
    && projection.projectedYearEndMRR < projection.currentMRR * 20;

  return (
    <div className="space-y-5">
      <SectionHeader>Revenue</SectionHeader>

      <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
        {/* Prior year total */}
        <Card>
          <p style={{ fontFamily: FONT_SANS, ...DS.label }}>
            {projection.year - 1} Total Revenue{priorYearRev > 0 && !isActualPrior ? " (Est.)" : ""}
          </p>
          {priorYearRev > 0 ? (
            <>
              <p className="stat-hero-value" style={{ fontFamily: FONT_SANS, fontWeight: DS.weight.bold, fontSize: DS.text.xl, color: "var(--st-text-primary)", letterSpacing: "-0.03em", lineHeight: 1.1, marginTop: "6px" }}>
                {formatCurrency(priorYearRev)}
              </p>
              <p style={{ fontFamily: FONT_SANS, fontWeight: DS.weight.normal, fontSize: DS.text.sm, color: "var(--st-text-secondary)", marginTop: "4px" }}>
                {isActualPrior ? "Actual net revenue" : "Estimated from MRR data"}
              </p>
            </>
          ) : (
            <p style={{ fontFamily: FONT_SANS, fontWeight: DS.weight.normal, fontSize: DS.text.sm, color: "var(--st-text-secondary)", marginTop: "10px", opacity: 0.6 }}>
              No data available
            </p>
          )}
        </Card>

        {/* MRR metrics — only if data is sane */}
        {mrrSane ? (
          <Card>
            <p style={{ fontFamily: FONT_SANS, ...DS.label }}>
              {projection.year} Subscription MRR
            </p>
            <p className="stat-hero-value" style={{ fontFamily: FONT_SANS, fontWeight: DS.weight.bold, fontSize: DS.text.xl, color: "var(--st-text-primary)", letterSpacing: "-0.03em", lineHeight: 1.1, marginTop: "6px" }}>
              {formatCurrency(projection.currentMRR)}
            </p>
            <p style={{ fontFamily: FONT_SANS, fontWeight: DS.weight.normal, fontSize: DS.text.sm, color: "var(--st-text-secondary)", marginTop: "4px" }}>
              {projection.monthlyGrowthRate > 0 ? "+" : ""}{projection.monthlyGrowthRate}% monthly growth
            </p>
          </Card>
        ) : (
          <Card>
            <p style={{ fontFamily: FONT_SANS, ...DS.label }}>
              {projection.year} MRR
            </p>
            <p style={{ fontFamily: FONT_SANS, fontWeight: DS.weight.normal, fontSize: DS.text.sm, color: "var(--st-text-secondary)", marginTop: "10px", opacity: 0.6 }}>
              Insufficient subscription data
            </p>
          </Card>
        )}
      </div>
    </div>
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
        <p style={{ color: "var(--st-text-secondary)", fontFamily: FONT_SANS, fontWeight: DS.weight.normal }}>
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
          <h1 style={{ color: "var(--st-text-primary)", fontFamily: FONT_SANS, fontWeight: DS.weight.bold, fontSize: DS.text.xl }}>
            Studio Dashboard
          </h1>
          <div className="rounded-2xl p-6" style={{ backgroundColor: "var(--st-bg-card)", border: "1px solid var(--st-border)" }}>
            <p style={{ color: "var(--st-text-secondary)", fontFamily: FONT_SANS }}>
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
          <h1 style={{ color: "var(--st-text-primary)", fontFamily: FONT_SANS, fontWeight: DS.weight.bold, fontSize: DS.text.xl }}>
            Studio Dashboard
          </h1>
          <div className="rounded-2xl p-5 text-center" style={{ backgroundColor: "#F5EFEF", border: "1px solid rgba(160, 64, 64, 0.2)" }}>
            <p className="font-medium" style={{ color: "var(--st-error)", fontFamily: FONT_SANS }}>
              Unable to load stats
            </p>
            <p className="text-sm mt-1" style={{ color: "var(--st-error)", opacity: 0.85, fontFamily: FONT_SANS }}>
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
    <div className="min-h-screen flex flex-col items-center p-6 sm:p-8 pb-16">
      <div className="max-w-6xl w-full space-y-8">
        {/* ── Header ─────────────────────────────────── */}
        <div className="text-center space-y-3 pt-4">
          <div className="flex justify-center">
            <SkyTingLogo />
          </div>
          <h1
            style={{
              color: "var(--st-text-primary)",
              fontFamily: FONT_SANS,
              fontWeight: DS.weight.bold,
              fontSize: DS.text.xl,
              letterSpacing: "-0.03em",
            }}
          >
            Studio Dashboard
          </h1>
          <FreshnessBadge lastUpdated={data.lastUpdated} spreadsheetUrl={data.spreadsheetUrl} dataSource={data.dataSource} />
        </div>

        {/* ━━ KPI Hero Strip ━━━━━━━━━━━━━━━━━━━━━━━ */}
        <KPIHeroStrip tiles={(() => {
          const latestW = weekly.length >= 1 ? weekly[weekly.length - 1] : null;
          const inStudioCount = data.activeSubscribers.member + data.activeSubscribers.sky3;
          const inStudioNet = latestW
            ? latestW.netMemberGrowth + latestW.netSky3Growth
            : null;
          const digitalNet = latestW
            ? latestW.newSkyTingTv - latestW.skyTingTvChurn
            : null;

          // Use latest COMPLETED monthly revenue for the KPI (exclude current month)
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
              label: "In-Studio Auto-Renews",
              value: formatNumber(inStudioCount),
              delta: inStudioNet,
              sublabel: inStudioNet != null ? "net this week" : undefined,
            },
            {
              label: "Sky Ting TV (Digital)",
              value: formatNumber(data.activeSubscribers.skyTingTv),
              delta: digitalNet,
              sublabel: digitalNet != null ? "net this week" : undefined,
            },
          ];
          return tiles;
        })()} />

        {/* ━━ Revenue ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */}
        <RevenueSection data={data} trends={trends} />

        {/* ━━ Year-over-Year ━━━━━━━━━━━━━━━━━━━━━━━ */}
        {data.monthOverMonth && (
          <MonthOverMonthSection data={data.monthOverMonth} />
        )}

        {/* ━━ MRR Breakdown ━━━━━━━━━━━━━━━━━━━━━━━━ */}
        <MRRBreakdown data={data} />

        {/* Revenue Categories removed — user never requested this section */}

        {/* ━━ Auto-Renews ━━━━━━━━━━━━━━━━━━━━━━━━━━━ */}
        <SectionHeader>Auto-Renews</SectionHeader>

        {/* Members */}
        <CategoryDetail
          title="Members"
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

        {/* SKY3 */}
        <CategoryDetail
          title="SKY3"
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

        {/* SKY TING TV */}
        <CategoryDetail
          title="SKY TING TV"
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

        {/* ━━ Churn ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */}
        {trends?.churnRates ? (
          <ChurnSection churnRates={trends.churnRates} />
        ) : (
          <NoData label="Churn" />
        )}

        {/* ━━ Non Members (First Visits + Returning Non-Members + Drop-Ins) ━━━━━ */}
        {(trends?.firstVisits || trends?.returningNonMembers || trends?.dropIns) ? (
          <NonMembersSection firstVisits={trends?.firstVisits ?? null} returningNonMembers={trends?.returningNonMembers ?? null} dropIns={trends?.dropIns ?? null} />
        ) : (
          <NoData label="Non-Members (First Visits, Drop-Ins)" />
        )}

        {/* ━━ Revenue ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */}
        {trends?.projection ? (
          <RevenueProjectionSection projection={trends.projection} />
        ) : (
          <div className="space-y-5">
            <SectionHeader>Revenue</SectionHeader>
            <NoData label="Revenue Projection" />
          </div>
        )}

        {/* ━━ Year-over-Year Revenue ━━━━━━━━━━━━━━━━ */}
        {data.monthlyRevenue && data.monthlyRevenue.length > 0 && (
          <YoYRevenueSection monthlyRevenue={data.monthlyRevenue} />
        )}

        {/* ── Footer ────────────────────────────────── */}
        <div className="text-center pt-6">
          <div className="flex justify-center gap-8 text-sm" style={{ color: "var(--st-text-secondary)" }}>
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
        <p style={{ color: "var(--st-text-secondary)", fontFamily: FONT_SANS, fontWeight: DS.weight.normal }}>
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
