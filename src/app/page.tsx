"use client";

import React, { useState, useEffect, useRef, Fragment, Children } from "react";
import { Ticket, Tag, ArrowRightLeft } from "lucide-react";
import {
  AreaChart as RAreaChart,
  Area,
  Bar,
  BarChart,
  Line,
  LineChart,
  LabelList,
  Pie,
  PieChart,
  XAxis,
  YAxis,
  ReferenceLine,
} from "recharts";
import {
  ChartContainer,
  ChartLegend,
  ChartLegendContent,
  ChartTooltip,
  ChartTooltipContent,
  type ChartConfig,
} from "@/components/ui/chart";
import { Tabs, TabsList, TabsTrigger, TabsContent } from "@/components/ui/tabs";
import { ToggleGroup, ToggleGroupItem } from "@/components/ui/toggle-group";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  DashboardCard,
  CardHeader,
  CardTitle,
  CardDescription,
  CardAction,
  CardContent,
  CardFooter,
  ModuleHeader as DModuleHeader,
  MetricRow,
  InfoTooltip,
  Chip as DChip,
  CardDisclosure as DCardDisclosure,
  SparklineSlot,
  SectionHeader as DSectionHeader,
} from "@/components/dashboard";
import type {
  DashboardStats,
  TrendRowData,
  PacingData,
  ProjectionData,
  DropInModuleData,
  FirstVisitSegment,
  FirstVisitData,
  ReturningNonMemberData,
  CategoryChurnData,
  ChurnRateData,
  TrendsData,
  MonthOverMonthData,
  NewCustomerVolumeData,
  NewCustomerCohortData,
  ConversionPoolModuleData,
  ConversionPoolSlice,
  ConversionPoolSliceData,
  IntroWeekData,
} from "@/types/dashboard";

// ─── Mobile detection ────────────────────────────────────────

function useIsMobile(breakpoint = 640) {
  const [isMobile, setIsMobile] = useState(false);
  useEffect(() => {
    const mql = window.matchMedia(`(max-width: ${breakpoint - 1}px)`);
    setIsMobile(mql.matches);
    const handler = (e: MediaQueryListEvent) => setIsMobile(e.matches);
    mql.addEventListener("change", handler);
    return () => mql.removeEventListener("change", handler);
  }, [breakpoint]);
  return isMobile;
}

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

const FONT_SANS = "'Helvetica Neue', Helvetica, Arial, system-ui, -apple-system, 'Segoe UI', Roboto, sans-serif";
const FONT_BRAND = "'Cormorant Garamond', 'Times New Roman', serif";

// ─── Design System ──────────────────────────────────────────
// Typography now uses shadcn/ui utility classes (text-sm, font-semibold, etc.)
// See /src/components/dashboard/ for shared components.

// ─── Section & Category Labels ──────────────────────────────
// RULE: Use honest data labels that match Union.fit terminology.
// Never rename these to marketing-friendly alternatives.
// "Auto-Renews" not "Subscriptions". "Sky3" not "Memberships".
const LABELS = {
  autoRenews: "Auto-Renews",
  members: "Members",
  sky3: "Sky3",
  tv: "Sky Ting TV",
  nonAutoRenew: "Non-Auto-Renew",
  firstVisits: "First Visits",
  dropIns: "Drop-Ins",
  returningNonMembers: "Returning Non-Members",
  newCustomers: "New Customers",
  newCustomerFunnel: "New Customer Funnel",
  conversionPool: "Non-Auto-Renew Funnel",
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
  newCustomer: "#5F6D7A", // slate for new customer funnel
  dropIn: "#8F7A5E",     // warm sienna for drop-ins (desaturated)
  conversionPool: "#6B5F78", // muted plum for conversion pool (desaturated)
};

// ─── Formatting helpers ──────────────────────────────────────

// Churn benchmark thresholds (monthly %)
const CHURN_GOOD_MAX = 7;   // <= 7% = healthy (green)
const CHURN_WARN_MAX = 12;  // <= 12% = caution (amber), > 12% = concerning (red)

function churnBenchmarkColor(rate: number): string {
  if (rate <= CHURN_GOOD_MAX) return COLORS.success;
  if (rate <= CHURN_WARN_MAX) return COLORS.warning;
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
      className="text-muted-foreground hover:text-foreground hover:underline transition-colors"
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
          <h1 className="scroll-m-20 text-4xl font-extrabold tracking-tight">
            Studio Analytics
          </h1>
          <p className="text-muted-foreground leading-7">
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
            <a href="/settings" className="underline font-medium text-foreground">
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
            className="w-full px-6 py-4 text-base font-bold tracking-widest uppercase transition-all disabled:opacity-40 disabled:cursor-not-allowed rounded-full"
            style={{
              backgroundColor: "var(--st-accent)",
              color: "var(--st-text-light)",
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
                  <span className="text-sm text-muted-foreground">{status.step}</span>
                  <span className="text-lg font-semibold">{status.percent}%</span>
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
                  <p className="text-xs text-center text-muted-foreground opacity-70">
                    {formatEta(etaMs)}
                  </p>
                )}
                <button
                  onClick={resetPipeline}
                  className="text-xs underline text-muted-foreground opacity-60 hover:opacity-100 transition-opacity"
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
                <p className="font-medium text-emerald-700">
                  Pipeline complete
                </p>
                <p className="text-sm text-emerald-700 opacity-80">
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
                      <span className="text-muted-foreground">{check.name}</span>
                      <div className="flex items-center gap-2">
                        <span className="font-semibold tabular-nums">
                          {check.count.toLocaleString()}
                        </span>
                        <span className="text-[0.7rem]" style={{
                          color: check.status === "ok" ? COLORS.success : check.status === "warn" ? COLORS.warning : COLORS.error,
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
                <details className="text-xs" style={{ color: COLORS.warning }}>
                  <summary className="cursor-pointer font-medium">{status.warnings.length} warning{status.warnings.length > 1 ? "s" : ""}</summary>
                  <ul className="mt-1 ml-4 space-y-0.5 list-disc text-muted-foreground">
                    {status.warnings.slice(0, 10).map((w, i) => <li key={i}>{w}</li>)}
                  </ul>
                </details>
              )}

              <div className="text-center">
                <a
                  href={status.sheetUrl}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="inline-block px-5 py-2 text-sm font-medium uppercase tracking-wider transition-colors rounded-full bg-emerald-700 text-white hover:bg-emerald-800"
                >
                  Open Google Sheet
                </a>
              </div>
            </div>
          )}

          {status.state === "error" && (
            <div className="rounded-2xl p-5 text-center bg-red-50 border border-red-200">
              <p className="font-medium text-destructive">Error</p>
              <p className="text-sm mt-1 text-destructive opacity-85">
                {status.message}
              </p>
              <button
                onClick={() => setStatus({ state: "idle" })}
                className="mt-3 text-sm underline text-destructive opacity-70"
              >
                Dismiss
              </button>
            </div>
          )}
        </div>

        <div className="flex justify-center gap-8 text-sm text-muted-foreground">
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
      <h2 className="scroll-m-20 text-xl font-semibold tracking-tight">
        {children}
      </h2>
      {subtitle && (
        <p className="text-muted-foreground text-sm mt-0.5">
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
  const colorClass = delta === 0 ? "text-muted-foreground" : isGood ? "text-emerald-700" : "text-destructive";
  const bgClass = delta === 0 ? "bg-muted" : isGood ? "bg-emerald-50" : "bg-red-50";
  const arrow = delta > 0 ? "▲" : delta < 0 ? "▼" : "";

  if (compact) {
    return (
      <span className={`inline-flex items-center gap-0.5 rounded-full px-2 py-0.5 text-xs font-medium whitespace-nowrap tabular-nums ${colorClass} ${bgClass}`}>
        <span className="text-[0.55rem]">{arrow}</span>
        {isCurrency ? formatDeltaCurrency(delta) : formatDelta(delta)}
        {deltaPercent != null && <span className="opacity-75">({formatDeltaPercent(deltaPercent)})</span>}
      </span>
    );
  }

  return (
    <div className={`inline-flex items-center gap-1 rounded-full px-2.5 py-0.5 mt-1.5 ${colorClass} ${bgClass}`}>
      <span className="text-xs font-semibold">{arrow}</span>
      <span className="text-base font-semibold tracking-tight tabular-nums">
        {isCurrency ? formatDeltaCurrency(delta) : formatDelta(delta)}
      </span>
      {deltaPercent != null && (
        <span className="text-sm font-medium opacity-80 tabular-nums">
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
          <span className="text-muted-foreground font-normal">
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
          <div className="flex justify-between items-baseline text-[0.72rem] mb-1">
            <span className="text-muted-foreground">{pipelineStep}</span>
            <span className="font-bold">{pipelinePercent}%</span>
          </div>
          <div className="rounded-full overflow-hidden" style={{ height: "6px", backgroundColor: "var(--st-border)" }}>
            <div
              className="rounded-full transition-all duration-500"
              style={{ height: "100%", width: `${pipelinePercent}%`, backgroundColor: "var(--st-accent)" }}
            />
          </div>
          {etaMs > 3000 && (
            <p className="text-[0.68rem] text-muted-foreground opacity-70 mt-0.5 text-center">
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
                  fontWeight: 500,
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
            fontWeight: 400,
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

      </svg>
    </div>
  );
}

// ─── Module spacing tokens ───────────────────────────────────
/** Spacing tokens (px) for module anatomy */
const MOD = {
  cardPad: "var(--st-card-pad)",
  headerToKpi: "12px",
  kpiToToggle: "0px",
  toggleToTabs: "20px",
  tabsToTable: "12px",
  rowH: "var(--st-row-h)",
} as const;

// ─── Card wrapper (used selectively) ─────────────────────────

function Card({ children, padding, matchHeight = false }: { children: React.ReactNode; padding?: string; matchHeight?: boolean }) {
  return (
    <DashboardCard matchHeight={matchHeight}>
      <CardContent>
        {children}
      </CardContent>
    </DashboardCard>
  );
}

function NoData({ label }: { label: string }) {
  return (
    <Card>
      <p className="text-sm text-muted-foreground">
        {label}: <span className="opacity-60">No data available</span>
      </p>
    </Card>
  );
}

// ─── Module Design System ────────────────────────────────────
// Shared component library for New Customer Funnel, Drop-Ins,
// and Conversion Pool. Identical anatomy, spacing, and behavior.

/** Module header: left dot + title, right optional control */
function ModuleHeader({ color, title, summaryPill, detailsOpen, onToggleDetails, children }: {
  color: string; title: string; summaryPill?: string;
  detailsOpen?: boolean; onToggleDetails?: () => void;
  children?: React.ReactNode;
}) {
  return (
    <div className="flex items-center justify-between shrink-0 pb-1">
      <div className="flex items-center gap-3">
        <span className="w-2.5 h-2.5 rounded-full shrink-0" style={{ backgroundColor: color }} />
        <span className="text-[16px] leading-[20px] font-semibold tracking-[-0.01em] text-zinc-900">{title}</span>
        {summaryPill && (
          <span className="text-[12px] leading-[16px] font-medium text-zinc-500 bg-zinc-100 rounded-full px-2 py-0.5">{summaryPill}</span>
        )}
      </div>
      <div className="flex items-center gap-3">
        {children}
        {onToggleDetails && (
          <button
            type="button"
            onClick={onToggleDetails}
            className="text-[12px] leading-[16px] text-zinc-500 hover:text-zinc-900 font-medium inline-flex items-center gap-1 transition-colors"
          >
            {detailsOpen ? "Hide" : "Details"}
            <span
              className="text-[8px] transition-transform duration-150"
              style={{ transform: detailsOpen ? "rotate(90deg)" : "rotate(0)" }}
            >&#9654;</span>
          </button>
        )}
      </div>
    </div>
  );
}

/** Subsection header — Apple Health style: icon + colored label */
function SubsectionHeader({ children, icon: Icon, color }: { children: React.ReactNode; icon?: React.ComponentType<{ className?: string; style?: React.CSSProperties }>; color?: string }) {
  return (
    <div className="flex items-center gap-2.5 pt-3">
      {Icon && <Icon className="size-[22px]" style={color ? { color } : undefined} />}
      <p className="text-[17px] leading-none font-bold capitalize" style={color ? { color } : undefined}>
        {children}
      </p>
    </div>
  );
}

/** Info tooltip icon (24x24 hit area) — shared across modules */
function MInfoIcon({ tooltip }: { tooltip: string }) {
  const [show, setShow] = useState(false);
  const timerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  function handleEnter() {
    timerRef.current = setTimeout(() => setShow(true), 250);
  }
  function handleLeave() {
    if (timerRef.current) clearTimeout(timerRef.current);
    setShow(false);
  }

  return (
    <span
      onMouseEnter={handleEnter}
      onMouseLeave={handleLeave}
      className="inline-flex items-center justify-center h-3.5 w-3.5 shrink-0 cursor-help text-[11px] text-neutral-400 relative"
    >
      &#9432;
      {show && (
        <span style={{
          position: "absolute", bottom: "calc(100% + 8px)", left: "50%",
          transform: "translateX(-50%)",
          padding: "8px 12px", borderRadius: 6,
          backgroundColor: "var(--st-bg-dark)", color: "var(--st-text-light)",
          fontSize: "12px", lineHeight: 1.4, maxWidth: 240, minWidth: 120,
          whiteSpace: "normal", zIndex: 50,
          pointerEvents: "none",
          boxShadow: "0 2px 8px rgba(0,0,0,0.15)",
        }}>
          {tooltip}
        </span>
      )}
    </span>
  );
}

/** Strict-alignment KPI component.
 *  size="hero"    — top-of-page KPIs (52px value, fixed 56/40/28 rows)
 *  size="section" — subsection cards (40px value, collapsed label+meta support block)
 *  emphasis       — "primary" (default) full weight, "secondary" slightly muted value
 *  tooltip        — renders (i) icon anchored to label (not floating in topRight) */
function KPI({
  value,
  valueSuffix,
  label,
  tooltip,
  topRight,
  metaLeft,
  metaRight,
  size = "hero",
  emphasis = "primary",
}: {
  value: React.ReactNode;
  valueSuffix?: React.ReactNode;
  label: string;
  tooltip?: string;
  topRight?: React.ReactNode;
  metaLeft?: React.ReactNode;
  metaRight?: React.ReactNode;
  size?: "hero" | "section";
  emphasis?: "primary" | "secondary";
}) {
  const s = size === "section";
  const sec = emphasis === "secondary";
  return (
    <div className="min-w-0">
      {/* Row A: Value (LOCK HEIGHT + BASELINE ALIGN) */}
      <div className={`${s ? "h-[48px]" : "h-[56px]"} flex items-end justify-between gap-2`}>
        <div className="flex items-baseline gap-2 min-w-0">
          <div className={`${s ? (sec ? "text-[34px] leading-[34px] text-neutral-700" : "text-[40px] leading-[40px] text-neutral-900") : "text-[52px] leading-[52px] text-neutral-900"} font-semibold tracking-[-0.02em] tabular-nums`}>
            {value}
          </div>
          {valueSuffix ? (
            <div className={`${s ? (sec ? "text-[14px] leading-[14px] text-neutral-400" : "text-[16px] leading-[16px] text-neutral-500") : "text-[18px] leading-[18px] text-neutral-600"} font-semibold tabular-nums`}>
              {valueSuffix}
            </div>
          ) : null}
        </div>
        {topRight ? <div className={`shrink-0 ${s ? "translate-y-[-4px]" : "translate-y-[-6px]"}`}>{topRight}</div> : null}
      </div>

      {s ? (
        /* Section variant: collapsed label + meta support block (no fixed heights) */
        <div className="mt-1">
          <div className="inline-flex items-center gap-1 max-w-full">
            <span className="text-[14px] leading-[18px] text-neutral-600 whitespace-nowrap truncate">{label}</span>
            {tooltip ? <MInfoIcon tooltip={tooltip} /> : null}
          </div>
          {(metaLeft || metaRight) ? (
            <div className="mt-1 flex items-center gap-2 min-w-0 leading-none">
              {metaLeft ? <div className="shrink-0">{metaLeft}</div> : null}
              {metaRight ? (
                <div className="truncate text-[12px] text-neutral-500 leading-none">{metaRight}</div>
              ) : null}
            </div>
          ) : null}
        </div>
      ) : (
        /* Hero variant: fixed-height rows for cross-component alignment */
        <>
          <div className="h-[40px] mt-1 text-[16px] leading-[20px] text-neutral-600">
            <span className="inline-flex items-center gap-1">
              <span>{label}</span>
              {tooltip ? <MInfoIcon tooltip={tooltip} /> : null}
            </span>
          </div>
          <div className="h-[28px] mt-2 flex items-center gap-2 min-w-0 leading-none">
            {metaLeft ? <div className="shrink-0">{metaLeft}</div> : null}
            {metaRight ? (
              <div className="truncate leading-none text-[14px] text-neutral-500">{metaRight}</div>
            ) : null}
          </div>
        </>
      )}
    </div>
  );
}

/** 3-column KPI grid for Non-Members cards — vertical dividers between columns */
function KPIGrid3({ children }: { children: React.ReactNode }) {
  const items = Children.toArray(children);
  return (
    <div className="grid grid-cols-3 gap-4 items-start" style={{ padding: "8px 0" }}>
      {items.map((child, i) => (
        <div key={i} className={`flex flex-col items-start${i > 0 ? " border-l border-zinc-200 pl-4" : ""}`}>
          {child}
        </div>
      ))}
    </div>
  );
}

/** Shared funnel KPI row: Total / 3-Week Conversion Rate / Expected Converts */
function FunnelSummaryRow({ total, totalLabel, rate, rateTooltip, expected, expectedTooltip, expectedLabel }: {
  total: number | null;
  totalLabel: string;
  rate: number | null;
  rateTooltip?: string;
  expected: number | null;
  expectedTooltip?: string;
  expectedLabel?: string;
}) {
  return (
    <div className="min-h-[76px] shrink-0 grid grid-cols-3 gap-6 items-start pt-1">
      {/* Left: Total */}
      <div className="min-w-0">
        <span className="text-[40px] leading-[44px] font-semibold tracking-[-0.02em] text-zinc-900 tabular-nums">
          {total != null ? formatNumber(total) : "\u2014"}
        </span>
        <div className="mt-1 text-[13px] leading-[16px] font-medium text-zinc-500">{totalLabel}</div>
      </div>
      {/* Middle: 3-Week Conversion Rate */}
      <div className="min-w-0 border-l border-zinc-200/50 pl-6">
        <div className="flex items-baseline">
          <span className="text-[40px] leading-[44px] font-semibold tracking-[-0.02em] text-zinc-900 tabular-nums">
            {rate != null ? rate.toFixed(1) : "\u2014"}
          </span>
          {rate != null && (
            <span className="text-[18px] leading-[22px] font-semibold tracking-[-0.01em] text-zinc-500 ml-0.5 tabular-nums">%</span>
          )}
        </div>
        <div className="mt-1 inline-flex items-center gap-1">
          <span className="text-[13px] leading-[16px] font-medium text-zinc-500">3-Week Conversion Rate</span>
          {rateTooltip && <MInfoIcon tooltip={rateTooltip} />}
        </div>
      </div>
      {/* Right: Expected Converts */}
      <div className="min-w-0 border-l border-zinc-200/50 pl-6">
        <span className="text-[40px] leading-[44px] font-semibold tracking-[-0.02em] text-zinc-900 tabular-nums">
          {expected != null ? formatNumber(expected) : "\u2014"}
        </span>
        <div className="mt-1 inline-flex items-center gap-1">
          <span className="text-[13px] leading-[16px] font-medium text-zinc-500">{expectedLabel ?? "Expected Converts"}</span>
          {expectedTooltip && <MInfoIcon tooltip={expectedTooltip} />}
        </div>
      </div>
    </div>
  );
}

/** Compute padded Y domain so the signal fills the chart height */
function paddedDomain(values: number[], minPad: number): [number, number] {
  const min = Math.min(...values);
  const max = Math.max(...values);
  const range = Math.max(max - min, 0);
  const pad = Math.max(range * 0.18, minPad);
  return [min - pad, max + pad];
}

/** Premium sparkline using shadcn/ui ChartContainer — tight domain, area fill, last-dot emphasis */
function Sparkline({ values, labels, color = "#71717a", minPad = 8, formatValue, chartLabel }: {
  values: number[];
  labels?: string[];
  color?: string;
  minPad?: number;
  formatValue?: (v: number) => string;
  chartLabel?: string;
}) {
  if (values.length < 2) return null;

  const safeLabels = labels ?? values.map(() => "");
  // Use raw weekly data — no interpolation
  const data = values.map((v, i) => ({ value: v, label: safeLabels[i] ?? "" }));
  const [yMin, yMax] = paddedDomain(values, minPad);

  const lastValue = values[values.length - 1];
  const displayValue = formatValue ? formatValue(lastValue) : formatNumber(lastValue);

  // shadcn ChartConfig — maps "value" key to color + label
  const chartConfig = {
    value: { label: chartLabel ?? "Value", color },
  } satisfies ChartConfig;

  // Unique gradient ID per color to avoid collisions
  const gradId = `spark-${color.replace(/[^a-zA-Z0-9]/g, "")}`;

  return (
    <ChartContainer config={chartConfig} className="h-[132px] w-full mt-1 [&_.recharts-surface]:overflow-visible">
      <RAreaChart
        data={data}
        margin={{ top: 18, right: 20, bottom: 18, left: 12 }}
      >
        <defs>
          <linearGradient id={gradId} x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor="var(--color-value)" stopOpacity={0.12} />
            <stop offset="100%" stopColor="var(--color-value)" stopOpacity={0.02} />
          </linearGradient>
        </defs>

        <XAxis dataKey="label" hide />
        <YAxis domain={[yMin, yMax]} hide />

        {/* Subtle reference line at mid-domain */}
        <ReferenceLine
          y={(yMin + yMax) / 2}
          stroke="var(--color-value)"
          strokeOpacity={0.10}
          strokeDasharray="3 3"
        />

        {/* Tooltip on hover */}
        <ChartTooltip
          content={<ChartTooltipContent hideLabel hideIndicator formatter={(v) => formatNumber(v as number)} />}
          cursor={false}
        />

        {/* Gradient fill under line (no tooltip) */}
        <Area
          type="monotone"
          dataKey="value"
          stroke="none"
          fill={`url(#${gradId})`}
          isAnimationActive={false}
          tooltipType="none"
        />

        {/* Main trend line */}
        <Area
          type="monotone"
          dataKey="value"
          stroke="var(--color-value)"
          strokeWidth={2.25}
          fill="none"
          dot={false}
          activeDot={false}
          isAnimationActive={false}
        />

        {/* Last-point dot + value label (no tooltip) */}
        <Area
          type="monotone"
          dataKey="value"
          stroke="none"
          fill="none"
          isAnimationActive={false}
          tooltipType="none"
          dot={(props: Record<string, unknown>) => {
            const { cx, cy, index } = props as { cx: number; cy: number; index: number };
            if (index !== data.length - 1) return <circle r="0" cx={0} cy={0} fill="none" />;
            return (
              <g>
                <circle cx={cx} cy={cy} r={4} fill="white" stroke="var(--color-value)" strokeWidth={2} />
                <text
                  x={cx}
                  y={cy - 10}
                  textAnchor="middle"
                  fill="var(--color-value)"
                  fontSize={12}
                  fontWeight={500}
                  fontFamily="'Helvetica Neue', Helvetica, Arial, system-ui, sans-serif"
                  style={{ fontVariantNumeric: "tabular-nums" }}
                >
                  {displayValue}
                </text>
              </g>
            );
          }}
          activeDot={false}
        />
      </RAreaChart>
    </ChartContainer>
  );
}

/** Underline tabs row with sliding indicator */
function UnderlineTabs({ tabs, active, onChange }: {
  tabs: { key: string; label: string }[];
  active: string;
  onChange: (key: string) => void;
}) {
  const containerRef = useRef<HTMLDivElement>(null);
  const [indicator, setIndicator] = useState({ left: 0, width: 0 });

  useEffect(() => {
    if (!containerRef.current) return;
    const activeBtn = containerRef.current.querySelector(`[data-tab-key="${active}"]`) as HTMLElement | null;
    if (activeBtn) {
      setIndicator({ left: activeBtn.offsetLeft, width: activeBtn.offsetWidth });
    }
  }, [active, tabs]);

  return (
    <div ref={containerRef} className="relative border-b border-border" style={{ marginBottom: MOD.tabsToTable }}>
      <div className="flex">
        {tabs.map((t) => {
          const isActive = t.key === active;
          return (
            <button
              key={t.key}
              data-tab-key={t.key}
              type="button"
              onClick={() => onChange(t.key)}
              className={`px-3 pt-1.5 pb-2 text-[13px] tracking-wide bg-transparent border-none cursor-pointer transition-colors outline-none ${isActive ? "font-semibold text-foreground" : "font-normal text-muted-foreground"}`}
            >
              {t.label}
            </button>
          );
        })}
      </div>
      {/* Sliding indicator */}
      <div className="absolute -bottom-px h-0.5 bg-foreground transition-all duration-200 ease-in-out" style={{
        left: indicator.left, width: indicator.width,
      }} />
    </div>
  );
}

/** Shared table class strings for module tables */
const modThClass = "text-right px-3 pt-1 pb-1.5 text-xs font-medium uppercase tracking-wide text-muted-foreground whitespace-nowrap tabular-nums leading-none border-b border-border";
const modTdClass = "text-right px-3 py-1.5 tabular-nums text-sm leading-5";

/** Mini segmented bar (timing, mix, distribution). High-contrast segment spec. */
function SegmentedBar({ segments, height = 8, colors, tooltip }: {
  segments: { value: number; label: string }[];
  height?: number;
  colors?: string[];
  tooltip?: string;
}) {
  const total = segments.reduce((s, seg) => s + seg.value, 0);
  if (total === 0) return <span className="text-xs text-muted-foreground">&mdash;</span>;
  // Default colors: dark→mid→light
  const defaultColors = [
    "rgba(65, 58, 58, 0.65)",
    "rgba(65, 58, 58, 0.38)",
    "rgba(65, 58, 58, 0.18)",
    "rgba(65, 58, 58, 0.10)",
  ];
  const c = colors ?? defaultColors;
  return (
    <div
      title={tooltip ?? segments.map((s) => `${s.label}: ${s.value} (${Math.round((s.value / total) * 100)}%)`).join(" · ")}
      style={{ display: "flex", height, borderRadius: Math.ceil(height / 2), overflow: "hidden", gap: 1, backgroundColor: "var(--st-bg-card)" }}
    >
      {segments.map((seg, i) => {
        if (seg.value <= 0) return null;
        const pct = Math.round((seg.value / total) * 100);
        const showInline = pct >= 12 && height >= 10;
        return (
          <div key={i} style={{
            flex: Math.max(seg.value, total * 0.03),
            backgroundColor: c[i % c.length],
            borderRadius: 1, minWidth: seg.value > 0 ? 2 : 0,
            display: "flex", alignItems: "center", justifyContent: "center",
          }}>
            {showInline && (
              <span style={{ fontSize: "9px", fontWeight: 600, color: "white", textShadow: "0 0 2px rgba(0,0,0,0.3)", lineHeight: 1 }}>
                {pct}%
              </span>
            )}
          </div>
        );
      })}
    </div>
  );
}

// ─── Chip (shared badge/label) ───────────────────────────────

type ChipVariant = "neutral" | "positive" | "negative" | "accent";

function Chip({ children, variant = "neutral", title: chipTitle }: {
  children: React.ReactNode;
  variant?: ChipVariant;
  title?: string;
}) {
  const variantClass: Record<ChipVariant, string> = {
    neutral: "text-muted-foreground bg-muted",
    positive: "text-emerald-700 bg-emerald-50",
    negative: "text-destructive bg-red-50",
    accent: "text-foreground bg-muted",
  };
  return (
    <span
      title={chipTitle}
      className={`inline-flex items-center gap-[3px] text-[11px] font-medium px-2 py-0.5 rounded tracking-wide tabular-nums whitespace-nowrap leading-none transition-colors ${variantClass[variant]}`}
    >
      {children}
    </span>
  );
}

// ─── CardDisclosure (collapse details by default) ────────────

function CardDisclosure({
  open,
  children,
}: {
  open: boolean;
  children: React.ReactNode;
}) {
  return (
    <div
      className="overflow-hidden transition-all duration-300 ease-in-out"
      style={{
        maxHeight: open ? "2000px" : "0px",
        opacity: open ? 1 : 0,
      }}
    >
      <div className="mt-4 pt-4 border-t border-zinc-200 space-y-4">
        {children}
      </div>
    </div>
  );
}

// ─── MiniBars (micro-trend sparkline) ────────────────────────

function MiniBars({ values }: { values: number[] }) {
  if (values.length === 0) return null;
  const max = Math.max(...values, 1);
  return (
    <div className="mt-3 h-10 flex items-end gap-1.5">
      {values.map((v, i) => {
        const isLast = i === values.length - 1;
        const h = max > 0 ? Math.max(Math.round((v / max) * 34), v > 0 ? 2 : 0) : 0;
        return (
          <div
            key={i}
            className={`w-6 rounded-[3px] ${isLast ? "bg-zinc-500/80" : "bg-zinc-200/70"}`}
            style={{ height: h }}
          />
        );
      })}
    </div>
  );
}

// ─── Dashboard Grid (bento container) ────────────────────────

function DashboardGrid({ children }: { children: React.ReactNode }) {
  return (
    <div
      className="bento-grid"
      style={{
        display: "grid",
        gridTemplateColumns: "repeat(12, 1fr)",
        gap: "var(--st-grid-gap)",
        alignItems: "start",
      }}
    >
      {children}
    </div>
  );
}

// ─── Skeleton (loading placeholder) ──────────────────────────

function Skeleton({ width, height = 16, style: extraStyle }: {
  width?: string | number;
  height?: number;
  style?: React.CSSProperties;
}) {
  return (
    <div
      style={{
        width: width ?? "100%",
        height,
        borderRadius: 4,
        backgroundColor: "rgba(65, 58, 58, 0.06)",
        animation: "skeleton-pulse 1.5s ease-in-out infinite",
        ...extraStyle,
      }}
    />
  );
}

function ModuleSkeleton() {
  return (
    <Card>
      <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: MOD.headerToKpi }}>
        <Skeleton width={7} height={7} style={{ borderRadius: "50%" }} />
        <Skeleton width={120} height={12} />
      </div>
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 12, marginBottom: MOD.kpiToToggle }}>
        {[0, 1, 2].map((i) => (
          <div key={i} style={{ padding: "0 12px" }}>
            <Skeleton width={80} height={28} />
            <Skeleton width={100} height={12} style={{ marginTop: 6 }} />
          </div>
        ))}
      </div>
      {[0, 1, 2, 3].map((i) => (
        <Skeleton key={i} height={48} style={{ marginBottom: 2 }} />
      ))}
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
    <div className={`py-2 ${isLast ? "" : "border-b border-border"}`}>
      <div className="flex items-center gap-6">
        <span className="text-sm font-medium text-muted-foreground">
          {label}
        </span>
        <div className="flex items-center gap-3">
          <span className="text-base font-semibold tracking-tight">
            {value}
          </span>
          <DeltaBadge delta={delta} deltaPercent={deltaPercent} isPositiveGood={isPositiveGood} isCurrency={isCurrency} compact />
        </div>
      </div>
      {sublabel && (
        <p className="text-xs text-muted-foreground mt-0.5 italic">
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
        <DashboardCard key={i}>
          <CardHeader>
            <CardDescription className="text-sm leading-none font-medium uppercase tracking-wide">
              {tile.label}
            </CardDescription>
            <CardTitle className="text-4xl font-extrabold tracking-tight tabular-nums">
              {tile.value}
            </CardTitle>
          </CardHeader>
          {(tile.sublabel || tile.delta != null) && (
            <CardContent className="flex items-center gap-2">
              {tile.delta != null && (
                <DeltaBadge delta={tile.delta} deltaPercent={tile.deltaPercent ?? null} isPositiveGood={tile.isPositiveGood} isCurrency={tile.isCurrency} compact />
              )}
              {tile.sublabel && (
                <span className="text-muted-foreground text-sm">
                  {tile.sublabel}
                </span>
              )}
            </CardContent>
          )}
        </DashboardCard>
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
  const isMobile = useIsMobile();
  // Only show completed months (exclude current calendar month)
  const nowDate = new Date();
  const currentMonthKey = `${nowDate.getFullYear()}-${String(nowDate.getMonth() + 1).padStart(2, "0")}`;
  const monthlyRevenue = (data.monthlyRevenue || []).filter((m) => m.month < currentMonthKey);

  // Bar chart data (last 12 months)
  const revenueBarData = monthlyRevenue.slice(-12).map((m) => ({
    month: formatShortMonth(m.month),
    gross: m.gross,
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

  const revenueChartConfig = {
    gross: { label: "Gross Revenue", color: COLORS.member },
  } satisfies ChartConfig;

  return (
    <div className="flex flex-col gap-4">
      <SectionHeader>{LABELS.revenue}</SectionHeader>

      {/* Monthly Gross Revenue — bar chart */}
      {revenueBarData.length > 1 && (
        <DashboardCard>
          <CardHeader>
            <CardTitle>Monthly Gross Revenue</CardTitle>
            {currentMonth && (
              <CardDescription>
                {formatShortMonth(currentMonth.month)}: {formatCurrency(currentMonth.gross)}
                {momDeltaPct != null && ` (${momDeltaPct > 0 ? "+" : ""}${momDeltaPct}%)`}
              </CardDescription>
            )}
          </CardHeader>
          <CardContent>
            <ChartContainer config={revenueChartConfig} className="h-[250px] w-full">
              <RAreaChart accessibilityLayer data={revenueBarData} margin={{ top: 20, left: isMobile ? 8 : 24, right: isMobile ? 8 : 24 }}>
                <defs>
                  <linearGradient id="fillGross" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="5%" stopColor="var(--color-gross)" stopOpacity={0.8} />
                    <stop offset="95%" stopColor="var(--color-gross)" stopOpacity={0.1} />
                  </linearGradient>
                </defs>
                <YAxis hide domain={["dataMin - 20000", "dataMax + 10000"]} />
                <XAxis
                  dataKey="month"
                  tickLine={false}
                  axisLine={false}
                  tickMargin={8}
                  interval={isMobile ? 2 : 0}
                  fontSize={isMobile ? 11 : 12}
                />
                <ChartTooltip
                  cursor={false}
                  content={<ChartTooltipContent indicator="line" formatter={(v) => formatCurrency(v as number)} />}
                />
                <Area
                  dataKey="gross"
                  type="natural"
                  fill="url(#fillGross)"
                  fillOpacity={0.4}
                  stroke="var(--color-gross)"
                  strokeWidth={2}
                  dot={{ fill: "var(--color-gross)" }}
                  activeDot={{ r: 6 }}
                >
                  {!isMobile && (
                    <LabelList
                      position="top"
                      offset={12}
                      className="fill-foreground"
                      fontSize={12}
                      formatter={(v: number) => formatCompactCurrency(v)}
                    />
                  )}
                </Area>
              </RAreaChart>
            </ChartContainer>
          </CardContent>
          {momDelta != null && momDeltaPct != null && (
            <CardFooter className="flex-col items-start gap-2 text-sm">
              <div className="flex gap-2 leading-none font-medium">
                {momDeltaPct > 0 ? "+" : ""}{momDeltaPct}% month over month
              </div>
              <div className="text-muted-foreground leading-none">
                Last 12 months of gross revenue
              </div>
            </CardFooter>
          )}
        </DashboardCard>
      )}

      {/* Breakdown card */}
      {currentMonth && (
        <DashboardCard>
          <CardHeader>
            <CardTitle>{formatMonthLabel(currentMonth.month)} Breakdown</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="flex flex-col">
              {[
                { label: "Gross Revenue", value: formatCurrency(currentMonth.gross), bold: true },
                { label: "Net Revenue", value: formatCurrency(currentMonth.net), bold: false },
                { label: "Fees + Refunds", value: `-${formatCurrency(currentMonth.gross - currentMonth.net)}`, bold: false, destructive: true },
              ].map((row, i) => (
                <div key={i} className={`flex justify-between items-center py-2.5 ${i < 2 ? "border-b border-border" : ""}`}>
                  <span className="text-sm text-muted-foreground">{row.label}</span>
                  <span className={`text-sm tabular-nums ${row.bold ? "font-semibold" : "font-medium"} ${row.destructive ? "text-destructive" : ""}`}>
                    {row.value}
                  </span>
                </div>
              ))}
            </div>
          </CardContent>
        </DashboardCard>
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
    <DashboardCard>
      <CardHeader>
        <CardTitle>{LABELS.mrr}</CardTitle>
        <CardDescription className="text-2xl font-semibold tracking-tight tabular-nums text-foreground">
          {formatCurrency(data.mrr.total)}
        </CardDescription>
      </CardHeader>
      <CardContent>
        <div className="flex flex-col">
          {segments.map((seg, i) => (
            <div key={i} className={`flex justify-between items-center py-2.5 ${i < segments.length - 1 ? "border-b border-border" : ""}`}>
              <div className="flex items-center gap-2">
                <span className="w-2 h-2 rounded-full shrink-0" style={{ backgroundColor: seg.color, opacity: 0.85 }} />
                <span className="text-sm text-muted-foreground">{seg.label}</span>
              </div>
              <span className="text-sm font-semibold tabular-nums">
                {formatCurrency(seg.value)}
              </span>
            </div>
          ))}
        </div>
      </CardContent>
    </DashboardCard>
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
      <div className="flex items-baseline justify-between mb-2">
        <div className="flex items-center gap-2">
          <span className="w-2 h-2 rounded-full shrink-0" style={{ backgroundColor: COLORS.teal, opacity: 0.85 }} />
          <span className="text-sm leading-none font-medium text-muted-foreground uppercase tracking-wide">{LABELS.firstVisits}</span>
        </div>
        <span className="text-4xl font-semibold tracking-tight tabular-nums">
          {formatNumber(firstVisits.currentWeekTotal)}
          <span className="text-xs font-medium text-muted-foreground uppercase tracking-wide ml-1.5">this week</span>
        </span>
      </div>
      <p className="text-xs text-muted-foreground mb-1">Last 5 weeks by source</p>
      <div className="flex flex-col gap-1">
        {segments.map((seg) => {
          const count = agg[seg] || 0;
          if (count === 0 && aggTotal === 0) return null;
          return (
            <div key={seg} className="flex items-center justify-between">
              <div className="flex items-center gap-1.5">
                <span className="w-1.5 h-1.5 rounded-full" style={{ backgroundColor: SEGMENT_COLORS[seg] }} />
                <span className="text-sm text-muted-foreground">{SEGMENT_LABELS[seg]}</span>
              </div>
              <span className="text-sm font-semibold tabular-nums">
                {formatNumber(count)}
                {aggTotal > 0 && <span className="font-normal text-xs text-muted-foreground ml-1">{Math.round((count / aggTotal) * 100)}%</span>}
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
      <div className="flex items-baseline justify-between mb-2">
        <div className="flex items-center gap-2">
          <span className="w-2 h-2 rounded-full shrink-0" style={{ backgroundColor: COLORS.copper, opacity: 0.85 }} />
          <span className="text-sm leading-none font-medium text-muted-foreground uppercase tracking-wide">{LABELS.returningNonMembers}</span>
        </div>
        <span className="text-4xl font-semibold tracking-tight tabular-nums">
          {formatNumber(returningNonMembers.currentWeekTotal)}
          <span className="text-xs font-medium text-muted-foreground uppercase tracking-wide ml-1.5">this week</span>
        </span>
      </div>
      <p className="text-xs text-muted-foreground mb-1">Last 5 weeks by source</p>
      <div className="flex flex-col gap-1">
        {rnmSegments.map((seg) => (
          <div key={seg} className="flex items-center justify-between">
            <div className="flex items-center gap-1.5">
              <span className="w-1.5 h-1.5 rounded-full" style={{ backgroundColor: RNM_SEGMENT_COLORS[seg] }} />
              <span className="text-sm text-muted-foreground">{RNM_SEGMENT_LABELS[seg]}</span>
            </div>
            <span className="text-sm font-semibold tabular-nums">
              {formatNumber(aggDisplay[seg] || 0)}
              {rnmAggTotal > 0 && <span className="font-normal text-xs text-muted-foreground ml-1">{Math.round(((aggDisplay[seg] || 0) / rnmAggTotal) * 100)}%</span>}
            </span>
          </div>
        ))}
      </div>
    </Card>
  );
}

// ─── Layer 3: New Customer Volume Card ──────────────────────

/** Shared logic for both New Customer cards */
function useNewCustomerData(volume: NewCustomerVolumeData | null, cohorts: NewCustomerCohortData | null) {
  const today = new Date();
  today.setHours(0, 0, 0, 0);

  function daysElapsed(cohortStart: string): number {
    const start = new Date(cohortStart + "T00:00:00");
    return Math.floor((today.getTime() - start.getTime()) / (1000 * 60 * 60 * 24));
  }

  const allCohorts = cohorts?.cohorts ?? [];
  const completeCohorts = allCohorts.filter((c) => daysElapsed(c.cohortStart) >= 21);
  const incompleteCohorts = allCohorts.filter((c) => daysElapsed(c.cohortStart) < 21);
  const displayComplete = completeCohorts.slice(-5);

  const avgRate = cohorts?.avgConversionRate ?? null;
  const currentCohortCount = volume?.currentWeekCount ?? (incompleteCohorts.length > 0 ? incompleteCohorts[incompleteCohorts.length - 1].newCustomers : null);
  const expectedAutoRenews = (currentCohortCount !== null && currentCohortCount !== undefined && avgRate !== null)
    ? Math.round(currentCohortCount * avgRate / 100) : null;

  const insightCohorts = displayComplete.filter((c) => c.total3Week > 0);
  const totalWk1 = insightCohorts.reduce((s, c) => s + c.week1, 0);
  const totalConv = insightCohorts.reduce((s, c) => s + c.total3Week, 0);
  const sameWeekPct = totalConv > 0 ? Math.round((totalWk1 / totalConv) * 100) : null;

  const convTooltip = avgRate !== null
    ? `Weighted avg across last ${Math.min(completeCohorts.length, 5)} complete cohorts.${sameWeekPct ? ` ${sameWeekPct}% of conversions happen in the same week.` : ""}`
    : "Needs 3+ complete cohorts";
  const projTooltip = expectedAutoRenews !== null
    ? `${formatNumber(currentCohortCount ?? 0)} new \u00d7 ${avgRate?.toFixed(1)}% conversion. Based on last ${Math.min(completeCohorts.length, 5)} complete cohorts.`
    : "";

  return { completeCohorts, incompleteCohorts, displayComplete, avgRate, currentCohortCount, expectedAutoRenews, convTooltip, projTooltip, daysElapsed };
}

// ─── New Customer Overview Card ─────────────────────────────

function NewCustomerOverviewCard({ volume, cohorts }: {
  volume: NewCustomerVolumeData | null;
  cohorts: NewCustomerCohortData | null;
}) {
  if (!volume && !cohorts) return null;
  const { completeCohorts, avgRate, currentCohortCount, expectedAutoRenews, convTooltip, projTooltip } = useNewCustomerData(volume, cohorts);

  return (
    <DashboardCard matchHeight>
      <CardHeader>
        <CardTitle>{LABELS.newCustomerFunnel}</CardTitle>
        <CardDescription>{completeCohorts.length} complete cohort{completeCohorts.length !== 1 ? "s" : ""}</CardDescription>
      </CardHeader>

      <CardContent>
        <MetricRow
          slots={[
            { value: currentCohortCount != null ? formatNumber(currentCohortCount) : "\u2014", label: "New Customers" },
            { value: avgRate != null ? avgRate.toFixed(1) : "\u2014", valueSuffix: avgRate != null ? "%" : undefined, label: "3-Week Conv. Rate", labelExtra: convTooltip ? <InfoTooltip tooltip={convTooltip} /> : undefined },
            { value: expectedAutoRenews != null ? formatNumber(expectedAutoRenews) : "\u2014", label: "Expected Converts", labelExtra: projTooltip ? <InfoTooltip tooltip={projTooltip} /> : undefined },
          ]}
        />
      </CardContent>

      <CardFooter className="flex-col items-start gap-2 text-sm">
        <div className="flex gap-2 leading-none font-medium">
          {avgRate != null ? `${avgRate.toFixed(1)}% avg conversion rate` : "Calculating..."}
        </div>
        <div className="text-muted-foreground leading-none">
          Weekly new customer cohorts
        </div>
      </CardFooter>
    </DashboardCard>
  );
}

// ─── New Customer Chart Card (line chart + cohort tables) ───

function NewCustomerChartCard({ volume, cohorts }: {
  volume: NewCustomerVolumeData | null;
  cohorts: NewCustomerCohortData | null;
}) {
  const isMobile = useIsMobile();
  if (!volume && !cohorts) return null;

  const [activeTab, setActiveTab] = useState<string>("complete");
  const [expandedRow, setExpandedRow] = useState<string | null>(null);
  const [hoveredCohort, setHoveredCohort] = useState<string | null>(null);

  const { completeCohorts, incompleteCohorts, displayComplete, daysElapsed } = useNewCustomerData(volume, cohorts);
  const mostRecentComplete = displayComplete.length > 0 ? displayComplete[displayComplete.length - 1].cohortStart : null;
  const timingColors = ["rgba(65, 58, 58, 0.65)", "rgba(65, 58, 58, 0.38)", "rgba(65, 58, 58, 0.18)"];

  const lineData = completeCohorts.slice(-8).map((c) => ({ date: formatWeekShort(c.cohortStart), newCustomers: c.newCustomers, converts: c.total3Week }));

  return (
    <DashboardCard matchHeight>
      <CardHeader>
        <CardTitle>Weekly New Customers</CardTitle>
        <CardDescription>Complete weeks and cohort breakdown</CardDescription>
      </CardHeader>

      <CardContent className="space-y-6">
        <ChartContainer config={{
          newCustomers: { label: "New Customers", color: COLORS.newCustomer },
          converts: { label: "Converts", color: "hsl(150, 45%, 42%)" },
        } satisfies ChartConfig} className="h-[200px] w-full">
          <RAreaChart accessibilityLayer data={lineData} margin={{ top: 20, left: 12, right: 12 }}>
            <defs>
              <linearGradient id="fillNewCustomers" x1="0" y1="0" x2="0" y2="1">
                <stop offset="5%" stopColor="var(--color-newCustomers)" stopOpacity={0.8} />
                <stop offset="95%" stopColor="var(--color-newCustomers)" stopOpacity={0.1} />
              </linearGradient>
              <linearGradient id="fillConverts" x1="0" y1="0" x2="0" y2="1">
                <stop offset="5%" stopColor="var(--color-converts)" stopOpacity={0.8} />
                <stop offset="95%" stopColor="var(--color-converts)" stopOpacity={0.1} />
              </linearGradient>
            </defs>
            <XAxis
              dataKey="date"
              tickLine={false}
              axisLine={false}
              tickMargin={8}
            />
            <Area
              dataKey="newCustomers"
              type="natural"
              fill="url(#fillNewCustomers)"
              fillOpacity={0.4}
              stroke="var(--color-newCustomers)"
              strokeWidth={2}
              dot={{ fill: "var(--color-newCustomers)" }}
              activeDot={{ r: 6 }}
            >
              {!isMobile && (
                <LabelList
                  position="top"
                  offset={12}
                  className="fill-foreground"
                  fontSize={12}
                />
              )}
            </Area>
            <Area
              dataKey="converts"
              type="natural"
              fill="url(#fillConverts)"
              fillOpacity={0.4}
              stroke="var(--color-converts)"
              strokeWidth={2}
              dot={{ fill: "var(--color-converts)" }}
              activeDot={{ r: 6 }}
            />
            <ChartLegend content={<ChartLegendContent />} />
          </RAreaChart>
        </ChartContainer>

        <Tabs value={activeTab} onValueChange={setActiveTab}>
          <TabsList variant="line" className="w-full justify-start">
            <TabsTrigger value="complete">Complete ({displayComplete.length})</TabsTrigger>
            {incompleteCohorts.length > 0 && (
              <TabsTrigger value="inProgress">In progress ({incompleteCohorts.length})</TabsTrigger>
            )}
          </TabsList>

      <TabsContent value="complete">
      {/* Complete table */}
      {cohorts && (
        <div>
          <table className="mod-table-responsive w-full border-collapse tabular-nums" style={{ fontFamily: FONT_SANS }}>
            <thead>
              <tr>
                <th className={`${modThClass} !text-left`}>Cohort</th>
                <th className={modThClass} style={{ width: "3.5rem" }}>New</th>
                <th className={modThClass} style={{ width: "4rem" }}>Converts</th>
                <th className={modThClass} style={{ width: "3.5rem" }}>Rate</th>
                <th className={`${modThClass} !text-center`} style={{ width: "5rem" }}>Timing</th>
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
                      className="transition-colors cursor-pointer border-b border-border"
                      style={{
                        borderLeft: isHovered ? `2px solid ${COLORS.newCustomer}` : "2px solid transparent",
                        backgroundColor: isNewest && !isHovered ? "rgba(65, 58, 58, 0.015)" : "transparent",
                      }}
                      onMouseEnter={() => setHoveredCohort(c.cohortStart)}
                      onMouseLeave={() => setHoveredCohort(null)}
                      onClick={() => setExpandedRow(isExpanded ? null : c.cohortStart)}
                    >
                      <td className={`${modTdClass} !text-left font-medium`}>
                        {formatWeekRangeLabel(c.cohortStart, c.cohortEnd)}
                      </td>
                      <td data-label="New" className={`${modTdClass} font-semibold`}>{formatNumber(c.newCustomers)}</td>
                      <td data-label="Converts" className={`${modTdClass} font-semibold`}>{c.total3Week}</td>
                      <td data-label="Rate" className={`${modTdClass} font-medium`}>{rate}%</td>
                      <td className={`mod-bar-cell ${modTdClass} !text-center`}>
                        <SegmentedBar
                          segments={[
                            { value: c.week1, label: "Same week" },
                            { value: c.week2, label: "+1 week" },
                            { value: c.week3, label: "+2 weeks" },
                          ]}
                          colors={timingColors}
                          tooltip={`Week 0: ${c.week1} \u00b7 Week 1: ${c.week2} \u00b7 Week 2: ${c.week3}`}
                        />
                      </td>
                    </tr>
                    {isExpanded && (
                      <tr className="border-b border-border" style={{ borderLeft: `2px solid ${COLORS.newCustomer}` }}>
                        <td colSpan={5} className="px-3 pt-1 pb-1.5 pl-6 bg-muted/30">
                          <div className="flex gap-4 text-xs text-muted-foreground tabular-nums">
                            <span>Same week: <strong className="font-medium text-foreground">{c.week1}</strong></span>
                            <span>+1 week: <strong className="font-medium text-foreground">{c.week2}</strong></span>
                            <span>+2 weeks: <strong className="font-medium text-foreground">{c.week3}</strong></span>
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
      </TabsContent>

      <TabsContent value="inProgress">
      {/* In-progress table */}
      {cohorts && (
        <div>
          <table className="mod-table-responsive w-full border-collapse tabular-nums table-fixed" style={{ fontFamily: FONT_SANS }}>
            <colgroup>
              <col style={{ width: "42%" }} />
              <col style={{ width: "14%" }} />
              <col style={{ width: "22%" }} />
              <col style={{ width: "22%" }} />
            </colgroup>
            <thead>
              <tr>
                <th className={`${modThClass} !text-left`}>Cohort</th>
                <th className={modThClass}>New</th>
                <th className={modThClass}>Converts</th>
                <th className={modThClass}>Days left</th>
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
                      className="transition-colors cursor-pointer border-b border-border"
                      style={{
                        borderLeft: isHovered ? `2px solid ${COLORS.newCustomer}` : "2px solid transparent",
                      }}
                      onMouseEnter={() => setHoveredCohort(c.cohortStart)}
                      onMouseLeave={() => setHoveredCohort(null)}
                      onClick={() => setExpandedRow(isExpanded ? null : c.cohortStart)}
                    >
                      <td className={`${modTdClass} !text-left font-medium truncate`}>
                        {formatWeekRangeLabel(c.cohortStart, c.cohortEnd)}
                      </td>
                      <td data-label="New" className={`${modTdClass} font-semibold`}>{formatNumber(c.newCustomers)}</td>
                      <td data-label="Converts" className={`${modTdClass} font-semibold`}>{convertsSoFar}</td>
                      <td data-label="Days left" className={`${modTdClass} text-muted-foreground`}>{daysRemaining} {daysRemaining === 1 ? "day" : "days"}</td>
                    </tr>
                    {isExpanded && (
                      <tr className="border-b border-border" style={{ borderLeft: `2px solid ${COLORS.newCustomer}` }}>
                        <td colSpan={4} className="px-3 pt-1 pb-1.5 pl-6 bg-muted/30">
                          <div className="flex gap-4 text-xs text-muted-foreground tabular-nums">
                            <span>Same week: <strong className="font-medium text-foreground">{c.week1}</strong></span>
                            <span>+1 week: <strong className="font-medium text-foreground">{wk2Possible ? c.week2 : "\u2014"}</strong></span>
                            <span>+2 weeks: <strong className="font-medium text-foreground">{"\u2014"}</strong></span>
                            <span className="opacity-70">{daysRemaining}d until complete</span>
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
      </TabsContent>
      </Tabs>
      </CardContent>
    </DashboardCard>
  );
}

// ─── Churn Section ──────────────────────────────────────────

// ─── Intro Week Placeholder ─────────────────────────────────

function IntroWeekModule({ introWeek }: { introWeek: IntroWeekData | null }) {
  if (!introWeek) return null;

  const { lastWeek, last4Weeks, last4WeekAvg } = introWeek;
  const lastWeekDelta = lastWeek && last4WeekAvg > 0
    ? Math.round(((lastWeek.customers - last4WeekAvg) / last4WeekAvg) * 100)
    : null;

  return (
    <DashboardCard matchHeight>
      <CardHeader>
        <CardTitle>Intro Week</CardTitle>
        <CardDescription>New intro week customers by week</CardDescription>
      </CardHeader>

      <CardContent>
        <MetricRow
          slots={[
            { value: lastWeek ? formatNumber(lastWeek.customers) : "\u2014", label: "This Week" },
            { value: formatNumber(last4WeekAvg), label: "4-wk Avg" },
            { value: lastWeekDelta !== null ? `${lastWeekDelta > 0 ? "+" : ""}${lastWeekDelta}` : "\u2014", valueSuffix: lastWeekDelta !== null ? "%" : "", label: "vs Avg" },
          ]}
        />
      </CardContent>

      {lastWeekDelta !== null && (
        <CardFooter className="flex-col items-start gap-2 text-sm">
          <div className="flex gap-2 leading-none font-medium">
            {lastWeekDelta > 0 ? "+" : ""}{lastWeekDelta}% vs 4-week average
          </div>
          <div className="text-muted-foreground leading-none">
            Intro week signups
          </div>
        </CardFooter>
      )}
    </DashboardCard>
  );
}

// ─── Non-auto-renew Section (Pass types + Conversion) ───────

function NonAutoRenewSection({ dropIns, introWeek, newCustomerVolume, newCustomerCohorts, conversionPool }: {
  dropIns: DropInModuleData | null;
  introWeek: IntroWeekData | null;
  newCustomerVolume: NewCustomerVolumeData | null;
  newCustomerCohorts: NewCustomerCohortData | null;
  conversionPool: ConversionPoolModuleData | null;
}) {
  if (!dropIns && !introWeek && !newCustomerVolume && !newCustomerCohorts && !conversionPool) return null;

  return (
    <div className="flex flex-col gap-4">
      <SectionHeader subtitle="Drop-ins, Intro Week, and other non-subscribed activity">{LABELS.nonAutoRenew}</SectionHeader>

      {/* ── Subsection A: Drop-ins ── */}
      {dropIns && (
        <div className="flex flex-col gap-3">
          <SubsectionHeader icon={Ticket} color={COLORS.dropIn}>Drop-ins</SubsectionHeader>
          <DropInsSubsection dropIns={dropIns} />
        </div>
      )}

      {/* ── Subsection B: Other pass types ── */}
      <div className="flex flex-col gap-3">
        <SubsectionHeader icon={Tag} color="hsl(200, 45%, 50%)">Other Pass Types</SubsectionHeader>
        <div className="grid grid-cols-1 sm:grid-cols-2 gap-3" style={{ alignItems: "stretch" }}>
          <IntroWeekModule introWeek={introWeek} />
        </div>
      </div>

      {/* ── Subsection C: Conversion ── */}
      <div className="flex flex-col gap-3">
        <SubsectionHeader icon={ArrowRightLeft} color="hsl(150, 45%, 42%)">Conversion</SubsectionHeader>
        <div className="grid grid-cols-1 sm:grid-cols-2 gap-3" style={{ alignItems: "stretch" }}>
          {(newCustomerVolume || newCustomerCohorts) && (
            <NewCustomerOverviewCard volume={newCustomerVolume} cohorts={newCustomerCohorts} />
          )}
          {(newCustomerVolume || newCustomerCohorts) && (
            <NewCustomerChartCard volume={newCustomerVolume} cohorts={newCustomerCohorts} />
          )}
        </div>
        {conversionPool && (
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-3" style={{ alignItems: "stretch" }}>
            <ConversionPoolModule pool={conversionPool} />
          </div>
        )}
      </div>
    </div>
  );
}

// ─── Drop-Ins Subsection (3-card layout: Overview + Frequency | Distribution) ──

function DropInsSubsection({ dropIns }: { dropIns: DropInModuleData }) {
  const isMobile = useIsMobile();
  const [activeTab, setActiveTab] = useState<string>("complete");
  const [hoveredWeek, setHoveredWeek] = useState<string | null>(null);

  const { completeWeeks, wtd, lastCompleteWeek, typicalWeekVisits, trend, trendDeltaPercent, wtdDelta, wtdDeltaPercent, wtdDayLabel, frequency } = dropIns;
  const displayWeeks = completeWeeks.slice(-8);

  // Mix bar colors: First (dark) vs Repeat (light)
  const mixColors = ["rgba(155, 118, 83, 0.70)", "rgba(155, 118, 83, 0.28)"];

  // Frequency pie chart data + config
  const freqData = frequency ? [
    { bucket: "1 visit", count: frequency.bucket1, fill: "var(--color-bucket1)" },
    { bucket: "2\u20134", count: frequency.bucket2to4, fill: "var(--color-bucket2to4)" },
    { bucket: "5\u201310", count: frequency.bucket5to10, fill: "var(--color-bucket5to10)" },
    { bucket: "11+", count: frequency.bucket11plus, fill: "var(--color-bucket11plus)" },
  ].filter(d => d.count > 0) : [];

  const freqConfig = {
    count: { label: "Visitors" },
    bucket1: { label: "1 visit", color: "rgba(155, 118, 83, 0.30)" },
    bucket2to4: { label: "2\u20134", color: "rgba(155, 118, 83, 0.50)" },
    bucket5to10: { label: "5\u201310", color: "rgba(155, 118, 83, 0.70)" },
    bucket11plus: { label: "11+", color: "rgba(155, 118, 83, 0.90)" },
  } satisfies ChartConfig;

  return (
    <div className="flex flex-col gap-3">
      {/* ── Row 1: Three KPI cards ── */}
      <div className="grid grid-cols-1 sm:grid-cols-3 gap-3">
        <DashboardCard>
          <CardHeader>
            <CardDescription>Visits (WTD)</CardDescription>
            <CardTitle className="text-2xl font-semibold tabular-nums">{formatNumber(wtd?.visits ?? 0)}</CardTitle>
          </CardHeader>
          <CardFooter className="text-sm text-muted-foreground">
            {wtdDayLabel ? `Through ${wtdDayLabel}` : "Week to date"}
          </CardFooter>
        </DashboardCard>

        <DashboardCard>
          <CardHeader>
            <CardDescription>Typical Week</CardDescription>
            <CardTitle className="text-2xl font-semibold tabular-nums">{formatNumber(typicalWeekVisits)}</CardTitle>
          </CardHeader>
          <CardFooter className="text-sm text-muted-foreground">
            4-week average
          </CardFooter>
        </DashboardCard>

        <DashboardCard>
          <CardHeader>
            <CardDescription>Trend</CardDescription>
            <CardTitle className="text-2xl font-semibold tabular-nums">
              {trendDeltaPercent > 0 ? "+" : ""}{trendDeltaPercent.toFixed(1)}%
            </CardTitle>
          </CardHeader>
          <CardFooter className="text-sm text-muted-foreground">
            vs prior 4 weeks
          </CardFooter>
        </DashboardCard>
      </div>

      {/* ── Row 2: Weekly Drop-ins — chart + tabbed table ── */}
      <Tabs value={activeTab} onValueChange={setActiveTab}>
        <DashboardCard className="@container/card">
          <CardHeader>
            <CardTitle>Weekly Drop-ins</CardTitle>
            <CardDescription>
              <span className="hidden @[540px]/card:block">Complete weeks and week-to-date breakdown</span>
              <span className="@[540px]/card:hidden">Weekly data</span>
            </CardDescription>
            <CardAction>
              <ToggleGroup
                variant="outline"
                type="single"
                value={activeTab}
                onValueChange={(v) => { if (v) setActiveTab(v); }}
                className="hidden @[540px]/card:flex"
              >
                <ToggleGroupItem value="complete">Complete weeks ({displayWeeks.length})</ToggleGroupItem>
                {wtd && <ToggleGroupItem value="wtd">This week (WTD)</ToggleGroupItem>}
              </ToggleGroup>
              <Select value={activeTab} onValueChange={setActiveTab}>
                <SelectTrigger className="w-44 @[540px]/card:hidden" size="sm">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent className="rounded-xl">
                  <SelectItem value="complete" className="rounded-lg">Complete weeks ({displayWeeks.length})</SelectItem>
                  {wtd && <SelectItem value="wtd" className="rounded-lg">This week (WTD)</SelectItem>}
                </SelectContent>
              </Select>
            </CardAction>
          </CardHeader>

          <CardContent className="space-y-6">
            {/* Bar chart — responds to active toggle */}
            <ChartContainer config={{ visits: { label: "Visits", color: COLORS.dropIn } } satisfies ChartConfig} className="h-[200px] w-full">
              <BarChart
                accessibilityLayer
                data={
                  activeTab === "wtd" && wtd
                    ? [
                        ...(lastCompleteWeek
                          ? [{ date: formatWeekShort(lastCompleteWeek.weekStart), visits: lastCompleteWeek.visits }]
                          : []),
                        { date: `${formatWeekShort(wtd.weekStart)} (WTD)`, visits: wtd.visits },
                      ]
                    : displayWeeks.map((w) => ({ date: formatWeekShort(w.weekStart), visits: w.visits }))
                }
                margin={{ top: 20 }}
              >
                <XAxis dataKey="date" tickLine={false} tickMargin={10} axisLine={false} />
                <ChartTooltip cursor={false} content={<ChartTooltipContent hideLabel formatter={(v) => formatNumber(v as number)} />} />
                <Bar dataKey="visits" fill="var(--color-visits)" radius={8}>
                  {!isMobile && (
                    <LabelList
                      position="top"
                      offset={12}
                      className="fill-foreground"
                      fontSize={12}
                    />
                  )}
                </Bar>
              </BarChart>
            </ChartContainer>

            {/* Tabbed tables — below the chart */}
            <TabsContent value="complete" className="mt-0">
              <div className="overflow-x-auto min-w-0">
                <table className="mod-table-responsive w-full border-collapse tabular-nums table-fixed" style={{ fontFamily: FONT_SANS }}>
                  <colgroup>
                    <col style={{ width: "32%" }} />
                    <col style={{ width: "16%" }} />
                    <col style={{ width: "18%" }} />
                    <col style={{ width: "14%" }} />
                    <col style={{ width: "20%" }} />
                  </colgroup>
                  <thead>
                    <tr>
                      <th className={`${modThClass} !text-left`}>Week</th>
                      <th className={modThClass}>Visits</th>
                      <th className={modThClass}>Customers</th>
                      <th className={modThClass}>First %</th>
                      <th className={`${modThClass} !text-center`}>Mix</th>
                    </tr>
                  </thead>
                  <tbody>
                    {displayWeeks.map((w) => {
                      const isHovered = hoveredWeek === w.weekStart;
                      const isLatest = w.weekStart === displayWeeks[displayWeeks.length - 1]?.weekStart;
                      const firstPct = w.uniqueCustomers > 0 ? Math.round((w.firstTime / w.uniqueCustomers) * 100) : 0;
                      return (
                        <tr
                          key={w.weekStart}
                          className="transition-colors border-b border-border"
                          style={{
                            borderLeft: isHovered ? `2px solid ${COLORS.dropIn}` : "2px solid transparent",
                            backgroundColor: isHovered ? "rgba(65, 58, 58, 0.02)" : "transparent",
                          }}
                          onMouseEnter={() => setHoveredWeek(w.weekStart)}
                          onMouseLeave={() => setHoveredWeek(null)}
                        >
                          <td className={`${modTdClass} !text-left font-medium`}>
                            {formatWeekRangeLabel(w.weekStart, w.weekEnd)}
                            {isLatest && <> <span className="text-[10px] bg-muted text-muted-foreground rounded-full px-2 py-0.5 ml-1">Latest</span></>}
                          </td>
                          <td data-label="Visits" className={`${modTdClass} font-semibold`}>{formatNumber(w.visits)}</td>
                          <td data-label="Customers" className={`${modTdClass} font-medium`}>{formatNumber(w.uniqueCustomers)}</td>
                          <td data-label="First %" className={`${modTdClass} text-muted-foreground`}>{firstPct}%</td>
                          <td className={`mod-bar-cell ${modTdClass} !text-center`}>
                            {w.uniqueCustomers > 0 ? (
                              <SegmentedBar
                                segments={[
                                  { value: w.firstTime, label: `First: ${w.firstTime}` },
                                  { value: w.repeatCustomers, label: `Repeat: ${w.repeatCustomers}` },
                                ]}
                                colors={mixColors}
                                tooltip={`First: ${w.firstTime} (${firstPct}%) \u00b7 Repeat: ${w.repeatCustomers} (${100 - firstPct}%)`}
                              />
                            ) : (
                              <span className="text-muted-foreground opacity-40">{"\u2014"}</span>
                            )}
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            </TabsContent>

            <TabsContent value="wtd" className="mt-0">
              {wtd && (
                <div className="overflow-x-auto min-w-0">
                  <table className="mod-table-responsive w-full border-collapse tabular-nums table-fixed" style={{ fontFamily: FONT_SANS }}>
                    <colgroup>
                      <col style={{ width: "30%" }} />
                      <col style={{ width: "16%" }} />
                      <col style={{ width: "18%" }} />
                      <col style={{ width: "14%" }} />
                      <col style={{ width: "22%" }} />
                    </colgroup>
                    <thead>
                      <tr>
                        <th className={`${modThClass} !text-left`}>Week</th>
                        <th className={modThClass}>Visits</th>
                        <th className={modThClass}>Customers</th>
                        <th className={modThClass}>First %</th>
                        <th className={modThClass}>Days left</th>
                      </tr>
                    </thead>
                    <tbody>
                      <tr className="border-b border-border">
                        <td className={`${modTdClass} !text-left font-medium`}>
                          {formatWeekRangeLabel(wtd.weekStart, wtd.weekEnd)}
                          {" "}<DChip variant="accent">WTD</DChip>
                        </td>
                        <td data-label="Visits" className={`${modTdClass} font-semibold`}>{formatNumber(wtd.visits)}</td>
                        <td data-label="Customers" className={`${modTdClass} font-medium`}>{formatNumber(wtd.uniqueCustomers)}</td>
                        <td data-label="First %" className={`${modTdClass} text-muted-foreground`}>
                          {wtd.uniqueCustomers > 0 ? `${Math.round((wtd.firstTime / wtd.uniqueCustomers) * 100)}%` : "\u2014"}
                        </td>
                        <td data-label="Days left" className={`${modTdClass} text-muted-foreground`}>{wtd.daysLeft} {wtd.daysLeft === 1 ? "day" : "days"}</td>
                      </tr>
                      {lastCompleteWeek && (
                        <tr className="border-b border-border opacity-55">
                          <td className={`${modTdClass} !text-left font-medium text-muted-foreground`}>
                            {formatWeekRangeLabel(lastCompleteWeek.weekStart, lastCompleteWeek.weekEnd)}
                            <span className="text-[11px] ml-1.5 font-normal italic">prev</span>
                          </td>
                          <td data-label="Visits" className={`${modTdClass} font-semibold text-muted-foreground`}>{formatNumber(lastCompleteWeek.visits)}</td>
                          <td data-label="Customers" className={`${modTdClass} font-medium text-muted-foreground`}>{formatNumber(lastCompleteWeek.uniqueCustomers)}</td>
                          <td data-label="First %" className={`${modTdClass} text-muted-foreground`}>
                            {lastCompleteWeek.uniqueCustomers > 0 ? `${Math.round((lastCompleteWeek.firstTime / lastCompleteWeek.uniqueCustomers) * 100)}%` : "\u2014"}
                          </td>
                          <td data-label="Days left" className={`${modTdClass} text-muted-foreground`}>{"\u2014"}</td>
                        </tr>
                      )}
                    </tbody>
                  </table>
                </div>
              )}
            </TabsContent>
          </CardContent>
        </DashboardCard>
      </Tabs>

      {/* ── Row 3: Drop-in Frequency (pie chart) ── */}
      <div className="grid grid-cols-1 sm:grid-cols-2 gap-3" style={{ alignItems: "stretch" }}>
        <DashboardCard matchHeight>
          <CardHeader>
            <div className="flex items-center gap-1.5">
              <CardTitle>Drop-in Frequency</CardTitle>
              <InfoTooltip tooltip="Distribution of unique drop-in customers by visit count over the last 90 days." />
            </div>
            <CardDescription>Last 90 days</CardDescription>
          </CardHeader>

          <CardContent className="flex-1 pb-0">
            {frequency && frequency.totalCustomers > 0 && freqData.length > 0 ? (
              <ChartContainer
                config={freqConfig}
                className="mx-auto h-[250px] w-full"
              >
                <PieChart>
                  <ChartTooltip
                    content={<ChartTooltipContent nameKey="count" hideLabel />}
                  />
                  <Pie
                    data={freqData}
                    dataKey="count"
                    labelLine={false}
                    label={({ payload, ...props }) => (
                      <text
                        x={props.x}
                        y={props.y}
                        textAnchor={props.textAnchor}
                        dominantBaseline={props.dominantBaseline}
                        fill="hsla(var(--foreground))"
                      >
                        <tspan fontWeight={700} fontSize={13}>{formatNumber(payload.count)}</tspan>
                        <tspan fontSize={11} opacity={0.6}>{` ${payload.bucket}`}</tspan>
                      </text>
                    )}
                    nameKey="bucket"
                  />
                </PieChart>
              </ChartContainer>
            ) : (
              <div className="flex items-center justify-center h-[200px]">
                <span className="text-sm text-muted-foreground">No data available</span>
              </div>
            )}
          </CardContent>

          {frequency && frequency.totalCustomers > 0 && (
            <CardFooter className="flex-col gap-2 text-sm">
              <div className="leading-none font-medium">
                {formatNumber(frequency.totalCustomers)} unique visitors
              </div>
              <div className="text-muted-foreground leading-none">
                Drop-in customers in the last 90 days
              </div>
            </CardFooter>
          )}
        </DashboardCard>
      </div>
    </div>
  );
}

// ─── Conversion Pool Module (non-auto → auto-renew) ─────────

function ConversionPoolModule({ pool }: { pool: ConversionPoolModuleData }) {
  const isMobile = useIsMobile();
  const [hoveredWeek, setHoveredWeek] = useState<string | null>(null);
  const [activeSlice, setActiveSlice] = useState<ConversionPoolSlice>("all");
  const [activeTab, setActiveTab] = useState<string>("complete");
  const [detailsOpen, setDetailsOpen] = useState(false);

  // Resolve active slice data (fall back to "all")
  const data: ConversionPoolSliceData | null = pool.slices[activeSlice] ?? pool.slices.all ?? null;
  if (!data) return null;

  const { completeWeeks, wtd, lagStats, lastCompleteWeek, avgPool7d, avgRate } = data;

  // Display last 8 complete weeks in table
  const displayWeeks = completeWeeks.slice(-8);

  // Second-to-last complete week for delta comparison
  const prevCompleteWeek = completeWeeks.length >= 2 ? completeWeeks[completeWeeks.length - 2] : null;

  // Deltas vs last complete week
  const heroPool = wtd?.activePool7d ?? lastCompleteWeek?.activePool7d ?? 0;
  const heroConverts = wtd?.converts ?? lastCompleteWeek?.converts ?? 0;
  const heroRate = wtd ? wtd.conversionRate : (lastCompleteWeek?.conversionRate ?? 0);

  const poolDelta = lastCompleteWeek && prevCompleteWeek ? lastCompleteWeek.activePool7d - prevCompleteWeek.activePool7d : null;
  const convertsDelta = lastCompleteWeek && prevCompleteWeek ? lastCompleteWeek.converts - prevCompleteWeek.converts : null;
  const rateDelta = lastCompleteWeek && prevCompleteWeek ? Math.round((lastCompleteWeek.conversionRate - prevCompleteWeek.conversionRate) * 10) / 10 : null;

  // Rate vs 8-week avg baseline
  const rateVsBaseline = avgRate > 0 ? Math.round((heroRate - avgRate) * 10) / 10 : null;
  const showRateWarning = rateVsBaseline !== null && rateVsBaseline < -(avgRate * 0.2);

  // Hot pool: high-intent slice's pool count (≥2 visits in 30d, no auto-renew)
  const highIntentSlice = pool.slices["high-intent"];
  const hotPoolCount = highIntentSlice?.wtd?.activePool7d ?? highIntentSlice?.lastCompleteWeek?.activePool7d ?? null;

  // Slice options
  const sliceOptions: { key: ConversionPoolSlice; label: string }[] = [
    { key: "all", label: "All" },
    { key: "drop-ins", label: "Drop-ins" },
    { key: "intro-week", label: "Intro Week" },
    { key: "class-packs", label: "Class Packs" },
    { key: "high-intent", label: "High intent" },
  ];

  // Distribution colors — high-contrast spec
  const distColors = [
    "rgba(107, 91, 123, 0.85)",
    "rgba(107, 91, 123, 0.55)",
    "rgba(107, 91, 123, 0.32)",
    "rgba(107, 91, 123, 0.16)",
  ];

  return (
    <DashboardCard matchHeight>
      {/* ── Header: title + slice filter ── */}
      <DModuleHeader
        color={COLORS.conversionPool}
        title={LABELS.conversionPool}
        summaryPill={`${displayWeeks.length} weeks`}
        detailsOpen={detailsOpen}
        onToggleDetails={() => setDetailsOpen(!detailsOpen)}
      >
        <Select value={activeSlice} onValueChange={(v) => setActiveSlice(v as ConversionPoolSlice)}>
          <SelectTrigger size="sm" className="h-7 text-xs font-medium border-border bg-muted text-muted-foreground shadow-none">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            {sliceOptions.map((o) => (
              <SelectItem key={o.key} value={o.key} disabled={!pool.slices[o.key]}>{o.label}</SelectItem>
            ))}
          </SelectContent>
        </Select>
      </DModuleHeader>

      <CardContent className="space-y-6">
        <MetricRow
          slots={[
            { value: formatNumber(heroPool), label: "Total Customers" },
            { value: heroRate.toFixed(1), valueSuffix: "%", label: "3-Week Conversion Rate", labelExtra: <InfoTooltip tooltip="Converts / Active pool (7d). 3-week window." /> },
            { value: formatNumber(heroConverts), label: "Converts", labelExtra: <InfoTooltip tooltip="Pool members who started their first in-studio auto-renew this week." /> },
          ]}
        />

        <ChartContainer config={{ converts: { label: "Converts", color: COLORS.conversionPool } } satisfies ChartConfig} className="h-[200px] w-full">
          <BarChart accessibilityLayer data={displayWeeks.map((w) => ({ date: formatWeekShort(w.weekStart), converts: w.converts }))} margin={{ top: 20 }}>

            <XAxis
              dataKey="date"
              tickLine={false}
              tickMargin={10}
              axisLine={false}
            />
            <ChartTooltip
              cursor={false}
              content={<ChartTooltipContent hideLabel formatter={(v) => formatNumber(v as number)} />}
            />
            <Bar dataKey="converts" fill="var(--color-converts)" radius={8}>
              {!isMobile && (
                <LabelList
                  position="top"
                  offset={12}
                  className="fill-foreground"
                  fontSize={12}
                />
              )}
            </Bar>
          </BarChart>
        </ChartContainer>
      </CardContent>

      <CardFooter className="flex-col items-start gap-2 text-sm">
        <div className="flex gap-2 leading-none font-medium">
          {heroRate.toFixed(1)}% conversion rate
        </div>
        <div className="text-muted-foreground leading-none">
          Weekly pool conversions to auto-renew
        </div>
      </CardFooter>

      <CardFooter className="flex-col items-stretch p-0">
        <DCardDisclosure open={detailsOpen}>

      {/* ── Secondary: Conversion quality (mini-KPI 2-up strip) ── */}
      {lagStats && (
        <div className="grid grid-cols-2 gap-6 items-start border-t border-neutral-900/6 py-3 mb-2 tabular-nums">
          <div title="Median days between first non-auto studio visit and auto-renew start.">
            <div className="text-[11px] uppercase tracking-wide text-neutral-500 mb-0.5">Median time</div>
            <div className="text-[13px] font-medium text-neutral-800">
              {lagStats.medianTimeToConvert != null ? `${lagStats.medianTimeToConvert}d` : "\u2014"}
              {lagStats.historicalMedianTimeToConvert != null && (
                <span className="text-[12px] text-neutral-500 ml-1.5">{`12wk: ${lagStats.historicalMedianTimeToConvert}d`}</span>
              )}
            </div>
          </div>
          <div title="Average distinct non-subscriber visits before conversion.">
            <div className="text-[11px] uppercase tracking-wide text-neutral-500 mb-0.5">Avg visits</div>
            <div className="text-[13px] font-medium text-neutral-800">
              {lagStats.avgVisitsBeforeConvert != null ? lagStats.avgVisitsBeforeConvert.toFixed(1) : "\u2014"}
              {lagStats.historicalAvgVisitsBeforeConvert != null && (
                <span className="text-[12px] text-neutral-500 ml-1.5">{`12wk: ${lagStats.historicalAvgVisitsBeforeConvert.toFixed(1)}`}</span>
              )}
            </div>
          </div>
        </div>
      )}

      {/* ── Distribution bars (Time to convert / Visits before convert) ── */}
      {lagStats && (
        <div className="grid grid-cols-2 gap-8 w-full pt-2 pb-3 mb-3 border-t border-neutral-900/6">
          {/* Time distribution bar */}
          <div>
            <div className="text-[12px] font-medium text-neutral-600 mb-1.5">Time to convert</div>
            <SegmentedBar
              segments={[
                { value: lagStats.timeBucket0to30, label: "0-30d" },
                { value: lagStats.timeBucket31to90, label: "31-90d" },
                { value: lagStats.timeBucket91to180, label: "91-180d" },
                { value: lagStats.timeBucket180plus, label: "180d+" },
              ]}
              height={16}
              colors={distColors}
            />
            <div className="flex gap-2 mt-2 flex-wrap">
              {[
                { label: "0-30d", val: lagStats.timeBucket0to30 },
                { label: "31-90d", val: lagStats.timeBucket31to90 },
                { label: "91-180d", val: lagStats.timeBucket91to180 },
                { label: "180d+", val: lagStats.timeBucket180plus },
              ].map((b, i) => b.val > 0 ? (
                <div key={i} className="text-[11px] text-neutral-600 flex items-center gap-1">
                  <span className="w-1.5 h-1.5 rounded-sm inline-block" style={{ backgroundColor: distColors[i] }} />
                  {b.label}
                </div>
              ) : null)}
            </div>
          </div>

          {/* Visit distribution bar */}
          <div>
            <div className="text-[12px] font-medium text-neutral-600 mb-1.5">Visits before convert</div>
            <SegmentedBar
              segments={[
                { value: lagStats.visitBucket1to2, label: "1-2" },
                { value: lagStats.visitBucket3to5, label: "3-5" },
                { value: lagStats.visitBucket6to10, label: "6-10" },
                { value: lagStats.visitBucket11plus, label: "11+" },
              ]}
              height={16}
              colors={distColors}
            />
            <div className="flex gap-2 mt-2 flex-wrap">
              {[
                { label: "1-2", val: lagStats.visitBucket1to2 },
                { label: "3-5", val: lagStats.visitBucket3to5 },
                { label: "6-10", val: lagStats.visitBucket6to10 },
                { label: "11+", val: lagStats.visitBucket11plus },
              ].map((b, i) => b.val > 0 ? (
                <div key={i} className="text-[11px] text-neutral-600 flex items-center gap-1">
                  <span className="w-1.5 h-1.5 rounded-sm inline-block" style={{ backgroundColor: distColors[i] }} />
                  {b.label}
                </div>
              ) : null)}
            </div>
          </div>
        </div>
      )}

      {/* ── Tabs: Complete | WTD ── */}
      <Tabs value={activeTab} onValueChange={setActiveTab} className="mt-6">
        <TabsList variant="line" className="w-full justify-start">
          <TabsTrigger value="complete">Complete weeks</TabsTrigger>
          {wtd && <TabsTrigger value="wtd">Week to date</TabsTrigger>}
        </TabsList>

      <TabsContent value="complete">
      {/* ── Weekly Grid-Table (Complete weeks tab) ── */}
      {displayWeeks.length > 0 && (
        <div className="w-full min-w-0 mt-4">
          {/* Header */}
          <div className="grid grid-cols-[minmax(160px,1.2fr)_140px_140px_120px] w-full border-b border-neutral-900/10">
            <div className="py-2 text-left text-xs font-medium uppercase tracking-[0.04em] text-muted-foreground leading-[16px]">Week</div>
            <div className="py-2 text-right text-xs font-medium uppercase tracking-[0.04em] text-muted-foreground leading-[16px] tabular-nums">Pool</div>
            <div className="py-2 text-right text-xs font-medium uppercase tracking-[0.04em] text-muted-foreground leading-[16px] tabular-nums">Converts</div>
            <div className="py-2 text-right text-xs font-medium uppercase tracking-[0.04em] text-muted-foreground leading-[16px] tabular-nums">Rate</div>
          </div>
          {/* Rows */}
          {displayWeeks.map((w, idx) => {
            const isHovered = hoveredWeek === w.weekStart;
            const isLatest = idx === displayWeeks.length - 1;
            const weekLabel = new Date(w.weekStart + "T12:00:00").toLocaleDateString("en-US", { month: "short", day: "numeric" });
            return (
              <div
                key={w.weekStart}
                className="grid grid-cols-[minmax(160px,1.2fr)_140px_140px_120px] w-full border-b border-neutral-900/6 transition-colors duration-100"
                onMouseEnter={() => setHoveredWeek(w.weekStart)}
                onMouseLeave={() => setHoveredWeek(null)}
                style={{
                  borderLeft: isHovered ? `2px solid ${COLORS.conversionPool}` : "2px solid transparent",
                  background: isHovered ? "rgba(107, 91, 123, 0.03)" : "transparent",
                }}
              >
                <div className="py-3 text-left text-[14px] leading-[20px] font-medium text-muted-foreground" style={{ fontFamily: FONT_SANS }}>
                  {weekLabel}
                  {isLatest && <> <span className="text-[10px] bg-zinc-100 text-zinc-500 rounded-full px-2 py-0.5 ml-1">Latest</span></>}
                </div>
                <div className="py-3 text-right text-[14px] leading-[20px] font-semibold tabular-nums" style={{ fontFamily: FONT_SANS }}>
                  {formatNumber(w.activePool7d)}
                </div>
                <div className="py-3 text-right text-[14px] leading-[20px] font-semibold tabular-nums" style={{ fontFamily: FONT_SANS }}>
                  {formatNumber(w.converts)}
                </div>
                <div className="py-3 text-right text-[14px] leading-[20px] font-semibold tabular-nums" style={{ fontFamily: FONT_SANS }}>
                  {w.conversionRate.toFixed(1)}%
                </div>
              </div>
            );
          })}
        </div>
      )}

      </TabsContent>

      <TabsContent value="wtd">
      {/* ── WTD Tab (CSS grid) ── */}
      {wtd && (
        <div className="w-full min-w-0 mt-4">
          {/* Header */}
          <div className="grid grid-cols-[minmax(160px,1.2fr)_140px_140px_120px] w-full border-b border-neutral-900/10">
            <div className="py-2 text-left text-xs font-medium uppercase tracking-[0.04em] text-muted-foreground leading-[16px]">Period</div>
            <div className="py-2 text-right text-xs font-medium uppercase tracking-[0.04em] text-muted-foreground leading-[16px] tabular-nums">Pool</div>
            <div className="py-2 text-right text-xs font-medium uppercase tracking-[0.04em] text-muted-foreground leading-[16px] tabular-nums">Converts</div>
            <div className="py-2 text-right text-xs font-medium uppercase tracking-[0.04em] text-muted-foreground leading-[16px] tabular-nums">Rate</div>
          </div>
          {/* Row */}
          <div
            className="grid grid-cols-[minmax(160px,1.2fr)_140px_140px_120px] w-full border-b border-neutral-900/6"
            style={{
              borderLeft: `2px solid ${COLORS.conversionPool}`,
              background: "rgba(107, 91, 123, 0.03)",
            }}
          >
            <div className="py-3 text-left text-[14px] leading-[20px] font-medium text-muted-foreground" style={{ fontFamily: FONT_SANS }}>
              <DChip variant="accent">WTD</DChip>{" "}
              {new Date(wtd.weekStart + "T12:00:00").toLocaleDateString("en-US", { month: "short", day: "numeric" })}
              {wtd.daysLeft > 0 && (
                <span className="text-[11px] text-muted-foreground ml-1.5">
                  ({wtd.daysLeft}d left)
                </span>
              )}
            </div>
            <div className="py-3 text-right text-[14px] leading-[20px] font-semibold italic tabular-nums" style={{ fontFamily: FONT_SANS }}>
              {formatNumber(wtd.activePool7d)}
            </div>
            <div className="py-3 text-right text-[14px] leading-[20px] font-semibold italic tabular-nums" style={{ fontFamily: FONT_SANS }}>
              {formatNumber(wtd.converts)}
            </div>
            <div className="py-3 text-right text-[14px] leading-[20px] font-semibold italic tabular-nums" style={{ fontFamily: FONT_SANS }}>
              {wtd.conversionRate.toFixed(1)}%
            </div>
          </div>
        </div>
      )}
      </TabsContent>
      </Tabs>

      </DCardDisclosure>
      </CardFooter>
    </DashboardCard>
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
      <div className="flex items-center justify-between mb-2">
        <div className="flex items-center gap-2">
          <span className="w-2.5 h-2.5 rounded-full shrink-0" style={{ backgroundColor: color, opacity: 0.85 }} />
          <span className="text-sm leading-none font-medium text-muted-foreground uppercase tracking-wide">
            {title}
          </span>
        </div>
        <span className="text-3xl font-semibold tracking-tight tabular-nums">
          {formatNumber(count)}
        </span>
      </div>

      {/* Metric rows — compact density */}
      <div className="flex flex-col">
        {metrics.map((m, i) => (
          <div key={i} className={`flex justify-between items-center py-1.5 ${i < metrics.length - 1 ? "border-b border-border" : ""}`}>
            <span className="text-xs text-muted-foreground">
              {m.label}
            </span>
            <div className="flex items-center gap-1.5">
              <span className="text-sm font-semibold tabular-nums" style={m.color ? { color: m.color } : undefined}>
                {m.value}
              </span>
              {m.delta != null && (
                <DeltaBadge delta={m.delta} deltaPercent={m.deltaPercent ?? null} isPositiveGood={m.isPositiveGood} compact />
              )}
            </div>
          </div>
        ))}
      </div>

      {/* Pacing — single condensed muted line */}
      {isPacing && pacingNew && (
        <p className="mt-2 text-[11px] text-muted-foreground tabular-nums">
          Pacing {pacing!.daysElapsed}/{pacing!.daysInMonth}d
          {" \u00b7 "}
          {pacingNew(pacing!).actual} new / {pacingNew(pacing!).paced} proj
          {pacingChurn && (<>{" \u00b7 "}{pacingChurn(pacing!).actual} churn / {pacingChurn(pacing!).paced} proj</>)}
        </p>
      )}

      {/* MEMBER annual/monthly breakdown */}
      {churnData?.category === "MEMBER" && (() => {
        const lastCompleted = churnData.monthly.length >= 2 ? churnData.monthly[churnData.monthly.length - 2] : null;
        if (!lastCompleted || !lastCompleted.annualActiveAtStart) return null;
        return (
          <p className="text-xs text-muted-foreground italic mt-2">
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
  const isMobile = useIsMobile();
  const currentYear = new Date().getFullYear();
  const priorYear = currentYear - 1;
  const twoYearsAgo = currentYear - 2;

  const priorData = monthlyRevenue.filter((m) => m.month.startsWith(String(priorYear)));
  const olderData = monthlyRevenue.filter((m) => m.month.startsWith(String(twoYearsAgo)));

  if (olderData.length === 0 && priorData.length === 0) return null;

  const olderTotal = olderData.reduce((s, m) => s + m.net, 0);
  const priorTotal = priorData.reduce((s, m) => s + m.net, 0);
  const yoyDelta = olderTotal > 0 ? ((priorTotal - olderTotal) / olderTotal * 100) : 0;

  const barData = [
    ...(olderTotal > 0 ? [{ year: String(twoYearsAgo), net: olderTotal }] : []),
    ...(priorTotal > 0 ? [{ year: String(priorYear), net: priorTotal }] : []),
  ];

  const annualChartConfig = {
    net: { label: "Net Revenue", color: COLORS.member },
  } satisfies ChartConfig;

  return (
    <DashboardCard>
      <CardHeader>
        <CardTitle>Annual Net Revenue</CardTitle>
        {olderTotal > 0 && priorTotal > 0 && (
          <CardDescription>
            {yoyDelta > 0 ? "+" : ""}{Math.round(yoyDelta * 10) / 10}% year over year
          </CardDescription>
        )}
      </CardHeader>
      <CardContent>
        <ChartContainer config={annualChartConfig} className="h-[200px] w-full">
          <BarChart accessibilityLayer data={barData} margin={{ top: 20 }}>

            <XAxis
              dataKey="year"
              tickLine={false}
              tickMargin={10}
              axisLine={false}
            />
            <ChartTooltip
              cursor={false}
              content={<ChartTooltipContent hideLabel formatter={(v) => formatCurrency(v as number)} />}
            />
            <Bar dataKey="net" fill="var(--color-net)" radius={8}>
              {!isMobile && (
                <LabelList
                  position="top"
                  offset={12}
                  className="fill-foreground"
                  fontSize={12}
                  formatter={(v: number) => formatCompactCurrency(v)}
                />
              )}
            </Bar>
          </BarChart>
        </ChartContainer>
      </CardContent>
      {projection && projection.currentMRR > 0 && Math.abs(projection.monthlyGrowthRate) < 50 && (
        <CardFooter className="flex-col items-start gap-2 text-sm">
          <div className="flex gap-2 leading-none font-medium">
            {projection.year} Subscription MRR: {formatCurrency(projection.currentMRR)}
          </div>
          <div className="text-muted-foreground leading-none">
            {projection.monthlyGrowthRate > 0 ? "+" : ""}{projection.monthlyGrowthRate}% monthly growth
          </div>
        </CardFooter>
      )}
    </DashboardCard>
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
      <div className="min-h-screen p-4 sm:p-6 lg:p-8" style={{ backgroundColor: "var(--st-bg-section)", fontFamily: FONT_SANS }}>
        <div style={{ textAlign: "center", paddingTop: "1rem", marginBottom: "2rem" }}>
          <div style={{ display: "flex", justifyContent: "center", marginBottom: "0.5rem" }}>
            <SkyTingLogo />
          </div>
          <h1 className="scroll-m-20 text-2xl font-semibold tracking-tight mb-4">
            Studio Dashboard
          </h1>
          <Skeleton width={200} height={24} style={{ margin: "0 auto", borderRadius: 12 }} />
        </div>
        <div style={{ maxWidth: "1280px", width: "100%", margin: "0 auto", padding: "0 1rem", display: "flex", flexDirection: "column", gap: "2rem" }}>
          {/* KPI hero skeleton */}
          <div className="grid gap-3 grid-cols-1 sm:grid-cols-3">
            {[0, 1, 2].map((i) => (
              <Card key={i}>
                <Skeleton width={80} height={12} style={{ marginBottom: 8 }} />
                <Skeleton width={140} height={36} style={{ marginBottom: 8 }} />
                <Skeleton width={100} height={14} />
              </Card>
            ))}
          </div>
          {/* Module skeletons */}
          <DashboardGrid>
            <div style={{ gridColumn: "span 7" }} className="bento-cell-a1"><ModuleSkeleton /></div>
            <div style={{ gridColumn: "span 5" }} className="bento-cell-a2"><ModuleSkeleton /></div>
            <div style={{ gridColumn: "span 12" }} className="bento-cell-b"><ModuleSkeleton /></div>
          </DashboardGrid>
        </div>
      </div>
    );
  }

  if (loadState.state === "not-configured") {
    return (
      <div className="min-h-screen flex flex-col items-center justify-center p-8">
        <div className="max-w-md text-center space-y-4">
          <SkyTingLogo />
          <h1 className="scroll-m-20 text-3xl font-semibold tracking-tight">
            Studio Dashboard
          </h1>
          <div className="rounded-2xl p-6 bg-card border border-border">
            <p className="text-muted-foreground">
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
          <h1 className="scroll-m-20 text-3xl font-semibold tracking-tight">
            Studio Dashboard
          </h1>
          <div className="rounded-2xl p-5 text-center bg-red-50 border border-red-200">
            <p className="font-medium text-destructive">
              Unable to load stats
            </p>
            <p className="text-sm mt-1 text-destructive opacity-85">
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
    <div className="min-h-screen p-4 sm:p-6 lg:p-8 pb-16 antialiased" style={{ backgroundColor: "var(--st-bg-section)", fontFamily: FONT_SANS }}>
      {/* ── Header ─────────────────────────────────── */}
      <div style={{ textAlign: "center", paddingTop: "1rem", marginBottom: "2rem" }}>
        <div style={{ display: "flex", justifyContent: "center", marginBottom: "0.5rem" }}>
          <SkyTingLogo />
        </div>
        <h1 className="scroll-m-20 text-2xl font-semibold tracking-tight mb-4">
          Studio Dashboard
        </h1>
        <FreshnessBadge lastUpdated={data.lastUpdated} spreadsheetUrl={data.spreadsheetUrl} dataSource={data.dataSource} />
      </div>

      {/* ━━ Single column — centered, spacious ━━━━━━━━ */}
      <div style={{ maxWidth: "1280px", width: "100%", margin: "0 auto", padding: "0 1rem", display: "flex", flexDirection: "column", gap: "2rem", boxSizing: "border-box" }}>

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
              <div className="flex items-center justify-between mb-2">
                <p className="text-sm leading-none font-medium text-muted-foreground uppercase tracking-wide">{LABELS.yoy}</p>
                <span className="text-sm font-medium text-muted-foreground tabular-nums">
                  {data.monthOverMonth.monthName} {data.monthOverMonth.current?.year}: {formatCurrency(data.monthOverMonth.current?.gross ?? 0)}
                </span>
              </div>
              <div className="flex flex-col">
                <div className="flex justify-between py-2.5 border-b border-border">
                  <span className="text-sm text-muted-foreground">
                    {data.monthOverMonth.monthName} {data.monthOverMonth.priorYear?.year}
                  </span>
                  <span className="text-sm font-semibold text-muted-foreground tabular-nums">
                    {formatCurrency(data.monthOverMonth.priorYear?.gross ?? 0)}
                  </span>
                </div>
                <div className="flex justify-between py-2.5 border-b border-border">
                  <span className="text-sm text-muted-foreground">
                    {data.monthOverMonth.monthName} {data.monthOverMonth.current?.year}
                  </span>
                  <span className="text-sm font-semibold tabular-nums">
                    {formatCurrency(data.monthOverMonth.current?.gross ?? 0)}
                  </span>
                </div>
                {data.monthOverMonth.yoyGrossPct !== null && (
                  <div className="flex justify-between items-center py-2.5">
                    <span className="text-sm text-muted-foreground">Change</span>
                    <DeltaBadge delta={data.monthOverMonth.yoyGrossChange ?? null} deltaPercent={data.monthOverMonth.yoyGrossPct} isCurrency compact />
                  </div>
                )}
              </div>
            </Card>
          ) : (
            <Card>
              <p className="text-sm leading-none font-medium text-muted-foreground uppercase tracking-wide mb-2">Year over Year</p>
              <p className="text-sm text-muted-foreground opacity-60">No data available</p>
            </Card>
          )}
        </div>

        {/* ── Auto-Renews ── */}
        <div className="flex flex-col gap-4">
          <SectionHeader>{LABELS.autoRenews}</SectionHeader>

          {/* Movement block: Members + Sky3 (in-studio plans) */}
          <div>
            <p className="text-xs font-medium text-muted-foreground uppercase tracking-wide mb-2">
              Movement (in-studio)
            </p>
            <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
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
            </div>
          </div>

          {/* Health block: Sky Ting TV (digital) */}
          <div>
            <p className="text-xs font-medium text-muted-foreground uppercase tracking-wide mb-2">
              Health (digital)
            </p>
            <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
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
          </div>
        </div>

        {/* ── Non-auto-renew ── */}
        {(trends?.dropIns || trends?.introWeek || trends?.newCustomerVolume || trends?.newCustomerCohorts || trends?.conversionPool) ? (
          <NonAutoRenewSection dropIns={trends?.dropIns ?? null} introWeek={trends?.introWeek ?? null} newCustomerVolume={trends?.newCustomerVolume ?? null} newCustomerCohorts={trends?.newCustomerCohorts ?? null} conversionPool={trends?.conversionPool ?? null} />
        ) : (
          <NoData label="Non-auto-renew (Drop-Ins, Funnel)" />
        )}

        {/* ── Revenue (top-level section) ── */}
        {data.monthlyRevenue && data.monthlyRevenue.length > 0 && (
          <div className="flex flex-col gap-3">
            <SectionHeader>Revenue</SectionHeader>
            <div className="grid gap-3 grid-cols-1 sm:grid-cols-2">
              <AnnualRevenueCard monthlyRevenue={data.monthlyRevenue} projection={trends?.projection} />
            </div>
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
        <p className="text-muted-foreground">
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
