"use client";

import React, { useState, useEffect, useRef, Fragment, Children } from "react";
import { Ticket, Tag, ArrowRightLeft, AlertTriangle, RefreshCw, CloudUpload } from "lucide-react";
import { DashboardLayout } from "@/components/dashboard/dashboard-layout";
import { SkyTingSwirl, SkyTingLogo } from "@/components/dashboard/sky-ting-logo";
import { SECTION_COLORS, type SectionKey } from "@/components/dashboard/sidebar-nav";
import {
  Eyeglass,
  ReportMoney,
  ClassRevenue,
  ShoppingBag,
  ChartBarPopular,
  Recycle,
  RecycleOff,
  ArrowFork,
  HourglassLow,
  UserPlus,
  UsersGroup,
  Database,
  ArrowBadgeDown,
  BrandSky,
  DeviceTv,
  BuildingIcon,
  Droplet,
  BulbIcon,
  AlertTriangleIcon,
  InfoIcon,
  CircleCheckIcon,
  DoorEnter,
  UserStar,
  CalendarWeek,
  ActivityIcon,
  MountainSun,
  DownloadIcon,
} from "@/components/dashboard/icons";
import {
  Card as ShadCard,
  CardHeader as ShadCardHeader,
  CardTitle as ShadCardTitle,
  CardDescription as ShadCardDesc,
  CardContent as ShadCardContent,
  CardFooter as ShadCardFooter,
} from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  AreaChart as RAreaChart,
  Area,
  Bar,
  BarChart,
  CartesianGrid,
  Line,
  LineChart,
  LabelList,
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
import { Tabs, TabsContent } from "@/components/ui/tabs";
import { ToggleGroup, ToggleGroupItem } from "@/components/ui/toggle-group";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  Table,
  TableHeader,
  TableBody,
  TableRow,
  TableHead,
  TableCell,
} from "@/components/ui/table";
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
  ShopifyMerchData,
  ShopifyStats,
  MerchCustomerBreakdown,
  AnnualRevenueBreakdown,
  RentalRevenueData,
  SpaData,
  InsightRow,
  OverviewData,
  TimeWindowMetrics,
  UsageData,
  UsageCategoryData,
  TenureMetrics,
  MemberAlerts,
  AtRiskMember,
  AtRiskByState,
  ExpiringIntroWeekData,
  ExpiringIntroCustomer,
  IntroWeekConversionData,
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
  nonAutoRenew: "Non Auto-Renew",
  firstVisits: "First Visits",
  dropIns: "Drop-Ins",
  returningNonMembers: "Returning Non-Members",
  newCustomers: "New Customers",
  newCustomerFunnel: "New Customer Funnel",
  conversionPool: "Non Auto-Renew Funnel",
  revenue: "Revenue",
  merch: "Merch",
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
  merch: "#B8860B",          // dark goldenrod for merch/shopify
  spa: "#6B8E9B",            // blue-grey for spa
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

function toTitleCase(s: string): string {
  return s.toLowerCase().replace(/\b\w/g, (c) => c.toUpperCase());
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

/** Smart "this week" / "last week" / "Week of Feb 17" label based on period date */
function weekLabel(period: string): string {
  const d = new Date(period + "T00:00:00");
  if (isNaN(d.getTime())) return "This Week";
  const now = new Date();
  // Get current week's Monday
  const dayOfWeek = now.getDay(); // 0=Sun, 1=Mon, ...
  const mondayOffset = dayOfWeek === 0 ? -6 : 1 - dayOfWeek;
  const thisMonday = new Date(now.getFullYear(), now.getMonth(), now.getDate() + mondayOffset);
  const lastMonday = new Date(thisMonday);
  lastMonday.setDate(lastMonday.getDate() - 7);

  if (d.getTime() === thisMonday.getTime()) return "This Week";
  if (d.getTime() === lastMonday.getTime()) return "Last Week";
  const month = d.toLocaleDateString("en-US", { month: "short" });
  return `Week of ${month} ${d.getDate()}`;
}

function formatMonthLabel(period: string): string {
  const [year, month] = period.split("-");
  const d = new Date(parseInt(year), parseInt(month) - 1);
  if (isNaN(d.getTime())) return period;
  return d.toLocaleDateString("en-US", { month: "short", year: "numeric" });
}

// ─── Shared Components ──────────────────────────────────────
// SkyTingLogo + SkyTingSwirl imported from @/components/dashboard/sky-ting-logo

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
          <h1 className="text-3xl font-semibold tracking-tight">
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
                <p className="font-medium text-emerald-600">
                  Pipeline complete
                </p>
                <p className="text-sm text-emerald-600 opacity-80">
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
    <div className="space-y-1">
      <h2 className="text-2xl font-semibold tracking-tight">
        {children}
      </h2>
      {subtitle && (
        <p className="text-sm text-muted-foreground">
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
  const colorClass = delta === 0 ? "text-muted-foreground" : isGood ? "text-emerald-600" : "text-red-500";
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

// ─── Data Section ────────────────────────────────────────────

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

// ─── Cloud Backup Status Card ───────────────────────────────

interface CloudBackupEntry {
  tag: string;
  title: string;
  createdAt: string;
  url: string;
  sizeBytes: number;
}

function BackupStatusCard() {
  const [backups, setBackups] = useState<CloudBackupEntry[]>([]);
  const [loading, setLoading] = useState(true);
  const [uploading, setUploading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const fetchBackups = React.useCallback(() => {
    setLoading(true);
    fetch("/api/backup?action=cloud-list")
      .then((r) => r.json())
      .then((d) => {
        setBackups(d.backups || []);
        setError(null);
      })
      .catch((err) => setError(err.message))
      .finally(() => setLoading(false));
  }, []);

  useEffect(() => { fetchBackups(); }, [fetchBackups]);

  async function handleBackupNow() {
    setUploading(true);
    try {
      const res = await fetch("/api/backup?action=cloud-upload");
      if (!res.ok) {
        const d = await res.json();
        throw new Error(d.error || "Backup failed");
      }
      fetchBackups();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Backup failed");
    } finally {
      setUploading(false);
    }
  }

  const latest = backups[0];

  return (
    <ShadCard className="md:col-span-2">
      <ShadCardHeader>
        <ShadCardTitle className="flex items-center gap-2">
          <CloudUpload className="size-5 text-blue-600" />
          Cloud Backups
          {latest ? (
            <Badge variant="secondary" className="bg-emerald-100 text-emerald-600 border-emerald-200">
              {backups.length} backup{backups.length !== 1 ? "s" : ""}
            </Badge>
          ) : (
            <Badge variant="outline">No backups</Badge>
          )}
        </ShadCardTitle>
        <ShadCardDesc>Database backups stored on GitHub Releases</ShadCardDesc>
      </ShadCardHeader>
      <ShadCardContent>
        {loading ? (
          <p className="text-sm text-muted-foreground">Loading...</p>
        ) : error && !latest ? (
          <p className="text-sm text-muted-foreground">{error}</p>
        ) : !latest ? (
          <p className="text-sm text-muted-foreground">No cloud backups yet. Backups are created automatically after each pipeline run and Shopify sync.</p>
        ) : (
          <div className="space-y-3">
            <div className="grid grid-cols-2 sm:grid-cols-3 gap-4">
              <div>
                <p className="text-2xl font-semibold tabular-nums">{backups.length}</p>
                <p className="text-sm text-muted-foreground">Total Backups</p>
              </div>
              <div>
                <p className="text-sm font-medium">{formatRelativeTime(latest.createdAt)}</p>
                <p className="text-sm text-muted-foreground">Last Backup</p>
              </div>
              <div>
                <p className="text-sm font-medium">{(latest.sizeBytes / 1024).toFixed(0)} KB</p>
                <p className="text-sm text-muted-foreground">Latest Size</p>
              </div>
            </div>
            {backups.length > 0 && (
              <div className="border rounded-lg overflow-hidden">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="border-b bg-muted/50">
                      <th className="text-left px-4 py-2 font-medium">Backup</th>
                      <th className="text-left px-3 py-2 font-medium hidden sm:table-cell">Date</th>
                      <th className="text-left px-3 py-2 font-medium hidden sm:table-cell">Size</th>
                      <th className="text-right px-4 py-2 font-medium">Link</th>
                    </tr>
                  </thead>
                  <tbody>
                    {backups.slice(0, 5).map((b) => (
                      <tr key={b.tag} className="border-b last:border-0">
                        <td className="px-4 py-2 font-mono text-xs">{b.tag.replace("backup-", "")}</td>
                        <td className="px-4 py-2 text-muted-foreground hidden sm:table-cell">{formatRelativeTime(b.createdAt)}</td>
                        <td className="px-4 py-2 text-muted-foreground hidden sm:table-cell">{(b.sizeBytes / 1024).toFixed(0)} KB</td>
                        <td className="px-4 py-2 text-right">
                          <a href={b.url} target="_blank" rel="noopener noreferrer" className="text-blue-600 hover:underline inline-flex items-center gap-1">
                            <DownloadIcon className="size-3" />
                            View
                          </a>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </div>
        )}
      </ShadCardContent>
      <ShadCardFooter className="border-t pt-4 flex items-center justify-between">
        <p className="text-xs text-muted-foreground">
          Auto-backup after pipeline runs. Stored as GitHub Releases (max 14).
        </p>
        <Button
          variant="outline"
          size="sm"
          disabled={uploading}
          onClick={handleBackupNow}
        >
          <CloudUpload className={`size-4 mr-1.5 ${uploading ? "animate-pulse" : ""}`} />
          {uploading ? "Backing up..." : "Backup Now"}
        </Button>
      </ShadCardFooter>
    </ShadCard>
  );
}

function DataSection({ lastUpdated, spreadsheetUrl, dataSource, shopify }: { lastUpdated: string | null; spreadsheetUrl?: string; dataSource?: "database" | "sheets" | "hybrid"; shopify?: ShopifyStats | null }) {
  const [refreshState, setRefreshState] = useState<"idle" | "running" | "done" | "error">("idle");
  const [pipelineStep, setPipelineStep] = useState("");
  const [pipelinePercent, setPipelinePercent] = useState(0);
  const [pipelineStartedAt, setPipelineStartedAt] = useState(0);
  const [now, setNow] = useState(Date.now());
  const eventSourceRef = useRef<EventSource | null>(null);
  const countdown = useNextRunCountdown();

  useEffect(() => {
    if (refreshState !== "running") return;
    const timer = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(timer);
  }, [refreshState]);

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
      if (res.status === 409) { setRefreshState("running"); return; }
      if (!res.ok) throw new Error("Failed");
      const { jobId } = await res.json();
      eventSourceRef.current?.close();
      const es = new EventSource(`/api/status?jobId=${jobId}`);
      eventSourceRef.current = es;
      es.addEventListener("progress", (e) => {
        const d = JSON.parse(e.data);
        setPipelineStep(d.step || "Processing...");
        setPipelinePercent(d.percent || 0);
        if (d.startedAt) setPipelineStartedAt(d.startedAt);
      });
      es.addEventListener("complete", () => { es.close(); setRefreshState("done"); setPipelinePercent(100); setTimeout(() => window.location.reload(), 2000); });
      es.addEventListener("error", (e) => { try { setPipelineStep(JSON.parse((e as MessageEvent).data).message || "Error"); } catch {} es.close(); setRefreshState("error"); setTimeout(() => setRefreshState("idle"), 8000); });
    } catch { setRefreshState("error"); setTimeout(() => setRefreshState("idle"), 5000); }
  }

  const elapsed = pipelineStartedAt ? now - pipelineStartedAt : 0;
  const etaMs = pipelinePercent > 5 && elapsed > 0 ? (elapsed / pipelinePercent) * (100 - pipelinePercent) : 0;

  const isStale = lastUpdated ? Date.now() - new Date(lastUpdated).getTime() > 24 * 60 * 60 * 1000 : true;

  return (
    <div className="grid gap-4 md:grid-cols-2">
      {/* ── Last Update ── */}
      <ShadCard>
        <ShadCardHeader>
          <ShadCardTitle className="flex items-center gap-2">
            Last Update
            {lastUpdated && (
              <Badge variant={isStale ? "destructive" : "secondary"} className={!isStale ? "bg-emerald-100 text-emerald-600 border-emerald-200" : ""}>
                {isStale ? "Stale" : "Fresh"}
              </Badge>
            )}
          </ShadCardTitle>
          <ShadCardDesc>Most recent data refresh</ShadCardDesc>
        </ShadCardHeader>
        <ShadCardContent>
          {lastUpdated ? (
            <div className="space-y-3">
              <div className="flex items-baseline gap-2">
                <span className="text-2xl font-semibold tabular-nums">{formatRelativeTime(lastUpdated)}</span>
              </div>
              <div className="text-sm text-muted-foreground">
                {formatDateTime(lastUpdated)}
              </div>
              {dataSource && (
                <div className="text-xs text-muted-foreground">
                  Source: {dataSource === "database" ? "PostgreSQL" : dataSource === "sheets" ? "Google Sheets" : "Database + Sheets"}
                </div>
              )}
            </div>
          ) : (
            <p className="text-sm text-muted-foreground">No data loaded yet</p>
          )}
        </ShadCardContent>
        {spreadsheetUrl && (
          <ShadCardFooter>
            <Button variant="outline" size="sm" asChild>
              <a href={spreadsheetUrl} target="_blank" rel="noopener noreferrer">
                <svg className="size-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                  <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z" />
                  <polyline points="14 2 14 8 20 8" />
                </svg>
                View in Google Sheets
              </a>
            </Button>
          </ShadCardFooter>
        )}
      </ShadCard>

      {/* ── Next Update ── */}
      <ShadCard>
        <ShadCardHeader>
          <ShadCardTitle className="flex items-center gap-2">
            Next Update
            {refreshState === "running" && (
              <Badge variant="secondary" className="bg-blue-100 text-blue-700 border-blue-200">Running</Badge>
            )}
          </ShadCardTitle>
          <ShadCardDesc>Scheduled pipeline run</ShadCardDesc>
        </ShadCardHeader>
        <ShadCardContent>
          <div className="space-y-3">
            {countdown && refreshState !== "running" ? (
              <div className="flex items-baseline gap-2">
                <span className="text-2xl font-semibold tabular-nums">{countdown}</span>
              </div>
            ) : refreshState === "running" ? (
              <div className="space-y-2">
                <div className="flex justify-between text-sm">
                  <span className="text-muted-foreground">{pipelineStep}</span>
                  <span className="font-medium tabular-nums">{pipelinePercent}%</span>
                </div>
                <div className="h-2 rounded-full overflow-hidden bg-muted">
                  <div className="h-full rounded-full bg-primary transition-all duration-500" style={{ width: `${pipelinePercent}%` }} />
                </div>
                {etaMs > 3000 && (
                  <p className="text-xs text-muted-foreground">{formatEta(etaMs)} remaining</p>
                )}
              </div>
            ) : (
              <p className="text-sm text-muted-foreground">No schedule configured</p>
            )}
          </div>
        </ShadCardContent>
        <ShadCardFooter>
          <Button
            variant={refreshState === "error" ? "destructive" : "outline"}
            size="sm"
            disabled={refreshState === "running"}
            onClick={triggerRefresh}
          >
            <svg className="size-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" style={{ animation: refreshState === "running" ? "spin 1s linear infinite" : "none" }}>
              <polyline points="23 4 23 10 17 10" />
              <polyline points="1 20 1 14 7 14" />
              <path d="M3.51 9a9 9 0 0 1 14.85-3.36L23 10M1 14l4.64 4.36A9 9 0 0 0 20.49 15" />
            </svg>
            {refreshState === "running" ? "Refreshing..." : refreshState === "done" ? "Done" : refreshState === "error" ? "Retry" : "Refresh Now"}
          </Button>
        </ShadCardFooter>
      </ShadCard>

      {/* ── Pipeline Info ── */}
      <ShadCard className="md:col-span-2">
        <ShadCardHeader>
          <ShadCardTitle>Data Pipeline</ShadCardTitle>
          <ShadCardDesc>How dashboard data gets updated</ShadCardDesc>
        </ShadCardHeader>
        <ShadCardContent>
          <div className="space-y-4">
            <div className="flex items-start gap-3">
              <div className="mt-0.5 flex size-6 shrink-0 items-center justify-center rounded-full bg-amber-100 text-amber-700 text-xs font-semibold">1</div>
              <div>
                <p className="text-sm font-medium">Union.fit sends daily zip</p>
                <p className="text-xs text-muted-foreground">Automated export to robot@skyting.com with all report CSVs</p>
              </div>
              <Badge variant="outline" className="ml-auto shrink-0">Pending</Badge>
            </div>
            <div className="flex items-start gap-3">
              <div className="mt-0.5 flex size-6 shrink-0 items-center justify-center rounded-full bg-muted text-muted-foreground text-xs font-semibold">2</div>
              <div>
                <p className="text-sm font-medium">Pipeline processes email</p>
                <p className="text-xs text-muted-foreground">Login to inbox, find zip, extract CSVs, parse and validate</p>
              </div>
              <Badge variant="outline" className="ml-auto shrink-0">Not started</Badge>
            </div>
            <div className="flex items-start gap-3">
              <div className="mt-0.5 flex size-6 shrink-0 items-center justify-center rounded-full bg-muted text-muted-foreground text-xs font-semibold">3</div>
              <div>
                <p className="text-sm font-medium">Upload to database</p>
                <p className="text-xs text-muted-foreground">Upsert records into PostgreSQL, update dashboard metrics</p>
              </div>
              <Badge variant="outline" className="ml-auto shrink-0">Not started</Badge>
            </div>
          </div>
        </ShadCardContent>
        <ShadCardFooter className="border-t pt-4">
          <p className="text-xs text-muted-foreground">
            Waiting for first zip from Union.fit. Current data loaded via manual CSV uploads and legacy pipeline.
          </p>
        </ShadCardFooter>
      </ShadCard>

      {/* ── Cloud Backups ── */}
      <BackupStatusCard />

      {/* ── Shopify ── */}
      <ShadCard className="md:col-span-2">
        <ShadCardHeader>
          <ShadCardTitle className="flex items-center gap-2">
            <ShoppingBag className="size-5" style={{ color: SECTION_COLORS["revenue-merch"] }} />
            Shopify
            {shopify ? (
              <Badge variant="secondary" className="bg-emerald-100 text-emerald-600 border-emerald-200">Connected</Badge>
            ) : (
              <Badge variant="outline">Not connected</Badge>
            )}
          </ShadCardTitle>
          <ShadCardDesc>Merch store data via Shopify API</ShadCardDesc>
        </ShadCardHeader>
        <ShadCardContent>
          {shopify ? (
            <div className="grid grid-cols-2 sm:grid-cols-4 gap-4">
              <div>
                <p className="text-2xl font-semibold tabular-nums">{shopify.totalOrders.toLocaleString()}</p>
                <p className="text-sm text-muted-foreground">Orders</p>
              </div>
              <div>
                <p className="text-2xl font-semibold tabular-nums">{formatCurrency(shopify.totalRevenue)}</p>
                <p className="text-sm text-muted-foreground">Total Revenue</p>
              </div>
              <div>
                <p className="text-2xl font-semibold tabular-nums">{shopify.productCount.toLocaleString()}</p>
                <p className="text-sm text-muted-foreground">Products</p>
              </div>
              <div>
                <p className="text-2xl font-semibold tabular-nums">{shopify.customerCount.toLocaleString()}</p>
                <p className="text-sm text-muted-foreground">Customers</p>
              </div>
            </div>
          ) : (
            <p className="text-sm text-muted-foreground">Connect Shopify in Settings to see merch data here.</p>
          )}
        </ShadCardContent>
        {shopify?.lastSyncAt && (
          <ShadCardFooter className="border-t pt-4">
            <p className="text-xs text-muted-foreground">
              Last synced {formatRelativeTime(shopify.lastSyncAt)} · {formatDateTime(shopify.lastSyncAt)}
            </p>
          </ShadCardFooter>
        )}
      </ShadCard>
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
      <CardContent className={matchHeight ? "flex-1 flex flex-col" : undefined}>
        {children}
      </CardContent>
    </DashboardCard>
  );
}

function NoData({ label }: { label: string }) {
  return (
    <Card>
      <div className="flex items-center justify-center py-8">
        <p className="text-sm text-muted-foreground">{label}</p>
      </div>
    </Card>
  );
}

/** Inline empty state for use inside an existing card/chart container */
function NoDataInline({ label = "No data available" }: { label?: string }) {
  return (
    <div className="flex items-center justify-center h-[200px]">
      <span className="text-sm text-muted-foreground">{label}</span>
    </div>
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
        <span className="text-base leading-5 font-semibold tracking-tight text-foreground">{title}</span>
        {summaryPill && (
          <span className="text-xs leading-4 font-medium text-muted-foreground bg-muted rounded-full px-2 py-0.5">{summaryPill}</span>
        )}
      </div>
      <div className="flex items-center gap-3">
        {children}
        {onToggleDetails && (
          <button
            type="button"
            onClick={onToggleDetails}
            className="text-xs leading-4 text-muted-foreground hover:text-foreground font-medium inline-flex items-center gap-1 transition-colors"
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

/** Subsection header — icon + label */
function SubsectionHeader({ children, icon: Icon, color }: { children: React.ReactNode; icon?: React.ComponentType<{ className?: string; style?: React.CSSProperties }>; color?: string }) {
  return (
    <div className="flex items-center gap-2.5 pt-2 pb-1">
      {Icon && <Icon className="size-5" style={color ? { color } : undefined} />}
      <h3 className="text-lg font-semibold tracking-tight" style={color ? { color } : undefined}>
        {children}
      </h3>
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
      className="inline-flex items-center justify-center h-3.5 w-3.5 shrink-0 cursor-help text-[11px] text-muted-foreground/70 relative"
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
          <div className={`${s ? (sec ? "text-[34px] leading-[34px] text-foreground/80" : "text-[40px] leading-[40px] text-foreground") : "text-[52px] leading-[52px] text-foreground"} font-semibold tracking-[-0.02em] tabular-nums`}>
            {value}
          </div>
          {valueSuffix ? (
            <div className={`${s ? (sec ? "text-[14px] leading-[14px] text-muted-foreground/70" : "text-[16px] leading-[16px] text-muted-foreground") : "text-[18px] leading-[18px] text-muted-foreground"} font-semibold tabular-nums`}>
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
            <span className="text-[14px] leading-[18px] text-muted-foreground whitespace-nowrap truncate">{label}</span>
            {tooltip ? <MInfoIcon tooltip={tooltip} /> : null}
          </div>
          {(metaLeft || metaRight) ? (
            <div className="mt-1 flex items-center gap-2 min-w-0 leading-none">
              {metaLeft ? <div className="shrink-0">{metaLeft}</div> : null}
              {metaRight ? (
                <div className="truncate text-[12px] text-muted-foreground leading-none">{metaRight}</div>
              ) : null}
            </div>
          ) : null}
        </div>
      ) : (
        /* Hero variant: fixed-height rows for cross-component alignment */
        <>
          <div className="h-[40px] mt-1 text-[16px] leading-[20px] text-muted-foreground">
            <span className="inline-flex items-center gap-1">
              <span>{label}</span>
              {tooltip ? <MInfoIcon tooltip={tooltip} /> : null}
            </span>
          </div>
          <div className="h-[28px] mt-2 flex items-center gap-2 min-w-0 leading-none">
            {metaLeft ? <div className="shrink-0">{metaLeft}</div> : null}
            {metaRight ? (
              <div className="truncate leading-none text-[14px] text-muted-foreground">{metaRight}</div>
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
        <div key={i} className={`flex flex-col items-start${i > 0 ? " border-l border-border pl-4" : ""}`}>
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
        <span className="text-[40px] leading-[44px] font-semibold tracking-[-0.02em] text-foreground tabular-nums">
          {total != null ? formatNumber(total) : "\u2014"}
        </span>
        <div className="mt-1 text-[13px] leading-[16px] font-medium text-muted-foreground">{totalLabel}</div>
      </div>
      {/* Middle: 3-Week Conversion Rate */}
      <div className="min-w-0 border-l border-border/50 pl-6">
        <div className="flex items-baseline">
          <span className="text-[40px] leading-[44px] font-semibold tracking-[-0.02em] text-foreground tabular-nums">
            {rate != null ? rate.toFixed(1) : "\u2014"}
          </span>
          {rate != null && (
            <span className="text-[18px] leading-[22px] font-semibold tracking-[-0.01em] text-muted-foreground ml-0.5 tabular-nums">%</span>
          )}
        </div>
        <div className="mt-1 inline-flex items-center gap-1">
          <span className="text-[13px] leading-[16px] font-medium text-muted-foreground">3-Week Conversion Rate</span>
          {rateTooltip && <MInfoIcon tooltip={rateTooltip} />}
        </div>
      </div>
      {/* Right: Expected Converts */}
      <div className="min-w-0 border-l border-border/50 pl-6">
        <span className="text-[40px] leading-[44px] font-semibold tracking-[-0.02em] text-foreground tabular-nums">
          {expected != null ? formatNumber(expected) : "\u2014"}
        </span>
        <div className="mt-1 inline-flex items-center gap-1">
          <span className="text-[13px] leading-[16px] font-medium text-muted-foreground">{expectedLabel ?? "Expected Converts"}</span>
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
// shadcn v4 DataTable classes (see STYLE_GUIDE.md → Tables)
const modThClass = "h-10 px-4 text-right align-middle font-medium text-muted-foreground whitespace-nowrap";
const modTdClass = "px-4 py-2 align-middle text-right tabular-nums whitespace-nowrap";

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
    positive: "text-emerald-600 bg-emerald-50",
    negative: "text-red-500 bg-red-50",
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
      <div className="mt-4 pt-4 border-t border-border space-y-4">
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
            className={`w-6 rounded-[3px] ${isLast ? "bg-muted-foreground/60" : "bg-border"}`}
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
            <CardDescription className="text-sm leading-none font-medium text-muted-foreground uppercase tracking-wide">
              {tile.label}
            </CardDescription>
            <CardTitle className={`text-3xl font-semibold tracking-tight tabular-nums ${
              tile.delta != null && tile.delta !== 0
                ? (tile.delta > 0) === (tile.isPositiveGood !== false)
                  ? "text-emerald-600"
                  : "text-red-500"
                : ""
            }`}>
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

// ─── Overview Section ─────────────────────────────────────────

function OverviewSection({ data }: { data: OverviewData }) {
  const isMobile = useIsMobile();
  const windowMap = {
    yesterday: data.yesterday,
    thisWeek: data.thisWeek,
    lastWeek: data.lastWeek,
    thisMonth: data.thisMonth,
    lastMonth: data.lastMonth,
  };
  const windowKeys = Object.keys(windowMap) as (keyof typeof windowMap)[];
  const [activeWindow, setActiveWindow] = useState<keyof typeof windowMap>("thisWeek");
  const windows: TimeWindowMetrics[] = [data.yesterday, data.thisWeek, data.lastWeek, data.thisMonth, data.lastMonth];
  const active = data.currentActive;

  const autoRenewRows: {
    key: string;
    icon: React.ComponentType<{ size?: number; className?: string; style?: React.CSSProperties }>;
    label: string;
    color: string;
    activeCount: number;
    getSub: (w: TimeWindowMetrics) => { new: number; churned: number };
  }[] = [
    { key: "member", icon: ArrowBadgeDown, label: LABELS.members, color: COLORS.member, activeCount: active.member, getSub: (w) => w.subscriptions.member },
    { key: "sky3", icon: BrandSky, label: LABELS.sky3, color: COLORS.sky3, activeCount: active.sky3, getSub: (w) => w.subscriptions.sky3 },
    { key: "tv", icon: DeviceTv, label: LABELS.tv, color: COLORS.tv, activeCount: active.skyTingTv, getSub: (w) => w.subscriptions.skyTingTv },
  ];

  const nonAutoRows: {
    key: string;
    icon: React.ComponentType<{ size?: number; className?: string; style?: React.CSSProperties }>;
    label: string;
    color: string;
    getCount: (w: TimeWindowMetrics) => number;
  }[] = [
    { key: "dropIns", icon: DoorEnter, label: LABELS.dropIns, color: COLORS.dropIn, getCount: (w) => w.activity.dropIns },
    { key: "guests", icon: UserStar, label: "Guests", color: COLORS.teal, getCount: (w) => w.activity.guests },
    { key: "introWeeks", icon: CalendarWeek, label: "Intro Weeks", color: COLORS.copper, getCount: (w) => w.activity.introWeeks },
  ];

  // ── Mobile layout: stacked cards + time window selector ──
  if (isMobile) {
    const w = windowMap[activeWindow];

    return (
      <div className="flex flex-col gap-4">
        {/* Time window selector */}
        <ToggleGroup
          variant="outline"
          type="single"
          value={activeWindow}
          onValueChange={(v) => { if (v) setActiveWindow(v as keyof typeof windowMap); }}
          className="justify-start"
        >
          {windowKeys.map((k) => (
            <ToggleGroupItem key={k} value={k} aria-label={windowMap[k].label} className="text-xs px-3 h-8">
              {windowMap[k].label}
            </ToggleGroupItem>
          ))}
        </ToggleGroup>
        <p className="text-[11px] text-muted-foreground -mt-2">{w.sublabel}</p>

        {/* Auto-Renews */}
        <div className="flex flex-col gap-2">
          <div className="flex items-center gap-2 px-1">
            <Recycle className="size-4" style={{ color: SECTION_COLORS["growth-auto"] }} />
            <span className="text-xs font-semibold text-muted-foreground uppercase tracking-wide">{LABELS.autoRenews}</span>
          </div>
          {autoRenewRows.map(({ key, icon: Icon, label, color, activeCount, getSub }) => {
            const sub = getSub(w);
            const net = sub.new - sub.churned;
            const isEmpty = sub.new === 0 && sub.churned === 0;
            return (
              <Card key={key}>
                <div className="flex items-center justify-between">
                  <div className="flex items-center gap-2">
                    <Icon size={18} style={{ color }} className="shrink-0" />
                    <div>
                      <span className="text-sm font-semibold">{label}</span>
                      <span className="text-xs text-muted-foreground ml-1.5 tabular-nums">{formatNumber(activeCount)} active</span>
                    </div>
                  </div>
                  <div className="text-right">
                    {isEmpty ? (
                      <span className="text-muted-foreground/40 text-sm">—</span>
                    ) : (
                      <>
                        <div className={`text-lg font-semibold tabular-nums ${net > 0 ? "text-emerald-600" : net < 0 ? "text-red-500" : "text-muted-foreground"}`}>
                          {net > 0 ? "+" : ""}{net}
                        </div>
                        <div className="text-[11px] text-muted-foreground/50 tabular-nums">
                          +{sub.new} / -{sub.churned}
                        </div>
                      </>
                    )}
                  </div>
                </div>
              </Card>
            );
          })}
        </div>

        {/* Non Auto-Renews */}
        <div className="flex flex-col gap-2">
          <div className="flex items-center gap-2 px-1">
            <RecycleOff className="size-4" style={{ color: SECTION_COLORS["growth-non-auto"] }} />
            <span className="text-xs font-semibold text-muted-foreground uppercase tracking-wide">Non Auto-Renews</span>
          </div>
          {nonAutoRows.map(({ key, icon: Icon, label, color, getCount }) => {
            const count = getCount(w);
            return (
              <Card key={key}>
                <div className="flex items-center justify-between">
                  <div className="flex items-center gap-2">
                    <Icon size={18} style={{ color }} className="shrink-0" />
                    <span className="text-sm font-semibold">{label}</span>
                  </div>
                  <span className={`text-lg font-semibold tabular-nums ${count === 0 ? "text-muted-foreground/40" : ""}`}>
                    {count === 0 ? "—" : formatNumber(count)}
                  </span>
                </div>
              </Card>
            );
          })}
        </div>
      </div>
    );
  }

  // ── Desktop layout: tables ──
  const thData = "text-right text-muted-foreground/70 font-normal text-xs";
  const tdData = "text-right tabular-nums";
  const rowHeight = "h-[50px]";

  return (
    <div className="flex flex-col gap-4">
      {/* ── AUTO-RENEWS ─────────────────────── */}
      <DashboardCard>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Recycle className="size-5" style={{ color: SECTION_COLORS["growth-auto"] }} />
            {LABELS.autoRenews}
          </CardTitle>
        </CardHeader>
        <CardContent>
          <Table style={{ tableLayout: "fixed", fontFamily: FONT_SANS }}>
            <TableHeader>
              <TableRow>
                <TableHead className="w-[150px] text-muted-foreground text-xs">Metric</TableHead>
                <TableHead className={`w-[80px] ${thData}`}>Active</TableHead>
                {windows.map((w) => (
                  <TableHead key={w.label} className={thData}>
                    <div>{w.label}</div>
                    <div className="text-[10px] text-muted-foreground/50">{w.sublabel}</div>
                  </TableHead>
                ))}
              </TableRow>
            </TableHeader>
            <TableBody>
              {autoRenewRows.map(({ key, icon: Icon, label, color, activeCount, getSub }) => (
                <TableRow key={key} className={rowHeight}>
                  <TableCell className="font-medium">
                    <div className="flex items-center gap-2">
                      <Icon size={16} style={{ color }} className="shrink-0" />
                      <span>{label}</span>
                    </div>
                  </TableCell>
                  <TableCell className={`${tdData} font-semibold`}>{formatNumber(activeCount)}</TableCell>
                  {windows.map((w) => {
                    const sub = getSub(w);
                    const net = sub.new - sub.churned;
                    const isEmpty = sub.new === 0 && sub.churned === 0;
                    return (
                      <TableCell key={w.label} className={tdData}>
                        {isEmpty ? (
                          <span className="text-muted-foreground/40">—</span>
                        ) : (
                          <>
                            <div className={`text-base font-semibold ${net > 0 ? "text-emerald-600" : net < 0 ? "text-red-500" : "text-muted-foreground"}`}>
                              {net > 0 ? "+" : ""}{net}
                            </div>
                            <div className="text-[11px] text-muted-foreground/40 leading-tight">
                              +{sub.new} / -{sub.churned}
                            </div>
                          </>
                        )}
                      </TableCell>
                    );
                  })}
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </CardContent>
      </DashboardCard>

      {/* ── NON AUTO-RENEWS ─────────────────── */}
      <DashboardCard>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <RecycleOff className="size-5" style={{ color: SECTION_COLORS["growth-non-auto"] }} />
            Non Auto-Renews
          </CardTitle>
        </CardHeader>
        <CardContent>
          <Table style={{ tableLayout: "fixed", fontFamily: FONT_SANS }}>
            <TableHeader>
              <TableRow>
                <TableHead className="w-[150px] text-muted-foreground text-xs">Metric</TableHead>
                <TableHead className="w-[80px]" />
                {windows.map((w) => (
                  <TableHead key={w.label} className={thData}>
                    <div>{w.label}</div>
                    <div className="text-[10px] text-muted-foreground/50">{w.sublabel}</div>
                  </TableHead>
                ))}
              </TableRow>
            </TableHeader>
            <TableBody>
              {nonAutoRows.map(({ key, icon: Icon, label, color, getCount }) => (
                <TableRow key={key} className={rowHeight}>
                  <TableCell className="font-medium">
                    <div className="flex items-center gap-2">
                      <Icon size={16} style={{ color }} className="shrink-0" />
                      <span>{label}</span>
                    </div>
                  </TableCell>
                  <TableCell />
                  {windows.map((w) => {
                    const count = getCount(w);
                    return (
                      <TableCell key={w.label} className={`${tdData} text-base font-semibold`}>
                        {count === 0 ? (
                          <span className="text-muted-foreground/40">—</span>
                        ) : (
                          formatNumber(count)
                        )}
                      </TableCell>
                    );
                  })}
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </CardContent>
      </DashboardCard>
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
                <CartesianGrid vertical={false} />
                <YAxis hide domain={["dataMin - 20000", "dataMax + 10000"]} />
                <XAxis
                  dataKey="month"
                  tickLine={false}
                  axisLine={false}
                  tickMargin={8}
                  interval={isMobile ? 2 : 0}
                  fontSize={isMobile ? 11 : 12}
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
                {momDeltaPct > 0 ? "+" : ""}{momDeltaPct}% Month over Month
              </div>
              <div className="text-muted-foreground leading-none">
                Last 12 Months of Gross Revenue
              </div>
            </CardFooter>
          )}
        </DashboardCard>
      )}

    </div>
  );
}

// ─── Retreat Revenue Section ─────────────────────────────────────
function RetreatRevenueSection({ monthlyRevenue }: {
  monthlyRevenue: { month: string; gross: number; net: number; retreatGross?: number; retreatNet?: number }[];
}) {
  const isMobile = useIsMobile();
  const nowDate = new Date();
  const currentMonthKey = `${nowDate.getFullYear()}-${String(nowDate.getMonth() + 1).padStart(2, "0")}`;

  // Build retreat-only monthly data
  const retreatMonths = monthlyRevenue
    .filter((m) => (m.retreatGross ?? 0) > 0)
    .map((m) => ({
      month: m.month,
      gross: m.retreatGross ?? 0,
      net: m.retreatNet ?? 0,
    }));

  const completedMonths = retreatMonths.filter((m) => m.month < currentMonthKey);
  const currentMonthEntry = retreatMonths.find((m) => m.month === currentMonthKey);

  // Chart data — last 12 completed months
  const chartData = completedMonths.slice(-12).map((m) => ({
    month: formatShortMonth(m.month),
    gross: m.gross,
  }));

  // MoM delta
  const lastTwo = completedMonths.slice(-2);
  const lastMonth = lastTwo.length >= 1 ? lastTwo[lastTwo.length - 1] : null;
  const prevMonth = lastTwo.length >= 2 ? lastTwo[0] : null;
  const momDeltaPct = lastMonth && prevMonth && prevMonth.gross > 0
    ? Math.round((lastMonth.gross - prevMonth.gross) / prevMonth.gross * 1000) / 10
    : null;

  // Average monthly retreat revenue (completed months only)
  const avgMonthly = completedMonths.length > 0
    ? Math.round(completedMonths.reduce((s, m) => s + m.gross, 0) / completedMonths.length)
    : 0;

  const retreatChartConfig = {
    gross: { label: "Retreat Revenue", color: "#B87333" },
  } satisfies ChartConfig;

  if (retreatMonths.length === 0) {
    return (
      <DashboardCard>
        <CardContent>
          <NoDataInline label="No retreat revenue data available." />
        </CardContent>
      </DashboardCard>
    );
  }

  return (
    <div className="flex flex-col gap-4">
      {/* KPI cards */}
      <div className="grid grid-cols-1 sm:grid-cols-3 gap-3">
        {currentMonthEntry && (
          <DashboardCard>
            <CardHeader className="pb-2">
              <CardDescription>MTD Retreat Revenue</CardDescription>
              <CardTitle className="text-2xl font-semibold tabular-nums">{formatCurrency(currentMonthEntry.gross)}</CardTitle>
            </CardHeader>
            <CardFooter className="text-sm text-muted-foreground">
              Net: {formatCurrency(currentMonthEntry.net)}
            </CardFooter>
          </DashboardCard>
        )}
        <DashboardCard>
          <CardHeader className="pb-2">
            <CardDescription>Avg Monthly</CardDescription>
            <CardTitle className="text-2xl font-semibold tabular-nums">{formatCurrency(avgMonthly)}</CardTitle>
          </CardHeader>
          <CardFooter className="text-sm text-muted-foreground">
            {completedMonths.length} months of data
          </CardFooter>
        </DashboardCard>
        {lastMonth && (
          <DashboardCard>
            <CardHeader className="pb-2">
              <CardDescription>Last Month</CardDescription>
              <CardTitle className="text-2xl font-semibold tabular-nums">{formatCurrency(lastMonth.gross)}</CardTitle>
            </CardHeader>
            <CardFooter className="text-sm text-muted-foreground">
              {formatShortMonth(lastMonth.month)}
              {momDeltaPct != null && ` (${momDeltaPct > 0 ? "+" : ""}${momDeltaPct}% MoM)`}
            </CardFooter>
          </DashboardCard>
        )}
      </div>

      {/* Monthly chart */}
      {chartData.length > 1 && (
        <DashboardCard>
          <CardHeader>
            <CardTitle>Monthly Retreat Revenue</CardTitle>
            {lastMonth && (
              <CardDescription>
                {formatShortMonth(lastMonth.month)}: {formatCurrency(lastMonth.gross)}
              </CardDescription>
            )}
          </CardHeader>
          <CardContent>
            <ChartContainer config={retreatChartConfig} className="h-[250px] w-full">
              <BarChart accessibilityLayer data={chartData} margin={{ top: 20, left: isMobile ? 8 : 24, right: isMobile ? 8 : 24 }}>
                <CartesianGrid vertical={false} />
                <YAxis hide />
                <XAxis
                  dataKey="month"
                  tickLine={false}
                  axisLine={false}
                  tickMargin={8}
                  interval={isMobile ? 2 : 0}
                  fontSize={isMobile ? 11 : 12}
                />
                <Bar dataKey="gross" fill="var(--color-gross)" radius={8}>
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
        </DashboardCard>
      )}

      {/* Month breakdown table */}
      {completedMonths.length > 0 && (
        <DashboardCard>
          <CardHeader>
            <CardTitle>Month Breakdown</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="overflow-x-auto">
              <table className="w-full caption-bottom text-sm" style={{ fontFamily: FONT_SANS }}>
                <thead className="bg-muted [&_tr]:border-b">
                  <tr>
                    <th className="h-10 px-4 text-left align-middle font-medium text-muted-foreground whitespace-nowrap">Month</th>
                    <th className="h-10 px-4 text-right align-middle font-medium text-muted-foreground whitespace-nowrap">Gross</th>
                    <th className="h-10 px-4 text-right align-middle font-medium text-muted-foreground whitespace-nowrap">Net</th>
                    <th className="h-10 px-4 text-right align-middle font-medium text-muted-foreground whitespace-nowrap">Margin</th>
                  </tr>
                </thead>
                <tbody className="[&_tr:last-child]:border-0">
                  {completedMonths.slice(-12).reverse().map((m) => (
                    <tr key={m.month} className="border-b">
                      <td className="px-4 py-2 align-middle font-medium whitespace-nowrap">{formatShortMonth(m.month)}</td>
                      <td className="px-4 py-2 align-middle text-right tabular-nums whitespace-nowrap">{formatCurrency(m.gross)}</td>
                      <td className="px-4 py-2 align-middle text-right tabular-nums whitespace-nowrap">{formatCurrency(m.net)}</td>
                      <td className="px-4 py-2 align-middle text-right tabular-nums whitespace-nowrap text-muted-foreground">
                        {m.gross > 0 ? `${Math.round((m.net / m.gross) * 100)}%` : "—"}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </CardContent>
        </DashboardCard>
      )}
    </div>
  );
}

function MonthBreakdownCard({ monthlyRevenue, monthOverMonth }: {
  monthlyRevenue: { month: string; gross: number; net: number }[];
  monthOverMonth?: MonthOverMonthData | null;
}) {
  const nowDate = new Date();
  const currentMonthKey = `${nowDate.getFullYear()}-${String(nowDate.getMonth() + 1).padStart(2, "0")}`;
  const completed = (monthlyRevenue || []).filter((m) => m.month < currentMonthKey);
  const currentMonth = completed.length >= 1 ? completed[completed.length - 1] : null;
  if (!currentMonth) return null;

  return (
    <DashboardCard>
      <CardHeader>
        <CardTitle>{formatMonthLabel(currentMonth.month)} Revenue</CardTitle>
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
              <span className={`text-sm tabular-nums ${row.bold ? "font-semibold" : "font-medium"} ${row.destructive ? "text-red-500" : ""}`}>
                {row.value}
              </span>
            </div>
          ))}
        </div>

        {/* Year-over-Year comparison */}
        {monthOverMonth?.priorYear && monthOverMonth?.current && (
          <div className="mt-4 pt-4 border-t border-border">
            <p className="text-xs font-medium text-muted-foreground uppercase tracking-wide mb-2">Year over Year</p>
            <div className="flex flex-col">
              <div className="flex justify-between py-2 border-b border-border">
                <span className="text-sm text-muted-foreground">
                  {monthOverMonth.monthName} {monthOverMonth.priorYear.year}
                </span>
                <span className="text-sm font-medium text-muted-foreground tabular-nums">
                  {formatCurrency(monthOverMonth.priorYear.gross)}
                </span>
              </div>
              <div className="flex justify-between py-2 border-b border-border">
                <span className="text-sm text-muted-foreground">
                  {monthOverMonth.monthName} {monthOverMonth.current.year}
                </span>
                <span className="text-sm font-semibold tabular-nums">
                  {formatCurrency(monthOverMonth.current.gross)}
                </span>
              </div>
              {monthOverMonth.yoyGrossPct !== null && (
                <div className="flex justify-between items-center py-2">
                  <span className="text-sm text-muted-foreground">Change</span>
                  <DeltaBadge delta={monthOverMonth.yoyGrossChange ?? null} deltaPercent={monthOverMonth.yoyGrossPct} isCurrency compact />
                </div>
              )}
            </div>
          </div>
        )}
      </CardContent>
    </DashboardCard>
  );
}

// ─── Annual Revenue Breakdown by Segment ────────────────────

const ANNUAL_SEGMENT_COLORS: Record<string, string> = {
  "In-Studio": "#5B7FA5",          // steel blue
  "Digital": "#7B68AE",            // purple
  "Retreats": "#B87333",           // copper
  "Teacher Training": "#D4764E",   // terracotta
  "Spa": "#4A7C59",                // forest green
  "Rentals": "#6B8E9B",            // teal
  "Merch": "#C4A35A",              // gold
  "Other": "#999999",              // gray
};

const SEGMENT_ORDER = ["In-Studio", "Digital", "Teacher Training", "Spa", "Rentals", "Merch", "Other"];

function AnnualSegmentBreakdownCard({ breakdown }: { breakdown: AnnualRevenueBreakdown[] }) {
  // Only show complete years (exclude current year since it's partial)
  const currentYear = new Date().getFullYear();
  const completeYears = breakdown.filter((b) => b.year < currentYear);
  const availableYears = completeYears.map((b) => b.year).sort((a, b) => b - a);

  const [selectedYear, setSelectedYear] = useState<number>(availableYears[0] || currentYear - 1);

  const yearData = breakdown.find((b) => b.year === selectedYear);
  if (!yearData || availableYears.length === 0) return null;

  // Sort segments by defined order
  const sorted = [...yearData.segments].sort((a, b) => {
    const ai = SEGMENT_ORDER.indexOf(a.segment);
    const bi = SEGMENT_ORDER.indexOf(b.segment);
    return (ai === -1 ? 99 : ai) - (bi === -1 ? 99 : bi);
  });

  return (
    <DashboardCard>
      <CardHeader>
        <div className="flex items-center justify-between">
          <div>
            <CardTitle>Annual Revenue by Segment</CardTitle>
            <CardDescription>
              {selectedYear} total: {formatCurrency(yearData.totalGross)} gross · {formatCurrency(yearData.totalNet)} net
            </CardDescription>
          </div>
          {availableYears.length > 1 && (
            <div className="flex gap-1">
              {availableYears.map((y) => (
                <Button
                  key={y}
                  variant={y === selectedYear ? "default" : "outline"}
                  size="sm"
                  onClick={() => setSelectedYear(y)}
                >
                  {y}
                </Button>
              ))}
            </div>
          )}
        </div>
      </CardHeader>
      <CardContent>
        <div className="flex flex-col gap-3">
          {/* Stacked bar */}
          <div className="flex h-4 rounded-full overflow-hidden">
            {sorted.map((seg) => {
              const pct = yearData.totalGross > 0 ? (seg.gross / yearData.totalGross) * 100 : 0;
              if (pct < 0.5) return null;
              return (
                <div
                  key={seg.segment}
                  className="h-full transition-all"
                  style={{
                    width: `${pct}%`,
                    backgroundColor: ANNUAL_SEGMENT_COLORS[seg.segment] || "#999",
                  }}
                />
              );
            })}
          </div>

          {/* Detail rows */}
          <div className="flex flex-col">
            {sorted.map((seg, i) => {
              const pct = yearData.totalGross > 0 ? Math.round((seg.gross / yearData.totalGross) * 1000) / 10 : 0;
              return (
                <div key={seg.segment} className={`flex items-center justify-between py-2.5 ${i < sorted.length - 1 ? "border-b border-border" : ""}`}>
                  <div className="flex items-center gap-2.5">
                    <div
                      className="size-3 rounded-full shrink-0"
                      style={{ backgroundColor: ANNUAL_SEGMENT_COLORS[seg.segment] || "#999" }}
                    />
                    <span className="text-sm font-medium">{seg.segment}</span>
                  </div>
                  <div className="flex items-center gap-4">
                    <span className="text-xs text-muted-foreground tabular-nums">{pct}%</span>
                    <span className="text-sm font-semibold tabular-nums w-24 text-right">{formatCurrency(seg.gross)}</span>
                    <span className="text-xs text-muted-foreground tabular-nums w-20 text-right hidden sm:block">net {formatCurrency(seg.net)}</span>
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      </CardContent>
    </DashboardCard>
  );
}

// ─── MRR Breakdown (standalone) ──────────────────────────────

function MRRBreakdown({ data }: { data: DashboardStats }) {
  const segments: { label: string; value: number; color: string; icon?: React.ComponentType<{ className?: string; style?: React.CSSProperties }> }[] = [
    { label: LABELS.members, value: data.mrr.member, color: COLORS.member, icon: ArrowBadgeDown },
    { label: LABELS.sky3, value: data.mrr.sky3, color: COLORS.sky3, icon: BrandSky },
    { label: LABELS.tv, value: data.mrr.skyTingTv, color: COLORS.tv, icon: DeviceTv },
    ...(data.mrr.unknown > 0 ? [{ label: "Other", value: data.mrr.unknown, color: "#999" }] : []),
  ].filter(s => s.value > 0);

  const now = new Date();
  const currentMonthLabel = now.toLocaleDateString("en-US", { month: "long", year: "numeric" });

  return (
    <DashboardCard>
      <CardHeader>
        <CardTitle>{LABELS.mrr}</CardTitle>
        <CardDescription className="text-2xl font-semibold tracking-tight tabular-nums text-foreground">
          {formatCurrency(data.mrr.total)}
        </CardDescription>
        <p className="text-xs text-muted-foreground">{currentMonthLabel}</p>
      </CardHeader>
      <CardContent>
        <div className="flex flex-col">
          {segments.map((seg, i) => (
            <div key={i} className={`flex justify-between items-center py-2.5 ${i < segments.length - 1 ? "border-b border-border" : ""}`}>
              <div className="flex items-center gap-2">
                {seg.icon ? (
                  <seg.icon className="size-4 shrink-0" style={{ color: seg.color }} />
                ) : (
                  <span className="w-2 h-2 rounded-full shrink-0" style={{ backgroundColor: seg.color, opacity: 0.85 }} />
                )}
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

// ─── New Customer KPI Cards (3-across) ──────────────────────

function NewCustomerKPICards({ volume, cohorts }: {
  volume: NewCustomerVolumeData | null;
  cohorts: NewCustomerCohortData | null;
}) {
  if (!volume && !cohorts) return null;
  const { completeCohorts, avgRate, currentCohortCount, expectedAutoRenews, convTooltip, projTooltip } = useNewCustomerData(volume, cohorts);

  return (
    <div className="grid grid-cols-1 sm:grid-cols-3 gap-3">
      <DashboardCard>
        <CardHeader className="pb-2">
          <CardDescription>New Customers</CardDescription>
          <CardTitle className="text-2xl font-semibold tabular-nums">
            {currentCohortCount != null ? formatNumber(currentCohortCount) : "\u2014"}
          </CardTitle>
        </CardHeader>
        <CardFooter className="text-sm text-muted-foreground">
          Current Week Cohort
        </CardFooter>
      </DashboardCard>

      <DashboardCard>
        <CardHeader className="pb-2">
          <CardDescription className="flex items-center gap-1">
            3-Week Conv. Rate
            {convTooltip && <InfoTooltip tooltip={convTooltip} />}
          </CardDescription>
          <CardTitle className="text-2xl font-semibold tabular-nums">
            {avgRate != null ? `${avgRate.toFixed(1)}%` : "\u2014"}
          </CardTitle>
        </CardHeader>
        <CardFooter className="text-sm text-muted-foreground">
          {completeCohorts.length} Complete Cohort{completeCohorts.length !== 1 ? "s" : ""}
        </CardFooter>
      </DashboardCard>

      <DashboardCard>
        <CardHeader className="pb-2">
          <CardDescription className="flex items-center gap-1">
            Expected Converts
            {projTooltip && <InfoTooltip tooltip={projTooltip} />}
          </CardDescription>
          <CardTitle className="text-2xl font-semibold tabular-nums">
            {expectedAutoRenews != null ? formatNumber(expectedAutoRenews) : "\u2014"}
          </CardTitle>
        </CardHeader>
        <CardFooter className="text-sm text-muted-foreground">
          Based on Avg Conversion Rate
        </CardFooter>
      </DashboardCard>
    </div>
  );
}

// ─── New Customer Chart Card (line chart + cohort tables) ───

function NewCustomerChartCard({ volume, cohorts }: {
  volume: NewCustomerVolumeData | null;
  cohorts: NewCustomerCohortData | null;
}) {
  const isMobile = useIsMobile();
  if (!volume && !cohorts) return null;

  const [expandedRow, setExpandedRow] = useState<string | null>(null);
  const [hoveredCohort, setHoveredCohort] = useState<string | null>(null);

  const { completeCohorts, incompleteCohorts, displayComplete, daysElapsed } = useNewCustomerData(volume, cohorts);
  const mostRecentComplete = displayComplete.length > 0 ? displayComplete[displayComplete.length - 1].cohortStart : null;

  const lineData = completeCohorts.slice(-8).map((c) => ({ date: formatWeekShort(c.cohortStart), newCustomers: c.newCustomers, converts: c.total3Week }));

  return (
    <DashboardCard matchHeight className="@container/card">
      <CardHeader>
        <CardTitle>Weekly New Customers</CardTitle>
        <CardDescription>Complete Weeks and Cohort Breakdown</CardDescription>
      </CardHeader>

      <CardContent className="space-y-6">
        <ChartContainer config={{
          newCustomers: { label: "New Customers", color: COLORS.newCustomer },
          converts: { label: "Converts", color: "hsl(150, 45%, 42%)" },
        } satisfies ChartConfig} className="h-[250px] w-full">
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
            <CartesianGrid vertical={false} />
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
              stroke="var(--color-newCustomers)"
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
              stroke="var(--color-converts)"
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
            <ChartLegend content={<ChartLegendContent />} />
          </RAreaChart>
        </ChartContainer>

      </CardContent>

      {/* Cohort table — separated by border-t, not nested card */}
      {cohorts && (
        <div className="border-t">
          <table className="w-full caption-bottom text-sm" style={{ fontFamily: FONT_SANS }}>
            <thead className="bg-muted [&_tr]:border-b">
              <tr>
                <th className={`${modThClass} !text-left`}>Week</th>
                <th className={modThClass} style={{ width: "3.5rem" }}>New</th>
                <th className={modThClass} style={{ width: "4.5rem" }}>Converts</th>
                <th className={modThClass} style={{ width: "3.5rem" }}>Rate</th>
              </tr>
            </thead>
            <tbody className="[&_tr:last-child]:border-0">
              {/* Complete cohorts */}
              {displayComplete.map((c) => {
                const rate = c.newCustomers > 0 ? (c.total3Week / c.newCustomers * 100).toFixed(1) : "0.0";
                const isHovered = hoveredCohort === c.cohortStart;
                const isExpanded = expandedRow === c.cohortStart;
                const isNewest = c.cohortStart === mostRecentComplete;
                return (
                  <Fragment key={c.cohortStart}>
                    <tr
                      className="border-b transition-colors cursor-pointer hover:bg-muted/50"
                      style={{
                        borderLeft: isHovered ? `2px solid ${COLORS.newCustomer}` : "2px solid transparent",
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
                    </tr>
                    {isExpanded && (
                      <tr className="border-b" style={{ borderLeft: `2px solid ${COLORS.newCustomer}` }}>
                        <td colSpan={4} className="px-4 py-2 pl-8 bg-muted/30">
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

              {/* In-progress cohorts — muted gray styling */}
              {incompleteCohorts.length > 0 && (
                <tr>
                  <td colSpan={4} className="px-4 pt-3 pb-1">
                    <span className="text-[10px] uppercase tracking-wider font-medium text-muted-foreground/60">In progress</span>
                  </td>
                </tr>
              )}
              {incompleteCohorts.map((c) => {
                const days = daysElapsed(c.cohortStart);
                const wk2Possible = days >= 7;
                const isHovered = hoveredCohort === c.cohortStart;
                const isExpanded = expandedRow === c.cohortStart;
                const daysRemaining = Math.max(21 - days, 0);
                const convertsSoFar = c.week1 + (wk2Possible ? c.week2 : 0);
                const partialRate = c.newCustomers > 0 ? (convertsSoFar / c.newCustomers * 100).toFixed(1) : "0.0";
                return (
                  <Fragment key={c.cohortStart}>
                    <tr
                      className="border-b transition-colors cursor-pointer text-muted-foreground hover:bg-muted/50"
                      style={{
                        borderLeft: isHovered ? `2px solid ${COLORS.newCustomer}` : "2px solid transparent",
                      }}
                      onMouseEnter={() => setHoveredCohort(c.cohortStart)}
                      onMouseLeave={() => setHoveredCohort(null)}
                      onClick={() => setExpandedRow(isExpanded ? null : c.cohortStart)}
                    >
                      <td className={`${modTdClass} !text-left font-medium`}>
                        {formatWeekRangeLabel(c.cohortStart, c.cohortEnd)}
                        <span className="ml-1.5 text-[10px] text-muted-foreground/50">{daysRemaining}d left</span>
                      </td>
                      <td data-label="New" className={`${modTdClass} font-semibold`}>{formatNumber(c.newCustomers)}</td>
                      <td data-label="Converts" className={`${modTdClass} font-semibold`}>{convertsSoFar}</td>
                      <td data-label="Rate" className={`${modTdClass} font-medium`}>{partialRate}%</td>
                    </tr>
                    {isExpanded && (
                      <tr className="border-b" style={{ borderLeft: `2px solid ${COLORS.newCustomer}` }}>
                        <td colSpan={4} className="px-4 py-2 pl-8 bg-muted/30">
                          <div className="flex gap-4 text-xs text-muted-foreground tabular-nums">
                            <span>Same week: <strong className="font-medium">{c.week1}</strong></span>
                            <span>+1 week: <strong className="font-medium">{wk2Possible ? c.week2 : "\u2014"}</strong></span>
                            <span>+2 weeks: <strong className="font-medium">{"\u2014"}</strong></span>
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
        <div className="flex items-center justify-between w-full">
          <div>
            <CardTitle>Customers By Week</CardTitle>
            <CardDescription>New Intro Week Customers by Week</CardDescription>
          </div>
          <Button
            variant="outline"
            size="icon"
            onClick={() => { window.location.href = "/api/intro-week-export"; }}
            title="Download active Intro Week customers as CSV"
          >
            <DownloadIcon className="size-4" />
          </Button>
        </div>
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
            Intro Week Signups
          </div>
        </CardFooter>
      )}
    </DashboardCard>
  );
}

// ─── Expired Intro Weeks Card (vertical bar chart) ──────────

const expiredIntroChartConfig = {
  converted: { label: "Converted", color: COLORS.success },
  notConverted: { label: "Did not convert", color: COLORS.error },
} satisfies ChartConfig;

function ExpiredIntroWeeksCard({ data }: { data: IntroWeekConversionData }) {
  const { totalExpired, converted, notConverted } = data;

  const convertedPct = totalExpired > 0 ? Math.round((converted / totalExpired) * 100) : 0;
  const notConvertedPct = totalExpired > 0 ? Math.round((notConverted / totalExpired) * 100) : 0;

  const chartData = [
    { label: "Converted", value: converted, pct: convertedPct, fill: "var(--color-converted)" },
    { label: "Did not convert", value: notConverted, pct: notConvertedPct, fill: "var(--color-notConverted)" },
  ];

  return (
    <DashboardCard matchHeight>
      <CardHeader>
        <div className="flex items-center justify-between w-full">
          <div>
            <CardTitle>Expired Intro Weeks</CardTitle>
            <CardDescription>Intro week passes that expired in the last 14 days</CardDescription>
          </div>
          {notConverted > 0 && (
            <Button
              variant="outline"
              size="icon"
              onClick={() => { window.location.href = "/api/intro-week-nonconvert-export"; }}
              title="Download non-converters as CSV"
            >
              <DownloadIcon className="size-4" />
            </Button>
          )}
        </div>
      </CardHeader>
      <CardContent>
        <ChartContainer config={expiredIntroChartConfig}>
          <BarChart accessibilityLayer data={chartData} margin={{ top: 28, left: 0, right: 0, bottom: 0 }}>
            <CartesianGrid vertical={false} />
            <XAxis
              dataKey="label"
              tickLine={false}
              tickMargin={10}
              axisLine={false}
            />
            <Bar dataKey="value" radius={8}>
              <LabelList dataKey="value" position="top" fontSize={12} fontWeight={600} formatter={(v: number) => {
                const d = chartData.find((c) => c.value === v);
                return d ? `${d.value} (${d.pct}%)` : `${v}`;
              }} />
            </Bar>
          </BarChart>
        </ChartContainer>
      </CardContent>
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
      <SectionHeader subtitle="Drop-ins, Intro Week, and Other Non-Subscribed Activity">{LABELS.nonAutoRenew}</SectionHeader>

      {/* ── Subsection A: Drop-ins ── */}
      {dropIns && (
        <div className="flex flex-col gap-3">
          <h3 className="text-[15px] font-semibold tracking-tight text-muted-foreground mb-3">Drop-ins</h3>
          <DropInsSubsection dropIns={dropIns} />
        </div>
      )}

      {/* ── Subsection B: Other pass types ── */}
      <div className="flex flex-col gap-3">
        <h3 className="text-[15px] font-semibold tracking-tight text-muted-foreground mb-3">Intro Week</h3>
        <IntroWeekModule introWeek={introWeek} />
      </div>

      {/* ── Subsection C: Conversion ── */}
      <div className="flex flex-col gap-3">
        <h3 className="text-[15px] font-semibold tracking-tight text-muted-foreground mb-3">Conversion</h3>
        {(newCustomerVolume || newCustomerCohorts) && (
          <p className="text-[15px] font-semibold text-muted-foreground">New Customers</p>
        )}
        {(newCustomerVolume || newCustomerCohorts) && (
          <NewCustomerKPICards volume={newCustomerVolume} cohorts={newCustomerCohorts} />
        )}
        {(newCustomerVolume || newCustomerCohorts) && (
          <NewCustomerChartCard volume={newCustomerVolume} cohorts={newCustomerCohorts} />
        )}
        {conversionPool && (
          <p className="text-[15px] font-semibold text-muted-foreground">Non Auto-Renew Customers</p>
        )}
        {conversionPool && (
          <ConversionPoolKPICards pool={conversionPool} />
        )}
        {conversionPool && (
          <ConversionPoolModule pool={conversionPool} />
        )}
        {conversionPool && (
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
            <ConversionTimeCard pool={conversionPool} />
            <ConversionVisitsCard pool={conversionPool} />
          </div>
        )}
      </div>
    </div>
  );
}

// ─── Drop-Ins Subsection (3-card layout: Overview + Frequency | Distribution) ──

function DropInsSubsection({ dropIns }: { dropIns: DropInModuleData }) {
  const isMobile = useIsMobile();
  const [hoveredWeek, setHoveredWeek] = useState<string | null>(null);

  const { completeWeeks, wtd, lastCompleteWeek, typicalWeekVisits, trend, trendDeltaPercent, wtdDelta, wtdDeltaPercent, wtdDayLabel, frequency } = dropIns;
  const displayWeeks = completeWeeks.slice(-8);

  // Frequency breakdown data
  const freqBuckets = frequency ? [
    { label: "1 visit", count: frequency.bucket1, color: "rgba(155, 118, 83, 0.30)" },
    { label: "2\u20134 visits", count: frequency.bucket2to4, color: "rgba(155, 118, 83, 0.50)" },
    { label: "5\u201310 visits", count: frequency.bucket5to10, color: "rgba(155, 118, 83, 0.70)" },
    { label: "11+ visits", count: frequency.bucket11plus, color: "rgba(155, 118, 83, 0.92)" },
  ].filter(d => d.count > 0) : [];
  const freqMax = Math.max(...freqBuckets.map(b => b.count), 1);

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
            {wtdDayLabel ? `Through ${wtdDayLabel}` : "Week to Date"}
          </CardFooter>
        </DashboardCard>

        <DashboardCard>
          <CardHeader>
            <CardDescription>Typical Week</CardDescription>
            <CardTitle className="text-2xl font-semibold tabular-nums">{formatNumber(typicalWeekVisits)}</CardTitle>
          </CardHeader>
          <CardFooter className="text-sm text-muted-foreground">
            4-Week Average
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
            vs Prior 4 Weeks
          </CardFooter>
        </DashboardCard>
      </div>

      {/* ── Row 2: Weekly Drop-ins — chart + table (WTD inline as gray row) ── */}
      <DashboardCard>
        <CardHeader>
          <CardTitle>Weekly Drop-ins</CardTitle>
          <CardDescription>Complete Weeks{wtd ? " and Week-to-Date" : ""}</CardDescription>
        </CardHeader>

        <CardContent className="space-y-6">
          {/* Bar chart */}
          <ChartContainer config={{ visits: { label: "Visits", color: COLORS.dropIn } } satisfies ChartConfig} className="h-[200px] w-full">
            <BarChart
              accessibilityLayer
              data={displayWeeks.map((w) => ({ date: formatWeekShort(w.weekStart), visits: w.visits }))}
              margin={{ top: 20 }}
            >
              <CartesianGrid vertical={false} />
              <XAxis dataKey="date" tickLine={false} tickMargin={10} axisLine={false} />
              <YAxis hide />
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

        </CardContent>

        {/* Table — separated by border-t, not nested card */}
        <div className="border-t">
          <table className="w-full caption-bottom text-sm" style={{ fontFamily: FONT_SANS }}>
            <thead className="bg-muted [&_tr]:border-b">
              <tr>
                <th className={`${modThClass} !text-left`}>Week</th>
                <th className={modThClass}>Visits</th>
                <th className={modThClass}>Customers</th>
                <th className={modThClass}>First %</th>
              </tr>
            </thead>
            <tbody className="[&_tr:last-child]:border-0">
              {displayWeeks.map((w) => {
                const isHovered = hoveredWeek === w.weekStart;
                const isLatest = w.weekStart === displayWeeks[displayWeeks.length - 1]?.weekStart;
                const firstPct = w.uniqueCustomers > 0 ? Math.round((w.firstTime / w.uniqueCustomers) * 100) : 0;
                return (
                  <tr
                    key={w.weekStart}
                    className="border-b transition-colors hover:bg-muted/50"
                    style={{
                      borderLeft: isHovered ? `2px solid ${COLORS.dropIn}` : "2px solid transparent",
                    }}
                    onMouseEnter={() => setHoveredWeek(w.weekStart)}
                    onMouseLeave={() => setHoveredWeek(null)}
                  >
                    <td className={`${modTdClass} !text-left font-medium`}>
                      {formatWeekRangeLabel(w.weekStart, w.weekEnd)}
                      {isLatest && <> <span className="text-[10px] bg-muted text-muted-foreground rounded-full px-2 py-0.5 ml-1">Latest</span></>}
                    </td>
                    <td className={`${modTdClass} font-semibold`}>{formatNumber(w.visits)}</td>
                    <td className={`${modTdClass} font-medium`}>{formatNumber(w.uniqueCustomers)}</td>
                    <td className={`${modTdClass} text-muted-foreground`}>{firstPct}%</td>
                  </tr>
                );
              })}
              {/* WTD row — inline as a muted/gray row */}
              {wtd && (
                <tr className="border-b bg-muted/50">
                  <td className={`${modTdClass} !text-left font-medium text-muted-foreground`}>
                    {formatWeekRangeLabel(wtd.weekStart, wtd.weekEnd)}
                    {" "}<span className="text-[10px] bg-muted text-muted-foreground rounded-full px-2 py-0.5 ml-1">WTD</span>
                  </td>
                  <td className={`${modTdClass} font-semibold text-muted-foreground`}>{formatNumber(wtd.visits)}</td>
                  <td className={`${modTdClass} font-medium text-muted-foreground`}>{formatNumber(wtd.uniqueCustomers)}</td>
                  <td className={`${modTdClass} text-muted-foreground`}>
                    {wtd.uniqueCustomers > 0 ? `${Math.round((wtd.firstTime / wtd.uniqueCustomers) * 100)}%` : "\u2014"}
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </DashboardCard>

      {/* ── Row 3: Drop-in Frequency (horizontal bars) ── */}
      <div className="grid grid-cols-1 sm:grid-cols-2 gap-3" style={{ alignItems: "stretch" }}>
        <DashboardCard matchHeight>
          <CardHeader>
            <div className="flex items-center gap-1.5">
              <CardTitle>Drop-in Frequency</CardTitle>
              <InfoTooltip tooltip="Distribution of unique drop-in customers by visit count over the last 90 days." />
            </div>
            <CardDescription>Last 90 Days{frequency ? ` · ${formatNumber(frequency.totalCustomers)} Unique Visitors` : ""}</CardDescription>
          </CardHeader>

          <CardContent className="flex-1">
            {frequency && frequency.totalCustomers > 0 && freqBuckets.length > 0 ? (
              <div className="flex flex-col gap-3">
                {freqBuckets.map((b) => {
                  const pct = Math.round((b.count / frequency.totalCustomers) * 100);
                  const barWidth = Math.max((b.count / freqMax) * 100, 2);
                  return (
                    <div key={b.label} className="flex flex-col gap-1">
                      <div className="flex items-baseline justify-between text-sm">
                        <span className="font-medium">{b.label}</span>
                        <span className="tabular-nums text-muted-foreground">
                          {formatNumber(b.count)} <span className="text-xs">({pct}%)</span>
                        </span>
                      </div>
                      <div className="h-5 w-full rounded bg-muted/40 overflow-hidden">
                        <div
                          className="h-full rounded transition-all"
                          style={{ width: `${barWidth}%`, backgroundColor: b.color }}
                        />
                      </div>
                    </div>
                  );
                })}
              </div>
            ) : (
              <NoDataInline />
            )}
          </CardContent>
        </DashboardCard>
      </div>
    </div>
  );
}

// ─── Conversion Pool Module (non-auto → auto-renew) ─────────

function ConversionPoolKPICards({ pool }: { pool: ConversionPoolModuleData }) {
  const data: ConversionPoolSliceData | null = pool.slices.all ?? null;
  if (!data) return null;

  const { wtd, lastCompleteWeek, avgRate } = data;
  const heroPool = wtd?.activePool7d || lastCompleteWeek?.activePool7d || 0;
  const heroConverts = wtd?.converts || lastCompleteWeek?.converts || 0;
  const heroRate = wtd?.conversionRate || lastCompleteWeek?.conversionRate || 0;

  return (
    <div className="grid grid-cols-1 sm:grid-cols-3 gap-3">
      <DashboardCard>
        <CardHeader className="pb-2">
          <CardDescription>Total Customers</CardDescription>
          <CardTitle className="text-2xl font-semibold tabular-nums">
            {formatNumber(heroPool)}
          </CardTitle>
        </CardHeader>
        <CardFooter className="text-sm text-muted-foreground">
          Active Pool (7d)
        </CardFooter>
      </DashboardCard>

      <DashboardCard>
        <CardHeader className="pb-2">
          <CardDescription className="flex items-center gap-1">
            3-Week Conv. Rate
            <InfoTooltip tooltip="Converts / Active pool (7d). 3-week window." />
          </CardDescription>
          <CardTitle className="text-2xl font-semibold tabular-nums">
            {heroRate.toFixed(1)}%
          </CardTitle>
        </CardHeader>
        <CardFooter className="text-sm text-muted-foreground">
          {avgRate > 0 ? `${avgRate.toFixed(1)}% avg` : "Calculating"}
        </CardFooter>
      </DashboardCard>

      <DashboardCard>
        <CardHeader className="pb-2">
          <CardDescription className="flex items-center gap-1">
            Converts
            <InfoTooltip tooltip="Pool members who started their first in-studio auto-renew this week." />
          </CardDescription>
          <CardTitle className="text-2xl font-semibold tabular-nums">
            {formatNumber(heroConverts)}
          </CardTitle>
        </CardHeader>
        <CardFooter className="text-sm text-muted-foreground">
          This Week
        </CardFooter>
      </DashboardCard>
    </div>
  );
}

// ─── Conversion Time Horizontal Bar Card ─────────────────────

function ConversionTimeCard({ pool }: { pool: ConversionPoolModuleData }) {
  const data: ConversionPoolSliceData | null = pool.slices.all ?? null;
  if (!data?.lagStats) return null;
  const { lagStats } = data;

  const barColors = ["hsl(270, 15%, 42%)", "hsl(270, 15%, 58%)", "hsl(270, 15%, 72%)", "hsl(270, 15%, 84%)"];
  const barData = [
    { bucket: "0–30d", count: lagStats.timeBucket0to30, fill: barColors[0] },
    { bucket: "31–90d", count: lagStats.timeBucket31to90, fill: barColors[1] },
    { bucket: "91–180d", count: lagStats.timeBucket91to180, fill: barColors[2] },
    { bucket: "180d+", count: lagStats.timeBucket180plus, fill: barColors[3] },
  ].filter((d) => d.count > 0);

  const barConfig = Object.fromEntries(barData.map((d) => [d.bucket, { label: d.bucket, color: d.fill }])) as ChartConfig;

  return (
    <DashboardCard>
      <CardHeader>
        <CardTitle>Time to Convert</CardTitle>
        <CardDescription>
          Median: {lagStats.historicalMedianTimeToConvert != null ? `${lagStats.historicalMedianTimeToConvert}d` : "\u2014"} (last 12 weeks)
        </CardDescription>
      </CardHeader>
      <CardContent>
        {barData.length > 0 ? (
          <ChartContainer config={barConfig} className="h-[200px] w-full">
            <BarChart
              accessibilityLayer
              data={barData}
              layout="vertical"
              margin={{ left: 0, right: 40 }}
            >
              <CartesianGrid horizontal={false} />
              <YAxis
                dataKey="bucket"
                type="category"
                tickLine={false}
                axisLine={false}
                width={70}
                tick={{ fontSize: 12 }}
              />
              <XAxis type="number" hide />
              <ChartTooltip cursor={false} content={<ChartTooltipContent hideLabel />} />
              <Bar dataKey="count" radius={8}>
                <LabelList
                  position="right"
                  offset={8}
                  className="fill-foreground"
                  fontSize={12}
                  fontWeight={600}
                />
              </Bar>
            </BarChart>
          </ChartContainer>
        ) : (
          <NoDataInline />
        )}
      </CardContent>
      <CardFooter className="text-sm text-muted-foreground">
        Days from First Visit to Auto-Renew Subscription
      </CardFooter>
    </DashboardCard>
  );
}

// ─── Conversion Visits Horizontal Bar Card ───────────────────

function ConversionVisitsCard({ pool }: { pool: ConversionPoolModuleData }) {
  const data: ConversionPoolSliceData | null = pool.slices.all ?? null;
  if (!data?.lagStats) return null;
  const { lagStats } = data;

  const barColors = ["hsl(270, 15%, 42%)", "hsl(270, 15%, 58%)", "hsl(270, 15%, 72%)", "hsl(270, 15%, 84%)"];
  const barData = [
    { bucket: "1–2", count: lagStats.visitBucket1to2, fill: barColors[0] },
    { bucket: "3–5", count: lagStats.visitBucket3to5, fill: barColors[1] },
    { bucket: "6–10", count: lagStats.visitBucket6to10, fill: barColors[2] },
    { bucket: "11+", count: lagStats.visitBucket11plus, fill: barColors[3] },
  ].filter((d) => d.count > 0);

  const barConfig = Object.fromEntries(barData.map((d) => [d.bucket, { label: d.bucket + " visits", color: d.fill }])) as ChartConfig;

  return (
    <DashboardCard>
      <CardHeader>
        <CardTitle>Visits Before Convert</CardTitle>
        <CardDescription>
          Avg: {lagStats.historicalAvgVisitsBeforeConvert != null ? lagStats.historicalAvgVisitsBeforeConvert.toFixed(1) : "\u2014"} (last 12 weeks)
        </CardDescription>
      </CardHeader>
      <CardContent>
        {barData.length > 0 ? (
          <ChartContainer config={barConfig} className="h-[200px] w-full">
            <BarChart
              accessibilityLayer
              data={barData}
              layout="vertical"
              margin={{ left: 0, right: 40 }}
            >
              <CartesianGrid horizontal={false} />
              <YAxis
                dataKey="bucket"
                type="category"
                tickLine={false}
                axisLine={false}
                width={50}
                tick={{ fontSize: 12 }}
              />
              <XAxis type="number" hide />
              <ChartTooltip cursor={false} content={<ChartTooltipContent hideLabel />} />
              <Bar dataKey="count" radius={8}>
                <LabelList
                  position="right"
                  offset={8}
                  className="fill-foreground"
                  fontSize={12}
                  fontWeight={600}
                />
              </Bar>
            </BarChart>
          </ChartContainer>
        ) : (
          <NoDataInline />
        )}
      </CardContent>
      <CardFooter className="text-sm text-muted-foreground">
        Total Drop-in Visits Before Subscribing
      </CardFooter>
    </DashboardCard>
  );
}

// ─── Conversion Pool Chart Card (area chart + tables) ───────

function ConversionPoolModule({ pool }: { pool: ConversionPoolModuleData }) {
  const isMobile = useIsMobile();
  const [hoveredWeek, setHoveredWeek] = useState<string | null>(null);
  const [activeSlice, setActiveSlice] = useState<ConversionPoolSlice>("all");


  const data: ConversionPoolSliceData | null = pool.slices[activeSlice] ?? pool.slices.all ?? null;
  if (!data) return null;

  const { completeWeeks, wtd } = data;
  const displayWeeks = completeWeeks.slice(-8);

  const sliceOptions: { key: ConversionPoolSlice; label: string }[] = [
    { key: "all", label: "All" },
    { key: "drop-ins", label: "Drop-ins" },
    { key: "intro-week", label: "Intro Week" },
    { key: "class-packs", label: "Class Packs" },
    { key: "high-intent", label: "High Intent" },
  ];

  const chartData = displayWeeks.map((w) => ({
    date: formatWeekShort(w.weekStart),
    pool: w.activePool7d,
    converts: w.converts,
  }));

  return (
    <DashboardCard className="@container/card">
      <CardHeader>
        <div className="flex flex-row items-center justify-between w-full">
          <div>
            <CardTitle>Weekly Non Auto-Renew Customers</CardTitle>
            <CardDescription>Complete Weeks{wtd ? " and Week-to-Date" : ""}</CardDescription>
          </div>
          <Select value={activeSlice} onValueChange={(v) => setActiveSlice(v as ConversionPoolSlice)}>
            <SelectTrigger size="sm" className="h-7 w-auto text-xs font-medium border-border bg-muted text-muted-foreground shadow-none">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {sliceOptions.map((o) => (
                <SelectItem key={o.key} value={o.key} disabled={!pool.slices[o.key]}>{o.label}</SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
      </CardHeader>

      <CardContent className="space-y-6">
        <ChartContainer config={{
          pool: { label: "Pool", color: COLORS.conversionPool },
          converts: { label: "Converts", color: "hsl(150, 45%, 42%)" },
        } satisfies ChartConfig} className="h-[250px] w-full">
          <RAreaChart accessibilityLayer data={chartData} margin={{ top: 20, left: 12, right: 12 }}>
            <defs>
              <linearGradient id="fillPool" x1="0" y1="0" x2="0" y2="1">
                <stop offset="5%" stopColor="var(--color-pool)" stopOpacity={0.8} />
                <stop offset="95%" stopColor="var(--color-pool)" stopOpacity={0.1} />
              </linearGradient>
              <linearGradient id="fillPoolConverts" x1="0" y1="0" x2="0" y2="1">
                <stop offset="5%" stopColor="var(--color-converts)" stopOpacity={0.8} />
                <stop offset="95%" stopColor="var(--color-converts)" stopOpacity={0.1} />
              </linearGradient>
            </defs>
            <CartesianGrid vertical={false} />
            <XAxis
              dataKey="date"
              tickLine={false}
              axisLine={false}
              tickMargin={8}
            />
            <Area
              dataKey="pool"
              type="natural"
              fill="url(#fillPool)"
              stroke="var(--color-pool)"
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
              fill="url(#fillPoolConverts)"
              stroke="var(--color-converts)"
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
            <ChartLegend content={<ChartLegendContent />} />
          </RAreaChart>
        </ChartContainer>
      </CardContent>

      {/* Table — separated by border-t */}
      <div className="border-t">
        <table className="w-full caption-bottom text-sm" style={{ fontFamily: FONT_SANS }}>
          <thead className="bg-muted [&_tr]:border-b">
            <tr>
              <th className={`${modThClass} !text-left`}>Week</th>
              <th className={modThClass}>Pool</th>
              <th className={modThClass}>Converts</th>
              <th className={modThClass}>Rate</th>
            </tr>
          </thead>
          <tbody className="[&_tr:last-child]:border-0">
            {displayWeeks.map((w, idx) => {
              const isHovered = hoveredWeek === w.weekStart;
              const isLatest = idx === displayWeeks.length - 1;
              return (
                <tr
                  key={w.weekStart}
                  className="border-b transition-colors hover:bg-muted/50"
                  onMouseEnter={() => setHoveredWeek(w.weekStart)}
                  onMouseLeave={() => setHoveredWeek(null)}
                  style={{
                    borderLeft: isHovered ? `2px solid ${COLORS.conversionPool}` : "2px solid transparent",
                  }}
                >
                  <td className={`${modTdClass} !text-left font-medium`}>
                    {formatWeekRangeLabel(w.weekStart, w.weekEnd)}
                    {isLatest && <> <span className="text-[10px] bg-muted text-muted-foreground rounded-full px-2 py-0.5 ml-1">Latest</span></>}
                  </td>
                  <td className={`${modTdClass} font-semibold`}>{formatNumber(w.activePool7d)}</td>
                  <td className={`${modTdClass} font-semibold`}>{formatNumber(w.converts)}</td>
                  <td className={`${modTdClass} text-muted-foreground`}>{w.conversionRate.toFixed(1)}%</td>
                </tr>
              );
            })}
            {/* WTD row — grayed out */}
            {wtd && (
              <tr className="border-b bg-muted/50">
                <td className={`${modTdClass} !text-left font-medium text-muted-foreground`}>
                  {formatWeekRangeLabel(wtd.weekStart, wtd.weekEnd)}
                  {" "}<span className="text-[10px] bg-muted text-muted-foreground rounded-full px-2 py-0.5 ml-1">WTD</span>
                </td>
                <td className={`${modTdClass} font-semibold text-muted-foreground`}>{formatNumber(wtd.activePool7d)}</td>
                <td className={`${modTdClass} font-semibold text-muted-foreground`}>{formatNumber(wtd.converts)}</td>
                <td className={`${modTdClass} text-muted-foreground`}>{wtd.conversionRate.toFixed(1)}%</td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </DashboardCard>
  );
}

// ─── Category Detail Card (Members / SKY3 / TV) ─────────────
// Clean card: big count, simple metric rows, no chart clutter

function CategoryDetail({ title, color, icon: Icon, count, weekly, monthly, pacing, weeklyKeyNew, weeklyKeyChurn, weeklyKeyNet, pacingNew, pacingChurn, churnData }: {
  title: string;
  color: string;
  icon?: React.ComponentType<{ className?: string; style?: React.CSSProperties }>;
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

  // ── Bar chart: new adds per week + total count label for last 4 completed weeks ──
  const last4 = completedWeekly.slice(-4);
  const currentWeekInProgress = weekly.length > 1 ? weekly[weekly.length - 1] : null;
  const currentWeekNet = currentWeekInProgress ? weeklyKeyNet(currentWeekInProgress) : 0;
  const endOfLastCompleted = count - currentWeekNet;
  const barData: { week: string; newAdds: number; total: number }[] = [];
  {
    let runningTotal = endOfLastCompleted;
    const reversed: { week: string; newAdds: number; total: number }[] = [];
    for (let i = last4.length - 1; i >= 0; i--) {
      reversed.push({
        week: formatWeekShort(last4[i].period),
        newAdds: weeklyKeyNew(last4[i]),
        total: runningTotal,
      });
      if (i > 0) {
        runningTotal -= weeklyKeyNet(last4[i]);
      }
    }
    reversed.reverse();
    barData.push(...reversed);
  }

  // Build metric rows
  // Columnar rows: label | count | change # | change %
  // "isNetChange" rows only show label + value (no delta columns)
  const metrics: { label: string; value: string; priorValue?: string | null; color?: string }[] = [];

  if (latestW) {
    const newVal = weeklyKeyNew(latestW);
    const prevNewVal = prevW ? weeklyKeyNew(prevW) : null;
    metrics.push({ label: "New", value: `+${newVal}`, priorValue: prevNewVal != null ? `+${prevNewVal}` : null, color: COLORS.success });

    const churnVal = weeklyKeyChurn(latestW);
    const prevChurnVal = prevW ? weeklyKeyChurn(prevW) : null;
    metrics.push({ label: "Churned", value: `-${churnVal}`, priorValue: prevChurnVal != null ? `-${prevChurnVal}` : null, color: COLORS.error });

    const netVal = weeklyKeyNet(latestW);
    const prevNetVal = prevW ? weeklyKeyNet(prevW) : null;
    metrics.push({
      label: "Net Change",
      value: formatDelta(netVal) || "0",
      priorValue: prevNetVal != null ? (formatDelta(prevNetVal) || "0") : null,
      color: netVal > 0 ? COLORS.success : netVal < 0 ? COLORS.error : undefined,
    });
  }

  if (churnData) {
    metrics.push({
      label: "User Churn Rate (Avg/Mo)",
      value: `${churnData.avgUserChurnRate.toFixed(1)}%`,
      color: churnBenchmarkColor(churnData.avgUserChurnRate),
    });
    if (churnData.avgEligibleChurnRate != null) {
      metrics.push({
        label: "Eligible Churn Rate (Avg/Mo)",
        value: `${churnData.avgEligibleChurnRate.toFixed(1)}%`,
        color: churnBenchmarkColor(churnData.avgEligibleChurnRate),
      });
    }
    metrics.push({
      label: "MRR Churn Rate (Avg/Mo)",
      value: `${churnData.avgMrrChurnRate.toFixed(1)}%`,
      color: churnBenchmarkColor(churnData.avgMrrChurnRate),
    });
    if (churnData.atRiskCount > 0) {
      metrics.push({
        label: "At Risk",
        value: String(churnData.atRiskCount),
        color: COLORS.warning,
      });
    }
  }

  // Net change for footer
  const netVal = latestW ? weeklyKeyNet(latestW) : 0;
  const netLabel = netVal > 0 ? `+${netVal}` : netVal < 0 ? String(netVal) : "0";
  const netColor = netVal > 0 ? "text-emerald-600" : netVal < 0 ? "text-red-500" : "text-muted-foreground";
  const wkLabel = latestW ? weekLabel(latestW.period) : "";

  // Churn note for MEMBER footer
  const churnNote = churnData?.category === "MEMBER" ? (() => {
    const lastCompleted = churnData.monthly.length >= 2 ? churnData.monthly[churnData.monthly.length - 2] : null;
    if (!lastCompleted || !lastCompleted.annualActiveAtStart) return null;
    return `Annual: ${lastCompleted.annualCanceledCount}/${lastCompleted.annualActiveAtStart} churned · Monthly: ${lastCompleted.monthlyCanceledCount}/${lastCompleted.monthlyActiveAtStart} churned`;
  })() : null;

  return (
    <DashboardCard>
      <CardHeader>
        <div className="flex items-center gap-2">
          {Icon ? (
            <Icon className="size-4 shrink-0" style={{ color }} />
          ) : (
            <span className="w-2.5 h-2.5 rounded-full shrink-0" style={{ backgroundColor: color, opacity: 0.85 }} />
          )}
          <CardTitle>{title}</CardTitle>
        </div>
        <CardDescription>New Adds per Week</CardDescription>
        <CardAction>
          <span className="text-2xl font-semibold tracking-tight tabular-nums">{formatNumber(count)}</span>
        </CardAction>
      </CardHeader>

      <CardContent>
        {barData.length > 0 && (
          <ChartContainer config={{ newAdds: { label: "New Adds", color } }} className="h-[200px] w-full">
            <BarChart
              accessibilityLayer
              data={barData}
              margin={{ top: 20 }}
            >
              <CartesianGrid vertical={false} />
              <XAxis
                dataKey="week"
                tickLine={false}
                tickMargin={10}
                axisLine={false}
              />
              <YAxis hide />
              <Bar dataKey="newAdds" fill="var(--color-newAdds)" radius={8}>
                <LabelList
                  position="top"
                  offset={12}
                  className="fill-foreground"
                  fontSize={12}
                  formatter={(v: number) => `+${v}`}
                />
              </Bar>
            </BarChart>
          </ChartContainer>
        )}
      </CardContent>

      <CardFooter className="flex-col items-start gap-2 text-sm">
        <div className={`flex gap-2 leading-none font-medium ${netColor}`}>
          Net {netLabel} {wkLabel}
        </div>
        {churnNote && (
          <div className="text-muted-foreground leading-none">
            {churnNote}
          </div>
        )}
      </CardFooter>

      {/* ── Metrics table — inside the card, separated by border-t ── */}
      <div className="border-t">
        <table className="w-full caption-bottom text-sm" style={{ fontFamily: FONT_SANS }}>
          <thead className="bg-muted [&_tr]:border-b">
            <tr>
              <th className="h-10 px-4 text-left align-middle font-medium text-muted-foreground whitespace-nowrap"></th>
              <th className="h-10 px-4 text-right align-middle font-medium text-muted-foreground whitespace-nowrap">Last Week</th>
              <th className="h-10 px-4 text-right align-middle font-medium text-muted-foreground whitespace-nowrap">Prior Week</th>
            </tr>
          </thead>
          <tbody className="[&_tr:last-child]:border-0">
            {metrics.map((m, i) => (
                <tr key={i} className="border-b">
                  <td className="px-4 py-2 align-middle text-muted-foreground whitespace-nowrap">{m.label}</td>
                  <td className="px-4 py-2 align-middle text-right font-medium tabular-nums whitespace-nowrap" style={m.color ? { color: m.color } : undefined}>
                    {m.value}
                  </td>
                  <td className="px-4 py-2 align-middle text-right tabular-nums whitespace-nowrap text-muted-foreground">
                    {m.priorValue ?? ""}
                  </td>
                </tr>
            ))}
          </tbody>
        </table>
      </div>
    </DashboardCard>
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
            {yoyDelta > 0 ? "+" : ""}{Math.round(yoyDelta * 10) / 10}% Year over Year
          </CardDescription>
        )}
      </CardHeader>
      <CardContent>
        <ChartContainer config={annualChartConfig} className="h-[200px] w-full">
          <BarChart accessibilityLayer data={barData} margin={{ top: 32 }}>
            <CartesianGrid vertical={false} />
            <XAxis
              dataKey="year"
              tickLine={false}
              tickMargin={10}
              axisLine={false}
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
//  INSIGHTS SECTION
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

const INSIGHT_SEVERITY: Record<string, {
  icon: typeof AlertTriangleIcon;
  color: string;           // text color for the icon
  badgeVariant: "destructive" | "secondary" | "outline" | "default";
  badgeClass?: string;     // override badge color
}> = {
  critical: { icon: AlertTriangleIcon, color: "text-red-500",     badgeVariant: "destructive" },
  warning:  { icon: AlertTriangleIcon, color: "text-amber-500",   badgeVariant: "secondary", badgeClass: "bg-amber-100 text-amber-800 dark:bg-amber-900/40 dark:text-amber-200" },
  info:     { icon: InfoIcon,          color: "text-blue-500",    badgeVariant: "secondary" },
  positive: { icon: CircleCheckIcon,   color: "text-emerald-500", badgeVariant: "secondary", badgeClass: "bg-emerald-100 text-emerald-800 dark:bg-emerald-900/40 dark:text-emerald-200" },
};

const CATEGORY_LABELS: Record<string, string> = {
  conversion: "Conversion",
  churn: "Churn",
  revenue: "Revenue",
  growth: "Growth",
};

function formatRelativeTimeShort(isoDate: string): string {
  const ms = Date.now() - new Date(isoDate).getTime();
  const mins = Math.floor(ms / 60_000);
  if (mins < 60) return `${mins}m ago`;
  const hrs = Math.floor(mins / 60);
  if (hrs < 24) return `${hrs}h ago`;
  const days = Math.floor(hrs / 24);
  if (days === 1) return "1 day ago";
  return `${days} days ago`;
}

function InsightCard({ insight }: { insight: InsightRow }) {
  const sev = INSIGHT_SEVERITY[insight.severity] || INSIGHT_SEVERITY.info;
  const SevIcon = sev.icon;

  return (
    <DashboardCard>
      <CardHeader>
        <div className="flex items-center gap-2">
          <SevIcon size={18} className={sev.color} />
          <CardTitle className="text-sm">{insight.headline}</CardTitle>
        </div>
        <CardDescription className="flex items-center gap-2 mt-1">
          <Badge variant={sev.badgeVariant} className={sev.badgeClass}>
            {CATEGORY_LABELS[insight.category] || insight.category}
          </Badge>
          <span className="text-xs text-muted-foreground">
            {formatRelativeTimeShort(insight.detectedAt)}
          </span>
        </CardDescription>
      </CardHeader>
      {insight.explanation && (
        <CardContent>
          <p className="text-sm text-muted-foreground leading-relaxed">
            {insight.explanation}
          </p>
        </CardContent>
      )}
    </DashboardCard>
  );
}

// ── Usage Section ─────────────────────────────────────────

const USAGE_CATEGORY_META: Record<string, { icon: React.FC<{ className?: string; style?: React.CSSProperties }>; color: string }> = {
  MEMBER:      { icon: ArrowBadgeDown, color: COLORS.member },
  SKY3:        { icon: BrandSky,       color: COLORS.sky3 },
  SKY_TING_TV: { icon: DeviceTv,       color: COLORS.tv },
};

function UsageCategoryCard({ data }: { data: UsageCategoryData }) {
  const maxPercent = Math.max(...data.segments.map(s => s.percent), 1);
  const meta = USAGE_CATEGORY_META[data.category];
  const Icon = meta?.icon;
  const iconColor = meta?.color;

  const downloadAll = () => {
    window.location.href = `/api/usage-export?category=${data.category}`;
  };
  const downloadSegment = (segName: string) => {
    window.location.href = `/api/usage-export?category=${data.category}&segment=${encodeURIComponent(segName)}`;
  };

  return (
    <DashboardCard>
      <CardHeader className="pb-2">
        <div className="flex items-center gap-2">
          {Icon && <Icon className="size-5" style={{ color: iconColor }} />}
          <CardTitle className="text-base font-semibold">{data.label}</CardTitle>
          <Button
            variant="outline"
            size="icon"
            onClick={downloadAll}
            title={`Download all ${data.label} as CSV`}
          >
            <DownloadIcon className="size-4" />
          </Button>
        </div>
        <CardDescription>
          Median {data.median}/Mo &middot; Mean {data.mean}/Mo &middot; Last 3 Months
        </CardDescription>
        <CardAction>
          <span className="text-2xl font-semibold tracking-tight tabular-nums">{data.totalActive.toLocaleString()}</span>
        </CardAction>
      </CardHeader>
      <CardContent className="pt-0">
        <Table style={{ fontFamily: FONT_SANS }}>
          <TableHeader>
            <TableRow>
              <TableHead className="w-[100px] text-xs">Persona</TableHead>
              <TableHead className="w-[60px] text-xs">Usage</TableHead>
              <TableHead className="text-xs">&nbsp;</TableHead>
              <TableHead className="w-[55px] text-right text-xs">Total</TableHead>
              <TableHead className="w-[50px] text-right text-xs">%</TableHead>
              <TableHead className="w-10 px-0 text-center"><DownloadIcon className="size-3.5 text-muted-foreground inline-block" /></TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {data.segments.map((seg) => (
              <TableRow key={seg.name}>
                <TableCell className="py-1.5 font-medium text-sm">{seg.name}</TableCell>
                <TableCell className="py-1.5 text-xs text-muted-foreground tabular-nums">{seg.rangeLabel}</TableCell>
                <TableCell className="py-1.5">
                  <div className="h-5 rounded-sm overflow-hidden bg-muted/40">
                    <div
                      className="h-full rounded-sm transition-all"
                      style={{
                        width: `${Math.max((seg.percent / maxPercent) * 100, 2)}%`,
                        backgroundColor: seg.color,
                      }}
                    />
                  </div>
                </TableCell>
                <TableCell className="py-1.5 text-right tabular-nums text-sm">{seg.count.toLocaleString()}</TableCell>
                <TableCell className="py-1.5 text-right tabular-nums text-sm text-muted-foreground">{seg.percent}%</TableCell>
                <TableCell className="py-1.5 px-0 text-center">
                  <Button
                    variant="ghost"
                    size="icon"
                    className="size-7 mx-auto"
                    onClick={() => downloadSegment(seg.name)}
                    title={`Download ${seg.name} as CSV`}
                  >
                    <DownloadIcon className="size-3.5" />
                  </Button>
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </CardContent>
    </DashboardCard>
  );
}

function UsageSection({ usage }: { usage: UsageData | null }) {
  if (!usage || usage.categories.length === 0) {
    return (
      <div className="flex flex-col gap-4">
        <NoData label="No usage data available. Ensure auto-renew and registration data is loaded." />
      </div>
    );
  }

  const memberData = usage.categories.find(c => c.category === "MEMBER");
  const sky3Data = usage.categories.find(c => c.category === "SKY3");
  const tvData = usage.categories.find(c => c.category === "SKY_TING_TV");

  return (
    <div className="flex flex-col gap-3">
      {/* Members card — full width (largest category, most detail) */}
      {memberData && <UsageCategoryCard data={memberData} />}

      {/* Sky3 + TV side-by-side */}
      <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
        {sky3Data && <UsageCategoryCard data={sky3Data} />}
        {tvData && <UsageCategoryCard data={tvData} />}
      </div>

    </div>
  );
}

function InsightsSection({ insights }: { insights: InsightRow[] | null }) {
  if (!insights || insights.length === 0) {
    return (
      <div className="flex flex-col gap-4">
        <NoData label="No insights detected yet. Run the pipeline to generate actionable insights." />
      </div>
    );
  }

  return (
    <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
      {insights.map((insight) => (
        <InsightCard key={insight.id} insight={insight} />
      ))}
    </div>
  );
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
//  CHURN SECTION (extracted from CategoryDetail cards)
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

type ChurnSubsection = "members" | "sky3" | "tv" | "intro";

function ChurnSection({ churnRates, weekly, expiringIntroWeeks, introWeekConversion, subsection }: {
  churnRates?: ChurnRateData | null;
  weekly: TrendRowData[];
  expiringIntroWeeks?: ExpiringIntroWeekData | null;
  introWeekConversion?: IntroWeekConversionData | null;
  subsection: ChurnSubsection;
}) {
  if (!churnRates && subsection !== "intro") {
    return (
      <div className="flex flex-col gap-4">
        <NoData label="Churn data" />
      </div>
    );
  }

  const byCategory = churnRates?.byCategory;
  const completedWeekly = weekly.length > 1 ? weekly.slice(0, -1) : weekly;
  const latestW = completedWeekly.length >= 1 ? completedWeekly[completedWeekly.length - 1] : null;

  const mem = byCategory?.member;
  const tenure = mem?.tenureMetrics;
  const alerts = churnRates?.memberAlerts;

  // Last completed month for raw counts
  const lastComplete = mem && mem.monthly.length >= 2 ? mem.monthly[mem.monthly.length - 2] : null;

  /** Reusable churn metric row */
  function MetricRow({ label, value, color, suffix = "%", context }: { label: string; value: number | undefined; color?: string; suffix?: string; context?: string }) {
    if (value == null) return null;
    return (
      <div className="flex justify-between items-center py-1.5 border-b border-border last:border-b-0">
        <div className="flex flex-col">
          <span className="text-xs text-muted-foreground">{label}</span>
          {context && <span className="text-[10px] text-muted-foreground/60">{context}</span>}
        </div>
        <span className="text-sm font-semibold tabular-nums" style={color ? { color } : undefined}>
          {value.toFixed(1)}{suffix}
        </span>
      </div>
    );
  }

  // ── Members ──
  if (subsection === "members") {
  if (!churnRates || !mem) return <NoData label="Member churn data" />;
  return (
    <div className="flex flex-col gap-6">

        {/* ═══ Section 1: Churn Data ═══ */}
        <div className="flex flex-col gap-3">
          <h3 className="text-sm font-semibold text-muted-foreground uppercase tracking-wider">Churn Data</h3>
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-3 items-stretch">
          {/* ── Weekly Churn bar chart (left) ── */}
        {(() => {
          const completedWeeks = weekly.length > 1 ? weekly.slice(0, -1) : weekly;
          const currentWeek = weekly.length > 1 ? weekly[weekly.length - 1] : null;
          const last4 = completedWeeks.slice(-4);
          if (last4.length === 0) return null;
          const weeklyChurnData = last4.map((w) => ({
            week: formatWeekShort(w.period),
            churn: w.memberChurn,
            fill: COLORS.member,
          }));
          if (currentWeek) {
            weeklyChurnData.push({
              week: formatWeekShort(currentWeek.period),
              churn: currentWeek.memberChurn,
              fill: `${COLORS.member}50`,
            });
          }
          const weeklyAvg = last4.length > 0
            ? (last4.reduce((s, w) => s + w.memberChurn, 0) / last4.length) : 0;
          const weeklyChurnConfig = { churn: { label: "Churned", color: COLORS.member } } satisfies ChartConfig;
          return (
              <Card>
                <div>
                  <div className="flex items-start justify-between">
                    <div className="flex items-center gap-2">
                      <Recycle className="size-5 shrink-0" style={{ color: COLORS.member }} />
                      <span className="text-base font-semibold leading-none tracking-tight">Weekly Churn</span>
                    </div>
                    <div className="text-right">
                      <div className="text-lg font-semibold tabular-nums" style={{ color: COLORS.error }}>{weeklyAvg.toFixed(1)}</div>
                      <div className="text-xs text-muted-foreground leading-tight">
                        avg / week
                      </div>
                    </div>
                  </div>
                  <p className="text-sm text-muted-foreground mt-0.5">Members who churned per week</p>
                </div>
                <ChartContainer config={weeklyChurnConfig} className="h-[200px] w-full">
                  <BarChart accessibilityLayer data={weeklyChurnData} margin={{ top: 20, left: 0, right: 0, bottom: 0 }}>
                    <CartesianGrid vertical={false} />
                    <XAxis dataKey="week" tickLine={false} tickMargin={10} axisLine={false} />
                    <Bar dataKey="churn" radius={8}>
                      <LabelList dataKey="churn" position="top" fontSize={11} fontWeight={600} />
                    </Bar>
                  </BarChart>
                </ChartContainer>
              </Card>
          );
        })()}
        {/* ── Monthly Churn bar chart (right) ── */}
        {(() => {
          const completedMonths = mem.monthly.slice(0, -1).filter((m) => m.month !== "2025-10");
          const currentMonth = mem.monthly.length > 0 ? mem.monthly[mem.monthly.length - 1] : null;
          if (completedMonths.length === 0) return null;
          const fmtShort = (m: string) => {
            const [y, mo] = m.split("-");
            const d = new Date(parseInt(y), parseInt(mo) - 1);
            return d.toLocaleDateString("en-US", { month: "short" }) + " '" + y.slice(2);
          };
          const monthlyData = completedMonths.map((m) => ({
            month: fmtShort(m.month),
            rate: parseFloat((m.eligibleChurnRate ?? 0).toFixed(1)),
            fill: COLORS.member,
          }));
          if (currentMonth) {
            monthlyData.push({
              month: fmtShort(currentMonth.month),
              rate: parseFloat((currentMonth.eligibleChurnRate ?? 0).toFixed(1)),
              fill: `${COLORS.member}50`,
            });
          }
          const last6 = completedMonths.slice(-6);
          const avgMonthly = last6.length > 0
            ? last6.reduce((s, m) => s + (m.eligibleChurnRate ?? 0), 0) / last6.length : 0;
          const monthlyConfig = { rate: { label: "Monthly churn", color: COLORS.member } } satisfies ChartConfig;
          return (
              <Card>
                <div>
                  <div className="flex items-start justify-between">
                    <div className="flex items-center gap-2">
                      <Recycle className="size-5 shrink-0" style={{ color: COLORS.member }} />
                      <span className="text-base font-semibold leading-none tracking-tight">Monthly Churn</span>
                    </div>
                    <div className="text-right">
                      <div className="text-lg font-semibold tabular-nums" style={{ color: churnBenchmarkColor(avgMonthly) }}>{avgMonthly.toFixed(1)}%</div>
                      <div className="text-xs text-muted-foreground leading-tight">6-mo avg</div>
                    </div>
                  </div>
                  <p className="text-sm text-muted-foreground mt-0.5">Monthly-billed member churn rate</p>
                </div>
                <ChartContainer config={monthlyConfig} className="h-[200px] w-full">
                  <BarChart accessibilityLayer data={monthlyData} margin={{ top: 20, left: 0, right: 0, bottom: 0 }}>
                    <CartesianGrid vertical={false} />
                    <XAxis dataKey="month" tickLine={false} tickMargin={10} axisLine={false} />
                    <Bar dataKey="rate" radius={8}>
                      <LabelList dataKey="rate" position="top" fontSize={11} fontWeight={600} formatter={(v: number) => `${v}%`} />
                    </Bar>
                  </BarChart>
                </ChartContainer>
              </Card>
          );
        })()}
          </div>
        </div>

        {/* ═══ Section 2: Historical Trends ═══ */}
        <div className="flex flex-col gap-3">
          <h3 className="text-sm font-semibold text-muted-foreground uppercase tracking-wider">Historical Trends</h3>
          {tenure && (
            <Card>
              <div className="flex items-center gap-2">
                <HourglassLow className="size-5 shrink-0" style={{ color: COLORS.member }} />
                <span className="text-base font-semibold leading-none tracking-tight">Member Retention</span>
              </div>

              {/* KPI tiles */}
              <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
                <div className="flex flex-col gap-0.5">
                  <span className="text-2xl font-semibold tabular-nums tracking-tight">{tenure.medianTenure.toFixed(1)} mo</span>
                  <span className="text-xs font-medium text-foreground">Median Tenure</span>
                  <span className="text-[11px] text-muted-foreground leading-snug">Half of all members stay longer than this, half leave sooner</span>
                </div>
                <div className="flex flex-col gap-0.5">
                  <span className="text-2xl font-semibold tabular-nums tracking-tight" style={{ color: tenure.month4RenewalRate >= 70 ? COLORS.success : COLORS.warning }}>{tenure.month4RenewalRate.toFixed(1)}%</span>
                  <span className="text-xs font-medium text-foreground">Month-4 Renewal Rate</span>
                  <span className="text-[11px] text-muted-foreground leading-snug">After the 3-month minimum, this % of members choose to continue</span>
                </div>
                <div className="flex flex-col gap-0.5">
                  <span className="text-2xl font-semibold tabular-nums tracking-tight">{tenure.avgPostCliffTenure.toFixed(1)} mo</span>
                  <span className="text-xs font-medium text-foreground">Avg Tenure</span>
                  <span className="text-[11px] text-muted-foreground leading-snug">Average total tenure of members who made it past the 3-month cliff</span>
                </div>
              </div>

              {/* Survival curve chart */}
              {tenure.survivalCurve.length > 1 && (
                <ChartContainer
                  config={{
                    retained: { label: "Members Retained", color: COLORS.member },
                  } satisfies ChartConfig}
                  className="h-[240px] w-full"
                >
                  <RAreaChart
                    accessibilityLayer
                    data={tenure.survivalCurve}
                    margin={{ top: 5, right: 10, left: 10, bottom: 5 }}
                  >
                    <CartesianGrid vertical={false} />
                    <XAxis
                      dataKey="month"
                      tickLine={false}
                      axisLine={false}
                      tickMargin={8}
                      tickFormatter={(v) => `Mo ${v}`}
                    />
                    <YAxis
                      tickLine={false}
                      axisLine={false}
                      tickFormatter={(v) => `${v}%`}
                      domain={[0, 100]}
                      ticks={[0, 25, 50, 75, 100]}
                    />
                    <ChartTooltip content={<ChartTooltipContent formatter={(v) => `${(v as number).toFixed(1)}%`} />} />
                    <defs>
                      <linearGradient id="survivalGradient" x1="0" y1="0" x2="0" y2="1">
                        <stop offset="5%" stopColor={COLORS.member} stopOpacity={0.3} />
                        <stop offset="95%" stopColor={COLORS.member} stopOpacity={0.05} />
                      </linearGradient>
                    </defs>
                    <Area
                      type="stepAfter"
                      dataKey="retained"
                      stroke={COLORS.member}
                      strokeWidth={2}
                      fill="url(#survivalGradient)"
                    />
                    {/* Cliff reference line at month 3 */}
                    <ReferenceLine x={3} stroke={COLORS.warning} strokeDasharray="4 4" label={{ value: "3-mo cliff", position: "top", fontSize: 10, fill: COLORS.warning }} />
                  </RAreaChart>
                </ChartContainer>
              )}
            </Card>
          )}
        </div>

        {/* ═══ Section 3: Churn Reduction Opportunities ═══ */}
        <div className="flex flex-col gap-3">
          <h3 className="text-sm font-semibold text-muted-foreground uppercase tracking-wider">Churn Reduction Opportunities</h3>
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-3 items-stretch">
        {/* ── Approaching Milestones ── */}
        {alerts && (
          (() => {
            const cliffMembers = alerts.tenureMilestones.filter((m) => m.milestone.includes("cliff"));
            const markMembers = alerts.tenureMilestones.filter((m) => m.milestone.includes("mark"));
            const downloadMilestoneCsv = (members: typeof cliffMembers, filename: string) => {
              const headers = ["Name", "Email", "Plan", "Annual/Monthly", "Start Date", "Tenure (months)", "Milestone"];
              const rows = members.map((m) => [
                m.name, m.email, m.planName, m.isAnnual ? "Annual" : "Monthly",
                m.createdAt.slice(0, 10), m.tenureMonths.toFixed(1), m.milestone,
              ]);
              const csv = [headers, ...rows].map((r) => r.map((c) => `"${String(c).replace(/"/g, '""')}"`).join(",")).join("\n");
              const blob = new Blob([csv], { type: "text/csv" });
              const url = URL.createObjectURL(blob);
              const a = document.createElement("a");
              a.href = url; a.download = filename; a.click();
              URL.revokeObjectURL(url);
            };
            return (cliffMembers.length > 0 || markMembers.length > 0) ? (
              <Card matchHeight>
                <div>
                  <div className="flex items-start justify-between">
                    <div className="flex items-center gap-2">
                      <HourglassLow className="size-5 shrink-0" style={{ color: COLORS.warning }} />
                      <span className="text-base font-semibold leading-none tracking-tight">Approaching Milestones</span>
                    </div>
                    {alerts.tenureMilestones.length > 0 && (
                      <Button variant="outline" size="icon" onClick={() => downloadMilestoneCsv(alerts.tenureMilestones, "all-milestone-members.csv")} title="Download all milestone members as CSV">
                        <DownloadIcon className="size-4" />
                      </Button>
                    )}
                  </div>
                  <p className="text-sm text-muted-foreground mt-0.5">Members within ±1 week of a critical tenure milestone</p>
                </div>
                <div className="flex-1 flex flex-col">
                  <Table style={{ fontFamily: FONT_SANS }}>
                    <TableHeader className="bg-muted">
                      <TableRow>
                        <TableHead className="text-xs text-muted-foreground">Milestone</TableHead>
                        <TableHead className="text-xs text-muted-foreground text-right"># Members</TableHead>
                        <TableHead className="w-10 px-0 text-center"><DownloadIcon className="size-3.5 text-muted-foreground inline-block" /></TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {cliffMembers.length > 0 && (
                        <TableRow>
                          <TableCell className="py-1.5 text-sm">3-Month Cliff</TableCell>
                          <TableCell className="py-1.5 text-sm font-semibold text-right tabular-nums">{cliffMembers.length}</TableCell>
                          <TableCell className="py-1.5 px-0 text-center">
                            <Button variant="ghost" size="icon" className="size-7 mx-auto" onClick={() => downloadMilestoneCsv(cliffMembers, "3-month-cliff-members.csv")} title="Download 3-month cliff members as CSV">
                              <DownloadIcon className="size-3.5" />
                            </Button>
                          </TableCell>
                        </TableRow>
                      )}
                      {markMembers.length > 0 && (
                        <TableRow>
                          <TableCell className="py-1.5 text-sm">7-Month Mark</TableCell>
                          <TableCell className="py-1.5 text-sm font-semibold text-right tabular-nums">{markMembers.length}</TableCell>
                          <TableCell className="py-1.5 px-0 text-center">
                            <Button variant="ghost" size="icon" className="size-7 mx-auto" onClick={() => downloadMilestoneCsv(markMembers, "7-month-mark-members.csv")} title="Download 7-month mark members as CSV">
                              <DownloadIcon className="size-3.5" />
                            </Button>
                          </TableCell>
                        </TableRow>
                      )}
                      <TableRow className="border-t">
                        <TableCell className="py-1.5 text-sm font-semibold">Total</TableCell>
                        <TableCell className="py-1.5 text-sm font-semibold text-right tabular-nums">{cliffMembers.length + markMembers.length}</TableCell>
                        <TableCell className="py-1.5 px-0" />
                      </TableRow>
                    </TableBody>
                  </Table>
                </div>
              </Card>
            ) : null;
          })()
        )}

        {/* ── Win-Back Members card ─────────────────────── */}
        {churnRates.winBack && churnRates.winBack.reactivated.total > 0 && (() => {
          const wb = churnRates.winBack!;
          const r = wb.reactivated;
          const nowMs = Date.now();
          const MS_DAY = 24 * 60 * 60 * 1000;

          // Bucket current targets by days since cancel
          const targetsByBucket = (minDays: number, maxDays: number | null) =>
            wb.targets.filter((t) => {
              const days = Math.round((nowMs - new Date(t.canceledAt).getTime()) / MS_DAY);
              return maxDays != null ? days >= minDays && days <= maxDays : days > minDays;
            });

          const buckets = [
            { label: "≤ 30 days", hist: r.within30, today: targetsByBucket(0, 30) },
            { label: "31–60 days", hist: r.within60, today: targetsByBucket(31, 60) },
            { label: "61–90 days", hist: r.within90, today: targetsByBucket(61, 90) },
            { label: "91–120 days", hist: r.within120, today: targetsByBucket(91, 120) },
            { label: "> 120 days", hist: r.beyond120, today: targetsByBucket(121, null) },
          ];
          const totalToday = buckets.reduce((s, b) => s + b.today.length, 0);

          const downloadBucketCsv = (members: typeof wb.targets, filename: string) => {
            const header = "Name,Email,Last Plan,Canceled At,Days Since Cancel\n";
            const rows = members.map((t) => {
              const days = Math.round((nowMs - new Date(t.canceledAt).getTime()) / MS_DAY);
              return `"${t.name}","${t.email}","${t.lastPlanName}","${t.canceledAt}",${days}`;
            }).join("\n");
            const blob = new Blob([header + rows], { type: "text/csv" });
            const url = URL.createObjectURL(blob);
            const a = document.createElement("a");
            a.href = url; a.download = filename; a.click();
            URL.revokeObjectURL(url);
          };

          return (
            <Card>
              <div>
                <div className="flex items-start justify-between">
                  <div className="flex items-center gap-2">
                    <UserPlus className="size-5 shrink-0" style={{ color: COLORS.member }} />
                    <span className="text-base font-semibold leading-none tracking-tight">Win-Back Members</span>
                  </div>
                  {totalToday > 0 && (
                    <Button variant="outline" size="icon" onClick={() => downloadBucketCsv(wb.targets, "winback-all-targets.csv")} title="Download all win-back targets as CSV">
                      <DownloadIcon className="size-4" />
                    </Button>
                  )}
                </div>
                <p className="text-sm text-muted-foreground mt-0.5">
                  {wb.reactivationRate}% of churned members eventually reactivate — here&apos;s when they come back
                </p>
              </div>

              <div className="flex-1 flex flex-col">
                <Table>
                  <TableHeader>
                    <TableRow className="bg-muted">
                      <TableHead className="text-xs py-1.5">Return Window</TableHead>
                      <TableHead className="text-xs py-1.5 text-right">Avg % of Returns (Historical)</TableHead>
                      <TableHead className="text-xs py-1.5 text-right"># Today</TableHead>
                      <TableHead className="w-10 px-0 text-center"><DownloadIcon className="size-3.5 text-muted-foreground inline-block" /></TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {buckets.map((b) => (
                      <TableRow key={b.label}>
                        <TableCell className="py-1.5 text-sm">{b.label}</TableCell>
                        <TableCell className="py-1.5 text-sm text-right tabular-nums">
                          {r.total > 0 ? (b.hist / r.total * 100).toFixed(1) : 0}%
                        </TableCell>
                        <TableCell className="py-1.5 text-sm font-semibold text-right tabular-nums">{b.today.length}</TableCell>
                        <TableCell className="py-1.5 px-0 text-center">
                          {b.today.length > 0 && (
                            <Button variant="ghost" size="icon" className="size-7 mx-auto" onClick={() => downloadBucketCsv(b.today, `winback-${b.label.replace(/[^a-z0-9]/gi, "-").toLowerCase()}.csv`)} title={`Download ${b.label} targets as CSV`}>
                              <DownloadIcon className="size-3.5" />
                            </Button>
                          )}
                        </TableCell>
                      </TableRow>
                    ))}
                    <TableRow className="font-medium border-t-2">
                      <TableCell className="py-1.5 text-sm">Total</TableCell>
                      <TableCell className="py-1.5 text-sm text-right tabular-nums text-muted-foreground">
                        {wb.reactivationRate}% of churned
                      </TableCell>
                      <TableCell className="py-1.5 text-sm font-semibold text-right tabular-nums">{totalToday}</TableCell>
                      <TableCell className="py-1.5 px-0" />
                    </TableRow>
                  </TableBody>
                </Table>
              </div>

              <p className="text-xs text-muted-foreground mt-3">
                When they return: {r.upgradePct}% upgrade · {r.downgradePct}% downgrade · {r.samePct}% same plan
              </p>
            </Card>
          );
        })()}

        {/* At Risk */}
        {churnRates.totalAtRisk > 0 && (
          (() => {
            const ars = churnRates.atRiskByState;
            const allAtRisk = ars ? [...ars.pastDue, ...ars.invalid, ...ars.pendingCancel] : [];
            const downloadAtRiskCsv = (members: AtRiskMember[], filename: string) => {
              const headers = ["Name", "Email", "Plan", "Category", "Status", "Start Date", "Tenure (months)"];
              const rows = members.map((m) => [
                m.name, m.email, m.planName, m.category, m.planState,
                m.createdAt ? m.createdAt.slice(0, 10) : "", m.tenureMonths.toFixed(1),
              ]);
              const csv = [headers, ...rows].map((r) => r.map((c) => `"${String(c).replace(/"/g, '""')}"`).join(",")).join("\n");
              const blob = new Blob([csv], { type: "text/csv" });
              const url = URL.createObjectURL(blob);
              const a = document.createElement("a");
              a.href = url; a.download = filename; a.click();
              URL.revokeObjectURL(url);
            };
            const stateRows = [
              { label: "Past Due", members: ars?.pastDue ?? [], file: "at-risk-past-due.csv" },
              { label: "Invalid", members: ars?.invalid ?? [], file: "at-risk-invalid.csv" },
              { label: "Pending Cancel", members: ars?.pendingCancel ?? [], file: "at-risk-pending-cancel.csv" },
            ];
            return (
              <Card matchHeight>
                <div>
                  <div className="flex items-start justify-between">
                    <div className="flex items-center gap-2">
                      <AlertTriangle className="size-5 shrink-0 text-amber-600" />
                      <span className="text-base font-semibold leading-none tracking-tight">At Risk</span>
                    </div>
                    {allAtRisk.length > 0 && (
                      <Button variant="outline" size="icon" onClick={() => downloadAtRiskCsv(allAtRisk, "all-at-risk.csv")} title="Download all at-risk subscribers as CSV">
                        <DownloadIcon className="size-4" />
                      </Button>
                    )}
                  </div>
                  <p className="text-sm text-muted-foreground mt-0.5">Auto-renew customers across all categories whose plan is past due, invalid, or pending cancel</p>
                </div>
                {ars && (
                  <div className="flex-1 flex flex-col">
                    <Table style={{ fontFamily: FONT_SANS }}>
                      <TableHeader className="bg-muted">
                        <TableRow>
                          <TableHead className="text-xs text-muted-foreground">Status</TableHead>
                          <TableHead className="text-xs text-muted-foreground text-right"># Members</TableHead>
                          <TableHead className="w-10 px-0 text-center"><DownloadIcon className="size-3.5 text-muted-foreground inline-block" /></TableHead>
                        </TableRow>
                      </TableHeader>
                      <TableBody>
                        {stateRows.map((sr) => (
                          <TableRow key={sr.label}>
                            <TableCell className="py-1.5 text-sm">{sr.label}</TableCell>
                            <TableCell className="py-1.5 text-sm font-semibold text-right tabular-nums">{sr.members.length}</TableCell>
                            <TableCell className="py-1.5 px-0 text-center">
                              {sr.members.length > 0 && (
                                <Button variant="ghost" size="icon" className="size-7 mx-auto" onClick={() => downloadAtRiskCsv(sr.members, sr.file)} title={`Download ${sr.label} as CSV`}>
                                  <DownloadIcon className="size-3.5" />
                                </Button>
                              )}
                            </TableCell>
                          </TableRow>
                        ))}
                        <TableRow className="border-t">
                          <TableCell className="py-1.5 text-sm font-semibold">Total</TableCell>
                          <TableCell className="py-1.5 text-sm font-semibold text-right tabular-nums">{churnRates.totalAtRisk}</TableCell>
                          <TableCell className="py-1.5 px-0" />
                        </TableRow>
                      </TableBody>
                    </Table>
                  </div>
                )}
              </Card>
            );
          })()
        )}
          </div>
        </div>
    </div>
  );
  }

  // ── Sky3 ──
  if (subsection === "sky3") {
  if (!churnRates || !byCategory) return <NoData label="Sky3 churn data" />;
  return (
    <div className="flex flex-col gap-3">
        <Card>
          <div>
            <div className="flex items-start justify-between">
              <div className="flex items-center gap-2">
                <BrandSky className="size-5 shrink-0" style={{ color: COLORS.sky3 }} />
                <span className="text-base font-semibold leading-none tracking-tight">SKY3 Churn</span>
              </div>
            </div>
            <p className="text-sm text-muted-foreground mt-0.5">User and MRR churn rates for Sky3 subscribers</p>
          </div>
          <div className="flex-1 flex flex-col">
            <Table style={{ fontFamily: FONT_SANS }}>
              <TableHeader className="bg-muted">
                <TableRow>
                  <TableHead className="text-xs text-muted-foreground">Metric</TableHead>
                  <TableHead className="text-xs text-muted-foreground text-right">Value</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                <TableRow>
                  <TableCell className="py-1.5 text-sm">User Churn (Avg/Mo)</TableCell>
                  <TableCell className="py-1.5 text-sm font-semibold text-right tabular-nums" style={{ color: byCategory.sky3.avgUserChurnRate != null ? churnBenchmarkColor(byCategory.sky3.avgUserChurnRate) : undefined }}>
                    {byCategory.sky3.avgUserChurnRate != null ? `${byCategory.sky3.avgUserChurnRate.toFixed(1)}%` : "–"}
                  </TableCell>
                </TableRow>
                <TableRow>
                  <TableCell className="py-1.5 text-sm">MRR Churn (Avg/Mo)</TableCell>
                  <TableCell className="py-1.5 text-sm font-semibold text-right tabular-nums" style={{ color: byCategory.sky3.avgMrrChurnRate != null ? churnBenchmarkColor(byCategory.sky3.avgMrrChurnRate) : undefined }}>
                    {byCategory.sky3.avgMrrChurnRate != null ? `${byCategory.sky3.avgMrrChurnRate.toFixed(1)}%` : "–"}
                  </TableCell>
                </TableRow>
                {latestW?.sky3Churn != null && (
                  <TableRow>
                    <TableCell className="py-1.5 text-sm">Churned {weekLabel(latestW.period)}</TableCell>
                    <TableCell className="py-1.5 text-sm font-semibold text-right tabular-nums">{latestW.sky3Churn}</TableCell>
                  </TableRow>
                )}
                {byCategory.sky3.atRiskCount > 0 && (
                  <TableRow>
                    <TableCell className="py-1.5 text-sm">At Risk</TableCell>
                    <TableCell className="py-1.5 text-sm font-semibold text-right tabular-nums" style={{ color: COLORS.warning }}>{byCategory.sky3.atRiskCount}</TableCell>
                  </TableRow>
                )}
              </TableBody>
            </Table>
          </div>
        </Card>
    </div>
  );
  }

  // ── Sky Ting TV ──
  if (subsection === "tv") {
  if (!churnRates || !byCategory) return <NoData label="Sky Ting TV churn data" />;
  return (
    <div className="flex flex-col gap-3">
        <Card>
          <div>
            <div className="flex items-start justify-between">
              <div className="flex items-center gap-2">
                <DeviceTv className="size-5 shrink-0" style={{ color: COLORS.tv }} />
                <span className="text-base font-semibold leading-none tracking-tight">Sky Ting TV Churn</span>
              </div>
            </div>
            <p className="text-sm text-muted-foreground mt-0.5">User and MRR churn rates for Sky Ting TV subscribers</p>
          </div>
          <div className="flex-1 flex flex-col">
            <Table style={{ fontFamily: FONT_SANS }}>
              <TableHeader className="bg-muted">
                <TableRow>
                  <TableHead className="text-xs text-muted-foreground">Metric</TableHead>
                  <TableHead className="text-xs text-muted-foreground text-right">Value</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                <TableRow>
                  <TableCell className="py-1.5 text-sm">User Churn (Avg/Mo)</TableCell>
                  <TableCell className="py-1.5 text-sm font-semibold text-right tabular-nums" style={{ color: byCategory.skyTingTv.avgUserChurnRate != null ? churnBenchmarkColor(byCategory.skyTingTv.avgUserChurnRate) : undefined }}>
                    {byCategory.skyTingTv.avgUserChurnRate != null ? `${byCategory.skyTingTv.avgUserChurnRate.toFixed(1)}%` : "–"}
                  </TableCell>
                </TableRow>
                <TableRow>
                  <TableCell className="py-1.5 text-sm">MRR Churn (Avg/Mo)</TableCell>
                  <TableCell className="py-1.5 text-sm font-semibold text-right tabular-nums" style={{ color: byCategory.skyTingTv.avgMrrChurnRate != null ? churnBenchmarkColor(byCategory.skyTingTv.avgMrrChurnRate) : undefined }}>
                    {byCategory.skyTingTv.avgMrrChurnRate != null ? `${byCategory.skyTingTv.avgMrrChurnRate.toFixed(1)}%` : "–"}
                  </TableCell>
                </TableRow>
                {latestW?.skyTingTvChurn != null && (
                  <TableRow>
                    <TableCell className="py-1.5 text-sm">Churned {weekLabel(latestW.period)}</TableCell>
                    <TableCell className="py-1.5 text-sm font-semibold text-right tabular-nums">{latestW.skyTingTvChurn}</TableCell>
                  </TableRow>
                )}
                {byCategory.skyTingTv.atRiskCount > 0 && (
                  <TableRow>
                    <TableCell className="py-1.5 text-sm">At Risk</TableCell>
                    <TableCell className="py-1.5 text-sm font-semibold text-right tabular-nums" style={{ color: COLORS.warning }}>{byCategory.skyTingTv.atRiskCount}</TableCell>
                  </TableRow>
                )}
              </TableBody>
            </Table>
          </div>
        </Card>
    </div>
  );
  }

  // ── Intro Week ──
  if (subsection === "intro") {
  return (
    <div className="flex flex-col gap-3">
        <div className="grid grid-cols-1 sm:grid-cols-2 gap-3 items-stretch">
        {introWeekConversion && (
            <ExpiredIntroWeeksCard data={introWeekConversion} />
        )}
        {/* Expiring Intro Weeks */}
        {expiringIntroWeeks && expiringIntroWeeks.customers.length > 0 && (
          (() => {
            const customers = expiringIntroWeeks.customers;
            const highFreq = customers.filter((c: ExpiringIntroCustomer) => c.classesAttended > 2);
            const lowFreq = customers.filter((c: ExpiringIntroCustomer) => c.classesAttended <= 2);
            const total = customers.length;
            const highPct = total > 0 ? Math.round((highFreq.length / total) * 100) : 0;
            const lowPct = total > 0 ? Math.round((lowFreq.length / total) * 100) : 0;
            const downloadExpiringCsv = () => {
              const headers = ["Name", "Email", "Classes Attended"];
              const rows = customers.map((c: ExpiringIntroCustomer) => [
                `${c.firstName} ${c.lastName}`.trim(),
                c.email,
                String(c.classesAttended),
              ]);
              const csv = [headers, ...rows]
                .map((r) => r.map((cell) => `"${String(cell).replace(/"/g, '""')}"`).join(","))
                .join("\n");
              const blob = new Blob([csv], { type: "text/csv" });
              const url = URL.createObjectURL(blob);
              const a = document.createElement("a");
              a.href = url;
              a.download = "expiring-intro-weeks.csv";
              a.click();
              URL.revokeObjectURL(url);
            };
            return (
              <Card matchHeight>
                <div className="flex items-start justify-between">
                  <div className="flex items-center gap-2">
                    <CalendarWeek className="size-5 shrink-0" style={{ color: COLORS.copper }} />
                    <span className="text-base font-semibold leading-none tracking-tight">Expiring Intro Weeks</span>
                  </div>
                  <Button variant="outline" size="icon" onClick={downloadExpiringCsv} title="Download all expiring intro week customers as CSV">
                    <DownloadIcon className="size-4" />
                  </Button>
                </div>
                <div className="flex-1 flex flex-col mt-2">
                  <Table style={{ fontFamily: FONT_SANS }}>
                    <TableHeader className="bg-muted">
                      <TableRow>
                        <TableHead className="text-xs text-muted-foreground">Frequency</TableHead>
                        <TableHead className="text-xs text-muted-foreground text-right"># People</TableHead>
                        <TableHead className="text-xs text-muted-foreground text-right">% of Total</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      <TableRow>
                        <TableCell className="py-1.5">
                          <div className="flex items-center gap-1.5 text-sm">
                            <span className="size-1.5 rounded-full bg-green-500 shrink-0" />
                            High Frequency
                          </div>
                          <div className="text-[11px] text-muted-foreground ml-3">More than 2 classes</div>
                        </TableCell>
                        <TableCell className="py-1.5 text-sm font-semibold text-right tabular-nums">{highFreq.length}</TableCell>
                        <TableCell className="py-1.5 text-sm text-right tabular-nums text-muted-foreground">{highPct}%</TableCell>
                      </TableRow>
                      <TableRow>
                        <TableCell className="py-1.5">
                          <div className="flex items-center gap-1.5 text-sm">
                            <span className="size-1.5 rounded-full bg-red-500 shrink-0" />
                            Low Frequency
                          </div>
                          <div className="text-[11px] text-muted-foreground ml-3">2 or fewer classes</div>
                        </TableCell>
                        <TableCell className="py-1.5 text-sm font-semibold text-right tabular-nums">{lowFreq.length}</TableCell>
                        <TableCell className="py-1.5 text-sm text-right tabular-nums text-muted-foreground">{lowPct}%</TableCell>
                      </TableRow>
                      <TableRow className="border-t">
                        <TableCell className="py-1.5 text-sm font-semibold">Total</TableCell>
                        <TableCell className="py-1.5 text-sm font-semibold text-right tabular-nums">{total}</TableCell>
                        <TableCell className="py-1.5 px-0" />
                      </TableRow>
                    </TableBody>
                  </Table>
                </div>
              </Card>
            );
          })()
        )}
        </div>
    </div>
  );
  }

  return null;
}

// ─── Shopify Sync Status ──────────────────────────────────────

function ShopifySyncStatus({ lastSyncAt, onSyncComplete }: { lastSyncAt: string; onSyncComplete: () => void }) {
  const [syncing, setSyncing] = useState(false);
  const [syncError, setSyncError] = useState<string | null>(null);
  const [nextRun, setNextRun] = useState<string | null>(null);

  // Fetch next scheduled run
  useEffect(() => {
    fetch("/api/schedule")
      .then((r) => r.json())
      .then((s) => {
        if (s.enabled && s.nextRun) {
          setNextRun(formatRelativeTime(new Date(s.nextRun).toISOString()));
        }
      })
      .catch(() => {});
  }, [lastSyncAt]);

  async function handleSync() {
    setSyncing(true);
    setSyncError(null);
    try {
      const res = await fetch("/api/shopify", { method: "POST" });
      if (!res.ok) {
        const body = await res.json().catch(() => ({ error: `HTTP ${res.status}` }));
        throw new Error(body.error || `Sync failed (${res.status})`);
      }
      onSyncComplete();
    } catch (err) {
      const msg = err instanceof Error ? err.message : "Sync failed";
      setSyncError(msg);
      console.error("[shopify-sync]", msg);
    } finally {
      setSyncing(false);
    }
  }

  return (
    <div className="flex items-center gap-3 mt-1 ml-10 flex-wrap">
      <p className="text-sm text-muted-foreground">
        Last synced {formatRelativeTime(lastSyncAt)}
        {nextRun && <> · Next refresh {nextRun}</>}
      </p>
      <Button
        variant="outline"
        size="sm"
        onClick={handleSync}
        disabled={syncing}
      >
        <RefreshCw className={`size-3.5 ${syncing ? "animate-spin" : ""}`} />
        {syncing ? "Syncing..." : "Refresh"}
      </Button>
      {syncError && (
        <p className="text-sm text-destructive w-full">
          <AlertTriangle className="inline size-3.5 mr-1" />
          {syncError}
        </p>
      )}
    </div>
  );
}

// ─── Merch Revenue Tab (Shopify data) ─────────────────────────

function MerchRevenueTab({ merch, lastSyncAt }: { merch: ShopifyMerchData; lastSyncAt?: string | null }) {
  const isMobile = useIsMobile();

  // Completed months only (exclude current)
  const nowDate = new Date();
  const currentYear = nowDate.getFullYear();
  const currentMonthKey = `${currentYear}-${String(nowDate.getMonth() + 1).padStart(2, "0")}`;
  const completedMonths = merch.monthlyRevenue.filter((m) => m.month < currentMonthKey);
  const chartData = completedMonths.slice(-6).map((m) => ({
    month: formatShortMonth(m.month),
    gross: m.gross,
    orders: m.orderCount,
  }));

  // AOV trend — last 12 completed months
  const aovData = completedMonths.slice(-12).map((m) => ({
    month: formatShortMonth(m.month),
    aov: m.orderCount > 0 ? Math.round(m.gross / m.orderCount) : 0,
  }));

  // Annual revenue YoY
  const annualData = merch.annualRevenue || [];
  const priorYear = currentYear - 1;
  const priorYearData = annualData.find((a) => a.year === priorYear);
  const olderYearData = annualData.find((a) => a.year === priorYear - 1);
  const yoyDelta = olderYearData && olderYearData.gross > 0 && priorYearData
    ? ((priorYearData.gross - olderYearData.gross) / olderYearData.gross * 100)
    : null;

  // YTD revenue: completed months of current year + MTD
  const ytdCompletedMonths = merch.monthlyRevenue.filter((m) => m.month.startsWith(String(currentYear)) && m.month < currentMonthKey);
  const ytdRevenue = ytdCompletedMonths.reduce((s, m) => s + m.gross, 0) + merch.mtdRevenue;

  // Category breakdown
  const categories = merch.categoryBreakdown || [];
  const totalCategoryRevenue = categories.reduce((s, c) => s + c.revenue, 0);

  const merchChartConfig = {
    gross: { label: "Gross Revenue", color: SECTION_COLORS["revenue-merch"] },
  } satisfies ChartConfig;

  const aovChartConfig = {
    aov: { label: "Avg Order Value", color: COLORS.merch },
  } satisfies ChartConfig;

  const annualChartConfig = {
    gross: { label: "Gross Revenue", color: COLORS.merch },
  } satisfies ChartConfig;

  // Bar chart data for annual revenue (complete years only)
  const annualBarData = annualData
    .filter((a) => a.year < currentYear)
    .map((a) => ({ year: String(a.year), gross: a.gross }));

  // Category colors
  const categoryColors = ["#B8860B", "#4A7C59", "#5B7FA5", "#8B6FA5", "#8F7A5E", "#3A8A8A"];

  return (
    <div className="flex flex-col gap-4">
      {/* KPI row — 4 cards */}
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
        <DashboardCard>
          <CardHeader>
            <CardDescription>MTD Revenue</CardDescription>
            <CardTitle className="text-2xl font-semibold tabular-nums">{formatCurrency(merch.mtdRevenue)}</CardTitle>
          </CardHeader>
          <CardFooter className="text-sm text-muted-foreground">
            Month to Date
          </CardFooter>
        </DashboardCard>

        <DashboardCard>
          <CardHeader>
            <CardDescription>{currentYear} YTD Revenue</CardDescription>
            <CardTitle className="text-2xl font-semibold tabular-nums">{formatCurrency(ytdRevenue)}</CardTitle>
          </CardHeader>
          <CardFooter className="text-sm text-muted-foreground">
            Year to Date
          </CardFooter>
        </DashboardCard>

        {yoyDelta !== null && (
          <DashboardCard>
            <CardHeader>
              <CardDescription>YoY Change</CardDescription>
              <CardTitle className={`text-2xl font-semibold tabular-nums ${yoyDelta >= 0 ? "text-emerald-600" : "text-red-500"}`}>
                {yoyDelta > 0 ? "+" : ""}{Math.round(yoyDelta * 10) / 10}%
              </CardTitle>
            </CardHeader>
            <CardFooter className="text-sm text-muted-foreground">
              {priorYear - 1} → {priorYear}
            </CardFooter>
          </DashboardCard>
        )}

        <DashboardCard>
          <CardHeader>
            <CardDescription>Repeat Rate</CardDescription>
            <CardTitle className="text-2xl font-semibold tabular-nums">{merch.repeatCustomerRate}%</CardTitle>
          </CardHeader>
          <CardFooter className="text-sm text-muted-foreground">
            Returning Customers
          </CardFooter>
        </DashboardCard>
      </div>

      {/* Annual Merch Revenue + Buyer Breakdown side by side */}
      <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
        {annualBarData.length >= 2 && (
          <DashboardCard>
            <CardHeader>
              <CardTitle>Annual Merch Revenue</CardTitle>
              {yoyDelta !== null && (
                <CardDescription>
                  {yoyDelta > 0 ? "+" : ""}{Math.round(yoyDelta * 10) / 10}% Year over Year
                </CardDescription>
              )}
            </CardHeader>
            <CardContent>
              <ChartContainer config={annualChartConfig} className="h-[200px] w-full">
                <BarChart accessibilityLayer data={annualBarData} margin={{ top: 32 }}>
                  <CartesianGrid vertical={false} />
                  <XAxis dataKey="year" tickLine={false} tickMargin={10} axisLine={false} />
                  <Bar dataKey="gross" fill="var(--color-gross)" radius={8}>
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
            {priorYearData && (
              <CardFooter className="text-sm text-muted-foreground">
                {priorYear} AOV: {formatCurrency(priorYearData.avgOrderValue)}
              </CardFooter>
            )}
          </DashboardCard>
        )}

        {/* Customer Breakdown: Members vs Non-Members */}
        {merch.customerBreakdown && (
          <DashboardCard>
            <CardHeader>
              <CardTitle>Buyer Breakdown</CardTitle>
              <CardDescription>Merch Orders by Auto-Renew Status</CardDescription>
            </CardHeader>
            <CardContent>
              <MerchBuyerBreakdown breakdown={merch.customerBreakdown} />
            </CardContent>
          </DashboardCard>
        )}
      </div>

      {/* Monthly Revenue chart + AOV trend side by side */}
      <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
        {chartData.length > 0 && (
          <DashboardCard>
            <CardHeader>
              <CardTitle>Monthly Merch Revenue</CardTitle>
              {completedMonths.length > 0 && (
                <CardDescription>
                  {formatShortMonth(completedMonths[completedMonths.length - 1].month)}: {formatCurrency(completedMonths[completedMonths.length - 1].gross)}
                </CardDescription>
              )}
            </CardHeader>
            <CardContent>
              <ChartContainer config={merchChartConfig} className="h-[200px] w-full">
                <BarChart
                  accessibilityLayer
                  data={chartData}
                  margin={{ top: 20 }}
                >
                  <CartesianGrid vertical={false} />
                  <XAxis
                    dataKey="month"
                    tickLine={false}
                    tickMargin={10}
                    axisLine={false}
                  />
                  <Bar dataKey="gross" fill={SECTION_COLORS["revenue-merch"]} radius={8}>
                    <LabelList
                      position="top"
                      offset={12}
                      className="fill-foreground"
                      fontSize={12}
                      formatter={(v: number) => formatCompactCurrency(v)}
                    />
                  </Bar>
                </BarChart>
              </ChartContainer>
            </CardContent>
            <CardFooter className="text-sm text-muted-foreground">
              Last {chartData.length} Months of Shopify Merch Revenue
            </CardFooter>
          </DashboardCard>
        )}

        {aovData.length > 0 && (
          <DashboardCard>
            <CardHeader>
              <CardTitle>Average Order Value</CardTitle>
              {aovData.length > 0 && (
                <CardDescription>
                  Latest: {formatCurrency(aovData[aovData.length - 1].aov)}
                </CardDescription>
              )}
            </CardHeader>
            <CardContent>
              <ChartContainer config={aovChartConfig} className="h-[200px] w-full">
                <LineChart
                  accessibilityLayer
                  data={aovData}
                  margin={{ top: 20, right: 10, left: 10, bottom: 5 }}
                >
                  <CartesianGrid vertical={false} />
                  <XAxis
                    dataKey="month"
                    tickLine={false}
                    tickMargin={10}
                    axisLine={false}
                    interval={0}
                    fontSize={11}
                  />
                  <ChartTooltip
                    cursor={false}
                    content={<ChartTooltipContent hideLabel formatter={(v) => formatCurrency(v as number)} />}
                  />
                  <Line
                    type="monotone"
                    dataKey="aov"
                    stroke="var(--color-aov)"
                    strokeWidth={2}
                    dot={{ r: 3 }}
                    activeDot={{ r: 5 }}
                  >
                    <LabelList
                      dataKey="aov"
                      position="top"
                      offset={8}
                      fontSize={11}
                      formatter={(v: number) => `$${v}`}
                    />
                  </Line>
                </LineChart>
              </ChartContainer>
            </CardContent>
            <CardFooter className="text-sm text-muted-foreground">
              Monthly AOV Trend (Last {aovData.length} Months)
            </CardFooter>
          </DashboardCard>
        )}
      </div>

      {/* Top Products + Category Breakdown — side by side */}
      <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
        {/* Top Products — expanded to 10 with rank + progress bar */}
        {merch.topProducts.length > 0 && (
          <DashboardCard>
            <CardHeader>
              <CardTitle>Top Products</CardTitle>
              <CardDescription>By Total Revenue (All Time)</CardDescription>
            </CardHeader>
            <CardContent>
              <div className="flex flex-col">
                {merch.topProducts.map((product, i) => {
                  const maxRevenue = merch.topProducts[0]?.revenue || 1;
                  const pct = Math.round((product.revenue / maxRevenue) * 100);
                  return (
                    <div key={i} className={`flex items-center gap-3 py-2.5 ${i < merch.topProducts.length - 1 ? "border-b border-border" : ""}`}>
                      <span className="text-xs font-medium text-muted-foreground w-5 text-right shrink-0">#{i + 1}</span>
                      <div className="flex-1 min-w-0">
                        <div className="flex justify-between items-baseline gap-2">
                          <span className="text-sm font-medium truncate">{toTitleCase(product.title)}</span>
                          <span className="text-sm font-semibold tabular-nums shrink-0">{formatCurrency(product.revenue)}</span>
                        </div>
                        <div className="flex items-center gap-2 mt-1">
                          <div className="flex-1 h-1.5 rounded-full bg-muted overflow-hidden">
                            <div
                              className="h-full rounded-full transition-all"
                              style={{ width: `${pct}%`, backgroundColor: COLORS.merch }}
                            />
                          </div>
                          <span className="text-xs text-muted-foreground shrink-0">{formatNumber(product.unitsSold)} units</span>
                        </div>
                      </div>
                    </div>
                  );
                })}
              </div>
            </CardContent>
          </DashboardCard>
        )}

        {/* Category Breakdown */}
        {categories.length > 0 && totalCategoryRevenue > 0 && (
          <DashboardCard>
          <CardHeader>
            <CardTitle>Product Categories</CardTitle>
            <CardDescription>Revenue by Product Type</CardDescription>
          </CardHeader>
          <CardContent>
            <div className="flex flex-col gap-3">
              {/* Stacked bar */}
              <div className="flex h-3 rounded-full overflow-hidden">
                {categories.map((cat, i) => {
                  const pct = (cat.revenue / totalCategoryRevenue) * 100;
                  if (pct < 1) return null;
                  return (
                    <div
                      key={i}
                      className="h-full transition-all"
                      style={{
                        width: `${pct}%`,
                        backgroundColor: categoryColors[i % categoryColors.length],
                        opacity: i === 0 ? 1 : 0.7 - (i * 0.1),
                      }}
                    />
                  );
                })}
              </div>

              {/* Detail rows */}
              <div className="flex flex-col">
                {categories.map((cat, i) => {
                  const pct = totalCategoryRevenue > 0 ? Math.round((cat.revenue / totalCategoryRevenue) * 100) : 0;
                  return (
                    <div key={i} className={`flex items-center justify-between py-2.5 ${i < categories.length - 1 ? "border-b border-border" : ""}`}>
                      <div className="flex items-center gap-2">
                        <div
                          className="size-3 rounded-full shrink-0"
                          style={{ backgroundColor: categoryColors[i % categoryColors.length], opacity: i === 0 ? 1 : 0.7 - (i * 0.1) }}
                        />
                        <div className="flex flex-col gap-0.5">
                          <span className="text-sm font-medium">{toTitleCase(cat.category)}</span>
                          <span className="text-xs text-muted-foreground">
                            {formatNumber(cat.units)} units · {formatNumber(cat.orders)} orders
                          </span>
                        </div>
                      </div>
                      <div className="flex flex-col items-end gap-0.5">
                        <span className="text-sm font-semibold tabular-nums">{formatCurrency(cat.revenue)}</span>
                        <span className="text-xs text-muted-foreground">{pct}%</span>
                      </div>
                    </div>
                  );
                })}
              </div>
            </div>
          </CardContent>
        </DashboardCard>
      )}
      </div>

    </div>
  );
}

function MerchBuyerBreakdown({ breakdown }: { breakdown: MerchCustomerBreakdown }) {
  const subPct = breakdown.total.revenue > 0
    ? Math.round((breakdown.subscriber.revenue / breakdown.total.revenue) * 100)
    : 0;
  const nonSubPct = 100 - subPct;

  const rows = [
    {
      label: "Auto-Renew",
      icon: Recycle,
      color: SECTION_COLORS["growth-auto"],
      orders: breakdown.subscriber.orders,
      revenue: breakdown.subscriber.revenue,
      customers: breakdown.subscriber.customers,
      pct: subPct,
    },
    {
      label: "Non Auto-Renew",
      icon: RecycleOff,
      color: SECTION_COLORS["growth-non-auto"],
      orders: breakdown.nonSubscriber.orders,
      revenue: breakdown.nonSubscriber.revenue,
      customers: breakdown.nonSubscriber.customers,
      pct: nonSubPct,
    },
  ];

  return (
    <div className="flex flex-col gap-3">
      {/* Revenue split bar */}
      <div className="flex h-3 rounded-full overflow-hidden">
        {subPct > 0 && (
          <div
            className="h-full transition-all"
            style={{ width: `${subPct}%`, backgroundColor: SECTION_COLORS["growth-auto"] }}
          />
        )}
        {nonSubPct > 0 && (
          <div
            className="h-full transition-all"
            style={{ width: `${nonSubPct}%`, backgroundColor: SECTION_COLORS["growth-non-auto"], opacity: 0.5 }}
          />
        )}
      </div>

      {/* Detail rows */}
      <div className="flex flex-col">
        {rows.map((row, i) => (
          <div key={i} className={`flex items-center justify-between py-2.5 ${i < rows.length - 1 ? "border-b border-border" : ""}`}>
            <div className="flex items-center gap-2">
              <row.icon className="size-4 shrink-0" style={{ color: row.color }} />
              <div className="flex flex-col gap-0.5">
                <span className="text-sm font-medium">{row.label}</span>
                <span className="text-xs text-muted-foreground">
                  {formatNumber(row.customers)} customers · {formatNumber(row.orders)} orders
                </span>
              </div>
            </div>
            <div className="flex flex-col items-end gap-0.5">
              <span className="text-sm font-semibold tabular-nums">{formatCurrency(row.revenue)}</span>
              <span className="text-xs text-muted-foreground tabular-nums">{row.pct}%</span>
            </div>
          </div>
        ))}
      </div>

      {/* Total */}
      <div className="flex justify-between items-center pt-2 border-t border-border">
        <span className="text-sm text-muted-foreground">Total</span>
        <span className="text-sm font-semibold tabular-nums">{formatCurrency(breakdown.total.revenue)}</span>
      </div>
    </div>
  );
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
//  SPA & WELLNESS TAB — Revenue + customer behavior analytics
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

function SpaRevenueTab({ spa }: { spa: SpaData }) {
  const now = new Date();
  const currentMonthKey = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, "0")}`;
  const cb = spa.customerBehavior;

  // Monthly revenue chart (completed months, last 12)
  const completedMonthly = spa.monthlyRevenue
    .filter((m) => m.month < currentMonthKey)
    .sort((a, b) => a.month.localeCompare(b.month));
  const revenueChartData = completedMonthly.slice(-12).map((m) => ({
    month: formatShortMonth(m.month),
    gross: m.gross,
  }));

  const revenueChartConfig = {
    gross: { label: "Revenue", color: COLORS.spa },
  } satisfies ChartConfig;

  // Monthly visits chart (from behavior data, sorted chronologically)
  const visitsChartData = cb
    ? [...cb.monthlyVisits].sort((a, b) => a.month.localeCompare(b.month)).slice(-12).map((m) => ({
        month: formatShortMonth(m.month),
        visits: m.visits,
        uniqueVisitors: m.uniqueVisitors,
      }))
    : [];

  const visitsChartConfig = {
    visits: { label: "Total Visits", color: COLORS.spa },
    uniqueVisitors: { label: "Unique Visitors", color: "#B87333" },
  } satisfies ChartConfig;

  // Service breakdown colors
  const serviceColors = ["#6B8E9B", "#B87333", "#4A7C59", "#8B6FA5", "#8F7A5E"];
  const totalServiceRevenue = spa.serviceBreakdown.reduce((s, svc) => s + svc.totalRevenue, 0);

  // Crossover/subscriber colors
  const crossoverColor = "#4A7C59";     // green = takes classes
  const spaOnlyColor = "#B87333";       // copper = spa only
  const subscriberColor = COLORS.member; // subscriber green
  const nonSubColor = "#9CA3AF";         // gray for non-subscriber

  // Frequency data
  const totalFreqCustomers = cb ? cb.frequency.reduce((s, f) => s + f.customers, 0) : 0;

  // Categorize subscriber plans into Member / Sky3 / TV / Other
  const planCategories = cb ? (() => {
    let memberCount = 0, sky3Count = 0, tvCount = 0, otherCount = 0;
    for (const p of cb.subscriberPlans) {
      const name = p.planName.toUpperCase();
      if (name.includes("TV") || name.includes("ON DEMAND")) tvCount += p.customers;
      else if (name.includes("SKY3") || name.includes("SKYHIGH3")) sky3Count += p.customers;
      else if (name.includes("MEMBER") || name.includes("UNLIMITED") || name.includes("10MEMBER") || name.includes("ALL ACCESS") || name.includes("VIRGIN") || name.includes("TING FAM")) memberCount += p.customers;
      else otherCount += p.customers;
    }
    return [
      { label: "Member", count: memberCount, color: COLORS.member },
      { label: "Sky3", count: sky3Count, color: COLORS.sky3 },
      { label: "Sky Ting TV", count: tvCount, color: COLORS.tv },
      ...(otherCount > 0 ? [{ label: "Other", count: otherCount, color: "#9CA3AF" }] : []),
    ].filter(c => c.count > 0);
  })() : [];

  return (
    <div className="flex flex-col gap-4">
      {/* KPI row */}
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
        <DashboardCard>
          <CardHeader>
            <CardDescription>MTD Revenue</CardDescription>
            <CardTitle className="text-2xl font-semibold tabular-nums">{formatCurrency(spa.mtdRevenue)}</CardTitle>
          </CardHeader>
          <CardFooter className="text-sm text-muted-foreground">Month to Date</CardFooter>
        </DashboardCard>

        <DashboardCard>
          <CardHeader>
            <CardDescription>Avg Monthly</CardDescription>
            <CardTitle className="text-2xl font-semibold tabular-nums">{formatCurrency(spa.avgMonthlyRevenue)}</CardTitle>
          </CardHeader>
          <CardFooter className="text-sm text-muted-foreground">{completedMonthly.length} Completed Months</CardFooter>
        </DashboardCard>

        <DashboardCard>
          <CardHeader>
            <CardDescription>Unique Customers</CardDescription>
            <CardTitle className="text-2xl font-semibold tabular-nums">{cb ? formatNumber(cb.uniqueCustomers) : "—"}</CardTitle>
          </CardHeader>
          <CardFooter className="text-sm text-muted-foreground">All Time</CardFooter>
        </DashboardCard>

        <DashboardCard>
          <CardHeader>
            <CardDescription>Repeat Rate</CardDescription>
            <CardTitle className="text-2xl font-semibold tabular-nums">
              {cb ? `${Math.round(((cb.uniqueCustomers - (cb.frequency.find(f => f.bucket === "1 visit")?.customers ?? 0)) / cb.uniqueCustomers) * 100)}%` : "—"}
            </CardTitle>
          </CardHeader>
          <CardFooter className="text-sm text-muted-foreground">2+ visits</CardFooter>
        </DashboardCard>
      </div>

      {/* Monthly Revenue — area chart (matches Revenue Overview style) */}
      {revenueChartData.length > 1 && (
        <DashboardCard>
          <CardHeader>
            <CardTitle>Monthly Revenue</CardTitle>
            <CardDescription>Last 12 Completed Months</CardDescription>
          </CardHeader>
          <CardContent>
            <ChartContainer config={revenueChartConfig} className="h-[250px] w-full">
              <RAreaChart accessibilityLayer data={revenueChartData} margin={{ top: 20, left: 24, right: 24 }}>
                <defs>
                  <linearGradient id="fillSpaGross" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="5%" stopColor="var(--color-gross)" stopOpacity={0.8} />
                    <stop offset="95%" stopColor="var(--color-gross)" stopOpacity={0.1} />
                  </linearGradient>
                </defs>
                <CartesianGrid vertical={false} />
                <YAxis hide domain={["dataMin - 2000", "dataMax + 1000"]} />
                <XAxis
                  dataKey="month"
                  tickLine={false}
                  axisLine={false}
                  tickMargin={8}
                  fontSize={12}
                />
                <ChartTooltip content={<ChartTooltipContent formatter={(v) => formatCurrency(Number(v))} />} />
                <Area
                  dataKey="gross"
                  type="natural"
                  fill="url(#fillSpaGross)"
                  fillOpacity={0.4}
                  stroke="var(--color-gross)"
                  strokeWidth={2}
                  dot={{ fill: "var(--color-gross)" }}
                  activeDot={{ r: 6 }}
                >
                  <LabelList
                    position="top"
                    offset={12}
                    className="fill-foreground"
                    fontSize={12}
                    formatter={(v: number) => formatCompactCurrency(v)}
                  />
                </Area>
              </RAreaChart>
            </ChartContainer>
          </CardContent>
        </DashboardCard>
      )}

      {/* Monthly Visits — area chart */}
      <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
        {visitsChartData.length > 0 && (
          <DashboardCard>
            <CardHeader>
              <CardTitle>Monthly Visits</CardTitle>
              <CardDescription>Visits and Unique Visitors</CardDescription>
            </CardHeader>
            <CardContent>
              <ChartContainer config={visitsChartConfig} className="h-[200px] w-full">
                <BarChart data={visitsChartData} margin={{ top: 4, right: 4, left: 4, bottom: 0 }}>
                  <CartesianGrid vertical={false} strokeDasharray="3 3" />
                  <XAxis dataKey="month" tickLine={false} axisLine={false} fontSize={11} />
                  <YAxis tickLine={false} axisLine={false} fontSize={11} />
                  <ChartTooltip content={<ChartTooltipContent />} />
                  <ChartLegend content={<ChartLegendContent />} />
                  <Bar dataKey="visits" fill={COLORS.spa} radius={[4, 4, 0, 0]} />
                  <Bar dataKey="uniqueVisitors" fill="#B87333" radius={[4, 4, 0, 0]} />
                </BarChart>
              </ChartContainer>
            </CardContent>
          </DashboardCard>
        )}
      </div>

      {/* Customer Behavior section */}
      {cb && (
        <>
          {/* Who Are Spa Customers? — Crossover + Subscriber overlap side by side */}
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
            {/* Crossover: classes vs spa-only */}
            <DashboardCard>
              <CardHeader>
                <CardTitle>Class Crossover</CardTitle>
                <CardDescription>Do Spa Customers Also Take Classes?</CardDescription>
              </CardHeader>
              <CardContent>
                <div className="flex flex-col gap-3">
                  {/* Split bar */}
                  <div className="flex h-3 rounded-full overflow-hidden">
                    {cb.crossover.alsoTakeClasses > 0 && (
                      <div className="h-full" style={{ width: `${Math.round((cb.crossover.alsoTakeClasses / cb.crossover.total) * 100)}%`, backgroundColor: crossoverColor }} />
                    )}
                    {cb.crossover.spaOnly > 0 && (
                      <div className="h-full" style={{ width: `${Math.round((cb.crossover.spaOnly / cb.crossover.total) * 100)}%`, backgroundColor: spaOnlyColor }} />
                    )}
                  </div>

                  {/* Detail rows */}
                  {[
                    { label: "Also take classes", count: cb.crossover.alsoTakeClasses, color: crossoverColor, pct: Math.round((cb.crossover.alsoTakeClasses / cb.crossover.total) * 100) },
                    { label: "Spa only", count: cb.crossover.spaOnly, color: spaOnlyColor, pct: Math.round((cb.crossover.spaOnly / cb.crossover.total) * 100) },
                  ].map((row, i) => (
                    <div key={i} className={`flex items-center justify-between py-2.5 ${i === 0 ? "border-b border-border" : ""}`}>
                      <div className="flex items-center gap-2">
                        <span className="inline-block size-2.5 rounded-full shrink-0" style={{ backgroundColor: row.color }} />
                        <span className="text-sm font-medium">{row.label}</span>
                      </div>
                      <div className="flex flex-col items-end gap-0.5">
                        <span className="text-sm font-semibold tabular-nums">{formatNumber(row.count)}</span>
                        <span className="text-xs text-muted-foreground tabular-nums">{row.pct}%</span>
                      </div>
                    </div>
                  ))}

                  <div className="flex justify-between items-center pt-2 border-t border-border">
                    <span className="text-sm text-muted-foreground">Total Spa Customers</span>
                    <span className="text-sm font-semibold tabular-nums">{formatNumber(cb.crossover.total)}</span>
                  </div>
                </div>
              </CardContent>
            </DashboardCard>

            {/* Subscriber overlap */}
            <DashboardCard>
              <CardHeader>
                <CardTitle>Subscriber Overlap</CardTitle>
                <CardDescription>Are Spa Customers Auto-Renew Subscribers?</CardDescription>
              </CardHeader>
              <CardContent>
                <div className="flex flex-col gap-3">
                  {/* Split bar */}
                  <div className="flex h-3 rounded-full overflow-hidden">
                    {cb.subscriberOverlap.areSubscribers > 0 && (
                      <div className="h-full" style={{ width: `${Math.round((cb.subscriberOverlap.areSubscribers / cb.subscriberOverlap.total) * 100)}%`, backgroundColor: subscriberColor }} />
                    )}
                    {cb.subscriberOverlap.notSubscribers > 0 && (
                      <div className="h-full" style={{ width: `${Math.round((cb.subscriberOverlap.notSubscribers / cb.subscriberOverlap.total) * 100)}%`, backgroundColor: nonSubColor }} />
                    )}
                  </div>

                  {/* Detail rows */}
                  {[
                    { label: "Subscribers", count: cb.subscriberOverlap.areSubscribers, color: subscriberColor, pct: Math.round((cb.subscriberOverlap.areSubscribers / cb.subscriberOverlap.total) * 100) },
                    { label: "Non-Subscribers", count: cb.subscriberOverlap.notSubscribers, color: nonSubColor, pct: Math.round((cb.subscriberOverlap.notSubscribers / cb.subscriberOverlap.total) * 100) },
                  ].map((row, i) => (
                    <div key={i} className={`flex items-center justify-between py-2.5 ${i === 0 ? "border-b border-border" : ""}`}>
                      <div className="flex items-center gap-2">
                        <span className="inline-block size-2.5 rounded-full shrink-0" style={{ backgroundColor: row.color }} />
                        <span className="text-sm font-medium">{row.label}</span>
                      </div>
                      <div className="flex flex-col items-end gap-0.5">
                        <span className="text-sm font-semibold tabular-nums">{formatNumber(row.count)}</span>
                        <span className="text-xs text-muted-foreground tabular-nums">{row.pct}%</span>
                      </div>
                    </div>
                  ))}

                  {/* Plan breakdown for subscribers */}
                  {planCategories.length > 0 && (
                    <div className="pt-2 border-t border-border">
                      <span className="text-xs font-medium text-muted-foreground uppercase tracking-wide">By Plan Type</span>
                      <div className="mt-2 flex flex-col gap-1.5">
                        {planCategories.map((cat, i) => (
                          <div key={i} className="flex items-center justify-between">
                            <div className="flex items-center gap-2">
                              <span className="inline-block size-2 rounded-full shrink-0" style={{ backgroundColor: cat.color }} />
                              <span className="text-sm text-muted-foreground">{cat.label}</span>
                            </div>
                            <span className="text-sm font-medium tabular-nums">{formatNumber(cat.count)}</span>
                          </div>
                        ))}
                      </div>
                    </div>
                  )}
                </div>
              </CardContent>
            </DashboardCard>
          </div>

          {/* Visit Frequency + Service Breakdown side by side */}
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
            {/* Visit frequency */}
            <DashboardCard>
              <CardHeader>
                <CardTitle>Visit Frequency</CardTitle>
                <CardDescription>How Often Do Customers Visit the Spa?</CardDescription>
              </CardHeader>
              <CardContent>
                <div className="flex flex-col gap-2">
                  {cb.frequency.map((f, i) => {
                    const pct = totalFreqCustomers > 0 ? Math.round((f.customers / totalFreqCustomers) * 100) : 0;
                    return (
                      <div key={i} className="flex items-center gap-3">
                        <span className="text-sm text-muted-foreground w-20 shrink-0">{f.bucket}</span>
                        <div className="flex-1 h-5 bg-muted/50 rounded overflow-hidden relative">
                          <div
                            className="h-full rounded transition-all"
                            style={{ width: `${pct}%`, backgroundColor: COLORS.spa, opacity: 0.8 }}
                          />
                          <span className="absolute inset-0 flex items-center px-2 text-xs font-medium tabular-nums">
                            {formatNumber(f.customers)} ({pct}%)
                          </span>
                        </div>
                      </div>
                    );
                  })}
                </div>
              </CardContent>
            </DashboardCard>

            {/* Service breakdown */}
            <DashboardCard>
              <CardHeader>
                <CardTitle>Revenue by Service</CardTitle>
                <CardDescription>All-Time Revenue Breakdown</CardDescription>
              </CardHeader>
              <CardContent>
                <div className="flex flex-col gap-3">
                  {/* Stacked bar */}
                  <div className="flex h-3 rounded-full overflow-hidden">
                    {spa.serviceBreakdown.map((svc, i) => {
                      const pct = totalServiceRevenue > 0 ? (svc.totalRevenue / totalServiceRevenue) * 100 : 0;
                      return pct > 0 ? (
                        <div key={i} className="h-full" style={{ width: `${pct}%`, backgroundColor: serviceColors[i % serviceColors.length] }} />
                      ) : null;
                    })}
                  </div>

                  {/* Detail rows */}
                  <div className="flex flex-col">
                    {spa.serviceBreakdown.map((svc, i) => {
                      const pct = totalServiceRevenue > 0 ? Math.round((svc.totalRevenue / totalServiceRevenue) * 100) : 0;
                      return (
                        <div key={i} className={`flex items-center justify-between py-2.5 ${i < spa.serviceBreakdown.length - 1 ? "border-b border-border" : ""}`}>
                          <div className="flex items-center gap-2">
                            <span className="inline-block size-2.5 rounded-full shrink-0" style={{ backgroundColor: serviceColors[i % serviceColors.length] }} />
                            <span className="text-sm font-medium">{svc.category}</span>
                          </div>
                          <div className="flex flex-col items-end gap-0.5">
                            <span className="text-sm font-semibold tabular-nums">{formatCurrency(svc.totalRevenue)}</span>
                            <span className="text-xs text-muted-foreground tabular-nums">{pct}%</span>
                          </div>
                        </div>
                      );
                    })}
                  </div>

                  <div className="flex justify-between items-center pt-2 border-t border-border">
                    <span className="text-sm text-muted-foreground">Total</span>
                    <span className="text-sm font-semibold tabular-nums">{formatCurrency(totalServiceRevenue)}</span>
                  </div>
                </div>
              </CardContent>
            </DashboardCard>
          </div>
        </>
      )}
    </div>
  );
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
//  RENTAL REVENUE TAB — Studio & Teacher rentals from spreadsheet
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

function RentalRevenueTab({ rental }: { rental: RentalRevenueData }) {
  const now = new Date();
  const currentYear = now.getFullYear();
  const currentMonthKey = `${currentYear}-${String(now.getMonth() + 1).padStart(2, "0")}`;

  // Monthly data for chart (completed months only)
  const completedMonthly = rental.monthly
    .filter((m) => m.month < currentMonthKey)
    .sort((a, b) => a.month.localeCompare(b.month));

  const chartData = completedMonthly.slice(-12).map((m) => ({
    month: formatShortMonth(m.month),
    studioRental: m.studioRental,
    teacherRentals: m.teacherRentals,
    total: m.total,
  }));

  // Annual data
  const annualData = rental.annual.sort((a, b) => a.year - b.year);

  // KPI computations
  const totalAllTime = rental.monthly.reduce((s, m) => s + m.total, 0);
  const currentYearData = annualData.find((a) => a.year === currentYear);
  const ytdRevenue = currentYearData?.total ?? 0;
  const avgMonthly = completedMonthly.length > 0
    ? completedMonthly.reduce((s, m) => s + m.total, 0) / completedMonthly.length
    : 0;

  // Split totals for all-time
  const totalStudio = rental.monthly.reduce((s, m) => s + m.studioRental, 0);
  const totalTeacher = rental.monthly.reduce((s, m) => s + m.teacherRentals, 0);
  const studioPct = totalAllTime > 0 ? Math.round((totalStudio / totalAllTime) * 100) : 0;

  const rentalChartConfig = {
    studioRental: { label: "Studio Rental", color: "#6B8E9B" },
    teacherRentals: { label: "Teacher Rentals", color: "#B8860B" },
  } satisfies ChartConfig;

  const annualChartConfig = {
    total: { label: "Total", color: "#6B8E9B" },
  } satisfies ChartConfig;

  const annualBarData = annualData.map((a) => ({
    year: String(a.year),
    total: a.total,
    studioRental: a.studioRental,
    teacherRentals: a.teacherRentals,
  }));

  return (
    <div className="flex flex-col gap-4">
      {/* KPI row */}
      <div className="grid grid-cols-2 gap-3">
        <DashboardCard>
          <CardHeader>
            <CardDescription>{currentYear} YTD</CardDescription>
            <CardTitle className="text-2xl font-semibold tabular-nums">{formatCurrency(ytdRevenue)}</CardTitle>
          </CardHeader>
          <CardFooter className="text-sm text-muted-foreground">Year to Date</CardFooter>
        </DashboardCard>

        <DashboardCard>
          <CardHeader>
            <CardDescription>Avg Monthly</CardDescription>
            <CardTitle className="text-2xl font-semibold tabular-nums">{formatCurrency(avgMonthly)}</CardTitle>
          </CardHeader>
          <CardFooter className="text-sm text-muted-foreground">{completedMonthly.length} Months</CardFooter>
        </DashboardCard>
      </div>

      {/* Monthly Revenue chart — stacked bar */}
      {chartData.length > 0 && (
        <DashboardCard>
          <CardHeader>
            <CardTitle>Monthly Rental Revenue</CardTitle>
            <CardDescription>Studio Rental vs Teacher Rentals (Last 12 Completed Months)</CardDescription>
          </CardHeader>
          <CardContent>
            <ChartContainer config={rentalChartConfig} className="h-[200px] w-full">
              <BarChart data={chartData} margin={{ top: 4, right: 4, left: 4, bottom: 0 }}>
                <CartesianGrid vertical={false} strokeDasharray="3 3" />
                <XAxis dataKey="month" tickLine={false} axisLine={false} fontSize={11} />
                <YAxis tickLine={false} axisLine={false} fontSize={11} tickFormatter={(v: number) => formatCompactCurrency(v)} />
                <ChartTooltip content={<ChartTooltipContent formatter={(v) => formatCurrency(Number(v))} />} />
                <ChartLegend content={<ChartLegendContent />} />
                <Bar dataKey="studioRental" stackId="a" fill="#6B8E9B" radius={[0, 0, 0, 0]} />
                <Bar dataKey="teacherRentals" stackId="a" fill="#B8860B" radius={[4, 4, 0, 0]} />
              </BarChart>
            </ChartContainer>
          </CardContent>
        </DashboardCard>
      )}

      {/* Annual summary + Revenue split side by side */}
      <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
        {/* Annual bar chart */}
        {annualBarData.length > 0 && (
          <DashboardCard>
            <CardHeader>
              <CardTitle>Annual Rental Revenue</CardTitle>
              <CardDescription>By Year</CardDescription>
            </CardHeader>
            <CardContent>
              <ChartContainer config={annualChartConfig} className="h-[200px] w-full">
                <BarChart data={annualBarData} margin={{ top: 4, right: 4, left: 4, bottom: 0 }}>
                  <CartesianGrid vertical={false} strokeDasharray="3 3" />
                  <XAxis dataKey="year" tickLine={false} axisLine={false} fontSize={11} />
                  <YAxis tickLine={false} axisLine={false} fontSize={11} tickFormatter={(v: number) => formatCompactCurrency(v)} />
                  <ChartTooltip content={<ChartTooltipContent formatter={(v) => formatCurrency(Number(v))} />} />
                  <Bar dataKey="total" fill="#6B8E9B" radius={[4, 4, 0, 0]}>
                    <LabelList dataKey="total" position="top" fontSize={11} formatter={(v: number) => formatCompactCurrency(v)} />
                  </Bar>
                </BarChart>
              </ChartContainer>
            </CardContent>
          </DashboardCard>
        )}

        {/* Revenue split — Studio vs Teacher */}
        <DashboardCard>
          <CardHeader>
            <CardTitle>Revenue Split</CardTitle>
            <CardDescription>Studio Rental vs Teacher Rentals (All Time)</CardDescription>
          </CardHeader>
          <CardContent>
            <div className="flex flex-col gap-3">
              {/* Split bar */}
              <div className="flex h-3 rounded-full overflow-hidden">
                {studioPct > 0 && (
                  <div className="h-full transition-all" style={{ width: `${studioPct}%`, backgroundColor: "#6B8E9B" }} />
                )}
                {(100 - studioPct) > 0 && (
                  <div className="h-full transition-all" style={{ width: `${100 - studioPct}%`, backgroundColor: "#B8860B" }} />
                )}
              </div>

              {/* Detail rows */}
              <div className="flex flex-col">
                {[
                  { label: "Studio Rental", color: "#6B8E9B", amount: totalStudio, pct: studioPct },
                  { label: "Teacher Rentals", color: "#B8860B", amount: totalTeacher, pct: 100 - studioPct },
                ].map((row, i) => (
                  <div key={i} className={`flex items-center justify-between py-2.5 ${i === 0 ? "border-b border-border" : ""}`}>
                    <div className="flex items-center gap-2">
                      <span className="inline-block size-2.5 rounded-full shrink-0" style={{ backgroundColor: row.color }} />
                      <span className="text-sm font-medium">{row.label}</span>
                    </div>
                    <div className="flex flex-col items-end gap-0.5">
                      <span className="text-sm font-semibold tabular-nums">{formatCurrency(row.amount)}</span>
                      <span className="text-xs text-muted-foreground tabular-nums">{row.pct}%</span>
                    </div>
                  </div>
                ))}
              </div>

              <div className="flex justify-between items-center pt-2 border-t border-border">
                <span className="text-sm text-muted-foreground">Total</span>
                <span className="text-sm font-semibold tabular-nums">{formatCurrency(totalAllTime)}</span>
              </div>
            </div>
          </CardContent>
        </DashboardCard>
      </div>

      {/* Monthly detail table */}
      <DashboardCard>
        <CardHeader>
          <CardTitle>Monthly Breakdown</CardTitle>
          <CardDescription>All Rental Revenue by Month</CardDescription>
        </CardHeader>
        <CardContent>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-border text-muted-foreground">
                  <th className="text-left py-2 pr-4 font-medium">Month</th>
                  <th className="text-right py-2 px-4 font-medium">Studio Rental</th>
                  <th className="text-right py-2 px-4 font-medium">Teacher Rentals</th>
                  <th className="text-right py-2 pl-4 font-medium">Total</th>
                </tr>
              </thead>
              <tbody>
                {[...rental.monthly].sort((a, b) => b.month.localeCompare(a.month)).map((m) => (
                  <tr key={m.month} className="border-b border-border/50 last:border-0">
                    <td className="py-2 pr-4 font-medium">{formatShortMonth(m.month)}</td>
                    <td className="py-2 px-4 text-right tabular-nums">{m.studioRental > 0 ? formatCurrency(m.studioRental) : "—"}</td>
                    <td className="py-2 px-4 text-right tabular-nums">{m.teacherRentals > 0 ? formatCurrency(m.teacherRentals) : "—"}</td>
                    <td className="py-2 pl-4 text-right font-semibold tabular-nums">{formatCurrency(m.total)}</td>
                  </tr>
                ))}
              </tbody>
              <tfoot>
                <tr className="border-t-2 border-border font-semibold">
                  <td className="py-2 pr-4">Total</td>
                  <td className="py-2 px-4 text-right tabular-nums">{formatCurrency(totalStudio)}</td>
                  <td className="py-2 px-4 text-right tabular-nums">{formatCurrency(totalTeacher)}</td>
                  <td className="py-2 pl-4 text-right tabular-nums">{formatCurrency(totalAllTime)}</td>
                </tr>
              </tfoot>
            </table>
          </div>
        </CardContent>
      </DashboardCard>
    </div>
  );
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
//  DASHBOARD CONTENT — switches visible section based on sidebar
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

function DashboardContent({ activeSection, data, refreshData }: {
  activeSection: SectionKey;
  data: DashboardStats;
  refreshData: () => void;
}) {
  const contentRef = useRef<HTMLDivElement>(null);
  const trends = data.trends;
  const weekly = trends?.weekly || [];
  const monthly = trends?.monthly || [];
  const pacing = trends?.pacing || null;

  // Scroll to top on section change
  useEffect(() => {
    contentRef.current?.scrollTo({ top: 0, behavior: "instant" });
  }, [activeSection]);

  return (
    <div ref={contentRef} className="flex flex-col gap-4" style={{ fontFamily: FONT_SANS }}>
      {/* ── OVERVIEW ── */}
      {activeSection === "overview" && (
        <>
          <div className="mb-2">
            <div className="flex items-center gap-3">
              <Eyeglass className="size-7 shrink-0" style={{ color: SECTION_COLORS.overview }} />
              <h1 className="text-3xl font-semibold tracking-tight">Overview</h1>
            </div>
            <p className="text-sm text-muted-foreground mt-1 ml-10">Key performance metrics at a glance</p>
          </div>
          {data.overviewData ? (
            <OverviewSection data={data.overviewData} />
          ) : (
            <NoData label="Overview data not available" />
          )}
        </>
      )}

      {/* ── REVENUE: REVENUE OVERVIEW ── */}
      {activeSection === "revenue" && (
        <div className="flex flex-col gap-4">
          <div className="mb-2">
            <div className="flex items-center gap-3">
              <ClassRevenue className="size-7 shrink-0" style={{ color: SECTION_COLORS.revenue }} />
              <h1 className="text-3xl font-semibold tracking-tight">Revenue Overview</h1>
            </div>
          </div>

          {/* Current month revenue-to-date card (or last completed month if no current data) */}
          {(() => {
            const nowD = new Date();
            const curKey = `${nowD.getFullYear()}-${String(nowD.getMonth() + 1).padStart(2, "0")}`;
            const mr = data.monthlyRevenue || [];
            const curEntry = mr.find((m) => m.month === curKey);
            const prevKey = (() => {
              const d = new Date(nowD.getFullYear(), nowD.getMonth() - 1, 1);
              return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}`;
            })();
            const prevEntry = mr.find((m) => m.month === prevKey);

            // If we have current month data, show MTD with pacing
            if (curEntry && curEntry.gross > 0) {
              const retreatGross = curEntry.retreatGross ?? 0;
              const retreatNet = curEntry.retreatNet ?? 0;
              const curGross = curEntry.gross - retreatGross;
              const curNet = curEntry.net - retreatNet;
              const dayOfMonth = nowD.getDate();
              const daysInMonth = new Date(nowD.getFullYear(), nowD.getMonth() + 1, 0).getDate();
              const pacedGross = daysInMonth > 0 ? Math.round((curGross / dayOfMonth) * daysInMonth) : curGross;
              // For pacing comparison, also subtract retreat from previous month
              const prevRetreat = prevEntry?.retreatGross ?? 0;
              const prevGross = (prevEntry?.gross ?? 0) - prevRetreat;
              const vsLastPct = prevGross > 0 ? Math.round(((pacedGross - prevGross) / prevGross) * 1000) / 10 : null;
              const monthLabel = nowD.toLocaleDateString("en-US", { month: "long", year: "numeric" });

              return (
                <DashboardCard>
                  <CardHeader>
                    <CardDescription>{monthLabel} — Revenue to Date</CardDescription>
                    <CardTitle className="text-3xl font-semibold tabular-nums">{formatCurrency(curGross)}</CardTitle>
                  </CardHeader>
                  <CardContent className="pb-2">
                    <div className="flex flex-wrap gap-x-6 gap-y-1 text-sm">
                      <div>
                        <span className="text-muted-foreground">Net: </span>
                        <span className="font-medium tabular-nums">{formatCurrency(curNet)}</span>
                      </div>
                      <div>
                        <span className="text-muted-foreground">Day {dayOfMonth} of {daysInMonth} · </span>
                        <span className="font-medium tabular-nums">Pacing: {formatCurrency(pacedGross)}</span>
                      </div>
                      {vsLastPct !== null && (
                        <div>
                          <span className="text-muted-foreground">vs {formatShortMonth(prevKey)}: </span>
                          <span className={`font-medium tabular-nums ${vsLastPct >= 0 ? "text-emerald-600" : "text-red-500"}`}>
                            {vsLastPct > 0 ? "+" : ""}{vsLastPct}%
                          </span>
                          <span className="text-muted-foreground"> (paced)</span>
                        </div>
                      )}
                    </div>
                  </CardContent>
                  <CardFooter>
                    <div className="w-full">
                      <div className="h-1.5 rounded-full bg-muted overflow-hidden">
                        <div
                          className="h-full rounded-full transition-all"
                          style={{
                            width: `${Math.min(100, Math.round((dayOfMonth / daysInMonth) * 100))}%`,
                            backgroundColor: SECTION_COLORS.revenue,
                          }}
                        />
                      </div>
                    </div>
                  </CardFooter>
                </DashboardCard>
              );
            }

            // No current month data — show missing data state
            const monthLabel = nowD.toLocaleDateString("en-US", { month: "long", year: "numeric" });
            return (
              <DashboardCard>
                <CardHeader>
                  <CardDescription>{monthLabel} — Revenue to Date</CardDescription>
                  <CardTitle className="text-2xl font-medium text-muted-foreground/60">No data yet</CardTitle>
                </CardHeader>
                <CardContent className="pb-2">
                  <p className="text-sm text-muted-foreground">
                    Revenue data for {monthLabel} hasn&apos;t been uploaded yet. Upload the revenue categories report from Union.fit to see this month&apos;s revenue.
                  </p>
                </CardContent>
                <CardFooter>
                  <div className="w-full">
                    <div className="h-1.5 rounded-full bg-muted overflow-hidden" />
                  </div>
                </CardFooter>
              </DashboardCard>
            );
          })()}

          <RevenueSection data={data} trends={trends} />

          {data.monthlyRevenue && data.monthlyRevenue.length > 0 && (
            <div className="grid gap-3 grid-cols-1 sm:grid-cols-2">
              <MonthBreakdownCard monthlyRevenue={data.monthlyRevenue} monthOverMonth={data.monthOverMonth} />
              <AnnualRevenueCard monthlyRevenue={data.monthlyRevenue} projection={trends?.projection} />
            </div>
          )}

          <div className="grid gap-3 grid-cols-2">
            {data.annualBreakdown && data.annualBreakdown.length > 0 && (
              <div className="min-w-0"><AnnualSegmentBreakdownCard breakdown={data.annualBreakdown} /></div>
            )}
            <div className="min-w-0"><MRRBreakdown data={data} /></div>
          </div>
        </div>
      )}

      {/* ── REVENUE: MERCH ── */}
      {activeSection === "revenue-merch" && (
        <div className="flex flex-col gap-4">
          <div className="mb-2">
            <div className="flex items-center gap-3">
              <ShoppingBag className="size-7 shrink-0" style={{ color: SECTION_COLORS["revenue-merch"] }} />
              <h1 className="text-3xl font-semibold tracking-tight">Merch</h1>
            </div>
            {data.shopify?.lastSyncAt && (
              <ShopifySyncStatus lastSyncAt={data.shopify.lastSyncAt} onSyncComplete={refreshData} />
            )}
          </div>
          {data.shopifyMerch ? (
            <MerchRevenueTab merch={data.shopifyMerch} lastSyncAt={data.shopify?.lastSyncAt} />
          ) : (
            <DashboardCard>
              <CardContent>
                <NoDataInline label="No merch data available. Connect Shopify to see merch revenue." />
              </CardContent>
            </DashboardCard>
          )}
        </div>
      )}

      {/* ── REVENUE: SPA & WELLNESS ── */}
      {activeSection === "revenue-spa" && (
        <div className="flex flex-col gap-4">
          <div className="mb-2">
            <div className="flex items-center gap-3">
              <Droplet className="size-7 shrink-0" style={{ color: SECTION_COLORS["revenue-spa"] }} />
              <h1 className="text-3xl font-semibold tracking-tight">Spa & Wellness</h1>
            </div>
            <p className="text-sm text-muted-foreground mt-1 ml-10">Revenue, visits, and customer behavior analytics</p>
          </div>
          {data.spa ? (
            <SpaRevenueTab spa={data.spa} />
          ) : (
            <DashboardCard>
              <CardContent>
                <NoDataInline label="No spa data available." />
              </CardContent>
            </DashboardCard>
          )}
        </div>
      )}

      {/* ── REVENUE: STUDIO RENTALS ── */}
      {activeSection === "revenue-rentals" && (
        <div className="flex flex-col gap-4">
          <div className="mb-2">
            <div className="flex items-center gap-3">
              <BuildingIcon className="size-7 shrink-0" style={{ color: SECTION_COLORS["revenue-rentals"] }} />
              <h1 className="text-3xl font-semibold tracking-tight">Studio Rentals</h1>
            </div>
            <p className="text-sm text-muted-foreground mt-1 ml-10">Studio and teacher rental revenue from spreadsheet data</p>
          </div>
          {data.rentalRevenue ? (
            <RentalRevenueTab rental={data.rentalRevenue} />
          ) : (
            <DashboardCard>
              <CardContent>
                <NoDataInline label="No rental revenue data available." />
              </CardContent>
            </DashboardCard>
          )}
        </div>
      )}

      {/* ── REVENUE: RETREATS ── */}
      {activeSection === "revenue-retreats" && (
        <div className="flex flex-col gap-4">
          <div className="mb-2">
            <div className="flex items-center gap-3">
              <MountainSun className="size-7 shrink-0" style={{ color: SECTION_COLORS["revenue-retreats"] }} />
              <h1 className="text-3xl font-semibold tracking-tight">Retreats</h1>
            </div>
            <p className="text-sm text-muted-foreground mt-1 ml-10">Retreat revenue (pass-through, excluded from operating totals)</p>
          </div>
          <RetreatRevenueSection monthlyRevenue={data.monthlyRevenue || []} />
        </div>
      )}

      {/* ── GROWTH: AUTO-RENEWS ── */}
      {activeSection === "growth-auto" && (
        <div className="flex flex-col gap-4">
          <div className="mb-2">
            <div className="flex items-center gap-3">
              <Recycle className="size-7 shrink-0" style={{ color: SECTION_COLORS["growth-auto"] }} />
              <h1 className="text-3xl font-semibold tracking-tight">Auto-Renews</h1>
            </div>
            <p className="text-sm text-muted-foreground mt-1 ml-10">Subscriber movement, pacing, and net growth by plan type</p>
          </div>
          {/* Movement block: Members + Sky3 (in-studio plans) */}
          <div>
            <h3 className="text-[15px] font-semibold tracking-tight text-muted-foreground mb-3">In-Studio Plans</h3>
            <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
              <CategoryDetail
                title={LABELS.members}
                color={COLORS.member}
                icon={ArrowBadgeDown}
                count={data.activeSubscribers.member}
                weekly={weekly}
                monthly={monthly}
                pacing={pacing}
                weeklyKeyNew={(r) => r.newMembers}
                weeklyKeyChurn={(r) => r.memberChurn}
                weeklyKeyNet={(r) => r.netMemberGrowth}
                pacingNew={(p) => ({ actual: p.newMembersActual, paced: p.newMembersPaced })}
                pacingChurn={(p) => ({ actual: p.memberCancellationsActual, paced: p.memberCancellationsPaced })}
              />
              <CategoryDetail
                title={LABELS.sky3}
                color={COLORS.sky3}
                icon={BrandSky}
                count={data.activeSubscribers.sky3}
                weekly={weekly}
                monthly={monthly}
                pacing={pacing}
                weeklyKeyNew={(r) => r.newSky3}
                weeklyKeyChurn={(r) => r.sky3Churn}
                weeklyKeyNet={(r) => r.netSky3Growth}
                pacingNew={(p) => ({ actual: p.newSky3Actual, paced: p.newSky3Paced })}
                pacingChurn={(p) => ({ actual: p.sky3CancellationsActual, paced: p.sky3CancellationsPaced })}
              />
            </div>
          </div>

          {/* Digital block: Sky Ting TV */}
          <div>
            <h3 className="text-[15px] font-semibold tracking-tight text-muted-foreground mb-3">Digital</h3>
            <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
              <CategoryDetail
                title={LABELS.tv}
                color={COLORS.tv}
                icon={DeviceTv}
                count={data.activeSubscribers.skyTingTv}
                weekly={weekly}
                monthly={monthly}
                pacing={pacing}
                weeklyKeyNew={(r) => r.newSkyTingTv}
                weeklyKeyChurn={(r) => r.skyTingTvChurn}
                weeklyKeyNet={(r) => r.newSkyTingTv - r.skyTingTvChurn}
              />
            </div>
          </div>
        </div>
      )}

      {/* ── GROWTH: NON-AUTO-RENEWS ── */}
      {activeSection === "growth-non-auto" && (
        <div className="flex flex-col gap-4">
          <div className="mb-2">
            <div className="flex items-center gap-3">
              <RecycleOff className="size-7 shrink-0" style={{ color: SECTION_COLORS["growth-non-auto"] }} />
              <h1 className="text-3xl font-semibold tracking-tight">Non Auto-Renews</h1>
            </div>
            <p className="text-sm text-muted-foreground mt-1 ml-10">Drop-in visits, intro week activity, and one-time purchases</p>
          </div>
          {trends?.dropIns && (
            <div className="flex flex-col gap-3">
              <h3 className="text-[15px] font-semibold tracking-tight text-muted-foreground mb-3">Drop-ins</h3>
              <DropInsSubsection dropIns={trends.dropIns} />
            </div>
          )}
          <div className="flex flex-col gap-3">
            <h3 className="text-[15px] font-semibold tracking-tight text-muted-foreground mb-3">Intro Week</h3>
            <IntroWeekModule introWeek={trends?.introWeek ?? null} />
          </div>
        </div>
      )}

      {/* ── CONVERSION: NEW CUSTOMERS ── */}
      {activeSection === "conversion-new" && (
        <div className="flex flex-col gap-3">
          <div className="mb-2">
            <div className="flex items-center gap-3">
              <UserPlus className="size-7 shrink-0" style={{ color: SECTION_COLORS["conversion-new"] }} />
              <h1 className="text-3xl font-semibold tracking-tight">New Customers</h1>
            </div>
            <p className="text-sm text-muted-foreground mt-1 ml-10">Weekly new customer volume, cohort analysis, and acquisition trends</p>
          </div>
          {(trends?.newCustomerVolume || trends?.newCustomerCohorts) ? (
            <>
              <NewCustomerKPICards volume={trends?.newCustomerVolume ?? null} cohorts={trends?.newCustomerCohorts ?? null} />
              <NewCustomerChartCard volume={trends?.newCustomerVolume ?? null} cohorts={trends?.newCustomerCohorts ?? null} />
            </>
          ) : (
            <NoData label="New Customer data" />
          )}
        </div>
      )}

      {/* ── CONVERSION: ALL NON AUTO-RENEW CUSTOMERS ── */}
      {activeSection === "conversion-pool" && (
        <div className="flex flex-col gap-3">
          <div className="mb-2">
            <div className="flex items-center gap-3">
              <UsersGroup className="size-7 shrink-0" style={{ color: SECTION_COLORS["conversion-pool"] }} />
              <h1 className="text-3xl font-semibold tracking-tight">Non Auto-Renew Customers</h1>
            </div>
            <p className="text-sm text-muted-foreground mt-1 ml-10">Non auto-renew conversion funnel, rates, and time-to-convert analysis</p>
          </div>
          {trends?.conversionPool ? (
            <>
              <ConversionPoolKPICards pool={trends.conversionPool} />
              <ConversionPoolModule pool={trends.conversionPool} />
              <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
                <ConversionTimeCard pool={trends.conversionPool} />
                <ConversionVisitsCard pool={trends.conversionPool} />
              </div>
            </>
          ) : (
            <NoData label="Conversion Pool data" />
          )}
        </div>
      )}

      {/* ── CHURN: MEMBERS ── */}
      {activeSection === "churn-members" && (
        <>
          <div className="mb-2">
            <div className="flex items-center gap-3">
              <ArrowBadgeDown className="size-7 shrink-0" style={{ color: SECTION_COLORS["churn-members"] }} />
              <h1 className="text-3xl font-semibold tracking-tight">Members</h1>
            </div>
            <p className="text-sm text-muted-foreground mt-1 ml-10">Retention metrics, churn trends, milestones, and at-risk members</p>
          </div>
          <ChurnSection churnRates={trends?.churnRates} weekly={weekly} expiringIntroWeeks={trends?.expiringIntroWeeks} subsection="members" />
        </>
      )}

      {/* ── CHURN: SKY3 ── */}
      {activeSection === "churn-sky3" && (
        <>
          <div className="mb-2">
            <div className="flex items-center gap-3">
              <BrandSky className="size-7 shrink-0" style={{ color: SECTION_COLORS["churn-sky3"] }} />
              <h1 className="text-3xl font-semibold tracking-tight">Sky3</h1>
            </div>
            <p className="text-sm text-muted-foreground mt-1 ml-10">User and MRR churn rates for Sky3 subscribers</p>
          </div>
          <ChurnSection churnRates={trends?.churnRates} weekly={weekly} expiringIntroWeeks={trends?.expiringIntroWeeks} subsection="sky3" />
        </>
      )}

      {/* ── CHURN: SKY TING TV ── */}
      {activeSection === "churn-tv" && (
        <>
          <div className="mb-2">
            <div className="flex items-center gap-3">
              <DeviceTv className="size-7 shrink-0" style={{ color: SECTION_COLORS["churn-tv"] }} />
              <h1 className="text-3xl font-semibold tracking-tight">Sky Ting TV</h1>
            </div>
            <p className="text-sm text-muted-foreground mt-1 ml-10">User and MRR churn rates for Sky Ting TV subscribers</p>
          </div>
          <ChurnSection churnRates={trends?.churnRates} weekly={weekly} expiringIntroWeeks={trends?.expiringIntroWeeks} subsection="tv" />
        </>
      )}

      {/* ── CHURN: INTRO WEEK ── */}
      {activeSection === "churn-intro" && (
        <>
          <div className="mb-2">
            <div className="flex items-center gap-3">
              <CalendarWeek className="size-7 shrink-0" style={{ color: SECTION_COLORS["churn-intro"] }} />
              <h1 className="text-3xl font-semibold tracking-tight">Intro Week</h1>
            </div>
            <p className="text-sm text-muted-foreground mt-1 ml-10">Expiring intro week passes and conversion tracking</p>
          </div>
          <ChurnSection churnRates={trends?.churnRates} weekly={weekly} expiringIntroWeeks={trends?.expiringIntroWeeks} introWeekConversion={trends?.introWeekConversion} subsection="intro" />
        </>
      )}

      {/* ── USAGE ── */}
      {activeSection === "usage" && (
        <>
          <div className="mb-2">
            <div className="flex items-center gap-3">
              <ActivityIcon className="size-7 shrink-0" style={{ color: SECTION_COLORS.usage }} />
              <h1 className="text-3xl font-semibold tracking-tight">Usage</h1>
            </div>
            <p className="text-sm text-muted-foreground mt-1 ml-10">Visit Frequency Segments by Plan Type</p>
          </div>
          <UsageSection usage={trends?.usage ?? null} />
        </>
      )}

      {/* ── INSIGHTS ── */}
      {activeSection === "insights" && (
        <>
          <div className="mb-2">
            <div className="flex items-center gap-3">
              <BulbIcon className="size-7 shrink-0" style={{ color: SECTION_COLORS.insights }} />
              <h1 className="text-3xl font-semibold tracking-tight">Insights</h1>
            </div>
            <p className="text-sm text-muted-foreground mt-1 ml-10">Actionable patterns detected in your data</p>
          </div>
          <InsightsSection insights={(data as unknown as { insights?: InsightRow[] | null }).insights ?? null} />
        </>
      )}

      {/* ── DATA ── */}
      {activeSection === "data" && (
        <div className="flex flex-col gap-6">
          <div className="mb-2">
            <div className="flex items-center gap-3">
              <Database className="size-7 shrink-0" style={{ color: SECTION_COLORS.data }} />
              <h1 className="text-3xl font-semibold tracking-tight">Data</h1>
            </div>
            <p className="text-sm text-muted-foreground mt-1 ml-10">Pipeline status, data freshness, and refresh controls</p>
          </div>
          <DataSection lastUpdated={data.lastUpdated} spreadsheetUrl={data.spreadsheetUrl} dataSource={data.dataSource} shopify={data.shopify} />
        </div>
      )}
    </div>
  );
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
//  DASHBOARD VIEW
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

function DashboardView() {
  const [loadState, setLoadState] = useState<DashboardLoadState>({ state: "loading" });

  const fetchStats = React.useCallback(() => {
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

  useEffect(() => { fetchStats(); }, [fetchStats]);

  return (
    <DashboardLayout>
      {(activeSection) => {
        if (loadState.state === "loading") {
          return (
            <div className="flex flex-col gap-6" style={{ fontFamily: FONT_SANS }}>
              <div className="grid gap-3 grid-cols-1 sm:grid-cols-3">
                {[0, 1, 2].map((i) => (
                  <Card key={i}>
                    <Skeleton width={80} height={12} style={{ marginBottom: 8 }} />
                    <Skeleton width={140} height={36} style={{ marginBottom: 8 }} />
                    <Skeleton width={100} height={14} />
                  </Card>
                ))}
              </div>
              <DashboardGrid>
                <div style={{ gridColumn: "span 7" }} className="bento-cell-a1"><ModuleSkeleton /></div>
                <div style={{ gridColumn: "span 5" }} className="bento-cell-a2"><ModuleSkeleton /></div>
                <div style={{ gridColumn: "span 12" }} className="bento-cell-b"><ModuleSkeleton /></div>
              </DashboardGrid>
            </div>
          );
        }

        if (loadState.state === "not-configured") {
          return (
            <div className="flex flex-col items-center justify-center py-20">
              <div className="max-w-md text-center space-y-4">
                <h1 className="text-3xl font-semibold tracking-tight">
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
            <div className="flex flex-col items-center justify-center py-20">
              <div className="max-w-md text-center space-y-4">
                <h1 className="text-3xl font-semibold tracking-tight">
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

        return <DashboardContent activeSection={activeSection} data={loadState.data} refreshData={fetchStats} />;
      }}
    </DashboardLayout>
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
