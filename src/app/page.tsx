"use client";

import { useState, useEffect, useRef } from "react";

// ─── Types ───────────────────────────────────────────────────

interface DashboardStats {
  lastUpdated: string | null;
  dateRange: string | null;
  spreadsheetUrl?: string;
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
  trends?: TrendsData | null;
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
}

interface TrendsData {
  weekly: TrendRowData[];
  monthly: TrendRowData[];
  pacing: PacingData | null;
  projection: ProjectionData | null;
}

type DashboardLoadState =
  | { state: "loading" }
  | { state: "loaded"; data: DashboardStats }
  | { state: "not-configured" }
  | { state: "error"; message: string };

type JobStatus =
  | { state: "idle" }
  | { state: "running"; jobId: string; step: string; percent: number }
  | { state: "complete"; sheetUrl: string; duration: number }
  | { state: "error"; message: string };

type AppMode = "loading" | "pipeline" | "dashboard";

// ─── Font constants ─────────────────────────────────────────

const FONT_SANS = "'Helvetica Neue', Helvetica, Arial, sans-serif";
const FONT_BRAND = "'Cormorant Garamond', 'Times New Roman', serif";

// ─── Formatting helpers ──────────────────────────────────────

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
  // "2026-02-10" → "Feb 10"
  const d = new Date(period + "T00:00:00");
  if (isNaN(d.getTime())) return period;
  return d.toLocaleDateString("en-US", { month: "short", day: "numeric" });
}

function formatMonthLabel(period: string): string {
  // "2026-02" → "Feb 2026"
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
      style={{ color: "var(--st-text-secondary)", fontFamily: FONT_SANS, fontWeight: 500 }}
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

function PipelineView() {
  const [status, setStatus] = useState<JobStatus>({ state: "idle" });
  const [hasCredentials, setHasCredentials] = useState<boolean | null>(null);
  const eventSourceRef = useRef<EventSource | null>(null);

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
        setStatus({ state: "running", jobId, step: data.step, percent: data.percent });
      });

      es.addEventListener("complete", (e) => {
        const data = JSON.parse(e.data);
        setStatus({ state: "complete", sheetUrl: data.sheetUrl, duration: data.duration });
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
        {/* Header */}
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

        {/* Credentials warning */}
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

        {/* Main card */}
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

          {/* Progress */}
          {status.state === "running" && (
            <div className="space-y-3">
              <div className="flex justify-between text-sm" style={{ color: "var(--st-text-secondary)", fontFamily: FONT_SANS }}>
                <span>{status.step}</span>
                <span>{status.percent}%</span>
              </div>
              <div
                className="h-1.5 rounded-full overflow-hidden"
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
              <button
                onClick={resetPipeline}
                className="text-xs underline opacity-60 hover:opacity-100 transition-opacity"
                style={{ color: "var(--st-text-secondary)", fontFamily: FONT_SANS }}
              >
                Reset if stuck
              </button>
            </div>
          )}

          {/* Complete */}
          {status.state === "complete" && (
            <div
              className="rounded-2xl p-5 text-center space-y-3"
              style={{
                backgroundColor: "#EFF5F0",
                border: "1px solid rgba(74, 124, 89, 0.2)",
              }}
            >
              <p className="font-medium" style={{ color: "var(--st-success)", fontFamily: FONT_SANS }}>
                Pipeline complete
              </p>
              <p className="text-sm" style={{ color: "var(--st-success)", opacity: 0.8, fontFamily: FONT_SANS }}>
                Finished in {Math.round(status.duration / 1000)}s
              </p>
              <a
                href={status.sheetUrl}
                target="_blank"
                rel="noopener noreferrer"
                className="inline-block mt-1 px-5 py-2 text-sm font-medium uppercase tracking-wider transition-colors"
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
          )}

          {/* Error */}
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

        {/* Footer nav */}
        <div className="flex justify-center gap-8 text-sm" style={{ color: "var(--st-text-secondary)" }}>
          <NavLink href="/settings">Settings</NavLink>
          <NavLink href="/results">Results</NavLink>
        </div>
      </div>
    </div>
  );
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
//  DASHBOARD VIEW (production / Railway)
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

interface StatCardProps {
  label: string;
  value: string;
  sublabel?: string;
  size?: "hero" | "standard";
}

function StatCard({ label, value, sublabel, size = "standard" }: StatCardProps) {
  const isHero = size === "hero";

  return (
    <div
      className="rounded-2xl flex flex-col justify-between"
      style={{
        backgroundColor: "var(--st-bg-card)",
        border: "1px solid var(--st-border)",
        padding: isHero ? "2rem" : "1.5rem",
        minHeight: isHero ? "180px" : "120px",
      }}
    >
      <p
        className="uppercase"
        style={{
          color: "var(--st-text-secondary)",
          fontFamily: FONT_SANS,
          fontWeight: 600,
          letterSpacing: "0.06em",
          fontSize: "0.7rem",
        }}
      >
        {label}
      </p>
      <div>
        <p
          className={isHero ? "stat-hero-value" : ""}
          style={{
            color: "var(--st-text-primary)",
            fontFamily: FONT_SANS,
            fontWeight: 700,
            letterSpacing: "-0.02em",
            lineHeight: 1.1,
            fontSize: isHero ? "3.2rem" : "1.85rem",
            overflow: "hidden",
            textOverflow: "ellipsis",
            whiteSpace: "nowrap",
          }}
        >
          {value}
        </p>
        {sublabel && (
          <p
            className="mt-1"
            style={{
              color: "var(--st-text-secondary)",
              fontFamily: FONT_SANS,
              fontWeight: 500,
              fontSize: "0.85rem",
            }}
          >
            {sublabel}
          </p>
        )}
      </div>
    </div>
  );
}

// ─── Trend Card with Delta Badge ────────────────────────────

interface TrendCardProps {
  label: string;
  value: string;
  delta: number | null;
  deltaPercent: number | null;
  isPositiveGood?: boolean; // default true; for churn, set to false
  isCurrency?: boolean; // format delta with $ sign
  sublabel?: string;
}

function DeltaBadge({ delta, deltaPercent, isPositiveGood = true, isCurrency = false }: { delta: number | null; deltaPercent: number | null; isPositiveGood?: boolean; isCurrency?: boolean }) {
  if (delta == null) return null;

  const isPositive = delta > 0;
  const isGood = isPositiveGood ? isPositive : !isPositive;
  const color = delta === 0 ? "var(--st-text-secondary)" : isGood ? "var(--st-success)" : "var(--st-error)";
  const arrow = delta > 0 ? "\u25B2" : delta < 0 ? "\u25BC" : "";
  const bgColor = delta === 0
    ? "rgba(128, 128, 128, 0.08)"
    : isGood
      ? "rgba(74, 124, 89, 0.08)"
      : "rgba(160, 64, 64, 0.08)";

  return (
    <span
      className="inline-flex items-center gap-1 rounded-full px-2 py-0.5"
      style={{
        color,
        backgroundColor: bgColor,
        fontFamily: FONT_SANS,
        fontWeight: 600,
        fontSize: "0.72rem",
      }}
    >
      {arrow} {isCurrency ? formatDeltaCurrency(delta) : formatDelta(delta)}
      {deltaPercent != null && (
        <span style={{ opacity: 0.75, fontWeight: 500 }}>{formatDeltaPercent(deltaPercent)}</span>
      )}
    </span>
  );
}

function TrendCard({ label, value, delta, deltaPercent, isPositiveGood = true, isCurrency = false, sublabel }: TrendCardProps) {
  return (
    <div
      className="rounded-2xl flex flex-col justify-between"
      style={{
        backgroundColor: "var(--st-bg-card)",
        border: "1px solid var(--st-border)",
        padding: "1.25rem",
        minHeight: "110px",
      }}
    >
      <p
        className="uppercase"
        style={{
          color: "var(--st-text-secondary)",
          fontFamily: FONT_SANS,
          fontWeight: 600,
          letterSpacing: "0.06em",
          fontSize: "0.65rem",
        }}
      >
        {label}
      </p>
      <div>
        <div className="flex items-end gap-2 flex-wrap">
          <p
            style={{
              color: "var(--st-text-primary)",
              fontFamily: FONT_SANS,
              fontWeight: 700,
              letterSpacing: "-0.02em",
              lineHeight: 1.1,
              fontSize: "1.65rem",
            }}
          >
            {value}
          </p>
          <DeltaBadge delta={delta} deltaPercent={deltaPercent} isPositiveGood={isPositiveGood} isCurrency={isCurrency} />
        </div>
        {sublabel && (
          <p
            className="mt-1"
            style={{
              color: "var(--st-text-secondary)",
              fontFamily: FONT_SANS,
              fontWeight: 500,
              fontSize: "0.78rem",
            }}
          >
            {sublabel}
          </p>
        )}
      </div>
    </div>
  );
}

function SectionHeader({ children }: { children: React.ReactNode }) {
  return (
    <h2
      style={{
        color: "var(--st-text-primary)",
        fontFamily: FONT_SANS,
        fontWeight: 700,
        fontSize: "1.15rem",
        letterSpacing: "-0.01em",
        textTransform: "uppercase" as const,
      }}
    >
      {children}
    </h2>
  );
}

function FreshnessBadge({ lastUpdated, spreadsheetUrl }: { lastUpdated: string | null; spreadsheetUrl?: string }) {
  if (!lastUpdated) return null;

  const date = new Date(lastUpdated);
  const isStale = Date.now() - date.getTime() > 24 * 60 * 60 * 1000;

  return (
    <div
      className="flex items-center justify-center gap-3 flex-wrap"
      style={{ fontSize: "0.85rem" }}
    >
      <div
        className="inline-flex items-center gap-2 rounded-full px-4 py-1.5"
        style={{
          backgroundColor: isStale ? "rgba(160, 64, 64, 0.08)" : "rgba(74, 124, 89, 0.08)",
          border: `1px solid ${isStale ? "rgba(160, 64, 64, 0.2)" : "rgba(74, 124, 89, 0.2)"}`,
          fontFamily: FONT_SANS,
          fontWeight: 600,
        }}
      >
        <span
          className="inline-block rounded-full"
          style={{
            width: "7px",
            height: "7px",
            backgroundColor: isStale ? "var(--st-error)" : "var(--st-success)",
          }}
        />
        <span style={{ color: isStale ? "var(--st-error)" : "var(--st-success)" }}>
          Updated {formatRelativeTime(lastUpdated)}
        </span>
        <span style={{ color: "var(--st-text-secondary)", fontWeight: 400 }}>
          {formatDateTime(lastUpdated)}
        </span>
      </div>

      {spreadsheetUrl && (
        <a
          href={spreadsheetUrl}
          target="_blank"
          rel="noopener noreferrer"
          className="inline-flex items-center gap-1.5 rounded-full px-4 py-1.5 transition-colors"
          style={{
            color: "var(--st-text-secondary)",
            border: "1px solid var(--st-border)",
            fontFamily: FONT_SANS,
            fontWeight: 500,
            fontSize: "0.85rem",
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
          <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
            <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z" />
            <polyline points="14 2 14 8 20 8" />
            <line x1="16" y1="13" x2="8" y2="13" />
            <line x1="16" y1="17" x2="8" y2="17" />
            <polyline points="10 9 9 9 8 9" />
          </svg>
          Google Sheet
        </a>
      )}
    </div>
  );
}

// ─── Trends Sections ─────────────────────────────────────────

function WeekOverWeekSection({ weekly }: { weekly: TrendRowData[] }) {
  if (weekly.length < 2) return null;

  // Show the most recent week that has a delta (i.e., not the first one)
  const latest = weekly[weekly.length - 1];

  return (
    <div className="space-y-4">
      <SectionHeader>Week over Week</SectionHeader>
      <p style={{ color: "var(--st-text-secondary)", fontFamily: FONT_SANS, fontSize: "0.82rem", marginTop: "-0.5rem" }}>
        Week of {formatWeekLabel(latest.period)} vs previous week
      </p>
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-4">
        <TrendCard
          label="New Members"
          value={String(latest.newMembers)}
          delta={latest.deltaNewMembers}
          deltaPercent={latest.deltaPctNewMembers}
        />
        <TrendCard
          label="New SKY3"
          value={String(latest.newSky3)}
          delta={latest.deltaNewSky3}
          deltaPercent={latest.deltaPctNewSky3}
        />
        <TrendCard
          label="Revenue Added"
          value={formatCurrency(latest.revenueAdded)}
          delta={latest.deltaRevenue}
          deltaPercent={latest.deltaPctRevenue}
          isCurrency={true}
        />
        <TrendCard
          label="Revenue Lost"
          value={formatCurrency(latest.revenueLost)}
          delta={null}
          deltaPercent={null}
          isPositiveGood={false}
          sublabel="from cancellations"
        />
      </div>
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-4">
        <TrendCard
          label="Member Churn"
          value={String(latest.memberChurn)}
          delta={latest.deltaNewMembers != null ? -(latest.memberChurn - (weekly[weekly.length - 2]?.memberChurn ?? 0)) : null}
          deltaPercent={null}
          isPositiveGood={false}
        />
        <TrendCard
          label="SKY3 Churn"
          value={String(latest.sky3Churn)}
          delta={weekly.length >= 2 ? -(latest.sky3Churn - weekly[weekly.length - 2].sky3Churn) : null}
          deltaPercent={null}
          isPositiveGood={false}
        />
        <TrendCard
          label="Net Member Growth"
          value={formatDelta(latest.netMemberGrowth) || "0"}
          delta={null}
          deltaPercent={null}
        />
        <TrendCard
          label="Net SKY3 Growth"
          value={formatDelta(latest.netSky3Growth) || "0"}
          delta={null}
          deltaPercent={null}
        />
      </div>
    </div>
  );
}

function MonthOverMonthSection({ monthly, pacing }: { monthly: TrendRowData[]; pacing: PacingData | null }) {
  if (monthly.length < 1) return null;

  const latest = monthly[monthly.length - 1];
  const isPacing = pacing && pacing.daysElapsed < pacing.daysInMonth;

  return (
    <div className="space-y-4">
      <SectionHeader>Month over Month</SectionHeader>
      <p style={{ color: "var(--st-text-secondary)", fontFamily: FONT_SANS, fontSize: "0.82rem", marginTop: "-0.5rem" }}>
        {formatMonthLabel(latest.period)}
        {isPacing && (
          <span style={{ color: "var(--st-accent)", fontWeight: 600 }}>
            {" "} ({pacing!.daysElapsed}/{pacing!.daysInMonth} days — pacing shown)
          </span>
        )}
      </p>
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-4">
        <TrendCard
          label="New Members"
          value={isPacing ? `${pacing!.newMembersActual}` : String(latest.newMembers)}
          delta={latest.deltaNewMembers}
          deltaPercent={latest.deltaPctNewMembers}
          sublabel={isPacing ? `Pacing: ${pacing!.newMembersPaced} projected` : undefined}
        />
        <TrendCard
          label="New SKY3"
          value={isPacing ? `${pacing!.newSky3Actual}` : String(latest.newSky3)}
          delta={latest.deltaNewSky3}
          deltaPercent={latest.deltaPctNewSky3}
          sublabel={isPacing ? `Pacing: ${pacing!.newSky3Paced} projected` : undefined}
        />
        <TrendCard
          label="Revenue Added"
          value={isPacing ? formatCurrency(pacing!.revenueActual) : formatCurrency(latest.revenueAdded)}
          delta={latest.deltaRevenue}
          deltaPercent={latest.deltaPctRevenue}
          isCurrency={true}
          sublabel={isPacing ? `Pacing: ${formatCurrency(pacing!.revenuePaced)}` : undefined}
        />
        <TrendCard
          label="Member Churn"
          value={isPacing ? `${pacing!.memberCancellationsActual}` : String(latest.memberChurn)}
          delta={null}
          deltaPercent={null}
          isPositiveGood={false}
          sublabel={isPacing ? `Pacing: ${pacing!.memberCancellationsPaced} projected` : undefined}
        />
      </div>
    </div>
  );
}

function RevenueProjectionSection({ projection }: { projection: ProjectionData }) {
  return (
    <div className="space-y-4">
      <SectionHeader>{projection.year} Revenue Forecast</SectionHeader>
      <div className="grid grid-cols-1 sm:grid-cols-2 gap-6">
        <StatCard
          label={`Est. ${projection.year} Annual Revenue`}
          value={formatCurrency(projection.projectedAnnualRevenue)}
          sublabel={`Based on ${projection.monthlyGrowthRate > 0 ? "+" : ""}${projection.monthlyGrowthRate}% monthly growth`}
          size="hero"
        />
        <div className="grid grid-rows-3 gap-4">
          <div
            className="rounded-2xl flex items-center justify-between"
            style={{
              backgroundColor: "var(--st-bg-card)",
              border: "1px solid var(--st-border)",
              padding: "1rem 1.25rem",
            }}
          >
            <p style={{ color: "var(--st-text-secondary)", fontFamily: FONT_SANS, fontWeight: 600, fontSize: "0.7rem", textTransform: "uppercase", letterSpacing: "0.06em" }}>
              Current MRR
            </p>
            <p style={{ color: "var(--st-text-primary)", fontFamily: FONT_SANS, fontWeight: 700, fontSize: "1.3rem" }}>
              {formatCurrency(projection.currentMRR)}
            </p>
          </div>
          <div
            className="rounded-2xl flex items-center justify-between"
            style={{
              backgroundColor: "var(--st-bg-card)",
              border: "1px solid var(--st-border)",
              padding: "1rem 1.25rem",
            }}
          >
            <p style={{ color: "var(--st-text-secondary)", fontFamily: FONT_SANS, fontWeight: 600, fontSize: "0.7rem", textTransform: "uppercase", letterSpacing: "0.06em" }}>
              Monthly Growth
            </p>
            <p style={{ color: projection.monthlyGrowthRate >= 0 ? "var(--st-success)" : "var(--st-error)", fontFamily: FONT_SANS, fontWeight: 700, fontSize: "1.3rem" }}>
              {projection.monthlyGrowthRate > 0 ? "+" : ""}{projection.monthlyGrowthRate}%
            </p>
          </div>
          <div
            className="rounded-2xl flex items-center justify-between"
            style={{
              backgroundColor: "var(--st-bg-card)",
              border: "1px solid var(--st-border)",
              padding: "1rem 1.25rem",
            }}
          >
            <p style={{ color: "var(--st-text-secondary)", fontFamily: FONT_SANS, fontWeight: 600, fontSize: "0.7rem", textTransform: "uppercase", letterSpacing: "0.06em" }}>
              Year-End MRR
            </p>
            <p style={{ color: "var(--st-text-primary)", fontFamily: FONT_SANS, fontWeight: 700, fontSize: "1.3rem" }}>
              {formatCurrency(projection.projectedYearEndMRR)}
            </p>
          </div>
        </div>
      </div>
    </div>
  );
}

// ─── Dashboard View ──────────────────────────────────────────

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
        <p style={{ color: "var(--st-text-secondary)", fontFamily: FONT_SANS, fontWeight: 500 }}>
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
          <h1 style={{ color: "var(--st-text-primary)", fontFamily: FONT_SANS, fontWeight: 700, fontSize: "2.4rem" }}>
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
          <h1 style={{ color: "var(--st-text-primary)", fontFamily: FONT_SANS, fontWeight: 700, fontSize: "2.4rem" }}>
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

  return (
    <div className="min-h-screen flex flex-col items-center p-8 pb-16">
      <div className="max-w-3xl w-full space-y-10">
        {/* Header */}
        <div className="text-center space-y-3 pt-4">
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
            Studio Dashboard
          </h1>
          {data.dateRange && (
            <p style={{ color: "var(--st-text-secondary)", fontFamily: FONT_SANS, fontSize: "0.95rem" }}>
              {data.dateRange}
            </p>
          )}
          <FreshnessBadge lastUpdated={data.lastUpdated} spreadsheetUrl={data.spreadsheetUrl} />
        </div>

        {/* ── MEMBERS ──────────────────────────────────── */}
        <div className="space-y-4">
          <SectionHeader>Members</SectionHeader>
          <StatCard label="Active Members" value={formatNumber(data.activeSubscribers.member)} size="hero" />

          {trends && trends.weekly.length >= 2 && (() => {
            const latest = trends.weekly[trends.weekly.length - 1];
            const prev = trends.weekly[trends.weekly.length - 2];
            return (
              <>
                <p style={{ color: "var(--st-text-secondary)", fontFamily: FONT_SANS, fontSize: "0.75rem", fontWeight: 600, letterSpacing: "0.06em", textTransform: "uppercase" as const, marginTop: "0.5rem" }}>
                  Week over Week
                </p>
                <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
                  <TrendCard label="New Members" value={String(latest.newMembers)} delta={latest.deltaNewMembers} deltaPercent={latest.deltaPctNewMembers} />
                  <TrendCard label="Churn" value={String(latest.memberChurn)} delta={prev ? -(latest.memberChurn - prev.memberChurn) : null} deltaPercent={null} isPositiveGood={false} />
                  <TrendCard label="Net Growth" value={formatDelta(latest.netMemberGrowth) || "0"} delta={null} deltaPercent={null} />
                </div>
              </>
            );
          })()}

          {trends && trends.monthly.length >= 1 && (() => {
            const latest = trends.monthly[trends.monthly.length - 1];
            const pacing = trends.pacing;
            const isPacing = pacing && pacing.daysElapsed < pacing.daysInMonth;
            return (
              <>
                <p style={{ color: "var(--st-text-secondary)", fontFamily: FONT_SANS, fontSize: "0.75rem", fontWeight: 600, letterSpacing: "0.06em", textTransform: "uppercase" as const, marginTop: "0.5rem" }}>
                  Month over Month
                  {isPacing && <span style={{ color: "var(--st-accent)" }}> ({pacing!.daysElapsed}/{pacing!.daysInMonth} days)</span>}
                </p>
                <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
                  <TrendCard
                    label="New Members"
                    value={isPacing ? `${pacing!.newMembersActual}` : String(latest.newMembers)}
                    delta={latest.deltaNewMembers} deltaPercent={latest.deltaPctNewMembers}
                    sublabel={isPacing ? `Pacing: ${pacing!.newMembersPaced} projected` : undefined}
                  />
                  <TrendCard
                    label="Churn"
                    value={isPacing ? `${pacing!.memberCancellationsActual}` : String(latest.memberChurn)}
                    delta={null} deltaPercent={null} isPositiveGood={false}
                    sublabel={isPacing ? `Pacing: ${pacing!.memberCancellationsPaced} projected` : undefined}
                  />
                </div>
              </>
            );
          })()}
        </div>

        {/* ── SKY3 ───────────────────────────────────── */}
        <div className="space-y-4">
          <SectionHeader>SKY3</SectionHeader>
          <StatCard label="Active SKY3" value={formatNumber(data.activeSubscribers.sky3)} size="hero" />

          {trends && trends.weekly.length >= 2 && (() => {
            const latest = trends.weekly[trends.weekly.length - 1];
            const prev = trends.weekly[trends.weekly.length - 2];
            return (
              <>
                <p style={{ color: "var(--st-text-secondary)", fontFamily: FONT_SANS, fontSize: "0.75rem", fontWeight: 600, letterSpacing: "0.06em", textTransform: "uppercase" as const, marginTop: "0.5rem" }}>
                  Week over Week
                </p>
                <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
                  <TrendCard label="New SKY3" value={String(latest.newSky3)} delta={latest.deltaNewSky3} deltaPercent={latest.deltaPctNewSky3} />
                  <TrendCard label="Churn" value={String(latest.sky3Churn)} delta={prev ? -(latest.sky3Churn - prev.sky3Churn) : null} deltaPercent={null} isPositiveGood={false} />
                  <TrendCard label="Net Growth" value={formatDelta(latest.netSky3Growth) || "0"} delta={null} deltaPercent={null} />
                </div>
              </>
            );
          })()}

          {trends && trends.monthly.length >= 1 && (() => {
            const latest = trends.monthly[trends.monthly.length - 1];
            const pacing = trends.pacing;
            const isPacing = pacing && pacing.daysElapsed < pacing.daysInMonth;
            return (
              <>
                <p style={{ color: "var(--st-text-secondary)", fontFamily: FONT_SANS, fontSize: "0.75rem", fontWeight: 600, letterSpacing: "0.06em", textTransform: "uppercase" as const, marginTop: "0.5rem" }}>
                  Month over Month
                  {isPacing && <span style={{ color: "var(--st-accent)" }}> ({pacing!.daysElapsed}/{pacing!.daysInMonth} days)</span>}
                </p>
                <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
                  <TrendCard
                    label="New SKY3"
                    value={isPacing ? `${pacing!.newSky3Actual}` : String(latest.newSky3)}
                    delta={latest.deltaNewSky3} deltaPercent={latest.deltaPctNewSky3}
                    sublabel={isPacing ? `Pacing: ${pacing!.newSky3Paced} projected` : undefined}
                  />
                  <TrendCard
                    label="Churn"
                    value={isPacing ? `${pacing!.sky3CancellationsActual}` : String(latest.sky3Churn)}
                    delta={null} deltaPercent={null} isPositiveGood={false}
                    sublabel={isPacing ? `Pacing: ${pacing!.sky3CancellationsPaced} projected` : undefined}
                  />
                </div>
              </>
            );
          })()}
        </div>

        {/* ── SKY TING TV ────────────────────────────── */}
        <div className="space-y-4">
          <SectionHeader>SKY TING TV</SectionHeader>
          <StatCard label="Active Subscribers" value={formatNumber(data.activeSubscribers.skyTingTv)} size="hero" />

          {trends && trends.weekly.length >= 2 && (() => {
            const latest = trends.weekly[trends.weekly.length - 1];
            const prev = trends.weekly[trends.weekly.length - 2];
            const netTvGrowth = latest.newSkyTingTv - latest.skyTingTvChurn;
            return (
              <>
                <p style={{ color: "var(--st-text-secondary)", fontFamily: FONT_SANS, fontSize: "0.75rem", fontWeight: 600, letterSpacing: "0.06em", textTransform: "uppercase" as const, marginTop: "0.5rem" }}>
                  Week over Week
                </p>
                <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
                  <TrendCard label="New Subscribers" value={String(latest.newSkyTingTv)} delta={prev ? latest.newSkyTingTv - prev.newSkyTingTv : null} deltaPercent={null} />
                  <TrendCard label="Churn" value={String(latest.skyTingTvChurn)} delta={prev ? -(latest.skyTingTvChurn - prev.skyTingTvChurn) : null} deltaPercent={null} isPositiveGood={false} />
                  <TrendCard label="Net Growth" value={formatDelta(netTvGrowth) || "0"} delta={null} deltaPercent={null} />
                </div>
              </>
            );
          })()}

          {trends && trends.monthly.length >= 1 && (() => {
            const latest = trends.monthly[trends.monthly.length - 1];
            return (
              <>
                <p style={{ color: "var(--st-text-secondary)", fontFamily: FONT_SANS, fontSize: "0.75rem", fontWeight: 600, letterSpacing: "0.06em", textTransform: "uppercase" as const, marginTop: "0.5rem" }}>
                  Month over Month
                </p>
                <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
                  <TrendCard label="New Subscribers" value={String(latest.newSkyTingTv)} delta={null} deltaPercent={null} />
                  <TrendCard label="Churn" value={String(latest.skyTingTvChurn)} delta={null} deltaPercent={null} isPositiveGood={false} />
                </div>
              </>
            );
          })()}
        </div>

        {/* ── FINANCIAL HEALTH ───────────────────────── */}
        <div className="space-y-4">
          <SectionHeader>Financial Health</SectionHeader>
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
            <StatCard label="Total MRR" value={formatCurrency(data.mrr.total)} size="hero" />
            <StatCard label="Member MRR" value={formatCurrency(data.mrr.member)} size="hero" />
            <StatCard label="SKY3 MRR" value={formatCurrency(data.mrr.sky3)} size="hero" />
            <StatCard label="TV MRR" value={formatCurrency(data.mrr.skyTingTv)} size="hero" />
          </div>
          <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
            <StatCard label="Overall ARPU" value={formatCurrencyDecimal(data.arpu.overall)} />
            <StatCard label="ARPU — Member" value={formatCurrencyDecimal(data.arpu.member)} />
            <StatCard label="ARPU — SKY3" value={formatCurrencyDecimal(data.arpu.sky3)} />
          </div>

          {trends && trends.weekly.length >= 2 && (() => {
            const latest = trends.weekly[trends.weekly.length - 1];
            return (
              <>
                <p style={{ color: "var(--st-text-secondary)", fontFamily: FONT_SANS, fontSize: "0.75rem", fontWeight: 600, letterSpacing: "0.06em", textTransform: "uppercase" as const, marginTop: "0.5rem" }}>
                  Revenue — Week over Week
                </p>
                <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
                  <TrendCard label="Revenue Added" value={formatCurrency(latest.revenueAdded)} delta={latest.deltaRevenue} deltaPercent={latest.deltaPctRevenue} isCurrency={true} />
                  <TrendCard label="Revenue Lost" value={formatCurrency(latest.revenueLost)} delta={null} deltaPercent={null} isPositiveGood={false} sublabel="from cancellations" />
                </div>
              </>
            );
          })()}

          {trends && trends.monthly.length >= 1 && (() => {
            const latest = trends.monthly[trends.monthly.length - 1];
            const pacing = trends.pacing;
            const isPacing = pacing && pacing.daysElapsed < pacing.daysInMonth;
            return (
              <>
                <p style={{ color: "var(--st-text-secondary)", fontFamily: FONT_SANS, fontSize: "0.75rem", fontWeight: 600, letterSpacing: "0.06em", textTransform: "uppercase" as const, marginTop: "0.5rem" }}>
                  Revenue — Month over Month
                  {isPacing && <span style={{ color: "var(--st-accent)" }}> ({pacing!.daysElapsed}/{pacing!.daysInMonth} days)</span>}
                </p>
                <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
                  <TrendCard
                    label="Revenue Added"
                    value={isPacing ? formatCurrency(pacing!.revenueActual) : formatCurrency(latest.revenueAdded)}
                    delta={latest.deltaRevenue} deltaPercent={latest.deltaPctRevenue} isCurrency={true}
                    sublabel={isPacing ? `Pacing: ${formatCurrency(pacing!.revenuePaced)}` : undefined}
                  />
                  <TrendCard label="Revenue Lost" value={formatCurrency(latest.revenueLost)} delta={null} deltaPercent={null} isPositiveGood={false} sublabel="from cancellations" />
                </div>
              </>
            );
          })()}
        </div>

        {/* Revenue Projection */}
        {trends?.projection && (
          <RevenueProjectionSection projection={trends.projection} />
        )}

        {/* Footer */}
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
    fetch("/api/mode")
      .then((res) => res.json())
      .then((data) => setMode(data.mode))
      .catch(() => setMode("pipeline"));
  }, []);

  if (mode === "loading") {
    return (
      <div className="min-h-screen flex items-center justify-center">
        <p style={{ color: "var(--st-text-secondary)", fontFamily: FONT_SANS, fontWeight: 500 }}>
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
