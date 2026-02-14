"use client";

import { useState, useEffect } from "react";

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
}

type LoadState =
  | { state: "loading" }
  | { state: "loaded"; data: DashboardStats }
  | { state: "not-configured" }
  | { state: "error"; message: string };

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

// ─── Reusable StatCard ──────────────────────────────────────

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
          style={{
            color: "var(--st-text-primary)",
            fontFamily: FONT_SANS,
            fontWeight: 700,
            letterSpacing: "-0.02em",
            lineHeight: 1.1,
            fontSize: isHero ? "3.2rem" : "1.85rem",
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

// ─── Brand Header ───────────────────────────────────────────

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

// ─── Freshness Badge ────────────────────────────────────────

function FreshnessBadge({ lastUpdated, spreadsheetUrl }: { lastUpdated: string | null; spreadsheetUrl?: string }) {
  if (!lastUpdated) return null;

  const date = new Date(lastUpdated);
  const isStale = Date.now() - date.getTime() > 24 * 60 * 60 * 1000; // >24h

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

// ─── Nav Link ───────────────────────────────────────────────

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

// ─── Main Page ──────────────────────────────────────────────

export default function Dashboard() {
  const [loadState, setLoadState] = useState<LoadState>({ state: "loading" });

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

  // ── Loading ──
  if (loadState.state === "loading") {
    return (
      <div className="min-h-screen flex items-center justify-center">
        <p
          style={{
            color: "var(--st-text-secondary)",
            fontFamily: FONT_SANS,
            fontWeight: 500,
            letterSpacing: "0.02em",
          }}
        >
          Loading...
        </p>
      </div>
    );
  }

  // ── Not configured ──
  if (loadState.state === "not-configured") {
    return (
      <div className="min-h-screen flex flex-col items-center justify-center p-8">
        <div className="max-w-md text-center space-y-4">
          <SkyTingLogo />
          <h1
            className="text-4xl"
            style={{ color: "var(--st-text-primary)", fontFamily: FONT_SANS, fontWeight: 700 }}
          >
            Studio Dashboard
          </h1>
          <div
            className="rounded-2xl p-6"
            style={{
              backgroundColor: "var(--st-bg-card)",
              border: "1px solid var(--st-border)",
            }}
          >
            <p style={{ color: "var(--st-text-secondary)", fontFamily: FONT_SANS }}>
              No analytics spreadsheet configured yet.
            </p>
            <p className="mt-2" style={{ color: "var(--st-text-secondary)", fontFamily: FONT_SANS }}>
              <a
                href="/settings"
                className="underline"
                style={{ color: "var(--st-text-primary)" }}
              >
                Configure in Settings
              </a>{" "}
              then run the pipeline to see stats here.
            </p>
          </div>
        </div>
      </div>
    );
  }

  // ── Error ──
  if (loadState.state === "error") {
    return (
      <div className="min-h-screen flex flex-col items-center justify-center p-8">
        <div className="max-w-md text-center space-y-4">
          <SkyTingLogo />
          <h1
            className="text-4xl"
            style={{ color: "var(--st-text-primary)", fontFamily: FONT_SANS, fontWeight: 700 }}
          >
            Studio Dashboard
          </h1>
          <div
            className="rounded-2xl p-5 text-center"
            style={{
              backgroundColor: "#F5EFEF",
              border: "1px solid rgba(160, 64, 64, 0.2)",
            }}
          >
            <p className="font-medium" style={{ color: "var(--st-error)", fontFamily: FONT_SANS }}>
              Unable to load stats
            </p>
            <p
              className="text-sm mt-1"
              style={{ color: "var(--st-error)", opacity: 0.85, fontFamily: FONT_SANS }}
            >
              {loadState.message}
            </p>
          </div>
          <NavLink href="/pipeline">Run Pipeline</NavLink>
        </div>
      </div>
    );
  }

  // ── Loaded ──
  const { data } = loadState;

  return (
    <div className="min-h-screen flex flex-col items-center p-8 pb-16">
      <div className="max-w-3xl w-full space-y-10">
        {/* ── Header ── */}
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
            <p
              style={{
                color: "var(--st-text-secondary)",
                fontFamily: FONT_SANS,
                fontWeight: 400,
                fontSize: "0.95rem",
              }}
            >
              {data.dateRange}
            </p>
          )}
          <FreshnessBadge lastUpdated={data.lastUpdated} spreadsheetUrl={data.spreadsheetUrl} />
        </div>

        {/* ── Hero Stats ── */}
        <div className="grid grid-cols-1 sm:grid-cols-2 gap-6">
          <StatCard
            label="Total Members"
            value={formatNumber(data.activeSubscribers.member)}
            sublabel={`${formatCurrency(data.mrr.member)} MRR`}
            size="hero"
          />
          <StatCard
            label="Total SKY3"
            value={formatNumber(data.activeSubscribers.sky3)}
            sublabel={`${formatCurrency(data.mrr.sky3)} MRR`}
            size="hero"
          />
        </div>

        {/* ── Overview ── */}
        <div className="space-y-4">
          <SectionHeader>Overview</SectionHeader>
          <div className="grid grid-cols-2 sm:grid-cols-4 gap-4">
            <StatCard
              label="All Subscribers"
              value={formatNumber(data.activeSubscribers.total)}
            />
            <StatCard
              label="Total MRR"
              value={formatCurrency(data.mrr.total)}
            />
            <StatCard
              label="Overall ARPU"
              value={formatCurrencyDecimal(data.arpu.overall)}
            />
            <StatCard
              label="SKY TING TV"
              value={formatNumber(data.activeSubscribers.skyTingTv)}
              sublabel={`${formatCurrency(data.mrr.skyTingTv)} MRR`}
            />
          </div>
        </div>

        {/* ── Revenue per Subscriber ── */}
        <div className="space-y-4">
          <SectionHeader>Revenue per Subscriber</SectionHeader>
          <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
            <StatCard
              label="ARPU — Member"
              value={formatCurrencyDecimal(data.arpu.member)}
            />
            <StatCard
              label="ARPU — SKY3"
              value={formatCurrencyDecimal(data.arpu.sky3)}
            />
            <StatCard
              label="ARPU — SKY TING TV"
              value={formatCurrencyDecimal(data.arpu.skyTingTv)}
            />
          </div>
        </div>

        {/* ── Footer ── */}
        <div className="text-center space-y-4 pt-6">
          <div
            className="flex justify-center gap-8 text-sm"
            style={{ color: "var(--st-text-secondary)" }}
          >
            <NavLink href="/pipeline">Pipeline</NavLink>
            <NavLink href="/results">Results</NavLink>
            <NavLink href="/settings">Settings</NavLink>
          </div>
        </div>
      </div>
    </div>
  );
}
