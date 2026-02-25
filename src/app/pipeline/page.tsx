"use client";

import { useState, useEffect, useRef, useCallback } from "react";

// ─── Types ───────────────────────────────────────────────────

interface CategoryStatus {
  state: "pending" | "downloading" | "parsing" | "saved" | "failed" | "skipped";
  recordCount?: number;
  error?: string;
  deliveryMethod?: "direct" | "email";
}

type PipelineStatus =
  | { state: "idle" }
  | { state: "running"; jobId: string; step: string; percent: number; startedAt: number | null; categories?: Record<string, CategoryStatus> | null }
  | { state: "complete"; duration: number; recordCounts: Record<string, number> | null }
  | { state: "error"; message: string };

interface WatermarkEntry {
  lastFetched: string | null;
  highWaterDate: string | null;
  recordCount: number;
}

interface FreshnessData {
  sources: {
    autoRenews: string | null;
    registrations: string | null;
    shopify: string | null;
  };
  watermarks?: Record<string, WatermarkEntry>;
  lastRun: {
    id: number;
    ranAt: string | null;
    durationMs: number | null;
    recordCounts: Record<string, number> | null;
  } | null;
  lastRunAgeMinutes: number | null;
  recentRuns: Array<{
    id: number;
    ranAt: string | null;
    durationMs: number | null;
    recordCounts: Record<string, number> | null;
  }>;
  tableCounts: Record<string, number>;
}

// ─── Helpers ─────────────────────────────────────────────────

function timeAgo(iso: string | null): string {
  if (!iso) return "Never";
  const diff = Date.now() - new Date(iso).getTime();
  const mins = Math.floor(diff / 60_000);
  if (mins < 1) return "Just now";
  if (mins < 60) return `${mins}m ago`;
  const hours = Math.floor(mins / 60);
  if (hours < 24) return `${hours}h ${mins % 60}m ago`;
  const days = Math.floor(hours / 24);
  return `${days}d ${hours % 24}h ago`;
}

function formatDuration(ms: number | null): string {
  if (!ms) return "—";
  const sec = Math.round(ms / 1000);
  const m = Math.floor(sec / 60);
  const s = sec % 60;
  return m > 0 ? `${m}m ${s}s` : `${s}s`;
}

function formatDate(iso: string | null): string {
  if (!iso) return "—";
  return new Date(iso).toLocaleString("en-US", {
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
  });
}

function freshnessColor(minutes: number | null): string {
  if (minutes === null) return "var(--st-text-secondary)";
  if (minutes < 60 * 12) return "var(--st-success)";       // < 12h = fresh
  if (minutes < 60 * 24) return "var(--st-warning)";       // < 24h = getting stale
  return "var(--st-error)";                                  // > 24h = stale
}

// ─── Components ──────────────────────────────────────────────

function StatusDot({ color }: { color: string }) {
  return (
    <span
      style={{
        display: "inline-block",
        width: 8,
        height: 8,
        borderRadius: "50%",
        backgroundColor: color,
        flexShrink: 0,
      }}
    />
  );
}

const CATEGORY_LABELS: Record<string, string> = {
  newCustomers: "New Customers",
  orders: "Orders",
  firstVisits: "First Visits",
  fullRegistrations: "Registrations",
  canceledAutoRenews: "Canceled Auto-Renews",
  activeAutoRenews: "Active Auto-Renews",
  pausedAutoRenews: "Paused Auto-Renews",
  trialingAutoRenews: "Trialing Auto-Renews",
  newAutoRenews: "New Auto-Renews",
  revenueCategories: "Revenue Categories",
  allRegistrations: "All Registrations",
};

function categoryStateColor(state: string): string {
  switch (state) {
    case "saved": return "var(--st-success)";
    case "failed": return "var(--st-error)";
    case "downloading": case "parsing": return "var(--st-accent)";
    case "skipped": return "var(--st-text-secondary)";
    default: return "var(--st-border)"; // pending
  }
}

function categoryStateLabel(cat: CategoryStatus): string {
  switch (cat.state) {
    case "saved": return cat.recordCount !== undefined ? `${cat.recordCount.toLocaleString()} rows` : "Saved";
    case "failed": return cat.error ? cat.error.slice(0, 40) : "Failed";
    case "downloading": return "Downloading...";
    case "parsing": return "Parsing...";
    case "skipped": return "Skipped";
    default: return cat.deliveryMethod === "email" ? "Waiting for email..." : "Pending";
  }
}

function CategoryStatusGrid({ categories }: { categories: Record<string, CategoryStatus> }) {
  // Sort: saved first, then active (downloading/parsing), then pending, then failed
  const order: Record<string, number> = { saved: 0, parsing: 1, downloading: 2, pending: 3, failed: 4, skipped: 5 };
  const sorted = Object.entries(categories).sort(
    ([, a], [, b]) => (order[a.state] ?? 3) - (order[b.state] ?? 3)
  );

  return (
    <div
      className="rounded-lg p-3 mt-2"
      style={{ backgroundColor: "var(--st-card-bg)", border: "1px solid var(--st-border)" }}
    >
      <div className="text-xs font-semibold mb-2" style={{ color: "var(--st-text-secondary)" }}>
        Report Categories
      </div>
      <div className="space-y-1">
        {sorted.map(([key, cat]) => (
          <div key={key} className="flex items-center gap-2 py-0.5">
            <StatusDot color={categoryStateColor(cat.state)} />
            <span className="text-xs flex-1 truncate" style={{ color: "var(--st-text-primary)" }}>
              {CATEGORY_LABELS[key] || key}
            </span>
            {cat.deliveryMethod && (
              <span
                className="text-[10px] px-1.5 py-0.5 rounded uppercase tracking-wide"
                style={{
                  color: "var(--st-text-secondary)",
                  backgroundColor: "var(--st-border)",
                  opacity: 0.8,
                }}
              >
                {cat.deliveryMethod}
              </span>
            )}
            <span
              className="text-xs tabular-nums text-right"
              style={{
                color: cat.state === "failed" ? "var(--st-error)"
                     : cat.state === "saved" ? "var(--st-success)"
                     : "var(--st-text-secondary)",
                minWidth: "80px",
              }}
            >
              {categoryStateLabel(cat)}
            </span>
          </div>
        ))}
      </div>
    </div>
  );
}

function DataSourceRow({ label, timestamp }: { label: string; timestamp: string | null }) {
  const age = timestamp ? Math.floor((Date.now() - new Date(timestamp).getTime()) / 60_000) : null;
  return (
    <div className="flex items-center justify-between py-1.5">
      <span className="text-sm" style={{ color: "var(--st-text-secondary)" }}>{label}</span>
      <div className="flex items-center gap-2">
        <StatusDot color={freshnessColor(age)} />
        <span className="text-sm font-medium tabular-nums" style={{ color: "var(--st-text-primary)" }}>
          {timeAgo(timestamp)}
        </span>
      </div>
    </div>
  );
}

function TableCountRow({ label, count }: { label: string; count: number }) {
  return (
    <div className="flex items-center justify-between py-1">
      <span className="text-xs" style={{ color: "var(--st-text-secondary)" }}>{label}</span>
      <span className="text-xs font-medium tabular-nums" style={{ color: "var(--st-text-primary)" }}>
        {count.toLocaleString()}
      </span>
    </div>
  );
}

// ─── Main Page ───────────────────────────────────────────────

export default function PipelineControlPage() {
  const [status, setStatus] = useState<PipelineStatus>({ state: "idle" });
  const [freshness, setFreshness] = useState<FreshnessData | null>(null);
  const [freshnessLoading, setFreshnessLoading] = useState(true);
  const eventSourceRef = useRef<EventSource | null>(null);
  const refreshTimerRef = useRef<ReturnType<typeof setInterval> | null>(null);

  // ── Fetch freshness on mount and every 60s ──
  const loadFreshness = useCallback(async () => {
    try {
      const res = await fetch("/api/freshness");
      if (res.ok) {
        const data = await res.json();
        setFreshness(data);
      }
    } catch {
      // silently fail, will retry
    } finally {
      setFreshnessLoading(false);
    }
  }, []);

  useEffect(() => {
    loadFreshness();
    refreshTimerRef.current = setInterval(loadFreshness, 60_000);
    return () => {
      if (refreshTimerRef.current) clearInterval(refreshTimerRef.current);
      eventSourceRef.current?.close();
    };
  }, [loadFreshness]);

  // ── Trigger pipeline ──
  async function triggerPipeline() {
    setStatus({ state: "running", jobId: "", step: "Starting...", percent: 0, startedAt: null });

    try {
      const res = await fetch("/api/pipeline", { method: "POST" });
      if (!res.ok) {
        const err = await res.json();
        setStatus({ state: "error", message: err.error || "Failed to start pipeline" });
        return;
      }

      const { jobId } = await res.json();
      setStatus({ state: "running", jobId, step: "Queued...", percent: 0, startedAt: Date.now() });

      // Open SSE connection
      eventSourceRef.current?.close();
      const es = new EventSource(`/api/status?jobId=${jobId}`);
      eventSourceRef.current = es;

      es.addEventListener("progress", (e) => {
        const data = JSON.parse(e.data);
        setStatus((prev) => ({
          state: "running",
          jobId,
          step: data.step,
          percent: data.percent,
          startedAt: prev.state === "running" ? prev.startedAt : Date.now(),
          categories: data.categories || null,
        }));
      });

      es.addEventListener("complete", (e) => {
        const data = JSON.parse(e.data);
        setStatus({
          state: "complete",
          duration: data.duration,
          recordCounts: data.recordCounts || null,
        });
        es.close();
        // Refresh freshness data
        loadFreshness();
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
      setStatus({ state: "error", message: "Network error — is the server running?" });
    }
  }

  // ── Reset stuck pipeline ──
  async function resetPipeline() {
    try {
      await fetch("/api/pipeline", { method: "DELETE" });
      eventSourceRef.current?.close();
      setStatus({ state: "idle" });
    } catch {
      setStatus({ state: "error", message: "Failed to reset queue" });
    }
  }

  // ── ETA calculation ──
  const eta = (() => {
    if (status.state !== "running" || !status.startedAt || status.percent < 5) return null;
    const elapsed = (Date.now() - status.startedAt) / 1000;
    const remaining = (elapsed / status.percent) * (100 - status.percent);
    if (remaining > 3600) return null;
    const m = Math.floor(remaining / 60);
    const s = Math.round(remaining % 60);
    return m > 0 ? `~${m}m ${s}s left` : `~${s}s left`;
  })();

  // ── Derived state ──
  const isStale = freshness?.lastRunAgeMinutes != null && freshness.lastRunAgeMinutes > 60 * 24;
  const canTrigger = status.state === "idle" || status.state === "complete" || status.state === "error";

  return (
    <div
      className="min-h-screen"
      style={{ backgroundColor: "var(--st-bg-primary)" }}
    >
      <div className="max-w-xl mx-auto px-4 py-10 space-y-6">

        {/* ── Header ── */}
        <div className="text-center space-y-1">
          <h1
            className="text-2xl font-bold tracking-tight"
            style={{ color: "var(--st-text-primary)" }}
          >
            Pipeline Control
          </h1>
          <p className="text-sm" style={{ color: "var(--st-text-secondary)" }}>
            Trigger data sync, monitor progress, check freshness
          </p>
        </div>

        {/* ── Trigger Card ── */}
        <div
          className="rounded-xl p-5 space-y-4"
          style={{
            backgroundColor: "var(--st-bg-card)",
            border: "1px solid var(--st-border)",
          }}
        >
          {/* Idle / ready to run */}
          {canTrigger && (
            <>
              <button
                onClick={triggerPipeline}
                className="w-full px-6 py-3.5 text-base font-bold tracking-wide uppercase rounded-full transition-all"
                style={{
                  backgroundColor: isStale ? "var(--st-error)" : "var(--st-accent)",
                  color: "var(--st-text-light)",
                }}
                onMouseOver={(e) => {
                  e.currentTarget.style.opacity = "0.9";
                }}
                onMouseOut={(e) => {
                  e.currentTarget.style.opacity = "1";
                }}
              >
                {isStale ? "Run Pipeline (Data Stale)" : "Run Pipeline"}
              </button>
              {isStale && (
                <p className="text-xs text-center" style={{ color: "var(--st-error)" }}>
                  Last pipeline run was {timeAgo(freshness?.lastRun?.ranAt ?? null)}
                </p>
              )}
            </>
          )}

          {/* Running */}
          {status.state === "running" && (
            <div className="space-y-3">
              <div className="flex items-center justify-between">
                <span
                  className="text-sm font-medium"
                  style={{ color: "var(--st-text-primary)" }}
                >
                  {status.step}
                </span>
                <span
                  className="text-sm tabular-nums font-medium"
                  style={{ color: "var(--st-text-secondary)" }}
                >
                  {status.percent}%
                </span>
              </div>

              {/* Progress bar */}
              <div
                className="h-2 rounded-full overflow-hidden"
                style={{ backgroundColor: "var(--st-border)" }}
              >
                <div
                  className="h-full rounded-full transition-all duration-700 ease-out"
                  style={{
                    width: `${status.percent}%`,
                    backgroundColor: "var(--st-accent)",
                  }}
                />
              </div>

              <div className="flex items-center justify-between">
                {eta && (
                  <span className="text-xs" style={{ color: "var(--st-text-secondary)" }}>
                    {eta}
                  </span>
                )}
                <button
                  onClick={resetPipeline}
                  className="text-xs underline opacity-60 hover:opacity-100 ml-auto"
                  style={{ color: "var(--st-text-secondary)" }}
                >
                  Reset if stuck
                </button>
              </div>

              {/* Per-category status grid */}
              {status.categories && Object.keys(status.categories).length > 0 && (
                <CategoryStatusGrid categories={status.categories} />
              )}
            </div>
          )}

          {/* Complete */}
          {status.state === "complete" && (
            <div
              className="rounded-lg p-4 space-y-2"
              style={{
                backgroundColor: "rgba(74, 124, 89, 0.08)",
                border: "1px solid rgba(74, 124, 89, 0.2)",
              }}
            >
              <div className="flex items-center gap-2">
                <StatusDot color="var(--st-success)" />
                <span className="text-sm font-semibold" style={{ color: "var(--st-success)" }}>
                  Pipeline complete
                </span>
                <span className="text-xs ml-auto tabular-nums" style={{ color: "var(--st-success)", opacity: 0.8 }}>
                  {formatDuration(status.duration)}
                </span>
              </div>
              {status.recordCounts && Object.keys(status.recordCounts).length > 0 && (
                <div className="grid grid-cols-2 gap-x-4 gap-y-0.5 mt-2">
                  {Object.entries(status.recordCounts).map(([key, val]) => (
                    <div key={key} className="flex justify-between text-xs">
                      <span style={{ color: "var(--st-text-secondary)" }}>{key}</span>
                      <span className="tabular-nums font-medium" style={{ color: "var(--st-text-primary)" }}>
                        {(val as number).toLocaleString()}
                      </span>
                    </div>
                  ))}
                </div>
              )}
              <button
                onClick={() => setStatus({ state: "idle" })}
                className="text-xs underline mt-2"
                style={{ color: "var(--st-success)", opacity: 0.7 }}
              >
                Dismiss
              </button>
            </div>
          )}

          {/* Error */}
          {status.state === "error" && (() => {
            const isAuthError = /login|credential|auth|session|forbidden|favicon/i.test(status.message);
            const isQueueStuck = /already running|stale|queued/i.test(status.message);

            const title = isAuthError
              ? "Union.fit session expired"
              : isQueueStuck
              ? "Pipeline queue stuck"
              : "Pipeline failed";

            const description = isAuthError
              ? "The Union.fit login session has expired. Re-authenticate in Settings, then retry."
              : isQueueStuck
              ? "A previous pipeline job is stuck in the queue. Reset it to try again."
              : status.message;

            return (
              <div
                className="rounded-lg p-4 space-y-3"
                style={{
                  backgroundColor: "rgba(160, 64, 64, 0.06)",
                  border: "1px solid rgba(160, 64, 64, 0.2)",
                }}
              >
                <div className="flex items-center gap-2">
                  <StatusDot color="var(--st-error)" />
                  <span className="text-sm font-semibold" style={{ color: "var(--st-error)" }}>
                    {title}
                  </span>
                </div>
                <p className="text-xs" style={{ color: "var(--st-error)", opacity: 0.85 }}>
                  {description}
                </p>
                <div className="flex items-center gap-3 pt-1">
                  {isAuthError ? (
                    <a
                      href="/settings?from=pipeline"
                      className="px-4 py-1.5 text-xs font-bold uppercase tracking-wide rounded-full transition-all inline-block"
                      style={{
                        backgroundColor: "var(--st-accent)",
                        color: "var(--st-text-light)",
                        textDecoration: "none",
                      }}
                      onMouseOver={(e) => { e.currentTarget.style.opacity = "0.9"; }}
                      onMouseOut={(e) => { e.currentTarget.style.opacity = "1"; }}
                    >
                      Go to Settings
                    </a>
                  ) : isQueueStuck ? (
                    <button
                      onClick={async () => {
                        await resetPipeline();
                        triggerPipeline();
                      }}
                      className="px-4 py-1.5 text-xs font-bold uppercase tracking-wide rounded-full transition-all"
                      style={{
                        backgroundColor: "var(--st-accent)",
                        color: "var(--st-text-light)",
                      }}
                      onMouseOver={(e) => { e.currentTarget.style.opacity = "0.9"; }}
                      onMouseOut={(e) => { e.currentTarget.style.opacity = "1"; }}
                    >
                      Reset &amp; Retry
                    </button>
                  ) : (
                    <button
                      onClick={triggerPipeline}
                      className="px-4 py-1.5 text-xs font-bold uppercase tracking-wide rounded-full transition-all"
                      style={{
                        backgroundColor: "var(--st-accent)",
                        color: "var(--st-text-light)",
                      }}
                      onMouseOver={(e) => { e.currentTarget.style.opacity = "0.9"; }}
                      onMouseOut={(e) => { e.currentTarget.style.opacity = "1"; }}
                    >
                      Retry
                    </button>
                  )}
                  <button
                    onClick={() => setStatus({ state: "idle" })}
                    className="text-xs underline"
                    style={{ color: "var(--st-text-secondary)", opacity: 0.6 }}
                  >
                    Dismiss
                  </button>
                </div>
              </div>
            );
          })()}
        </div>

        {/* ── Data Freshness Card ── */}
        <div
          className="rounded-xl p-5 space-y-3"
          style={{
            backgroundColor: "var(--st-bg-card)",
            border: "1px solid var(--st-border)",
          }}
        >
          <div className="flex items-center justify-between">
            <h2
              className="text-xs font-bold tracking-widest uppercase"
              style={{ color: "var(--st-text-secondary)" }}
            >
              Data Freshness
            </h2>
            {!freshnessLoading && (
              <button
                onClick={loadFreshness}
                className="text-xs underline opacity-50 hover:opacity-100"
                style={{ color: "var(--st-text-secondary)" }}
              >
                Refresh
              </button>
            )}
          </div>

          {freshnessLoading ? (
            <p className="text-sm" style={{ color: "var(--st-text-secondary)" }}>Loading...</p>
          ) : freshness ? (
            <>
              <DataSourceRow label="Auto-Renews" timestamp={freshness.sources.autoRenews} />
              <DataSourceRow label="Registrations" timestamp={freshness.sources.registrations} />
              <DataSourceRow label="Shopify Orders" timestamp={freshness.sources.shopify} />

              <div
                className="pt-2 mt-2"
                style={{ borderTop: "1px solid var(--st-border)" }}
              >
                <div className="flex items-center justify-between py-1.5">
                  <span className="text-sm font-medium" style={{ color: "var(--st-text-primary)" }}>
                    Last Pipeline Run
                  </span>
                  <span
                    className="text-sm font-semibold tabular-nums"
                    style={{ color: freshnessColor(freshness.lastRunAgeMinutes) }}
                  >
                    {freshness.lastRun?.ranAt ? timeAgo(freshness.lastRun.ranAt) : "Never"}
                  </span>
                </div>
                {freshness.lastRun?.durationMs && (
                  <div className="flex items-center justify-between py-1">
                    <span className="text-xs" style={{ color: "var(--st-text-secondary)" }}>Duration</span>
                    <span className="text-xs tabular-nums" style={{ color: "var(--st-text-primary)" }}>
                      {formatDuration(freshness.lastRun.durationMs)}
                    </span>
                  </div>
                )}
              </div>
            </>
          ) : (
            <p className="text-sm" style={{ color: "var(--st-error)" }}>
              Could not load freshness data
            </p>
          )}
        </div>

        {/* ── Report Freshness Card (per-report watermarks) ── */}
        {freshness?.watermarks && Object.keys(freshness.watermarks).length > 0 && (
          <div
            className="rounded-xl p-5 space-y-2"
            style={{
              backgroundColor: "var(--st-bg-card)",
              border: "1px solid var(--st-border)",
            }}
          >
            <h2
              className="text-xs font-bold tracking-widest uppercase mb-2"
              style={{ color: "var(--st-text-secondary)" }}
            >
              Report Freshness
            </h2>
            <p className="text-[11px] mb-3" style={{ color: "var(--st-text-secondary)", opacity: 0.7 }}>
              Per-report high water mark — shows how current each data source is.
              Stale reports will auto-fetch a larger date range on next pipeline run.
            </p>
            <div className="space-y-0.5">
              {Object.entries(freshness.watermarks)
                .sort(([, a], [, b]) => {
                  // Sort by staleness: stale first
                  if (!a.highWaterDate) return -1;
                  if (!b.highWaterDate) return 1;
                  return a.highWaterDate < b.highWaterDate ? -1 : 1;
                })
                .map(([key, wm]) => {
                  const daysBehind = wm.highWaterDate
                    ? Math.floor((Date.now() - new Date(wm.highWaterDate).getTime()) / 86_400_000)
                    : null;
                  const staleColor = daysBehind === null
                    ? "var(--st-text-secondary)"
                    : daysBehind <= 1
                    ? "var(--st-success)"
                    : daysBehind <= 7
                    ? "var(--st-warning)"
                    : "var(--st-error)";
                  const WATERMARK_LABELS: Record<string, string> = {
                    autoRenews: "Auto-Renews",
                    newCustomers: "New Customers",
                    orders: "Orders",
                    firstVisits: "First Visits",
                    registrations: "Registrations",
                    revenueCategories: "Revenue Categories",
                  };

                  return (
                    <div key={key} className="flex items-center justify-between py-1.5">
                      <div className="flex items-center gap-2">
                        <StatusDot color={staleColor} />
                        <span className="text-sm" style={{ color: "var(--st-text-primary)" }}>
                          {WATERMARK_LABELS[key] || key}
                        </span>
                      </div>
                      <div className="flex items-center gap-3">
                        <span className="text-xs tabular-nums" style={{ color: "var(--st-text-secondary)" }}>
                          {wm.recordCount.toLocaleString()} rows
                        </span>
                        <span
                          className="text-xs font-medium tabular-nums"
                          style={{ color: staleColor, minWidth: "80px", textAlign: "right" }}
                        >
                          {wm.highWaterDate
                            ? daysBehind === 0
                              ? "Today"
                              : daysBehind === 1
                              ? "Yesterday"
                              : `${daysBehind}d behind`
                            : "No data"}
                        </span>
                      </div>
                    </div>
                  );
                })}
            </div>
          </div>
        )}

        {/* ── Table Counts Card ── */}
        {freshness && Object.keys(freshness.tableCounts).length > 0 && (
          <div
            className="rounded-xl p-5 space-y-2"
            style={{
              backgroundColor: "var(--st-bg-card)",
              border: "1px solid var(--st-border)",
            }}
          >
            <h2
              className="text-xs font-bold tracking-widest uppercase mb-2"
              style={{ color: "var(--st-text-secondary)" }}
            >
              Database Tables
            </h2>
            {Object.entries(freshness.tableCounts)
              .sort(([, a], [, b]) => (b as number) - (a as number))
              .map(([table, count]) => (
                <TableCountRow
                  key={table}
                  label={table.replace(/_/g, " ")}
                  count={count as number}
                />
              ))}
          </div>
        )}

        {/* ── Recent Runs Card ── */}
        {freshness && freshness.recentRuns.length > 0 && (
          <div
            className="rounded-xl p-5 space-y-2"
            style={{
              backgroundColor: "var(--st-bg-card)",
              border: "1px solid var(--st-border)",
            }}
          >
            <h2
              className="text-xs font-bold tracking-widest uppercase mb-2"
              style={{ color: "var(--st-text-secondary)" }}
            >
              Recent Runs
            </h2>
            {freshness.recentRuns.map((run) => (
              <div
                key={run.id}
                className="flex items-center justify-between py-1.5"
                style={{ borderBottom: "1px solid var(--st-border)" }}
              >
                <span className="text-xs tabular-nums" style={{ color: "var(--st-text-primary)" }}>
                  {formatDate(run.ranAt)}
                </span>
                <span className="text-xs tabular-nums" style={{ color: "var(--st-text-secondary)" }}>
                  {formatDuration(run.durationMs)}
                </span>
              </div>
            ))}
          </div>
        )}

        {/* ── Nav Footer ── */}
        <div className="flex justify-center gap-8 pt-4 text-sm">
          {[
            { href: "/", label: "Dashboard" },
            { href: "/settings", label: "Settings" },
          ].map(({ href, label }) => (
            <a
              key={href}
              href={href}
              className="hover:underline transition-colors"
              style={{ color: "var(--st-text-secondary)", fontWeight: 500 }}
            >
              {label}
            </a>
          ))}
        </div>
      </div>
    </div>
  );
}
