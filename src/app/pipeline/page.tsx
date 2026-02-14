"use client";

import { useState, useEffect, useRef } from "react";

type JobStatus =
  | { state: "idle" }
  | { state: "running"; jobId: string; step: string; percent: number }
  | { state: "complete"; sheetUrl: string; duration: number }
  | { state: "error"; message: string };

/* Text-based SKY TING wordmark styled to match brand */
function SkyTingLogo() {
  return (
    <span
      style={{
        fontFamily: "'Cormorant Garamond', 'Times New Roman', serif",
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

export default function Dashboard() {
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
          <h1 className="text-5xl" style={{ color: "var(--st-text-primary)" }}>
            Studio Analytics
          </h1>
          <p style={{ color: "var(--st-text-secondary)", fontSize: "0.95rem", letterSpacing: "-0.01em" }}>
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
              fontFamily: "'DM Sans', sans-serif",
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
              <div className="flex justify-between text-sm" style={{ color: "var(--st-text-secondary)" }}>
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
                style={{ color: "var(--st-text-secondary)" }}
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
              <p className="font-medium" style={{ color: "var(--st-success)" }}>
                Pipeline complete
              </p>
              <p className="text-sm" style={{ color: "var(--st-success)", opacity: 0.8 }}>
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

        {/* Footer nav */}
        <div className="flex justify-center gap-8 text-sm" style={{ color: "var(--st-text-secondary)" }}>
          <a
            href="/"
            className="hover:underline transition-colors"
            style={{ color: "var(--st-text-secondary)" }}
            onMouseOver={(e) => (e.currentTarget.style.color = "var(--st-text-primary)")}
            onMouseOut={(e) => (e.currentTarget.style.color = "var(--st-text-secondary)")}
          >
            Dashboard
          </a>
          <a
            href="/settings"
            className="hover:underline transition-colors"
            style={{ color: "var(--st-text-secondary)" }}
            onMouseOver={(e) => (e.currentTarget.style.color = "var(--st-text-primary)")}
            onMouseOut={(e) => (e.currentTarget.style.color = "var(--st-text-secondary)")}
          >
            Settings
          </a>
          <a
            href="/results"
            className="hover:underline transition-colors"
            style={{ color: "var(--st-text-secondary)" }}
            onMouseOver={(e) => (e.currentTarget.style.color = "var(--st-text-primary)")}
            onMouseOut={(e) => (e.currentTarget.style.color = "var(--st-text-secondary)")}
          >
            Results
          </a>
        </div>
      </div>
    </div>
  );
}
