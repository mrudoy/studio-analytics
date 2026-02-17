"use client";

import { useState, useEffect, useRef, useCallback } from "react";

// ─── Types ───────────────────────────────────────────────────

type UploadStatus =
  | { state: "idle" }
  | { state: "processing"; step: string }
  | { state: "complete"; sheetUrl: string; duration: number; recordCounts: Record<string, number> }
  | { state: "error"; message: string };

type ScraperStatus =
  | { state: "idle" }
  | { state: "running"; jobId: string; step: string; percent: number }
  | { state: "complete"; sheetUrl: string; duration: number }
  | { state: "error"; message: string };

type ReportType =
  | "newCustomers"
  | "orders"
  | "firstVisits"
  | "allRegistrations"
  | "canceledAutoRenews"
  | "activeAutoRenews"
  | "pausedAutoRenews"
  | "trialingAutoRenews"
  | "newAutoRenews"
  | "revenueCategories"
  | "fullRegistrations";

interface DetectedFile {
  file: File;
  reportType: ReportType | null;
  confidence: "high" | "low";
}

// ─── Constants ───────────────────────────────────────────────

const FONT_SANS = "'Helvetica Neue', Helvetica, Arial, sans-serif";
const FONT_BRAND = "'Cormorant Garamond', 'Times New Roman', serif";

const REQUIRED_REPORTS: ReportType[] = [
  "newCustomers",
  "orders",
  "firstVisits",
  "allRegistrations",
  "canceledAutoRenews",
  "activeAutoRenews",
  "pausedAutoRenews",
  "trialingAutoRenews",
  "newAutoRenews",
];

const OPTIONAL_REPORTS: ReportType[] = [
  "revenueCategories",
  "fullRegistrations",
];

const ALL_REPORTS: ReportType[] = [...REQUIRED_REPORTS, ...OPTIONAL_REPORTS];

const REPORT_LABELS: Record<ReportType, string> = {
  newCustomers: "New Customers",
  orders: "Orders / Transactions",
  firstVisits: "First Visits",
  allRegistrations: "All Registrations",
  canceledAutoRenews: "Canceled Subscriptions",
  activeAutoRenews: "Active Subscriptions",
  pausedAutoRenews: "Paused Subscriptions",
  trialingAutoRenews: "Trialing Subscriptions",
  newAutoRenews: "New Subscriptions",
  revenueCategories: "Revenue Categories",
  fullRegistrations: "Full Registrations",
};

// ─── Header detection ────────────────────────────────────────

function detectReportType(headers: string[]): { type: ReportType | null; confidence: "high" | "low" } {
  const h = new Set(headers.map((s) => s.trim().toLowerCase().replace(/[^a-z0-9]/g, "")));

  // First Visits: attendee, performance
  if (h.has("attendee") && h.has("performance")) {
    return { type: "firstVisits", confidence: "high" };
  }

  // Full Registrations: eventname or event_name, locationname or location_name
  if ((h.has("eventname") || h.has("eventName")) && (h.has("locationname") || h.has("locationName"))) {
    return { type: "fullRegistrations", confidence: "high" };
  }

  // Revenue Categories: revenuecategory
  if (h.has("revenuecategory") && h.has("netrevenue")) {
    return { type: "revenueCategories", confidence: "high" };
  }

  // Orders: code + total (or ordercode, ordertotal)
  if ((h.has("code") || h.has("ordercode")) && (h.has("total") || h.has("ordertotal") || h.has("amount"))) {
    return { type: "orders", confidence: "high" };
  }

  // New Customers: name, email, role, orders
  if (h.has("name") && h.has("email") && h.has("role") && h.has("orders")) {
    return { type: "newCustomers", confidence: "high" };
  }

  // All Registrations: customer, pass, remaining
  if (h.has("customer") && h.has("pass") && h.has("remaining")) {
    return { type: "allRegistrations", confidence: "high" };
  }

  // Auto-renew: has subscription-like columns
  const isAutoRenew =
    (h.has("subscriptionname") || (h.has("name") && h.has("state") && h.has("price"))) &&
    !h.has("attendee") && !h.has("role");

  if (isAutoRenew) {
    // Disambiguate: canceled has canceledAt
    if (h.has("canceledat") || h.has("canceledAt")) {
      return { type: "canceledAutoRenews", confidence: "high" };
    }
    // Can't reliably distinguish active/paused/trialing/new from headers alone
    return { type: "activeAutoRenews", confidence: "low" };
  }

  return { type: null, confidence: "low" };
}

async function readFirstLine(file: File): Promise<string[]> {
  const text = await file.slice(0, 4096).text();
  const firstLine = text.split("\n")[0] || "";
  return firstLine.split(",").map((s) => s.trim().replace(/^"|"$/g, ""));
}

// ─── Components ──────────────────────────────────────────────

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

function formatDuration(ms: number): string {
  const sec = ms > 100_000 ? Math.round(ms / 1000) : Math.round(ms);
  const m = Math.floor(sec / 60);
  const s = sec % 60;
  return m > 0 ? `${m}m ${s}s` : `${s}s`;
}

// ─── Main Page ───────────────────────────────────────────────

export default function PipelinePage() {
  const [uploadStatus, setUploadStatus] = useState<UploadStatus>({ state: "idle" });
  const [scraperStatus, setScraperStatus] = useState<ScraperStatus>({ state: "idle" });
  const [detectedFiles, setDetectedFiles] = useState<DetectedFile[]>([]);
  const [isDragging, setIsDragging] = useState(false);
  const [showScraper, setShowScraper] = useState(false);
  const fileInputRef = useRef<HTMLInputElement>(null);
  const eventSourceRef = useRef<EventSource | null>(null);

  useEffect(() => {
    return () => {
      eventSourceRef.current?.close();
    };
  }, []);

  // ── File handling ──────────────────────────────────────────

  const processFiles = useCallback(async (fileList: FileList | File[]) => {
    const files = Array.from(fileList).filter((f) => f.name.endsWith(".csv"));
    if (files.length === 0) return;

    const detected: DetectedFile[] = [];

    for (const file of files) {
      const headers = await readFirstLine(file);
      const { type, confidence } = detectReportType(headers);
      detected.push({ file, reportType: type, confidence });
    }

    setDetectedFiles((prev) => {
      // Merge with existing, replacing if same report type detected
      const byType = new Map<string, DetectedFile>();
      let undetectedIdx = 0;
      for (const f of [...prev, ...detected]) {
        if (f.reportType) {
          byType.set(f.reportType, f);
        } else {
          byType.set(`undetected-${undetectedIdx}`, f);
          undetectedIdx++;
        }
      }
      return Array.from(byType.values());
    });
  }, []);

  const handleDrop = useCallback(
    (e: React.DragEvent) => {
      e.preventDefault();
      setIsDragging(false);
      processFiles(e.dataTransfer.files);
    },
    [processFiles]
  );

  const handleFileInput = useCallback(
    (e: React.ChangeEvent<HTMLInputElement>) => {
      if (e.target.files) processFiles(e.target.files);
    },
    [processFiles]
  );

  const updateFileType = useCallback((index: number, newType: ReportType) => {
    setDetectedFiles((prev) =>
      prev.map((f, i) => (i === index ? { ...f, reportType: newType, confidence: "high" as const } : f))
    );
  }, []);

  const removeFile = useCallback((index: number) => {
    setDetectedFiles((prev) => prev.filter((_, i) => i !== index));
  }, []);

  // ── Upload & process ───────────────────────────────────────

  const assignedTypes = new Set(detectedFiles.filter((f) => f.reportType).map((f) => f.reportType));
  const missingRequired = REQUIRED_REPORTS.filter((r) => !assignedTypes.has(r));
  const canProcess = missingRequired.length === 0 && uploadStatus.state !== "processing";

  async function processUpload() {
    setUploadStatus({ state: "processing", step: "Uploading files..." });

    try {
      const formData = new FormData();
      for (const df of detectedFiles) {
        if (df.reportType) {
          formData.append(df.reportType, df.file);
        }
      }

      const res = await fetch("/api/upload-pipeline", { method: "POST", body: formData });
      const data = await res.json();

      if (!res.ok) {
        setUploadStatus({ state: "error", message: data.error || "Processing failed" });
        return;
      }

      setUploadStatus({
        state: "complete",
        sheetUrl: data.sheetUrl,
        duration: data.duration,
        recordCounts: data.recordCounts,
      });
    } catch {
      setUploadStatus({ state: "error", message: "Network error" });
    }
  }

  // ── Scraper (legacy) ───────────────────────────────────────

  async function resetPipeline() {
    try {
      await fetch("/api/pipeline", { method: "DELETE" });
      eventSourceRef.current?.close();
      setScraperStatus({ state: "idle" });
    } catch {
      setScraperStatus({ state: "error", message: "Failed to reset queue" });
    }
  }

  async function runScraper() {
    setScraperStatus({ state: "running", jobId: "", step: "Starting...", percent: 0 });

    try {
      const res = await fetch("/api/pipeline", { method: "POST" });
      if (!res.ok) {
        const err = await res.json();
        setScraperStatus({ state: "error", message: err.error || "Failed to start" });
        return;
      }

      const { jobId } = await res.json();
      setScraperStatus({ state: "running", jobId, step: "Queued...", percent: 0 });

      eventSourceRef.current?.close();
      const es = new EventSource(`/api/status?jobId=${jobId}`);
      eventSourceRef.current = es;

      es.addEventListener("progress", (e) => {
        const data = JSON.parse(e.data);
        setScraperStatus({ state: "running", jobId, step: data.step, percent: data.percent });
      });

      es.addEventListener("complete", (e) => {
        const data = JSON.parse(e.data);
        setScraperStatus({ state: "complete", sheetUrl: data.sheetUrl, duration: data.duration });
        es.close();
      });

      es.addEventListener("error", (e) => {
        try {
          const data = JSON.parse((e as MessageEvent).data);
          setScraperStatus({ state: "error", message: data.message });
        } catch {
          setScraperStatus({ state: "error", message: "Connection lost" });
        }
        es.close();
      });
    } catch {
      setScraperStatus({ state: "error", message: "Network error" });
    }
  }

  // ── Render ─────────────────────────────────────────────────

  return (
    <div className="min-h-screen flex flex-col items-center justify-start p-8 pt-12">
      <div className="max-w-2xl w-full space-y-8">
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
            Upload CSVs from Union.fit, run analytics, export to Google Sheets
          </p>
        </div>

        {/* ── Upload Section ─────────────────────────────────── */}
        <div
          className="rounded-2xl p-6 space-y-5"
          style={{
            backgroundColor: "var(--st-bg-card)",
            border: "1px solid var(--st-border)",
          }}
        >
          <h2
            style={{
              fontFamily: FONT_SANS,
              fontWeight: 700,
              fontSize: "1rem",
              letterSpacing: "0.06em",
              textTransform: "uppercase" as const,
              color: "var(--st-text-primary)",
            }}
          >
            Upload CSV Reports
          </h2>

          {/* Drop zone */}
          <div
            onDragOver={(e) => {
              e.preventDefault();
              setIsDragging(true);
            }}
            onDragLeave={() => setIsDragging(false)}
            onDrop={handleDrop}
            onClick={() => fileInputRef.current?.click()}
            className="rounded-xl p-8 text-center cursor-pointer transition-all"
            style={{
              border: isDragging
                ? "2px solid var(--st-accent)"
                : "2px dashed var(--st-border)",
              backgroundColor: isDragging
                ? "rgba(90, 75, 65, 0.05)"
                : "transparent",
            }}
          >
            <input
              ref={fileInputRef}
              type="file"
              accept=".csv"
              multiple
              onChange={handleFileInput}
              className="hidden"
            />
            <p
              style={{
                fontFamily: FONT_SANS,
                fontWeight: 600,
                fontSize: "0.95rem",
                color: "var(--st-text-primary)",
              }}
            >
              Drop CSV files here or click to browse
            </p>
            <p
              className="mt-1"
              style={{
                fontFamily: FONT_SANS,
                fontSize: "0.82rem",
                color: "var(--st-text-secondary)",
              }}
            >
              Download reports from Union.fit and drop them all at once
            </p>
          </div>

          {/* Detected files list */}
          {detectedFiles.length > 0 && (
            <div className="space-y-2">
              {detectedFiles.map((df, i) => (
                <div
                  key={i}
                  className="flex items-center gap-3 px-3 py-2 rounded-lg"
                  style={{
                    backgroundColor: "var(--st-bg-section)",
                    border: "1px solid var(--st-border)",
                    fontFamily: FONT_SANS,
                    fontSize: "0.85rem",
                  }}
                >
                  {/* Status indicator */}
                  <span
                    style={{
                      width: "8px",
                      height: "8px",
                      borderRadius: "50%",
                      backgroundColor:
                        df.reportType && df.confidence === "high"
                          ? "var(--st-success)"
                          : df.reportType
                          ? "#D4A017"
                          : "var(--st-error)",
                      flexShrink: 0,
                    }}
                  />

                  {/* Filename */}
                  <span
                    className="truncate"
                    style={{
                      color: "var(--st-text-secondary)",
                      minWidth: 0,
                      flex: "0 1 auto",
                      maxWidth: "180px",
                    }}
                    title={df.file.name}
                  >
                    {df.file.name}
                  </span>

                  {/* Arrow */}
                  <span style={{ color: "var(--st-text-secondary)", flexShrink: 0 }}>
                    →
                  </span>

                  {/* Type selector */}
                  <select
                    value={df.reportType || ""}
                    onChange={(e) => updateFileType(i, e.target.value as ReportType)}
                    style={{
                      fontFamily: FONT_SANS,
                      fontSize: "0.82rem",
                      fontWeight: 600,
                      color: "var(--st-text-primary)",
                      backgroundColor: "var(--st-bg-card)",
                      border: "1px solid var(--st-border)",
                      borderRadius: "6px",
                      padding: "2px 6px",
                      flex: "1 1 auto",
                      minWidth: 0,
                    }}
                  >
                    <option value="">-- Select report type --</option>
                    {ALL_REPORTS.map((rt) => (
                      <option key={rt} value={rt}>
                        {REPORT_LABELS[rt]}
                      </option>
                    ))}
                  </select>

                  {/* Remove button */}
                  <button
                    onClick={(e) => {
                      e.stopPropagation();
                      removeFile(i);
                    }}
                    style={{
                      color: "var(--st-text-secondary)",
                      fontSize: "1.1rem",
                      lineHeight: 1,
                      flexShrink: 0,
                      opacity: 0.6,
                    }}
                    title="Remove"
                  >
                    x
                  </button>
                </div>
              ))}
            </div>
          )}

          {/* Missing reports checklist */}
          {detectedFiles.length > 0 && missingRequired.length > 0 && (
            <div
              className="rounded-lg p-3"
              style={{
                backgroundColor: "#F5EFEF",
                border: "1px solid rgba(160, 64, 64, 0.15)",
                fontFamily: FONT_SANS,
                fontSize: "0.82rem",
              }}
            >
              <p style={{ color: "var(--st-error)", fontWeight: 600, marginBottom: "4px" }}>
                Missing required reports:
              </p>
              <div className="flex flex-wrap gap-x-3 gap-y-1">
                {missingRequired.map((r) => (
                  <span key={r} style={{ color: "var(--st-error)", opacity: 0.8 }}>
                    {REPORT_LABELS[r]}
                  </span>
                ))}
              </div>
            </div>
          )}

          {/* Process button */}
          {detectedFiles.length > 0 && (
            <button
              onClick={processUpload}
              disabled={!canProcess}
              className="w-full px-6 py-3.5 text-base tracking-wide uppercase transition-all disabled:opacity-40 disabled:cursor-not-allowed"
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
              {uploadStatus.state === "processing"
                ? "Processing..."
                : `Process ${detectedFiles.filter((f) => f.reportType).length} Reports`}
            </button>
          )}

          {/* Upload status: processing */}
          {uploadStatus.state === "processing" && (
            <div className="text-center py-2">
              <p style={{ color: "var(--st-text-secondary)", fontFamily: FONT_SANS, fontSize: "0.9rem" }}>
                {uploadStatus.step}
              </p>
            </div>
          )}

          {/* Upload status: complete */}
          {uploadStatus.state === "complete" && (
            <div
              className="rounded-xl p-5 text-center space-y-3"
              style={{
                backgroundColor: "#EFF5F0",
                border: "1px solid rgba(74, 124, 89, 0.2)",
              }}
            >
              <p style={{ color: "var(--st-success)", fontFamily: FONT_SANS, fontWeight: 700 }}>
                Analytics complete
              </p>
              <p className="text-sm" style={{ color: "var(--st-success)", opacity: 0.8, fontFamily: FONT_SANS }}>
                Processed in {formatDuration(uploadStatus.duration)}
              </p>
              <a
                href={uploadStatus.sheetUrl}
                target="_blank"
                rel="noopener noreferrer"
                className="inline-block mt-1 px-5 py-2 text-sm uppercase tracking-wider transition-colors"
                style={{
                  backgroundColor: "var(--st-success)",
                  color: "#fff",
                  borderRadius: "var(--st-radius-pill)",
                  fontFamily: FONT_SANS,
                  fontWeight: 700,
                  letterSpacing: "0.06em",
                }}
              >
                Open Google Sheet
              </a>
              <div className="mt-2">
                <button
                  onClick={() => {
                    setUploadStatus({ state: "idle" });
                    setDetectedFiles([]);
                  }}
                  className="text-sm underline"
                  style={{ color: "var(--st-success)", opacity: 0.7, fontFamily: FONT_SANS }}
                >
                  Upload new files
                </button>
              </div>
            </div>
          )}

          {/* Upload status: error */}
          {uploadStatus.state === "error" && (
            <div
              className="rounded-xl p-4 text-center"
              style={{
                backgroundColor: "#F5EFEF",
                border: "1px solid rgba(160, 64, 64, 0.2)",
              }}
            >
              <p style={{ color: "var(--st-error)", fontFamily: FONT_SANS, fontWeight: 700, fontSize: "0.9rem" }}>
                Error
              </p>
              <p className="text-sm mt-1" style={{ color: "var(--st-error)", opacity: 0.85, fontFamily: FONT_SANS }}>
                {uploadStatus.message}
              </p>
              <button
                onClick={() => setUploadStatus({ state: "idle" })}
                className="mt-2 text-sm underline"
                style={{ color: "var(--st-error)", opacity: 0.7, fontFamily: FONT_SANS }}
              >
                Dismiss
              </button>
            </div>
          )}
        </div>

        {/* ── Scraper Section (collapsed by default) ──────────── */}
        <div className="text-center">
          <button
            onClick={() => setShowScraper(!showScraper)}
            className="text-sm transition-opacity"
            style={{
              color: "var(--st-text-secondary)",
              fontFamily: FONT_SANS,
              opacity: 0.6,
              background: "none",
              border: "none",
              cursor: "pointer",
            }}
          >
            {showScraper ? "Hide auto-scraper" : "Or use auto-scraper (requires browser)"}
          </button>
        </div>

        {showScraper && (
          <div
            className="rounded-2xl p-6 space-y-5"
            style={{
              backgroundColor: "var(--st-bg-card)",
              border: "1px solid var(--st-border)",
              opacity: 0.75,
            }}
          >
            <h2
              style={{
                fontFamily: FONT_SANS,
                fontWeight: 600,
                fontSize: "0.85rem",
                letterSpacing: "0.06em",
                textTransform: "uppercase" as const,
                color: "var(--st-text-secondary)",
              }}
            >
              Auto-Scraper (Legacy)
            </h2>

            <button
              onClick={runScraper}
              disabled={scraperStatus.state === "running"}
              className="w-full px-5 py-3 text-sm tracking-wide uppercase transition-all disabled:opacity-40 disabled:cursor-not-allowed"
              style={{
                backgroundColor: "var(--st-text-secondary)",
                color: "var(--st-text-light)",
                borderRadius: "var(--st-radius-pill)",
                fontFamily: FONT_SANS,
                fontWeight: 600,
                letterSpacing: "0.06em",
              }}
            >
              {scraperStatus.state === "running" ? "Scraping..." : "Run Auto-Scraper"}
            </button>

            {scraperStatus.state === "running" && (
              <div className="space-y-2">
                <div className="flex justify-between text-sm" style={{ color: "var(--st-text-secondary)", fontFamily: FONT_SANS }}>
                  <span>{scraperStatus.step}</span>
                  <span>{scraperStatus.percent}%</span>
                </div>
                <div className="h-1.5 rounded-full overflow-hidden" style={{ backgroundColor: "var(--st-border)" }}>
                  <div
                    className="h-full rounded-full transition-all duration-500"
                    style={{ width: `${scraperStatus.percent}%`, backgroundColor: "var(--st-accent)" }}
                  />
                </div>
                <button
                  onClick={resetPipeline}
                  className="text-xs underline opacity-60 hover:opacity-100"
                  style={{ color: "var(--st-text-secondary)", fontFamily: FONT_SANS }}
                >
                  Reset if stuck
                </button>
              </div>
            )}

            {scraperStatus.state === "complete" && (
              <div className="rounded-xl p-4 text-center" style={{ backgroundColor: "#EFF5F0", border: "1px solid rgba(74, 124, 89, 0.2)" }}>
                <p style={{ color: "var(--st-success)", fontFamily: FONT_SANS, fontWeight: 700, fontSize: "0.9rem" }}>Complete</p>
                <p className="text-sm mt-1" style={{ color: "var(--st-success)", opacity: 0.8, fontFamily: FONT_SANS }}>
                  {formatDuration(scraperStatus.duration)}
                </p>
                <a
                  href={scraperStatus.sheetUrl}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="inline-block mt-2 px-4 py-1.5 text-xs uppercase tracking-wider"
                  style={{
                    backgroundColor: "var(--st-success)",
                    color: "#fff",
                    borderRadius: "var(--st-radius-pill)",
                    fontFamily: FONT_SANS,
                    fontWeight: 700,
                  }}
                >
                  Open Sheet
                </a>
              </div>
            )}

            {scraperStatus.state === "error" && (
              <div className="rounded-xl p-4 text-center" style={{ backgroundColor: "#F5EFEF", border: "1px solid rgba(160, 64, 64, 0.2)" }}>
                <p style={{ color: "var(--st-error)", fontFamily: FONT_SANS, fontWeight: 700, fontSize: "0.9rem" }}>Error</p>
                <p className="text-sm mt-1" style={{ color: "var(--st-error)", opacity: 0.85, fontFamily: FONT_SANS }}>{scraperStatus.message}</p>
                <button onClick={() => setScraperStatus({ state: "idle" })} className="mt-2 text-sm underline" style={{ color: "var(--st-error)", opacity: 0.7, fontFamily: FONT_SANS }}>
                  Dismiss
                </button>
              </div>
            )}
          </div>
        )}

        {/* Footer nav */}
        <div className="flex justify-center gap-8 text-sm" style={{ color: "var(--st-text-secondary)" }}>
          {[
            { href: "/", label: "Dashboard" },
            { href: "/settings", label: "Settings" },
            { href: "/results", label: "Results" },
          ].map(({ href, label }) => (
            <a
              key={href}
              href={href}
              className="hover:underline transition-colors"
              style={{ color: "var(--st-text-secondary)", fontFamily: FONT_SANS, fontWeight: 500 }}
              onMouseOver={(e) => (e.currentTarget.style.color = "var(--st-text-primary)")}
              onMouseOut={(e) => (e.currentTarget.style.color = "var(--st-text-secondary)")}
            >
              {label}
            </a>
          ))}
        </div>
      </div>
    </div>
  );
}
