"use client";

import { useState, useEffect } from "react";

interface SettingsState {
  email: string;
  password: string;
  analyticsSpreadsheetId: string;
  rawDataSpreadsheetId: string;
}

export default function SettingsPage() {
  const [settings, setSettings] = useState<SettingsState>({
    email: "",
    password: "",
    analyticsSpreadsheetId: "",
    rawDataSpreadsheetId: "",
  });
  const [status, setStatus] = useState<"idle" | "saving" | "saved" | "error">("idle");
  const [errorMsg, setErrorMsg] = useState("");
  const [currentEmail, setCurrentEmail] = useState<string | null>(null);
  const [testStatus, setTestStatus] = useState<"idle" | "testing" | "success" | "error">("idle");
  const [testMessage, setTestMessage] = useState("");

  useEffect(() => {
    fetch("/api/settings")
      .then((res) => res.json())
      .then((data) => {
        setCurrentEmail(data.email || null);
      })
      .catch(() => {});
  }, []);

  async function handleSave() {
    setStatus("saving");
    setErrorMsg("");

    try {
      const body: Record<string, string> = {};
      if (settings.email && settings.password) {
        body.email = settings.email;
        body.password = settings.password;
      }
      if (settings.analyticsSpreadsheetId) {
        body.analyticsSpreadsheetId = settings.analyticsSpreadsheetId;
      }
      if (settings.rawDataSpreadsheetId) {
        body.rawDataSpreadsheetId = settings.rawDataSpreadsheetId;
      }

      const res = await fetch("/api/settings", {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });

      if (!res.ok) {
        const err = await res.json();
        throw new Error(err.error || "Save failed");
      }

      setStatus("saved");
      setSettings((s) => ({ ...s, password: "" }));

      const refreshRes = await fetch("/api/settings");
      const refreshData = await refreshRes.json();
      setCurrentEmail(refreshData.email || null);
    } catch (err) {
      setStatus("error");
      setErrorMsg(err instanceof Error ? err.message : "Save failed");
    }
  }

  async function handleTestConnection() {
    setTestStatus("testing");
    setTestMessage("Opening browser — please complete the Cloudflare verification in the popup window...");

    try {
      const res = await fetch("/api/test-connection", { method: "POST" });
      const data = await res.json();

      if (data.success) {
        setTestStatus("success");
        setTestMessage(data.message);
      } else {
        setTestStatus("error");
        setTestMessage(data.message || data.error || "Test failed");
      }
    } catch {
      setTestStatus("error");
      setTestMessage("Failed to connect to the test endpoint.");
    }
  }

  const inputStyle = {
    backgroundColor: "var(--st-bg-primary)",
    border: "1px solid var(--st-border)",
    color: "var(--st-text-primary)",
    borderRadius: "12px",
  };

  const inputFocusClass =
    "w-full px-4 py-2.5 focus:outline-none focus:ring-2 transition-shadow";

  return (
    <div className="min-h-screen flex flex-col items-center justify-center p-8">
      <div className="max-w-xl w-full space-y-10">
        <div className="text-center space-y-2">
          <h1 className="text-5xl" style={{ color: "var(--st-text-primary)" }}>
            Settings
          </h1>
          <p style={{ color: "var(--st-text-secondary)", fontSize: "0.95rem" }}>
            Configure your Union.fit and Google Sheets credentials
          </p>
        </div>

        <div
          className="rounded-2xl p-8 space-y-8"
          style={{
            backgroundColor: "var(--st-bg-card)",
            border: "1px solid var(--st-border)",
          }}
        >
          {/* Union.fit Credentials */}
          <div className="space-y-4">
            <h2 className="text-2xl" style={{ color: "var(--st-text-primary)" }}>
              Union.fit Credentials
            </h2>
            {currentEmail && (
              <p className="text-sm" style={{ color: "var(--st-text-secondary)" }}>
                Currently configured: {currentEmail}
              </p>
            )}
            <div>
              <label
                className="block text-sm font-medium mb-1.5"
                style={{ color: "var(--st-text-secondary)" }}
              >
                Email
              </label>
              <input
                type="email"
                value={settings.email}
                onChange={(e) => setSettings((s) => ({ ...s, email: e.target.value }))}
                className={inputFocusClass}
                style={{ ...inputStyle, outlineColor: "var(--st-accent)" }}
                placeholder="your@email.com"
              />
            </div>
            <div>
              <label
                className="block text-sm font-medium mb-1.5"
                style={{ color: "var(--st-text-secondary)" }}
              >
                Password
              </label>
              <input
                type="password"
                value={settings.password}
                onChange={(e) => setSettings((s) => ({ ...s, password: e.target.value }))}
                className={inputFocusClass}
                style={{ ...inputStyle, outlineColor: "var(--st-accent)" }}
                placeholder="••••••••"
              />
            </div>

            {/* Test Connection */}
            <div
              className="rounded-xl p-4 space-y-3"
              style={{
                backgroundColor: "var(--st-bg-section)",
                border: "1px solid var(--st-border)",
              }}
            >
              <p className="text-sm" style={{ color: "var(--st-text-secondary)" }}>
                <strong style={{ color: "var(--st-text-primary)" }}>Important:</strong> Union.fit
                uses Cloudflare bot protection. Click below to open a visible browser window.
                Complete the &quot;Verify you are human&quot; checkbox once.
              </p>
              <button
                onClick={handleTestConnection}
                disabled={testStatus === "testing"}
                className="px-5 py-2 text-sm font-medium uppercase tracking-wider transition-all disabled:opacity-50"
                style={{
                  backgroundColor: "var(--st-bg-dark)",
                  color: "var(--st-text-light)",
                  borderRadius: "var(--st-radius-pill)",
                  letterSpacing: "0.06em",
                }}
              >
                {testStatus === "testing" ? "Browser open — waiting..." : "Test Connection"}
              </button>
              {testMessage && (
                <p
                  className="text-sm"
                  style={{
                    color:
                      testStatus === "success"
                        ? "var(--st-success)"
                        : testStatus === "error"
                        ? "var(--st-error)"
                        : "var(--st-text-secondary)",
                  }}
                >
                  {testMessage}
                </p>
              )}
            </div>
          </div>

          <hr style={{ borderColor: "var(--st-border)" }} />

          {/* Google Sheets */}
          <div className="space-y-4">
            <h2 className="text-2xl" style={{ color: "var(--st-text-primary)" }}>
              Google Sheets
            </h2>
            <p className="text-sm" style={{ color: "var(--st-text-secondary)" }}>
              Enter the Spreadsheet IDs from your Google Sheet URLs. The ID is the long string
              between /d/ and /edit in the URL.
            </p>
            <div>
              <label
                className="block text-sm font-medium mb-1.5"
                style={{ color: "var(--st-text-secondary)" }}
              >
                Analytics Spreadsheet ID
              </label>
              <input
                type="text"
                value={settings.analyticsSpreadsheetId}
                onChange={(e) =>
                  setSettings((s) => ({ ...s, analyticsSpreadsheetId: e.target.value }))
                }
                className={`${inputFocusClass} font-mono text-sm`}
                style={{ ...inputStyle, outlineColor: "var(--st-accent)" }}
                placeholder="1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgVE2upms"
              />
            </div>
            <div>
              <label
                className="block text-sm font-medium mb-1.5"
                style={{ color: "var(--st-text-secondary)" }}
              >
                Raw Data Spreadsheet ID (optional)
              </label>
              <input
                type="text"
                value={settings.rawDataSpreadsheetId}
                onChange={(e) =>
                  setSettings((s) => ({ ...s, rawDataSpreadsheetId: e.target.value }))
                }
                className={`${inputFocusClass} font-mono text-sm`}
                style={{ ...inputStyle, outlineColor: "var(--st-accent)" }}
                placeholder="1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgVE2upms"
              />
            </div>
          </div>

          <hr style={{ borderColor: "var(--st-border)" }} />

          {/* Save */}
          <div className="flex items-center gap-4">
            <button
              onClick={handleSave}
              disabled={status === "saving"}
              className="px-6 py-3 text-sm font-medium uppercase tracking-wider transition-all disabled:opacity-50"
              style={{
                backgroundColor: "var(--st-accent)",
                color: "var(--st-text-light)",
                borderRadius: "var(--st-radius-pill)",
                letterSpacing: "0.06em",
              }}
              onMouseOver={(e) => (e.currentTarget.style.backgroundColor = "var(--st-accent-hover)")}
              onMouseOut={(e) => (e.currentTarget.style.backgroundColor = "var(--st-accent)")}
            >
              {status === "saving" ? "Saving..." : "Save Settings"}
            </button>
            {status === "saved" && (
              <span className="text-sm" style={{ color: "var(--st-success)" }}>
                Settings saved!
              </span>
            )}
            {status === "error" && (
              <span className="text-sm" style={{ color: "var(--st-error)" }}>
                {errorMsg}
              </span>
            )}
          </div>
        </div>

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
            href="/pipeline"
            className="hover:underline transition-colors"
            style={{ color: "var(--st-text-secondary)" }}
            onMouseOver={(e) => (e.currentTarget.style.color = "var(--st-text-primary)")}
            onMouseOut={(e) => (e.currentTarget.style.color = "var(--st-text-secondary)")}
          >
            Pipeline
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
