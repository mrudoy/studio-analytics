"use client";

import { useState, useEffect } from "react";

interface SettingsState {
  email: string;
  password: string;
  analyticsSpreadsheetId: string;
  rawDataSpreadsheetId: string;
  robotEmail: string;
}

interface ScheduleState {
  enabled: boolean;
  preset: "10am-4pm" | "every-6h" | "every-12h" | "custom";
  customCron: string;
  timezone: string;
  nextRun: number | null;
  saving: boolean;
  message: string;
}

interface DigestState {
  enabled: boolean;
  recipients: string[];
  resendApiKey: string;
  fromAddress: string;
  hasResendKey: boolean; // from server (key exists in env or settings)
  newEmail: string;      // input field for adding a recipient
  testEmail: string;     // input field for test email target
  testStatus: "idle" | "sending" | "sent" | "error";
  testMessage: string;
}

const SCHEDULE_PRESETS: Record<string, { label: string; cron: string }> = {
  "10am-4pm": { label: "10am & 4pm daily", cron: "0 10,16 * * *" },
  "every-6h": { label: "Every 6 hours", cron: "0 */6 * * *" },
  "every-12h": { label: "8am & 8pm daily", cron: "0 8,20 * * *" },
};

export default function SettingsPage() {
  const [settings, setSettings] = useState<SettingsState>({
    email: "",
    password: "",
    analyticsSpreadsheetId: "",
    rawDataSpreadsheetId: "",
    robotEmail: "",
  });
  const [status, setStatus] = useState<"idle" | "saving" | "saved" | "error">("idle");
  const [errorMsg, setErrorMsg] = useState("");
  const [currentEmail, setCurrentEmail] = useState<string | null>(null);
  const [currentRobotEmail, setCurrentRobotEmail] = useState<string | null>(null);
  const [testStatus, setTestStatus] = useState<"idle" | "testing" | "success" | "error">("idle");
  const [testMessage, setTestMessage] = useState("");
  const [schedule, setSchedule] = useState<ScheduleState>({
    enabled: false,
    preset: "10am-4pm",
    customCron: "",
    timezone: "America/New_York",
    nextRun: null,
    saving: false,
    message: "",
  });
  const [digest, setDigest] = useState<DigestState>({
    enabled: false,
    recipients: [],
    resendApiKey: "",
    fromAddress: "",
    hasResendKey: false,
    newEmail: "",
    testEmail: "",
    testStatus: "idle",
    testMessage: "",
  });
  const [unionApiKey, setUnionApiKey] = useState("");
  const [hasUnionApiKey, setHasUnionApiKey] = useState(false);

  useEffect(() => {
    fetch("/api/settings")
      .then((res) => res.json())
      .then((data) => {
        setCurrentEmail(data.email || null);
        setCurrentRobotEmail(data.robotEmail || null);
        setHasUnionApiKey(data.hasUnionApiKey ?? false);
        if (data.emailDigest) {
          setDigest((d) => ({
            ...d,
            enabled: data.emailDigest.enabled ?? false,
            recipients: data.emailDigest.recipients ?? [],
            hasResendKey: data.emailDigest.hasResendKey ?? false,
            fromAddress: data.emailDigest.fromAddress || "",
          }));
        }
      })
      .catch(() => {});

    // Load current schedule
    fetch("/api/schedule")
      .then((res) => res.json())
      .then((data) => {
        if (data.error) return;
        const cron = data.cronPattern || "";
        let preset: ScheduleState["preset"] = "custom";
        for (const [key, val] of Object.entries(SCHEDULE_PRESETS)) {
          if (val.cron === cron) { preset = key as ScheduleState["preset"]; break; }
        }
        setSchedule((s) => ({
          ...s,
          enabled: data.enabled,
          preset,
          customCron: preset === "custom" ? cron : "",
          timezone: data.timezone || "America/New_York",
          nextRun: data.nextRun,
        }));
      })
      .catch(() => {});
  }, []);

  async function handleSave() {
    setStatus("saving");
    setErrorMsg("");

    try {
      const body: Record<string, unknown> = {};
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
      if (settings.robotEmail) {
        body.robotEmail = settings.robotEmail;
      }

      if (unionApiKey) {
        body.unionApiKey = unionApiKey;
      }

      // Always include email digest config so enabled/disabled state persists
      body.emailDigest = {
        enabled: digest.enabled,
        recipients: digest.recipients,
        ...(digest.resendApiKey ? { resendApiKey: digest.resendApiKey } : {}),
        ...(digest.fromAddress ? { fromAddress: digest.fromAddress } : {}),
      };

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

      if (new URLSearchParams(window.location.search).get("from") === "pipeline") {
        window.location.href = "/pipeline";
        return;
      }

      const refreshRes = await fetch("/api/settings");
      const refreshData = await refreshRes.json();
      setCurrentEmail(refreshData.email || null);
      setCurrentRobotEmail(refreshData.robotEmail || null);
      setHasUnionApiKey(refreshData.hasUnionApiKey ?? false);
      setUnionApiKey(""); // clear after save — stored server-side
      if (refreshData.emailDigest) {
        setDigest((d) => ({
          ...d,
          enabled: refreshData.emailDigest.enabled ?? false,
          recipients: refreshData.emailDigest.recipients ?? [],
          hasResendKey: refreshData.emailDigest.hasResendKey ?? false,
          resendApiKey: "", // clear after save — it's stored server-side
        }));
      }
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

  async function handleScheduleSave() {
    setSchedule((s) => ({ ...s, saving: true, message: "" }));
    try {
      const cronPattern = schedule.preset === "custom"
        ? schedule.customCron
        : SCHEDULE_PRESETS[schedule.preset]?.cron || "0 10,16 * * *";

      const res = await fetch("/api/schedule", {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          enabled: schedule.enabled,
          cronPattern,
          timezone: schedule.timezone,
        }),
      });

      const data = await res.json();
      if (!res.ok) throw new Error(data.error || "Failed to save schedule");

      setSchedule((s) => ({
        ...s,
        saving: false,
        nextRun: data.nextRun,
        message: schedule.enabled ? "Schedule saved" : "Schedule disabled",
      }));
    } catch (err) {
      setSchedule((s) => ({
        ...s,
        saving: false,
        message: err instanceof Error ? err.message : "Failed to save",
      }));
    }
  }

  function addRecipient() {
    const email = digest.newEmail.trim().toLowerCase();
    if (!email || !email.includes("@")) return;
    if (digest.recipients.includes(email)) {
      setDigest((d) => ({ ...d, newEmail: "" }));
      return;
    }
    setDigest((d) => ({
      ...d,
      recipients: [...d.recipients, email],
      newEmail: "",
    }));
  }

  function removeRecipient(email: string) {
    setDigest((d) => ({
      ...d,
      recipients: d.recipients.filter((e) => e !== email),
    }));
  }

  async function handleTestEmail() {
    const target = digest.testEmail.trim() || digest.recipients[0];
    if (!target) return;
    setDigest((d) => ({ ...d, testStatus: "sending", testMessage: "" }));

    try {
      const res = await fetch("/api/settings/test-email", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          toAddress: target,
          resendApiKey: digest.resendApiKey || undefined,
          fromAddress: digest.fromAddress || undefined,
        }),
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || "Failed");
      setDigest((d) => ({ ...d, testStatus: "sent", testMessage: data.message }));
    } catch (err) {
      setDigest((d) => ({
        ...d,
        testStatus: "error",
        testMessage: err instanceof Error ? err.message : "Failed to send test email",
      }));
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

          {/* Robot Email (for automated pipeline) */}
          <div className="space-y-4">
            <h2 className="text-2xl" style={{ color: "var(--st-text-primary)" }}>
              Robot Email
            </h2>
            <p className="text-sm" style={{ color: "var(--st-text-secondary)" }}>
              The automated pipeline uses a dedicated email address to receive CSV exports from Union.fit.
              Create a Google Workspace email (e.g. robot@skyting.com) and a Union.fit account with the same address.
              When the pipeline runs, it clicks &quot;Download CSV&quot; on Union.fit, which emails the data to this address.
            </p>
            {currentRobotEmail && (
              <p className="text-sm" style={{ color: "var(--st-text-secondary)" }}>
                Currently configured: {currentRobotEmail}
              </p>
            )}
            <div>
              <label
                className="block text-sm font-medium mb-1.5"
                style={{ color: "var(--st-text-secondary)" }}
              >
                Robot Email Address
              </label>
              <input
                type="email"
                value={settings.robotEmail}
                onChange={(e) => setSettings((s) => ({ ...s, robotEmail: e.target.value }))}
                className={inputFocusClass}
                style={{ ...inputStyle, outlineColor: "var(--st-accent)" }}
                placeholder="robot@skyting.com"
              />
              <p className="text-xs mt-1" style={{ color: "var(--st-text-secondary)" }}>
                Must be a Google Workspace email with Gmail API access via the service account.
                The Union.fit credentials above should also use this email address.
              </p>
            </div>
          </div>

          <hr style={{ borderColor: "var(--st-border)" }} />

          {/* Schedule */}
          <div className="space-y-4">
            <h2 className="text-2xl" style={{ color: "var(--st-text-primary)" }}>
              Automatic Schedule
            </h2>
            <p className="text-sm" style={{ color: "var(--st-text-secondary)" }}>
              Run the analytics pipeline automatically on a schedule. The dashboard updates without any manual work.
            </p>

            {/* Enable toggle */}
            <label className="flex items-center gap-3 cursor-pointer">
              <input
                type="checkbox"
                checked={schedule.enabled}
                onChange={(e) => setSchedule((s) => ({ ...s, enabled: e.target.checked }))}
                className="w-5 h-5 rounded"
                style={{ accentColor: "var(--st-accent)" }}
              />
              <span className="text-sm font-medium" style={{ color: "var(--st-text-primary)" }}>
                Enable automatic pipeline runs
              </span>
            </label>

            {schedule.enabled && (
              <div className="space-y-4 pl-1">
                {/* Preset selector */}
                <div>
                  <label
                    className="block text-sm font-medium mb-1.5"
                    style={{ color: "var(--st-text-secondary)" }}
                  >
                    Frequency
                  </label>
                  <select
                    value={schedule.preset}
                    onChange={(e) =>
                      setSchedule((s) => ({
                        ...s,
                        preset: e.target.value as ScheduleState["preset"],
                      }))
                    }
                    className={inputFocusClass}
                    style={{ ...inputStyle, outlineColor: "var(--st-accent)" }}
                  >
                    {Object.entries(SCHEDULE_PRESETS).map(([key, val]) => (
                      <option key={key} value={key}>
                        {val.label}
                      </option>
                    ))}
                    <option value="custom">Custom cron</option>
                  </select>
                </div>

                {/* Custom cron input */}
                {schedule.preset === "custom" && (
                  <div>
                    <label
                      className="block text-sm font-medium mb-1.5"
                      style={{ color: "var(--st-text-secondary)" }}
                    >
                      Cron pattern
                    </label>
                    <input
                      type="text"
                      value={schedule.customCron}
                      onChange={(e) =>
                        setSchedule((s) => ({ ...s, customCron: e.target.value }))
                      }
                      className={`${inputFocusClass} font-mono text-sm`}
                      style={{ ...inputStyle, outlineColor: "var(--st-accent)" }}
                      placeholder="0 10,16 * * *"
                    />
                    <p className="text-xs mt-1" style={{ color: "var(--st-text-secondary)" }}>
                      Standard cron format: minute hour day month weekday
                    </p>
                  </div>
                )}

                {/* Timezone */}
                <div>
                  <label
                    className="block text-sm font-medium mb-1.5"
                    style={{ color: "var(--st-text-secondary)" }}
                  >
                    Timezone
                  </label>
                  <select
                    value={schedule.timezone}
                    onChange={(e) =>
                      setSchedule((s) => ({ ...s, timezone: e.target.value }))
                    }
                    className={inputFocusClass}
                    style={{ ...inputStyle, outlineColor: "var(--st-accent)" }}
                  >
                    <option value="America/New_York">Eastern (New York)</option>
                    <option value="America/Chicago">Central (Chicago)</option>
                    <option value="America/Denver">Mountain (Denver)</option>
                    <option value="America/Los_Angeles">Pacific (Los Angeles)</option>
                    <option value="UTC">UTC</option>
                  </select>
                </div>

                {/* Next run display */}
                {schedule.nextRun && (
                  <p className="text-sm" style={{ color: "var(--st-text-secondary)" }}>
                    Next run:{" "}
                    <span style={{ color: "var(--st-text-primary)" }}>
                      {new Date(schedule.nextRun).toLocaleString()}
                    </span>
                  </p>
                )}
              </div>
            )}

            {/* Save schedule button */}
            <div className="flex items-center gap-3">
              <button
                onClick={handleScheduleSave}
                disabled={schedule.saving}
                className="px-5 py-2 text-sm font-medium uppercase tracking-wider transition-all disabled:opacity-50"
                style={{
                  backgroundColor: "var(--st-bg-dark)",
                  color: "var(--st-text-light)",
                  borderRadius: "var(--st-radius-pill)",
                  letterSpacing: "0.06em",
                }}
              >
                {schedule.saving ? "Saving..." : "Save Schedule"}
              </button>
              {schedule.message && (
                <span className="text-sm" style={{ color: "var(--st-success)" }}>
                  {schedule.message}
                </span>
              )}
            </div>
          </div>

          <hr style={{ borderColor: "var(--st-border)" }} />

          {/* Data Exporter API */}
          <div className="space-y-4">
            <div className="flex items-center justify-between">
              <h2 className="text-2xl" style={{ color: "var(--st-text-primary)" }}>
                Data Exporter API
              </h2>
              <span
                className="text-xs font-medium px-2.5 py-1 rounded-full"
                style={{
                  backgroundColor: hasUnionApiKey ? "rgba(22, 163, 74, 0.1)" : "rgba(107, 114, 128, 0.1)",
                  color: hasUnionApiKey ? "#16a34a" : "#6b7280",
                }}
              >
                {hasUnionApiKey ? "Configured" : "Not configured"}
              </span>
            </div>
            <p className="text-sm" style={{ color: "var(--st-text-secondary)" }}>
              Automatically fetch the latest data export from Union.fit using their API.
              When configured, the pipeline will poll for new exports before falling back to email-based delivery.
            </p>
            <div>
              <label
                className="block text-sm font-medium mb-1.5"
                style={{ color: "var(--st-text-secondary)" }}
              >
                Union API Key
              </label>
              <input
                type="password"
                value={unionApiKey}
                onChange={(e) => setUnionApiKey(e.target.value)}
                className={inputFocusClass}
                style={{ ...inputStyle, outlineColor: "var(--st-accent)" }}
                placeholder={hasUnionApiKey ? "Key configured (enter new to replace)" : "Enter your API key"}
              />
              <p className="text-xs mt-1" style={{ color: "var(--st-text-secondary)" }}>
                Get your API key from Union.fit → Reports → Data Exporters → API.
                Or set <code style={{ fontSize: "0.75rem" }}>UNION_API_KEY</code> env var on Railway.
              </p>
            </div>
          </div>

          <hr style={{ borderColor: "var(--st-border)" }} />

          {/* Email Digest */}
          <div className="space-y-4">
            <div className="flex items-center justify-between">
              <h2 className="text-2xl" style={{ color: "var(--st-text-primary)" }}>
                Email Digest
              </h2>
              <span
                className="text-xs font-medium px-2.5 py-1 rounded-full"
                style={{
                  backgroundColor: digest.enabled
                    ? "rgba(22, 163, 74, 0.1)"
                    : digest.recipients.length > 0
                    ? "rgba(234, 179, 8, 0.1)"
                    : "rgba(107, 114, 128, 0.1)",
                  color: digest.enabled
                    ? "#16a34a"
                    : digest.recipients.length > 0
                    ? "#ca8a04"
                    : "#6b7280",
                }}
              >
                {digest.enabled ? "Active" : digest.recipients.length > 0 ? "Paused" : "Not configured"}
              </span>
            </div>
            <p className="text-sm" style={{ color: "var(--st-text-secondary)" }}>
              Send the auto-renew summary to your team after each pipeline run.
              Powered by{" "}
              <a
                href="https://resend.com"
                target="_blank"
                rel="noopener noreferrer"
                style={{ color: "var(--st-accent)" }}
              >
                Resend
              </a>
              .
            </p>

            {/* Enable toggle */}
            <label className="flex items-center gap-3 cursor-pointer">
              <input
                type="checkbox"
                checked={digest.enabled}
                onChange={(e) => setDigest((d) => ({ ...d, enabled: e.target.checked }))}
                className="w-5 h-5 rounded"
                style={{ accentColor: "var(--st-accent)" }}
              />
              <span className="text-sm font-medium" style={{ color: "var(--st-text-primary)" }}>
                Send digest after pipeline completes
              </span>
            </label>

            {/* Resend API Key */}
            <div>
              <label
                className="block text-sm font-medium mb-1.5"
                style={{ color: "var(--st-text-secondary)" }}
              >
                Resend API Key
              </label>
              <input
                type="password"
                value={digest.resendApiKey}
                onChange={(e) => setDigest((d) => ({ ...d, resendApiKey: e.target.value }))}
                className={inputFocusClass}
                style={{ ...inputStyle, outlineColor: "var(--st-accent)" }}
                placeholder={digest.hasResendKey ? "Key configured (enter new to replace)" : "re_xxxxxxxx"}
              />
              <p className="text-xs mt-1" style={{ color: "var(--st-text-secondary)" }}>
                Get a free API key at{" "}
                <a
                  href="https://resend.com/api-keys"
                  target="_blank"
                  rel="noopener noreferrer"
                  style={{ color: "var(--st-accent)" }}
                >
                  resend.com/api-keys
                </a>
                {" "}(100 emails/day free).
                Or set <code style={{ fontSize: "0.75rem" }}>RESEND_API_KEY</code> env var on Railway.
              </p>
            </div>

            {/* Recipients list */}
            <div>
              <label
                className="block text-sm font-medium mb-1.5"
                style={{ color: "var(--st-text-secondary)" }}
              >
                Recipients
              </label>
              {digest.recipients.length > 0 ? (
                <div className="flex flex-wrap gap-2 mb-3">
                  {digest.recipients.map((email) => (
                    <span
                      key={email}
                      className="inline-flex items-center gap-1.5 px-3 py-1.5 rounded-full text-sm"
                      style={{
                        backgroundColor: "var(--st-bg-section)",
                        border: "1px solid var(--st-border)",
                        color: "var(--st-text-primary)",
                      }}
                    >
                      {email}
                      <button
                        onClick={() => removeRecipient(email)}
                        className="ml-0.5 hover:opacity-70 transition-opacity"
                        style={{ color: "var(--st-text-secondary)", fontSize: "1.1rem", lineHeight: 1 }}
                        title="Remove"
                      >
                        &times;
                      </button>
                    </span>
                  ))}
                </div>
              ) : (
                <p className="text-sm mb-3" style={{ color: "var(--st-text-secondary)" }}>
                  No recipients added yet.
                </p>
              )}

              {/* Add recipient input */}
              <div className="flex gap-2">
                <input
                  type="email"
                  value={digest.newEmail}
                  onChange={(e) => setDigest((d) => ({ ...d, newEmail: e.target.value }))}
                  onKeyDown={(e) => {
                    if (e.key === "Enter") { e.preventDefault(); addRecipient(); }
                  }}
                  className={inputFocusClass}
                  style={{ ...inputStyle, outlineColor: "var(--st-accent)", flex: 1 }}
                  placeholder="team@skyting.com"
                />
                <button
                  onClick={addRecipient}
                  className="px-4 py-2 text-sm font-medium transition-all"
                  style={{
                    backgroundColor: "var(--st-bg-dark)",
                    color: "var(--st-text-light)",
                    borderRadius: "var(--st-radius-pill)",
                  }}
                >
                  Add
                </button>
              </div>
            </div>

            {/* Test email */}
            <div
              className="rounded-xl p-4 space-y-3"
              style={{
                backgroundColor: "var(--st-bg-section)",
                border: "1px solid var(--st-border)",
              }}
            >
              <p className="text-sm" style={{ color: "var(--st-text-secondary)" }}>
                Send a test email to verify everything works. Uses live dashboard data.
              </p>
              <div className="flex gap-2">
                <input
                  type="email"
                  value={digest.testEmail}
                  onChange={(e) => setDigest((d) => ({ ...d, testEmail: e.target.value }))}
                  className={inputFocusClass}
                  style={{ ...inputStyle, outlineColor: "var(--st-accent)", flex: 1 }}
                  placeholder={digest.recipients[0] || "your@email.com"}
                />
                <button
                  onClick={handleTestEmail}
                  disabled={digest.testStatus === "sending"}
                  className="px-5 py-2 text-sm font-medium uppercase tracking-wider transition-all disabled:opacity-50 whitespace-nowrap"
                  style={{
                    backgroundColor: "var(--st-bg-dark)",
                    color: "var(--st-text-light)",
                    borderRadius: "var(--st-radius-pill)",
                    letterSpacing: "0.06em",
                  }}
                >
                  {digest.testStatus === "sending" ? "Sending..." : "Send Test"}
                </button>
              </div>
              {digest.testMessage && (
                <p
                  className="text-sm"
                  style={{
                    color:
                      digest.testStatus === "sent"
                        ? "var(--st-success)"
                        : digest.testStatus === "error"
                        ? "var(--st-error)"
                        : "var(--st-text-secondary)",
                  }}
                >
                  {digest.testMessage}
                </p>
              )}
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
