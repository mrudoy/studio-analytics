"use client";

import { useState, useRef } from "react";

const FONT_SANS = "'Helvetica Neue', Helvetica, Arial, sans-serif";

export default function LoginPage() {
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);
  const inputRef = useRef<HTMLInputElement>(null);

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    const password = inputRef.current?.value ?? "";
    setLoading(true);
    setError("");

    const res = await fetch("/api/auth/login", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ password }),
    });

    setLoading(false);

    if (res.ok) {
      const params = new URLSearchParams(window.location.search);
      const from = params.get("from");
      const redirect =
        from && from.startsWith("/") && !from.startsWith("//") ? from : "/";
      window.location.replace(redirect);
    } else if (res.status === 429) {
      setError("Too many attempts. Try again later.");
    } else {
      setError("Incorrect password");
      inputRef.current?.select();
    }
  }

  return (
    <div
      style={{
        fontFamily: FONT_SANS,
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        minHeight: "100vh",
        background: "var(--st-bg-section)",
      }}
    >
      <div
        style={{
          width: 300,
          background: "var(--st-bg-card)",
          border: "1px solid var(--st-border)",
          borderRadius: "12px",
          padding: "1.5rem",
          display: "flex",
          flexDirection: "column",
          gap: "1rem",
        }}
      >
        <p
          style={{
            margin: 0,
            fontSize: "1.05rem",
            fontWeight: 500,
            color: "var(--st-text-primary)",
          }}
        >
          Studio Analytics
        </p>
        <form
          onSubmit={handleSubmit}
          style={{ display: "flex", flexDirection: "column", gap: "0.5rem" }}
        >
          <input
            ref={inputRef}
            type="password"
            placeholder="Password"
            autoFocus
            required
            style={{
              padding: "0.5rem 0.75rem",
              border: "1px solid var(--st-border)",
              borderRadius: "6px",
              background: "transparent",
              color: "var(--st-text-primary)",
              fontSize: "0.9rem",
              fontFamily: FONT_SANS,
              outline: "none",
              width: "100%",
              boxSizing: "border-box",
            }}
          />
          {error && (
            <p
              style={{
                margin: 0,
                color: "#c0392b",
                fontSize: "0.85rem",
              }}
            >
              {error}
            </p>
          )}
          <button
            type="submit"
            disabled={loading}
            style={{
              marginTop: "0.25rem",
              padding: "0.5rem",
              background: "var(--st-text-primary)",
              color: "var(--st-bg-card)",
              border: "none",
              borderRadius: "6px",
              fontSize: "0.9rem",
              fontFamily: FONT_SANS,
              cursor: loading ? "not-allowed" : "pointer",
              opacity: loading ? 0.6 : 1,
            }}
          >
            {loading ? "..." : "Enter"}
          </button>
        </form>
      </div>
    </div>
  );
}
