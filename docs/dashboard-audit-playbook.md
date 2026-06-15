# Dashboard audit playbook (Layer 2 — the nightly auditor)

Continuous bug detection has two layers:

- **Layer 1 — `src/lib/db/drift-check.ts`** runs after every pipeline cycle and asserts the invariants behind bugs we've already fixed (dup `order_id`, future-dated rows, unknown-category plans, zero-churn months, revenue net > gross). It guards **known** regressions, with no human.
- **Layer 2 — this playbook** is the proactive net: a nightly agent that hunts for the **next, unknown** data-correctness bug and opens a reviewed, ready-to-merge fix. A human only clicks merge.

## What the nightly routine does

1. Pull latest `main`. Ensure deps installed.
2. Run the detector:
   ```bash
   DATABASE_URL=… AUDIT_BASE_URL=https://studio-analytics-production.up.railway.app \
     npx tsx scripts/audit-dashboard.ts
   ```
   It logs into prod, pulls live `/api/stats`, independently recomputes a panel of anchor metrics from the database, and diffs. Exit `0` = clean, `2` = at least one anchor disagrees, `1` = the audit itself errored.
3. **If exit 0:** stop. Optionally log "clean".
4. **If exit 2:** for each flagged anchor, investigate the root cause (read the owning function, recompute by hand against the DB, confirm it's a real bug and not a stale figure). For each *confirmed* bug:
   - Fix it on a fresh branch off `main`.
   - `npx tsc --noEmit` and `npx vitest run` must pass.
   - **Run the adversarial review loop** ([[adversarial-review-loop]]): Codex first (via the `codex:codex-rescue` subagent), then Greptile (auto-runs on PR open). Address findings; loop until both are clean.
   - Open a **DRAFT** PR with the diagnosis, the independent vs rendered numbers, and the fix.
   - **Add an invariant to Layer 1 (`drift-check.ts`)** so this exact bug can never silently recur.
   - Notify (the PR + routine summary). **Never merge** — a human merges.
5. **If exit 1:** report the error; do not open PRs.

## Hard rules

- **Detection and a reviewed draft PR are fully autonomous; the final merge is not.** Never auto-merge a data-logic change to this prod dashboard. (PR #15 — a plausible auto-fix that destroyed real subscriptions — is why.)
- The detector is **read-only**: it never writes the DB and never edits code.
- Extend `scripts/audit-dashboard.ts` with a new anchor whenever a new headline metric ships, and add the matching Layer-1 invariant whenever a bug is fixed. The two layers grow together.

## Anchors currently checked

active counts (per category, vs raw SQL) · at-risk counts (active + at-risk states, deduped) · MRR = Σcategory · ARPU = MRR / active · new-customer "current week" = calendar week · monthly revenue net ≤ gross & ≥ 0 · monthly churn has no zero-churn completed months. Add more as the dashboard grows.
