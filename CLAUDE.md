# Studio Analytics — Style Guide & Rules

## Active Subscriber Definition

**"Active" means `plan_state IN ('Valid Now', 'Paused', 'Pending Cancel', 'In Trial', 'Invalid', 'Past Due')`.** This matches Union.fit's admin definition. The single canonical source is `ACTIVE_STATES` in `src/lib/analytics/metrics/filters.ts` — import from there, never inline.

All queries that filter for active subscribers must use this exact set of states. Only `Canceled` is excluded. This rule applies to all files that query the `auto_renews` table.

## Fonts

- **Body font**: `FONT_SANS` = `'Helvetica Neue', Helvetica, Arial, sans-serif`
- **Brand font**: `FONT_BRAND` = `'Cormorant Garamond', 'Times New Roman', serif`
- FONT_SANS must be applied as an inline style on the Card component and the outer dashboard wrapper. Do NOT rely on CSS inheritance alone — Tailwind v4 Preflight overrides it.
- Never remove FONT_SANS inline declarations. They exist to beat Tailwind's specificity.
- FONT_BRAND is only used on the SkyTingLogo component via inline style. It must NEVER appear in globals.css on h1/h2/h3 — that overrides FONT_SANS on dashboard headings.
- Never add a global CSS rule that sets font-family on h1, h2, or h3 elements.

## Design System Tokens (DS object)

All spacing, sizing, and typography must use the DS object in page.tsx. Never use raw values.

### Typography (5 sizes only)
- `DS.text.xs` (0.75rem) — uppercase labels, captions
- `DS.text.sm` (0.9rem) — secondary text, sublabels
- `DS.text.md` (1.05rem) — body text, card titles
- `DS.text.lg` (1.75rem) — metric values, hero numbers
- `DS.text.xl` (2.5rem) — page-level KPIs (KPIHeroStrip, CategoryDetail)

### Font Weights (3 only)
- `DS.weight.normal` (400)
- `DS.weight.medium` (500)
- `DS.weight.bold` (600)

### Card Layout
- `DS.cardPad` (0.75rem) — padding inside every Card component
- `DS.cardHeaderMb` (0.5rem) — gap between card header row and content below. Every card header must use this.

### Spacing Scale
- `DS.space.xs` (0.25rem), `DS.space.sm` (0.5rem), `DS.space.md` (1rem), `DS.space.lg` (1.5rem), `DS.space.xl` (2rem)

### Label Style
- `DS.label` — spread object for uppercase section labels: `{...DS.label}`

## Colors

Use the COLORS object and CSS custom properties from globals.css. Never use raw hex values inline.

- `COLORS.member` — forest green (#4A7C59)
- `COLORS.sky3` — steel blue (#5B7FA5)
- `COLORS.tv` — muted purple (#8B6FA5)
- `COLORS.teal` — first visits (#3A8A8A)
- `COLORS.copper` — returning non-members (#B87333)
- `var(--st-text-primary)` — main text (#413A3A)
- `var(--st-text-secondary)` — secondary text (rgba)
- `var(--st-bg-card)` — card background
- `var(--st-bg-section)` — page background
- `var(--st-border)` — card borders

## Labels

All user-facing labels must come from the `LABELS` constant. Never use inline strings for category or section names.

Labels use honest data terminology matching Union.fit. Not marketing names.
- "Auto-Renews" not "Subscriptions"
- "Sky3" not "Memberships" or "SKY3 / Packs"
- "Drop-Ins" not "Walk-ins"

## Currency Formatting

- **Millions**: Always show as `$X.XM` (one decimal, drop `.0`). Example: `$2,234,000` becomes `$2.2M`. Never show millions as thousands (`$2234k` is wrong).
- **Thousands** (compact only): `$234k` via `formatCompactCurrency()`.
- **Under 1000**: Full dollar amount with comma formatting.
- `formatCurrency()` — use for most displays. Auto-formats millions as `$X.XM`.
- `formatCompactCurrency()` — use for chart axis labels. Millions as `$X.XM`, thousands as `$Xk`.

## Layout Rules

- No horizontal bar charts. No pie charts.
- Weekly and monthly data never mixed in the same card.
- All dates under bar graphs must be MM/DD/YY format (e.g. `02/19/26`). Use `formatDateShort()` or `formatWeekShort()`.
- Numbers must use `formatNumber()` for comma formatting.
- Delta values must use `formatDelta()` which includes comma formatting.
- Currency values use `formatCurrency()` (auto-handles millions).

## NEVER DELETE DATA (PERMANENT RULE)

**Revenue data is append-only. The database is the permanent archive. Data must NEVER be erased.**

- `saveRevenueCategories()` uses pure `INSERT ... ON CONFLICT DO UPDATE` (upsert). No DELETE statements.
- Multiple period ranges for the same month can coexist. The dedup queries (`DISTINCT ON ... ORDER BY period_end DESC`) pick the best/latest data per category per month.
- There is no `deleteMonthData()` function. It was removed. Do not recreate it.
- If new data is partial (fewer categories or less revenue), it gets inserted alongside existing data — it does NOT replace it. The dedup queries handle picking the right rows.
- This rule applies to ALL tables, not just `revenue_categories`. The DB is the single source of truth and historical archive. Pipeline operations are additive only.
- If you need to "fix" data, upsert the correct values. Never delete the old ones.

## QUERYING REVENUE (PERMANENT RULE)

**Never write a bare `SUM(revenue) FROM revenue_categories WHERE ...` query.** Because the table is append-only, multiple period slices can coexist for the same month, and a raw SUM will double-count.

- **Prefer canonical functions** in `src/lib/db/revenue-store.ts`:
  - `getAllMonthlyRevenue()` — per-month totals
  - `getMonthlyRetreatRevenue()` — per-month retreat gross/net
  - `getMonthlyMerchRevenue()` — per-month merch gross/net
  - `getMonthlySubscriptionBilling()` — per-month subscription (Member + Sky3 + TV) gross/net
  - `getRevenueForPeriod(start, end)` — single-period category breakdown
- **If you must write ad-hoc SQL**, use the same dedup pattern every canonical function uses:
  ```sql
  WITH deduped AS (
    SELECT DISTINCT ON (TRIM(category), TO_CHAR(period_start, 'YYYY-MM'))
      TRIM(category) AS category, revenue, net_revenue, period_start
    FROM revenue_categories
    WHERE DATE_TRUNC('month', period_start) = DATE_TRUNC('month', period_end)
    ORDER BY TRIM(category), TO_CHAR(period_start, 'YYYY-MM'), period_end DESC, created_at DESC
  )
  SELECT ... FROM deduped ...
  ```
- **TRIM category names** — the raw data contains trailing-whitespace variants ("SKY UNLIMITED" vs "SKY UNLIMITED "). Always `TRIM(category)` or `GROUP BY TRIM(category)`.
- **Before reporting a revenue number to the user**, sanity-check it against `/api/stats` — if they disagree, the ad-hoc query is wrong, not the dashboard.

Reason: 2026-04-13 investigation showed a bare `SUM` query inflated March subscription billing from the correct $153,525 to $176,727 because duplicate period slices were summed twice.

## METRIC SOURCES (PERMANENT RULE)

**Every dashboard metric has exactly one canonical function that owns it. Never recompute a dashboard metric inline; always consume from the canonical function or the API field it produces.**

### plan_state filter constants — `src/lib/analytics/metrics/filters.ts`

Always import these. Never inline the state strings in a new SQL query or TypeScript filter. The constants are paired with `*_SQL` versions (e.g., `ACTIVE_STATES_SQL`) for use inside `IN (...)` clauses.

| Constant | States | Use for |
|---|---|---|
| `ACTIVE_STATES` | Valid Now, Paused, Pending Cancel, In Trial, Invalid, Past Due | Counting active subscribers; active-at-start denominator for churn rates |
| `BILLING_STATES` | Valid Now, Pending Cancel | MRR (revenue being recognized this month, ASC 606-aligned) |
| `STILL_PAYING_STATES` | Valid Now, Paused | For canceled_at gating: rows in these states have `canceled_at = next billing date`, NOT a real cancellation. Real cancels = `plan_state NOT IN STILL_PAYING_STATES` |
| `AT_RISK_STATES` | Past Due, Invalid, Pending Cancel | Churn-risk alerts and insight detectors |

### Canonical function map

| Metric | Canonical function | File |
|---|---|---|
| Active subscriber counts | `getActiveCounts()` → `getAutoRenewStats()` | `src/lib/db/auto-renew-store.ts` |
| MRR (run-rate) | `getAutoRenewStats()` (uses `BILLING_STATES`) | `src/lib/db/auto-renew-store.ts` |
| Subscription billing per month | `getMonthlySubscriptionBilling()` | `src/lib/db/revenue-store.ts` |
| Monthly gross revenue | `getAllMonthlyRevenue()` + `getMonthlyRetreatRevenue()` | `src/lib/db/revenue-store.ts` |
| Cancellations by window (yesterday / week / month) | `getSubscriberMovement()` | `src/lib/analytics/metrics/subscriber-movement.ts` |
| New signups by window | `getSubscriberMovement()` | `src/lib/analytics/metrics/subscriber-movement.ts` |
| Plan changes (upgrades/downgrades) | `getSubscriberMovement()` → `WindowMovement.planChanges` | `src/lib/analytics/metrics/subscriber-movement.ts` |
| Weekly + monthly churn rates | Derived from `getSubscriberMovement()` at API layer (route.ts override) | `src/app/api/stats/route.ts` |
| 6-month avg churn rate | Computed at API layer from the canonical per-month rates | `src/app/api/stats/route.ts` |
| Active-at-period-start (for rate denominator) | `getSubscriberMovement()` → `CategoryMovement.activeAtStart` | `src/lib/analytics/metrics/subscriber-movement.ts` |

**API output shape:** Every consumer reads from `data.movement.*` (per-window/period canonical output) OR from `data.trends.*` (which has the same values overwritten at API assembly in route.ts). Never recompute inline.

### Anti-patterns

- **Don't** inline `plan_state IN ('Valid Now', ...)` in new SQL — import `ACTIVE_STATES_SQL`.
- **Don't** write a new function that recomputes cancellations or MRR from scratch — consume the canonical one or its API output.
- **Don't** add a new variant of "active" with one extra/missing state without first updating the canonical filter and checking every consumer.

`grep -rE "'(Valid Now|Paused|Pending Cancel|In Trial|Invalid|Canceled|Past Due)'" src/` should return matches only inside `filters.ts`, `migrations.ts` (DB-level SQL/triggers), the upload route's parser, and bespoke filters explicitly commented as broader/narrower than canonical.

## ACTIVE SUBSCRIBER COUNTING (PERMANENT RULE)

**An active subscriber row = `plan_state IN ('Valid Now', 'Paused', 'In Trial', 'Invalid', 'Pending Cancel', 'Past Due')`.** (6 states. Only `Canceled` is excluded.)

- **6 states are active.** Invalid = used all passes (still a subscriber). Pending Cancel = canceling next cycle (currently active). Past Due = payment failed but Union is still retrying — Union counts these as active, so we do too.
- **Counts are subscription rows, NOT unique people.** A person paying for both `SKY3 Monthly` and `SKYHIGH3 Monthly` counts twice in Sky3 — they're paying for both, and Union's admin counts the same way. A person with subscriptions in multiple categories (e.g. Member + TV) counts once in each category. `total` = sum of category counts.
- **Do NOT use `canceled_at` as a filter for active subscribers.** For active subscribers, Union.fit sets `canceled_at` to the next billing/renewal date, NOT the cancellation date. Using `canceled_at <= NOW()` as a guard will filter out nearly everyone.
- **Daily zip exports are DELTAS, not full snapshots.** They contain only recent changes — NOT all active subscribers. NEVER run reconciliation against a daily export or it will mass-cancel everyone.
- `reconcileAutoRenews()` exists but must ONLY be called with a full subscriber list (e.g. the "subscriptions changes" report from Union.fit admin, or a manual CSV upload). It is NOT wired into the daily pipeline.
- **`auto_renews.created_at` and `canceled_at` store the EASTERN calendar date** (America/New_York), not the UTC date. Always write them through `toEasternDate()` in `src/lib/db/eastern-date.ts`. Never pass a raw Union timestamp directly to these DATE columns — the UTC session will roll evening-ET subscriptions +1 day, breaking dedup and reconcile keys. `pending_canceled_at` is TIMESTAMPTZ and keeps full precision (no conversion needed).
- **Reconcile and dedup must key on `order_id` first, then `union_pass_id`, and only use `email|plan|created_date` as a last resort.** The `email|plan|created_date` tuple is fragile: before dates are backfilled it can still be +1 day off. A reconcile keyed solely on the date tuple will produce false-positive cancellations.
- **History:** Mike set the original "5 states + dedup by email" rule on 2026-04-01. Reverted on 2026-05-08 after a fresh CSV from Union showed our counts diverging again — gap analysis revealed (a) 129 `FABxSKYTING` rows were silently miscategorized as UNKNOWN, (b) 17 people had multiple subscriptions in the same category that the dedup was hiding, and (c) Union includes Past Due. Switching to row counts + including Past Due + fixing FABxSKYTING categorization aligns the dashboard exactly with Union's admin (482/395/1868 against the 2026-05-08 CSV). On 2026-06-05 a further root cause was identified: `created_at` was stored +1 day due to UTC session + DATE column, fixed by `toEasternDate()` (PR #14).

## INTRO WEEK CONVERSION (PERMANENT RULE)

**The "Intro Week → sub start" conversion rate is cohort-based, not bucket-based, and includes Sky Ting TV.**

- **Pool (denominator):** unique `LOWER(email)` whose **first** intro attendance (`attended_at IS NOT NULL`, non-AR, pass matching `INTRO | TRIAL | FIRST`) falls in the last N complete Mon–Sun weeks. Each person counts once total — in their first cohort week — never once per week they attended.
- **Converts (numerator):** subset of the same cohort whose email later started a Member, Sky3, **or Sky Ting TV** auto-renew (`auto_renews.created_at >= first_intro_at`). State-agnostic: a sub started then canceled still counts as a conversion.
- **Plan filter for conversion:** use `ANY_AR_PLAN_FILTER` in `src/lib/db/registration-store.ts` (Member/Sky3 set + TV). Do NOT use `IN_STUDIO_PLAN_FILTER` for this metric — it strips TV.
- **Canonical function:** `getIntroWeekCohortConversionWeekly(weeksBack)` in `src/lib/db/registration-store.ts`. Wired in via `runJourneyConversion()` in `src/lib/analytics/db-trends.ts` and rendered by the "Intro Week" `ConversionByTypeCard` in `src/app/page.tsx`.
- **Recency caveat:** the most recent cohort weeks have had less time to convert, so their per-week rates appear low. The headline rate is a lower bound that grows as cohorts mature. This is by design — do not "correct" it by dropping recent weeks unless explicitly asked.

**Intro pass matching (PERMANENT):**
- All intro-pass matching goes through `introPassLike()` / `introPassFilter()` in `src/lib/db/registration-store.ts` (and `isDropInOrIntro()` in `categories.ts` for TS-side checks). Never inline `'%INTRO WEEK%'` in SQL.
- The match is `'%INTRO%'` (broad), NOT `'%INTRO WEEK%'`: the studio renames its intro offer. "2 WEEK INTRO" launched 2026-04-07, contains "INTRO" but not the substring "INTRO WEEK", and the old pattern silently zeroed the intro pool/conversions for two months (caught 2026-06-10). Every INTRO-containing pass name in 26 months of data is a genuine intro offer, so the broad match is safe.
- **Intro window length is pass-dependent:** `introWindowDaysSql()` — 14 days for "2 WEEK INTRO" passes (`'%2 WEEK%'`), 7 days otherwise. The active/expiring/expired intro cards must use it, never a hardcoded 7-day window.

**Anti-patterns:**
- Do NOT compute the intro-week rate by summing per-week distinct-email pools. That double-counts emails who attended in 2+ weeks (e.g. 215 vs the correct 183 unique people for the 2026-05-08 8-wk window).
- Do NOT reuse `getConversionByJourneyWeekly(_, "intro-week")` for this card — that function uses bucket math (numerator and denominator are different sets of people, TV excluded). It remains valid for `"all"` and `"a-la-carte"` buckets, where bucket math is the intended semantics.

History: 2026-05-08 verification against the user's stated definition exposed the bucket-math vs cohort-math gap. Dashboard previously displayed 38.6% under bucket math; cohort math + TV produces ~13.7% on the same window. Switched the canonical function and saved this rule. 2026-06-10: "2 WEEK INTRO" rebrand exposed the brittle `'%INTRO WEEK%'` match (pool collapsed to ~21 people/8wk vs the true 281, converts to 0); switched to `introPassLike()` + pass-dependent windows.

## CHURN / SIGNUP EVENTS (PERMANENT RULE)

**All churn and signup counts for time-window cards come from `auto_renew_events`, not from static columns on `auto_renews`.**

- `auto_renew_events` is an append-only log written by a Postgres trigger on every `plan_state` change in `auto_renews`. See migration `019_auto_renew_events_log`.
- **Churn = entering a churned state (`Canceled` or `Pending Cancel`) from any other state.** This is when the user clicked cancel — NOT when their paid period ends. Dashboard queries filter `event_type IN ('churn','backfill_churn')`.
- **`final_cancel`** events (Pending Cancel → Canceled) are logged for audit but EXCLUDED from churn counts so the same cancellation isn't counted twice.
- **Backfill events** (`backfill_signup`, `backfill_churn`) were synthesized from `created_at` / `canceled_at` at migration time. They count identically to live events in dashboard queries. The `is_backfill` flag exists for audit only.
- Do NOT add new consumers that compute churn from `canceled_at + plan_state='Canceled'`. That pattern is semantically broken (misses Pending Cancel + wrong timing). Use `getChurnEventsInWindow()` in `src/lib/db/auto-renew-events-store.ts`.
- Follow-up: `src/lib/analytics/db-trends.ts` still uses the legacy `getCanceledAutoRenews` for weekly/monthly churn rollups. Migrate those next.

## Architecture

- `src/app/page.tsx` — single-file dashboard, data logic and module-level components live here
- `src/types/dashboard.ts` — canonical type definitions, page.tsx imports from it
- `src/app/globals.css` — CSS variables, Tailwind import, font rules
- `src/components/dashboard/` — shared dashboard UI primitives (DashboardCard, ModuleHeader, MetricRow, InfoTooltip, Chip, CardDisclosure, SparklineSlot, SectionHeader). Barrel-exported from `index.ts`.
- `src/components/ui/` — shadcn/ui primitives (Card, Chart, Tabs, Select, Tooltip, Badge, Separator). Do not modify these directly.
- Do not add new npm dependencies without explicit approval.

## Card Spacing & Typography Rules

- **Uniform gap between ALL cards**: `gap-3` everywhere. Never use different gaps between sections vs within sections.
- **Cards are 50% width** by default (in `grid-cols-2`), not full-width, unless the content requires it (e.g. wide tables).
- **Orphan card rule**: A single card alone in a 2-column grid (50% width) is OK **only if it's the last card in the section**. If a single card is followed by more cards in the same section, it must be full width — no dead space in the middle of a layout. In other words: `[1 card] [2 cards]` within the same section is wrong; `[2 cards] [1 card]` is fine.
- Before adding a new card, check whether the nearest existing grid has an odd number of children — if so, add the new card there instead of creating a new grid wrapper.
- **Title → subtitle gap**: Zero extra margin. Title row has NO `mb-*` class and NO `min-h-*` class. The subtitle sits flush below the title.
- **Subtitle → content gap**: `mb-3` on the subtitle `<p>` tag. Generous space before the chart/table (matches Shadcn reference).
- **Card descriptions** use `text-sm text-muted-foreground mb-3` — matches Shadcn's CardDescription size (14px), not text-xs (12px).
- **No Annual Churn card** — data is not useful (mostly 0% with rare spikes). Removed by user request.
- **October 2025 excluded** from all churn averages — bulk admin cleanup, not real churn.

## API & Performance

- `/api/stats` — main dashboard endpoint, returns all data as JSON
- **In-memory cache**: Full response is cached in `globalThis` with 15-min TTL (`src/lib/cache/stats-cache.ts`). Cache is invalidated on pipeline completion and manual uploads.
- `?nocache=1` query param bypasses the cache (forces fresh DB queries).
- Response includes `X-Cache: HIT/MISS` and `X-Cache-Age` headers.
- `computeTrendsFromDB()` runs sections 5-11 in parallel via `Promise.all` (each section is independent).
- Conversion pool queries use an UNLOGGED materialized table (`_mat_first_in_studio_sub`) to avoid 15 redundant CTE scans.
- Migration 007 adds 6 indexes on hot join columns for conversion pool performance.

## Critical Path — DO NOT BREAK

These are the system invariants that keep the pipeline, database, and emails working. Before modifying any of these files, verify that ALL of these still hold after your change.

### Pipeline Scheduler (`src/instrumentation.ts`)
- `runScheduledPipeline()` must be called on server startup (after 30s delay) and every 4 hours via `setInterval`.
- If you refactor instrumentation.ts, the scheduler MUST remain. Without it, no data gets fetched, no metrics are computed, and no emails are sent.

### Data Pipeline (`src/lib/email/zip-download-pipeline.ts`)
- Must call `saveRegistrations()` for registration data.
- Must call `backfillRegistrationEmails()` after registrations are saved.
- Must call `recomputeFirstVisitFlags()` after email backfill — this marks each email's earliest `attended_at` as `is_first_visit = TRUE`. Without it, dashboard first-visit metrics, new customer volume, and cohort conversion all go stale.
- Must call `recomputeRevenueFromDB()` to derive revenue from raw DB data.
- Must update watermarks for all processed data types at the end.

### Digest Email
- `sendDigestEmail()` must be called after a successful pipeline run. Currently called from: `instrumentation.ts` (scheduler), `/api/cron/pipeline`, and `/api/reprocess`.
- Has a once-per-day atomic guard (watermark claim in `fetch_watermarks`), so multiple call sites are safe.
- If you add a new pipeline entry point, it must also call `sendDigestEmail()`.

### Verification after changes to these files
- `npm run build` must pass.
- Check that `instrumentation.ts` still starts the scheduler.
- Check that the zip pipeline still calls: `saveRegistrations`, `backfillRegistrationEmails`, `recomputeFirstVisitFlags`, `recomputeRevenueFromDB`.
- Check that `sendDigestEmail()` is called after successful processing.

## Do NOT

- **DELETE revenue data from the database — EVER.** No DELETE queries on revenue_categories. No "clear and replace" patterns. Data is append-only. Upsert only.
- Remove FONT_SANS inline styles (Tailwind will override fonts)
- Mix Tailwind classes with inline styles for the same property
- Add emojis to the UI
- Create documentation files unless explicitly asked
- Commit changes without building first (`npm run build`)
- Do things not explicitly asked for
- Make cards full-width unless explicitly asked — default is 50% (grid-cols-2)
- Put a single card at 50% width in the middle of a section with more cards below it — orphans are only OK as the last card
- Add mb-* or min-h-* to card title rows — titles sit flush against subtitles
- Use text-xs for card descriptions — always use text-sm
