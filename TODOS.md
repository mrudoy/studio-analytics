# Running TODO List

Anything written here persists across sessions. Claude reads this at the start of every conversation.

## Open

- [x] ~~Fix stretched fonts~~ — `FONT_SANS` was overriding body's DM Sans with Helvetica Neue; `DS.weight.normal` was 500 (too heavy); DM Sans 700 wasn't loaded but used as bold. Fixed: FONT_SANS → DM Sans, weights → 400/500/600, loaded 700 in layout.tsx.
- [x] ~~**Pipeline automation**: local pipeline server now always-on at `localhost:3099` via launchd (`com.skyting.pipeline-server`). Control panel at `localhost:3099/pipeline` — trigger, progress, freshness display. 6am auto-trigger via `com.skyting.pipeline` launchd agent curls the server with staleness guard (skips if <18h old). Env copied to `~/.local/skyting-pipeline.env` to bypass macOS TCC.~~
- [ ] Schedule is now set to every 6 hours (`0 */6 * * *`) — need to re-save from Settings UI on Railway for it to take effect
- [x] ~~Verify FreshnessBadge pill spacing looks correct on prod after deploy~~ — multiple deploys since, layout confirmed
- [x] ~~Verify Revenue section shows 2025 total alongside 2026 forecast on prod~~ — revenue section redesigned with full monthly bar chart
- [x] ~~Push latest changes to prod~~ — pushed `35b140e` to origin/main
- [x] Compact DeltaBadge -> small rounded pills with tinted background
- [x] 2025 revenue: sum all periods in prior year instead of annualizing first match
- [x] ~~Auto-renews UPSERT~~ — converted `saveAutoRenews()` from DELETE+INSERT (destroyed history each run) to INSERT...ON CONFLICT DO UPDATE SET. Dedup key: `(customer_email, plan_name, created_at)`. Mutable fields (plan_state, plan_price, canceled_at, etc.) updated on conflict. Historical records never deleted.
- [ ] "pipeline and database should be flexible" — user wants the data pipeline and DB layer to be more adaptable/extensible (note: pipeline-core.ts is monolithic but stable; refactor when a specific new report type is needed, not before)
- [x] ~~Build automated database backup system~~ — `/api/backup` endpoint: GET creates backup (JSON export of all 9 tables), saves to `data/backups/`, prunes to last 7. POST restores from file or inline JSON. `?action=download` returns full backup as file download. `?action=list` shows history.
- [x] ~~Prevent data display bugs from reaching prod~~ — all major sections now show "No data available" instead of hiding silently
- [x] ~~Upload 2025 revenue CSV to prod~~ — uploaded 55 categories, $2.2M net, zero warnings
- [ ] User note: email is the atomic unit for a user (for churn/subscriber identity)
- [ ] User note: prefer line charts or bars for churn over time (no pie charts)
- [ ] Churn visualization: consider adding line chart or bar chart for churn trend over time (user preference)
- [x] ~~Pipeline completeness checker~~ — `validateCompleteness()` added to pipeline-core.ts, results shown on completion
- [x] ~~Pipeline progress UI~~ — real-time progress %, ETA, step label in both PipelineView and dashboard FreshnessBadge
- [ ] Cloudflare blocking pipeline on Railway — stealth plugin didn't help. Solution: run pipeline locally (Playwright works on local machine), write data to Railway DB. Need `.env.production.local` with Railway DATABASE_URL.
- [x] ~~Fix churn date parsing~~ — `computeChurnRates()` was doing raw string comparisons on CSV dates. Now uses `parseDate()` to normalize to YYYY-MM-DD (`749db51`)
- [x] ~~Churn section UI~~ — dedicated ChurnSection with 3-tile overview + monthly grouped bar chart (`4a5f9e2`)
- [ ] Churn section shows "No data available" on prod — auto_renews table is empty because pipeline hasn't run against prod DB. Need to run pipeline locally with Railway DATABASE_URL.
- [x] ~~Auto-renew upload endpoint~~ — `/api/upload` now accepts `type=auto_renews` to upload auto-renew CSVs directly
- [ ] Gmail-only pipeline (`gmail-pipeline.ts`) — reads CSVs from Gmail without browser trigger. For future use when emails are pre-triggered.
- [ ] **NEW: Union.fit daily zip pipeline** — Union.fit will send a daily zip to robot@skyting.com with all data. Pipeline needs: login to email → find zip attachment → unzip → parse CSVs → upload to DB → update dashboard. First zip not yet received. This replaces the Playwright scraping approach.
- [ ] **Railway staging environment** — Railway project `handsome-energy` has both production and staging environments. Staging is used for development/testing before promoting to production. Mike built a Shopify API on staging.
- [ ] **Shopify API + Merch tab** — Built on Railway staging. Branch `desktop/member-card-v2` (other machine) is merging main into it, then adding a "Merch" sub-tab inside Revenue section alongside "Class Revenue". Uses Tabs component with variant="line". Shopify API feeds the Merch data. DO NOT edit Revenue section here — being modified on other machine.
- [ ] User note: NO SCRAPING — pipeline triggers CSV downloads via Playwright locally, emails arrive, data gets processed. Browser part must run locally (not Railway).
- [ ] User note: Mike works on TWO machines — another machine may be running dev server or making changes simultaneously. Watch for port conflicts, stale processes, and git divergence.
- [ ] **Daily email digest** — Send the Overview page data (Yesterday, Last Week, This Month, Last Month) as an email to 4 recipients at 7am every morning. Need to decide: email service (Resend, SendGrid, SES, or Gmail API), template format (HTML email matching dashboard style), and where it runs (Railway cron or local launchd trigger after pipeline completes). Recipients TBD (get 4 addresses from Mike).
- [ ] **Shopify refresh button broken** — Button on merch page silently fails. Root cause: API version `2024-01` sunset + empty catch block hid all errors. Fix in progress: updated to `2025-01`, added error display to UI. Still needs: push + verify on prod.
- [x] ~~**Intro Week purchases not showing on dashboard** — 14 Intro Week orders placed Mon-Tue (2/23-2/24, $343.20 total) but dashboard shows none. Root cause: dashboard queried stale `first_visits` table instead of `registrations`. Fixed: switched 6 query functions to use `registrations` table. Committed in `742e464`.~~
- [ ] **Intro Weeks count = visits, not unique people** — The "Intro Weeks" number (e.g. 18) on the overview page counts total registrations (`COUNT(*)`) not unique people (`COUNT(DISTINCT email)`). One person attending 3 intro week classes = counted 3 times. Source: `getIntroWeekCountForRange()` in `registration-store.ts`. Should clarify label or switch to unique people. Also: this is redeemed (attended a class), not purchased.
- [ ] **CategoryDetail: churn row Change column has inverted color** — The "Change" column shows WoW delta (vs prior week). For "Churned last week", the color is inverted: +7 (more cancellations than prior week) shows GREEN but should be RED because more churn is bad. Conversely, if churn drops, it shows red but should be green. The "New" row colors are correct. Fix: invert polarity on the churn row only. User confirmed they understand the Change column is WoW comparison.
- [ ] **Import Union.fit full data export** — 380K orders, 364K registrations, 28K memberships, 60K passes sitting in `~/Downloads/union_data_export-sky-ting-20230830-20260223-zFoHME8GZ2ESfu8LzvDe27NU/`. Every order linkable to email via membership_id. Full relational DB export (33 CSVs). Need import script.
- [x] ~~**Remove redundant tooltip on annual revenue bar chart** — Removed ChartTooltip from AnnualRevenueCard. Committed in `742e464`.~~
- [x] ~~**MRR card needs month labels** — Added "February 2026" label to MRR card. Committed in `742e464`.~~
- [ ] **Revenue double-counting bug** — `revenue_categories` table has overlapping period ranges for the same month (e.g. Feb 1-23, Feb 24-24, plus all-time rows). Queries SUM all matching rows, inflating numbers ~2x. Spa showed $15K instead of ~$7K for Feb. Root cause: pipeline runs save cumulative Union.fit data with different period ranges; unique key `(period_start, period_end, category)` doesn't prevent within-month overlap. Fix: dedup queries with `DISTINCT ON`, fix save layer to replace same-month data, cleanup migration.
- [x] ~~**Overview page redesign** — Replaced KPI hero with time-based sections (Yesterday, Last Week, This Month, Last Month). Grouped cards: Subscriptions, Activity, Merch Revenue. Committed `3e08e04`.~~
- [x] ~~Style guide~~ — created `docs/style-guide.md` with full typography hierarchy (section headers, sub-group headers, card titles, KPI values), color system, icon mapping, chart rules, and anti-patterns. Sub-group headers: text-[15px] font-semibold text-muted-foreground, no icons, no border-left.

### Data Report Key (from Data.pdf)
When user asks about data, tell them which report to pull based on this mapping:

**SALES:**
| Data | Report Name | Path | What it tells you |
|------|------------|------|-------------------|
| Net Revenue | Sales by Revenue Category | Reports > Sales > "Sales by Revenue Category" | Where the revenue came from |
| Itemized Sales | Sales By Service | Reports > Sales > Sales By Service | The number (#) of services sold by service |
| New Auto-renew Purchases | New | Reports > Auto Renew > New | New auto-renew purchases within a specified window |
| New Customers | New customers | Reports > Customers > New customers | New Accounts Created |
| First Visits | First Visit | Reports > Registrations > First Visit | Anyone who redeemed their first class in studio or online |
| Registrations | All | Reports > Registrations > All | All registrations |

**AUTO-RENEW** (includes Sky Ting TV, Sky3, Member):
| Data | Report Name | Path | What it tells you |
|------|------------|------|-------------------|
| Auto-Renewing Count | Summary | Reports > Auto Renew > Summary | Total people currently subscribed (net of churn) |
| Auto-Renew Cancellations | Cancelled | Reports > Auto Renew > Cancelled | Total people that cancelled during a specific window |
| Auto-Renew New | New | Reports > Auto Renew > New | Total people that signed up for an auto-renew during a specific window |

### Monthly Revenue Rule (NEW)
**RULE**: Revenue data MUST be stored at monthly granularity so we can do MoM comparisons and growth analysis. Each month gets its own `period_start` / `period_end` (e.g. `2024-01-01` to `2024-01-31`). The report source is "Sales by Revenue Category" (`/reports/revenue`) on Union.fit, pulled once per month with the date range set to that month.
- User will manually upload historical months (Jan 2024 through present)
- Pipeline must auto-pull current month's data going forward
- Annual summary rows (Jan 1 - Dec 31) can coexist since the unique key is `(period_start, period_end, category)`

### Reports We Need to Track
These are the Union.fit reports the pipeline needs to pull regularly. User uploads historical data manually; pipeline handles ongoing.

| Report | Union.fit Path | Frequency | Granularity | Status |
|--------|---------------|-----------|-------------|--------|
| **Revenue by Category** | `/reports/revenue` | Monthly | Per month | Manual backfill in progress |
| Active Auto-Renews | `/report/subscriptions/list?status=active` | Weekly | Snapshot | In pipeline |
| Paused Auto-Renews | `/report/subscriptions/list?status=paused` | Weekly | Snapshot | In pipeline |
| Canceled Auto-Renews | `/report/subscriptions/growth?filter=cancelled` | Weekly | Snapshot | In pipeline |
| New Auto-Renews | `/report/subscriptions/growth?filter=new` | Weekly | Snapshot | In pipeline |
| New Customers | `/report/customers/created_within` | Weekly | Date range | In pipeline (empty) |
| Orders | `/reports/transactions?transaction_type=orders` | Weekly | Date range | In pipeline (empty) |
| First Visits | `/report/registrations/first_visit` | Weekly | Date range | In pipeline (empty) |
| All Registrations | `/report/registrations/remaining` | Weekly | Date range | In pipeline (empty) |
| Customer Export | People > Export | Monthly | Full snapshot | Manual upload done (8,661) |

### Revenue Category Terms Rule
**RULE**: Only use defined business category labels. Any raw Union.fit category name that doesn't match a known pattern MUST be flagged as "Other" and logged so we can ask the user how to classify it. Never invent or guess new category names. The defined labels are:
- Members, SKY3 / Packs, SKY TING TV, Drop-Ins, Intro / Trial, Workshops, Wellness / Spa, Teacher Training, Retail / Merch, Privates, Donations, Rentals, Retreats, Community
- Anything else → "Other" (flag for review)

## Done

- [x] Inline per-category churn into CategoryDetail cards (user churn %, MRR churn %, at-risk, monthly bars, MEMBER annual/monthly split) — `c77eca0`
- [x] Fix TrendRow last-child bottom border (isLast prop)
- [x] Fix KPI hero spacing (gap-1.5, marginTop 3px)
- [x] Drop partial week bars from charts
- [x] Fix WoW metrics to use completed weeks
- [x] Normalize all date fields to YYYY-MM-DD (migration 002)
- [x] Migrate from SQLite to PostgreSQL (Feb 2025)
- [x] Health endpoint with DB + Redis checks
- [x] Graceful shutdown + pool error handling
- [x] Connection pool tuning (limits, timeouts)
- [x] SQL migration system (`src/lib/db/migrations.ts`)
- [x] Rename sqlite-*.ts to db-*.ts, clean up all "sqlite" references
- [x] Split FreshnessBadge into separate pills (updated time vs next run)
- [x] Revenue section redesigned: "Revenue" header with 2025 actual + 2026 forecast cards
- [x] Default cron changed from twice daily to every 6 hours
