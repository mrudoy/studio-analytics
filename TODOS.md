# Running TODO List

Anything written here persists across sessions. Claude reads this at the start of every conversation.

## Open

- [ ] Schedule is now set to every 6 hours (`0 */6 * * *`) — need to re-save from Settings UI on Railway for it to take effect
- [ ] Verify FreshnessBadge pill spacing looks correct on prod after deploy
- [ ] Verify Revenue section shows 2025 total alongside 2026 forecast on prod
- [x] ~~Push latest changes to prod~~ — pushed `35b140e` to origin/main
- [x] Compact DeltaBadge -> small rounded pills with tinted background
- [x] 2025 revenue: sum all periods in prior year instead of annualizing first match
- [ ] "pipeline and database should be flexible" — user wants the data pipeline and DB layer to be more adaptable/extensible
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
- [ ] User note: NO SCRAPING — pipeline triggers CSV downloads via Playwright locally, emails arrive, data gets processed. Browser part must run locally (not Railway).

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
