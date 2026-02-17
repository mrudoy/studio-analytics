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

## Done

- [x] Migrate from SQLite to PostgreSQL (Feb 2025)
- [x] Health endpoint with DB + Redis checks
- [x] Graceful shutdown + pool error handling
- [x] Connection pool tuning (limits, timeouts)
- [x] SQL migration system (`src/lib/db/migrations.ts`)
- [x] Rename sqlite-*.ts to db-*.ts, clean up all "sqlite" references
- [x] Split FreshnessBadge into separate pills (updated time vs next run)
- [x] Revenue section redesigned: "Revenue" header with 2025 actual + 2026 forecast cards
- [x] Default cron changed from twice daily to every 6 hours
