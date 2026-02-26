# Studio Analytics — Style Guide & Rules

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
- **Title → subtitle gap**: Zero extra margin. Title row has NO `mb-*` class. The subtitle sits flush below the title.
- **Subtitle → content gap**: `mb-1` on the subtitle `<p>` tag. Just enough space before the chart/table.
- **Card descriptions** use `text-sm text-muted-foreground mb-1` — matches Shadcn's CardDescription size (14px), not text-xs (12px).
- **No Annual Churn card** — data is not useful (mostly 0% with rare spikes). Removed by user request.
- **October 2025 excluded** from all churn averages — bulk admin cleanup, not real churn.

## Do NOT

- Remove FONT_SANS inline styles (Tailwind will override fonts)
- Mix Tailwind classes with inline styles for the same property
- Add emojis to the UI
- Create documentation files unless explicitly asked
- Commit changes without building first (`npm run build`)
- Do things not explicitly asked for
- Make cards full-width unless explicitly asked — default is 50% (grid-cols-2)
- Add mb-1 or other margin to card title rows — titles sit flush against subtitles
- Use text-xs for card descriptions — always use text-sm
