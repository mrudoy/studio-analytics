# Studio Analytics — Style Guide & Rules

## Fonts

- **Body font**: `FONT_SANS` = `'Helvetica Neue', Helvetica, Arial, sans-serif`
- **Brand font**: `FONT_BRAND` = `'Cormorant Garamond', 'Times New Roman', serif`
- FONT_SANS must be applied as an inline style on the Card component and the outer dashboard wrapper. Do NOT rely on CSS inheritance alone — Tailwind v4 Preflight overrides it.
- Never remove FONT_SANS inline declarations. They exist to beat Tailwind's specificity.
- FONT_BRAND is only used on the SkyTingLogo component and h1/h2/h3 via globals.css.

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
- `DS.cardPad` (1rem) — padding inside every Card component
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

## Layout Rules

- No horizontal bar charts. No pie charts.
- Weekly and monthly data never mixed in the same card.
- Numbers must use `formatNumber()` for comma formatting.
- Delta values must use `formatDelta()` which includes comma formatting.
- Currency values use `formatCurrency()`.

## Architecture

- `src/app/page.tsx` — single-file dashboard, all components live here
- `src/types/dashboard.ts` — canonical type definitions, page.tsx imports from it
- `src/app/globals.css` — CSS variables, Tailwind import, font rules
- Do not create new component files. Everything stays in page.tsx.
- Do not add new npm dependencies without explicit approval.

## Do NOT

- Remove FONT_SANS inline styles (Tailwind will override fonts)
- Mix Tailwind classes with inline styles for the same property
- Add emojis to the UI
- Create documentation files unless explicitly asked
- Commit changes without building first (`npm run build`)
- Do things not explicitly asked for
