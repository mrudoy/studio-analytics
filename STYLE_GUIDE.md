# Sky Ting Dashboard — Style Guide

Reference this file before creating or modifying any UI component.

---

## Charts (Recharts)

### Required Elements
- **`<CartesianGrid vertical={false} />`** — ALWAYS include on every chart. Horizontal gridlines only.
- **`<XAxis tickLine={false} axisLine={false} />`** — always hide tick marks and axis line
- **`<YAxis hide />`** — always hide Y axis

### Axis Text
- Font size: `fontSize: 10` or `fontSize: 12` (mobile: 11)
- Color: `fill: "var(--muted-foreground)"`
- Tick margin: `tickMargin={8}` to `tickMargin={10}`

### Area Charts
- Curve type: `type="natural"`
- Fill opacity: `fillOpacity={0.4}` with gradient
- Stroke width: `strokeWidth={2}`
- Active dot: `activeDot={{ r: 6 }}`

### Bar Charts
- Corner radius: `radius={[3, 3, 0, 0]}` (top corners only)
- Opacity: `opacity={0.85}`
- Inside labels: `fontSize: 11, fontWeight: 700, fill: "#fff"`
- Top labels: `fontSize: 9-12, fontWeight: 500, fill: "var(--muted-foreground)"`

### Chart Container
- Always wrap in `<ChartContainer config={...} className="h-[Xpx] w-full">`
- Common heights: `h-[100px]` (compact), `h-[220px]` (standard), `h-[250px]` (large)
- Margins: `margin={{ top: 20-24, left: 12-24, right: 12-24 }}`

### Tooltips
- Use `<ChartTooltipContent hideLabel />` for clean tooltips
- Format currency: `formatter={(v) => formatCurrency(v as number)}`

---

## Cards

### Structure
```tsx
<DashboardCard>
  <CardHeader>
    <CardDescription>Label</CardDescription>
    <CardTitle className="text-2xl font-semibold tabular-nums">Value</CardTitle>
  </CardHeader>
  <CardContent>...</CardContent>
  <CardFooter>...</CardFooter>
</DashboardCard>
```

### Compact Card (no inner padding gaps)
When a card needs tight header-to-content spacing (e.g. category detail cards):
```tsx
<DashboardCard className="py-0 gap-0">
  <CardHeader className="pb-0">
    {/* header content — uses CardHeader's built-in px-6 pt via card py */}
  </CardHeader>
  <CardContent>
    {/* chart + metrics — uses CardContent's built-in px-6 */}
  </CardContent>
</DashboardCard>
```
- Override `py-0 gap-0` on `DashboardCard` to remove default `py-6 gap-6`
- Use `<CardHeader className="pb-0">` so header sits flush with content
- Title style: `text-sm leading-none font-semibold` (title case, not uppercase)

### Spacing
- Card padding: `px-6 py-6` (built into Card component, override with `py-0 gap-0` for compact)
- Between cards in grid: `gap-3`
- Between sections: `gap-4` or `gap-6`

---

## Typography

### Font Sizes (Tailwind only — no custom sizes)
| Class | Size | Use |
|-------|------|-----|
| `text-xs` | 12px | Labels, captions, table headers |
| `text-sm` | 14px | Descriptions, secondary text |
| `text-base` | 16px | Body text |
| `text-2xl` | 24px | Card totals, KPI values |
| `text-3xl` | 30px | Section titles, hero metrics |

### Font Weights (only these three)
| Class | Weight | Use |
|-------|--------|-----|
| `font-medium` | 500 | Secondary labels, small details |
| `font-semibold` | 600 | Primary emphasis, titles, metric values |
| `font-bold` | 700 | Rare — inside bar chart labels only |

### Number Display
- **Always** use `tabular-nums` on numeric values
- **Always** use `tracking-tight` on headings and large numbers
- Currency: `formatCurrency(n)` — auto handles $X.XM for millions
- Integers: `formatNumber(n)` — comma separated
- Deltas: `formatDelta(n)` — always includes +/- sign

---

## Colors

### Named Colors (COLORS constant)
| Key | Hex | Use |
|-----|-----|-----|
| member | `#4A7C59` | Members, success states |
| sky3 | `#5B7FA5` | Sky3 plans |
| tv | `#8B6FA5` | Sky Ting TV |
| copper | `#B87333` | Returning non-members, intro |
| dropIn | `#8F7A5E` | Drop-ins |
| merch | `#B8860B` | Merch/Shopify |
| success | `#4A7C59` | Positive change |
| error | `#A04040` | Negative change, churn |
| warning | `#8B7340` | Warnings |

### Delta/Status Colors
| State | Text | Background |
|-------|------|------------|
| Positive (good) | `text-emerald-700` or `text-emerald-600` | `bg-emerald-50` |
| Negative (bad) | `text-destructive` or `text-red-500` | `bg-red-50` |
| Neutral (zero) | `text-muted-foreground` | `bg-muted` |

### Section Colors
- Revenue: `#4A7C59` (forest green)
- Growth: `#5B7FA5` (steel blue)
- Conversion: `#B87333` (copper)
- Churn: `#A04040` (muted red)

---

## Tables

### Header Cells (th)
```
text-right px-3 pt-1 pb-1.5 text-xs font-medium uppercase tracking-wide
text-muted-foreground whitespace-nowrap tabular-nums leading-none border-b border-border
```
- First column: `!text-left`

### Data Cells (td)
```
text-right px-3 py-1.5 tabular-nums text-sm leading-5
```
- First column: `!text-left font-medium`
- Primary values: `font-semibold`
- Row separator: `border-b border-border` (except last row)

### Font Stack
- Always apply `style={{ fontFamily: FONT_SANS }}` on tables

---

## Layout & Grid

### Common Grid Patterns
| Pattern | Classes |
|---------|---------|
| 2-col always | `grid grid-cols-2 gap-3` |
| 1-col → 2-col | `grid grid-cols-1 sm:grid-cols-2 gap-3` |
| 2-col → 3-col | `grid grid-cols-2 sm:grid-cols-3 gap-3` |
| 2-col → 4-col | `grid grid-cols-2 sm:grid-cols-4 gap-3` |

### Spacing Scale
| Gap | Size | Use |
|-----|------|-----|
| `gap-1` | 4px | Icon + label pairs |
| `gap-2` | 8px | Items within sections |
| `gap-3` | 12px | Most common — between cards |
| `gap-4` | 16px | Between major sections |
| `gap-6` | 24px | Card header to content |

---

## Badges & Indicators

### DeltaBadge
- Compact: `rounded-full px-2 py-0.5 text-xs font-medium`
- Full: `rounded-full px-2.5 py-0.5` with arrow (▲/▼)
- Always includes: arrow + value + optional percent in parens

### Colored Dot Indicator
- Size: `w-2.5 h-2.5 rounded-full shrink-0`
- Applied via: `style={{ backgroundColor: color, opacity: 0.85 }}`

---

## Section Headers

### Page Section (icon + title + description)
```tsx
<div className="flex items-center gap-3 px-1">
  <Icon className="size-7 shrink-0" style={{ color: SECTION_COLORS.revenue }} />
  <h1 className="text-3xl font-semibold tracking-tight">Section Title</h1>
</div>
<p className="text-sm text-muted-foreground mt-1 ml-10">Description text</p>
```

### Card Module Header (colored dot + title)
```tsx
<div className="flex items-center gap-2.5">
  <span className="w-2.5 h-2.5 rounded-full" style={{ backgroundColor: color }} />
  <span>Title</span>
</div>
```

---

## Checklist — Before Creating Any New Component

- [ ] Charts have `<CartesianGrid vertical={false} />`
- [ ] XAxis has `tickLine={false} axisLine={false}`
- [ ] YAxis is `hide`
- [ ] All numbers use `tabular-nums`
- [ ] Currency uses `formatCurrency()`, not manual formatting
- [ ] Deltas use color coding (emerald/red/muted)
- [ ] Tables follow th/td class patterns above
- [ ] Font family enforced via `style={{ fontFamily: FONT_SANS }}`
- [ ] Grid uses standard gap-3 between cards
- [ ] Card uses `<DashboardCard>` wrapper
- [ ] Responsive: test mobile (640px breakpoint)
