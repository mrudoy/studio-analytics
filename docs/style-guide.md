# Dashboard Style Guide

Typography, spacing, component, and chart rules for the studio analytics dashboard. All styles use Tailwind CSS v4 utility classes. Charts use Recharts via shadcn/ui ChartContainer.

## Font

- **Family**: DM Sans (loaded via next/font at weights 400, 500, 600, 700)
- **Tabular numbers**: Always use `tabular-nums` on numeric values for column alignment

## Typography Hierarchy

### Level 1: Page / Section Header
The top-level title for each sidebar section (Overview, Revenue, Auto-Renews, etc.).

```
<h1 class="text-3xl font-semibold tracking-tight">
```

- Always paired with a colored icon (size-7) from `icons.tsx`
- Optional subtitle below: `text-sm text-muted-foreground mt-1 ml-10`
- Layout: `flex items-center gap-3` for icon + title row

### Level 2: Sub-Group Header
Groups cards within a section (e.g. "In-Studio Plans", "Digital" inside Auto-Renews).

```
<h3 class="text-[15px] font-semibold tracking-tight text-muted-foreground mb-3">
```

- **No icon**
- **No border-left line**
- **No color** — always `text-muted-foreground`
- Plain text label only
- Used to visually separate groups of cards within a section

### Level 3: Card Title (inside CategoryDetail)
The title bar of a subscription or data card.

```
<span class="text-base leading-5 font-semibold tracking-tight text-foreground">
```

- Paired with a colored category icon (size-4) from `icons.tsx`
- Icon color matches the category color (e.g. Members = blue, SKY3 = purple, TV = rose)
- Layout: `flex items-center gap-2`

### Level 4: KPI Card Label
The descriptor above a big number in a KPI card.

```
<CardDescription class="text-sm leading-none font-medium text-muted-foreground uppercase tracking-wide">
```

### Level 5: KPI Card Value
The big number inside a KPI card.

```
<CardTitle class="text-3xl font-semibold tracking-tight tabular-nums">
```

- For hero tiles: conditionally colored green (`text-emerald-600`) or red (`text-red-600`) based on trend direction
- For standard KPI cards: `text-2xl font-semibold tabular-nums`

### Level 6: Table Headers

```
class="text-xs font-medium uppercase tracking-[0.04em] text-muted-foreground"
```

### Level 7: Table Cell Values

```
class="text-[14px] leading-[20px] font-semibold tabular-nums"   // numeric values
class="text-[14px] leading-[20px] font-medium text-muted-foreground"  // labels
```

## Color System

### Section Colors (sidebar + section header icons)
Each section has a dedicated color used for its sidebar icon and page header icon:

```tsx
const SECTION_COLORS = {
  overview:        "#6366f1",  // indigo
  revenue:         "#22c55e",  // green
  "growth-auto":   "#3b82f6",  // blue
  "growth-non-auto": "#f97316", // orange
  "conversion-new": "#a855f7",  // purple
  "conversion-pool": "#ec4899", // pink
  churn:           "#ef4444",  // red
  data:            "#64748b",  // slate
};
```

### Category Colors (cards, charts, icons)
Each subscription/data category has its own color:

```tsx
const COLORS = {
  member:  "#5B8DEF",  // blue
  sky3:    "#A78BFA",  // purple
  tv:      "#F472B6",  // rose/pink
  dropIn:  "#F59E0B",  // amber
  intro:   "#34D399",  // emerald
  warning: "#F59E0B",  // amber (at-risk)
};
```

### Semantic Colors
- Positive/good: `text-emerald-600`
- Negative/bad: `text-red-600` or `text-destructive`
- Neutral: `text-muted-foreground`

## Icons

### Source
All custom icons live in `src/components/dashboard/icons.tsx`. They follow the Tabler icon style (24x24 viewBox, stroke-based, 2px stroke width).

### Usage by Context

| Context | Icon Size | Example |
|---------|-----------|---------|
| Section header (h1) | `size-7` | `<Recycle className="size-7" />` |
| Category card header | `size-4` | `<ArrowBadgeDown className="size-4" />` |
| Sidebar nav item | `size-4` | Via Sidebar component |

### Category Icon Mapping

| Category | Icon | Component |
|----------|------|-----------|
| Members | Arrow badge down | `ArrowBadgeDown` |
| SKY3 / Packs | Butterfly | `BrandSky` |
| Sky Ting TV | TV device | `DeviceTv` |
| Class Revenue | Home + dollar | `ClassRevenue` |
| Merch | T-shirt | `ShoppingBag` |

## Components

### DashboardCard
Wrapper for all card-style content. Uses shadcn Card with consistent padding.

### Canonical Card Interior Layout

**Every** card that contains a title + table (or title + content) MUST follow this exact structure so that titles, descriptions, and content rows align across sibling cards in a grid:

```tsx
<Card matchHeight>
  {/* Row 1 — Title bar: icon + title on left, optional download button on right */}
  <div className="flex items-center justify-between mb-1">
    <div className="flex items-center gap-2">
      <Icon className="size-5 shrink-0" style={{ color: COLORS.category }} />
      <span className="text-base font-semibold leading-none tracking-tight">Card Title</span>
    </div>
    {/* Optional global download button */}
    <Button variant="outline" size="icon" onClick={download} title="Download all as CSV">
      <DownloadIcon className="size-4" />
    </Button>
  </div>

  {/* Row 2 — Description (REQUIRED — keeps vertical rhythm consistent across cards) */}
  <p className="text-[11px] text-muted-foreground mb-3">Short description of the card's data</p>

  {/* Row 3 — Table or other content */}
  <Table style={{ fontFamily: FONT_SANS }}>...</Table>
</Card>
```

**Rules:**
- Title row always uses `justify-between` (even without a download button) for consistent alignment
- Title row always has `mb-1`
- Description is **mandatory** — it keeps table headers aligned across sibling cards in the same grid row
- Description always has `mb-3`
- Never skip the description or use different margin values — sibling cards will misalign

### DeltaBadge
Compact pill showing WoW or MoM change.

```
rounded-full px-2 py-0.5 text-xs font-medium tabular-nums
```

- Green tint for positive-is-good improvements
- Red tint for negative-is-good deterioration
- Uses up/down arrows, not icons

### ModuleHeader
Used for sub-modules (Drop-Ins, Intro Week, etc.) inside a section.

```
dot (2.5x2.5 rounded-full) + text-base font-semibold + optional summary pill
```

### Card Tab Switching (ToggleGroup + Select)

When a card has multiple views (e.g. "Complete weeks" vs "This week (WTD)"), use this canonical pattern. **Never** use `TabsList` / `TabsTrigger` with `variant="line"` — always use `ToggleGroup variant="outline"` with a `Select` mobile fallback.

```tsx
<Tabs value={activeTab} onValueChange={setActiveTab}>
  <DashboardCard className="@container/card">
    <CardHeader>
      <CardTitle>Card Title</CardTitle>
      <CardDescription>
        <span className="hidden @[540px]/card:block">Full description</span>
        <span className="@[540px]/card:hidden">Short description</span>
      </CardDescription>
      <CardAction>
        <ToggleGroup
          variant="outline"
          type="single"
          value={activeTab}
          onValueChange={(v) => { if (v) setActiveTab(v); }}
          className="hidden @[540px]/card:flex"
        >
          <ToggleGroupItem value="complete">Complete weeks ({count})</ToggleGroupItem>
          {wtd && <ToggleGroupItem value="wtd">This week (WTD)</ToggleGroupItem>}
        </ToggleGroup>
        <Select value={activeTab} onValueChange={setActiveTab}>
          <SelectTrigger className="w-44 @[540px]/card:hidden" size="sm">
            <SelectValue />
          </SelectTrigger>
          <SelectContent className="rounded-xl">
            <SelectItem value="complete" className="rounded-lg">Complete weeks ({count})</SelectItem>
            {wtd && <SelectItem value="wtd" className="rounded-lg">This week (WTD)</SelectItem>}
          </SelectContent>
        </Select>
      </CardAction>
    </CardHeader>
    <CardContent>
      <TabsContent value="complete">...</TabsContent>
      <TabsContent value="wtd">...</TabsContent>
    </CardContent>
  </DashboardCard>
</Tabs>
```

**Rules:**
- `<Tabs>` wraps the entire `<DashboardCard>` (not nested inside CardContent)
- `@container/card` on the DashboardCard enables container queries
- `ToggleGroup` is hidden below `@[540px]/card` breakpoint; `Select` is shown instead
- `ToggleGroupItem` labels should include counts where relevant (e.g. "Complete (5)")
- Use `Tabs` / `TabsContent` only for content switching — never render `TabsList` or `TabsTrigger`

---

## Charts (Recharts + shadcn/ui)

All charts use shadcn/ui `ChartContainer` with `ChartConfig`. Every chart MUST follow these rules.

### Canonical Bar Chart Pattern

This is the reference implementation. All bar charts should follow this structure:

```tsx
<ChartContainer config={chartConfig}>
  <BarChart
    accessibilityLayer
    data={chartData}
    margin={{ top: 20 }}
  >
    <CartesianGrid vertical={false} />
    <XAxis
      dataKey="month"
      tickLine={false}
      tickMargin={10}
      axisLine={false}
    />
    <ChartTooltip
      cursor={false}
      content={<ChartTooltipContent hideLabel />}
    />
    <Bar dataKey="value" fill="var(--color-value)" radius={8}>
      <LabelList
        position="top"
        offset={12}
        className="fill-foreground"
        fontSize={12}
      />
    </Bar>
  </BarChart>
</ChartContainer>
```

### Canonical Area Chart Pattern

```tsx
<ChartContainer config={chartConfig}>
  <RAreaChart accessibilityLayer data={chartData} margin={{ top: 20, left: 12, right: 12 }}>
    <defs>
      <linearGradient id="fillSeries" x1="0" y1="0" x2="0" y2="1">
        <stop offset="5%" stopColor="var(--color-series)" stopOpacity={0.8} />
        <stop offset="95%" stopColor="var(--color-series)" stopOpacity={0.1} />
      </linearGradient>
    </defs>
    <CartesianGrid vertical={false} />
    <XAxis dataKey="date" tickLine={false} axisLine={false} tickMargin={8} />
    <Area dataKey="series" type="natural" fill="url(#fillSeries)" stroke="var(--color-series)">
      <LabelList position="top" offset={12} className="fill-foreground" fontSize={12} />
    </Area>
    <ChartLegend content={<ChartLegendContent />} />
  </RAreaChart>
</ChartContainer>
```

### Canonical Line Chart Pattern

```tsx
<ChartContainer config={chartConfig}>
  <LineChart accessibilityLayer data={chartData} margin={{ top: 10, right: 10, left: 10, bottom: 5 }}>
    <CartesianGrid vertical={false} />
    <XAxis dataKey="month" tickLine={false} axisLine={false} tickMargin={8} />
    <YAxis tickLine={false} axisLine={false} tickFormatter={(v) => `${v}%`} />
    <ChartTooltip content={<ChartTooltipContent />} />
    <ChartLegend content={<ChartLegendContent />} />
    <Line type="monotone" dataKey="value" stroke="var(--color-value)" strokeWidth={2} dot={false} />
  </LineChart>
</ChartContainer>
```

### Mandatory Chart Elements

Every chart MUST include:

| Element | Rule |
|---------|------|
| `CartesianGrid` | Always present. Always `vertical={false}` (solid horizontal lines only, no strokeDasharray) |
| `XAxis` | Always `tickLine={false}` `axisLine={false}` `tickMargin={8-10}` |
| `LabelList` | On every data series (Bar or Area). Hidden on mobile: `{!isMobile && <LabelList ... />}` |
| `ChartTooltip` | Always present. Use `cursor={false}` for bar charts |
| `ChartLegend` | Required when chart has 2+ data series |
| `accessibilityLayer` | Always on the root chart component |

### LabelList Standard

Every `Bar` and `Area` MUST have a `LabelList`:

```tsx
<LabelList
  position="top"
  offset={12}
  className="fill-foreground"
  fontSize={12}
/>
```

- Hidden on mobile: wrap in `{!isMobile && ( ... )}`
- For currency values, add `formatter={(v: number) => formatCompactCurrency(v)}`

### Y-Axis Rules

**For percentage charts (churn, conversion rates):**
- Always start at 0
- Round max up to nearest multiple of 5: `Math.ceil(dataMax / 5) * 5`
- No extra padding multiplier
- Step size: 5 when max <= 15, otherwise 10
- Format: `tickFormatter={(v) => \`${v}%\`}`
- No axis line, no tick line

**For count/currency charts:**
- Hide Y-axis entirely (`<YAxis hide />`) — data labels on bars/areas make it redundant
- Or show with clean formatting if needed

### Chart Heights

| Chart Type | Height | Notes |
|-----------|--------|-------|
| Standard (area, line, bar) | `h-[250px]` | Default for most charts |
| Churn trend | `h-[300px]` | Taller for 3-line comparison |
| Compact (annual summary) | `h-[220px]` | Side-by-side layout |
| Sparkline (inline) | `h-[132px]` | Inside CategoryDetail cards |
| Drop-ins bar | `h-[200px]` | Simple single-series |

### Chart Margins

| Scenario | Margin |
|----------|--------|
| Bar chart with top labels | `{{ top: 20 }}` |
| Bar chart with formatted labels (e.g. "$2.2M") | `{{ top: 32 }}` |
| Area chart | `{{ top: 20, left: 12, right: 12 }}` |
| Line chart | `{{ top: 10, right: 10, left: 10, bottom: 5 }}` |

### Bar Radius
All bars use `radius={8}` for rounded corners.

### Colors in Charts
- Use `var(--color-{key})` from ChartConfig — never hardcode hex in chart elements
- Define colors in ChartConfig using `COLORS` constant values
- Gradient fills for areas: 5% opacity at top, 10% at bottom

---

## Layout Rules

### Card Grid
- Default: `grid gap-3 grid-cols-1 sm:grid-cols-2` for 2-up cards
- KPI hero: `grid gap-3 grid-cols-1 sm:grid-cols-3` for 3-up
- Always use `items-stretch` on the grid container so cards in the same row share equal height
- Use `matchHeight` prop on `<Card>` (applies `h-full`) to ensure cards stretch to fill the grid cell

### Section Spacing
- Between sections: handled by sidebar navigation (each section is its own view)
- Between sub-groups within a section: `gap-4` in flex column
- Between cards in a grid: `gap-3`

## Download Buttons

### Global Download (Card-Level)
A card-level "download all" button placed inline with the card title, to the right of the title text and any badges. Uses `variant="outline"` with `size="icon"`.

```tsx
<div className="flex items-center gap-2 mb-1">
  <Icon className="size-5 shrink-0" style={{ color: COLORS.category }} />
  <span className="text-base font-semibold leading-none tracking-tight">Card Title</span>
  <Badge variant="secondary" className="text-[10px] px-1.5 py-0 tabular-nums">{count}</Badge>
  <Button variant="outline" size="icon" onClick={downloadAll} title="Download all as CSV">
    <DownloadIcon className="size-4" />
  </Button>
</div>
```

**Rules:**
- Always `variant="outline"` `size="icon"` — never `variant="ghost"` for global downloads
- Placed **inline** with the title row (same `flex` container), not floated to the right
- Include a descriptive `title` attribute for hover tooltip

### Row-Level Download (Table Rows)
Per-row download icons inside tables. Uses `variant="ghost"` `size="icon"` for minimal visual weight.

```tsx
<TableHead className="w-10 px-0 text-center">
  <DownloadIcon className="size-3.5 text-muted-foreground inline-block" />
</TableHead>
{/* ... per row: */}
<TableCell className="py-1.5 px-0 text-center">
  <Button variant="ghost" size="icon" className="size-7 mx-auto" onClick={download} title="Download as CSV">
    <DownloadIcon className="size-3.5" />
  </Button>
</TableCell>
```

**Rules:**
- Column header: just the icon (`inline-block`), no text
- Cell: `variant="ghost"` `size="icon"` with `mx-auto` for centering
- Column width: `w-10 px-0`

---

## Anti-Patterns (Do Not Use)

- Charts without `<CartesianGrid vertical={false} />` — every chart needs solid horizontal grid lines (no strokeDasharray)
- Charts without `<LabelList>` on data series — every bar/area needs data labels
- Colored border-left lines on sub-group headers
- Pie charts for churn or time-series data
- Mixed weekly/monthly data in the same card
- Arbitrary Y-axis values (always round to clean multiples of 5)
- Colored dots instead of icons for category identification (use icons from icons.tsx)
- Hardcoded hex colors in chart elements (use `var(--color-*)` from ChartConfig)
- `TabsList` / `TabsTrigger` with `variant="line"` for card view switching — use `ToggleGroup variant="outline"` + `Select` mobile fallback instead
