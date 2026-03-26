"use client";

import React, { useState, useEffect, useCallback } from "react";
import {
  DashboardCard,
  CardHeader,
  CardTitle,
  CardDescription,
  CardContent,
} from "@/components/dashboard";
import {
  Table,
  TableHeader,
  TableBody,
  TableRow,
  TableHead,
  TableCell,
} from "@/components/ui/table";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  ReferenceLine,
} from "recharts";
import {
  ChartContainer,
  ChartTooltip,
  ChartTooltipContent,
  type ChartConfig,
} from "@/components/ui/chart";
import { ToggleGroup, ToggleGroupItem } from "@/components/ui/toggle-group";
import { DownloadIcon, ActivityIcon, ArrowBadgeDown, BrandSky, DeviceTv } from "@/components/dashboard/icons";
import { TrendingUp, TrendingDown, Minus, ArrowRight, X as XIcon, Copy, ChevronRight, ChevronDown } from "lucide-react";
import { SECTION_COLORS, type SectionKey } from "@/components/dashboard/sidebar-nav";
import type {
  UsageScorecardCard,
  UsageTrendSeries,
  UsageSegmentRow,
  UsageMovementData,
  UsageMemberRow,
  UsageAnnotation,
  Sky3TierRow,
  TvEngagementRow,
} from "@/types/dashboard";

// ─── Constants ──────────────────────────────────────────────

const FONT_SANS = "'Helvetica Neue', Helvetica, Arial, system-ui, -apple-system, 'Segoe UI', Roboto, sans-serif";

const TIER_DISPLAY_LABELS: Record<string, string> = {
  dormant: "Dormant", low: "Low", target: "Target", strong: "Strong", power_user: "Power User",
  unused_pack: "Unused Pack", save_candidate: "Save Candidate", building_habit: "Building Habit",
  upgrade_candidate: "Upgrade Candidate", ready_to_upgrade: "Ready to Upgrade",
  inactive: "Inactive", light: "Light", active: "Active", engaged: "Engaged",
};

const TIER_COLORS: Record<string, string> = {
  dormant: "#C0392B", low: "#E67E22", target: "#27AE60", strong: "#2ECC71", power_user: "#1ABC9C",
  unused_pack: "#C0392B", save_candidate: "#E67E22", building_habit: "#F1C40F",
  upgrade_candidate: "#27AE60", ready_to_upgrade: "#1ABC9C",
  inactive: "#C0392B", light: "#E67E22", active: "#27AE60", engaged: "#1ABC9C",
};

const DIRECTION_LABELS: Record<string, string> = { up: "\u2191", down: "\u2193", same: "\u2192" };
const DIRECTION_COLORS: Record<string, string> = { up: "#27AE60", down: "#C0392B", same: "#95A5A6" };

const DELTA_COLORS = { positive: "#27AE60", negative: "#C0392B", neutral: "#95A5A6" };

const SEGMENT_LINE_COLORS: Record<string, string> = {
  members: "#2C3E50",
  sky3: "#E67E22",
  tv: "#8E44AD",
};

const SEGMENT_LABELS: Record<string, string> = {
  members: "Members",
  sky3: "Sky3",
  tv: "Sky Ting TV",
};

const HEALTH_METRIC_LABELS: Record<string, string> = {
  members: "hitting target",
  sky3: "at full use",
  tv: "active (14d)",
};

const REVENUE_OPPORTUNITY_COPY: Record<string, { means: string; action: string }> = {
  unused_pack: { means: "Will probably cancel", action: "Win-back sequence" },
  save_candidate: { means: "At risk of canceling", action: "Engagement check-in" },
  building_habit: { means: "Getting value, could go either way", action: "Nurture" },
  upgrade_candidate: { means: "Maxed out \u2014 want more", action: "Offer membership" },
  ready_to_upgrade: { means: "Already buying extra", action: "Priority upgrade outreach" },
};

const TIER_DEFINITIONS: Record<string, string> = {
  dormant: "0 visits/mo", low: "1\u20132 visits/mo", target: "3\u20134 visits/mo",
  strong: "5\u20138 visits/mo", power_user: "9+ visits/mo",
  unused_pack: "0 visits/mo", save_candidate: "1 visit/mo", building_habit: "2 visits/mo",
  upgrade_candidate: "3 visits/mo", ready_to_upgrade: "4+ visits/mo",
  inactive: "0 sessions in 14 days", light: "1 session in 14 days",
  active: "2\u20133 sessions in 14 days", engaged: "4+ sessions in 14 days",
};

const ALERT_THRESHOLDS = {
  members_dormant_pct: 10,
  sky3_breakage_pct_critical: 50,
  sky3_breakage_pct_warning: 30,
  tv_inactive_pct: 30,
};

// ─── Formatting ─────────────────────────────────────────────

function formatPct(value: number): string {
  return `${value.toFixed(1)}%`;
}

function formatCount(value: number): string {
  return value.toLocaleString();
}

function formatSignedCount(value: number): string {
  return value >= 0 ? `+${value.toLocaleString()}` : `\u2212${Math.abs(value).toLocaleString()}`;
}

function formatDeltaStr(value: number, type: "pct" | "count", periodWeeks: number): string {
  const sign = value >= 0 ? "+" : "\u2212";
  const abs = Math.abs(value);
  const num = type === "pct" ? abs.toFixed(1) : abs.toLocaleString();
  return `${sign}${num} vs. prior ${periodWeeks}wk`;
}

function formatValue(value: number, format: string): string {
  if (format === "pct") return formatPct(value);
  if (format === "signed_count") return formatSignedCount(value);
  if (format === "decimal") return value.toFixed(1);
  return formatCount(value);
}

function deltaColor(value: number, type: "pct" | "count", invert: boolean): string {
  const threshold = type === "pct" ? 0.5 : 3;
  if (Math.abs(value) < threshold) return DELTA_COLORS.neutral;
  const isPositive = invert ? value < 0 : value > 0;
  return isPositive ? DELTA_COLORS.positive : DELTA_COLORS.negative;
}

function formatWeekLabel(dateStr: string): string {
  const d = new Date(dateStr + "T00:00:00Z");
  return d.toLocaleDateString("en-US", { month: "short", day: "numeric", timeZone: "UTC" });
}

// ─── Sparkline Component ────────────────────────────────────

function Sparkline({ data, color, width = 64, height = 24 }: { data: number[]; color: string; width?: number; height?: number }) {
  if (!data || data.length < 2) return null;
  const min = Math.min(...data);
  const max = Math.max(...data);
  const range = max - min || 1;
  const points = data.map((v, i) => {
    const x = (i / (data.length - 1)) * width;
    const y = height - ((v - min) / range) * height;
    return `${x},${y}`;
  }).join(" ");

  return (
    <svg width={width} height={height} className="inline-block">
      <polyline points={points} fill="none" stroke={color} strokeWidth={1.5} />
    </svg>
  );
}

// ─── Tier Badge Component ───────────────────────────────────

function TierBadge({ tier, muted = false }: { tier: string; muted?: boolean }) {
  const color = TIER_COLORS[tier] || "#95A5A6";
  const label = TIER_DISPLAY_LABELS[tier] || tier;
  const definition = TIER_DEFINITIONS[tier];
  return (
    <span
      className="inline-flex items-center rounded-md px-2 py-0.5 text-xs font-medium"
      style={{
        backgroundColor: `${color}18`,
        color,
        opacity: muted ? 0.5 : 1,
        border: `1px solid ${color}30`,
      }}
      title={definition}
    >
      {label}
    </span>
  );
}

// ─── Status Icon (replaces sparklines on scorecard cards) ───

function StatusIcon({ delta, deltaType, invert, size = 18 }: { delta: number; deltaType: "pct" | "count"; invert: boolean; size?: number }) {
  const threshold = deltaType === "pct" ? 0.5 : 3;
  if (Math.abs(delta) < threshold) return <Minus size={size} color="#95A5A6" />;
  const isPositive = invert ? delta < 0 : delta > 0;
  if (isPositive) return <TrendingUp size={size} color="#27AE60" />;
  return <TrendingDown size={size} color="#C0392B" />;
}

// ─── Time Window Control ────────────────────────────────────

function TimeWindowControl({ value, onChange }: { value: number; onChange: (v: number) => void }) {
  return (
    <ToggleGroup
      type="single"
      value={String(value)}
      onValueChange={(v) => v && onChange(Number(v))}
      className="gap-0 rounded-md border"
    >
      {[4, 8, 12].map(w => (
        <ToggleGroupItem key={w} value={String(w)} className="px-3 py-1 text-xs rounded-none first:rounded-l-md last:rounded-r-md">
          {w}wk
        </ToggleGroupItem>
      ))}
    </ToggleGroup>
  );
}

// ─── Scorecard Card ─────────────────────────────────────────

function ScorecardCard({ card, periodWeeks }: { card: UsageScorecardCard; periodWeeks: number }) {
  const color = deltaColor(card.delta, card.deltaType, card.invertDirection);
  return (
    <DashboardCard>
      <CardContent className="p-3">
        <div className="flex items-center justify-between mb-1">
          <span className="text-xs text-muted-foreground uppercase tracking-wide">{card.label}</span>
          <StatusIcon delta={card.delta} deltaType={card.deltaType} invert={card.invertDirection} size={18} />
        </div>
        <div className="text-2xl font-semibold tabular-nums tracking-tight">
          {formatValue(card.value, card.format)}
        </div>
        <div className="text-xs tabular-nums mt-0.5" style={{ color }}>
          {formatDeltaStr(card.delta, card.deltaType, periodWeeks)}
        </div>
      </CardContent>
    </DashboardCard>
  );
}

// ─── Alert Banner ───────────────────────────────────────────

function AlertBanner({ segment, tierData }: {
  segment: string;
  tierData: Sky3TierRow[] | TvEngagementRow[] | null;
}) {
  if (!tierData || !Array.isArray(tierData) || tierData.length === 0) return null;

  let text = "";
  let bgColor = "";
  let borderColor = "";

  if (segment === "sky3") {
    const total = (tierData as Sky3TierRow[]).reduce((s, t) => s + t.count, 0);
    const breakageTiers = ["unused_pack", "save_candidate"];
    const breakageCount = (tierData as Sky3TierRow[]).filter(t => breakageTiers.includes(t.tier)).reduce((s, t) => s + t.count, 0);
    const breakagePct = total > 0 ? Math.round((breakageCount / total) * 1000) / 10 : 0;
    const saveCandCount = (tierData as Sky3TierRow[]).find(t => t.tier === "save_candidate")?.count ?? 0;

    if (breakagePct > ALERT_THRESHOLDS.sky3_breakage_pct_critical) {
      text = `${breakageCount} of ${total} Sky3 members are at 0\u20131 visits. ${breakageCount} members are at risk of canceling.`;
      bgColor = "#FDF2F2"; borderColor = "#C0392B";
    } else if (breakagePct > ALERT_THRESHOLDS.sky3_breakage_pct_warning) {
      text = `${breakagePct}% of Sky3 members are underusing their pack. ${saveCandCount} are Save Candidates.`;
      bgColor = "#FFF8E7"; borderColor = "#E67E22";
    }
  } else if (segment === "tv") {
    const total = (tierData as TvEngagementRow[]).reduce((s, t) => s + t.count, 0);
    const inactiveCount = (tierData as TvEngagementRow[]).find(t => t.tier === "inactive")?.count ?? 0;
    const inactivePct = total > 0 ? Math.round((inactiveCount / total) * 1000) / 10 : 0;

    if (inactivePct > ALERT_THRESHOLDS.tv_inactive_pct) {
      text = `${inactiveCount.toLocaleString()} subscribers haven't watched anything in 14 days. That's ${inactivePct}% of all TV subscribers.`;
      bgColor = "#FDF2F2"; borderColor = "#C0392B";
    }
  }

  if (!text) return null;

  return (
    <div style={{ padding: "16px 20px", borderLeft: `4px solid ${borderColor}`, backgroundColor: bgColor, borderRadius: 4, marginBottom: 4 }}>
      <span style={{ color: borderColor, marginRight: 8 }}>{"\u26a0"}</span>
      <span className="text-sm">{text}</span>
    </div>
  );
}

// ─── Scorecard Row ──────────────────────────────────────────

function UsageScorecard({ cards, periodWeeks }: { cards: UsageScorecardCard[]; periodWeeks: number }) {
  return (
    <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-5 gap-3">
      {cards.map(card => (
        <ScorecardCard key={card.key} card={card} periodWeeks={periodWeeks} />
      ))}
    </div>
  );
}

// ─── Trend Line Chart ───────────────────────────────────────

function UsageTrendLine({ series, annotations }: { series: UsageTrendSeries[]; annotations: UsageAnnotation[] }) {
  if (!series.length || !series[0].data.length) {
    return <DashboardCard><CardContent className="p-4 text-sm text-muted-foreground">No trend data available yet. Data will appear after the first pipeline run.</CardContent></DashboardCard>;
  }

  // Build merged data array for Recharts
  const allWeeks = new Set<string>();
  for (const s of series) {
    for (const d of s.data) allWeeks.add(d.week);
  }
  const weeks = Array.from(allWeeks).sort();

  const chartData = weeks.map(week => {
    const point: Record<string, string | number> = { week, label: formatWeekLabel(week) };
    for (const s of series) {
      const d = s.data.find(d => d.week === week);
      point[s.segment] = d?.value ?? 0;
    }
    // Check for annotation
    const ann = annotations.find(a => a.week === week);
    if (ann) point.annotation = ann.label;
    return point;
  });

  // Compute baseline (avg of first 4 data points of first series)
  const firstSeries = series[0]?.data ?? [];
  const baselineValues = firstSeries.slice(0, 4).map(d => d.value);
  const baseline = baselineValues.length > 0
    ? Math.round(baselineValues.reduce((a, b) => a + b, 0) / baselineValues.length * 10) / 10
    : 0;

  const chartConfig: ChartConfig = {};
  for (const s of series) {
    chartConfig[s.segment] = { label: SEGMENT_LABELS[s.segment] || s.segment, color: SEGMENT_LINE_COLORS[s.segment] || "#888" };
  }

  return (
    <DashboardCard>
      <CardHeader>
        <CardTitle className="text-base font-semibold">% Hitting Target</CardTitle>
        <CardDescription>12-week trend by segment</CardDescription>
      </CardHeader>
      <CardContent>
        <ChartContainer config={chartConfig} className="h-[320px] w-full">
          <LineChart data={chartData} margin={{ top: 10, right: 20, bottom: 30, left: 40 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="var(--st-border)" />
            <XAxis
              dataKey="label"
              tick={{ fontSize: 11, fill: "var(--st-text-secondary)" }}
              tickLine={false}
              axisLine={false}
            />
            <YAxis
              tick={{ fontSize: 11, fill: "var(--st-text-secondary)" }}
              tickLine={false}
              axisLine={false}
              tickFormatter={(v: number) => `${v}%`}
              domain={["auto", "auto"]}
            />
            {baseline > 0 && (
              <ReferenceLine y={baseline} stroke="#BDC3C7" strokeDasharray="6 4" label={{ value: "3-Mo Baseline", position: "right", fontSize: 10, fill: "#BDC3C7" }} />
            )}
            <ChartTooltip content={<ChartTooltipContent />} />
            {series.map(s => (
              <Line
                key={s.segment}
                type="monotone"
                dataKey={s.segment}
                stroke={SEGMENT_LINE_COLORS[s.segment] || "#888"}
                strokeWidth={2}
                dot={false}
                activeDot={{ r: 4 }}
              />
            ))}
          </LineChart>
        </ChartContainer>
      </CardContent>
    </DashboardCard>
  );
}

// ─── Segment Comparison Table ───────────────────────────────

function UsageSegmentTable({
  segments,
  onNavigate,
}: {
  segments: UsageSegmentRow[];
  onNavigate: (section: SectionKey) => void;
}) {
  const segmentToSection: Record<string, SectionKey> = {
    members: "usage-members",
    sky3: "usage-sky3",
    tv: "usage-tv",
  };

  return (
    <DashboardCard>
      <CardContent className="p-0">
        <Table style={{ fontFamily: FONT_SANS }}>
          <TableHeader>
            <TableRow>
              <TableHead className="w-[120px] text-xs">Segment</TableHead>
              <TableHead className="w-[100px] text-right text-xs">Subscribed</TableHead>
              <TableHead className="w-[160px] text-right text-xs">Health Metric</TableHead>
              <TableHead className="w-[120px] text-right text-xs">&Delta; vs. Prior 4wk</TableHead>
              <TableHead className="w-[120px] text-right text-xs">Net Movement</TableHead>
              <TableHead className="w-[60px] text-center text-xs"></TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {segments.map(seg => {
              const dColor = deltaColor(seg.delta, "pct", false);
              const mColor = deltaColor(seg.netMovement, "count", false);
              return (
                <TableRow
                  key={seg.segment}
                  className="cursor-pointer hover:bg-muted/50"
                  onClick={() => onNavigate(segmentToSection[seg.segment])}
                >
                  <TableCell className="py-2 font-medium text-sm">{SEGMENT_LABELS[seg.segment]}</TableCell>
                  <TableCell className="py-2 text-right tabular-nums text-sm">{formatCount(seg.subscribed)}</TableCell>
                  <TableCell className="py-2 text-right tabular-nums text-sm">
                    {formatPct(seg.healthMetric)} {HEALTH_METRIC_LABELS[seg.segment]}
                  </TableCell>
                  <TableCell className="py-2 text-right tabular-nums text-sm" style={{ color: dColor }}>
                    {seg.delta >= 0 ? "+" : "\u2212"}{Math.abs(seg.delta).toFixed(1)}
                  </TableCell>
                  <TableCell className="py-2 text-right tabular-nums text-sm" style={{ color: mColor }}>
                    {formatSignedCount(seg.netMovement)}
                  </TableCell>
                  <TableCell className="py-2 text-center text-xs text-muted-foreground">Detail &rarr;</TableCell>
                </TableRow>
              );
            })}
          </TableBody>
        </Table>
      </CardContent>
    </DashboardCard>
  );
}

// ─── Movement Bar ───────────────────────────────────────────

function UsageMovementBar({ data }: { data: UsageMovementData }) {
  const total = data.movedUp + data.stayed + data.slippedDown;
  if (total === 0) {
    return (
      <DashboardCard>
        <CardContent className="p-4 text-sm text-muted-foreground">
          No movement data available yet.
        </CardContent>
      </DashboardCard>
    );
  }

  const minPx = 40;
  const getPct = (n: number) => Math.max((n / total) * 100, (minPx / 800) * 100);
  const upPct = getPct(data.movedUp);
  const downPct = getPct(data.slippedDown);
  const stayedPct = 100 - upPct - downPct;

  return (
    <DashboardCard>
      <CardContent className="p-4">
        <div className="flex h-12 rounded-md overflow-hidden" style={{ fontFamily: FONT_SANS }}>
          {data.movedUp > 0 && (
            <div
              className="flex items-center justify-center text-white text-xs font-medium"
              style={{ width: `${upPct}%`, backgroundColor: "#27AE60" }}
              title={`${data.movedUp} Moved Up`}
            >
              {upPct > 8 && `\u2191 ${data.movedUp} Moved Up`}
            </div>
          )}
          <div
            className="flex items-center justify-center text-xs font-medium"
            style={{ width: `${stayedPct}%`, backgroundColor: "#BDC3C7", color: "#555" }}
            title={`${data.stayed} Stayed`}
          >
            {stayedPct > 8 && `\u2192 ${data.stayed} Stayed`}
          </div>
          {data.slippedDown > 0 && (
            <div
              className="flex items-center justify-center text-white text-xs font-medium"
              style={{ width: `${downPct}%`, backgroundColor: "#C0392B" }}
              title={`${data.slippedDown} Slipped Down`}
            >
              {downPct > 8 && `\u2193 ${data.slippedDown} Slipped Down`}
            </div>
          )}
        </div>
        <p className="text-sm text-muted-foreground mt-2">
          This period, {data.movedUp} members moved to a higher usage tier, {data.slippedDown} slipped down, {data.stayed} stayed put.
        </p>
      </CardContent>
    </DashboardCard>
  );
}

// ─── Filter Bar ─────────────────────────────────────────────

type ActionFilter = "at_risk" | "newly_on_target" | "dormant" | "improving" | null;

function UsageFilterBar({
  activeFilter,
  onFilterChange,
  segment,
  periodWeeks,
}: {
  activeFilter: ActionFilter;
  onFilterChange: (f: ActionFilter) => void;
  segment: string;
  periodWeeks: number;
}) {
  const filters: { key: ActionFilter; label: string }[] = [
    { key: "at_risk", label: "At Risk" },
    { key: "newly_on_target", label: "Newly On Target" },
    { key: "dormant", label: "Dormant" },
    { key: "improving", label: "Improving" },
    { key: null, label: "All" },
  ];

  const handleExport = () => {
    const params = new URLSearchParams({ segment });
    if (activeFilter) params.set("filter", activeFilter);
    params.set("period_weeks", String(periodWeeks));
    window.location.href = `/api/usage/members/export?${params}`;
  };

  return (
    <div className="flex items-center justify-between flex-wrap gap-2">
      <div className="flex gap-1">
        {filters.map(f => (
          <Button
            key={f.key ?? "all"}
            variant={activeFilter === f.key ? "default" : "outline"}
            size="sm"
            className="text-xs"
            onClick={() => onFilterChange(f.key)}
          >
            {f.label}
          </Button>
        ))}
      </div>
      <Button variant="outline" size="sm" className="text-xs gap-1" onClick={handleExport}>
        <DownloadIcon className="size-3.5" />
        Export CSV
      </Button>
    </div>
  );
}

// ─── Action Table ───────────────────────────────────────────

function UsageActionTable({
  members,
  total,
  page,
  perPage,
  onPageChange,
}: {
  members: UsageMemberRow[];
  total: number;
  page: number;
  perPage: number;
  onPageChange: (p: number) => void;
}) {
  const totalPages = Math.ceil(total / perPage);

  if (members.length === 0) {
    return (
      <DashboardCard>
        <CardContent className="p-4 text-sm text-muted-foreground">
          No members match this filter for the selected period.
        </CardContent>
      </DashboardCard>
    );
  }

  return (
    <DashboardCard>
      <CardContent className="p-0">
        <Table style={{ fontFamily: FONT_SANS }}>
          <TableHeader>
            <TableRow>
              <TableHead className="w-[180px] text-xs">Name</TableHead>
              <TableHead className="w-[200px] text-xs">Email</TableHead>
              <TableHead className="w-[130px] text-xs">Current Tier</TableHead>
              <TableHead className="w-[130px] text-xs">Previous Tier</TableHead>
              <TableHead className="w-[60px] text-center text-xs">Dir</TableHead>
              <TableHead className="w-[90px] text-right text-xs">Visits (Now)</TableHead>
              <TableHead className="w-[100px] text-right text-xs">Visits (Before)</TableHead>
              <TableHead className="w-[80px] text-xs">Trend</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {members.map((m, i) => {
              // Sparkline color: compare first half avg to second half avg
              const sp = m.sparkline ?? [];
              let spColor = DELTA_COLORS.neutral;
              if (sp.length >= 4) {
                const mid = Math.floor(sp.length / 2);
                const firstHalf = sp.slice(0, mid).reduce((a, b) => a + b, 0) / mid;
                const secondHalf = sp.slice(mid).reduce((a, b) => a + b, 0) / (sp.length - mid);
                if (secondHalf > firstHalf + 0.5) spColor = DELTA_COLORS.positive;
                else if (secondHalf < firstHalf - 0.5) spColor = DELTA_COLORS.negative;
              }
              return (
              <TableRow key={`${m.email}-${i}`}>
                <TableCell className="py-1.5 text-sm truncate max-w-[180px]">{m.name}</TableCell>
                <TableCell className="py-1.5 text-sm text-muted-foreground truncate max-w-[200px]">{m.email}</TableCell>
                <TableCell className="py-1.5"><TierBadge tier={m.currentTier} /></TableCell>
                <TableCell className="py-1.5"><TierBadge tier={m.priorTier} muted /></TableCell>
                <TableCell className="py-1.5 text-center text-sm font-medium" style={{ color: DIRECTION_COLORS[m.direction] }}>
                  {DIRECTION_LABELS[m.direction]}
                </TableCell>
                <TableCell className="py-1.5 text-right tabular-nums text-sm">{m.currentVisits}</TableCell>
                <TableCell className="py-1.5 text-right tabular-nums text-sm text-muted-foreground">{m.priorVisits}</TableCell>
                <TableCell className="py-1.5">{sp.length > 1 && <Sparkline data={sp} color={spColor} width={64} height={20} />}</TableCell>
              </TableRow>
              );
            })}
          </TableBody>
        </Table>
        {totalPages > 1 && (
          <div className="flex items-center justify-between px-4 py-2 border-t text-xs text-muted-foreground">
            <span>{total} total members</span>
            <div className="flex gap-1">
              <Button variant="outline" size="sm" disabled={page <= 1} onClick={() => onPageChange(page - 1)} className="text-xs h-7 px-2">Prev</Button>
              <span className="flex items-center px-2">Page {page} of {totalPages}</span>
              <Button variant="outline" size="sm" disabled={page >= totalPages} onClick={() => onPageChange(page + 1)} className="text-xs h-7 px-2">Next</Button>
            </div>
          </div>
        )}
      </CardContent>
    </DashboardCard>
  );
}

// ─── Sky3 Revenue Opportunity Table ─────────────────────────

function Sky3RevenueOpportunityTable({ tiers }: { tiers: Sky3TierRow[] }) {
  if (!tiers.length) return null;

  const maxCount = Math.max(...tiers.map(t => t.count), 1);

  return (
    <DashboardCard>
      <CardHeader>
        <CardTitle className="text-base font-semibold">Revenue Opportunity</CardTitle>
        <CardDescription>Sky3 members by usage tier</CardDescription>
      </CardHeader>
      <CardContent className="p-0">
        <Table style={{ fontFamily: FONT_SANS, tableLayout: "fixed", width: "100%" }}>
          <TableHeader>
            <TableRow>
              <TableHead className="text-xs" style={{ width: 240 }}>Tier</TableHead>
              <TableHead className="text-right text-xs" style={{ width: 80 }}>Count</TableHead>
              <TableHead className="text-right text-xs" style={{ width: 60 }}>%</TableHead>
              <TableHead className="text-xs">What This Means</TableHead>
              <TableHead className="text-xs" style={{ width: 200 }}>What To Do</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {tiers.map(row => {
              const copy = REVENUE_OPPORTUNITY_COPY[row.tier];
              const color = TIER_COLORS[row.tier] || "#95A5A6";
              return (
                <TableRow key={row.tier} style={{ borderLeft: `4px solid ${color}` }}>
                  <TableCell className="py-2 text-sm">
                    <span className="font-medium">{TIER_DISPLAY_LABELS[row.tier] || row.tier}</span>
                    <span style={{ color: "#95A5A6", fontWeight: "normal", marginLeft: 6 }}>({TIER_DEFINITIONS[row.tier]})</span>
                  </TableCell>
                  <TableCell className="py-2 text-right tabular-nums text-sm" style={{ position: "relative" }}>
                    <div style={{ position: "absolute", left: 0, top: 0, bottom: 0, width: `${(row.count / maxCount) * 100}%`, backgroundColor: `${color}20`, borderRadius: 4, zIndex: 0 }} />
                    <span style={{ position: "relative", zIndex: 1 }}>{row.count}</span>
                  </TableCell>
                  <TableCell className="py-2 text-right tabular-nums text-sm text-muted-foreground">{row.pct}%</TableCell>
                  <TableCell className="py-2 text-sm text-muted-foreground">{copy?.means ?? ""}</TableCell>
                  <TableCell className="py-2 text-sm font-medium">{copy?.action ?? ""}</TableCell>
                </TableRow>
              );
            })}
          </TableBody>
        </Table>
      </CardContent>
    </DashboardCard>
  );
}

// ─── TV Engagement Bars ─────────────────────────────────────

function TvEngagementBars({ tiers }: { tiers: TvEngagementRow[] }) {
  if (!tiers.length) return null;

  const maxCount = Math.max(...tiers.map(t => Math.max(t.count, t.priorCount)), 1);

  return (
    <DashboardCard>
      <CardHeader>
        <CardTitle className="text-base font-semibold">Engagement Distribution</CardTitle>
        <CardDescription>Current vs. prior period</CardDescription>
      </CardHeader>
      <CardContent>
        <div className="space-y-3">
          {tiers.map(row => {
            const color = TIER_COLORS[row.tier] || "#95A5A6";
            const currentPct = (row.count / maxCount) * 100;
            const priorPct = (row.priorCount / maxCount) * 100;
            return (
              <div key={row.tier}>
                <div className="flex items-center justify-between mb-1">
                  <span className="text-sm font-medium">{TIER_DISPLAY_LABELS[row.tier]}</span>
                  <span className="text-xs tabular-nums text-muted-foreground">
                    {row.count} ({row.pct}%) {row.priorCount !== row.count && <span className="ml-1">was {row.priorCount}</span>}
                  </span>
                </div>
                <div className="relative h-6 rounded-sm bg-muted/20">
                  {/* Ghost bar (prior period) */}
                  <div
                    className="absolute top-0 left-0 h-full rounded-sm"
                    style={{ width: `${Math.max(priorPct, 1)}%`, backgroundColor: color, opacity: 0.15 }}
                  />
                  {/* Current bar */}
                  <div
                    className="absolute top-0 left-0 h-full rounded-sm transition-all"
                    style={{ width: `${Math.max(currentPct, 1)}%`, backgroundColor: color }}
                  />
                </div>
              </div>
            );
          })}
        </div>
        {/* Flow summary */}
        {(() => {
          const becameActive = tiers.filter(t => t.tier !== "inactive").reduce((sum, t) => sum + Math.max(t.count - t.priorCount, 0), 0);
          const becameInactive = Math.max((tiers.find(t => t.tier === "inactive")?.count ?? 0) - (tiers.find(t => t.tier === "inactive")?.priorCount ?? 0), 0);
          const net = becameActive - becameInactive;
          return (
            <p className="text-sm text-muted-foreground mt-3">
              This period, {becameActive} subscribers became more active, {becameInactive} became inactive. Net: {formatSignedCount(net)}.
            </p>
          );
        })()}
      </CardContent>
    </DashboardCard>
  );
}

// ─── Data Fetching Hook ─────────────────────────────────────

function useUsageData<T>(url: string, deps: unknown[]): { data: T | null; loading: boolean } {
  const [data, setData] = useState<T | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    fetch(url)
      .then(r => r.json())
      .then(d => { if (!cancelled) setData(d); })
      .catch(err => console.error(`[usage] Fetch error: ${url}`, err))
      .finally(() => { if (!cancelled) setLoading(false); });
    return () => { cancelled = true; };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, deps);

  return { data, loading };
}

// ─── Overview Page ──────────────────────────────────────────

export function UsageOverviewPage({ onNavigate }: { onNavigate: (section: SectionKey) => void }) {
  const [periodWeeks, setPeriodWeeks] = useState(4);

  const { data: scorecardData } = useUsageData<{ cards: UsageScorecardCard[] }>(
    `/api/usage/scorecard?period_weeks=${periodWeeks}`,
    [periodWeeks]
  );
  const { data: trendData } = useUsageData<{ series: UsageTrendSeries[] }>(
    `/api/usage/trend?weeks=12`,
    []
  );
  const { data: segmentsData } = useUsageData<{ segments: UsageSegmentRow[] }>(
    `/api/usage/segments?period_weeks=${periodWeeks}`,
    [periodWeeks]
  );
  const { data: movementData } = useUsageData<UsageMovementData>(
    `/api/usage/movement?period_weeks=${periodWeeks}`,
    [periodWeeks]
  );
  const { data: annotationsData } = useUsageData<{ annotations: UsageAnnotation[] }>(
    `/api/usage/annotations?weeks=12`,
    []
  );

  return (
    <div className="flex flex-col gap-3">
      <div className="flex items-center justify-between mb-1">
        <div className="flex items-center gap-3">
          <ActivityIcon className="size-7 shrink-0" style={{ color: SECTION_COLORS["usage-overview"] }} />
          <h1 className="text-3xl font-semibold tracking-tight">Usage Overview</h1>
        </div>
        <TimeWindowControl value={periodWeeks} onChange={setPeriodWeeks} />
      </div>
      <p className="text-sm text-muted-foreground ml-10 -mt-2">Are we moving more members toward 3 visits per month?</p>

      {/* Scorecard */}
      {scorecardData?.cards && (
        <UsageScorecard cards={scorecardData.cards} periodWeeks={periodWeeks} />
      )}

      {/* Trend Line */}
      <UsageTrendLine
        series={trendData?.series ?? []}
        annotations={annotationsData?.annotations ?? []}
      />

      {/* Segment Comparison Table */}
      {segmentsData?.segments && (
        <UsageSegmentTable segments={segmentsData.segments} onNavigate={onNavigate} />
      )}

      {/* Movement Bar */}
      {movementData && <UsageMovementBar data={movementData} />}
    </div>
  );
}

// ─── Members Detail Page ────────────────────────────────────

export function UsageMembersPage() {
  const [periodWeeks, setPeriodWeeks] = useState(4);
  const [filter, setFilter] = useState<ActionFilter>(null);
  const [page, setPage] = useState(1);
  const perPage = 25;

  // Reset page when filter changes
  useEffect(() => setPage(1), [filter]);

  const { data: scorecardData } = useUsageData<{ cards: UsageScorecardCard[] }>(
    `/api/usage/scorecard?period_weeks=${periodWeeks}&segment=members`,
    [periodWeeks]
  );

  const filterParam = filter ? `&filter=${filter}` : "";
  const { data: membersData } = useUsageData<{ members: UsageMemberRow[]; total: number; page: number }>(
    `/api/usage/members?segment=members&period_weeks=${periodWeeks}&page=${page}&per_page=${perPage}${filterParam}`,
    [periodWeeks, filter, page]
  );

  return (
    <div className="flex flex-col gap-3">
      <div className="flex items-center justify-between mb-1">
        <div className="flex items-center gap-3">
          <ArrowBadgeDown className="size-7 shrink-0" style={{ color: SECTION_COLORS["usage-members"] }} />
          <h1 className="text-3xl font-semibold tracking-tight">Members</h1>
        </div>
        <TimeWindowControl value={periodWeeks} onChange={setPeriodWeeks} />
      </div>
      {(() => {
        if (scorecardData?.cards) {
          const dormant = scorecardData.cards.find(c => c.key === "dormant_count")?.value ?? 0;
          const netMov = scorecardData.cards.find(c => c.key === "net_movement")?.value ?? 0;
          const atRisk = netMov < 0 ? Math.abs(netMov) : 0;
          const improving = netMov > 0 ? netMov : 0;
          return <p className="text-sm ml-10 -mt-2" style={{ color: "#7F8C8D" }}>{atRisk} at risk — {improving} improving — {dormant} dormant</p>;
        }
        return <p className="text-sm text-muted-foreground ml-10 -mt-2">Unlimited membership usage deep dive</p>;
      })()}

      {(() => {
        // Members alert banner: check dormant %
        if (scorecardData?.cards) {
          const dormant = scorecardData.cards.find(c => c.key === "dormant_count")?.value ?? 0;
          const total = scorecardData.cards.find(c => c.key === "total_subscribed")?.value ?? 1;
          const dormantPct = Math.round((dormant / total) * 1000) / 10;
          if (dormantPct > ALERT_THRESHOLDS.members_dormant_pct) {
            return (
              <div style={{ padding: "16px 20px", borderLeft: "4px solid #C0392B", backgroundColor: "#FDF2F2", borderRadius: 4, marginBottom: 4 }}>
                <span style={{ color: "#C0392B", marginRight: 8 }}>{"\u26a0"}</span>
                <span className="text-sm">{dormant} members are subscribed but haven't visited. That's {dormantPct}% of all Members.</span>
              </div>
            );
          }
        }
        return null;
      })()}

      {scorecardData?.cards && (
        <UsageScorecard cards={scorecardData.cards} periodWeeks={periodWeeks} />
      )}

      <UsageFilterBar activeFilter={filter} onFilterChange={setFilter} segment="members" periodWeeks={periodWeeks} />

      {membersData && (
        <UsageActionTable
          members={membersData.members}
          total={membersData.total}
          page={page}
          perPage={perPage}
          onPageChange={setPage}
        />
      )}
    </div>
  );
}

// ─── Sky3 Detail Page ───────────────────────────────────────

// ─── Sky3 Side Panel ────────────────────────────────────────

function Sky3SidePanel({ band, periodWeeks, onClose }: { band: string; periodWeeks: number; onClose: () => void }) {
  const [page, setPage] = useState(1);
  const [copied, setCopied] = useState(false);
  const perPage = 25;

  const label = SKY3_BAND_LABELS[band] || band;
  const { data } = useUsageData<{ members: { name: string; email: string }[]; total: number; page: number }>(
    `/api/usage/sky3/members?band=${band}&period_weeks=${periodWeeks}&page=${page}&per_page=${perPage}`,
    [band, periodWeeks, page]
  );

  const handleCopyEmails = async () => {
    const res = await fetch(`/api/usage/sky3/members?band=${band}&period_weeks=${periodWeeks}&fields=email&per_page=9999`);
    const d = await res.json();
    const emails = d.members.map((m: { email: string }) => m.email).join(", ");
    await navigator.clipboard.writeText(emails);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  const handleExport = () => {
    window.location.href = `/api/usage/members/export?segment=sky3&filter=${band}&period_weeks=${periodWeeks}`;
  };

  const totalPages = data ? Math.ceil(data.total / perPage) : 0;

  return (
    <>
      {/* Backdrop */}
      <div className="fixed inset-0 bg-black/20 z-40" onClick={onClose} />
      {/* Panel */}
      <div className="fixed top-0 right-0 bottom-0 z-50 bg-white shadow-xl flex flex-col" style={{ width: "max(400px, 33vw)", fontFamily: FONT_SANS }}>
        <div className="flex items-center justify-between p-4 border-b">
          <div>
            <div className="font-semibold text-base">{label}</div>
            <div className="text-sm text-muted-foreground">{data?.total ?? 0} members</div>
          </div>
          <button onClick={onClose} className="p-1 hover:bg-muted rounded"><XIcon size={18} /></button>
        </div>
        <div className="flex gap-2 p-4 border-b">
          <Button variant="outline" size="sm" className="text-xs gap-1" onClick={handleCopyEmails}>
            <Copy size={14} /> {copied ? "Copied!" : "Copy All Emails"}
          </Button>
          <Button variant="outline" size="sm" className="text-xs gap-1" onClick={handleExport}>
            <DownloadIcon className="size-3.5" /> Export CSV
          </Button>
        </div>
        <div className="flex-1 overflow-auto p-4">
          {data?.members.length ? (
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b">
                  <th className="text-left py-1.5 text-xs text-muted-foreground font-medium">Name</th>
                  <th className="text-left py-1.5 text-xs text-muted-foreground font-medium">Email</th>
                </tr>
              </thead>
              <tbody>
                {data.members.map((m, i) => (
                  <tr key={i} className="border-b border-muted/30">
                    <td className="py-1.5 truncate max-w-[160px]">{m.name}</td>
                    <td className="py-1.5 text-muted-foreground truncate max-w-[200px]">{m.email}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          ) : (
            <p className="text-sm text-muted-foreground">No members in this band.</p>
          )}
        </div>
        {totalPages > 1 && (
          <div className="flex items-center justify-between px-4 py-2 border-t text-xs text-muted-foreground">
            <span>Showing {((page - 1) * perPage) + 1}–{Math.min(page * perPage, data?.total ?? 0)} of {data?.total}</span>
            <div className="flex gap-1">
              <Button variant="outline" size="sm" disabled={page <= 1} onClick={() => setPage(page - 1)} className="text-xs h-7 px-2">Prev</Button>
              <Button variant="outline" size="sm" disabled={page >= totalPages} onClick={() => setPage(page + 1)} className="text-xs h-7 px-2">Next</Button>
            </div>
          </div>
        )}
      </div>
    </>
  );
}

// ─── Sky3 Constants ─────────────────────────────────────────

const SKY3_BANDS = ["not_using", "barely_using", "getting_there", "full_use", "wants_more"] as const;

const SKY3_BAND_LABELS: Record<string, string> = {
  not_using: "Not Using (0 visits)",
  barely_using: "Barely Using (1 visit)",
  getting_there: "Getting There (2 visits)",
  full_use: "Using All 3 Classes (3 visits)",
  wants_more: "Wants More (4+ visits)",
};

const SKY3_VISIT_LABELS: Record<string, string> = {
  not_using: "0 visits",
  barely_using: "1 visit",
  getting_there: "2 visits",
  full_use: "3 visits",
  wants_more: "4+ visits",
};

const SKY3_BAR_COLORS: Record<string, string> = {
  not_using: "#E8D5D0",
  barely_using: "#F0DCC8",
  getting_there: "#F5EAB8",
  full_use: "#C8E6C9",
  wants_more: "#A5D6A7",
};

// ─── Sky3 Types ─────────────────────────────────────────────

interface Sky3BandData { count: number; pct: number }
interface Sky3CohortInfo { stable_count: number; excluded: { new_joins: number; paused: number; pending_cancel: number } }
interface Sky3DistData {
  periodDays: number;
  cohort: Sky3CohortInfo;
  current: Record<string, Sky3BandData>;
  takeaway: { trend: string; text: string };
  total: number;
}

interface Sky3Transition { from: string; to: string; count: number }
interface Sky3MovementData {
  period_days: number;
  improving: { count: number; transitions: Sky3Transition[] };
  stable: { count: number; by_band: Record<string, number> };
  declining: { count: number; transitions: Sky3Transition[] };
  cohort_size: number;
}

// ─── Sky3 Movement Detail Slide-Out ─────────────────────────

function Sky3MovementPanel({
  direction, movementData, periodWeeks, onClose
}: {
  direction: "improving" | "stable" | "declining";
  movementData: Sky3MovementData;
  periodWeeks: number;
  onClose: () => void;
}) {
  const [copied, setCopied] = useState(false);
  const [expandedTransition, setExpandedTransition] = useState<string | null>(null);
  const [transitionMembers, setTransitionMembers] = useState<Record<string, { name: string; email: string }[]>>({});

  const dirData = movementData[direction];
  const count = dirData.count;
  const title = direction === "improving" ? "Improving" : direction === "declining" ? "Declining" : "Stable";

  const handleCopyEmails = async () => {
    const res = await fetch(`/api/usage/sky3/movement/members?direction=${direction}&period_weeks=${periodWeeks}&fields=email&per_page=9999`);
    const d = await res.json();
    const emails = d.members.map((m: { email: string }) => m.email).join(", ");
    await navigator.clipboard.writeText(emails);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  const handleExport = () => {
    window.location.href = `/api/usage/sky3/movement/members?direction=${direction}&period_weeks=${periodWeeks}&fields=email&per_page=9999&format=csv`;
  };

  const handleToggleTransition = async (key: string, from?: string, to?: string) => {
    if (expandedTransition === key) {
      setExpandedTransition(null);
      return;
    }
    setExpandedTransition(key);
    if (!transitionMembers[key]) {
      const params = new URLSearchParams({ direction, period_weeks: String(periodWeeks), per_page: "100" });
      if (from) params.set("from", from);
      if (to) params.set("to", to);
      const res = await fetch(`/api/usage/sky3/movement/members?${params}`);
      const d = await res.json();
      setTransitionMembers(prev => ({ ...prev, [key]: d.members }));
    }
  };

  // Build rows
  let rows: { key: string; label: string; count: number; from?: string; to?: string }[] = [];
  if (direction === "stable") {
    const byBand = (dirData as Sky3MovementData["stable"]).by_band;
    rows = SKY3_BANDS.map(band => ({
      key: band,
      label: `Still at ${SKY3_VISIT_LABELS[band]}`,
      count: byBand[band] || 0,
    })).filter(r => r.count > 0);
  } else {
    const transitions = (dirData as Sky3MovementData["improving"]).transitions;
    rows = transitions.map(t => ({
      key: `${t.from}|${t.to}`,
      label: `${SKY3_VISIT_LABELS[t.from]} \u2192 ${SKY3_VISIT_LABELS[t.to]}`,
      count: t.count,
      from: t.from,
      to: t.to,
    }));
  }

  return (
    <>
      <div className="fixed inset-0 bg-black/20 z-40" onClick={onClose} />
      <div className="fixed top-0 right-0 bottom-0 z-50 bg-white shadow-xl flex flex-col" style={{ width: "max(400px, 33vw)", fontFamily: FONT_SANS }}>
        <div className="flex items-center justify-between p-4 border-b">
          <div>
            <div className="font-semibold text-base">{title} ({count} members)</div>
          </div>
          <button onClick={onClose} className="p-1 hover:bg-muted rounded"><XIcon size={18} /></button>
        </div>
        <div className="flex gap-2 p-4 border-b">
          <Button variant="outline" size="sm" className="text-xs gap-1" onClick={handleCopyEmails}>
            <Copy size={14} /> {copied ? "Copied!" : "Copy All Emails"}
          </Button>
          <Button variant="outline" size="sm" className="text-xs gap-1" onClick={handleExport}>
            <DownloadIcon className="size-3.5" /> Export CSV
          </Button>
        </div>
        <div className="flex-1 overflow-auto p-4">
          <div className="flex flex-col gap-1">
            {rows.map(row => (
              <div key={row.key}>
                <button
                  className="flex items-center justify-between w-full py-2 px-2 rounded hover:bg-muted/30 text-sm"
                  onClick={() => handleToggleTransition(row.key, row.from, row.to)}
                >
                  <div className="flex items-center gap-2">
                    {expandedTransition === row.key ? <ChevronDown size={14} className="text-muted-foreground" /> : <ChevronRight size={14} className="text-muted-foreground" />}
                    <span>{row.label}</span>
                  </div>
                  <span className="text-muted-foreground tabular-nums">{row.count} member{row.count !== 1 ? "s" : ""}</span>
                </button>
                {expandedTransition === row.key && transitionMembers[row.key] && (
                  <div className="ml-8 mb-2">
                    <table className="w-full text-sm">
                      <tbody>
                        {transitionMembers[row.key].map((m, i) => (
                          <tr key={i} className="border-b border-muted/30">
                            <td className="py-1 truncate max-w-[160px]">{m.name}</td>
                            <td className="py-1 text-muted-foreground truncate max-w-[200px]">{m.email}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}
              </div>
            ))}
          </div>
        </div>
      </div>
    </>
  );
}

// ─── Sky3 Page (Redesigned) ─────────────────────────────────

export function UsageSky3Page() {
  const [periodWeeks, setPeriodWeeks] = useState(4);
  const [selectedBand, setSelectedBand] = useState<string | null>(null);
  const [selectedDirection, setSelectedDirection] = useState<"improving" | "stable" | "declining" | null>(null);
  const periodDays = periodWeeks * 7;

  const { data: distData } = useUsageData<Sky3DistData>(
    `/api/usage/sky3/distribution?period_weeks=${periodWeeks}`,
    [periodWeeks]
  );

  const { data: movementData } = useUsageData<Sky3MovementData>(
    `/api/usage/sky3/movement?period_weeks=${periodWeeks}`,
    [periodWeeks]
  );

  const maxCount = distData ? Math.max(...SKY3_BANDS.map(b => distData.current[b]?.count ?? 0), 1) : 1;

  // Takeaway icon + color
  const takeawayColor = distData?.takeaway.trend === "improving" || distData?.takeaway.trend === "slightly_improving"
    ? "#27AE60"
    : distData?.takeaway.trend === "declining" || distData?.takeaway.trend === "slightly_declining"
    ? "#C0392B"
    : "#95A5A6";
  const TakeawayIcon = distData?.takeaway.trend === "improving" || distData?.takeaway.trend === "slightly_improving"
    ? TrendingUp
    : distData?.takeaway.trend === "declining" || distData?.takeaway.trend === "slightly_declining"
    ? TrendingDown
    : Minus;

  return (
    <div className="flex flex-col gap-3" style={{ fontFamily: FONT_SANS }}>
      {/* Header */}
      <div className="flex items-center justify-between mb-1">
        <div className="flex items-center gap-3">
          <BrandSky className="size-7 shrink-0" style={{ color: SECTION_COLORS["usage-sky3"] }} />
          <h1 className="text-3xl font-semibold tracking-tight">Sky3</h1>
        </div>
        <TimeWindowControl value={periodWeeks} onChange={setPeriodWeeks} />
      </div>

      {/* Section 1: Takeaway */}
      {distData?.takeaway && (
        <div className="flex items-start gap-2 mb-4 ml-1">
          <TakeawayIcon size={18} color={takeawayColor} className="mt-0.5 shrink-0" />
          <span className="text-base font-medium" style={{ color: takeawayColor }}>{distData.takeaway.text}</span>
        </div>
      )}

      {/* Section 2: Movement Summary */}
      {movementData && (
        <div className="mb-4 ml-1">
          <h2 className="text-sm font-semibold text-muted-foreground uppercase tracking-wide mb-3">Movement (last {periodDays} days)</h2>
          <div className="flex flex-col gap-2">
            {([
              { key: "improving" as const, icon: TrendingUp, color: "#27AE60", label: "improving", desc: "moved to a higher usage band" },
              { key: "stable" as const, icon: ArrowRight, color: "#95A5A6", label: "stable", desc: "same band as last period" },
              { key: "declining" as const, icon: TrendingDown, color: "#C0392B", label: "declining", desc: "moved to a lower usage band" },
            ]).map(({ key, icon: Icon, color, label, desc }) => (
              <button
                key={key}
                className="flex items-center gap-3 text-left hover:bg-muted/20 rounded px-2 py-1.5 transition-colors"
                onClick={() => setSelectedDirection(key)}
              >
                <Icon size={16} color={color} className="shrink-0" />
                <span className="text-lg font-bold tabular-nums" style={{ color }}>{movementData[key].count}</span>
                <span className="text-sm font-medium" style={{ color }}>{label}</span>
                <span className="text-sm text-muted-foreground">{desc}</span>
              </button>
            ))}
          </div>
        </div>
      )}

      {/* Section 3: Where Members Are Now (no inline deltas) */}
      <div>
        <h2 className="text-lg font-bold mb-3">Where Members Are Now <span className="text-sm font-normal text-muted-foreground">(last {periodDays} days)</span></h2>
        <div className="flex flex-col gap-2">
          {distData && SKY3_BANDS.map(band => {
            const d = distData.current[band];
            if (!d) return null;
            const barWidth = (d.count / maxCount) * 100;

            return (
              <div
                key={band}
                className="flex items-center gap-3 cursor-pointer group"
                onClick={() => setSelectedBand(band)}
              >
                <div className="w-[220px] shrink-0 text-sm font-medium">{SKY3_BAND_LABELS[band]}</div>
                <div className="flex-1 relative h-10 rounded bg-muted/20 group-hover:bg-muted/30 transition-colors">
                  <div
                    className="absolute top-0 left-0 h-full rounded transition-all"
                    style={{ width: `${Math.max(barWidth, 2)}%`, backgroundColor: SKY3_BAR_COLORS[band] }}
                  />
                </div>
                <div className="w-[50px] text-right tabular-nums text-sm font-medium">{d.count}</div>
                <div className="w-[50px] text-right tabular-nums text-sm text-muted-foreground">{d.pct}%</div>
                <div className="w-[20px] text-muted-foreground text-xs">&rsaquo;</div>
              </div>
            );
          })}
        </div>
      </div>

      {/* Footer: Export + Cohort Footnote */}
      <div className="mt-4 flex justify-end">
        <div className="text-right">
          <Button
            variant="outline"
            size="sm"
            className="text-xs gap-1"
            onClick={() => { window.location.href = `/api/usage/members/export?segment=sky3&period_weeks=${periodWeeks}`; }}
          >
            <DownloadIcon className="size-3.5" /> Export Full Member List
          </Button>
          {distData?.cohort && (
            <p className="text-xs text-muted-foreground mt-2" style={{ fontSize: "13px" }}>
              Based on {distData.cohort.stable_count} members subscribed in both periods. Excludes {distData.cohort.excluded.new_joins} new joins, {distData.cohort.excluded.paused} paused, and {distData.cohort.excluded.pending_cancel} pending cancel.
            </p>
          )}
        </div>
      </div>

      {/* Side Panel: Distribution band detail */}
      {selectedBand && (
        <Sky3SidePanel band={selectedBand} periodWeeks={periodWeeks} onClose={() => setSelectedBand(null)} />
      )}

      {/* Side Panel: Movement detail */}
      {selectedDirection && movementData && (
        <Sky3MovementPanel
          direction={selectedDirection}
          movementData={movementData}
          periodWeeks={periodWeeks}
          onClose={() => setSelectedDirection(null)}
        />
      )}
    </div>
  );
}

// ─── TV Detail Page ─────────────────────────────────────────

export function UsageTvPage() {
  const [periodWeeks, setPeriodWeeks] = useState(4);
  const [filter, setFilter] = useState<ActionFilter>(null);
  const [page, setPage] = useState(1);
  const perPage = 25;

  useEffect(() => setPage(1), [filter]);

  const { data: scorecardData } = useUsageData<{ cards: UsageScorecardCard[] }>(
    `/api/usage/scorecard?period_weeks=${periodWeeks}&segment=tv`,
    [periodWeeks]
  );

  const { data: engagementData } = useUsageData<TvEngagementRow[]>(
    `/api/usage/tv-engagement`,
    []
  );

  const filterParam = filter ? `&filter=${filter}` : "";
  const { data: membersData } = useUsageData<{ members: UsageMemberRow[]; total: number; page: number }>(
    `/api/usage/members?segment=tv&period_weeks=${periodWeeks}&page=${page}&per_page=${perPage}${filterParam}`,
    [periodWeeks, filter, page]
  );

  return (
    <div className="flex flex-col gap-3">
      <div className="flex items-center justify-between mb-1">
        <div className="flex items-center gap-3">
          <DeviceTv className="size-7 shrink-0" style={{ color: SECTION_COLORS["usage-tv"] }} />
          <h1 className="text-3xl font-semibold tracking-tight">Sky Ting TV</h1>
        </div>
        <TimeWindowControl value={periodWeeks} onChange={setPeriodWeeks} />
      </div>
      {(() => {
        if (engagementData && Array.isArray(engagementData) && engagementData.length > 0) {
          const inactiveCount = engagementData.find(t => t.tier === "inactive")?.count ?? 0;
          const activeCount = engagementData.filter(t => t.tier !== "inactive").reduce((s, t) => s + t.count, 0);
          return <p className="text-sm ml-10 -mt-2" style={{ color: "#7F8C8D" }}>{inactiveCount.toLocaleString()} inactive — {activeCount.toLocaleString()} active in last 14 days</p>;
        }
        return <p className="text-sm text-muted-foreground ml-10 -mt-2">Digital subscription engagement</p>;
      })()}

      <AlertBanner segment="tv" tierData={engagementData && Array.isArray(engagementData) ? engagementData : null} />

      {scorecardData?.cards && (
        <UsageScorecard cards={scorecardData.cards} periodWeeks={periodWeeks} />
      )}

      {engagementData && Array.isArray(engagementData) && (
        <TvEngagementBars tiers={engagementData} />
      )}

      <UsageFilterBar activeFilter={filter} onFilterChange={setFilter} segment="tv" periodWeeks={periodWeeks} />

      {membersData && (
        <UsageActionTable
          members={membersData.members}
          total={membersData.total}
          page={page}
          perPage={perPage}
          onPageChange={setPage}
        />
      )}
    </div>
  );
}
