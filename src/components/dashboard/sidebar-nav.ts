import type { ComponentType, SVGProps } from "react";
import {
  Eyeglass,
  ReportMoney,
  ChartBarPopular,
  Recycle,
  RecycleOff,
  ArrowFork,
  HourglassLow,
  UserPlus,
  UsersGroup,
  Database,
} from "./icons";

export type SectionKey =
  | "overview"
  | "revenue"
  | "growth-auto"
  | "growth-non-auto"
  | "conversion-new"
  | "conversion-pool"
  | "churn"
  | "data";

type IconComponent = ComponentType<SVGProps<SVGSVGElement> & { size?: number }>;

export interface NavItem {
  key: SectionKey;
  label: string;
  icon?: IconComponent;
  color?: string;
  children?: NavItem[];
}

/** Section colors — used for sidebar icons and section headers */
export const SECTION_COLORS: Record<SectionKey, string> = {
  overview:       "#413A3A",  // warm charcoal
  revenue:        "#4A7C59",  // forest green
  "growth-auto":  "#5B7FA5",  // steel blue
  "growth-non-auto": "#5B7FA5",
  "conversion-new":  "#B87333",  // copper
  "conversion-pool": "#B87333",
  churn:          "#A04040",  // muted red
  data:           "#6B5B73",  // dusty purple
};

export const NAV_ITEMS: NavItem[] = [
  { key: "overview", label: "Overview", icon: Eyeglass, color: SECTION_COLORS.overview },
  { key: "revenue", label: "Revenue", icon: ReportMoney, color: SECTION_COLORS.revenue },
  {
    key: "growth-auto",
    label: "Growth",
    icon: ChartBarPopular,
    color: SECTION_COLORS["growth-auto"],
    children: [
      { key: "growth-auto", label: "Auto-Renews", icon: Recycle },
      { key: "growth-non-auto", label: "Non-Auto-Renews", icon: RecycleOff },
    ],
  },
  {
    key: "conversion-new",
    label: "Conversion",
    icon: ArrowFork,
    color: SECTION_COLORS["conversion-new"],
    children: [
      { key: "conversion-new", label: "New Customers", icon: UserPlus },
      { key: "conversion-pool", label: "All Non Auto-Renew Customers", icon: UsersGroup },
    ],
  },
  { key: "churn", label: "Churn", icon: HourglassLow, color: SECTION_COLORS.churn },
  { key: "data", label: "Data", icon: Database, color: SECTION_COLORS.data },
];

export const BREADCRUMB_MAP: Record<SectionKey, string[]> = {
  overview: ["Overview"],
  revenue: ["Revenue"],
  "growth-auto": ["Growth", "Auto-Renews"],
  "growth-non-auto": ["Growth", "Non-Auto-Renews"],
  "conversion-new": ["Conversion", "New Customers"],
  "conversion-pool": ["Conversion", "All Non Auto-Renew Customers"],
  churn: ["Churn"],
  data: ["Data"],
};
