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
  ClassRevenue,
  ShoppingBag,
  Droplet,
  BuildingIcon,
  BulbIcon,
  ActivityIcon,
  MountainSun,
} from "./icons";

export type SectionKey =
  | "overview"
  | "revenue"
  | "revenue-merch"
  | "revenue-spa"
  | "revenue-rentals"
  | "revenue-retreats"
  | "growth-auto"
  | "growth-non-auto"
  | "conversion-new"
  | "conversion-pool"
  | "churn"
  | "usage"
  | "insights"
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
  "revenue-merch": "#4A7C59",
  "revenue-spa":  "#4A7C59",
  "revenue-rentals": "#4A7C59",
  "revenue-retreats": "#B87333",  // copper
  "growth-auto":  "#5B7FA5",  // steel blue
  "growth-non-auto": "#5B7FA5",
  "conversion-new":  "#B87333",  // copper
  "conversion-pool": "#B87333",
  churn:          "#A04040",  // muted red
  usage:          "#7C6B8A",  // muted purple
  insights:       "#D4A030",  // amber/gold
  data:           "#6B5B73",  // dusty purple
};

export const NAV_ITEMS: NavItem[] = [
  { key: "overview", label: "Overview", icon: Eyeglass, color: SECTION_COLORS.overview },
  {
    key: "revenue",
    label: "Revenue",
    icon: ReportMoney,
    color: SECTION_COLORS.revenue,
    children: [
      { key: "revenue", label: "Revenue Overview", icon: ClassRevenue },
      { key: "revenue-merch", label: "Merch", icon: ShoppingBag },
      { key: "revenue-spa", label: "Spa & Wellness", icon: Droplet },
      { key: "revenue-rentals", label: "Studio Rentals", icon: BuildingIcon },
      { key: "revenue-retreats", label: "Retreats", icon: MountainSun },
    ],
  },
  {
    key: "growth-auto",
    label: "Growth",
    icon: ChartBarPopular,
    color: SECTION_COLORS["growth-auto"],
    children: [
      { key: "growth-auto", label: "Auto-Renews", icon: Recycle },
      { key: "growth-non-auto", label: "Non Auto-Renews", icon: RecycleOff },
    ],
  },
  {
    key: "conversion-new",
    label: "Conversion",
    icon: ArrowFork,
    color: SECTION_COLORS["conversion-new"],
    children: [
      { key: "conversion-new", label: "New Customers", icon: UserPlus },
      { key: "conversion-pool", label: "Non Auto-Renew Customers", icon: UsersGroup },
    ],
  },
  { key: "churn", label: "Churn", icon: HourglassLow, color: SECTION_COLORS.churn },
  { key: "usage", label: "Usage", icon: ActivityIcon, color: SECTION_COLORS.usage },
  { key: "insights", label: "Insights", icon: BulbIcon, color: SECTION_COLORS.insights },
  { key: "data", label: "Data", icon: Database, color: SECTION_COLORS.data },
];

export const BREADCRUMB_MAP: Record<SectionKey, string[]> = {
  overview: ["Overview"],
  revenue: ["Revenue", "Revenue Overview"],
  "revenue-merch": ["Revenue", "Merch"],
  "revenue-spa": ["Revenue", "Spa & Wellness"],
  "revenue-rentals": ["Revenue", "Studio Rentals"],
  "revenue-retreats": ["Revenue", "Retreats"],
  "growth-auto": ["Growth", "Auto-Renews"],
  "growth-non-auto": ["Growth", "Non Auto-Renews"],
  "conversion-new": ["Conversion", "New Customers"],
  "conversion-pool": ["Conversion", "Non Auto-Renew Customers"],
  churn: ["Churn"],
  usage: ["Usage"],
  insights: ["Insights"],
  data: ["Data"],
};
