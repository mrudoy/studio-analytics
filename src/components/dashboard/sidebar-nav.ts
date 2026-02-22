import {
  LayoutDashboard,
  DollarSign,
  TrendingUp,
  ArrowRightLeft,
  AlertTriangle,
  type LucideIcon,
} from "lucide-react";

export type SectionKey =
  | "overview"
  | "revenue"
  | "growth-auto"
  | "growth-non-auto"
  | "conversion-new"
  | "conversion-pool"
  | "churn";

export interface NavItem {
  key: SectionKey;
  label: string;
  icon?: LucideIcon;
  children?: NavItem[];
}

export const NAV_ITEMS: NavItem[] = [
  { key: "overview", label: "Overview", icon: LayoutDashboard },
  { key: "revenue", label: "Revenue", icon: DollarSign },
  {
    key: "growth-auto",
    label: "Growth",
    icon: TrendingUp,
    children: [
      { key: "growth-auto", label: "Auto-Renews" },
      { key: "growth-non-auto", label: "Non-Auto-Renews" },
    ],
  },
  {
    key: "conversion-new",
    label: "Conversion",
    icon: ArrowRightLeft,
    children: [
      { key: "conversion-new", label: "New Customers" },
      { key: "conversion-pool", label: "All Non Auto-Renew Customers" },
    ],
  },
  { key: "churn", label: "Churn", icon: AlertTriangle },
];

export const BREADCRUMB_MAP: Record<SectionKey, string[]> = {
  overview: ["Overview"],
  revenue: ["Revenue"],
  "growth-auto": ["Growth", "Auto-Renews"],
  "growth-non-auto": ["Growth", "Non-Auto-Renews"],
  "conversion-new": ["Conversion", "New Customers"],
  "conversion-pool": ["Conversion", "All Non Auto-Renew Customers"],
  churn: ["Churn"],
};
