import type { ComponentType, SVGProps } from "react";
import {
  Eyeglass,
  ReportMoney,
  ChartBarPopular,
  ArrowFork,
  HourglassLow,
} from "./icons";

export type SectionKey =
  | "overview"
  | "revenue"
  | "growth-auto"
  | "growth-non-auto"
  | "conversion-new"
  | "conversion-pool"
  | "churn";

type IconComponent = ComponentType<SVGProps<SVGSVGElement> & { size?: number }>;

export interface NavItem {
  key: SectionKey;
  label: string;
  icon?: IconComponent;
  children?: NavItem[];
}

export const NAV_ITEMS: NavItem[] = [
  { key: "overview", label: "Overview", icon: Eyeglass },
  { key: "revenue", label: "Revenue", icon: ReportMoney },
  {
    key: "growth-auto",
    label: "Growth",
    icon: ChartBarPopular,
    children: [
      { key: "growth-auto", label: "Auto-Renews" },
      { key: "growth-non-auto", label: "Non-Auto-Renews" },
    ],
  },
  {
    key: "conversion-new",
    label: "Conversion",
    icon: ArrowFork,
    children: [
      { key: "conversion-new", label: "New Customers" },
      { key: "conversion-pool", label: "All Non Auto-Renew Customers" },
    ],
  },
  { key: "churn", label: "Churn", icon: HourglassLow },
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
