import type { RevenueCategory } from "@/types/union-data";

export interface RevenueCategoryAnalysis {
  categories: {
    category: string;
    revenue: number;
    netRevenue: number;
    fees: number;
    refunded: number;
  }[];
  totalRevenue: number;
  totalNetRevenue: number;
  totalFees: number;
  totalRefunded: number;
  /** Drop-in revenue (category name contains "Drop-in") */
  dropInRevenue: number;
  dropInNetRevenue: number;
  /** Subscription-related revenue (SKY UNLIMITED, SKY3, SKY TING TV, etc.) */
  subscriptionRevenue: number;
  subscriptionNetRevenue: number;
  /** Workshop revenue */
  workshopRevenue: number;
  /** Everything else */
  otherRevenue: number;
}

const SUBSCRIPTION_PATTERNS = [
  /sky\s*unlimited/i,
  /sky\s*3/i,
  /sky\s*ting\s*tv/i,
  /all\s*access/i,
  /digital\s*all/i,
  /auto\s*renew/i,
  /10member/i,
  /10skyting/i,
  /community/i,
];

const DROP_IN_PATTERNS = [/drop[\s-]*in/i];
const WORKSHOP_PATTERNS = [/workshop/i];

function matchesAny(name: string, patterns: RegExp[]): boolean {
  return patterns.some((p) => p.test(name));
}

export function analyzeRevenueCategories(
  data: RevenueCategory[]
): RevenueCategoryAnalysis {
  let totalRevenue = 0;
  let totalNetRevenue = 0;
  let totalFees = 0;
  let totalRefunded = 0;
  let dropInRevenue = 0;
  let dropInNetRevenue = 0;
  let subscriptionRevenue = 0;
  let subscriptionNetRevenue = 0;
  let workshopRevenue = 0;
  let otherRevenue = 0;

  const categories = data.map((row) => {
    const fees = row.unionFees + row.stripeFees + row.transfers;
    totalRevenue += row.revenue;
    totalNetRevenue += row.netRevenue;
    totalFees += fees;
    totalRefunded += row.refunded;

    const name = row.revenueCategory;
    if (matchesAny(name, DROP_IN_PATTERNS)) {
      dropInRevenue += row.revenue;
      dropInNetRevenue += row.netRevenue;
    } else if (matchesAny(name, SUBSCRIPTION_PATTERNS)) {
      subscriptionRevenue += row.revenue;
      subscriptionNetRevenue += row.netRevenue;
    } else if (matchesAny(name, WORKSHOP_PATTERNS)) {
      workshopRevenue += row.revenue;
    } else {
      otherRevenue += row.revenue;
    }

    return {
      category: name,
      revenue: row.revenue,
      netRevenue: row.netRevenue,
      fees,
      refunded: row.refunded,
    };
  });

  // Sort by revenue descending
  categories.sort((a, b) => b.revenue - a.revenue);

  return {
    categories,
    totalRevenue: Math.round(totalRevenue * 100) / 100,
    totalNetRevenue: Math.round(totalNetRevenue * 100) / 100,
    totalFees: Math.round(totalFees * 100) / 100,
    totalRefunded: Math.round(totalRefunded * 100) / 100,
    dropInRevenue: Math.round(dropInRevenue * 100) / 100,
    dropInNetRevenue: Math.round(dropInNetRevenue * 100) / 100,
    subscriptionRevenue: Math.round(subscriptionRevenue * 100) / 100,
    subscriptionNetRevenue: Math.round(subscriptionNetRevenue * 100) / 100,
    workshopRevenue: Math.round(workshopRevenue * 100) / 100,
    otherRevenue: Math.round(otherRevenue * 100) / 100,
  };
}
