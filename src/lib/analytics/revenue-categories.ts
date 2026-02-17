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
  /** Auto-renew revenue (SKY UNLIMITED, SKY3, SKY TING TV, etc.) */
  autoRenewRevenue: number;
  autoRenewNetRevenue: number;
  /** Workshop revenue */
  workshopRevenue: number;
  /** Everything else */
  otherRevenue: number;
}

// ── Business category mapping ─────────────────────────────────
// Maps raw Union.fit revenue category names to meaningful business groups.
// Order matters: first match wins.

const CATEGORY_MAP: { label: string; patterns: RegExp[] }[] = [
  // Subscriptions — Members (in-person unlimited)
  { label: "Members", patterns: [
    /sky\s*unlimited/i, /all\s*access/i, /10member/i,
    /sky\s*ting\s*(monthly\s*)?membership/i, /sky\s*ting\s*in\s*person/i,
    /ting\s*fam/i, /sky\s*virgin.*membership/i, /founding\s*member\s*annual\b/i,
    /new\s*member/i, /back\s*to\s*school/i, /secret\s*membership/i,
    /monthly\s*membership\s*special/i,
  ]},
  // Subscriptions — SKY3 / Packs
  { label: "SKY3 / Packs", patterns: [
    /sky\s*3/i, /sky\s*5/i, /skyhigh/i, /5[\s-]*pack/i,
  ]},
  // Subscriptions — SKY TING TV (digital)
  { label: "SKY TING TV", patterns: [
    /sky\s*ting\s*tv/i, /10skyting/i, /digital\s*all/i,
    /a\s*la\s*carte\s*sky\s*ting/i, /sky\s*week\s*tv/i,
    /friends\s*of\s*sky\s*ting/i, /new\s*subscriber\s*special/i,
    /limited\s*edition\s*sky\s*ting/i, /retreat\s*ting/i,
    /come\s*back\s*sky\s*ting/i,
  ]},
  // Drop-ins
  { label: "Drop-Ins", patterns: [/drop[\s-]*in/i, /droplet/i] },
  // Intro / trial
  { label: "Intro / Trial", patterns: [/intro\s*week/i, /unlimited\s*week/i, /sky\s*virgin\s*2\s*week/i] },
  // Workshops & events
  { label: "Workshops", patterns: [/workshop/i, /masterclass/i, /specialty\s*class/i] },
  // Wellness & spa
  { label: "Wellness / Spa", patterns: [
    /infrared/i, /sauna/i, /spa\s*lounge/i, /cupping/i,
    /contrast\s*suite/i, /treatment\s*room/i,
  ]},
  // Teacher training
  { label: "Teacher Training", patterns: [/teacher\s*training/i, /200hr/i, /training/i] },
  // Retail & merch
  { label: "Retail / Merch", patterns: [/merch/i, /product/i, /food.*bev/i, /gift\s*card/i] },
  // Privates
  { label: "Privates", patterns: [/private/i] },
  // Donations
  { label: "Donations", patterns: [/donation/i] },
  // Rentals
  { label: "Rentals", patterns: [/rental/i, /teacher\s*rental/i, /studio\s*rental/i] },
  // Retreats
  { label: "Retreats", patterns: [/retreat/i] },
  // Community
  { label: "Community", patterns: [/community/i] },
];

// Top-level rollup (for the stacked bar)
const MEMBER_PATTERNS = [
  /sky\s*unlimited/i, /all\s*access/i, /10member/i,
  /sky\s*ting\s*(monthly\s*)?membership/i, /sky\s*ting\s*in\s*person/i,
  /ting\s*fam/i, /sky\s*virgin.*membership/i, /founding\s*member\s*annual\b/i,
  /new\s*member/i, /back\s*to\s*school/i, /secret\s*membership/i,
  /monthly\s*membership\s*special/i,
];
const SKY3_PATTERNS = [/sky\s*3/i, /sky\s*5/i, /skyhigh/i, /5[\s-]*pack/i];
const TV_PATTERNS = [
  /sky\s*ting\s*tv/i, /10skyting/i, /digital\s*all/i,
  /a\s*la\s*carte\s*sky\s*ting/i, /sky\s*week\s*tv/i,
  /friends\s*of\s*sky\s*ting/i, /new\s*subscriber\s*special/i,
  /limited\s*edition\s*sky\s*ting/i, /retreat\s*ting/i,
  /come\s*back\s*sky\s*ting/i,
];
const AUTO_RENEW_PATTERNS = [...MEMBER_PATTERNS, ...SKY3_PATTERNS, ...TV_PATTERNS, /auto\s*renew/i, /community/i];
const DROP_IN_PATTERNS = [/drop[\s-]*in/i, /droplet/i];
const WORKSHOP_PATTERNS = [/workshop/i];

function matchesAny(name: string, patterns: RegExp[]): boolean {
  return patterns.some((p) => p.test(name));
}

/** Map a raw Union.fit revenue category name to a business group label */
function mapToBusinessCategory(name: string): string {
  for (const { label, patterns } of CATEGORY_MAP) {
    if (matchesAny(name, patterns)) return label;
  }
  return "Other";
}

export function analyzeRevenueCategories(
  data: RevenueCategory[]
): RevenueCategoryAnalysis {
  // Filter out summary/total rows and empty rows from HTML table scraping
  const filtered = data.filter((row) => {
    const name = row.revenueCategory.trim().toLowerCase();
    return name !== "" && name !== "total";
  });

  let totalRevenue = 0;
  let totalNetRevenue = 0;
  let totalFees = 0;
  let totalRefunded = 0;
  let dropInRevenue = 0;
  let dropInNetRevenue = 0;
  let autoRenewRevenue = 0;
  let autoRenewNetRevenue = 0;
  let workshopRevenue = 0;
  let otherRevenue = 0;

  // Group raw categories by business label
  const grouped = new Map<string, { revenue: number; netRevenue: number; fees: number; refunded: number }>();

  for (const row of filtered) {
    const fees = row.unionFees + row.stripeFees + (row.otherFees ?? 0) + (row.transfers ?? 0);
    totalRevenue += row.revenue;
    totalNetRevenue += row.netRevenue;
    totalFees += fees;
    totalRefunded += row.refunded;

    // Top-level rollup buckets
    const name = row.revenueCategory;
    if (matchesAny(name, DROP_IN_PATTERNS)) {
      dropInRevenue += row.revenue;
      dropInNetRevenue += row.netRevenue;
    } else if (matchesAny(name, AUTO_RENEW_PATTERNS)) {
      autoRenewRevenue += row.revenue;
      autoRenewNetRevenue += row.netRevenue;
    } else if (matchesAny(name, WORKSHOP_PATTERNS)) {
      workshopRevenue += row.revenue;
    } else {
      otherRevenue += row.revenue;
    }

    // Group by business category label
    const label = mapToBusinessCategory(name);
    if (label === "Other") {
      console.warn(`[revenue-categories] Unmapped category: "${name}" — assign to a defined business term`);
    }
    const existing = grouped.get(label);
    if (existing) {
      existing.revenue += row.revenue;
      existing.netRevenue += row.netRevenue;
      existing.fees += fees;
      existing.refunded += row.refunded;
    } else {
      grouped.set(label, { revenue: row.revenue, netRevenue: row.netRevenue, fees, refunded: row.refunded });
    }
  }

  // Convert to sorted array
  const categories = Array.from(grouped.entries())
    .map(([category, vals]) => ({
      category,
      revenue: Math.round(vals.revenue * 100) / 100,
      netRevenue: Math.round(vals.netRevenue * 100) / 100,
      fees: Math.round(vals.fees * 100) / 100,
      refunded: Math.round(vals.refunded * 100) / 100,
    }))
    .sort((a, b) => b.revenue - a.revenue);

  return {
    categories,
    totalRevenue: Math.round(totalRevenue * 100) / 100,
    totalNetRevenue: Math.round(totalNetRevenue * 100) / 100,
    totalFees: Math.round(totalFees * 100) / 100,
    totalRefunded: Math.round(totalRefunded * 100) / 100,
    dropInRevenue: Math.round(dropInRevenue * 100) / 100,
    dropInNetRevenue: Math.round(dropInNetRevenue * 100) / 100,
    autoRenewRevenue: Math.round(autoRenewRevenue * 100) / 100,
    autoRenewNetRevenue: Math.round(autoRenewNetRevenue * 100) / 100,
    workshopRevenue: Math.round(workshopRevenue * 100) / 100,
    otherRevenue: Math.round(otherRevenue * 100) / 100,
  };
}
