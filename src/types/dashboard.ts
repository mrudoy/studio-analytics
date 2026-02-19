/**
 * Dashboard type definitions — single source of truth.
 *
 * Both the API routes and the client (page.tsx) import from here.
 */

export interface DashboardStats {
  lastUpdated: string | null;
  dateRange: string | null;
  spreadsheetUrl?: string;
  dataSource?: "database" | "sheets" | "hybrid";
  mrr: {
    member: number;
    sky3: number;
    skyTingTv: number;
    unknown: number;
    total: number;
  };
  activeSubscribers: {
    member: number;
    sky3: number;
    skyTingTv: number;
    unknown: number;
    total: number;
  };
  arpu: {
    member: number;
    sky3: number;
    skyTingTv: number;
    overall: number;
  };
  currentMonthRevenue: number;
  previousMonthRevenue: number;
  trends?: TrendsData | null;
  revenueCategories?: RevenueCategoryData | null;
  monthOverMonth?: MonthOverMonthData | null;
  monthlyRevenue?: { month: string; gross: number; net: number }[];
}

export interface RevenueCategoryData {
  periodStart: string;
  periodEnd: string;
  categories: { category: string; revenue: number; netRevenue: number; fees: number; refunded: number }[];
  totalRevenue: number;
  totalNetRevenue: number;
  totalFees: number;
  totalRefunded: number;
  dropInRevenue: number;
  dropInNetRevenue: number;
  autoRenewRevenue: number;
  autoRenewNetRevenue: number;
  workshopRevenue: number;
  otherRevenue: number;
}

export interface TrendRowData {
  period: string;
  type: string;
  newMembers: number;
  newSky3: number;
  newSkyTingTv: number;
  memberChurn: number;
  sky3Churn: number;
  skyTingTvChurn: number;
  netMemberGrowth: number;
  netSky3Growth: number;
  revenueAdded: number;
  revenueLost: number;
  deltaNewMembers: number | null;
  deltaNewSky3: number | null;
  deltaRevenue: number | null;
  deltaPctNewMembers: number | null;
  deltaPctNewSky3: number | null;
  deltaPctRevenue: number | null;
}

export interface PacingData {
  month: string;
  daysElapsed: number;
  daysInMonth: number;
  newMembersActual: number;
  newMembersPaced: number;
  newSky3Actual: number;
  newSky3Paced: number;
  revenueActual: number;
  revenuePaced: number;
  memberCancellationsActual: number;
  memberCancellationsPaced: number;
  sky3CancellationsActual: number;
  sky3CancellationsPaced: number;
}

export interface ProjectionData {
  year: number;
  projectedAnnualRevenue: number;
  currentMRR: number;
  projectedYearEndMRR: number;
  monthlyGrowthRate: number;
  priorYearRevenue: number;
  /** Actual total revenue from revenue_categories table for the prior calendar year */
  priorYearActualRevenue: number | null;
}

export interface DropInData {
  currentMonthTotal: number;
  currentMonthDaysElapsed: number;
  currentMonthDaysInMonth: number;
  currentMonthPaced: number;
  previousMonthTotal: number;
  weeklyAvg6w: number;
  weeklyBreakdown: { week: string; count: number }[];
}

export type FirstVisitSegment = "introWeek" | "dropIn" | "guest" | "other";

export interface FirstVisitData {
  currentWeekTotal: number;
  currentWeekSegments: Record<FirstVisitSegment, number>;
  completedWeeks: { week: string; uniqueVisitors: number; segments: Record<FirstVisitSegment, number> }[];
  aggregateSegments: Record<FirstVisitSegment, number>;
  otherBreakdownTop5?: { passName: string; count: number }[];
}

export interface ReturningNonMemberData {
  currentWeekTotal: number;
  currentWeekSegments: Record<FirstVisitSegment, number>;
  completedWeeks: { week: string; uniqueVisitors: number; segments: Record<FirstVisitSegment, number> }[];
  aggregateSegments: Record<FirstVisitSegment, number>;
  otherBreakdownTop5?: { passName: string; count: number }[];
}

/** Per-category churn data for a single month */
export interface CategoryMonthlyChurn {
  month: string;
  userChurnRate: number;         // count-based: canceledCount / activeAtStart * 100
  mrrChurnRate: number;          // revenue-based: canceledMRR / activeMRR * 100
  activeAtStart: number;
  activeMrrAtStart: number;
  canceledCount: number;
  canceledMrr: number;
  // MEMBER-only: annual vs monthly breakdown
  annualCanceledCount?: number;
  annualActiveAtStart?: number;
  monthlyCanceledCount?: number;
  monthlyActiveAtStart?: number;
}

/** Churn summary for one auto-renew category */
export interface CategoryChurnData {
  category: "MEMBER" | "SKY3" | "SKY_TING_TV";
  monthly: CategoryMonthlyChurn[];
  avgUserChurnRate: number;
  avgMrrChurnRate: number;
  atRiskCount: number;
}

export interface ChurnRateData {
  /** Per-category churn data */
  byCategory: {
    member: CategoryChurnData;
    sky3: CategoryChurnData;
    skyTingTv: CategoryChurnData;
  };
  totalAtRisk: number;
  /** Legacy flat fields (backward compat) */
  monthly: {
    month: string;
    memberRate: number;
    sky3Rate: number;
    memberActiveStart: number;
    sky3ActiveStart: number;
    memberCanceled: number;
    sky3Canceled: number;
  }[];
  avgMemberRate: number;
  avgSky3Rate: number;
  atRisk: number;
}

export interface MonthOverMonthPeriod {
  year: number;
  periodStart: string;
  periodEnd: string;
  gross: number;
  net: number;
  categoryCount: number;
}

export interface MonthOverMonthData {
  month: number;
  monthName: string;
  current: MonthOverMonthPeriod | null;
  priorYear: MonthOverMonthPeriod | null;
  yoyGrossChange: number | null;
  yoyNetChange: number | null;
  yoyGrossPct: number | null;
  yoyNetPct: number | null;
}

// ── New Customer types ─────────────────────────────────────

export interface NewCustomerVolumeWeek {
  weekStart: string;   // Monday YYYY-MM-DD
  weekEnd: string;     // Sunday YYYY-MM-DD
  count: number;
}

export interface NewCustomerVolumeData {
  currentWeekCount: number;
  completedWeeks: NewCustomerVolumeWeek[];
}

export interface NewCustomerCohortRow {
  cohortStart: string;   // Monday YYYY-MM-DD
  cohortEnd: string;     // Sunday YYYY-MM-DD
  newCustomers: number;
  week1: number;
  week2: number;
  week3: number;
  total3Week: number;
}

export interface NewCustomerCohortData {
  cohorts: NewCustomerCohortRow[];
  avgConversionRate: number | null;  // null if < 3 complete cohorts
}

// ── Trends aggregate ───────────────────────────────────────

export interface TrendsData {
  weekly: TrendRowData[];
  monthly: TrendRowData[];
  pacing: PacingData | null;
  projection: ProjectionData | null;
  dropIns: DropInData | null;
  firstVisits: FirstVisitData | null;
  returningNonMembers: ReturningNonMemberData | null;
  churnRates: ChurnRateData | null;
  newCustomerVolume: NewCustomerVolumeData | null;
  newCustomerCohorts: NewCustomerCohortData | null;
}
