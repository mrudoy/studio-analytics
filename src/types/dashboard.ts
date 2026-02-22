/**
 * Dashboard type definitions — single source of truth.
 *
 * Both the API routes and the client (page.tsx) import from here.
 */

export interface ShopifyStats {
  totalOrders: number;
  totalRevenue: number;
  productCount: number;
  customerCount: number;
  lastSyncAt: string | null;
}

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
  shopify?: ShopifyStats | null;
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

// ── Drop-In Module types (weekly-first redesign) ──────────

export interface DropInWeekRow {
  weekStart: string;    // Monday YYYY-MM-DD
  weekEnd: string;      // Sunday YYYY-MM-DD
  visits: number;
  uniqueCustomers: number;
  firstTime: number;
  repeatCustomers: number;
}

export interface DropInWTD {
  weekStart: string;
  weekEnd: string;
  visits: number;
  uniqueCustomers: number;
  firstTime: number;
  repeatCustomers: number;
  daysLeft: number;
}

export interface DropInFrequency {
  bucket1: number;       // visited exactly once
  bucket2to4: number;    // 2-4 visits
  bucket5to10: number;   // 5-10 visits
  bucket11plus: number;  // 11+ visits
  totalCustomers: number;
}

export interface DropInModuleData {
  completeWeeks: DropInWeekRow[];
  wtd: DropInWTD | null;
  lastCompleteWeek: DropInWeekRow | null;
  typicalWeekVisits: number;       // 8-week avg of complete weeks
  trend: "up" | "flat" | "down";
  trendDeltaPercent: number;       // last 4 vs prior 4
  wtdDelta: number;                // WTD visits - last week through same weekday
  wtdDeltaPercent: number;
  wtdDayLabel: string;             // "As of Mon", "As of Tue", etc.
  frequency: DropInFrequency | null;
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

// ── Intro Week types ──────────────────────────────────────

export interface IntroWeekWeekRow {
  weekStart: string;   // Monday YYYY-MM-DD
  customers: number;   // unique intro-week customers that week
}

export interface IntroWeekData {
  lastWeek: IntroWeekWeekRow | null;
  last4Weeks: IntroWeekWeekRow[];
  last4WeekAvg: number;
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

// ── Conversion Pool types ─────────────────────────────────

export interface ConversionPoolWeekRow {
  weekStart: string;          // Monday YYYY-MM-DD
  weekEnd: string;            // Sunday YYYY-MM-DD
  activePool7d: number;       // unique non-auto emails who visited this week
  converts: number;           // pool members who started in-studio auto-renew this week
  conversionRate: number;     // converts / activePool7d * 100
  yieldPer100: number;        // same as rate, explicit "per 100" framing
}

export interface ConversionPoolWTD {
  weekStart: string;
  weekEnd: string;
  activePool7d: number;
  activePool30d: number;      // unique non-auto emails in last 30 days
  converts: number;
  conversionRate: number;
  daysLeft: number;
}

export interface ConversionPoolLagStats {
  medianTimeToConvert: number | null;     // median days (this week's converters)
  avgVisitsBeforeConvert: number | null;  // avg visit count before conversion
  // Time-to-convert buckets (this week's converters)
  timeBucket0to30: number;
  timeBucket31to90: number;
  timeBucket91to180: number;
  timeBucket180plus: number;
  // Visits-to-convert buckets (this week's converters)
  visitBucket1to2: number;
  visitBucket3to5: number;
  visitBucket6to10: number;
  visitBucket11plus: number;
  totalConvertersInBuckets: number;
  // Historical aggregates (last 12 complete weeks)
  historicalMedianTimeToConvert: number | null;
  historicalAvgVisitsBeforeConvert: number | null;
}

export type ConversionPoolSlice = "all" | "drop-ins" | "intro-week" | "class-packs" | "high-intent";

export interface ConversionPoolSliceData {
  completeWeeks: ConversionPoolWeekRow[];   // last 16 complete weeks
  wtd: ConversionPoolWTD | null;
  lagStats: ConversionPoolLagStats | null;
  lastCompleteWeek: ConversionPoolWeekRow | null;
  avgPool7d: number;                         // 8-week average of activePool7d
  avgRate: number;                           // 8-week average conversion rate
}

export interface ConversionPoolModuleData {
  slices: Partial<Record<ConversionPoolSlice, ConversionPoolSliceData>>;
}

// ── Trends aggregate ───────────────────────────────────────

export interface TrendsData {
  weekly: TrendRowData[];
  monthly: TrendRowData[];
  pacing: PacingData | null;
  projection: ProjectionData | null;
  dropIns: DropInModuleData | null;
  introWeek: IntroWeekData | null;
  firstVisits: FirstVisitData | null;
  returningNonMembers: ReturningNonMemberData | null;
  churnRates: ChurnRateData | null;
  newCustomerVolume: NewCustomerVolumeData | null;
  newCustomerCohorts: NewCustomerCohortData | null;
  conversionPool: ConversionPoolModuleData | null;
}
