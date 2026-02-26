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

export interface ShopifyTopProduct {
  title: string;
  revenue: number;
  unitsSold: number;
}

export interface MerchCustomerBreakdown {
  subscriber: { orders: number; revenue: number; customers: number };
  nonSubscriber: { orders: number; revenue: number; customers: number };
  total: { orders: number; revenue: number; customers: number };
}

export interface ShopifyAnnualRevenue {
  year: number;
  gross: number;
  net: number;
  orderCount: number;
  avgOrderValue: number;
}

export interface ShopifyCategoryBreakdown {
  category: string;
  revenue: number;
  units: number;
  orders: number;
}

export interface ShopifyMerchData {
  mtdRevenue: number;
  monthlyRevenue: Array<{ month: string; gross: number; net: number; orderCount: number }>;
  avgMonthlyRevenue: number;
  topProducts: ShopifyTopProduct[];
  repeatCustomerRate: number;
  repeatCustomerCount: number;
  totalCustomersWithOrders: number;
  customerBreakdown?: MerchCustomerBreakdown | null;
  annualRevenue: ShopifyAnnualRevenue[];
  categoryBreakdown: ShopifyCategoryBreakdown[];
}

export interface DataFreshness {
  unionAutoRenews: string | null;   // MAX(imported_at) from auto_renews
  unionRegistrations: string | null; // MAX(imported_at) from registrations
  shopifySync: string | null;        // MAX(synced_at) from shopify_orders
  lastPipelineRun: string | null;    // MAX(ran_at) from pipeline_runs
  overall: string | null;            // most recent of all sources
  isPartial: boolean;                // true if sources have different timestamps (partial update)
}

export interface DashboardStats {
  lastUpdated: string | null;
  dateRange: string | null;
  spreadsheetUrl?: string;
  dataSource?: "database" | "sheets" | "hybrid";
  dataFreshness?: DataFreshness | null;
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
  monthlyRevenue?: { month: string; gross: number; net: number; retreatGross?: number; retreatNet?: number }[];
  shopify?: ShopifyStats | null;
  shopifyMerch?: ShopifyMerchData | null;
  spa?: SpaData | null;
  annualBreakdown?: AnnualRevenueBreakdown[] | null;
  rentalRevenue?: RentalRevenueData | null;
  overviewData?: OverviewData | null;
}

export interface RentalMonthRow {
  month: string;
  studioRental: number;
  teacherRentals: number;
  total: number;
}

export interface RentalAnnualRow {
  year: number;
  studioRental: number;
  teacherRentals: number;
  total: number;
}

export interface RentalRevenueData {
  monthly: RentalMonthRow[];
  annual: RentalAnnualRow[];
}

export interface AnnualRevenueBreakdown {
  year: number;
  segments: Array<{ segment: string; gross: number; net: number }>;
  totalGross: number;
  totalNet: number;
}

export interface SpaServiceRow {
  category: string;
  totalRevenue: number;
  totalNetRevenue: number;
}

export interface SpaVisitFrequency {
  bucket: string;
  customers: number;
}

export interface SpaCrossover {
  total: number;
  alsoTakeClasses: number;
  spaOnly: number;
}

export interface SpaSubscriberOverlap {
  total: number;
  areSubscribers: number;
  notSubscribers: number;
}

export interface SpaSubscriberPlan {
  planName: string;
  customers: number;
}

export interface SpaMonthlyVisits {
  month: string;
  visits: number;
  uniqueVisitors: number;
}

export interface SpaCustomerBehavior {
  uniqueCustomers: number;
  frequency: SpaVisitFrequency[];
  crossover: SpaCrossover;
  subscriberOverlap: SpaSubscriberOverlap;
  subscriberPlans: SpaSubscriberPlan[];
  monthlyVisits: SpaMonthlyVisits[];
}

export interface SpaData {
  mtdRevenue: number;
  avgMonthlyRevenue: number;
  totalRevenue: number;
  monthlyRevenue: Array<{ month: string; gross: number; net: number }>;
  serviceBreakdown: SpaServiceRow[];
  customerBehavior: SpaCustomerBehavior | null;
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
  /** MEMBER-only: churn rate using only monthly (eligible) subscribers as denominator */
  eligibleChurnRate?: number;
  // MEMBER-only: per-billing-type rates and MRR
  annualUserChurnRate?: number;
  annualActiveMrrAtStart?: number;
  annualCanceledMrr?: number;
  annualMrrChurnRate?: number;
  monthlyActiveMrrAtStart?: number;
  monthlyCanceledMrr?: number;
  monthlyMrrChurnRate?: number;
}

/** Churn summary for one auto-renew category */
export interface CategoryChurnData {
  category: "MEMBER" | "SKY3" | "SKY_TING_TV";
  monthly: CategoryMonthlyChurn[];
  avgUserChurnRate: number;
  avgMrrChurnRate: number;
  atRiskCount: number;
  /** MEMBER-only: avg churn rate using only monthly (eligible) subscribers */
  avgEligibleChurnRate?: number;
  // MEMBER-only: split averages for annual vs monthly cards
  avgAnnualUserChurnRate?: number;
  avgAnnualMrrChurnRate?: number;
  avgMonthlyMrrChurnRate?: number;
  annualAtRiskCount?: number;
  monthlyAtRiskCount?: number;
  /** MEMBER-only: tenure / retention metrics */
  tenureMetrics?: TenureMetrics;
}

/** Tenure & retention metrics for a subscriber category */
export interface TenureMetrics {
  medianTenure: number;          // months
  month4RenewalRate: number;     // % of members reaching month 3 who renew at month 4
  avgPostCliffTenure: number;    // avg total tenure for members surviving past month 3
  survivalCurve: { month: number; retained: number }[];  // month 0..24+, retained as %
}

/** A member approaching their billing renewal date */
export interface RenewalAlertMember {
  name: string;
  email: string;
  planName: string;
  isAnnual: boolean;
  createdAt: string;
  renewalDate: string;
  daysUntilRenewal: number;
  tenureMonths: number;
}

/** A member approaching a critical tenure milestone */
export interface TenureMilestoneMember {
  name: string;
  email: string;
  planName: string;
  isAnnual: boolean;
  createdAt: string;
  tenureMonths: number;
  milestone: string;
}

/** Actionable member alerts for the Retention section */
export interface MemberAlerts {
  renewalApproaching: RenewalAlertMember[];
  tenureMilestones: TenureMilestoneMember[];
}

/** A subscriber in an at-risk plan state */
export interface AtRiskMember {
  name: string;
  email: string;
  planName: string;
  category: string;
  planState: string;
  createdAt: string;
  tenureMonths: number;
}

/** At-risk subscribers grouped by plan state */
export interface AtRiskByState {
  pastDue: AtRiskMember[];
  invalid: AtRiskMember[];
  pendingCancel: AtRiskMember[];
}

export interface ChurnRateData {
  /** Per-category churn data */
  byCategory: {
    member: CategoryChurnData;
    sky3: CategoryChurnData;
    skyTingTv: CategoryChurnData;
  };
  totalAtRisk: number;
  atRiskByState?: AtRiskByState;
  /** Member-specific alerts: renewals approaching + tenure milestones */
  memberAlerts?: MemberAlerts;
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
  gross: number;
  net: number;
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

// ── Expiring Intro Week types ─────────────────────────────

/** A single intro-week customer whose intro period is expiring soon */
export interface ExpiringIntroCustomer {
  firstName: string;
  lastName: string;
  email: string;
  introStartDate: string;   // YYYY-MM-DD (first attended class)
  introEndDate: string;      // YYYY-MM-DD (startDate + 6 days)
  classesAttended: number;   // classes attended during the 7-day window
  daysUntilExpiry: number;   // 0 = expires today, 1 = expires tomorrow
}

export interface ExpiringIntroWeekData {
  customers: ExpiringIntroCustomer[];
}

// ── Intro Week Conversion Funnel types ────────────────────

export interface IntroWeekNonConverter {
  name: string;
  email: string;
  introStart: string;   // YYYY-MM-DD
  introEnd: string;      // YYYY-MM-DD (start + 7 days)
  classesAttended: number;
}

export interface IntroWeekConversionData {
  totalExpired: number;
  converted: number;
  notConverted: number;
  conversionRate: number;        // converted / totalExpired * 100
  nonConverters: IntroWeekNonConverter[];
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

// ── Overview types ─────────────────────────────────────────

/** Metrics for a single time window (Yesterday, Last Week, This Month, Last Month) */
export interface TimeWindowMetrics {
  label: string;        // "Yesterday", "Last Week", etc.
  sublabel: string;     // "Mon, Feb 23", "Feb 17 - Feb 23"
  startDate: string;    // YYYY-MM-DD
  endDate: string;      // YYYY-MM-DD (exclusive)
  subscriptions: {
    member:    { new: number; churned: number };
    sky3:      { new: number; churned: number };
    skyTingTv: { new: number; churned: number };
  };
  activity: {
    dropIns: number;
    introWeeks: number;
    guests: number;
  };
  revenue: {
    merch: number;
  };
}

/** Current active auto-renew subscriber counts by tier */
export interface CurrentActiveSubscribers {
  member: number;
  sky3: number;
  skyTingTv: number;
}

/** Full overview data: all 5 time windows + current active counts */
export interface OverviewData {
  yesterday: TimeWindowMetrics;
  thisWeek: TimeWindowMetrics;
  lastWeek: TimeWindowMetrics;
  thisMonth: TimeWindowMetrics;
  lastMonth: TimeWindowMetrics;
  currentActive: CurrentActiveSubscribers;
}

// ── Trends aggregate ───────────────────────────────────────

// ── Insights types ────────────────────────────────────────

export interface InsightRow {
  id: number;
  detector: string;
  headline: string;
  explanation: string | null;
  category: "conversion" | "churn" | "revenue" | "growth";
  severity: "critical" | "warning" | "info" | "positive";
  metricValue: number | null;
  metricContext: Record<string, unknown> | null;
  detectedAt: string;
  pipelineRunId: number | null;
  dismissed: boolean;
}

// ── Usage frequency segments ──────────────────────────────

export interface UsageSegment {
  name: string;        // "Dormant", "Casual", etc.
  rangeLabel: string;  // "0/mo", "1-2/mo", etc.
  count: number;
  percent: number;
  color: string;
}

export interface UsageCategoryData {
  category: string;              // "MEMBER" | "SKY3" | "SKY_TING_TV"
  label: string;                 // "Members" | "Sky3" | "Sky Ting TV"
  totalActive: number;
  withVisits: number;
  dormant: number;
  segments: UsageSegment[];
  median: number;                // median avg visits/mo
  mean: number;                  // mean avg visits/mo
}

export interface UsageData {
  categories: UsageCategoryData[];
  upgradeOpportunities: number;  // Sky3 members visiting 3+/mo
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
  introWeekConversion: IntroWeekConversionData | null;
  usage: UsageData | null;
  expiringIntroWeeks: ExpiringIntroWeekData | null;
}
