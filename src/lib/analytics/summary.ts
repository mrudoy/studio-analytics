import type { AutoRenew, Order } from "@/types/union-data";
import { getCategory, isAnnualPlan } from "./categories";
import { parseDate, getMonthKey } from "./date-utils";

/**
 * Only count auto-renews in these states for active subscriber totals and MRR.
 * "Valid Now" = actively billing, "Paused" = on hold but still a subscriber.
 * "In Trial" / "Trialing" = trial period, counted as active (matches Union.fit behavior).
 * Excluded: "Pending Cancellation", "Past Due"
 */
const COUNTABLE_STATES = new Set(["valid now", "paused", "in trial", "trialing"]);

export interface SummaryKPIs {
  mrrMember: number;
  mrrSky3: number;
  mrrSkyTingTv: number;
  mrrUnknown: number;
  mrrTotal: number;
  activeMembers: number;
  activeSky3: number;
  activeSkyTingTv: number;
  activeUnknown: number;
  activeTotal: number;
  arpuMember: number;
  arpuSky3: number;
  arpuSkyTingTv: number;
  arpuOverall: number;
  /** Map of unrecognized plan names → count of subscribers with that plan */
  unknownPlanNames: Record<string, number>;
  /** Map of states that were skipped (not in COUNTABLE_STATES) → count */
  skippedStates: Record<string, number>;
  /** Actual revenue from orders for the current calendar month */
  currentMonthRevenue: number;
  /** Actual revenue from orders for the previous calendar month */
  previousMonthRevenue: number;
}

export function computeSummary(activeAutoRenews: AutoRenew[], orders: Order[] = []): SummaryKPIs {
  let mrrMember = 0, mrrSky3 = 0, mrrSkyTingTv = 0, mrrUnknown = 0;
  const unknownPlanNames: Record<string, number> = {};
  const skippedStates: Record<string, number> = {};
  let annualPlanCount = 0;

  // Deduplicate by email per category — one person counts once per category
  // even if they appear in multiple CSV files (active + paused + trialing).
  const seenMember = new Set<string>();
  const seenSky3 = new Set<string>();
  const seenTv = new Set<string>();
  const seenUnknown = new Set<string>();

  for (const ar of activeAutoRenews) {
    // Only count these states; skip Pending Cancel, Past Due, etc.
    const normalizedState = ar.state.toLowerCase().trim();
    if (!COUNTABLE_STATES.has(normalizedState)) {
      skippedStates[normalizedState] = (skippedStates[normalizedState] || 0) + 1;
      continue;
    }

    const cat = getCategory(ar.name);
    const annual = isAnnualPlan(ar.name);
    const monthlyPrice = annual ? ar.price / 12 : ar.price;
    if (annual) annualPlanCount++;

    // Use email for deduplication; fall back to customer name if email missing
    const key = (ar.email || ar.customer || "").toLowerCase().trim();

    switch (cat) {
      case "MEMBER":
        mrrMember += monthlyPrice;
        seenMember.add(key);
        break;
      case "SKY3":
        mrrSky3 += monthlyPrice;
        seenSky3.add(key);
        break;
      case "SKY_TING_TV":
        mrrSkyTingTv += monthlyPrice;
        seenTv.add(key);
        break;
      default:
        mrrUnknown += monthlyPrice;
        seenUnknown.add(key);
        unknownPlanNames[ar.name] = (unknownPlanNames[ar.name] || 0) + 1;
        break;
    }
  }

  const activeMembers = seenMember.size;
  const activeSky3 = seenSky3.size;
  const activeSkyTingTv = seenTv.size;
  const activeUnknown = seenUnknown.size;

  // Log filtered-out states for visibility
  if (Object.keys(skippedStates).length > 0) {
    const total = Object.values(skippedStates).reduce((a, b) => a + b, 0);
    console.log(`[summary] Filtered out ${total} auto-renews by state: ${JSON.stringify(skippedStates)}`);
  }

  // Log unknown plan names for debugging
  if (activeUnknown > 0) {
    console.log(`[summary] ${activeUnknown} active auto-renews have UNKNOWN category:`);
    const sorted = Object.entries(unknownPlanNames).sort((a, b) => b[1] - a[1]);
    for (const [name, count] of sorted) {
      console.log(`  "${name}" × ${count}`);
    }
  }

  // Log annual plan detection
  if (annualPlanCount > 0) {
    console.log(`[summary] ${annualPlanCount} annual plans detected — prices divided by 12 for MRR`);
  }

  const mrrTotal = mrrMember + mrrSky3 + mrrSkyTingTv + mrrUnknown;
  const activeTotal = activeMembers + activeSky3 + activeSkyTingTv + activeUnknown;

  // Compute actual monthly revenue from orders
  const now = new Date();
  const currentMonthKey = getMonthKey(now);
  const prevMonth = new Date(now.getFullYear(), now.getMonth() - 1, 1);
  const prevMonthKey = getMonthKey(prevMonth);

  let currentMonthRevenue = 0;
  let previousMonthRevenue = 0;
  for (const order of orders) {
    const date = parseDate(order.created || "");
    if (!date || order.total <= 0) continue;
    const mk = getMonthKey(date);
    if (mk === currentMonthKey) currentMonthRevenue += order.total;
    else if (mk === prevMonthKey) previousMonthRevenue += order.total;
  }

  return {
    mrrMember: Math.round(mrrMember * 100) / 100,
    mrrSky3: Math.round(mrrSky3 * 100) / 100,
    mrrSkyTingTv: Math.round(mrrSkyTingTv * 100) / 100,
    mrrUnknown: Math.round(mrrUnknown * 100) / 100,
    mrrTotal: Math.round(mrrTotal * 100) / 100,
    activeMembers,
    activeSky3,
    activeSkyTingTv,
    activeUnknown,
    activeTotal,
    arpuMember: activeMembers > 0 ? Math.round((mrrMember / activeMembers) * 100) / 100 : 0,
    arpuSky3: activeSky3 > 0 ? Math.round((mrrSky3 / activeSky3) * 100) / 100 : 0,
    arpuSkyTingTv: activeSkyTingTv > 0 ? Math.round((mrrSkyTingTv / activeSkyTingTv) * 100) / 100 : 0,
    arpuOverall: activeTotal > 0 ? Math.round((mrrTotal / activeTotal) * 100) / 100 : 0,
    unknownPlanNames,
    skippedStates,
    currentMonthRevenue: Math.round(currentMonthRevenue * 100) / 100,
    previousMonthRevenue: Math.round(previousMonthRevenue * 100) / 100,
  };
}
