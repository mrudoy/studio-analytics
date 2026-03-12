/**
 * DB-Based Revenue Recomputation — Sean's 7-Step Algorithm
 *
 * Implements the exact revenue computation algorithm from Union.fit CEO's
 * spec ("Recreating the Revenue Report from Data Export CSVs") using
 * accumulated DB data instead of CSV exports.
 *
 * The 7 steps:
 *   1. Choose date range (month boundaries)
 *   2. Non-subscription pass revenue (uses pass.total, NOT order.total)
 *   3. Non-pass order revenue (excludes pass orders from Step 2)
 *   4. Refunds:
 *      A) Pass-linked refunds (uses pass.total, NOT refund.amount_refunded)
 *      B) Non-pass refunds (uses refund.amount_refunded)
 *   5. Transfers → "Uncategorized" as other_fees
 *   6. Combine per category, compute net revenue
 *   7. Sort alphabetically, save
 *
 * Tables used: passes, orders, refunds, transfers, pass_type_lookups, revenue_category_lookups
 */

import { getPool } from "../db/database";
import { saveRevenueCategories, isMonthLocked } from "../db/revenue-store";
import { loadPassTypeLookups, loadRevenueCategoryLookups } from "../db/lookup-store";
import type { CachedPassType, CachedRevenueCategory } from "../db/lookup-store";
import { inferCategoryFromName } from "./category-utils";
import type { RevenueCategory } from "@/types/union-data";

interface RevenueBucket {
  revenue: number;
  unionFees: number;
  stripeFees: number;
  otherFees: number;
  refunded: number;
  unionFeesRefunded: number;
}

/**
 * Resolve a revenue category name from a pass_type_id using the lookup chain:
 *   pass_type_id → pass_type_lookups.revenue_category_id → revenue_category_lookups.name
 *
 * Falls back to inferCategoryFromName() on the pass/order name.
 * Returns "Uncategorized" if nothing matches.
 */
function resolveCategory(
  passTypeId: string | null | undefined,
  fallbackName: string | null | undefined,
  passTypeLookups: Map<string, CachedPassType>,
  revCatLookups: Map<string, CachedRevenueCategory>
): string {
  if (passTypeId) {
    const pt = passTypeLookups.get(passTypeId);
    if (pt?.revenueCategoryId) {
      const rc = revCatLookups.get(pt.revenueCategoryId);
      if (rc?.name) return rc.name;
    }
  }
  if (fallbackName) {
    return inferCategoryFromName(fallbackName) || "Uncategorized";
  }
  return "Uncategorized";
}

/**
 * Recompute revenue for a specific month from accumulated DB data,
 * following Sean's exact 7-step algorithm.
 *
 * @param monthStr - "YYYY-MM" format
 * @returns true if revenue was saved, false if skipped (locked or no data)
 */
export async function recomputeMonthRevenue(monthStr: string): Promise<boolean> {
  const pool = getPool();
  const year = parseInt(monthStr.slice(0, 4));
  const month = parseInt(monthStr.slice(5, 7));

  // Check if this month is locked (manually uploaded data takes priority)
  const locked = await isMonthLocked(year, month);
  if (locked) {
    console.log(`[db-revenue] Skipping locked month ${monthStr}`);
    return false;
  }

  // Load lookup tables once for category resolution
  const passTypeLookups = await loadPassTypeLookups();
  const revCatLookups = await loadRevenueCategoryLookups();

  // Revenue buckets per category
  const buckets = new Map<string, RevenueBucket>();

  const getOrCreate = (category: string): RevenueBucket => {
    if (!buckets.has(category)) {
      buckets.set(category, {
        revenue: 0,
        unionFees: 0,
        stripeFees: 0,
        otherFees: 0,
        refunded: 0,
        unionFeesRefunded: 0,
      });
    }
    return buckets.get(category)!;
  };

  // Stats for logging
  let passRevCount = 0;
  let orderRevCount = 0;
  let uncategorizedCount = 0;

  // ══════════════════════════════════════════════════════════════
  // Step 2: Non-subscription pass revenue
  //
  // Per Sean's spec: "Keep only passes where pass_category_name is NOT Subscription"
  // Uses pass.total for revenue (NOT order.total)
  // Joins to orders for date filtering (orders.completed_at in month)
  // ══════════════════════════════════════════════════════════════

  const { rows: passRows } = await pool.query(
    `SELECT p.id, p.order_id, p.total, p.fee_union_total, p.fee_payment_total,
            p.fees_outside, p.pass_type_id, p.name AS pass_name
     FROM passes p
     JOIN orders o ON p.order_id = o.union_order_id
     WHERE LOWER(p.pass_category_name) != 'subscription'
       AND p.total > 0
       AND p.order_id IS NOT NULL AND p.order_id != ''
       AND TO_CHAR(COALESCE(o.completed_at, o.created_at), 'YYYY-MM') = $1
       AND (o.state IS NULL OR LOWER(o.state) IN ('completed', 'refunded'))`,
    [monthStr]
  );

  // Collect pass order IDs to exclude from Step 3
  const passOrderIds = new Set<string>();

  for (const row of passRows) {
    passOrderIds.add(row.order_id as string);

    const cat = resolveCategory(
      row.pass_type_id as string,
      row.pass_name as string,
      passTypeLookups,
      revCatLookups
    );

    if (cat === "Uncategorized") uncategorizedCount++;
    else passRevCount++;

    const bucket = getOrCreate(cat);
    bucket.revenue += Number(row.total) || 0;

    // Respect fees_outside flag: if customer pays fees, don't deduct from org revenue
    if (!row.fees_outside) {
      bucket.unionFees += Number(row.fee_union_total) || 0;
      bucket.stripeFees += Number(row.fee_payment_total) || 0;
    }
  }

  // ══════════════════════════════════════════════════════════════
  // Step 3: Non-pass orders (subscriptions, registrations, other)
  //
  // Per Sean's spec: "Exclude any order whose id is in the pass order IDs set"
  // Uses order.total for revenue
  // Category via subscription_pass_id → pass → passType → revenueCategoryId
  // No subscription_pass_id → "Uncategorized"
  // ══════════════════════════════════════════════════════════════

  // LEFT JOIN to passes for subscription_pass_id category resolution in a single query
  const { rows: orderRows } = await pool.query(
    `SELECT o.union_order_id, o.total, o.fee_union_total, o.fee_payment_total,
            o.fees_outside, o.subscription_pass_id, o.revenue_category,
            sp.pass_type_id AS sub_pass_type_id, sp.name AS sub_pass_name
     FROM orders o
     LEFT JOIN passes sp ON o.subscription_pass_id = sp.id
     WHERE TO_CHAR(COALESCE(o.completed_at, o.created_at), 'YYYY-MM') = $1
       AND (o.state IS NULL OR LOWER(o.state) IN ('completed', 'refunded'))
       AND o.total > 0`,
    [monthStr]
  );

  for (const row of orderRows) {
    // Skip orders already counted in pass revenue (Step 2)
    if (passOrderIds.has(row.union_order_id as string)) continue;

    // Resolve category via subscription_pass_id lookup chain
    let cat = "Uncategorized";

    if (row.subscription_pass_id) {
      if (row.sub_pass_type_id) {
        // Subscription pass found in DB — resolve via lookup chain
        cat = resolveCategory(
          row.sub_pass_type_id as string,
          row.sub_pass_name as string,
          passTypeLookups,
          revCatLookups
        );
      } else {
        // Pass not in DB — fall back to pre-computed revenue_category from order import
        cat = (row.revenue_category as string) || "Uncategorized";
      }
    } else {
      // No subscription_pass_id → per Sean's spec, this is "Uncategorized"
      // But use pre-computed revenue_category if available (import-time resolution)
      cat = (row.revenue_category as string) || "Uncategorized";
    }

    if (cat === "Uncategorized") uncategorizedCount++;
    else orderRevCount++;

    const bucket = getOrCreate(cat);
    bucket.revenue += Number(row.total) || 0;

    if (!row.fees_outside) {
      bucket.unionFees += Number(row.fee_union_total) || 0;
      bucket.stripeFees += Number(row.fee_payment_total) || 0;
    }
  }

  // ══════════════════════════════════════════════════════════════
  // Step 4A: Pass-linked refunds
  //
  // Per Sean's spec: "Use pass.total for refund amount (not refund.amount_refunded)"
  // Passes with refund_id, joined to refunds for date filtering
  // ══════════════════════════════════════════════════════════════

  const { rows: passRefundRows } = await pool.query(
    `SELECT p.total AS pass_total, p.fee_union_total AS pass_fee_union,
            p.pass_type_id, p.name AS pass_name, r.id AS refund_id
     FROM passes p
     JOIN refunds r ON p.refund_id = r.id
     WHERE TO_CHAR(r.created_at, 'YYYY-MM') = $1
       AND NOT r.to_balance
       AND p.refund_id IS NOT NULL AND p.refund_id != ''`,
    [monthStr]
  );

  // Collect pass-linked refund IDs to exclude from Step 4B
  const passRefundIds = new Set<string>();
  let refundsPassApplied = 0;

  for (const row of passRefundRows) {
    passRefundIds.add(row.refund_id as string);

    const cat = resolveCategory(
      row.pass_type_id as string,
      row.pass_name as string,
      passTypeLookups,
      revCatLookups
    );

    const bucket = getOrCreate(cat);
    // Use pass.total for refund amount per Sean's spec
    bucket.refunded += Math.abs(Number(row.pass_total) || 0);
    // Fees are returned on refund (positive — offsets the negative fee deduction)
    bucket.unionFeesRefunded += Math.abs(Number(row.pass_fee_union) || 0);
    refundsPassApplied++;
  }

  // ══════════════════════════════════════════════════════════════
  // Step 4B: Non-pass refunds
  //
  // Per Sean's spec: "Use refund.amount_refunded"
  // Category via refunds.revenue_category_id → revenue_category_lookups
  // ══════════════════════════════════════════════════════════════

  const { rows: refundRows } = await pool.query(
    `SELECT r.id, r.revenue_category, r.revenue_category_id,
            r.amount_refunded, r.fee_union_total_refunded
     FROM refunds r
     WHERE TO_CHAR(r.created_at, 'YYYY-MM') = $1
       AND NOT r.to_balance`,
    [monthStr]
  );

  let refundsNonPassApplied = 0;

  for (const row of refundRows) {
    // Skip refunds already handled in Step 4A
    if (passRefundIds.has(row.id as string)) continue;

    // Resolve category from revenue_category_id lookup
    let cat = "Uncategorized";
    if (row.revenue_category_id) {
      const rc = revCatLookups.get(row.revenue_category_id as string);
      if (rc?.name) cat = rc.name;
    }
    // Fall back to pre-resolved revenue_category column
    if (cat === "Uncategorized" && row.revenue_category) {
      cat = row.revenue_category as string;
    }

    const bucket = getOrCreate(cat);
    bucket.refunded += Math.abs(Number(row.amount_refunded) || 0);
    bucket.unionFeesRefunded += Math.abs(Number(row.fee_union_total_refunded) || 0);
    refundsNonPassApplied++;
  }

  // ══════════════════════════════════════════════════════════════
  // Step 5: Transfers → "Uncategorized" as other_fees
  //
  // Per Sean's spec: "Transfers are only added to the Uncategorized category"
  // ══════════════════════════════════════════════════════════════

  const { rows: transferRows } = await pool.query(
    `SELECT ABS(payout_total) as amount
     FROM transfers
     WHERE TO_CHAR(created_at, 'YYYY-MM') = $1`,
    [monthStr]
  );

  for (const row of transferRows) {
    const bucket = getOrCreate("Uncategorized");
    bucket.otherFees += Number(row.amount) || 0;
  }

  // ══════════════════════════════════════════════════════════════
  // Step 6: Build RevenueCategory[] and compute net revenue
  // ══════════════════════════════════════════════════════════════

  if (buckets.size === 0) {
    console.log(`[db-revenue] No data found for ${monthStr}`);
    return false;
  }

  const categories: RevenueCategory[] = [];
  for (const [name, totals] of buckets.entries()) {
    // Net = Revenue - UnionFees - StripeFees - OtherFees - Refunded + UnionFeesRefunded
    const netRevenue =
      totals.revenue -
      totals.unionFees -
      totals.stripeFees -
      totals.otherFees -
      totals.refunded +
      totals.unionFeesRefunded;

    categories.push({
      revenueCategory: name,
      revenue: Math.round(totals.revenue * 100) / 100,
      unionFees: Math.round(totals.unionFees * 100) / 100,
      stripeFees: Math.round(totals.stripeFees * 100) / 100,
      otherFees: Math.round(totals.otherFees * 100) / 100,
      transfers: 0,
      refunded: Math.round(totals.refunded * 100) / 100,
      unionFeesRefunded: Math.round(totals.unionFeesRefunded * 100) / 100,
      netRevenue: Math.round(netRevenue * 100) / 100,
    });
  }

  // ══════════════════════════════════════════════════════════════
  // Step 7: Sort alphabetically and save
  // ══════════════════════════════════════════════════════════════

  categories.sort((a, b) => a.revenueCategory.localeCompare(b.revenueCategory));

  const lastDay = new Date(year, month, 0).getDate();
  const periodStart = `${monthStr}-01`;
  const periodEnd = `${monthStr}-${String(lastDay).padStart(2, "0")}`;

  await saveRevenueCategories(periodStart, periodEnd, categories);

  const totalGross = categories.reduce((s, c) => s + c.revenue, 0);
  const totalNet = categories.reduce((s, c) => s + c.netRevenue, 0);
  console.log(
    `[db-revenue] ${monthStr}: ${categories.length} categories, ` +
      `$${Math.round(totalGross).toLocaleString()} gross, ` +
      `$${Math.round(totalNet).toLocaleString()} net ` +
      `(${passRevCount} pass items, ${orderRevCount} order items, ` +
      `${uncategorizedCount} uncategorized, ` +
      `${refundsPassApplied} pass refunds, ${refundsNonPassApplied} non-pass refunds, ` +
      `${transferRows.length} transfers)`
  );

  return true;
}

/**
 * Recompute revenue for current month + previous month.
 * Called after each pipeline run to ensure both months are up to date.
 * Previous month is included because daily exports near month boundaries
 * may contain orders dated to the prior month.
 */
export async function recomputeRevenueFromDB(): Promise<void> {
  const now = new Date();
  const currentMonth = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, "0")}`;

  // Previous month
  const prevDate = new Date(now.getFullYear(), now.getMonth() - 1, 1);
  const previousMonth = `${prevDate.getFullYear()}-${String(prevDate.getMonth() + 1).padStart(2, "0")}`;

  console.log(`[db-revenue] Recomputing revenue for ${previousMonth} and ${currentMonth}...`);

  await recomputeMonthRevenue(previousMonth);
  await recomputeMonthRevenue(currentMonth);
}
