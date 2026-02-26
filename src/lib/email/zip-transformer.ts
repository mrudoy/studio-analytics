/**
 * ZipTransformer — joins Union.fit raw relational CSVs into denormalized
 * rows matching our existing DB store interfaces.
 *
 * The daily zip export contains normalized tables with foreign-key IDs.
 * This class loads small lookup tables into memory as Maps, then transforms
 * the large tables (orders, registrations) into the flat row shapes our
 * store functions expect.
 *
 * Usage:
 *   const t = new ZipTransformer(tables);
 *   const autoRenews = t.transformAutoRenews();
 *   const customers  = t.transformCustomers();
 *   // For large tables, iterate transformOrdersBatch / transformRegistrationsBatch
 */

import type {
  RawMembership,
  RawPass,
  RawOrder,
  RawRegistration,
  RawPerformance,
  RawEvent,
  RawLocation,
  RawPassType,
  RawRevenueCategoryLookup,
  RawRefund,
} from "./zip-schemas";

import type { AutoRenewRow } from "../db/auto-renew-store";
import type { OrderRow } from "../db/order-store";
import type { RegistrationRow } from "../db/registration-store";
import type { CustomerRow } from "../db/customer-store";
import type { RevenueCategory } from "@/types/union-data";

// ── Extended row types with Union ID fields ─────────────────

export interface ZipAutoRenewRow extends AutoRenewRow {
  unionPassId: string;
}

export interface ZipOrderRow extends OrderRow {
  unionOrderId: string;
}

export interface ZipRegistrationRow extends RegistrationRow {
  unionRegistrationId: string;
}

// ── Input tables ────────────────────────────────────────────

export interface RawZipTables {
  memberships: RawMembership[];
  passes: RawPass[];
  events: RawEvent[];
  performances: RawPerformance[];
  locations: RawLocation[];
  passTypes?: RawPassType[];
  revenueCategoryLookups?: RawRevenueCategoryLookup[];
}

// ── State mapping ───────────────────────────────────────────

/**
 * Map Union.fit raw pass states to the states our dashboard expects.
 * Admin reports use "Valid Now", "Pending Cancel", etc.
 */
const PASS_STATE_MAP: Record<string, string> = {
  Active: "Valid Now",
  Canceled: "Canceled",
  Paused: "Paused",
  Trialing: "In Trial",
  "Pending Cancel": "Pending Cancel",
};

// ── Transformer ─────────────────────────────────────────────

export class ZipTransformer {
  private membershipById: Map<string, RawMembership>;
  private passById: Map<string, RawPass>;
  private performanceById: Map<string, RawPerformance>;
  private eventById: Map<string, RawEvent>;
  private locationById: Map<string, RawLocation>;
  private passTypeById: Map<string, RawPassType>;
  private revenueCategoryById: Map<string, RawRevenueCategoryLookup>;

  constructor(tables: RawZipTables) {
    this.membershipById = new Map(tables.memberships.map((m) => [m.id, m]));
    this.passById = new Map(tables.passes.map((p) => [p.id, p]));
    this.performanceById = new Map(tables.performances.map((p) => [p.id, p]));
    this.eventById = new Map(tables.events.map((e) => [e.id, e]));
    this.locationById = new Map(tables.locations.map((l) => [l.id, l]));
    this.passTypeById = new Map((tables.passTypes ?? []).map((pt) => [pt.id, pt]));
    this.revenueCategoryById = new Map((tables.revenueCategoryLookups ?? []).map((rc) => [rc.id, rc]));

    console.log(
      `[zip-transformer] Loaded lookup maps: ` +
        `${this.membershipById.size} memberships, ` +
        `${this.passById.size} passes, ` +
        `${this.performanceById.size} performances, ` +
        `${this.eventById.size} events, ` +
        `${this.locationById.size} locations, ` +
        `${this.passTypeById.size} pass types, ` +
        `${this.revenueCategoryById.size} revenue categories`
    );
  }

  // ── Accessors for streaming transforms ────────────────────

  getMembership(id: string): RawMembership | undefined {
    return this.membershipById.get(id);
  }

  getPass(id: string): RawPass | undefined {
    return this.passById.get(id);
  }

  // ── Auto-Renews: passes → AutoRenewRow[] ──────────────────

  /**
   * Transform passes into auto-renew subscription rows.
   * Only includes passes that are auto-renewing (subscription plans).
   * Skips expired passes (no longer active subscriptions).
   */
  transformAutoRenews(): ZipAutoRenewRow[] {
    const rows: ZipAutoRenewRow[] = [];
    let skippedNoMember = 0;
    let skippedNotAutoRenew = 0;
    let skippedExpired = 0;

    for (const pass of this.passById.values()) {
      // Only auto-renew subscriptions
      if (!pass.autoRenewUnlimited && pass.autoRenewPeriodLimit <= 0) {
        skippedNotAutoRenew++;
        continue;
      }

      // Skip expired (no longer an active subscription record)
      if (pass.state === "Expired") {
        skippedExpired++;
        continue;
      }

      const membership = this.membershipById.get(pass.membershipId);
      if (!membership || !membership.email) {
        skippedNoMember++;
        continue;
      }

      const planState = PASS_STATE_MAP[pass.state] || pass.state;

      rows.push({
        planName: pass.name,
        planState,
        planPrice: pass.price,
        customerName: `${membership.firstName} ${membership.lastName}`.trim(),
        customerEmail: membership.email,
        createdAt: pass.createdAt,
        canceledAt: pass.canceledAt || undefined,
        unionPassId: pass.id,
      });
    }

    console.log(
      `[zip-transformer] Auto-renews: ${rows.length} subscriptions ` +
        `(skipped: ${skippedNotAutoRenew} not-auto-renew, ${skippedExpired} expired, ${skippedNoMember} no-member)`
    );

    return rows;
  }

  // ── Orders: batch transform ───────────────────────────────

  /**
   * Transform a batch of raw orders into OrderRow[].
   * Called per-batch during streaming parse of orders.csv.
   */
  transformOrdersBatch(rawOrders: RawOrder[]): ZipOrderRow[] {
    const rows: ZipOrderRow[] = [];

    for (const order of rawOrders) {
      const membership = this.membershipById.get(order.membershipId);
      const pass = order.paidWithPassId
        ? this.passById.get(order.paidWithPassId)
        : undefined;

      rows.push({
        created: order.createdAt,
        code: order.id, // Union.fit order ID as dedup key
        customer: membership
          ? `${membership.firstName} ${membership.lastName}`.trim()
          : "",
        email: membership?.email || "",
        type: pass?.name || order.paymentMethod || "",
        payment: order.paymentMethod || "",
        total: order.total,
        unionOrderId: order.id,
      });
    }

    return rows;
  }

  // ── Registrations: batch transform ────────────────────────

  /**
   * Transform a batch of raw registrations into RegistrationRow[].
   * Called per-batch during streaming parse of registrations.csv.
   */
  transformRegistrationsBatch(
    rawRegistrations: RawRegistration[]
  ): ZipRegistrationRow[] {
    const rows: ZipRegistrationRow[] = [];

    for (const reg of rawRegistrations) {
      const pass = reg.passId ? this.passById.get(reg.passId) : undefined;
      const membership = pass?.membershipId
        ? this.membershipById.get(pass.membershipId)
        : undefined;
      const performance = reg.performanceId
        ? this.performanceById.get(reg.performanceId)
        : undefined;
      const event = performance?.eventId
        ? this.eventById.get(performance.eventId)
        : undefined;
      const location = performance?.locationId
        ? this.locationById.get(performance.locationId)
        : undefined;

      // Look up teacher name from performance's teacher_membership_id
      const teacherMembership = performance?.teacherMembershipId
        ? this.membershipById.get(performance.teacherMembershipId)
        : undefined;
      const teacherName = teacherMembership
        ? `${teacherMembership.firstName} ${teacherMembership.lastName}`.trim()
        : "";

      // Determine if this is a subscription visit
      const isSubscription = pass
        ? pass.autoRenewUnlimited || pass.autoRenewPeriodLimit > 0
        : false;

      rows.push({
        eventName: event?.name || performance?.name || "",
        eventId: event?.id,
        performanceId: performance?.id,
        performanceStartsAt: performance?.startsAt || "",
        locationName: location?.name || "",
        teacherName,
        firstName: membership?.firstName || "",
        lastName: membership?.lastName || "",
        email: membership?.email || "",
        attendedAt: reg.attendedAt || "",
        registrationType: "",
        state: reg.state || "",
        pass: pass?.name || "",
        subscription: String(isSubscription),
        revenue: reg.revenue,
        unionRegistrationId: reg.id,
      });
    }

    return rows;
  }

  // ── Customers: memberships → CustomerRow[] ────────────────

  /**
   * Transform memberships into customer rows.
   * Filters out entries without email (can't be useful for CRM).
   */
  transformCustomers(): CustomerRow[] {
    const rows: CustomerRow[] = [];

    for (const m of this.membershipById.values()) {
      if (!m.email) continue;

      rows.push({
        name: `${m.firstName} ${m.lastName}`.trim(),
        email: m.email,
        role: m.role || "",
        orders: 0, // Could count from orders table if needed
        created: m.createdAt,
      });
    }

    console.log(
      `[zip-transformer] Customers: ${rows.length} with email ` +
        `(${this.membershipById.size - rows.length} skipped — no email)`
    );

    return rows;
  }

  // ── Revenue category resolution ─────────────────────────────

  /**
   * Resolve the revenue category name for an order.
   * Path A: order.eventId → event.revenueCategoryId → name
   * Path B: order pass → pass.passTypeId → passType.revenueCategoryId → name
   */
  private resolveRevenueCategory(order: RawOrder): string | null {
    // Path A: via event
    if (order.eventId) {
      const event = this.eventById.get(order.eventId);
      if (event?.revenueCategoryId) {
        const rc = this.revenueCategoryById.get(event.revenueCategoryId);
        if (rc?.name) return rc.name;
      }
    }

    // Path B: via pass → pass_type
    const passId = order.subscriptionPassId || order.paidWithPassId;
    if (passId) {
      const pass = this.passById.get(passId);
      if (pass?.passTypeId) {
        const passType = this.passTypeById.get(pass.passTypeId);
        if (passType?.revenueCategoryId) {
          const rc = this.revenueCategoryById.get(passType.revenueCategoryId);
          if (rc?.name) return rc.name;
        }
      }
    }

    return null;
  }

  /**
   * Extract YYYY-MM from an ISO datetime string.
   * Handles formats like "2024-01-15T10:00:00Z" and "2024-01-15".
   */
  private extractMonth(dateStr: string): string | null {
    const match = dateStr.match(/^(\d{4}-\d{2})/);
    return match ? match[1] : null;
  }

  // ── Compute revenue by category from orders + refunds ───────

  /**
   * Aggregate all orders by (month, revenue category), applying refunds.
   * Returns a Map keyed by "YYYY-MM" with RevenueCategory[] per month.
   */
  computeRevenueByCategory(
    orders: RawOrder[],
    refunds: RawRefund[]
  ): Map<string, RevenueCategory[]> {
    if (this.revenueCategoryById.size === 0) {
      console.warn("[zip-transformer] No revenue category lookups — skipping revenue computation");
      return new Map();
    }

    // Accumulator: month → category → totals
    const buckets = new Map<string, Map<string, {
      revenue: number;
      unionFees: number;
      stripeFees: number;
      otherFees: number;
      refunded: number;
      unionFeesRefunded: number;
    }>>();

    const getOrCreate = (month: string, category: string) => {
      if (!buckets.has(month)) buckets.set(month, new Map());
      const monthMap = buckets.get(month)!;
      if (!monthMap.has(category)) {
        monthMap.set(category, {
          revenue: 0, unionFees: 0, stripeFees: 0, otherFees: 0,
          refunded: 0, unionFeesRefunded: 0,
        });
      }
      return monthMap.get(category)!;
    };

    // Build order lookup for refund resolution
    const orderById = new Map<string, RawOrder>();
    for (const o of orders) orderById.set(o.id, o);

    let categorized = 0;
    let uncategorized = 0;
    let skippedState = 0;
    let skippedNoDate = 0;
    const sampleUncategorized: string[] = [];

    // ── Process orders ──────────────────────────────────────
    for (const order of orders) {
      // Only completed or refunded orders represent real revenue
      const state = order.state.toLowerCase();
      if (state !== "completed" && state !== "refunded") {
        skippedState++;
        continue;
      }

      const dateStr = order.completedAt || order.createdAt;
      const month = dateStr ? this.extractMonth(dateStr) : null;
      if (!month) {
        skippedNoDate++;
        continue;
      }

      const categoryName = this.resolveRevenueCategory(order);
      if (!categoryName) {
        uncategorized++;
        if (sampleUncategorized.length < 5) sampleUncategorized.push(order.id);
        // Still count under "Uncategorized"
        const bucket = getOrCreate(month, "Uncategorized");
        bucket.revenue += order.total;
        bucket.unionFees += order.feeUnionTotal;
        bucket.stripeFees += order.feePaymentTotal;
        bucket.otherFees += order.feeOutsideTotal;
        continue;
      }

      categorized++;
      const bucket = getOrCreate(month, categoryName);
      bucket.revenue += order.total;
      bucket.unionFees += order.feeUnionTotal;
      bucket.stripeFees += order.feePaymentTotal;
      bucket.otherFees += order.feeOutsideTotal;
    }

    // ── Process refunds ─────────────────────────────────────
    let refundsApplied = 0;
    let refundsSkipped = 0;

    for (const refund of refunds) {
      const state = refund.state?.toLowerCase() || "";
      if (state !== "refunded" && state !== "completed" && state !== "") {
        refundsSkipped++;
        continue;
      }

      const dateStr = refund.createdAt;
      const month = dateStr ? this.extractMonth(dateStr) : null;
      if (!month) {
        refundsSkipped++;
        continue;
      }

      // Resolve category: try via refund's own revenueCategoryId, else via order
      let categoryName: string | null = null;
      if (refund.revenueCategoryId) {
        const rc = this.revenueCategoryById.get(refund.revenueCategoryId);
        if (rc?.name) categoryName = rc.name;
      }
      if (!categoryName && refund.orderId) {
        const order = orderById.get(refund.orderId);
        if (order) categoryName = this.resolveRevenueCategory(order);
      }

      if (!categoryName) {
        refundsSkipped++;
        continue;
      }

      // amountRefunded from Union.fit is negative (e.g. "$-199.00")
      // Store as positive in the `refunded` bucket
      const refundAmount = Math.abs(refund.amountRefunded);
      const refundUnionFees = Math.abs(refund.feeUnionTotalRefunded);

      const bucket = getOrCreate(month, categoryName);
      bucket.refunded += refundAmount;
      bucket.unionFeesRefunded += refundUnionFees;
      refundsApplied++;
    }

    // ── Build output ────────────────────────────────────────
    const result = new Map<string, RevenueCategory[]>();

    for (const [month, categoryMap] of buckets.entries()) {
      const categories: RevenueCategory[] = [];
      for (const [name, totals] of categoryMap.entries()) {
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
      // Sort by revenue desc
      categories.sort((a, b) => b.revenue - a.revenue);
      result.set(month, categories);
    }

    const uncatPct = orders.length > 0
      ? ((uncategorized / (categorized + uncategorized)) * 100).toFixed(1)
      : "0";

    console.log(
      `[zip-transformer] Revenue categories: ${categorized} orders categorized, ` +
        `${uncategorized} uncategorized (${uncatPct}%), ` +
        `${skippedState} skipped (wrong state), ${skippedNoDate} skipped (no date). ` +
        `${refundsApplied} refunds applied, ${refundsSkipped} refunds skipped. ` +
        `${result.size} months produced.`
    );

    if (sampleUncategorized.length > 0) {
      console.log(
        `[zip-transformer] Sample uncategorized order IDs: ${sampleUncategorized.join(", ")}`
      );
    }

    return result;
  }
}
