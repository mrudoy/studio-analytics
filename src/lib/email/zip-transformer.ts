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
} from "./zip-schemas";

import type { AutoRenewRow } from "../db/auto-renew-store";
import type { OrderRow } from "../db/order-store";
import type { RegistrationRow } from "../db/registration-store";
import type { CustomerRow } from "../db/customer-store";

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

  constructor(tables: RawZipTables) {
    this.membershipById = new Map(tables.memberships.map((m) => [m.id, m]));
    this.passById = new Map(tables.passes.map((p) => [p.id, p]));
    this.performanceById = new Map(tables.performances.map((p) => [p.id, p]));
    this.eventById = new Map(tables.events.map((e) => [e.id, e]));
    this.locationById = new Map(tables.locations.map((l) => [l.id, l]));

    console.log(
      `[zip-transformer] Loaded lookup maps: ` +
        `${this.membershipById.size} memberships, ` +
        `${this.passById.size} passes, ` +
        `${this.performanceById.size} performances, ` +
        `${this.eventById.size} events, ` +
        `${this.locationById.size} locations`
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
}
