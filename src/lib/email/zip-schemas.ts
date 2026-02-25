/**
 * Zod schemas for the Union.fit raw relational database export CSVs.
 *
 * These are different from the admin-report schemas in parser/schemas.ts.
 * The raw export uses snake_case columns (normalized to camelCase by PapaParse)
 * and contains foreign-key IDs instead of denormalized email/name fields.
 *
 * Key tables:
 *   memberships.csv — customers (has email/name)
 *   passes.csv      — subscriptions & passes (has membership_id FK)
 *   orders.csv      — purchases (has membership_id FK)
 *   registrations.csv — class attendance (has pass_id FK)
 *   performances.csv — class schedule (has event_id FK)
 *   events.csv      — class/event definitions
 *   locations.csv   — studio locations
 */

import { z } from "zod";

// ── Helpers ──────────────────────────────────────────────────

const money = z
  .union([z.string(), z.number()])
  .transform((v) => {
    if (typeof v === "number") return v;
    return parseFloat(String(v).replace(/[$,]/g, "")) || 0;
  })
  .default(0);

const boolField = z
  .union([z.string(), z.boolean()])
  .transform((v) => {
    if (typeof v === "boolean") return v;
    return v.trim().toLowerCase() === "true";
  })
  .default(false);

const optStr = z.string().default("");

// ── Memberships (customers) ─────────────────────────────────

export const RawMembershipSchema = z.object({
  id: z.string(),
  createdAt: optStr,
  updatedAt: optStr,
  role: optStr,
  firstName: optStr,
  lastName: optStr,
  email: optStr,
  phone: optStr,
  howDidYouHearAboutUs_: optStr,
});

export type RawMembership = z.infer<typeof RawMembershipSchema>;

// ── Passes (subscriptions + pass packages) ──────────────────

export const RawPassSchema = z.object({
  id: z.string(),
  createdAt: optStr,
  updatedAt: optStr,
  deletedAt: optStr,
  canceledAt: optStr,
  pendingCanceledAt: optStr,
  completedAt: optStr,
  expiredAt: optStr,
  passTypeId: optStr,
  eventId: optStr,
  membershipId: optStr,
  orderId: optStr,
  refundId: optStr,
  locationId: optStr,
  passCategoryName: optStr,
  name: optStr,
  state: optStr, // Active, Canceled, Expired, Paused, Trialing, etc.
  price: money,
  discount: money,
  total: money,
  feePaymentTotal: money,
  feeUnionTotal: money,
  donation: optStr,
  feesOutside: optStr,
  unlimited: boolField,
  redemptions: boolField,
  validFor: z.coerce.number().default(0),
  validForPeriod: optStr,
  validStartsAt: optStr,
  validEndsAt: optStr,
  autoRenewUnlimited: boolField,
  autoRenewPeriodLimit: z.coerce.number().default(0),
  paymentPlan: boolField,
  giftCard: boolField,
  giftCardAmount: money,
  pausedAt: optStr,
  resumedAt: optStr,
  trialStartsAt: optStr,
  trialEndsAt: optStr,
  subscriptionPaymentBeginsAt: optStr,
  resumesAt: optStr,
  courseId: optStr,
});

export type RawPass = z.infer<typeof RawPassSchema>;

// ── Orders (purchases) ──────────────────────────────────────

export const RawOrderSchema = z.object({
  id: z.string(),
  createdAt: optStr,
  updatedAt: optStr,
  completedAt: optStr,
  membershipId: optStr,
  paidWithPassId: optStr,
  locationId: optStr,
  eventId: optStr,
  performanceId: optStr,
  videoId: optStr,
  sellerMembershipId: optStr,
  subscriptionPassId: optStr,
  paymentMethod: optStr, // comp, credit_card, etc.
  salesChannel: optStr,  // register, online, etc.
  state: optStr,         // completed, refunded, etc.
  taxTotal: money,
  total: money,
  feesOutside: optStr,
  feeOutsideTotal: money,
  feePaymentTotal: money,
  feeUnionTotal: money,
  discountsTotal: money,
  notes: optStr,
  last4: optStr,
  accountBalanceUsed: money,
  paidOutAt: optStr,
  payoutId: optStr,
});

export type RawOrder = z.infer<typeof RawOrderSchema>;

// ── Registrations (class attendance) ────────────────────────

export const RawRegistrationSchema = z.object({
  id: z.string(),
  createdAt: optStr,
  updatedAt: optStr,
  attendedAt: optStr,
  canceledAt: optStr,
  performanceId: optStr,
  videoId: optStr,
  passId: optStr,
  locationId: optStr,
  registrationTypeId: optStr,
  state: optStr, // issued, redeemed, confirmed, canceled, etc.
  watchedSeconds: optStr,
  watchedPercentage: optStr,
  selfCheckin: boolField,
  revenueState: optStr,
  revenueLastUpdatedAt: optStr,
  revenue: money,
});

export type RawRegistration = z.infer<typeof RawRegistrationSchema>;

// ── Performances (class schedule) ───────────────────────────

export const RawPerformanceSchema = z.object({
  id: z.string(),
  createdAt: optStr,
  startsAt: optStr,
  endsAt: optStr,
  eventId: optStr,
  spaceId: optStr,
  locationId: optStr,
  teacherMembershipId: optStr,
  name: optStr,
});

export type RawPerformance = z.infer<typeof RawPerformanceSchema>;

// ── Events (class/event definitions) ────────────────────────

export const RawEventSchema = z.object({
  id: z.string(),
  name: optStr,
  createdAt: optStr,
});

export type RawEvent = z.infer<typeof RawEventSchema>;

// ── Locations ───────────────────────────────────────────────

export const RawLocationSchema = z.object({
  id: z.string(),
  name: optStr,
});

export type RawLocation = z.infer<typeof RawLocationSchema>;
