import { z } from "zod";

/**
 * Zod schemas for each Union.fit CSV export type.
 * Field names match HTML table headers after normalizeHeader() camelCase conversion.
 *
 * Note: Dollar signs in price/total fields are stripped by the `money` transformer.
 */

/** Transform "$50.00" or "$1,234.56" to a number */
const money = z
  .string()
  .or(z.number())
  .transform((val) => {
    if (typeof val === "number") return val;
    const cleaned = val.replace(/[$,]/g, "").trim();
    const num = parseFloat(cleaned);
    return isNaN(num) ? 0 : num;
  })
  .default(0);

export const NewCustomerSchema = z.object({
  name: z.string().default(""),
  email: z.string().default(""),
  role: z.string().default(""),
  orders: z.coerce.number().default(0),
  created: z.string().default(""),
});

export const OrderSchema = z.object({
  created: z.string().default(""),
  code: z.string().default(""),
  customer: z.string().default(""),
  type: z.string().default(""),
  payment: z.string().default(""),
  total: money,
});

export const FirstVisitSchema = z.object({
  attendee: z.string().default(""),
  performance: z.string().default(""),
  type: z.string().default(""),
  redeemedAt: z.string().default(""),
  pass: z.string().default(""),
  status: z.string().default(""),
});

/**
 * Registration schema matches the /report/registrations/remaining page.
 * Columns: Customer, Pass, Remaining, Total, Expires, Price, Last Teacher, Revenue Category
 */
export const RegistrationSchema = z.object({
  customer: z.string().default(""),
  pass: z.string().default(""),
  remaining: z.string().or(z.number()).transform((val) => {
    if (typeof val === "number") return val;
    const num = parseInt(val.trim(), 10);
    return isNaN(num) ? -1 : num; // -1 = unlimited (∞)
  }).default(-1),
  total: z.string().or(z.number()).transform((val) => {
    if (typeof val === "number") return val;
    const num = parseInt(val.trim(), 10);
    return isNaN(num) ? -1 : num; // -1 = unlimited (∞)
  }).default(-1),
  expires: z.string().default(""),
  price: money,
  lastTeacher: z.string().default(""),
  revenueCategory: z.string().default(""),
});

/**
 * Revenue Categories schema — matches the /reports/revenue page.
 * Columns: Revenue Category, Revenue, Union Fees, Stripe Fees, Transfers, Refunded, Union Fees Refunded, Net Revenue
 */
export const RevenueCategorySchema = z.object({
  revenueCategory: z.string().default(""),
  revenue: money,
  unionFees: money,
  stripeFees: money,
  otherFees: money,           // "other_fees" in CSV download
  transfers: money,           // "transfers" in HTML scrape (0 in CSV download)
  refunded: money,
  unionFeesRefunded: money,   // "refunded_union_fees" in CSV download
  netRevenue: money,
});

/**
 * Full Registration schema — matches the /registrations/all CSV export (22 columns).
 * CSV headers use snake_case (event_name, attended_at, etc.) which normalizeHeader()
 * converts to camelCase automatically.
 *
 * If fetched via HTML scrape (fallback), only 7 columns are available and most fields
 * will default to empty/false. The analytics layer checks for email presence before
 * running returning-non-members analysis.
 */
export const FullRegistrationSchema = z.object({
  eventName: z.string().default(""),
  locationName: z.string().default(""),
  teacherName: z.string().default(""),
  firstName: z.string().default(""),
  lastName: z.string().default(""),
  email: z.string().default(""),
  attendedAt: z.string().default(""),
  registrationType: z.string().default(""),
  state: z.string().default(""),
  pass: z.string().default(""),
  subscription: z
    .string()
    .or(z.boolean())
    .transform((val) => {
      if (typeof val === "boolean") return val;
      return val.trim().toLowerCase() === "true";
    })
    .default(false),
  revenue: money,
});

/**
 * AutoRenew schema — used for active, new, and canceled auto-renew reports.
 * - Active/New have: Name, State, Price, Customer, Created
 * - Canceled has:    Name, State, Price, Customer, Canceled At
 *
 * The "name" field contains plan name + "\nSubscription" suffix from HTML scraping;
 * this is cleaned during post-processing.
 */
export const AutoRenewSchema = z.object({
  name: z
    .string()
    .transform((val) =>
      val
        .replace(/\n\s*Subscription/gi, "")
        .replace(/\s+/g, " ")
        .trim()
    )
    .default(""),
  state: z
    .string()
    .transform((val) =>
      val
        .replace(/\n\s*Register/gi, "")
        .replace(/\s+/g, " ")
        .trim()
    )
    .default(""),
  price: money,
  customer: z
    .string()
    .transform((val) =>
      val
        .replace(/\n\s*Register/gi, "")
        .replace(/\s+/g, " ")
        .trim()
    )
    .default(""),
  email: z.string().optional().default(""),
  canceledAt: z.string().optional().default(""),
  created: z.string().optional().default(""),
});
