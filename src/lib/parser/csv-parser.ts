import Papa from "papaparse";
import { readFileSync } from "fs";
import { z } from "zod";

/**
 * Column name aliases: maps direct-CSV column names to our schema field names.
 * The direct CSV from Union.fit's format=csv uses "subscription_name" etc.
 * The HTML-scraped CSV uses "Name" etc.
 * Both need to map to the same schema fields.
 */
const COLUMN_ALIASES: Record<string, string> = {
  // Auto-renew direct CSV → schema fields
  subscriptionName: "name",
  subscriptionState: "state",
  subscriptionPrice: "price",
  customerName: "customer",
  customerEmail: "email",
  createdAt: "created",
  // Keep these as-is (they already match)
  canceledAt: "canceledAt",
  canceledBy: "canceledBy",
  orderId: "orderId",
  salesChannel: "salesChannel",
  currentState: "currentState",
  currentSubscription: "currentSubscription",
  // Revenue categories CSV → schema fields
  // CSV has "refunded_union_fees" → "refundedUnionFees", but schema uses "unionFeesRefunded"
  refundedUnionFees: "unionFeesRefunded",
  // CSV has "other_fees" → "otherFees", maps to "transfers" in our schema (same concept)
  otherFees: "otherFees",
  // Orders/transactions direct CSV → schema fields
  orderCode: "code",
  transactionType: "type",
  paymentMethod: "payment",
  orderTotal: "total",
  amount: "total",
  transactionTotal: "total",
};

/**
 * Normalize CSV column headers to camelCase keys, then apply aliases.
 * E.g., "subscription_name" -> "subscriptionName" -> "name"
 *       "REDEEMED AT" -> "redeemedAt"
 *       "Canceled At" -> "canceledAt"
 *
 * Note: Must be idempotent — PapaParse may call transformHeader twice.
 * If already camelCase (no separators), skip re-processing to avoid
 * destroying existing casing (e.g., "redeemedAt" → "redeemedat").
 */
function normalizeHeader(header: string): string {
  const trimmed = header.trim();

  // If already looks like a valid camelCase key (only alphanumeric, starts lowercase),
  // just apply alias without re-processing — this makes the function idempotent.
  if (/^[a-z][a-zA-Z0-9]*$/.test(trimmed)) {
    return COLUMN_ALIASES[trimmed] || trimmed;
  }

  const camelCase = trimmed
    .toLowerCase()
    .replace(/[^a-z0-9]+(.)/g, (_, char) => char.toUpperCase())
    .replace(/^[A-Z]/, (c) => c.toLowerCase());

  // Apply alias if one exists
  return COLUMN_ALIASES[camelCase] || camelCase;
}

/**
 * Parse a CSV file and validate each row against a Zod schema.
 * Returns validated rows and any warnings for rows that failed validation.
 */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export function parseCSV<T>(
  filePath: string,
  schema: z.ZodType<T, any, any>
): { data: T[]; warnings: string[] } {
  const fileContent = readFileSync(filePath, "utf8");

  // Remove BOM if present
  const cleanContent = fileContent.replace(/^\uFEFF/, "");

  const parsed = Papa.parse(cleanContent, {
    header: true,
    skipEmptyLines: true,
    dynamicTyping: false,
    transformHeader: normalizeHeader,
  });

  // Diagnostic: log actual column headers after normalization + first row
  if (parsed.data.length > 0) {
    const headers = Object.keys(parsed.data[0] as Record<string, unknown>);
    const shortPath = filePath.split("/").pop() || filePath;
    console.log(`[csv-parser] ${shortPath} headers (${headers.length}): [${headers.join(", ")}]`);
    console.log(`[csv-parser] ${shortPath} first row:`, JSON.stringify(parsed.data[0]));
    console.log(`[csv-parser] ${shortPath} total rows: ${parsed.data.length}`);
  }

  const data: T[] = [];
  const warnings: string[] = [];

  for (let i = 0; i < parsed.data.length; i++) {
    const row = parsed.data[i];
    const result = schema.safeParse(row);

    if (result.success) {
      data.push(result.data);
    } else {
      // Only warn for the first few rows to avoid spam
      if (warnings.length < 10) {
        warnings.push(
          `Row ${i + 1}: ${result.error.issues.map((e) => `${e.path.join(".")}: ${e.message}`).join(", ")}`
        );
      }
    }
  }

  if (parsed.errors.length > 0) {
    warnings.push(
      ...parsed.errors.slice(0, 5).map((e) => `CSV parse error at row ${e.row}: ${e.message}`)
    );
  }

  return { data, warnings };
}
