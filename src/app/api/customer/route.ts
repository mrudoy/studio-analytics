import { NextRequest, NextResponse } from "next/server";
import {
  getCustomerByEmail,
  searchFullCustomers,
  hasFullCustomerData,
  getFullCustomerCount,
} from "@/lib/db/customer-store";

/**
 * CRM API: look up customer profiles by email or search by name.
 *
 * GET /api/customer?email=jane@example.com   → full profile
 * GET /api/customer?q=jane                   → search results
 * GET /api/customer                          → summary stats
 */
export async function GET(request: NextRequest) {
  try {
    const { searchParams } = new URL(request.url);
    const email = searchParams.get("email");
    const query = searchParams.get("q");

    // Full profile lookup by email
    if (email) {
      const profile = getCustomerByEmail(email);
      if (!profile) {
        return NextResponse.json(
          { error: `No customer found with email: ${email}` },
          { status: 404 }
        );
      }
      return NextResponse.json(profile);
    }

    // Search by name or email
    if (query) {
      const results = searchFullCustomers(query, 20);
      return NextResponse.json({ results, count: results.length });
    }

    // Summary
    const hasData = hasFullCustomerData();
    const count = hasData ? getFullCustomerCount() : 0;
    return NextResponse.json({
      hasData,
      customerCount: count,
      message: hasData
        ? `CRM has ${count} customer profiles. Use ?email=... or ?q=... to search.`
        : "No customer profiles imported yet.",
    });
  } catch (error) {
    const message =
      error instanceof Error ? error.message : "CRM lookup failed";
    console.error("[api/customer] Error:", message);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
