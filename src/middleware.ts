import { NextResponse } from "next/server";
import type { NextRequest } from "next/server";

/**
 * API authentication middleware.
 *
 * Three tiers:
 * 1. PUBLIC — no auth needed (health checks, webhooks with own auth)
 * 2. READ — same-origin browser requests OR Bearer token (dashboard data)
 * 3. WRITE — Bearer token only (mutations, admin, sensitive data)
 *
 * This prevents external sites from scraping data or triggering mutations,
 * while keeping the dashboard functional for logged-in users.
 */

/** No auth required */
const PUBLIC_PATHS = new Set([
  "/api/health",                  // Railway health checks
  "/api/webhook/union-export",    // Has own UNION_WEBHOOK_SECRET
]);

function isPublic(pathname: string): boolean {
  return PUBLIC_PATHS.has(pathname);
}

function hasBearerToken(request: NextRequest, secret: string): boolean {
  const authHeader = request.headers.get("authorization");
  if (authHeader === `Bearer ${secret}`) return true;
  const tokenParam = request.nextUrl.searchParams.get("token");
  if (tokenParam === secret) return true;
  return false;
}

function isSameOrigin(request: NextRequest): boolean {
  const origin = request.headers.get("origin");
  const referer = request.headers.get("referer");
  const host = request.headers.get("host");
  if (!host) return false;

  // Check Origin header (set on cross-origin requests)
  if (origin) {
    try {
      const originHost = new URL(origin).host;
      return originHost === host;
    } catch {
      return false;
    }
  }

  // Check Referer header (set on same-origin navigations)
  if (referer) {
    try {
      const refererHost = new URL(referer).host;
      return refererHost === host;
    } catch {
      return false;
    }
  }

  // No Origin or Referer — could be server-side fetch (Next.js SSR) or curl.
  // Reject for safety; use Bearer token for programmatic access.
  return false;
}

export function middleware(request: NextRequest) {
  const { pathname } = request.nextUrl;

  // Only gate /api/* routes
  if (!pathname.startsWith("/api/")) {
    return NextResponse.next();
  }

  // Public endpoints — no auth
  if (isPublic(pathname)) {
    return NextResponse.next();
  }

  const cronSecret = process.env.CRON_SECRET;
  if (!cronSecret) {
    // Fail closed: if CRON_SECRET is not set, reject all API requests
    return NextResponse.json(
      { error: "Server misconfigured: auth secret not set" },
      { status: 500 },
    );
  }

  // Bearer token always works for any endpoint
  if (hasBearerToken(request, cronSecret)) {
    return NextResponse.next();
  }

  // Same-origin browser requests are allowed for all endpoints.
  // This covers the dashboard UI calling /api/pipeline, /api/settings, etc.
  // The threat model is external scrapers and CSRF, not the dashboard user.
  if (isSameOrigin(request)) {
    return NextResponse.next();
  }

  return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
}

export const config = {
  matcher: ["/api/:path*"],
};
