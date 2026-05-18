import { NextResponse } from "next/server";
import type { NextRequest } from "next/server";
import { verifySession, COOKIE_NAME } from "@/lib/auth/session";

/**
 * Two-layer auth middleware:
 *
 * Layer 1 — Password gate (page + API level)
 *   Checks the studio_session cookie. Unauthenticated page requests redirect
 *   to /login; unauthenticated API requests return 401. Bearer tokens also
 *   satisfy this layer for programmatic API access (cron, scripts).
 *
 * Layer 2 — API auth (existing, unchanged)
 *   Three tiers: PUBLIC (no auth), READ (same-origin or Bearer), WRITE (Bearer only).
 */

/** API paths accessible without a session cookie */
const PUBLIC_PATHS = new Set([
  "/api/health",               // Railway health checks
  "/api/diagnose-pipeline",    // Operational diagnostics (read-only metadata)
  "/api/diagnose-conversions", // Operational diagnostics (read-only metadata)
  "/api/webhook/union-export", // Has own UNION_WEBHOOK_SECRET
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

  if (origin) {
    try {
      const originHost = new URL(origin).host;
      return originHost === host;
    } catch {
      return false;
    }
  }

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

export async function middleware(request: NextRequest) {
  const { pathname } = request.nextUrl;

  // Static assets — always pass through
  if (pathname.startsWith("/_next/") || pathname === "/favicon.ico") {
    return NextResponse.next();
  }

  // Auth endpoints — always accessible (no session gate)
  if (pathname.startsWith("/api/auth/")) {
    return NextResponse.next();
  }

  // Public API paths (health checks, webhooks) — skip session gate,
  // fall through to Layer 2 API auth below
  const isPublicPath = isPublic(pathname);

  // --- Layer 1: Session gate ---
  if (!isPublicPath) {
    let authenticated = false;

    // Bearer token satisfies the session gate for API routes
    if (pathname.startsWith("/api/")) {
      const cronSecret = process.env.CRON_SECRET;
      if (cronSecret && hasBearerToken(request, cronSecret)) {
        authenticated = true;
      }
    }

    if (!authenticated) {
      const session = request.cookies.get(COOKIE_NAME)?.value;
      authenticated = session ? await verifySession(session) : false;
    }

    if (pathname === "/login") {
      // Authenticated users don't need the login page
      if (authenticated) return NextResponse.redirect(new URL("/", request.url));
      return NextResponse.next();
    }

    if (!authenticated) {
      if (pathname.startsWith("/api/")) {
        return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
      }
      const loginUrl = new URL("/login", request.url);
      loginUrl.searchParams.set("from", pathname);
      return NextResponse.redirect(loginUrl);
    }
  }

  // Authenticated (or public/auth path). Non-API pages proceed immediately.
  if (!pathname.startsWith("/api/")) {
    return NextResponse.next();
  }

  // --- Layer 2: API auth (unchanged logic) ---
  if (isPublicPath) {
    return NextResponse.next();
  }

  // Same-origin browser requests are allowed for all endpoints.
  if (isSameOrigin(request)) {
    return NextResponse.next();
  }

  const cronSecret = process.env.CRON_SECRET;
  if (!cronSecret) {
    return NextResponse.json(
      { error: "Server misconfigured: auth secret not set" },
      { status: 500 },
    );
  }

  if (hasBearerToken(request, cronSecret)) {
    return NextResponse.next();
  }

  return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
}

export const config = {
  matcher: ["/((?!_next/static|_next/image|favicon.ico).*)"],
};
