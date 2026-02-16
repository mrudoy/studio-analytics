import { NextRequest, NextResponse } from "next/server";

export async function GET(request: NextRequest) {
  // Query param override for local preview: ?mode=dashboard
  const qsMode = request.nextUrl.searchParams.get("mode");
  if (qsMode === "dashboard" || qsMode === "pipeline") {
    return NextResponse.json({ mode: qsMode });
  }

  // Explicit override takes priority
  if (process.env.APP_MODE === "dashboard" || process.env.APP_MODE === "pipeline") {
    return NextResponse.json({ mode: process.env.APP_MODE });
  }

  // Auto-detect Railway (sets various RAILWAY_* env vars)
  const isRailway = !!(
    process.env.RAILWAY_ENVIRONMENT ||
    process.env.RAILWAY_ENVIRONMENT_NAME ||
    process.env.RAILWAY_PROJECT_ID ||
    process.env.RAILWAY_SERVICE_NAME
  );
  return NextResponse.json({ mode: isRailway ? "dashboard" : "pipeline" });
}
