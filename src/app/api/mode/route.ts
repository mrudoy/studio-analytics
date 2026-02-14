import { NextResponse } from "next/server";

export async function GET() {
  // Railway sets various RAILWAY_* env vars automatically
  const isRailway = !!(
    process.env.RAILWAY_ENVIRONMENT ||
    process.env.RAILWAY_ENVIRONMENT_NAME ||
    process.env.RAILWAY_PROJECT_ID ||
    process.env.RAILWAY_SERVICE_NAME
  );
  return NextResponse.json({ mode: isRailway ? "dashboard" : "pipeline" });
}
