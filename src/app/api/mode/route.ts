import { NextResponse } from "next/server";

export async function GET() {
  // Railway sets RAILWAY_ENVIRONMENT automatically
  const isProduction = !!process.env.RAILWAY_ENVIRONMENT;
  return NextResponse.json({ mode: isProduction ? "dashboard" : "pipeline" });
}
