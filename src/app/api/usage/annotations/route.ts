import { NextRequest, NextResponse } from "next/server";
import { getAnnotations, createAnnotation } from "@/lib/db/usage-store";

export async function GET(request: NextRequest) {
  const { searchParams } = request.nextUrl;
  const weeks = Number(searchParams.get("weeks")) || 12;

  try {
    const annotations = await getAnnotations(weeks);
    return NextResponse.json({ annotations });
  } catch (err) {
    console.error("[api/usage/annotations] Error:", err);
    return NextResponse.json({ error: "Internal server error" }, { status: 500 });
  }
}

export async function POST(request: NextRequest) {
  try {
    const body = await request.json();
    const { week_start, label } = body;

    if (!week_start || !label) {
      return NextResponse.json({ error: "Missing week_start or label" }, { status: 400 });
    }

    const annotation = await createAnnotation(week_start, label);
    return NextResponse.json({ annotation });
  } catch (err) {
    console.error("[api/usage/annotations] Error:", err);
    return NextResponse.json({ error: "Internal server error" }, { status: 500 });
  }
}
