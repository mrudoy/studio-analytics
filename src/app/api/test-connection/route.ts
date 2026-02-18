import { NextResponse } from "next/server";
import { loadSettings } from "@/lib/crypto/credentials";

// Force dynamic — Playwright can't be loaded during static build phase
export const dynamic = "force-dynamic";

export async function POST() {
  const settings = loadSettings();
  if (!settings?.credentials) {
    return NextResponse.json(
      { error: "No credentials configured. Save your Union.fit credentials first." },
      { status: 400 }
    );
  }

  const { UnionClient } = await import("@/lib/scraper/union-client");
  const client = new UnionClient();
  try {
    await client.initializeHeaded();
    const result = await client.interactiveLogin(
      settings.credentials.email,
      settings.credentials.password
    );
    return NextResponse.json(result);
  } catch (error) {
    const message = error instanceof Error ? error.message : "Test connection failed";
    return NextResponse.json({ success: false, message }, { status: 500 });
  } finally {
    await client.cleanup();
  }
}
