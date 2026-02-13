import { NextResponse } from "next/server";
import { UnionClient } from "@/lib/scraper/union-client";
import { loadSettings } from "@/lib/crypto/credentials";

export async function POST() {
  const settings = loadSettings();
  if (!settings?.credentials) {
    return NextResponse.json(
      { error: "No credentials configured. Save your Union.fit credentials first." },
      { status: 400 }
    );
  }

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
