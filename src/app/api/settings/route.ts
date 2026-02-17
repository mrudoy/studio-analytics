import { NextResponse } from "next/server";
import {
  loadSettings,
  saveSettings,
  hasStoredCredentials,
  type AppSettings,
} from "@/lib/crypto/credentials";
import { mkdirSync, existsSync } from "fs";
import { join } from "path";

export async function GET() {
  try {
    const has = hasStoredCredentials();
    const settings = loadSettings();

    return NextResponse.json({
      hasCredentials: has,
      hasAnalyticsSheet: !!settings?.analyticsSpreadsheetId,
      hasRawDataSheet: !!settings?.rawDataSpreadsheetId,
      hasRobotEmail: !!settings?.robotEmail?.address,
      // Never return actual credentials — only existence flags
      email: settings?.credentials?.email ? maskEmail(settings.credentials.email) : null,
      robotEmail: settings?.robotEmail?.address ? maskEmail(settings.robotEmail.address) : null,
    });
  } catch {
    return NextResponse.json({ hasCredentials: false });
  }
}

export async function PUT(request: Request) {
  try {
    const body = await request.json();

    // Ensure data directory exists
    const dataDir = join(process.cwd(), "data");
    if (!existsSync(dataDir)) {
      mkdirSync(dataDir, { recursive: true });
    }

    // Load existing settings and merge
    const existing = loadSettings() || {};
    const updated: AppSettings = { ...existing };

    if (body.email && body.password) {
      updated.credentials = {
        email: body.email,
        password: body.password,
      };
    }

    if (body.analyticsSpreadsheetId !== undefined) {
      updated.analyticsSpreadsheetId = body.analyticsSpreadsheetId;
    }

    if (body.rawDataSpreadsheetId !== undefined) {
      updated.rawDataSpreadsheetId = body.rawDataSpreadsheetId;
    }

    if (body.robotEmail) {
      updated.robotEmail = {
        address: body.robotEmail,
        ...(body.robotEmailAppPassword ? { appPassword: body.robotEmailAppPassword } : {}),
      };
    }

    saveSettings(updated);

    return NextResponse.json({ success: true });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Failed to save settings";
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

function maskEmail(email: string): string {
  const [local, domain] = email.split("@");
  if (!domain) return "***";
  const masked = local.slice(0, 2) + "***";
  return `${masked}@${domain}`;
}
