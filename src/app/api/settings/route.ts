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
      hasShopify: !!(settings?.shopify?.storeName && settings?.shopify?.clientId && settings?.shopify?.clientSecret),
      // Never return actual credentials — only existence flags
      email: settings?.credentials?.email ? maskEmail(settings.credentials.email) : null,
      robotEmail: settings?.robotEmail?.address ? maskEmail(settings.robotEmail.address) : null,
      shopifyStore: settings?.shopify?.storeName || null,
      // Email digest — return full config (recipients are not secret)
      emailDigest: {
        enabled: settings?.emailDigest?.enabled ?? false,
        recipients: settings?.emailDigest?.recipients ?? [],
        hasResendKey: !!(settings?.emailDigest?.resendApiKey || process.env.RESEND_API_KEY),
        fromAddress: settings?.emailDigest?.fromAddress || null,
      },
      hasUnionApiKey: !!(settings?.unionApiKey || process.env.UNION_API_KEY),
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

    if (body.shopifyStoreName && body.shopifyClientId && body.shopifyClientSecret) {
      updated.shopify = {
        storeName: body.shopifyStoreName,
        clientId: body.shopifyClientId,
        clientSecret: body.shopifyClientSecret,
      };
    }

    // Union Data Exporter API key
    if (body.unionApiKey) {
      updated.unionApiKey = body.unionApiKey;
    }

    // Email digest config
    if (body.emailDigest !== undefined) {
      const d = body.emailDigest;
      const existingDigest = existing.emailDigest || { enabled: false, recipients: [] };

      updated.emailDigest = {
        enabled: d.enabled ?? existingDigest.enabled,
        recipients: Array.isArray(d.recipients)
          ? d.recipients.filter((e: string) => typeof e === "string" && e.includes("@"))
          : existingDigest.recipients,
        // Only update API key if explicitly provided (don't overwrite with undefined)
        resendApiKey: d.resendApiKey || existingDigest.resendApiKey,
        fromAddress: d.fromAddress !== undefined ? d.fromAddress : existingDigest.fromAddress,
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
