import { NextResponse } from "next/server";
import { loadSettings } from "@/lib/crypto/credentials";
import { sendTestDigestEmail } from "@/lib/email/email-sender";

/**
 * POST /api/settings/test-email
 *
 * Sends a test digest email to a single address.
 * Uses the Resend API key from settings or the request body.
 */
export async function POST(request: Request) {
  try {
    const body = await request.json();
    const { toAddress, resendApiKey, fromAddress } = body;

    if (!toAddress || !toAddress.includes("@")) {
      return NextResponse.json(
        { error: "Valid email address required" },
        { status: 400 },
      );
    }

    // Resolve API key: body > settings > env
    const settings = loadSettings();
    const key =
      resendApiKey ||
      settings?.emailDigest?.resendApiKey ||
      process.env.RESEND_API_KEY;

    if (!key) {
      return NextResponse.json(
        { error: "No Resend API key configured. Enter one above or set RESEND_API_KEY env var." },
        { status: 400 },
      );
    }

    const from =
      fromAddress ||
      settings?.emailDigest?.fromAddress ||
      undefined;

    await sendTestDigestEmail(toAddress, key, from);

    return NextResponse.json({ success: true, message: `Test email sent to ${toAddress}` });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Failed to send test email";
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
