/**
 * Email sender — sends the daily auto-renew digest after pipeline completion.
 *
 * Uses Resend (https://resend.com) for delivery.
 * Reads config from encrypted AppSettings (or env vars on Railway).
 */

import { Resend } from "resend";
import { loadSettings } from "../crypto/credentials";
import { getOverviewData } from "../db/overview-store";
import { buildDigestHtml } from "./digest-template";

export interface DigestResult {
  sent: number;
  skipped: string;
}

/**
 * Send the auto-renew digest email to all configured recipients.
 * Returns silently if digest is disabled, paused, or missing config.
 */
export async function sendDigestEmail(): Promise<DigestResult> {
  const settings = loadSettings();
  const digest = settings?.emailDigest;

  // Guard: skip if not configured or paused
  if (!digest?.enabled) {
    return { sent: 0, skipped: "Email digest disabled" };
  }

  if (!digest.recipients?.length) {
    return { sent: 0, skipped: "No recipients configured" };
  }

  const apiKey = digest.resendApiKey || process.env.RESEND_API_KEY;
  if (!apiKey) {
    return { sent: 0, skipped: "No Resend API key configured" };
  }

  // Fetch fresh overview data
  const data = await getOverviewData();
  const html = buildDigestHtml(data);

  // Format today's date for subject line (no year)
  const today = new Date().toLocaleDateString("en-US", {
    month: "short",
    day: "numeric",
    timeZone: "America/New_York",
  });

  const resend = new Resend(apiKey);
  const from = digest.fromAddress || "Sky Ting Analytics <onboarding@resend.dev>";

  const { error } = await resend.emails.send({
    from,
    to: digest.recipients,
    subject: `SKY HIGH Daily Update — ${today}`,
    html,
  });

  if (error) {
    throw new Error(`Resend error: ${error.message}`);
  }

  return { sent: digest.recipients.length, skipped: "" };
}

/**
 * Send a test digest email to a single address.
 * Used from the Settings UI to verify the API key and template.
 */
export async function sendTestDigestEmail(
  toAddress: string,
  apiKey: string,
  fromAddress?: string,
): Promise<void> {
  const data = await getOverviewData();
  const html = buildDigestHtml(data);

  const today = new Date().toLocaleDateString("en-US", {
    month: "short",
    day: "numeric",
    timeZone: "America/New_York",
  });

  const resend = new Resend(apiKey);
  const from = fromAddress || "Sky Ting Analytics <onboarding@resend.dev>";

  const { error } = await resend.emails.send({
    from,
    to: [toAddress],
    subject: `[TEST] SKY HIGH Daily Update — ${today}`,
    html,
  });

  if (error) {
    throw new Error(`Resend error: ${error.message}`);
  }
}
