/**
 * Email sender — sends the daily auto-renew digest after pipeline completion.
 *
 * Uses Resend (https://resend.com) for delivery.
 * Reads config from encrypted AppSettings (or env vars on Railway).
 */

import { Resend } from "resend";
import { loadSettings } from "../crypto/credentials";
import { getOverviewData } from "../db/overview-store";
import { getPool } from "../db/database";
import { buildDigestHtml } from "./digest-template";

export interface DigestResult {
  sent: number;
  skipped: string;
}

/**
 * Send the auto-renew digest email to all configured recipients.
 * Returns silently if digest is disabled, paused, or missing config.
 *
 * Includes an atomic once-per-day guard: uses a DB watermark row so that
 * no matter how many callers invoke this function (Railway worker, local
 * cron, staging env, etc.), only ONE email is sent per calendar day (ET).
 */
export async function sendDigestEmail(): Promise<DigestResult> {
  const settings = loadSettings();
  const digest = settings?.emailDigest;

  // Guard: only send from the production Railway service.
  // Preview/PR deployments have their own DB, so the atomic guard can't
  // prevent duplicates across environments. Require an explicit env flag.
  const railwayEnv = process.env.RAILWAY_ENVIRONMENT_NAME || process.env.RAILWAY_ENVIRONMENT || "";
  if (railwayEnv && railwayEnv !== "production") {
    return { sent: 0, skipped: `Non-production environment (${railwayEnv})` };
  }

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

  // ── Once-per-day atomic claim ──────────────────────────────────
  // Uses INSERT ... ON CONFLICT with a WHERE guard on today's date.
  // Only ONE caller per day gets rowCount=1; all others get 0.
  const todayET = new Date().toLocaleDateString("en-CA", {
    timeZone: "America/New_York",
  }); // YYYY-MM-DD
  const pool = getPool();
  const { rowCount } = await pool.query(
    `INSERT INTO fetch_watermarks (report_type, last_fetched_at, high_water_date, record_count, notes)
     VALUES ('digestEmail', NOW(), $1, 0, 'claimed')
     ON CONFLICT (report_type) DO UPDATE SET
       last_fetched_at = NOW(),
       high_water_date = EXCLUDED.high_water_date,
       record_count = 0,
       notes = 'claimed'
     WHERE fetch_watermarks.high_water_date IS DISTINCT FROM $1`,
    [todayET],
  );

  if (!rowCount || rowCount === 0) {
    return { sent: 0, skipped: `Already sent today (${todayET})` };
  }

  // We won the atomic claim — build and send the email
  const data = await getOverviewData();
  const html = buildDigestHtml(data);

  // Format today's date for subject line
  const today = new Date().toLocaleDateString("en-US", {
    weekday: "long",
    month: "long",
    day: "numeric",
    year: "numeric",
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

  // Update watermark with actual send count
  await pool.query(
    `UPDATE fetch_watermarks SET record_count = $1, notes = $2 WHERE report_type = 'digestEmail'`,
    [digest.recipients.length, `Sent to ${digest.recipients.length} recipients`],
  );

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
