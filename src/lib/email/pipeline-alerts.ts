/**
 * Pipeline failure alerts — sends email notifications when the pipeline
 * exhausts all retries or when data goes stale.
 *
 * Uses the same Resend config as the daily digest email.
 * Each alert type is sent at most once per calendar day (ET).
 */

import { Resend } from "resend";
import { loadSettings } from "../crypto/credentials";
import { getPool } from "../db/database";

type AlertType = "retry_exhausted" | "stale_data";

/**
 * Send a pipeline alert email. At most one email per alert type per day.
 * Returns silently if email is not configured or alert was already sent today.
 */
export async function sendPipelineAlert(
  type: AlertType,
  detail: string,
): Promise<void> {
  const settings = loadSettings();
  const digest = settings?.emailDigest;

  if (!digest?.enabled || !digest.recipients?.length) return;

  const apiKey = digest.resendApiKey || process.env.RESEND_API_KEY;
  if (!apiKey) return;

  // Once-per-day guard per alert type
  const todayET = new Date().toLocaleDateString("en-CA", {
    timeZone: "America/New_York",
  });
  const watermarkKey = `pipelineAlert_${type}`;

  try {
    const pool = getPool();
    const { rowCount } = await pool.query(
      `INSERT INTO fetch_watermarks (report_type, last_fetched_at, high_water_date, record_count, notes)
       VALUES ($1, NOW(), $2, 0, $3)
       ON CONFLICT (report_type) DO UPDATE SET
         last_fetched_at = NOW(),
         high_water_date = EXCLUDED.high_water_date,
         notes = EXCLUDED.notes
       WHERE fetch_watermarks.high_water_date IS DISTINCT FROM $2`,
      [watermarkKey, todayET, detail],
    );

    if (!rowCount || rowCount === 0) return; // Already sent today
  } catch {
    // If watermark check fails, still try to send (better to alert than not)
  }

  const subject =
    type === "retry_exhausted"
      ? "Pipeline Failed — All Retries Exhausted"
      : "Data Stale — Union Export Missing";

  const html = buildAlertHtml(type, detail);

  try {
    const resend = new Resend(apiKey);
    const from = digest.fromAddress || "Sky Ting Analytics <onboarding@resend.dev>";
    const { error } = await resend.emails.send({
      from,
      to: digest.recipients,
      subject: `[ALERT] ${subject}`,
      html,
    });

    if (error) {
      console.error(`[pipeline-alerts] Resend error: ${error.message}`);
    } else {
      console.log(`[pipeline-alerts] Sent ${type} alert to ${digest.recipients.length} recipients`);
    }
  } catch (err) {
    console.error(
      "[pipeline-alerts] Failed to send alert:",
      err instanceof Error ? err.message : err,
    );
  }
}

function buildAlertHtml(type: AlertType, detail: string): string {
  const title =
    type === "retry_exhausted"
      ? "Pipeline Failed"
      : "Data Going Stale";

  const description =
    type === "retry_exhausted"
      ? "The data pipeline exhausted all retry attempts and could not complete. Dashboard data may be outdated until the next successful run."
      : "Union.fit export data has not been updated recently. The dashboard may be showing outdated numbers.";

  const now = new Date().toLocaleString("en-US", {
    timeZone: "America/New_York",
    dateStyle: "full",
    timeStyle: "short",
  });

  return `<!DOCTYPE html>
<html lang="en">
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0;padding:0;background-color:#ffffff;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,'Helvetica Neue',Arial,sans-serif;">
  <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background-color:#ffffff;padding:32px 16px;">
    <tr>
      <td align="center">
        <table role="presentation" width="600" cellpadding="0" cellspacing="0" style="background-color:#ffffff;border:1px solid #e5e7eb;border-radius:12px;overflow:hidden;">
          <!-- Header -->
          <tr>
            <td style="padding:24px;background-color:#fef2f2;border-bottom:1px solid #fecaca;">
              <div style="font-size:20px;font-weight:700;color:#991b1b;">${title}</div>
              <div style="font-size:13px;color:#b91c1c;margin-top:4px;">${now}</div>
            </td>
          </tr>
          <!-- Body -->
          <tr>
            <td style="padding:24px;">
              <p style="font-size:14px;color:#374151;margin:0 0 16px;">${description}</p>
              <div style="background-color:#f9fafb;border:1px solid #e5e7eb;border-radius:8px;padding:16px;font-size:13px;color:#6b7280;font-family:monospace;white-space:pre-wrap;">${detail}</div>
            </td>
          </tr>
          <!-- CTA -->
          <tr>
            <td style="padding:0 24px 24px;text-align:center;">
              <a href="https://considerate-perfection-staging.up.railway.app" target="_blank" style="display:inline-block;padding:12px 32px;background-color:#111827;color:#ffffff;font-size:14px;font-weight:600;text-decoration:none;border-radius:8px;">
                Check Dashboard
              </a>
            </td>
          </tr>
          <!-- Footer -->
          <tr>
            <td style="padding:12px 24px 16px;border-top:1px solid #e5e7eb;">
              <div style="font-size:11px;color:#9ca3af;text-align:center;">
                Sky Ting Analytics — automated pipeline alert
              </div>
            </td>
          </tr>
        </table>
      </td>
    </tr>
  </table>
</body>
</html>`;
}
