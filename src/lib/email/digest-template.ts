/**
 * HTML email template for the daily auto-renew digest.
 *
 * Renders a compact table with 3 subscription tiers (Members, Sky Ting TV, Sky3)
 * showing Active count + Yesterday / This Week / Last Week net changes with
 * +new / -churned breakdown. Inline CSS only (email-safe).
 */

import type { OverviewData, TimeWindowMetrics } from "@/types/dashboard";

// ── Helpers ────────────────────────────────────────────────

function netColor(net: number): string {
  if (net > 0) return "#16a34a"; // green-600
  if (net < 0) return "#dc2626"; // red-600
  return "#6b7280";              // gray-500
}

function formatNet(net: number): string {
  if (net > 0) return `+${net}`;
  if (net < 0) return `${net}`;
  return "0";
}

function formatComma(n: number): string {
  return n.toLocaleString("en-US");
}

// ── Cell builder ───────────────────────────────────────────

function windowCell(sub: { new: number; churned: number }): string {
  const net = sub.new - sub.churned;
  return `
    <td style="padding:10px 14px;text-align:center;vertical-align:top;border-bottom:1px solid #e5e7eb;">
      <div style="font-size:16px;font-weight:600;color:${netColor(net)};">
        ${formatNet(net)}
      </div>
      <div style="font-size:11px;color:#9ca3af;margin-top:2px;">
        +${sub.new} / -${sub.churned}
      </div>
    </td>`;
}

type SubKey = "member" | "sky3" | "skyTingTv";

function tierRow(
  label: string,
  activeCount: number,
  key: SubKey,
  windows: TimeWindowMetrics[],
): string {
  const cells = windows.map((w) => windowCell(w.subscriptions[key])).join("");
  return `
    <tr>
      <td style="padding:10px 14px;font-weight:600;font-size:14px;color:#111827;border-bottom:1px solid #e5e7eb;white-space:nowrap;">
        ${label}
      </td>
      <td style="padding:10px 14px;text-align:center;font-size:18px;font-weight:700;color:#111827;border-bottom:1px solid #e5e7eb;">
        ${formatComma(activeCount)}
      </td>
      ${cells}
    </tr>`;
}

function totalRow(
  active: { member: number; sky3: number; skyTingTv: number },
  windows: TimeWindowMetrics[],
): string {
  const totalActive = active.member + active.sky3 + active.skyTingTv;
  const cells = windows
    .map((w) => {
      const totalNew = w.subscriptions.member.new + w.subscriptions.sky3.new + w.subscriptions.skyTingTv.new;
      const totalChurned = w.subscriptions.member.churned + w.subscriptions.sky3.churned + w.subscriptions.skyTingTv.churned;
      const net = totalNew - totalChurned;
      return `
    <td style="padding:10px 14px;text-align:center;vertical-align:top;background-color:#f9fafb;">
      <div style="font-size:16px;font-weight:700;color:${netColor(net)};">
        ${formatNet(net)}
      </div>
      <div style="font-size:11px;color:#9ca3af;margin-top:2px;">
        +${totalNew} / -${totalChurned}
      </div>
    </td>`;
    })
    .join("");

  return `
    <tr>
      <td style="padding:10px 14px;font-weight:700;font-size:14px;color:#111827;background-color:#f9fafb;white-space:nowrap;">
        Total
      </td>
      <td style="padding:10px 14px;text-align:center;font-size:18px;font-weight:700;color:#111827;background-color:#f9fafb;">
        ${formatComma(totalActive)}
      </td>
      ${cells}
    </tr>`;
}

// ── Public ─────────────────────────────────────────────────

export function buildDigestHtml(data: OverviewData): string {
  const windows = [data.yesterday, data.thisWeek, data.lastWeek];
  const { currentActive } = data;

  const today = new Date().toLocaleDateString("en-US", {
    weekday: "long",
    month: "long",
    day: "numeric",
    year: "numeric",
    timeZone: "America/New_York",
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
            <td style="padding:24px 24px 12px;border-bottom:1px solid #e5e7eb;">
              <div style="font-size:20px;font-weight:700;color:#111827;">Auto-Renews</div>
              <div style="font-size:13px;color:#6b7280;margin-top:4px;">${today}</div>
            </td>
          </tr>

          <!-- Table -->
          <tr>
            <td style="padding:0;">
              <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse;">
                <!-- Column headers -->
                <tr style="background-color:#f9fafb;">
                  <th style="padding:10px 14px;text-align:left;font-size:11px;font-weight:600;color:#6b7280;text-transform:uppercase;letter-spacing:0.05em;border-bottom:2px solid #e5e7eb;">
                  </th>
                  <th style="padding:10px 14px;text-align:center;font-size:11px;font-weight:600;color:#6b7280;text-transform:uppercase;letter-spacing:0.05em;border-bottom:2px solid #e5e7eb;">
                    Active
                  </th>
                  ${windows
                    .map(
                      (w) => `
                  <th style="padding:10px 14px;text-align:center;font-size:11px;font-weight:600;color:#6b7280;text-transform:uppercase;letter-spacing:0.05em;border-bottom:2px solid #e5e7eb;">
                    ${w.label}<br><span style="font-weight:400;text-transform:none;letter-spacing:0;">${w.sublabel}</span>
                  </th>`
                    )
                    .join("")}
                </tr>

                ${tierRow("Members", currentActive.member, "member", windows)}
                ${tierRow("Sky3", currentActive.sky3, "sky3", windows)}
                ${tierRow("Sky Ting TV", currentActive.skyTingTv, "skyTingTv", windows)}
                ${totalRow(currentActive, windows)}
              </table>
            </td>
          </tr>

          <!-- CTA Button -->
          <tr>
            <td style="padding:24px 24px 12px;text-align:center;">
              <a href="https://considerate-perfection-staging.up.railway.app" target="_blank" style="display:inline-block;padding:12px 32px;background-color:#111827;color:#ffffff;font-size:14px;font-weight:600;text-decoration:none;border-radius:8px;">
                See Dashboard
              </a>
            </td>
          </tr>

          <!-- Footer -->
          <tr>
            <td style="padding:12px 24px 16px;border-top:none;">
              <div style="font-size:11px;color:#9ca3af;text-align:center;">
                Sky Ting Analytics — sent automatically after data refresh
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
