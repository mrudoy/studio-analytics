/**
 * Insights Engine — detects actionable patterns in the data.
 *
 * Each detector is an async function that queries the DB and returns
 * 0-1 InsightInput objects. The orchestrator runs all detectors and
 * returns the combined results.
 *
 * Detectors:
 *  1. Drop-in-to-Subscription Converter — frequent visitors without subscriptions
 *  2. SKY3 Early Churn — % of SKY3 cancellations within first 3 months
 *  3. New Customer Return Rate — % of first-timers who come back
 *  4. Revenue Trend Anomaly — current month vs trailing 3-month average
 */

import type { Pool } from "pg";
import type { InsightInput } from "../db/insights-store";

type Detector = (pool: Pool) => Promise<InsightInput | null>;

// ── Detector 1: Drop-in-to-Subscription Converter ────────────

const detectDropInConverter: Detector = async (pool) => {
  try {
    const res = await pool.query(`
      SELECT COUNT(*) AS cnt FROM (
        SELECT r.email
        FROM registrations r
        LEFT JOIN auto_renews ar
          ON LOWER(r.email) = LOWER(ar.customer_email)
          AND ar.plan_state IN ('Active', 'In Trial')
        WHERE r.attended_at >= TO_CHAR(NOW() - INTERVAL '90 days', 'YYYY-MM-DD')
          AND r.email IS NOT NULL AND r.email != ''
          AND ar.customer_email IS NULL
        GROUP BY r.email
        HAVING COUNT(*) >= 4
      ) sub
    `);

    const count = parseInt(res.rows[0]?.cnt ?? "0", 10);
    if (count === 0) return null;

    const severity: InsightInput["severity"] =
      count > 100 ? "critical" : count > 30 ? "warning" : "info";

    return {
      detector: "drop-in-converter",
      headline: `${count.toLocaleString()} regulars visited 4+ times in 90 days with no subscription`,
      explanation:
        `These customers are paying per visit (~$37/class) when a SKY3 ($95/mo for 3 classes) ` +
        `or Member plan ($230/mo unlimited) would save them money. ` +
        `At 4+ visits/month, SKY3 breaks even; at 7+, Member breaks even.`,
      category: "conversion",
      severity,
      metricValue: count,
      metricContext: {
        visitThreshold: 4,
        window: "90d",
        dropInPrice: 37,
        sky3Price: 95,
        memberPrice: 230,
        breakEvenSky3: 3,
        breakEvenMember: 7,
      },
    };
  } catch (err) {
    console.warn("[insights] drop-in-converter failed:", err);
    return null;
  }
};

// ── Detector 2: SKY3 Early Churn ─────────────────────────────

const detectSky3EarlyChurn: Detector = async (pool) => {
  try {
    // Count SKY3 cancellations in the last 90 days
    const totalRes = await pool.query(`
      SELECT COUNT(*) AS total
      FROM auto_renews
      WHERE plan_state = 'Canceled'
        AND (plan_name ILIKE '%sky3%' OR plan_name ILIKE '%skyhigh%' OR plan_name ILIKE '%pack%')
        AND canceled_at >= TO_CHAR(NOW() - INTERVAL '90 days', 'YYYY-MM-DD')
    `);

    const totalCanceled = parseInt(totalRes.rows[0]?.total ?? "0", 10);
    if (totalCanceled < 5) return null; // Not enough data

    // Count those that churned within 90 days of creation
    const earlyRes = await pool.query(`
      SELECT COUNT(*) AS early
      FROM auto_renews
      WHERE plan_state = 'Canceled'
        AND (plan_name ILIKE '%sky3%' OR plan_name ILIKE '%skyhigh%' OR plan_name ILIKE '%pack%')
        AND canceled_at >= TO_CHAR(NOW() - INTERVAL '90 days', 'YYYY-MM-DD')
        AND created_at IS NOT NULL AND created_at != ''
        AND canceled_at IS NOT NULL AND canceled_at != ''
        AND (
          TO_DATE(canceled_at, 'YYYY-MM-DD') - TO_DATE(created_at, 'YYYY-MM-DD')
        ) < 90
    `);

    const earlyCanceled = parseInt(earlyRes.rows[0]?.early ?? "0", 10);
    const earlyPct = Math.round((earlyCanceled / totalCanceled) * 100);

    if (earlyPct === 0) return null;

    const severity: InsightInput["severity"] =
      earlyPct > 50 ? "critical" : earlyPct > 30 ? "warning" : "info";

    return {
      detector: "sky3-early-churn",
      headline: `${earlyPct}% of SKY3 cancellations happen within 3 months`,
      explanation:
        `${earlyCanceled} of ${totalCanceled} recent SKY3 cancellations occurred before the 90-day mark. ` +
        `A 3-month minimum commitment (like the Member plan) could significantly reduce early churn.`,
      category: "churn",
      severity,
      metricValue: earlyPct,
      metricContext: {
        earlyCanceled,
        totalCanceled,
        windowDays: 90,
        thresholdDays: 90,
      },
    };
  } catch (err) {
    console.warn("[insights] sky3-early-churn failed:", err);
    return null;
  }
};

// ── Detector 3: New Customer Return Rate ─────────────────────

const detectNewCustomerReturnRate: Detector = async (pool) => {
  try {
    // First visits in the last 60 days (with email)
    const fvRes = await pool.query(`
      SELECT COUNT(DISTINCT email) AS total_fv
      FROM first_visits
      WHERE attended_at >= TO_CHAR(NOW() - INTERVAL '60 days', 'YYYY-MM-DD')
        AND email IS NOT NULL AND email != ''
    `);

    const totalFirstVisits = parseInt(fvRes.rows[0]?.total_fv ?? "0", 10);
    if (totalFirstVisits < 10) return null;

    // Of those, how many have a subsequent registration?
    const returnedRes = await pool.query(`
      SELECT COUNT(DISTINCT fv.email) AS returned
      FROM first_visits fv
      INNER JOIN registrations r
        ON LOWER(fv.email) = LOWER(r.email)
        AND r.attended_at > fv.attended_at
      WHERE fv.attended_at >= TO_CHAR(NOW() - INTERVAL '60 days', 'YYYY-MM-DD')
        AND fv.email IS NOT NULL AND fv.email != ''
    `);

    const returned = parseInt(returnedRes.rows[0]?.returned ?? "0", 10);
    const returnPct = Math.round((returned / totalFirstVisits) * 100);

    const severity: InsightInput["severity"] =
      returnPct > 40 ? "positive" : returnPct >= 20 ? "info" : "warning";

    return {
      detector: "new-customer-return-rate",
      headline: `${returnPct}% of new customers returned after their first visit (60-day window)`,
      explanation:
        `${returned} of ${totalFirstVisits} first-time visitors in the last 60 days came back for another class. ` +
        (returnPct < 20
          ? `This is below typical benchmarks. Consider follow-up outreach to first-timers.`
          : returnPct >= 40
            ? `Strong return rate — your intro experience is converting well.`
            : `Room to improve with targeted follow-up offers for first-timers.`),
      category: "growth",
      severity,
      metricValue: returnPct,
      metricContext: {
        returned,
        totalFirstVisits,
        windowDays: 60,
      },
    };
  } catch (err) {
    console.warn("[insights] new-customer-return-rate failed:", err);
    return null;
  }
};

// ── Detector 4: Revenue Trend Anomaly ────────────────────────

const detectRevenueTrendAnomaly: Detector = async (pool) => {
  try {
    // Get monthly net revenue for the last 4 months
    const res = await pool.query(`
      SELECT LEFT(period_start, 7) AS month,
             SUM(net_revenue) AS net
      FROM revenue_categories
      WHERE LEFT(period_start, 7) = LEFT(period_end, 7)
        AND period_start >= TO_CHAR(NOW() - INTERVAL '4 months', 'YYYY-MM-01')
      GROUP BY LEFT(period_start, 7)
      ORDER BY month DESC
      LIMIT 4
    `);

    if (res.rows.length < 2) return null; // Need at least current + 1 prior month

    const months = res.rows.map((r) => ({
      month: r.month as string,
      net: parseFloat(r.net as string),
    }));

    const currentMonth = months[0];
    const priorMonths = months.slice(1);

    if (priorMonths.length === 0) return null;

    const trailingAvg = priorMonths.reduce((s, m) => s + m.net, 0) / priorMonths.length;
    if (trailingAvg === 0) return null;

    const pctDiff = Math.round(((currentMonth.net - trailingAvg) / trailingAvg) * 100);
    const direction = pctDiff >= 0 ? "above" : "below";
    const absPct = Math.abs(pctDiff);

    // Only surface if meaningful deviation
    if (absPct < 5) return null;

    const severity: InsightInput["severity"] =
      pctDiff > 10 ? "positive" : pctDiff < -15 ? "warning" : "info";

    return {
      detector: "revenue-trend-anomaly",
      headline: `This month's revenue is ${absPct}% ${direction} the ${priorMonths.length}-month average`,
      explanation:
        `${currentMonth.month} net revenue: $${Math.round(currentMonth.net).toLocaleString()} vs ` +
        `trailing average: $${Math.round(trailingAvg).toLocaleString()}.` +
        (pctDiff < -15
          ? ` Significant decline — investigate whether this is seasonal or structural.`
          : pctDiff > 10
            ? ` Positive momentum — consider what's driving growth.`
            : ``),
      category: "revenue",
      severity,
      metricValue: pctDiff,
      metricContext: {
        currentMonth: currentMonth.month,
        currentNet: Math.round(currentMonth.net),
        trailingAvg: Math.round(trailingAvg),
        trailingMonths: priorMonths.length,
      },
    };
  } catch (err) {
    console.warn("[insights] revenue-trend-anomaly failed:", err);
    return null;
  }
};

// ── Orchestrator ──────────────────────────────────────────────

const ALL_DETECTORS: Detector[] = [
  detectDropInConverter,
  detectSky3EarlyChurn,
  detectNewCustomerReturnRate,
  detectRevenueTrendAnomaly,
];

/**
 * Run all insight detectors against the database.
 * Returns an array of detected insights (nulls filtered out).
 */
export async function computeInsights(pool: Pool): Promise<InsightInput[]> {
  const results = await Promise.allSettled(
    ALL_DETECTORS.map((detector) => detector(pool))
  );

  const insights: InsightInput[] = [];
  for (const result of results) {
    if (result.status === "fulfilled" && result.value !== null) {
      insights.push(result.value);
    } else if (result.status === "rejected") {
      console.warn("[insights] Detector threw:", result.reason);
    }
  }

  console.log(`[insights] Computed ${insights.length} insights from ${ALL_DETECTORS.length} detectors`);
  return insights;
}
