/**
 * Insights store — save and read computed insights from the database.
 */

import { getPool } from "./database";
import type { InsightRow } from "@/types/dashboard";

export interface InsightInput {
  detector: string;
  headline: string;
  explanation: string | null;
  category: "conversion" | "churn" | "revenue" | "growth";
  severity: "critical" | "warning" | "info" | "positive";
  metricValue: number | null;
  metricContext: Record<string, unknown> | null;
  pipelineRunId?: number | null;
}

/**
 * Save insights to the database.
 * Deletes all previous non-dismissed insights from the same detectors
 * before inserting fresh ones. This ensures only the latest version per
 * detector is shown (no duplicates across different days).
 */
export async function saveInsights(insights: InsightInput[]): Promise<void> {
  if (insights.length === 0) return;

  const pool = getPool();

  // Delete ALL non-dismissed insights from the same detectors, then insert fresh
  const detectors = [...new Set(insights.map((i) => i.detector))];
  await pool.query(
    `DELETE FROM insights
     WHERE detector = ANY($1)
       AND dismissed = FALSE`,
    [detectors]
  );

  for (const insight of insights) {
    await pool.query(
      `INSERT INTO insights (detector, headline, explanation, category, severity, metric_value, metric_context, pipeline_run_id)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
      [
        insight.detector,
        insight.headline,
        insight.explanation,
        insight.category,
        insight.severity,
        insight.metricValue,
        insight.metricContext ? JSON.stringify(insight.metricContext) : null,
        insight.pipelineRunId ?? null,
      ]
    );
  }

  console.log(`[insights-store] Saved ${insights.length} insights`);
}

/**
 * Get recent insights, ordered by severity then date.
 * Returns undismissed insights from the last 30 days by default.
 */
export async function getRecentInsights(limit = 20): Promise<InsightRow[]> {
  const pool = getPool();

  const res = await pool.query(
    `SELECT id, detector, headline, explanation, category, severity,
            metric_value, metric_context, detected_at, pipeline_run_id, dismissed
     FROM insights
     WHERE dismissed = FALSE
       AND detected_at >= NOW() - INTERVAL '30 days'
     ORDER BY
       CASE severity
         WHEN 'critical' THEN 0
         WHEN 'warning' THEN 1
         WHEN 'info' THEN 2
         WHEN 'positive' THEN 3
         ELSE 4
       END,
       detected_at DESC
     LIMIT $1`,
    [limit]
  );

  return res.rows.map((r) => ({
    id: r.id,
    detector: r.detector,
    headline: r.headline,
    explanation: r.explanation,
    category: r.category,
    severity: r.severity,
    metricValue: r.metric_value,
    metricContext: r.metric_context ? JSON.parse(r.metric_context) : null,
    detectedAt: new Date(r.detected_at).toISOString(),
    pipelineRunId: r.pipeline_run_id,
    dismissed: r.dismissed,
  }));
}

/**
 * Mark an insight as dismissed (future use for UI).
 */
export async function dismissInsight(id: number): Promise<void> {
  const pool = getPool();
  await pool.query(`UPDATE insights SET dismissed = TRUE WHERE id = $1`, [id]);
}
