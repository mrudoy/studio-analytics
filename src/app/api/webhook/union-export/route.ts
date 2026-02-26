import { NextResponse } from "next/server";
import { enqueuePipeline } from "@/lib/queue/pipeline-queue";

/**
 * POST /api/webhook/union-export
 *
 * Webhook endpoint for Union.fit to call when a daily data export is ready.
 * Accepts a JSON payload with the download URL and enqueues a pipeline job.
 *
 * Expected payload:
 *   {
 *     "event": "export_ready",
 *     "download_url": "https://export.union.fit/downloads/abc123.zip",
 *     "generated_at": "2026-02-25T06:00:00Z",  // optional
 *     "expires_at": "2026-02-26T06:00:00Z"      // optional
 *   }
 *
 * Auth: Bearer token in Authorization header, matched against UNION_WEBHOOK_SECRET env var.
 */
export async function POST(request: Request) {
  // ── Auth ────────────────────────────────────────────────
  const secret = process.env.UNION_WEBHOOK_SECRET;
  if (!secret) {
    console.error("[webhook] UNION_WEBHOOK_SECRET not configured");
    return NextResponse.json(
      { error: "Webhook not configured" },
      { status: 503 }
    );
  }

  const authHeader = request.headers.get("authorization");
  const token = authHeader?.replace(/^Bearer\s+/i, "");

  if (!token || token !== secret) {
    console.warn("[webhook] Unauthorized webhook attempt");
    return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
  }

  // ── Parse payload ───────────────────────────────────────
  let body: Record<string, unknown>;
  try {
    body = await request.json();
  } catch {
    return NextResponse.json(
      { error: "Invalid JSON body" },
      { status: 400 }
    );
  }

  const downloadUrl = body.download_url as string | undefined;
  if (!downloadUrl || typeof downloadUrl !== "string") {
    return NextResponse.json(
      { error: "Missing required field: download_url" },
      { status: 400 }
    );
  }

  // Basic URL validation
  try {
    new URL(downloadUrl);
  } catch {
    return NextResponse.json(
      { error: "Invalid download_url" },
      { status: 400 }
    );
  }

  // Check expiry if provided
  const expiresAt = body.expires_at as string | undefined;
  if (expiresAt) {
    const expiry = new Date(expiresAt);
    if (!isNaN(expiry.getTime()) && expiry < new Date()) {
      return NextResponse.json(
        { error: "Download URL has expired" },
        { status: 410 }
      );
    }
  }

  // ── Enqueue pipeline job ────────────────────────────────
  try {
    const jobId = await enqueuePipeline({
      triggeredBy: "union-webhook",
      downloadUrl,
    });

    console.log(
      `[webhook] Pipeline enqueued: jobId=${jobId}, url=${downloadUrl.slice(0, 60)}...`
    );

    return NextResponse.json(
      { status: "accepted", jobId },
      { status: 202 }
    );
  } catch (error) {
    const message =
      error instanceof Error ? error.message : "Failed to enqueue pipeline";

    if (message.includes("already running")) {
      return NextResponse.json({ error: message }, { status: 409 });
    }

    console.error("[webhook] Failed to enqueue:", message);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
