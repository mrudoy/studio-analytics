import { NextResponse } from "next/server";
import { enqueuePipeline, clearQueue } from "@/lib/queue/pipeline-queue";

export async function POST(request: Request) {
  try {
    const body = await request.json().catch(() => ({}));

    const jobId = await enqueuePipeline({
      triggeredBy: "web-ui",
      dateRangeStart: body.dateRangeStart,
      dateRangeEnd: body.dateRangeEnd,
    });

    return NextResponse.json({ jobId });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Failed to start pipeline";

    if (message.includes("already running")) {
      return NextResponse.json({ error: message }, { status: 409 });
    }

    return NextResponse.json({ error: message }, { status: 500 });
  }
}

/**
 * DELETE /api/pipeline — Clear all stale/stuck jobs from the queue.
 */
export async function DELETE() {
  try {
    const result = await clearQueue();
    return NextResponse.json({ success: true, ...result });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Failed to clear queue";
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
