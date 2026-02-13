import { NextResponse } from "next/server";
import { getPipelineQueue } from "@/lib/queue/pipeline-queue";

export async function GET() {
  try {
    const queue = getPipelineQueue();

    // Get recent completed jobs
    const completed = await queue.getCompleted(0, 10);
    const failed = await queue.getFailed(0, 5);

    const results = completed
      .sort((a, b) => (b.finishedOn || 0) - (a.finishedOn || 0))
      .map((job) => ({
        jobId: job.id,
        completedAt: job.finishedOn ? new Date(job.finishedOn).toISOString() : null,
        duration: job.returnvalue?.duration || 0,
        sheetUrl: job.returnvalue?.sheetUrl || "",
        rawDataSheetUrl: job.returnvalue?.rawDataSheetUrl || "",
        recordCounts: job.returnvalue?.recordCounts || {},
        warnings: job.returnvalue?.warnings?.length || 0,
      }));

    const errors = failed.map((job) => ({
      jobId: job.id,
      failedAt: job.finishedOn ? new Date(job.finishedOn).toISOString() : null,
      error: job.failedReason,
    }));

    return NextResponse.json({
      latest: results[0] || null,
      history: results,
      recentErrors: errors,
    });
  } catch {
    return NextResponse.json({ latest: null, history: [], recentErrors: [] });
  }
}
