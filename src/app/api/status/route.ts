import { NextRequest } from "next/server";
import { getPipelineQueue } from "@/lib/queue/pipeline-queue";

export const dynamic = "force-dynamic";

export async function GET(request: NextRequest) {
  const jobId = request.nextUrl.searchParams.get("jobId");
  if (!jobId) {
    return new Response("Missing jobId", { status: 400 });
  }

  const encoder = new TextEncoder();
  const stream = new ReadableStream({
    async start(controller) {
      const queue = getPipelineQueue();
      let closed = false;

      function send(event: string, data: Record<string, unknown>) {
        if (closed) return;
        try {
          controller.enqueue(
            encoder.encode(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`)
          );
        } catch {
          // Controller may already be closed by the client disconnecting
          closed = true;
        }
      }

      function closeStream() {
        if (closed) return;
        closed = true;
        try { controller.close(); } catch { /* already closed */ }
      }

      // Poll for job status
      const interval = setInterval(async () => {
        try {
          const job = await queue.getJob(jobId);
          if (!job) {
            send("error", { message: "Job not found" });
            clearInterval(interval);
            closeStream();
            return;
          }

          const state = await job.getState();
          const progress = job.progress as { step?: string; percent?: number; startedAt?: number } | undefined;

          if (state === "active" || state === "waiting" || state === "delayed") {
            send("progress", {
              step: progress?.step || "Processing...",
              percent: progress?.percent || 0,
              startedAt: progress?.startedAt || 0,
            });
          } else if (state === "completed") {
            const result = job.returnvalue;
            send("complete", {
              sheetUrl: result?.sheetUrl || "",
              rawDataSheetUrl: result?.rawDataSheetUrl || "",
              duration: result?.duration || 0,
              recordCounts: result?.recordCounts || {},
              validation: result?.validation || null,
              warnings: result?.warnings || [],
            });
            clearInterval(interval);
            closeStream();
          } else if (state === "failed") {
            send("error", {
              message: job.failedReason || "Pipeline failed",
            });
            clearInterval(interval);
            closeStream();
          }
        } catch {
          if (!closed) {
            send("error", { message: "Status polling error" });
            clearInterval(interval);
            closeStream();
          }
        }
      }, 1000);

      // Timeout after 30 minutes — safety net in case pipeline is slow
      setTimeout(() => {
        if (!closed) {
          send("error", { message: "Job timed out" });
          clearInterval(interval);
          closeStream();
        }
      }, 1800000);
    },
  });

  return new Response(stream, {
    headers: {
      "Content-Type": "text/event-stream",
      "Cache-Control": "no-cache",
      Connection: "keep-alive",
    },
  });
}
