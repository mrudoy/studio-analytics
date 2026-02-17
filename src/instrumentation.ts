export async function register() {
  // Start the BullMQ worker and scheduler when the Next.js server starts
  if (process.env.NEXT_RUNTIME === "nodejs") {
    try {
      // Initialize PostgreSQL schema
      console.log("[instrumentation] Initializing database schema...");
      const { initDatabase } = await import("./lib/db/database");
      await initDatabase();
      console.log("[instrumentation] Database schema initialized");

      console.log("[instrumentation] Starting pipeline worker (runtime: nodejs)...");
      const { startPipelineWorker } = await import("./lib/queue/pipeline-worker");
      startPipelineWorker();
      console.log("[instrumentation] Pipeline worker started successfully");

      console.log("[instrumentation] Syncing schedule...");
      const { syncSchedule } = await import("./lib/queue/scheduler");
      await syncSchedule();
      console.log("[instrumentation] Schedule sync complete");
    } catch (err) {
      console.error("[instrumentation] Failed to start pipeline worker:", err);
    }
  } else {
    console.log(`[instrumentation] Skipping worker start (runtime: ${process.env.NEXT_RUNTIME || "unknown"})`);
  }
}
