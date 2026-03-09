export async function register() {
  // Start the BullMQ worker and scheduler when the Next.js server starts
  if (process.env.NEXT_RUNTIME === "nodejs") {
    try {
      // Initialize PostgreSQL schema
      console.log("[instrumentation] Initializing database schema...");
      const { initDatabase } = await import("./lib/db/database");
      await initDatabase();
      console.log("[instrumentation] Database schema initialized");

      // Run pending migrations
      const { runMigrations } = await import("./lib/db/migrations");
      await runMigrations();

      // Ensure data_version table for DB-based cache invalidation
      const { ensureDataVersionTable } = await import("./lib/cache/stats-cache");
      await ensureDataVersionTable();

      console.log("[instrumentation] Starting pipeline worker (runtime: nodejs)...");
      const { startPipelineWorker } = await import("./lib/queue/pipeline-worker");
      startPipelineWorker();
      console.log("[instrumentation] Pipeline worker started successfully");

      console.log("[instrumentation] Syncing schedule...");
      const { syncSchedule } = await import("./lib/queue/scheduler");
      await syncSchedule();
      console.log("[instrumentation] Schedule sync complete");

      // Boot-time catch-up: if the pipeline missed runs while the server
      // was down, auto-enqueue after a 60s delay to let Redis stabilize.
      setTimeout(async () => {
        try {
          const { checkAndCatchUp } = await import("./lib/queue/catchup");
          const result = await checkAndCatchUp();
          console.log(`[instrumentation] Catch-up check: ${result.reason}`);
        } catch (err) {
          console.warn("[instrumentation] Catch-up check failed:", err instanceof Error ? err.message : err);
        }
      }, 60_000);

      // Graceful shutdown
      const shutdown = async (signal: string) => {
        console.log(`[instrumentation] ${signal} received, shutting down...`);
        try {
          const { closePool } = await import("./lib/db/database");
          await closePool();
        } catch (err) {
          console.error("[instrumentation] Error during shutdown:", err);
        }
        process.exit(0);
      };
      process.on("SIGTERM", () => shutdown("SIGTERM"));
      process.on("SIGINT", () => shutdown("SIGINT"));
    } catch (err) {
      console.error("[instrumentation] Failed to start pipeline worker:", err);
    }
  } else {
    console.log(`[instrumentation] Skipping worker start (runtime: ${process.env.NEXT_RUNTIME || "unknown"})`);
  }
}
