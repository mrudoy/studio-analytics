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

      console.log("[instrumentation] Starting pipeline worker (runtime: nodejs)...");
      const { startPipelineWorker } = await import("./lib/queue/pipeline-worker");
      startPipelineWorker();
      console.log("[instrumentation] Pipeline worker started successfully");

      console.log("[instrumentation] Syncing schedule...");
      const { syncSchedule } = await import("./lib/queue/scheduler");
      await syncSchedule();
      console.log("[instrumentation] Schedule sync complete");

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
