export async function register() {
  // Initialize database and run migrations when the Next.js server starts.
  // Pipeline execution is triggered via CRON API routes (/api/cron/pipeline
  // and /api/cron/revenue), NOT via BullMQ workers.
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

      console.log("[instrumentation] Database ready. Pipeline runs via CRON API routes.");

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
      console.error("[instrumentation] Failed to initialize database:", err);
    }
  } else {
    console.log(`[instrumentation] Skipping init (runtime: ${process.env.NEXT_RUNTIME || "unknown"})`);
  }
}
