import type { ConnectionOptions } from "bullmq";

/** Build BullMQ connection from REDIS_URL env var (default: local dev Redis). */
export function getRedisConnection(): ConnectionOptions {
  // Default matches env.ts Zod schema — keep in sync
  const url = process.env.REDIS_URL || "redis://localhost:6379";
  const parsed = new URL(url);

  return {
    host: parsed.hostname,
    port: parseInt(parsed.port || "6379"),
    password: parsed.password || undefined,
    maxRetriesPerRequest: null,
  };
}
