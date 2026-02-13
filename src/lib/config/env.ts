import { z } from "zod";

const envSchema = z.object({
  ENCRYPTION_MASTER_KEY: z
    .string()
    .min(32, "ENCRYPTION_MASTER_KEY must be at least 32 characters (hex-encoded 16+ bytes)"),
  REDIS_URL: z.string().default("redis://localhost:6379"),
  GOOGLE_SERVICE_ACCOUNT_EMAIL: z.string().email().optional(),
  GOOGLE_PRIVATE_KEY: z.string().optional(),
  ANALYTICS_SPREADSHEET_ID: z.string().optional(),
  RAW_DATA_SPREADSHEET_ID: z.string().optional(),
  NODE_ENV: z.enum(["development", "production", "test"]).default("development"),
});

export type Env = z.infer<typeof envSchema>;

let _env: Env | null = null;

export function getEnv(): Env {
  if (_env) return _env;
  _env = envSchema.parse(process.env);
  return _env;
}
