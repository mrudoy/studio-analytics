import { createCipheriv, createDecipheriv, randomBytes } from "crypto";
import { readFileSync, writeFileSync, existsSync } from "fs";
import { join } from "path";

const CREDENTIALS_FILE = join(process.cwd(), "data", "credentials.enc");
const ALGORITHM = "aes-256-gcm";

function getKey(): Buffer {
  const key = process.env.ENCRYPTION_MASTER_KEY;
  if (!key) throw new Error("ENCRYPTION_MASTER_KEY not set");
  // Use first 32 bytes of hex-encoded key, or pad if needed
  const keyBuffer = Buffer.from(key, "hex");
  if (keyBuffer.length < 16) throw new Error("ENCRYPTION_MASTER_KEY too short");
  // Ensure 32 bytes for AES-256
  const fullKey = Buffer.alloc(32);
  keyBuffer.copy(fullKey);
  return fullKey;
}

export interface UnionCredentials {
  email: string;
  password: string;
}

export interface ScheduleConfig {
  enabled: boolean;
  cronPattern: string;  // e.g. "0 10,16 * * *" for 10am and 4pm daily
  timezone: string;     // e.g. "America/New_York"
}

export interface RobotEmailConfig {
  /** Robot email address (e.g. robot@skyting.com) — used for both Union.fit login and Gmail inbox */
  address: string;
  /** Gmail App Password (only needed if not using service account domain-wide delegation) */
  appPassword?: string;
}

export interface ShopifyConfig {
  /** Shopify store name (e.g. "skyting" for skyting.myshopify.com) */
  storeName: string;
  /** OAuth Client ID from Shopify Dev Dashboard */
  clientId: string;
  /** OAuth Client Secret from Shopify Dev Dashboard */
  clientSecret: string;
}

export interface EmailDigestConfig {
  /** Whether the digest is active (false = paused) */
  enabled: boolean;
  /** Recipient email addresses */
  recipients: string[];
  /** Resend API key (stored encrypted alongside other credentials) */
  resendApiKey?: string;
  /** From address for outgoing emails (e.g. "Sky Ting <digest@skyting.com>") */
  fromAddress?: string;
}

export interface AppSettings {
  credentials?: UnionCredentials;
  robotEmail?: RobotEmailConfig;
  analyticsSpreadsheetId?: string;
  rawDataSpreadsheetId?: string;
  schedule?: ScheduleConfig;
  shopify?: ShopifyConfig;
  emailDigest?: EmailDigestConfig;
}

export function encryptSettings(settings: AppSettings): string {
  const key = getKey();
  const iv = randomBytes(12);
  const cipher = createCipheriv(ALGORITHM, key, iv);

  const plaintext = JSON.stringify(settings);
  let encrypted = cipher.update(plaintext, "utf8", "hex");
  encrypted += cipher.final("hex");
  const authTag = cipher.getAuthTag();

  // Format: iv:authTag:ciphertext (all hex)
  return `${iv.toString("hex")}:${authTag.toString("hex")}:${encrypted}`;
}

export function decryptSettings(blob: string): AppSettings {
  const key = getKey();
  const parts = blob.split(":");
  if (parts.length !== 3) throw new Error("Invalid encrypted data format");

  const iv = Buffer.from(parts[0], "hex");
  const authTag = Buffer.from(parts[1], "hex");
  const ciphertext = parts[2];

  const decipher = createDecipheriv(ALGORITHM, key, iv);
  decipher.setAuthTag(authTag);

  let decrypted = decipher.update(ciphertext, "hex", "utf8");
  decrypted += decipher.final("utf8");

  return JSON.parse(decrypted);
}

export function saveSettings(settings: AppSettings): void {
  const encrypted = encryptSettings(settings);
  writeFileSync(CREDENTIALS_FILE, encrypted, "utf8");
}

/**
 * Build AppSettings from environment variables.
 * Used on Railway where there's no persistent volume for credentials.enc.
 */
function loadSettingsFromEnv(): AppSettings | null {
  const email = process.env.UNION_EMAIL;
  const password = process.env.UNION_PASSWORD;

  const shopify = process.env.SHOPIFY_STORE_NAME && process.env.SHOPIFY_CLIENT_ID && process.env.SHOPIFY_CLIENT_SECRET
    ? {
        storeName: process.env.SHOPIFY_STORE_NAME,
        clientId: process.env.SHOPIFY_CLIENT_ID,
        clientSecret: process.env.SHOPIFY_CLIENT_SECRET,
      }
    : undefined;

  const emailDigest: EmailDigestConfig | undefined = process.env.RESEND_API_KEY
    ? {
        enabled: true,
        recipients: process.env.DIGEST_RECIPIENTS
          ? process.env.DIGEST_RECIPIENTS.split(",").map((e) => e.trim()).filter(Boolean)
          : [],
        resendApiKey: process.env.RESEND_API_KEY,
        fromAddress: process.env.DIGEST_FROM_ADDRESS,
      }
    : undefined;

  // Return settings if we have at least Union credentials or Shopify config
  if (!email || !password) {
    // No Union creds — still return partial settings if Shopify is configured
    if (!shopify && !emailDigest) return null;
    return { shopify, emailDigest };
  }

  return {
    credentials: { email, password },
    robotEmail: process.env.ROBOT_EMAIL
      ? { address: process.env.ROBOT_EMAIL }
      : undefined,
    analyticsSpreadsheetId: process.env.ANALYTICS_SPREADSHEET_ID,
    rawDataSpreadsheetId: process.env.RAW_DATA_SPREADSHEET_ID,
    schedule: process.env.SCHEDULE_CRON
      ? {
          enabled: true,
          cronPattern: process.env.SCHEDULE_CRON,
          timezone: process.env.SCHEDULE_TIMEZONE || "America/New_York",
        }
      : undefined,
    shopify,
    emailDigest,
  };
}

export function loadSettings(): AppSettings | null {
  // Env vars take precedence (works on Railway without persistent volume)
  const envSettings = loadSettingsFromEnv();
  if (envSettings) return envSettings;

  // Fall back to encrypted file (local dev)
  try {
    if (!existsSync(CREDENTIALS_FILE)) return null;
    const blob = readFileSync(CREDENTIALS_FILE, "utf8");
    if (!blob.trim()) return null;
    return decryptSettings(blob);
  } catch {
    // Missing ENCRYPTION_MASTER_KEY or corrupt file — skip
    return null;
  }
}

export function hasStoredCredentials(): boolean {
  const settings = loadSettings();
  return !!(settings?.credentials?.email && settings?.credentials?.password);
}
