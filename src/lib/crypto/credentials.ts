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

export interface AppSettings {
  credentials?: UnionCredentials;
  analyticsSpreadsheetId?: string;
  rawDataSpreadsheetId?: string;
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

export function loadSettings(): AppSettings | null {
  if (!existsSync(CREDENTIALS_FILE)) return null;
  const blob = readFileSync(CREDENTIALS_FILE, "utf8");
  if (!blob.trim()) return null;
  return decryptSettings(blob);
}

export function hasStoredCredentials(): boolean {
  const settings = loadSettings();
  return !!(settings?.credentials?.email && settings?.credentials?.password);
}
