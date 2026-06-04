const SITE_PASSWORD = "skyting";

export const COOKIE_NAME = "studio_session";
export const SESSION_MAX_AGE = 30 * 24 * 60 * 60; // 30 days in seconds
const SESSION_TTL_MS = SESSION_MAX_AGE * 1000;

function getSecret(): string {
  const s = process.env.SESSION_SECRET;
  if (!s) throw new Error("SESSION_SECRET env var is not set");
  return s;
}

async function hmacSign(secret: string, data: string): Promise<string> {
  const enc = new TextEncoder();
  const key = await crypto.subtle.importKey(
    "raw",
    enc.encode(secret),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"],
  );
  const sig = await crypto.subtle.sign("HMAC", key, enc.encode(data));
  return btoa(String.fromCharCode(...new Uint8Array(sig)))
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=/g, "");
}

async function hmacEqual(a: string, b: string): Promise<boolean> {
  if (a.length !== b.length) return false;
  let diff = 0;
  for (let i = 0; i < a.length; i++) diff |= a.charCodeAt(i) ^ b.charCodeAt(i);
  return diff === 0;
}

export async function signSession(): Promise<string> {
  const secret = getSecret();
  const expiry = String(Date.now() + SESSION_TTL_MS);
  const hmac = await hmacSign(secret, expiry);
  return `${expiry}.${hmac}`;
}

export async function verifySession(cookieValue: string): Promise<boolean> {
  try {
    const secret = process.env.SESSION_SECRET;
    if (!secret) return false;
    const dot = cookieValue.lastIndexOf(".");
    if (dot === -1) return false;
    const expiry = cookieValue.slice(0, dot);
    const hmac = cookieValue.slice(dot + 1);
    if (Number(expiry) < Date.now()) return false;
    const expected = await hmacSign(secret, expiry);
    return hmacEqual(expected, hmac);
  } catch {
    return false;
  }
}

export async function checkPassword(submitted: string): Promise<boolean> {
  const enc = new TextEncoder();
  const [a, b] = await Promise.all([
    crypto.subtle.digest("SHA-256", enc.encode(submitted)),
    crypto.subtle.digest("SHA-256", enc.encode(SITE_PASSWORD)),
  ]);
  const av = new Uint8Array(a);
  const bv = new Uint8Array(b);
  let diff = 0;
  for (let i = 0; i < av.length; i++) diff |= av[i] ^ bv[i];
  return diff === 0;
}
