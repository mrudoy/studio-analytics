import { GoogleSpreadsheet, GoogleSpreadsheetWorksheet } from "google-spreadsheet";
import { JWT } from "google-auth-library";

let cachedAuth: JWT | null = null;

/**
 * Clean up the private key string from environment variables.
 * Railway and other hosts may store the key with literal "\n" strings,
 * double-escaped newlines, or missing PEM headers.
 */
function cleanPrivateKey(rawKey: string): string {
  let key = rawKey;
  // Handle double-escaped newlines (\\n → \n)
  key = key.replace(/\\\\n/g, "\n");
  // Handle single-escaped newlines (\n → actual newline)
  key = key.replace(/\\n/g, "\n");
  // Strip surrounding quotes if present
  key = key.replace(/^["']|["']$/g, "");
  // Ensure proper PEM format
  if (!key.includes("-----BEGIN")) {
    key = `-----BEGIN PRIVATE KEY-----\n${key}\n-----END PRIVATE KEY-----\n`;
  }
  return key;
}

function getAuth(): JWT {
  if (cachedAuth) return cachedAuth;

  const email = process.env.GOOGLE_SERVICE_ACCOUNT_EMAIL;
  const key = process.env.GOOGLE_PRIVATE_KEY;

  if (!email || !key) {
    throw new Error("Google Service Account credentials not configured. Set GOOGLE_SERVICE_ACCOUNT_EMAIL and GOOGLE_PRIVATE_KEY.");
  }

  cachedAuth = new JWT({
    email,
    key: cleanPrivateKey(key),
    scopes: ["https://www.googleapis.com/auth/spreadsheets"],
  });

  return cachedAuth;
}

export async function getSpreadsheet(spreadsheetId: string): Promise<GoogleSpreadsheet> {
  const auth = getAuth();
  const doc = new GoogleSpreadsheet(spreadsheetId, auth);
  try {
    await doc.loadInfo();
  } catch (err) {
    // Clear cached auth so next attempt re-creates the JWT
    cachedAuth = null;
    throw err;
  }
  return doc;
}

export async function getOrCreateSheet(
  doc: GoogleSpreadsheet,
  title: string,
  headers: string[]
): Promise<GoogleSpreadsheetWorksheet> {
  let sheet = doc.sheetsByTitle[title];

  if (sheet) {
    await sheet.clear();
    await sheet.setHeaderRow(headers);
  } else {
    sheet = await doc.addSheet({ title, headerValues: headers });
  }

  return sheet;
}

export async function writeRows(
  sheet: GoogleSpreadsheetWorksheet,
  rows: Record<string, string | number>[]
): Promise<void> {
  if (rows.length === 0) return;
  await sheet.addRows(rows);
}

export function getSheetUrl(spreadsheetId: string): string {
  return `https://docs.google.com/spreadsheets/d/${spreadsheetId}`;
}
