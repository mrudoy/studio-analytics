import { GoogleSpreadsheet, GoogleSpreadsheetWorksheet } from "google-spreadsheet";
import { JWT } from "google-auth-library";

let cachedAuth: JWT | null = null;

function getAuth(): JWT {
  if (cachedAuth) return cachedAuth;

  const email = process.env.GOOGLE_SERVICE_ACCOUNT_EMAIL;
  const key = process.env.GOOGLE_PRIVATE_KEY;

  if (!email || !key) {
    throw new Error("Google Service Account credentials not configured. Set GOOGLE_SERVICE_ACCOUNT_EMAIL and GOOGLE_PRIVATE_KEY.");
  }

  cachedAuth = new JWT({
    email,
    key: key.replace(/\\n/g, "\n"),
    scopes: ["https://www.googleapis.com/auth/spreadsheets"],
  });

  return cachedAuth;
}

export async function getSpreadsheet(spreadsheetId: string): Promise<GoogleSpreadsheet> {
  const auth = getAuth();
  const doc = new GoogleSpreadsheet(spreadsheetId, auth);
  await doc.loadInfo();
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
