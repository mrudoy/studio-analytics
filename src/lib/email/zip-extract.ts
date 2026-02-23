/**
 * Extract CSV files from a .zip archive.
 *
 * Union.fit sends large exports as .zip files containing one or more CSVs.
 * The daily zip will contain all reports in a single archive.
 */

import AdmZip from "adm-zip";
import { writeFileSync, mkdirSync, existsSync } from "fs";
import { join, basename } from "path";

const EXTRACT_DIR = join(process.cwd(), "data", "email-attachments");

export interface ExtractedFile {
  /** Original filename inside the zip */
  originalName: string;
  /** Path where the extracted file was saved */
  filePath: string;
  /** File size in bytes */
  size: number;
}

/**
 * Extract all CSV files from a .zip archive.
 *
 * @param zipPath — Absolute path to the downloaded .zip file
 * @returns Array of extracted CSV file info
 */
export function extractCSVsFromZip(zipPath: string): ExtractedFile[] {
  if (!existsSync(EXTRACT_DIR)) {
    mkdirSync(EXTRACT_DIR, { recursive: true });
  }

  const zip = new AdmZip(zipPath);
  const entries = zip.getEntries();
  const extracted: ExtractedFile[] = [];

  for (const entry of entries) {
    const name = entry.entryName;

    // Skip directories and non-CSV files
    if (entry.isDirectory) continue;
    if (!name.toLowerCase().endsWith(".csv")) {
      console.log(`[zip-extract] Skipping non-CSV: ${name}`);
      continue;
    }

    // Extract the file
    const data = entry.getData();
    const safeName = basename(name); // strip any directory path inside zip
    const savePath = join(EXTRACT_DIR, `${Date.now()}-${safeName}`);
    writeFileSync(savePath, data);

    extracted.push({
      originalName: safeName,
      filePath: savePath,
      size: data.length,
    });

    console.log(`[zip-extract] Extracted: ${safeName} (${(data.length / 1024).toFixed(0)} KB) → ${savePath}`);
  }

  if (extracted.length === 0) {
    console.warn(`[zip-extract] No CSV files found in ${basename(zipPath)}`);
  } else {
    console.log(`[zip-extract] Extracted ${extracted.length} CSV(s) from ${basename(zipPath)}`);
  }

  return extracted;
}
