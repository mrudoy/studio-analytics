import { mkdirSync, existsSync, readdirSync, unlinkSync } from "fs";
import { join } from "path";

const DOWNLOADS_DIR = join(process.cwd(), "data", "downloads");

export function ensureDownloadDir(): string {
  if (!existsSync(DOWNLOADS_DIR)) {
    mkdirSync(DOWNLOADS_DIR, { recursive: true });
  }
  return DOWNLOADS_DIR;
}

export function cleanupDownloads(): void {
  if (!existsSync(DOWNLOADS_DIR)) return;

  const files = readdirSync(DOWNLOADS_DIR);
  for (const file of files) {
    if (file.endsWith(".csv")) {
      try {
        unlinkSync(join(DOWNLOADS_DIR, file));
      } catch {
        // Ignore cleanup errors
      }
    }
  }
}

export function getDownloadPath(reportType: string): string {
  ensureDownloadDir();
  return join(DOWNLOADS_DIR, `${reportType}-${Date.now()}.csv`);
}
