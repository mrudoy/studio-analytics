/**
 * Cloud backup via GitHub Releases.
 *
 * Uploads gzipped backup JSON as a release asset to the studio-analytics repo.
 * This provides durable, versioned, off-site backups that survive Railway
 * redeployments, database wipes, and infrastructure changes.
 *
 * Requires GITHUB_TOKEN env var with `repo` scope.
 */

import { createBackup, type BackupData } from "./backup";
import { gzipSync, gunzipSync } from "zlib";

const GITHUB_OWNER = "mrudoy";
const GITHUB_REPO = "studio-analytics";
const BACKUP_TAG_PREFIX = "backup-";
const MAX_BACKUPS = 14; // keep last 14 cloud backups

function getToken(): string {
  const token = process.env.GITHUB_TOKEN;
  if (!token) throw new Error("GITHUB_TOKEN not set — cannot upload backup to GitHub");
  return token;
}

function githubHeaders(token: string): Record<string, string> {
  return {
    Authorization: `token ${token}`,
    Accept: "application/vnd.github.v3+json",
    "User-Agent": "studio-analytics-backup",
  };
}

/** Format a backup tag from a date. */
function makeTag(date: Date): string {
  const pad = (n: number) => String(n).padStart(2, "0");
  return `${BACKUP_TAG_PREFIX}${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}-${pad(date.getHours())}${pad(date.getMinutes())}`;
}

/** Format a human-readable title. */
function makeTitle(backup: BackupData): string {
  const totalRows = Object.values(backup.metadata.tables).reduce((a, b) => a + b, 0);
  const date = new Date(backup.metadata.createdAt);
  return `DB Backup — ${date.toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric" })} (${totalRows.toLocaleString()} rows)`;
}

// ── Upload ──────────────────────────────────────────────────

export interface CloudBackupResult {
  tag: string;
  title: string;
  url: string;
  sizeBytes: number;
  compressedBytes: number;
  totalRows: number;
}

/**
 * Create a full backup and upload it to GitHub Releases as a gzipped JSON asset.
 */
export async function uploadBackupToGitHub(backup?: BackupData): Promise<CloudBackupResult> {
  const token = getToken();
  const headers = githubHeaders(token);

  // Create backup if not provided
  if (!backup) {
    backup = await createBackup();
  }

  const tag = makeTag(new Date(backup.metadata.createdAt));
  const title = makeTitle(backup);
  const totalRows = Object.values(backup.metadata.tables).reduce((a, b) => a + b, 0);

  // Gzip the backup JSON
  const json = JSON.stringify(backup);
  const compressed = gzipSync(Buffer.from(json, "utf-8"));

  console.log(`[backup-cloud] Uploading backup: ${tag} (${totalRows} rows, ${(compressed.length / 1024).toFixed(0)}KB compressed)`);

  // Create release
  const releaseRes = await fetch(`https://api.github.com/repos/${GITHUB_OWNER}/${GITHUB_REPO}/releases`, {
    method: "POST",
    headers: { ...headers, "Content-Type": "application/json" },
    body: JSON.stringify({
      tag_name: tag,
      name: title,
      body: `Automated database backup.\n\n**Tables:**\n${Object.entries(backup.metadata.tables).map(([t, c]) => `- ${t}: ${c} rows`).join("\n")}\n\n**Size:** ${(backup.metadata.sizeBytes / 1024 / 1024).toFixed(1)}MB (${(compressed.length / 1024).toFixed(0)}KB compressed)`,
      prerelease: true, // mark as prerelease so it doesn't clutter the releases page
    }),
  });

  if (!releaseRes.ok) {
    const err = await releaseRes.text();
    // If tag already exists, try to find and update instead
    if (releaseRes.status === 422 && err.includes("already_exists")) {
      console.log(`[backup-cloud] Tag ${tag} already exists, skipping upload`);
      return {
        tag,
        title,
        url: `https://github.com/${GITHUB_OWNER}/${GITHUB_REPO}/releases/tag/${tag}`,
        sizeBytes: backup.metadata.sizeBytes,
        compressedBytes: compressed.length,
        totalRows,
      };
    }
    throw new Error(`GitHub release creation failed (${releaseRes.status}): ${err}`);
  }

  const release = await releaseRes.json();

  // Upload asset
  const uploadUrl = (release.upload_url as string).replace("{?name,label}", `?name=${tag}.json.gz`);
  const assetRes = await fetch(uploadUrl, {
    method: "POST",
    headers: {
      ...headers,
      "Content-Type": "application/gzip",
      "Content-Length": String(compressed.length),
    },
    body: compressed,
  });

  if (!assetRes.ok) {
    const err = await assetRes.text();
    throw new Error(`GitHub asset upload failed (${assetRes.status}): ${err}`);
  }

  console.log(`[backup-cloud] Backup uploaded: ${tag} → ${release.html_url}`);

  // Prune old backups (non-blocking)
  pruneGitHubBackups(MAX_BACKUPS).catch((err) =>
    console.warn("[backup-cloud] Prune failed:", err instanceof Error ? err.message : err)
  );

  return {
    tag,
    title,
    url: release.html_url,
    sizeBytes: backup.metadata.sizeBytes,
    compressedBytes: compressed.length,
    totalRows,
  };
}

// ── List ────────────────────────────────────────────────────

export interface CloudBackupEntry {
  tag: string;
  title: string;
  createdAt: string;
  url: string;
  assetUrl: string | null;
  sizeBytes: number;
}

/**
 * List backup releases from GitHub.
 */
export async function listGitHubBackups(): Promise<CloudBackupEntry[]> {
  const token = getToken();
  const headers = githubHeaders(token);

  const res = await fetch(
    `https://api.github.com/repos/${GITHUB_OWNER}/${GITHUB_REPO}/releases?per_page=50`,
    { headers }
  );

  if (!res.ok) {
    throw new Error(`GitHub list releases failed (${res.status})`);
  }

  const releases = await res.json();

  return (releases as { tag_name: string; name: string; created_at: string; html_url: string; assets: { name: string; browser_download_url: string; size: number }[] }[])
    .filter((r) => r.tag_name.startsWith(BACKUP_TAG_PREFIX))
    .map((r) => {
      const asset = r.assets.find((a) => a.name.endsWith(".json.gz"));
      return {
        tag: r.tag_name,
        title: r.name,
        createdAt: r.created_at,
        url: r.html_url,
        assetUrl: asset?.browser_download_url ?? null,
        sizeBytes: asset?.size ?? 0,
      };
    });
}

// ── Download & Restore ──────────────────────────────────────

/**
 * Download a backup from GitHub Releases by tag.
 */
export async function downloadGitHubBackup(tag: string): Promise<BackupData> {
  const token = getToken();
  const headers = githubHeaders(token);

  // Find the release
  const res = await fetch(
    `https://api.github.com/repos/${GITHUB_OWNER}/${GITHUB_REPO}/releases/tags/${tag}`,
    { headers }
  );

  if (!res.ok) {
    throw new Error(`GitHub release not found: ${tag} (${res.status})`);
  }

  const release = await res.json();
  const asset = (release.assets as { name: string; url: string }[]).find((a) => a.name.endsWith(".json.gz"));

  if (!asset) {
    throw new Error(`No backup asset found in release ${tag}`);
  }

  // Download the asset (need to use the asset API URL, not browser_download_url)
  const assetRes = await fetch(asset.url, {
    headers: {
      ...headers,
      Accept: "application/octet-stream",
    },
  });

  if (!assetRes.ok) {
    throw new Error(`Failed to download backup asset (${assetRes.status})`);
  }

  const buffer = Buffer.from(await assetRes.arrayBuffer());
  const decompressed = gunzipSync(buffer);
  return JSON.parse(decompressed.toString("utf-8")) as BackupData;
}

// ── Prune ───────────────────────────────────────────────────

/**
 * Delete old backup releases beyond keepCount.
 */
export async function pruneGitHubBackups(keepCount: number): Promise<number> {
  const token = getToken();
  const headers = githubHeaders(token);

  const backups = await listGitHubBackups();

  // Already sorted newest-first by GitHub API
  const toDelete = backups.slice(keepCount);
  let deleted = 0;

  for (const backup of toDelete) {
    try {
      // Get release ID from tag
      const res = await fetch(
        `https://api.github.com/repos/${GITHUB_OWNER}/${GITHUB_REPO}/releases/tags/${backup.tag}`,
        { headers }
      );
      if (!res.ok) continue;
      const release = await res.json();

      // Delete the release
      await fetch(
        `https://api.github.com/repos/${GITHUB_OWNER}/${GITHUB_REPO}/releases/${release.id}`,
        { method: "DELETE", headers }
      );

      // Delete the tag
      await fetch(
        `https://api.github.com/repos/${GITHUB_OWNER}/${GITHUB_REPO}/git/refs/tags/${backup.tag}`,
        { method: "DELETE", headers }
      );

      deleted++;
    } catch {
      // Non-fatal — skip this one
    }
  }

  if (deleted > 0) {
    console.log(`[backup-cloud] Pruned ${deleted} old backup(s)`);
  }

  return deleted;
}
