/**
 * Backup/restore endpoint.
 *
 * GET actions:
 *   ?action=list         — List local disk backups
 *   ?action=create       — Create backup, save to disk (default)
 *   ?action=download     — Create backup, return full JSON as download
 *   ?action=cloud-list   — List cloud (GitHub Releases) backups
 *   ?action=cloud-upload — Create backup + upload to GitHub Releases
 *
 * POST body:
 *   { "filePath": "..." }    — Restore from local file
 *   { "backup": { ... } }    — Restore from inline JSON
 *   { "tag": "backup-..." }  — Restore from GitHub Releases
 */
import { NextRequest, NextResponse } from "next/server";
import {
  createBackup,
  saveBackupToDisk,
  saveBackupMetadata,
  restoreFromBackup,
  listBackups,
  loadBackupFromDisk,
  pruneBackups,
} from "@/lib/db/backup";
import {
  uploadBackupToGitHub,
  listGitHubBackups,
  downloadGitHubBackup,
} from "@/lib/db/backup-cloud";

export async function GET(request: NextRequest) {
  const action = request.nextUrl.searchParams.get("action") || "create";

  try {
    // ── Cloud: list GitHub Releases backups ──
    if (action === "cloud-list") {
      const backups = await listGitHubBackups();
      return NextResponse.json({ backups });
    }

    // ── Cloud: create + upload to GitHub ──
    if (action === "cloud-upload") {
      console.log("[backup] Creating backup for cloud upload...");
      const start = Date.now();
      const backup = await createBackup();
      const cloud = await uploadBackupToGitHub(backup);

      // Also save locally
      const { filePath, metadata } = await saveBackupToDisk(backup);
      await saveBackupMetadata(metadata, filePath);
      await pruneBackups(7);

      const elapsed = Date.now() - start;
      console.log(`[backup] Cloud upload complete: ${cloud.tag} in ${elapsed}ms`);

      return NextResponse.json({
        status: "ok",
        cloud,
        metadata,
        filePath,
        elapsedMs: elapsed,
      });
    }

    // ── Local: list disk backups ──
    if (action === "list") {
      const backups = await listBackups();
      return NextResponse.json({ backups });
    }

    // ── Local: create backup ──
    console.log("[backup] Creating backup...");
    const start = Date.now();
    const backup = await createBackup();
    const elapsed = Date.now() - start;

    const totalRows = Object.values(backup.metadata.tables).reduce((a, b) => a + b, 0);
    console.log(`[backup] Backup created: ${totalRows} rows across ${Object.keys(backup.metadata.tables).length} tables in ${elapsed}ms`);

    if (action === "download") {
      // Return full backup as downloadable JSON
      const json = JSON.stringify(backup, null, 2);
      return new NextResponse(json, {
        status: 200,
        headers: {
          "Content-Type": "application/json",
          "Content-Disposition": `attachment; filename="studio-backup-${new Date().toISOString().slice(0, 10)}.json"`,
        },
      });
    }

    // Default: save to disk + record metadata
    const { filePath, metadata } = await saveBackupToDisk(backup);
    await saveBackupMetadata(metadata, filePath);

    // Prune old backups (keep last 7)
    const pruned = await pruneBackups(7);
    if (pruned > 0) {
      console.log(`[backup] Pruned ${pruned} old backup(s)`);
    }

    return NextResponse.json({
      status: "ok",
      metadata,
      filePath,
      pruned,
      elapsedMs: elapsed,
    });
  } catch (err) {
    console.error("[backup] Error:", err);
    return NextResponse.json(
      { error: err instanceof Error ? err.message : "Backup failed" },
      { status: 500 }
    );
  }
}

/**
 * POST /api/backup — Restore from a backup.
 *
 * Body options:
 *   { "filePath": "/path/to/backup.json" }  — Restore from a file on disk
 *   { "backup": { ... } }                    — Restore from inline JSON
 */
export async function POST(request: NextRequest) {
  try {
    const body = await request.json();

    let backup;
    if (body.tag) {
      // ── Restore from GitHub Releases ──
      console.log(`[backup] Downloading cloud backup: ${body.tag}`);
      backup = await downloadGitHubBackup(body.tag);
      console.log(`[backup] Cloud backup downloaded, restoring...`);
    } else if (body.filePath) {
      console.log(`[backup] Restoring from file: ${body.filePath}`);
      backup = await loadBackupFromDisk(body.filePath);
    } else if (body.backup) {
      console.log("[backup] Restoring from inline data");
      backup = body.backup;
    } else {
      return NextResponse.json(
        { error: "Provide 'tag' (cloud), 'filePath' (local), or 'backup' (inline) in request body" },
        { status: 400 }
      );
    }

    // Validate backup structure
    if (!backup.metadata || !backup.tables) {
      return NextResponse.json(
        { error: "Invalid backup format: missing metadata or tables" },
        { status: 400 }
      );
    }

    const start = Date.now();
    const result = await restoreFromBackup(backup);
    const elapsed = Date.now() - start;

    console.log(`[backup] Restore complete: ${result.totalRows} rows in ${result.tablesRestored} tables (${elapsed}ms)`);

    return NextResponse.json({
      status: "ok",
      ...result,
      elapsedMs: elapsed,
      restoredFrom: backup.metadata.createdAt,
      source: body.tag ? `cloud:${body.tag}` : body.filePath ? `disk:${body.filePath}` : "inline",
    });
  } catch (err) {
    console.error("[backup] Restore error:", err);
    return NextResponse.json(
      { error: err instanceof Error ? err.message : "Restore failed" },
      { status: 500 }
    );
  }
}
