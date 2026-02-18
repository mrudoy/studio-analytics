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

/**
 * GET /api/backup — Create a new backup and return it.
 *
 * Query params:
 *   ?action=list    — List available backups (no new backup created)
 *   ?action=create  — Create backup, save to disk, return metadata (default)
 *   ?action=download — Create backup and return full JSON as download
 */
export async function GET(request: NextRequest) {
  const action = request.nextUrl.searchParams.get("action") || "create";

  try {
    if (action === "list") {
      const backups = await listBackups();
      return NextResponse.json({ backups });
    }

    // Create backup
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
    if (body.filePath) {
      console.log(`[backup] Restoring from file: ${body.filePath}`);
      backup = await loadBackupFromDisk(body.filePath);
    } else if (body.backup) {
      console.log("[backup] Restoring from inline data");
      backup = body.backup;
    } else {
      return NextResponse.json(
        { error: "Provide either 'filePath' or 'backup' in request body" },
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
    });
  } catch (err) {
    console.error("[backup] Restore error:", err);
    return NextResponse.json(
      { error: err instanceof Error ? err.message : "Restore failed" },
      { status: 500 }
    );
  }
}
