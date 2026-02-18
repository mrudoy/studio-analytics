import { getPool } from "./database";
import * as fs from "fs/promises";
import * as path from "path";

// Tables to back up (order matters for restore — no FK dependencies currently)
const BACKUP_TABLES = [
  "revenue_categories",
  "auto_renews",
  "first_visits",
  "registrations",
  "orders",
  "new_customers",
  "customers",
  "pipeline_runs",
  "fetch_watermarks",
] as const;

// Tables we skip on backup (raw upload archive is huge and reconstructable)
const SKIP_TABLES = ["uploaded_data", "_migrations"];

export interface BackupMetadata {
  version: 1;
  createdAt: string;
  tables: Record<string, number>; // table -> row count
  sizeBytes: number;
}

export interface BackupData {
  metadata: BackupMetadata;
  tables: Record<string, Record<string, unknown>[]>;
}

/**
 * Export all tables as a JSON backup object.
 * Returns the full backup data — caller decides where to store it.
 */
export async function createBackup(): Promise<BackupData> {
  const pool = getPool();
  const tables: Record<string, Record<string, unknown>[]> = {};
  const counts: Record<string, number> = {};

  for (const table of BACKUP_TABLES) {
    try {
      const { rows } = await pool.query(`SELECT * FROM ${table}`);
      tables[table] = rows;
      counts[table] = rows.length;
    } catch {
      // Table might not exist yet — skip silently
      tables[table] = [];
      counts[table] = 0;
    }
  }

  const backup: BackupData = {
    metadata: {
      version: 1,
      createdAt: new Date().toISOString(),
      tables: counts,
      sizeBytes: 0, // filled after serialization
    },
    tables,
  };

  // Compute size
  const serialized = JSON.stringify(backup);
  backup.metadata.sizeBytes = Buffer.byteLength(serialized, "utf-8");

  return backup;
}

/**
 * Save backup to the data/backups/ directory on disk.
 * Returns the file path and metadata.
 */
export async function saveBackupToDisk(backup: BackupData): Promise<{ filePath: string; metadata: BackupMetadata }> {
  const dir = path.join(process.cwd(), "data", "backups");
  await fs.mkdir(dir, { recursive: true });

  const timestamp = backup.metadata.createdAt.replace(/[:.]/g, "-").replace("T", "_").replace("Z", "");
  const filename = `backup-${timestamp}.json`;
  const filePath = path.join(dir, filename);

  await fs.writeFile(filePath, JSON.stringify(backup), "utf-8");

  return { filePath, metadata: backup.metadata };
}

/**
 * Save backup metadata to the database (without the full data, to keep the DB small).
 */
export async function saveBackupMetadata(metadata: BackupMetadata, filePath: string): Promise<void> {
  const pool = getPool();

  await pool.query(`
    CREATE TABLE IF NOT EXISTS db_backups (
      id SERIAL PRIMARY KEY,
      created_at TIMESTAMPTZ NOT NULL,
      file_path TEXT,
      table_counts JSONB,
      size_bytes INTEGER,
      status TEXT DEFAULT 'completed'
    )
  `);

  await pool.query(
    `INSERT INTO db_backups (created_at, file_path, table_counts, size_bytes)
     VALUES ($1, $2, $3, $4)`,
    [metadata.createdAt, filePath, JSON.stringify(metadata.tables), metadata.sizeBytes]
  );
}

/**
 * Restore from a backup JSON object.
 * WARNING: This truncates all tables and replaces with backup data.
 */
export async function restoreFromBackup(backup: BackupData): Promise<{ tablesRestored: number; totalRows: number }> {
  const pool = getPool();
  let tablesRestored = 0;
  let totalRows = 0;

  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    for (const table of BACKUP_TABLES) {
      const rows = backup.tables[table];
      if (!rows || rows.length === 0) continue;

      // Truncate existing data
      await client.query(`TRUNCATE TABLE ${table} RESTART IDENTITY CASCADE`);

      // Insert rows in batches of 100
      for (let i = 0; i < rows.length; i += 100) {
        const batch = rows.slice(i, i + 100);
        for (const row of batch) {
          const cols = Object.keys(row).filter((k) => k !== "id"); // skip serial ID
          const vals = cols.map((c) => (row as Record<string, unknown>)[c]);
          const placeholders = cols.map((_, j) => `$${j + 1}`).join(", ");
          const colNames = cols.map((c) => `"${c}"`).join(", ");

          await client.query(
            `INSERT INTO ${table} (${colNames}) VALUES (${placeholders})`,
            vals
          );
        }
      }

      tablesRestored++;
      totalRows += rows.length;
    }

    await client.query("COMMIT");
  } catch (err) {
    await client.query("ROLLBACK");
    throw err;
  } finally {
    client.release();
  }

  return { tablesRestored, totalRows };
}

/**
 * List available backups (from db_backups table).
 */
export async function listBackups(): Promise<{ id: number; createdAt: string; filePath: string; tableCounts: Record<string, number>; sizeBytes: number }[]> {
  const pool = getPool();

  // Ensure table exists
  await pool.query(`
    CREATE TABLE IF NOT EXISTS db_backups (
      id SERIAL PRIMARY KEY,
      created_at TIMESTAMPTZ NOT NULL,
      file_path TEXT,
      table_counts JSONB,
      size_bytes INTEGER,
      status TEXT DEFAULT 'completed'
    )
  `);

  const { rows } = await pool.query(
    `SELECT id, created_at, file_path, table_counts, size_bytes
     FROM db_backups
     ORDER BY created_at DESC
     LIMIT 20`
  );

  return rows.map((r) => ({
    id: r.id,
    createdAt: r.created_at,
    filePath: r.file_path,
    tableCounts: r.table_counts || {},
    sizeBytes: r.size_bytes || 0,
  }));
}

/**
 * Load a backup from a file path on disk.
 */
export async function loadBackupFromDisk(filePath: string): Promise<BackupData> {
  const content = await fs.readFile(filePath, "utf-8");
  return JSON.parse(content) as BackupData;
}

/**
 * Prune old backups, keeping only the last N.
 */
export async function pruneBackups(keepCount: number = 7): Promise<number> {
  const dir = path.join(process.cwd(), "data", "backups");

  let files: string[];
  try {
    files = await fs.readdir(dir);
  } catch {
    return 0; // directory doesn't exist yet
  }

  const backupFiles = files
    .filter((f) => f.startsWith("backup-") && f.endsWith(".json"))
    .sort()
    .reverse(); // newest first

  let pruned = 0;
  for (let i = keepCount; i < backupFiles.length; i++) {
    await fs.unlink(path.join(dir, backupFiles[i]));
    pruned++;
  }

  // Also prune db_backups table
  if (pruned > 0) {
    const pool = getPool();
    try {
      await pool.query(
        `DELETE FROM db_backups WHERE id NOT IN (
          SELECT id FROM db_backups ORDER BY created_at DESC LIMIT $1
        )`,
        [keepCount]
      );
    } catch {
      // Table might not exist — ignore
    }
  }

  return pruned;
}
