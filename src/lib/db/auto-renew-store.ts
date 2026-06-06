import type { PoolClient } from "pg";
import { getPool } from "./database";
import { toEasternDate } from "./eastern-date";
import { getCategory, isAnnualPlan } from "../analytics/categories";
import { BILLING_STATES, ACTIVE_STATES_SQL } from "../analytics/metrics/filters";
import type { AutoRenewCategory } from "@/types/union-data";

// ── Types ────────────────────────────────────────────────────

export interface AutoRenewRow {
  planName: string;
  planState: string;
  planPrice: number;
  customerName: string;
  customerEmail: string;
  createdAt: string;
  orderId?: string;
  salesChannel?: string;
  canceledAt?: string;
  /**
   * When the user clicked cancel (entered Pending Cancel state) per Union.
   * Distinct from canceledAt which is the period-end (Canceled state) date.
   * This is the click-date signal used for cancellation-window churn counts.
   */
  pendingCanceledAt?: string;
  canceledBy?: string;
  admin?: string;
  currentState?: string;
  currentPlan?: string;
  /** Union.fit pass ID from raw export (for precise dedup) */
  unionPassId?: string;
}

export interface StoredAutoRenew {
  id: number;
  snapshotId: string | null;
  planName: string;
  planState: string;
  planPrice: number;
  customerName: string;
  customerEmail: string;
  createdAt: string;
  canceledAt: string | null;
  category: AutoRenewCategory;
  isAnnual: boolean;
  /** Monthly rate: price / 12 for annual plans, price for monthly */
  monthlyRate: number;
}

export interface AutoRenewStats {
  active: {
    member: number;
    sky3: number;
    skyTingTv: number;
    unknown: number;
    total: number;
  };
  mrr: {
    member: number;
    sky3: number;
    skyTingTv: number;
    unknown: number;
    total: number;
  };
  arpu: {
    member: number;
    sky3: number;
    skyTingTv: number;
    overall: number;
  };
}

// ── Write Operations ─────────────────────────────────────────

/**
 * Postgres advisory-lock key serializing all bulk auto_renews writes.
 * Both saveAutoRenews() (daily pipeline) and reconcileFromFullExport()
 * (reconcile.ts) acquire pg_advisory_xact_lock(this) at the start of their
 * transactions, so a reconcile and a concurrent daily delta cannot interleave
 * — the lock auto-releases on COMMIT/ROLLBACK. Cross-process safe.
 */
export const AUTO_RENEW_WRITE_LOCK = 472900;

/**
 * Save a batch of auto-renews using UPSERT (INSERT ... ON CONFLICT DO UPDATE).
 *
 * Dedup key: (customer_email, plan_name, created_at) — from migration 003.
 * On conflict, mutable fields are updated (plan_state, plan_price, canceled_at, etc.)
 * so the database always reflects the latest known state while preserving history.
 *
 * This replaces the old DELETE + INSERT approach that destroyed historical records.
 */
export async function saveAutoRenews(
  snapshotId: string,
  rows: AutoRenewRow[]
): Promise<{ inserted: number; updated: number }> {
  const pool = getPool();
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    // Serialize against a concurrent reconcile (Phase 0.5 concurrency control).
    await client.query("SELECT pg_advisory_xact_lock($1)", [AUTO_RENEW_WRITE_LOCK]);
    const res = await upsertAutoRenewRowsTx(client, snapshotId, rows);
    await client.query("COMMIT");
    console.log(
      `[auto-renew-store] Upserted ${rows.length} auto-renews (snapshot: ${snapshotId}, ` +
        `${res.inserted} inserted, ${res.updated} updated)`,
    );
    return res;
  } catch (e) {
    await client.query("ROLLBACK");
    throw e;
  } finally {
    client.release();
  }
}

/**
 * Upsert auto-renew rows on an EXISTING transaction (caller owns BEGIN/COMMIT
 * and client.release()). Enables the reconcile orchestrator to run the upsert
 * and the cancel pass atomically in one transaction. saveAutoRenews() wraps
 * this for standalone callers.
 */
export async function upsertAutoRenewRowsTx(
  client: PoolClient,
  snapshotId: string,
  rows: AutoRenewRow[],
): Promise<{ inserted: number; updated: number }> {
  let inserted = 0;
  let updated = 0;

    // Dedupe input rows by conflict key before writing.
    //
    // Union.fit can have multiple pass records for the same subscription
    // (different union_pass_id, same customer + plan_name + created_at).
    // Without dedup, each row writes the DB sequentially and whichever is
    // processed LAST wins the plan_state — and order varies between runs.
    // Result: identical rows flip states every pipeline import, generating
    // fake churn+resume events and inflating the overview cancellation count.
    //
    // Fix: for each (email, plan_name, created_at) key, keep ONE row — the
    // one with the highest union_pass_id (newer Union.fit record = more
    // authoritative). Rows without union_pass_id fall back to last-seen order.
    const dedupedRows = (() => {
      const byKey = new Map<string, AutoRenewRow>();
      for (const row of rows) {
        if (!row.customerEmail) continue;
        const key = `${row.customerEmail.toLowerCase()}|${row.planName}|${row.createdAt || ""}`;
        const existing = byKey.get(key);
        if (!existing) {
          byKey.set(key, row);
          continue;
        }
        // Prefer the row with the higher union_pass_id (numeric compare)
        const existingId = existing.unionPassId ? parseInt(existing.unionPassId) : 0;
        const newId = row.unionPassId ? parseInt(row.unionPassId) : 0;
        if (newId > existingId) byKey.set(key, row);
      }
      return Array.from(byKey.values());
    })();
    if (dedupedRows.length < rows.length) {
      console.log(`[auto-renew-store] Deduped ${rows.length - dedupedRows.length} duplicate pass rows before save`);
    }

    let skipped = 0;
    for (const row of dedupedRows) {
      // Skip rows without email — they can't be deduped
      if (!row.customerEmail) continue;

      // Per-row savepoint: a single row that violates idx_ar_dedup (e.g. an
      // UPDATE-by-union_pass_id step would shift this row's
      // (customer_email, plan_name, created_at) onto a triple already held
      // by another row) must NOT abort the whole batch. Without this, ONE
      // bad row caused the entire export to fail with `duplicate key value
      // violates unique constraint "idx_ar_dedup"`.
      await client.query("SAVEPOINT row_save");
      let updatedExisting = false;
      try {

      const values = [
        snapshotId,
        row.planName,
        row.planState,
        row.planPrice,
        row.customerName,
        row.customerEmail.toLowerCase(),
        // created_at / canceled_at are DATE columns — store Union's EASTERN calendar
        // date so a "-0500" evening timestamp isn't rolled to +1 day in the UTC
        // session. See eastern-date.ts. (pending_canceled_at below is TIMESTAMPTZ —
        // it keeps full precision and is NOT date-coerced.)
        toEasternDate(row.createdAt),
        row.orderId || null,
        row.salesChannel || null,
        toEasternDate(row.canceledAt),
        row.canceledBy || null,
        row.admin || null,
        row.currentState || null,
        row.currentPlan || null,
        row.unionPassId || null,
        getCategory(row.planName),
        row.pendingCanceledAt || null,
      ];

      // If union_pass_id is provided, try UPDATE-by-id first. This handles
      // the case where the same subscription arrives with a different business
      // key (email/plan_name/created_at drift), which would otherwise collide
      // on the partial unique index idx_ar_union_pass_id.
      //
      // This replaces the old DELETE-then-INSERT workaround, which violated
      // the permanent NEVER DELETE DATA rule. Same pattern as registration-store.ts.
      if (row.unionPassId) {
        const upd = await client.query(
          `UPDATE auto_renews SET
             snapshot_id = $1,
             plan_name = $2,
             plan_state = $3,
             plan_price = $4,
             customer_name = $5,
             customer_email = $6,
             created_at = COALESCE($7, created_at),
             order_id = COALESCE($8, order_id),
             sales_channel = COALESCE($9, sales_channel),
             canceled_at = COALESCE($10, canceled_at),
             canceled_by = COALESCE($11, canceled_by),
             admin = COALESCE($12, admin),
             current_state = COALESCE($13, current_state),
             current_plan = COALESCE($14, current_plan),
             plan_category = $16,
             pending_canceled_at = COALESCE($17, pending_canceled_at),
             imported_at = NOW()
           WHERE union_pass_id = $15`,
          values
        );
        if (upd.rowCount && upd.rowCount > 0) {
          updated += upd.rowCount;
          updatedExisting = true;
        }
      }

      if (!updatedExisting) {
        const result = await client.query(
          `INSERT INTO auto_renews (
            snapshot_id, plan_name, plan_state, plan_price,
            customer_name, customer_email, created_at, order_id, sales_channel,
            canceled_at, canceled_by, admin, current_state, current_plan,
            union_pass_id, plan_category, pending_canceled_at
          ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
          ON CONFLICT (customer_email, plan_name, created_at)
          DO UPDATE SET
            snapshot_id = EXCLUDED.snapshot_id,
            plan_state = EXCLUDED.plan_state,
            plan_price = EXCLUDED.plan_price,
            customer_name = EXCLUDED.customer_name,
            order_id = COALESCE(EXCLUDED.order_id, auto_renews.order_id),
            sales_channel = COALESCE(EXCLUDED.sales_channel, auto_renews.sales_channel),
            canceled_at = COALESCE(EXCLUDED.canceled_at, auto_renews.canceled_at),
            canceled_by = COALESCE(EXCLUDED.canceled_by, auto_renews.canceled_by),
            admin = COALESCE(EXCLUDED.admin, auto_renews.admin),
            current_state = COALESCE(EXCLUDED.current_state, auto_renews.current_state),
            current_plan = COALESCE(EXCLUDED.current_plan, auto_renews.current_plan),
            union_pass_id = COALESCE(EXCLUDED.union_pass_id, auto_renews.union_pass_id),
            plan_category = EXCLUDED.plan_category,
            pending_canceled_at = COALESCE(EXCLUDED.pending_canceled_at, auto_renews.pending_canceled_at),
            imported_at = NOW()`,
          values
        );

        if (result.rowCount === 1) {
          inserted++;
        }
      }

        await client.query("RELEASE SAVEPOINT row_save");
      } catch (rowErr) {
        await client.query("ROLLBACK TO SAVEPOINT row_save");
        skipped++;
        const msg = rowErr instanceof Error ? rowErr.message : String(rowErr);
        console.warn(
          `[auto-renew-store] Skipped row email=${row.customerEmail} plan=${row.planName} created_at=${row.createdAt ?? "null"} union_pass_id=${row.unionPassId ?? "null"}: ${msg}`,
        );
      }
    }

    if (skipped > 0) {
      console.warn(`[auto-renew-store] Skipped ${skipped}/${dedupedRows.length} rows due to per-row errors (others were saved)`);
    }

  // ── Order-ID dedup ────────────────────────────────────────────────────────
  // Union renews a subscription by issuing a NEW pass under the SAME order_id.
  // The old pass stays "Valid Now" in the export until Union explicitly expires
  // it. Both get upserted here with different (email, plan, created_at) keys,
  // creating two active rows for one subscription. Union's admin counts ONE per
  // order_id; we must do the same.
  //
  // After the batch upsert, for any order_id that now has multiple active rows,
  // keep only the most-recently-created (highest id as tiebreaker) and mark the
  // rest current_state='canceled'. No plan_state change → no churn event fired.
  const batchOrderIds = dedupedRows
    .filter((r) => r.orderId)
    .map((r) => r.orderId as string);

  if (batchOrderIds.length > 0) {
    const deduped = await client.query(
      `UPDATE auto_renews
       SET current_state = 'canceled'
       WHERE order_id = ANY($1)
         AND plan_state IN (${ACTIVE_STATES_SQL})
         AND (current_state IS NULL OR current_state = 'active')
         AND id NOT IN (
           SELECT DISTINCT ON (order_id) id
           FROM auto_renews
           WHERE order_id = ANY($1)
             AND plan_state IN (${ACTIVE_STATES_SQL})
             AND (current_state IS NULL OR current_state = 'active')
           ORDER BY order_id, created_at DESC, id DESC
         )`,
      [batchOrderIds],
    );
    if (deduped.rowCount && deduped.rowCount > 0) {
      console.log(
        `[auto-renew-store] Order-ID dedup: deactivated ${deduped.rowCount} older pass(es) sharing order_id`,
      );
    }
  }

  return { inserted, updated };
}

/**
 * Reconcile active auto-renews against a fresh export.
 *
 * Compares DB active subscribers against the emails present in the current
 * export. Anyone active in DB but missing from the export has gone stale
 * (canceled, changed plan, etc.) — update their plan_state to 'Canceled'.
 *
 * This fixes ghost subscribers whose plan_state was never updated by the
 * daily export. Data is never deleted — only plan_state is updated.
 */
export async function reconcileAutoRenews(
  exportEmails: Set<string>
): Promise<{ reconciled: number; details: { email: string; planName: string; oldState: string }[] }> {
  const pool = getPool();

  // Get all currently "active" rows from DB.
  // NOTE: filter intentionally broader than canonical ACTIVE_STATES — includes
  // 'Past Due' so reconciliation can mark Past Due rows as Canceled when they
  // disappear from the export.
  const { rows } = await pool.query(
    `SELECT id, customer_email, plan_name, plan_state
     FROM auto_renews
     WHERE plan_state IN ('Valid Now', 'Paused', 'In Trial', 'Invalid', 'Pending Cancel', 'Past Due')`
  );

  // Find rows whose email is NOT in the export
  const toReconcile: { id: number; email: string; planName: string; oldState: string }[] = [];
  for (const row of rows) {
    const email = (row.customer_email as string).toLowerCase();
    if (!exportEmails.has(email)) {
      toReconcile.push({
        id: row.id as number,
        email,
        planName: row.plan_name as string,
        oldState: row.plan_state as string,
      });
    }
  }

  if (toReconcile.length === 0) {
    console.log("[auto-renew-store] Reconciliation: all DB active subscribers found in export");
    return { reconciled: 0, details: [] };
  }

  // Update stale rows: set plan_state to 'Canceled' and canceled_at to now
  const ids = toReconcile.map((r) => r.id);
  await pool.query(
    `UPDATE auto_renews
     SET plan_state = 'Canceled',
         canceled_at = COALESCE(canceled_at, NOW())
     WHERE id = ANY($1)`,
    [ids]
  );

  console.log(
    `[auto-renew-store] Reconciliation: marked ${toReconcile.length} stale subscribers as Canceled`
  );
  for (const r of toReconcile) {
    console.log(`  ${r.email} | ${r.planName} | was ${r.oldState}`);
  }

  return {
    reconciled: toReconcile.length,
    details: toReconcile.map((r) => ({ email: r.email, planName: r.planName, oldState: r.oldState })),
  };
}

/**
 * Row-level reconciliation against a fresh full export.
 *
 * For every currently-active row in the DB, check whether its
 * (email, plan_name, created_at) tuple appears in the export's active set.
 * If not, mark it Canceled. This catches stale duplicate rows where Union
 * historically reissued a subscription with a different created_at — the
 * active-looking ghost rows that drift our row counts above Union's.
 *
 * `activeTuples` MUST be the complete set of currently-active subscriptions
 * from a full export — keys are `${email.toLowerCase()}|${planName}|${YYYY-MM-DD}`
 * (date portion of created_at). Calling this with a partial/delta set will
 * mass-cancel valid subscriptions.
 *
 * `csvMaxCreatedMs` is the latest created_at timestamp in the CSV (in ms).
 * DB rows created after this are skipped — they're newer signups the daily
 * delta caught after the CSV was generated, not stale data.
 */
export async function reconcileAutoRenewsFromExport(
  activeTuples: Set<string>,
  csvMaxCreatedMs: number,
): Promise<{ reconciled: number }> {
  const pool = getPool();

  const { rows } = await pool.query(
    `SELECT id, LOWER(customer_email) AS email, plan_name,
            TO_CHAR(created_at, 'YYYY-MM-DD') AS created_date,
            created_at
     FROM auto_renews
     WHERE plan_state IN ('Valid Now', 'Paused', 'In Trial', 'Invalid', 'Pending Cancel', 'Past Due')
       AND (current_state IS NULL OR current_state = 'active')`,
  );

  const staleIds: number[] = [];
  for (const row of rows) {
    const tuple = `${row.email}|${row.plan_name}|${row.created_date}`;
    if (activeTuples.has(tuple)) continue;
    const ms = row.created_at ? new Date(row.created_at).getTime() : 0;
    if (ms > csvMaxCreatedMs) continue; // newer signup, leave alone
    staleIds.push(row.id);
  }

  if (staleIds.length === 0) {
    console.log("[auto-renew-store] Row-level reconciliation: no stale rows");
    return { reconciled: 0 };
  }

  await pool.query(
    `UPDATE auto_renews
     SET plan_state = 'Canceled',
         canceled_at = COALESCE(canceled_at, NOW())
     WHERE id = ANY($1)`,
    [staleIds],
  );

  console.log(
    `[auto-renew-store] Row-level reconciliation: marked ${staleIds.length} stale rows as Canceled`,
  );
  return { reconciled: staleIds.length };
}

// ── Read Operations ──────────────────────────────────────────

interface RawAutoRenewRow {
  id: number;
  snapshot_id: string | null;
  plan_name: string;
  plan_state: string;
  plan_price: number;
  customer_name: string;
  customer_email: string;
  created_at: string;
  canceled_at: string | null;
}

function mapRow(raw: RawAutoRenewRow): StoredAutoRenew {
  const name = raw.plan_name || "";
  const cat = getCategory(name);
  const annual = isAnnualPlan(name);
  const price = parseFloat(String(raw.plan_price)) || 0;

  return {
    id: raw.id,
    snapshotId: raw.snapshot_id,
    planName: name,
    planState: raw.plan_state || "",
    planPrice: price,
    customerName: raw.customer_name || "",
    customerEmail: raw.customer_email || "",
    createdAt: raw.created_at || "",
    canceledAt: raw.canceled_at,
    category: cat,
    isAnnual: annual,
    monthlyRate: annual ? Math.round((price / 12) * 100) / 100 : price,
  };
}

/**
 * Get all active auto-renews.
 *
 * Plan states considered "active" (per CLAUDE.md ACTIVE_STATES / ACTIVE_STATES_SQL):
 *   Valid Now, Paused, Pending Cancel, In Trial, Invalid, Past Due.
 *   (Past Due IS active — Union keeps retrying payment and counts them active,
 *   so we do too. Only 'Canceled' is excluded. This query uses ACTIVE_STATES_SQL.)
 *
 * Counting strategy — LATEST row per (email, plan_name):
 *   - The latest row's plan_state must be in ACTIVE_STATES.
 *   - The latest row's current_state must be NULL or 'active'. If it's
 *     'canceled' or 'changed', the person genuinely canceled or moved
 *     plans even though Union may keep plan_state='Valid Now' until the
 *     paid period ends. Excluding these is what aligns the dashboard
 *     count with Union.fit's admin "active members" number.
 *
 * Why "latest row" and not "any row had current_state='active'":
 *   The previous implementation included anyone who had EVER had
 *   current_state='active', which double-counted people who later
 *   canceled or upgraded — they appear with latest plan_state='Valid Now'
 *   / current_state='canceled'. That bug inflated the Member count by
 *   ~10 (canonical 475 vs reality 466 on 2026-04-29).
 *
 * Why NULL current_state passes through:
 *   Daily delta exports do not include current_state. For a row whose
 *   latest snapshot is a daily delta, current_state is NULL and we trust
 *   plan_state alone. The previous code's "fall back only if no row ever
 *   had current_state" rule was too restrictive once any full export had
 *   landed.
 *
 * NOTE: canceled_at is NOT used as a filter. For active subscribers,
 * Union.fit sets canceled_at to the next billing/renewal date, NOT the
 * cancellation date. Using it as a guard would filter out nearly everyone.
 *
 * Downstream callers (getActiveCounts, getAutoRenewStats) deduplicate by
 * customer_email so duplicate rows from multiple imports don't inflate counts.
 */
export async function getActiveAutoRenews(): Promise<StoredAutoRenew[]> {
  const pool = getPool();
  // No DISTINCT ON — `auto_renews` has a UNIQUE constraint on
  // (customer_email, plan_name, created_at) (migration 003), so each
  // distinct subscription is exactly one row. People who genuinely have
  // two of the "same" plan (e.g. two SKY3 Monthly subscriptions on
  // different Union passes) will have different created_at values and
  // thus appear as two rows — Union counts them twice in their admin UI,
  // and we now match.
  //
  // The previous DISTINCT ON (email, plan_name) was a workaround for stale
  // snapshots, but it under-counted real dual subscriptions. The current
  // approach: trust the row data, filter strictly on plan_state +
  // current_state, and rely on the upload-route reconciliation to mark
  // genuinely-stale rows as Canceled (so they're filtered out here).
  const { rows } = await pool.query(
    `SELECT id, snapshot_id, plan_name, plan_state, plan_price,
            customer_name, customer_email, created_at, canceled_at
     FROM auto_renews
     WHERE plan_state IN (${ACTIVE_STATES_SQL})
       AND (current_state IS NULL OR current_state = 'active')
     ORDER BY plan_name`
  );

  return (rows as RawAutoRenewRow[]).map(mapRow);
}

/**
 * Get auto-renews created within a date range (new auto-renews).
 */
export async function getNewAutoRenews(startDate: string, endDate: string): Promise<StoredAutoRenew[]> {
  const pool = getPool();
  // Only count subscriptions that are currently active (not historical canceled rows
  // that happen to have created_at in this range from daily delta imports).
  // NOTE: filter intentionally broader than canonical ACTIVE_STATES — includes
  // 'Past Due' so we still count signups whose first payment failed.
  // Stage 2 will replace this function with getSubscriberMovement().
  const { rows } = await pool.query(
    `SELECT id, snapshot_id, plan_name, plan_state, plan_price,
            customer_name, customer_email, created_at, canceled_at
     FROM auto_renews
     WHERE created_at >= $1 AND created_at < $2
       AND (current_state = 'active' OR (current_state IS NULL
            AND plan_state IN ('Valid Now', 'Paused', 'In Trial', 'Invalid', 'Pending Cancel', 'Past Due')))
     ORDER BY created_at`,
    [startDate, endDate]
  );

  return (rows as RawAutoRenewRow[]).map(mapRow);
}

/**
 * Get auto-renews canceled within a date range.
 */
export async function getCanceledAutoRenews(startDate: string, endDate: string): Promise<StoredAutoRenew[]> {
  const pool = getPool();
  // Only count rows where plan_state is actually 'Canceled'.
  // For active subscribers, canceled_at is the next billing date — NOT a cancellation.
  // Daily deltas import active rows with canceled_at in recent ranges; filtering by
  // plan_state='Canceled' ensures we only count real cancellations.
  const { rows } = await pool.query(
    `SELECT id, snapshot_id, plan_name, plan_state, plan_price,
            customer_name, customer_email, created_at, canceled_at
     FROM auto_renews
     WHERE canceled_at IS NOT NULL AND canceled_at >= $1 AND canceled_at < $2
       AND plan_state = 'Canceled'
     ORDER BY canceled_at`,
    [startDate, endDate]
  );

  return (rows as RawAutoRenewRow[]).map(mapRow);
}

export interface DailyMovementRow {
  date: string; // YYYY-MM-DD
  newMembers: number;
  newSky3: number;
  newTv: number;
  churnedMembers: number;
  churnedSky3: number;
  churnedTv: number;
}

/**
 * Daily new + churned subscriber counts per category for the last `days` days.
 */
export async function getDailySubscriberMovement(days = 7): Promise<DailyMovementRow[]> {
  const pool = getPool();
  const { rows } = await pool.query(`
    WITH dates AS (
      SELECT generate_series(
        (CURRENT_DATE - ($1 || ' days')::interval)::date,
        CURRENT_DATE,
        '1 day'
      )::date AS d
    ),
    new_by_day AS (
      SELECT created_at AS d,
        COUNT(*) FILTER (WHERE plan_category = 'MEMBER') AS new_members,
        COUNT(*) FILTER (WHERE plan_category = 'SKY3') AS new_sky3,
        COUNT(*) FILTER (WHERE plan_category = 'SKY_TING_TV') AS new_tv
      FROM auto_renews
      WHERE created_at IS NOT NULL
        AND created_at >= CURRENT_DATE - ($1 || ' days')::interval
        -- Filter intentionally broader than canonical ACTIVE_STATES (includes Past Due)
        AND (current_state = 'active' OR (current_state IS NULL
             AND plan_state IN ('Valid Now', 'Paused', 'In Trial', 'Invalid', 'Pending Cancel', 'Past Due')))
      GROUP BY created_at
    ),
    churned_by_day AS (
      SELECT canceled_at AS d,
        COUNT(*) FILTER (WHERE plan_category = 'MEMBER') AS churned_members,
        COUNT(*) FILTER (WHERE plan_category = 'SKY3') AS churned_sky3,
        COUNT(*) FILTER (WHERE plan_category = 'SKY_TING_TV') AS churned_tv
      FROM auto_renews
      WHERE canceled_at IS NOT NULL
        AND canceled_at >= CURRENT_DATE - ($1 || ' days')::interval
        AND plan_state = 'Canceled'
      GROUP BY canceled_at
    )
    SELECT
      dates.d::text AS date,
      COALESCE(n.new_members, 0)::int AS new_members,
      COALESCE(n.new_sky3, 0)::int AS new_sky3,
      COALESCE(n.new_tv, 0)::int AS new_tv,
      COALESCE(c.churned_members, 0)::int AS churned_members,
      COALESCE(c.churned_sky3, 0)::int AS churned_sky3,
      COALESCE(c.churned_tv, 0)::int AS churned_tv
    FROM dates
    LEFT JOIN new_by_day n ON n.d = dates.d
    LEFT JOIN churned_by_day c ON c.d = dates.d
    ORDER BY dates.d
  `, [days]);

  return rows.map((r: Record<string, unknown>) => ({
    date: String(r.date).slice(0, 10),
    newMembers: Number(r.new_members),
    newSky3: Number(r.new_sky3),
    newTv: Number(r.new_tv),
    churnedMembers: Number(r.churned_members),
    churnedSky3: Number(r.churned_sky3),
    churnedTv: Number(r.churned_tv),
  }));
}

/**
 * Canonical active subscriber counts by category — SINGLE SOURCE OF TRUTH.
 *
 * Returns subscription rows per category, NOT unique people. A person with
 * two subscriptions in the same category (e.g. SKY3 Monthly + SKYHIGH3
 * Monthly) counts twice in that category — they're paying for both, and
 * Union's admin counts the same way. A person with subscriptions in
 * multiple categories (e.g. Member + TV) counts once in each.
 *
 * Every section of the dashboard (Growth, Usage, Churn, Overview) MUST
 * use this function for "X Active" numbers.
 */
export interface ActiveCounts {
  member: number;
  sky3: number;
  skyTingTv: number;
  unknown: number;
  total: number;
}

export async function getActiveCounts(): Promise<ActiveCounts> {
  const active = await getActiveAutoRenews();

  const counts: ActiveCounts = {
    member: 0,
    sky3: 0,
    skyTingTv: 0,
    unknown: 0,
    total: 0,
  };

  for (const ar of active) {
    if (!ar.customerEmail) continue;
    const key = ar.category === "MEMBER" ? "member"
      : ar.category === "SKY3" ? "sky3"
      : ar.category === "SKY_TING_TV" ? "skyTingTv"
      : "unknown";
    counts[key]++;
  }
  counts.total = counts.member + counts.sky3 + counts.skyTingTv + counts.unknown;

  return counts;
}

/**
 * Compute aggregate auto-renew stats: active counts, MRR, ARPU by category.
 * This is the primary function the dashboard uses.
 *
 * Active counts use getActiveCounts() — subscription rows, not unique people.
 * MRR sums all subscription rows (a person with 2 plans contributes 2x MRR).
 */
export async function getAutoRenewStats(): Promise<AutoRenewStats | null> {
  const active = await getActiveAutoRenews();
  if (active.length === 0) return null;

  const counts = await getActiveCounts();

  // MRR reflects revenue being recognized per ASC 606 — only Valid Now and
  // Pending Cancel subscribers are actively billing. Paused, Invalid (passes
  // used up), In Trial, and Past Due are "active" in the subscriber sense
  // but don't contribute to recognized revenue this month:
  //   - Paused: no money flowing (user paused their subscription)
  //   - Invalid: passes used up, revenue already recognized at purchase/use
  //   - In Trial: plan_price shows full post-trial rate, not trial rate
  //   - Past Due: payment failed, no revenue recognized until card retries succeed
  // Excluding these keeps MRR aligned with actual cash being collected.
  const BILLING_SET = new Set<string>(BILLING_STATES);

  // Sum monthlyRate across ALL billing rows per category. A person with two
  // subscriptions (e.g. SKY3 Monthly + SKYHIGH3 Monthly) contributes both
  // rates — they're paying for both. Matches the row-count doctrine in
  // getActiveCounts() so ARPU = MRR / count is per-subscription, not per-person.
  const mrr = { member: 0, sky3: 0, skyTingTv: 0, unknown: 0 };

  for (const ar of active) {
    if (!BILLING_SET.has(ar.planState)) continue;
    const key = ar.category === "MEMBER" ? "member"
      : ar.category === "SKY3" ? "sky3"
      : ar.category === "SKY_TING_TV" ? "skyTingTv"
      : "unknown";
    mrr[key] += ar.monthlyRate;
  }
  for (const cat of Object.keys(mrr) as Array<keyof typeof mrr>) {
    mrr[cat] = Math.round(mrr[cat] * 100) / 100;
  }

  const totalMRR = mrr.member + mrr.sky3 + mrr.skyTingTv + mrr.unknown;

  return {
    active: {
      ...counts,
    },
    mrr: {
      ...mrr,
      total: Math.round(totalMRR * 100) / 100,
    },
    arpu: {
      member: counts.member > 0 ? Math.round((mrr.member / counts.member) * 100) / 100 : 0,
      sky3: counts.sky3 > 0 ? Math.round((mrr.sky3 / counts.sky3) * 100) / 100 : 0,
      skyTingTv: counts.skyTingTv > 0 ? Math.round((mrr.skyTingTv / counts.skyTingTv) * 100) / 100 : 0,
      overall: counts.total > 0 ? Math.round((totalMRR / counts.total) * 100) / 100 : 0,
    },
  };
}

/**
 * Check if auto-renew data exists in the database.
 */
export async function hasAutoRenewData(): Promise<boolean> {
  const pool = getPool();
  const { rows } = await pool.query(
    `SELECT COUNT(*) as count FROM auto_renews`
  );
  return Number(rows[0].count) > 0;
}
