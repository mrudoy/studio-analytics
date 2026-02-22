/**
 * Shopify data store — UPSERT operations for orders, products, customers, inventory.
 * Follows the same pattern as revenue-store.ts and auto-renew-store.ts.
 */

import { getPool } from "./database";
import type {
  ShopifyOrder,
  ShopifyProduct,
  ShopifyCustomer,
  ShopifyInventoryLevel,
} from "../shopify/shopify-client";

// ── Orders ───────────────────────────────────────────────────

export async function saveShopifyOrders(orders: ShopifyOrder[]): Promise<number> {
  if (orders.length === 0) return 0;
  const pool = getPool();
  const client = await pool.connect();
  let saved = 0;

  try {
    await client.query("BEGIN");

    for (const o of orders) {
      const lineItems = o.line_items.map((li) => ({
        title: li.title,
        quantity: li.quantity,
        price: li.price,
        sku: li.sku,
        product_id: li.product_id,
      }));

      await client.query(
        `INSERT INTO shopify_orders
           (id, order_number, email, financial_status, fulfillment_status,
            total_price, subtotal_price, total_tax, total_discounts, currency,
            line_items, customer_id, created_at, updated_at, canceled_at, synced_at)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15, NOW())
         ON CONFLICT (id) DO UPDATE SET
           financial_status = EXCLUDED.financial_status,
           fulfillment_status = EXCLUDED.fulfillment_status,
           total_price = EXCLUDED.total_price,
           subtotal_price = EXCLUDED.subtotal_price,
           total_tax = EXCLUDED.total_tax,
           total_discounts = EXCLUDED.total_discounts,
           line_items = EXCLUDED.line_items,
           updated_at = EXCLUDED.updated_at,
           canceled_at = EXCLUDED.canceled_at,
           synced_at = NOW()`,
        [
          o.id,
          o.order_number,
          o.email,
          o.financial_status,
          o.fulfillment_status,
          parseFloat(o.total_price),
          parseFloat(o.subtotal_price),
          parseFloat(o.total_tax),
          parseFloat(o.total_discounts),
          o.currency,
          JSON.stringify(lineItems),
          o.customer?.id ?? null,
          o.created_at,
          o.updated_at,
          o.cancelled_at,
        ]
      );
      saved++;
    }

    await client.query("COMMIT");
  } catch (err) {
    await client.query("ROLLBACK");
    throw err;
  } finally {
    client.release();
  }

  return saved;
}

// ── Products ─────────────────────────────────────────────────

export async function saveShopifyProducts(products: ShopifyProduct[]): Promise<number> {
  if (products.length === 0) return 0;
  const pool = getPool();
  const client = await pool.connect();
  let saved = 0;

  try {
    await client.query("BEGIN");

    for (const p of products) {
      const variants = p.variants.map((v) => ({
        id: v.id,
        title: v.title,
        price: v.price,
        sku: v.sku,
        inventory_quantity: v.inventory_quantity,
        inventory_item_id: v.inventory_item_id,
      }));

      await client.query(
        `INSERT INTO shopify_products
           (id, title, product_type, vendor, status, tags, variants, created_at, updated_at, synced_at)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9, NOW())
         ON CONFLICT (id) DO UPDATE SET
           title = EXCLUDED.title,
           product_type = EXCLUDED.product_type,
           vendor = EXCLUDED.vendor,
           status = EXCLUDED.status,
           tags = EXCLUDED.tags,
           variants = EXCLUDED.variants,
           updated_at = EXCLUDED.updated_at,
           synced_at = NOW()`,
        [
          p.id,
          p.title,
          p.product_type,
          p.vendor,
          p.status,
          p.tags,
          JSON.stringify(variants),
          p.created_at,
          p.updated_at,
        ]
      );
      saved++;
    }

    await client.query("COMMIT");
  } catch (err) {
    await client.query("ROLLBACK");
    throw err;
  } finally {
    client.release();
  }

  return saved;
}

// ── Customers ────────────────────────────────────────────────

export async function saveShopifyCustomers(customers: ShopifyCustomer[]): Promise<number> {
  if (customers.length === 0) return 0;
  const pool = getPool();
  const client = await pool.connect();
  let saved = 0;

  try {
    await client.query("BEGIN");

    for (const c of customers) {
      await client.query(
        `INSERT INTO shopify_customers
           (id, email, first_name, last_name, orders_count, total_spent, tags, created_at, updated_at, synced_at)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9, NOW())
         ON CONFLICT (id) DO UPDATE SET
           email = EXCLUDED.email,
           first_name = EXCLUDED.first_name,
           last_name = EXCLUDED.last_name,
           orders_count = EXCLUDED.orders_count,
           total_spent = EXCLUDED.total_spent,
           tags = EXCLUDED.tags,
           updated_at = EXCLUDED.updated_at,
           synced_at = NOW()`,
        [
          c.id,
          c.email,
          c.first_name,
          c.last_name,
          c.orders_count,
          parseFloat(c.total_spent),
          c.tags,
          c.created_at,
          c.updated_at,
        ]
      );
      saved++;
    }

    await client.query("COMMIT");
  } catch (err) {
    await client.query("ROLLBACK");
    throw err;
  } finally {
    client.release();
  }

  return saved;
}

// ── Inventory ────────────────────────────────────────────────

export async function saveShopifyInventory(
  levels: ShopifyInventoryLevel[]
): Promise<number> {
  if (levels.length === 0) return 0;
  const pool = getPool();
  const client = await pool.connect();
  let saved = 0;

  try {
    await client.query("BEGIN");

    for (const lv of levels) {
      await client.query(
        `INSERT INTO shopify_inventory
           (inventory_item_id, location_id, available, updated_at, synced_at)
         VALUES ($1,$2,$3,$4, NOW())
         ON CONFLICT (inventory_item_id, location_id) DO UPDATE SET
           available = EXCLUDED.available,
           updated_at = EXCLUDED.updated_at,
           synced_at = NOW()`,
        [lv.inventory_item_id, lv.location_id, lv.available ?? 0, lv.updated_at]
      );
      saved++;
    }

    await client.query("COMMIT");
  } catch (err) {
    await client.query("ROLLBACK");
    throw err;
  } finally {
    client.release();
  }

  return saved;
}

// ── Read helpers ─────────────────────────────────────────────

/** Get the highest Shopify order ID for incremental since_id pagination. */
export async function getLatestShopifyOrderId(): Promise<number | null> {
  const pool = getPool();
  const res = await pool.query("SELECT MAX(id) AS max_id FROM shopify_orders");
  return res.rows[0]?.max_id ?? null;
}

/** Get the highest Shopify product ID for incremental fetching. */
export async function getLatestShopifyProductId(): Promise<number | null> {
  const pool = getPool();
  const res = await pool.query("SELECT MAX(id) AS max_id FROM shopify_products");
  return res.rows[0]?.max_id ?? null;
}

/** Get the highest Shopify customer ID for incremental fetching. */
export async function getLatestShopifyCustomerId(): Promise<number | null> {
  const pool = getPool();
  const res = await pool.query("SELECT MAX(id) AS max_id FROM shopify_customers");
  return res.rows[0]?.max_id ?? null;
}

/** Aggregate Shopify order revenue by month (for dashboard integration). */
export async function getShopifyRevenueSummary(): Promise<
  Array<{ month: string; gross: number; net: number; orderCount: number }>
> {
  const pool = getPool();
  const res = await pool.query(`
    SELECT
      TO_CHAR(created_at, 'YYYY-MM') AS month,
      SUM(total_price) AS gross,
      SUM(subtotal_price) AS net,
      COUNT(*) AS order_count
    FROM shopify_orders
    WHERE financial_status NOT IN ('voided', 'refunded')
      AND canceled_at IS NULL
    GROUP BY TO_CHAR(created_at, 'YYYY-MM')
    ORDER BY month
  `);

  return res.rows.map((r: { month: string; gross: string; net: string; order_count: string }) => ({
    month: r.month,
    gross: parseFloat(r.gross) || 0,
    net: parseFloat(r.net) || 0,
    orderCount: parseInt(r.order_count) || 0,
  }));
}

/** Summary stats for the dashboard. */
export async function getShopifyStats(): Promise<{
  totalOrders: number;
  totalRevenue: number;
  productCount: number;
  customerCount: number;
  lastSyncAt: string | null;
}> {
  const pool = getPool();

  const [orders, products, customers] = await Promise.all([
    pool.query(`
      SELECT COUNT(*) AS cnt, COALESCE(SUM(total_price), 0) AS rev, MAX(synced_at) AS last_sync
      FROM shopify_orders
    `),
    pool.query("SELECT COUNT(*) AS cnt FROM shopify_products"),
    pool.query("SELECT COUNT(*) AS cnt FROM shopify_customers"),
  ]);

  return {
    totalOrders: parseInt(orders.rows[0].cnt) || 0,
    totalRevenue: parseFloat(orders.rows[0].rev) || 0,
    productCount: parseInt(products.rows[0].cnt) || 0,
    customerCount: parseInt(customers.rows[0].cnt) || 0,
    lastSyncAt: orders.rows[0].last_sync || null,
  };
}
