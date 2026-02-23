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

/** Month-to-date Shopify revenue (current calendar month). */
export async function getShopifyMTDRevenue(): Promise<number> {
  const pool = getPool();
  const res = await pool.query(`
    SELECT COALESCE(SUM(total_price), 0) AS mtd
    FROM shopify_orders
    WHERE created_at >= DATE_TRUNC('month', CURRENT_DATE)
      AND financial_status NOT IN ('voided', 'refunded')
      AND canceled_at IS NULL
  `);
  return parseFloat(res.rows[0].mtd) || 0;
}

/** Top products by total revenue from order line_items (JSONB). */
export async function getShopifyTopProducts(
  limit = 3
): Promise<Array<{ title: string; revenue: number; unitsSold: number }>> {
  const pool = getPool();
  const res = await pool.query(
    `SELECT
       li->>'title' AS title,
       SUM((li->>'price')::numeric * (li->>'quantity')::int) AS revenue,
       SUM((li->>'quantity')::int) AS units_sold
     FROM shopify_orders,
          jsonb_array_elements(line_items) AS li
     WHERE financial_status NOT IN ('voided', 'refunded')
       AND canceled_at IS NULL
     GROUP BY li->>'title'
     ORDER BY revenue DESC
     LIMIT $1`,
    [limit]
  );

  return res.rows.map((r: { title: string; revenue: string; units_sold: string }) => ({
    title: r.title,
    revenue: parseFloat(r.revenue) || 0,
    unitsSold: parseInt(r.units_sold) || 0,
  }));
}

/** Repeat customer rate: % of purchasing customers with orders_count > 1. */
export async function getShopifyRepeatCustomerRate(): Promise<{
  repeatRate: number;
  repeatCount: number;
  totalWithOrders: number;
}> {
  const pool = getPool();
  const res = await pool.query(`
    SELECT
      COUNT(*) FILTER (WHERE orders_count > 1) AS repeat_count,
      COUNT(*) FILTER (WHERE orders_count > 0) AS total_with_orders
    FROM shopify_customers
  `);

  const repeatCount = parseInt(res.rows[0].repeat_count) || 0;
  const totalWithOrders = parseInt(res.rows[0].total_with_orders) || 0;
  const repeatRate = totalWithOrders > 0
    ? Math.round((repeatCount / totalWithOrders) * 1000) / 10
    : 0;

  return { repeatRate, repeatCount, totalWithOrders };
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

/**
 * Cross-reference Shopify merch orders with Union.fit auto-renew subscriptions.
 * Matches on LOWER(email). A customer is "active subscriber" if they had ANY
 * non-Canceled/non-Invalid auto-renew at the time of the Shopify order.
 */
export async function getShopifyCustomerBreakdown(): Promise<{
  subscriber: { orders: number; revenue: number; customers: number };
  nonSubscriber: { orders: number; revenue: number; customers: number };
  total: { orders: number; revenue: number; customers: number };
}> {
  const pool = getPool();

  const res = await pool.query(`
    WITH order_sub_status AS (
      SELECT
        so.id AS order_id,
        so.email,
        so.total_price,
        CASE WHEN EXISTS (
          SELECT 1 FROM auto_renews ar
          WHERE LOWER(ar.customer_email) = LOWER(so.email)
            AND ar.plan_state NOT IN ('Canceled', 'Invalid')
        ) THEN true ELSE false END AS is_subscriber
      FROM shopify_orders so
      WHERE so.financial_status NOT IN ('voided', 'refunded')
        AND so.canceled_at IS NULL
        AND so.email IS NOT NULL
        AND so.email <> ''
    )
    SELECT
      is_subscriber,
      COUNT(*) AS order_count,
      SUM(total_price) AS revenue,
      COUNT(DISTINCT LOWER(email)) AS customer_count
    FROM order_sub_status
    GROUP BY is_subscriber
  `);

  const sub = res.rows.find((r: { is_subscriber: boolean }) => r.is_subscriber === true);
  const nonSub = res.rows.find((r: { is_subscriber: boolean }) => r.is_subscriber === false);

  const subscriber = {
    orders: parseInt(sub?.order_count) || 0,
    revenue: parseFloat(sub?.revenue) || 0,
    customers: parseInt(sub?.customer_count) || 0,
  };
  const nonSubscriber = {
    orders: parseInt(nonSub?.order_count) || 0,
    revenue: parseFloat(nonSub?.revenue) || 0,
    customers: parseInt(nonSub?.customer_count) || 0,
  };

  return {
    subscriber,
    nonSubscriber,
    total: {
      orders: subscriber.orders + nonSubscriber.orders,
      revenue: subscriber.revenue + nonSubscriber.revenue,
      customers: subscriber.customers + nonSubscriber.customers,
    },
  };
}
