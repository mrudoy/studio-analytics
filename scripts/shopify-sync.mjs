#!/usr/bin/env node
/**
 * Standalone Shopify sync script — fetches orders, products, customers
 * from Shopify Admin API and saves to PostgreSQL.
 */
import pg from "pg";

const DATABASE_URL = process.env.DATABASE_URL;
if (!DATABASE_URL) throw new Error("DATABASE_URL env var is required");

const SHOPIFY_STORE = process.env.SHOPIFY_STORE_NAME || "skytingyoga";
const SHOPIFY_CLIENT_ID = process.env.SHOPIFY_CLIENT_ID;
const SHOPIFY_CLIENT_SECRET = process.env.SHOPIFY_CLIENT_SECRET;
if (!SHOPIFY_CLIENT_ID || !SHOPIFY_CLIENT_SECRET) {
  throw new Error("SHOPIFY_CLIENT_ID and SHOPIFY_CLIENT_SECRET env vars are required");
}

const API_VERSION = "2024-01";
const MAX_PER_PAGE = 250;
const STORE_URL = `https://${SHOPIFY_STORE}.myshopify.com`;
const BASE_URL = `${STORE_URL}/admin/api/${API_VERSION}`;

const pool = new pg.Pool({ connectionString: DATABASE_URL, ssl: false });

// ── OAuth token ──────────────────────────────────────────────
let cachedToken = null;
let tokenExpiresAt = 0;

async function getAccessToken() {
  if (cachedToken && Date.now() < tokenExpiresAt) return cachedToken;
  console.log("[shopify] Requesting access token...");

  const res = await fetch(`${STORE_URL}/admin/oauth/access_token`, {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({
      grant_type: "client_credentials",
      client_id: SHOPIFY_CLIENT_ID,
      client_secret: SHOPIFY_CLIENT_SECRET,
    }).toString(),
  });

  if (!res.ok) {
    const body = await res.text().catch(() => "");
    throw new Error(`Token exchange failed (${res.status}): ${body.slice(0, 300)}`);
  }

  const data = await res.json();
  cachedToken = data.access_token;
  tokenExpiresAt = Date.now() + data.expires_in * 1000 - 5 * 60 * 1000;
  console.log(`[shopify] Got token (expires_in: ${data.expires_in}s)`);
  return cachedToken;
}

// ── API request with rate limiting ───────────────────────────
async function shopifyGet(url) {
  const token = await getAccessToken();
  const res = await fetch(url, {
    headers: { "X-Shopify-Access-Token": token, "Content-Type": "application/json" },
  });

  if (!res.ok) {
    const body = await res.text().catch(() => "");
    throw new Error(`Shopify API ${res.status}: ${body.slice(0, 200)}`);
  }

  const callLimit = res.headers.get("X-Shopify-Shop-Api-Call-Limit");
  if (callLimit) {
    const [used, max] = callLimit.split("/").map(Number);
    if (used / max >= 0.8) {
      console.log(`  Rate limit ${callLimit} — throttling 1s`);
      await new Promise((r) => setTimeout(r, 1000));
    }
  }

  const linkHeader = res.headers.get("Link");
  let nextUrl = null;
  if (linkHeader) {
    const m = linkHeader.match(/<([^>]+)>;\s*rel="next"/);
    if (m) nextUrl = m[1];
  }

  return { data: await res.json(), nextUrl };
}

// ── Sync orders ──────────────────────────────────────────────
async function syncOrders() {
  console.log("\n📦 Syncing Shopify orders...");
  const maxIdRes = await pool.query("SELECT MAX(id) AS max_id FROM shopify_orders");
  const sinceId = maxIdRes.rows[0]?.max_id;
  console.log(`  since_id: ${sinceId ?? "none (full fetch)"}`);

  let url = `${BASE_URL}/orders.json?status=any&limit=${MAX_PER_PAGE}`;
  if (sinceId) url += `&since_id=${sinceId}`;

  let total = 0;
  while (url) {
    const { data, nextUrl } = await shopifyGet(url);
    const orders = data.orders || [];
    if (orders.length === 0) break;

    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      for (const o of orders) {
        const lineItems = (o.line_items || []).map((li) => ({
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
            o.id, o.order_number, o.email, o.financial_status,
            o.fulfillment_status, parseFloat(o.total_price),
            parseFloat(o.subtotal_price), parseFloat(o.total_tax),
            parseFloat(o.total_discounts), o.currency,
            JSON.stringify(lineItems), o.customer?.id ?? null,
            o.created_at, o.updated_at, o.cancelled_at,
          ]
        );
      }
      await client.query("COMMIT");
      total += orders.length;
      console.log(`  ... ${total} orders`);
    } catch (e) {
      await client.query("ROLLBACK");
      throw e;
    } finally {
      client.release();
    }

    url = nextUrl || "";
  }
  console.log(`  ✅ Orders synced: ${total}`);
  return total;
}

// ── Sync products ────────────────────────────────────────────
async function syncProducts() {
  console.log("\n🏷️  Syncing Shopify products...");
  let url = `${BASE_URL}/products.json?limit=${MAX_PER_PAGE}`;
  let total = 0;

  while (url) {
    const { data, nextUrl } = await shopifyGet(url);
    const products = data.products || [];
    if (products.length === 0) break;

    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      for (const p of products) {
        const variants = (p.variants || []).map((v) => ({
          id: v.id, title: v.title, price: v.price, sku: v.sku,
          inventory_quantity: v.inventory_quantity, inventory_item_id: v.inventory_item_id,
        }));
        await client.query(
          `INSERT INTO shopify_products
             (id, title, product_type, vendor, status, tags, variants, created_at, updated_at, synced_at)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9, NOW())
           ON CONFLICT (id) DO UPDATE SET
             title=EXCLUDED.title, product_type=EXCLUDED.product_type, vendor=EXCLUDED.vendor,
             status=EXCLUDED.status, tags=EXCLUDED.tags, variants=EXCLUDED.variants,
             updated_at=EXCLUDED.updated_at, synced_at=NOW()`,
          [p.id, p.title, p.product_type, p.vendor, p.status, p.tags,
           JSON.stringify(variants), p.created_at, p.updated_at]
        );
      }
      await client.query("COMMIT");
      total += products.length;
    } catch (e) {
      await client.query("ROLLBACK");
      throw e;
    } finally {
      client.release();
    }
    url = nextUrl || "";
  }
  console.log(`  ✅ Products synced: ${total}`);
  return total;
}

// ── Sync customers ───────────────────────────────────────────
async function syncCustomers() {
  console.log("\n👥 Syncing Shopify customers...");
  let url = `${BASE_URL}/customers.json?limit=${MAX_PER_PAGE}`;
  let total = 0;

  while (url) {
    const { data, nextUrl } = await shopifyGet(url);
    const customers = data.customers || [];
    if (customers.length === 0) break;

    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      for (const c of customers) {
        await client.query(
          `INSERT INTO shopify_customers
             (id, email, first_name, last_name, orders_count, total_spent, tags, created_at, updated_at, synced_at)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9, NOW())
           ON CONFLICT (id) DO UPDATE SET
             email=EXCLUDED.email, first_name=EXCLUDED.first_name, last_name=EXCLUDED.last_name,
             orders_count=EXCLUDED.orders_count, total_spent=EXCLUDED.total_spent,
             tags=EXCLUDED.tags, updated_at=EXCLUDED.updated_at, synced_at=NOW()`,
          [c.id, c.email, c.first_name, c.last_name, c.orders_count,
           parseFloat(c.total_spent), c.tags, c.created_at, c.updated_at]
        );
      }
      await client.query("COMMIT");
      total += customers.length;
      if (total % 500 === 0) console.log(`  ... ${total} customers`);
    } catch (e) {
      await client.query("ROLLBACK");
      throw e;
    } finally {
      client.release();
    }
    url = nextUrl || "";
  }
  console.log(`  ✅ Customers synced: ${total}`);
  return total;
}

// ── Main ─────────────────────────────────────────────────────
async function main() {
  console.log("🚀 Starting Shopify sync...\n");

  // Test connection
  const { data } = await shopifyGet(`${BASE_URL}/shop.json`);
  console.log(`Connected to: ${data.shop.name} (${data.shop.myshopify_domain})`);

  const orders = await syncOrders();
  const products = await syncProducts();
  const customers = await syncCustomers();

  // Final counts
  const [o, p, c] = await Promise.all([
    pool.query("SELECT COUNT(*) as cnt, COALESCE(SUM(total_price),0) as rev FROM shopify_orders"),
    pool.query("SELECT COUNT(*) as cnt FROM shopify_products"),
    pool.query("SELECT COUNT(*) as cnt FROM shopify_customers"),
  ]);

  console.log("\n=== Shopify Sync Complete ===");
  console.log(`  Orders:    ${o.rows[0].cnt} (total revenue: $${Number(o.rows[0].rev).toLocaleString()})`);
  console.log(`  Products:  ${p.rows[0].cnt}`);
  console.log(`  Customers: ${c.rows[0].cnt}`);

  await pool.end();
  console.log("\n🏁 Done!");
}

main().catch((e) => {
  console.error("Fatal:", e.message || e);
  process.exit(1);
});
