/**
 * Shopify sync orchestrator.
 *
 * Fetches orders, products, customers, and inventory from Shopify Admin API
 * and saves them to PostgreSQL using incremental (since_id) fetching.
 */

import { ShopifyClient } from "./shopify-client";
import {
  saveShopifyOrders,
  saveShopifyProducts,
  saveShopifyCustomers,
  saveShopifyInventory,
  getLatestShopifyOrderId,
  getLatestShopifyProductId,
  getLatestShopifyCustomerId,
} from "../db/shopify-store";

export interface ShopifySyncOptions {
  storeName: string;
  clientId: string;
  clientSecret: string;
  onProgress?: (step: string, percent: number) => void;
}

export interface ShopifySyncResult {
  orderCount: number;
  productCount: number;
  customerCount: number;
  inventoryCount: number;
  shopName: string;
}

export async function runShopifySync(opts: ShopifySyncOptions): Promise<ShopifySyncResult> {
  const { storeName, clientId, clientSecret, onProgress } = opts;
  const progress = onProgress || (() => {});

  const client = new ShopifyClient({ storeName, clientId, clientSecret });

  // Step 1: Test connection
  progress("Connecting to Shopify", 0);
  const shop = await client.testConnection();
  console.log(`[shopify] Connected to "${shop.name}" (${shop.myshopify_domain})`);

  // Step 2: Sync orders (incremental via since_id)
  progress("Syncing orders", 0.1);
  let orderCount = 0;
  const lastOrderId = await getLatestShopifyOrderId();
  console.log(`[shopify] Orders: since_id=${lastOrderId ?? "none (full fetch)"}`);

  for await (const batch of client.fetchOrders({ sinceId: lastOrderId ?? undefined })) {
    const saved = await saveShopifyOrders(batch);
    orderCount += saved;
    progress(`Syncing orders (${orderCount})`, 0.1 + Math.min(0.3, orderCount / 1000 * 0.3));
  }
  console.log(`[shopify] Orders synced: ${orderCount}`);

  // Step 3: Sync products (full refresh — typically small catalog)
  progress("Syncing products", 0.4);
  let productCount = 0;
  const lastProductId = await getLatestShopifyProductId();

  for await (const batch of client.fetchProducts({ sinceId: lastProductId ?? undefined })) {
    const saved = await saveShopifyProducts(batch);
    productCount += saved;
  }
  console.log(`[shopify] Products synced: ${productCount}`);

  // Step 4: Sync customers (incremental via since_id)
  progress("Syncing customers", 0.6);
  let customerCount = 0;
  const lastCustomerId = await getLatestShopifyCustomerId();

  for await (const batch of client.fetchCustomers({ sinceId: lastCustomerId ?? undefined })) {
    const saved = await saveShopifyCustomers(batch);
    customerCount += saved;
  }
  console.log(`[shopify] Customers synced: ${customerCount}`);

  // Step 5: Sync inventory (full refresh for all active locations)
  progress("Syncing inventory", 0.8);
  let inventoryCount = 0;

  try {
    const locations = await client.fetchLocations();
    const activeLocationIds = locations.filter((l) => l.active).map((l) => l.id);

    if (activeLocationIds.length > 0) {
      for await (const batch of client.fetchInventoryLevels(activeLocationIds)) {
        const saved = await saveShopifyInventory(batch);
        inventoryCount += saved;
      }
    }
    console.log(`[shopify] Inventory synced: ${inventoryCount} levels across ${activeLocationIds.length} locations`);
  } catch (err) {
    // Inventory access might not be granted — non-fatal
    console.warn(`[shopify] Inventory sync skipped:`, err instanceof Error ? err.message : err);
  }

  progress("Shopify sync complete", 1.0);

  return {
    orderCount,
    productCount,
    customerCount,
    inventoryCount,
    shopName: shop.name,
  };
}
