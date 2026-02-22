/**
 * Shopify Admin REST API client.
 *
 * Uses built-in fetch (no extra deps). Handles:
 * - OAuth client credentials grant (exchanges clientId/clientSecret for 24h token)
 * - Token caching + auto-refresh on expiry
 * - Cursor-based pagination via Link header
 * - Rate limiting via X-Shopify-Shop-Api-Call-Limit header
 */

const API_VERSION = "2024-01";
const MAX_PER_PAGE = 250;

/** Pause when we've used this fraction of the rate-limit bucket. */
const THROTTLE_THRESHOLD = 0.8;
const THROTTLE_DELAY_MS = 1_000;

/** Refresh the token 5 minutes before it actually expires. */
const TOKEN_REFRESH_BUFFER_MS = 5 * 60 * 1_000;

export interface ShopifyClientConfig {
  storeName: string;
  clientId: string;
  clientSecret: string;
}

export interface ShopifyShop {
  id: number;
  name: string;
  email: string;
  domain: string;
  myshopify_domain: string;
  plan_name: string;
  currency: string;
}

export interface ShopifyOrder {
  id: number;
  order_number: number;
  email: string | null;
  financial_status: string;
  fulfillment_status: string | null;
  total_price: string;
  subtotal_price: string;
  total_tax: string;
  total_discounts: string;
  currency: string;
  line_items: Array<{
    id: number;
    title: string;
    quantity: number;
    price: string;
    sku: string | null;
    product_id: number | null;
    variant_id: number | null;
  }>;
  customer?: { id: number } | null;
  created_at: string;
  updated_at: string;
  cancelled_at: string | null;
}

export interface ShopifyProduct {
  id: number;
  title: string;
  product_type: string;
  vendor: string;
  status: string;
  tags: string;
  variants: Array<{
    id: number;
    title: string;
    price: string;
    sku: string | null;
    inventory_quantity: number;
    inventory_item_id: number;
  }>;
  created_at: string;
  updated_at: string;
}

export interface ShopifyCustomer {
  id: number;
  email: string | null;
  first_name: string | null;
  last_name: string | null;
  orders_count: number;
  total_spent: string;
  tags: string;
  created_at: string;
  updated_at: string;
}

export interface ShopifyInventoryLevel {
  inventory_item_id: number;
  location_id: number;
  available: number | null;
  updated_at: string;
}

export interface ShopifyLocation {
  id: number;
  name: string;
  active: boolean;
}

// ─── Client ──────────────────────────────────────────────────

export class ShopifyClient {
  private baseUrl: string;
  private storeUrl: string;
  private cachedToken: string | null = null;
  private tokenExpiresAt = 0;

  constructor(private config: ShopifyClientConfig) {
    this.storeUrl = `https://${config.storeName}.myshopify.com`;
    this.baseUrl = `${this.storeUrl}/admin/api/${API_VERSION}`;
  }

  // ── OAuth client credentials grant ───────────────────────────

  /**
   * Exchange clientId + clientSecret for a short-lived (24h) access token.
   * Caches the token and auto-refreshes when expired.
   */
  private async getAccessToken(): Promise<string> {
    if (this.cachedToken && Date.now() < this.tokenExpiresAt) {
      return this.cachedToken;
    }

    console.log("[shopify] Requesting new access token via client credentials grant");

    const res = await fetch(`${this.storeUrl}/admin/oauth/access_token`, {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body: new URLSearchParams({
        grant_type: "client_credentials",
        client_id: this.config.clientId,
        client_secret: this.config.clientSecret,
      }).toString(),
    });

    if (!res.ok) {
      const body = await res.text().catch(() => "");
      throw new Error(
        `Shopify token exchange failed (${res.status}): ${res.statusText} — ${body.slice(0, 300)}`
      );
    }

    const data = (await res.json()) as {
      access_token: string;
      scope: string;
      expires_in: number;
    };

    this.cachedToken = data.access_token;
    // expires_in is in seconds (typically 86399 = ~24h). Buffer by 5 min.
    this.tokenExpiresAt = Date.now() + data.expires_in * 1000 - TOKEN_REFRESH_BUFFER_MS;

    console.log(`[shopify] Got access token (scope: ${data.scope}, expires_in: ${data.expires_in}s)`);
    return this.cachedToken;
  }

  // ── Core fetch with rate limiting ───────────────────────────

  private async request<T>(url: string): Promise<{ data: T; nextUrl: string | null }> {
    const token = await this.getAccessToken();
    const res = await fetch(url, {
      headers: {
        "X-Shopify-Access-Token": token,
        "Content-Type": "application/json",
      },
    });

    if (!res.ok) {
      const body = await res.text().catch(() => "");
      throw new Error(`Shopify API ${res.status}: ${res.statusText} — ${body.slice(0, 200)}`);
    }

    // Rate limiting: "32/40" means 32 used out of 40
    const callLimit = res.headers.get("X-Shopify-Shop-Api-Call-Limit");
    if (callLimit) {
      const [used, max] = callLimit.split("/").map(Number);
      if (used / max >= THROTTLE_THRESHOLD) {
        console.log(`[shopify] Rate limit ${callLimit} — throttling ${THROTTLE_DELAY_MS}ms`);
        await sleep(THROTTLE_DELAY_MS);
      }
    }

    // Pagination: parse Link header for rel="next"
    const linkHeader = res.headers.get("Link");
    const nextUrl = parseLinkNext(linkHeader);

    const data = (await res.json()) as T;
    return { data, nextUrl };
  }

  // ── Public methods ──────────────────────────────────────────

  /** Test connection by fetching shop info. */
  async testConnection(): Promise<ShopifyShop> {
    const { data } = await this.request<{ shop: ShopifyShop }>(`${this.baseUrl}/shop.json`);
    return data.shop;
  }

  /**
   * Fetch all orders, paginated.
   * Uses `since_id` for incremental fetching (most efficient for Shopify REST API).
   * Falls back to `created_at_min` if provided.
   */
  async *fetchOrders(opts?: { sinceId?: number; createdAtMin?: string }): AsyncGenerator<ShopifyOrder[]> {
    let url = `${this.baseUrl}/orders.json?status=any&limit=${MAX_PER_PAGE}`;
    if (opts?.sinceId) url += `&since_id=${opts.sinceId}`;
    else if (opts?.createdAtMin) url += `&created_at_min=${opts.createdAtMin}`;

    while (url) {
      const { data, nextUrl } = await this.request<{ orders: ShopifyOrder[] }>(url);
      if (data.orders.length > 0) yield data.orders;
      url = nextUrl || "";
    }
  }

  /** Fetch all products, paginated. */
  async *fetchProducts(opts?: { sinceId?: number }): AsyncGenerator<ShopifyProduct[]> {
    let url = `${this.baseUrl}/products.json?limit=${MAX_PER_PAGE}`;
    if (opts?.sinceId) url += `&since_id=${opts.sinceId}`;

    while (url) {
      const { data, nextUrl } = await this.request<{ products: ShopifyProduct[] }>(url);
      if (data.products.length > 0) yield data.products;
      url = nextUrl || "";
    }
  }

  /** Fetch all customers, paginated. */
  async *fetchCustomers(opts?: { sinceId?: number; updatedAtMin?: string }): AsyncGenerator<ShopifyCustomer[]> {
    let url = `${this.baseUrl}/customers.json?limit=${MAX_PER_PAGE}`;
    if (opts?.sinceId) url += `&since_id=${opts.sinceId}`;
    else if (opts?.updatedAtMin) url += `&updated_at_min=${opts.updatedAtMin}`;

    while (url) {
      const { data, nextUrl } = await this.request<{ customers: ShopifyCustomer[] }>(url);
      if (data.customers.length > 0) yield data.customers;
      url = nextUrl || "";
    }
  }

  /** Fetch all locations (needed for inventory). */
  async fetchLocations(): Promise<ShopifyLocation[]> {
    const { data } = await this.request<{ locations: ShopifyLocation[] }>(`${this.baseUrl}/locations.json`);
    return data.locations;
  }

  /** Fetch inventory levels for given location IDs, paginated. */
  async *fetchInventoryLevels(locationIds: number[]): AsyncGenerator<ShopifyInventoryLevel[]> {
    for (const locId of locationIds) {
      let url = `${this.baseUrl}/inventory_levels.json?location_ids=${locId}&limit=${MAX_PER_PAGE}`;
      while (url) {
        const { data, nextUrl } = await this.request<{ inventory_levels: ShopifyInventoryLevel[] }>(url);
        if (data.inventory_levels.length > 0) yield data.inventory_levels;
        url = nextUrl || "";
      }
    }
  }
}

// ── Helpers ──────────────────────────────────────────────────

function parseLinkNext(header: string | null): string | null {
  if (!header) return null;
  // Link header format: <https://...>; rel="next", <https://...>; rel="previous"
  const parts = header.split(",");
  for (const part of parts) {
    const match = part.match(/<([^>]+)>;\s*rel="next"/);
    if (match) return match[1];
  }
  return null;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
