import { NextResponse } from "next/server";
import { loadSettings } from "@/lib/crypto/credentials";
import { ShopifyClient } from "@/lib/shopify/shopify-client";
import { runShopifySync } from "@/lib/shopify/shopify-sync";
import { getShopifyStats } from "@/lib/db/shopify-store";
import { uploadBackupToGitHub } from "@/lib/db/backup-cloud";

export const dynamic = "force-dynamic";

/**
 * GET /api/shopify — Test connection + return shop info and sync stats.
 */
export async function GET() {
  try {
    const settings = loadSettings();

    if (!settings?.shopify?.storeName || !settings?.shopify?.clientId || !settings?.shopify?.clientSecret) {
      return NextResponse.json({
        connected: false,
        error: "Shopify not configured. Set SHOPIFY_STORE_NAME, SHOPIFY_CLIENT_ID, and SHOPIFY_CLIENT_SECRET.",
      });
    }

    const client = new ShopifyClient({
      storeName: settings.shopify.storeName,
      clientId: settings.shopify.clientId,
      clientSecret: settings.shopify.clientSecret,
    });

    // Test connection
    const shop = await client.testConnection();

    // Get local stats
    let stats = null;
    try {
      stats = await getShopifyStats();
    } catch {
      // Tables might not exist yet (migration hasn't run)
    }

    return NextResponse.json({
      connected: true,
      shop: {
        name: shop.name,
        domain: shop.domain,
        myshopifyDomain: shop.myshopify_domain,
        plan: shop.plan_name,
        currency: shop.currency,
      },
      stats,
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Failed to connect to Shopify";
    return NextResponse.json({ connected: false, error: message }, { status: 500 });
  }
}

/**
 * POST /api/shopify — Trigger a manual Shopify sync (runs inline).
 */
export async function POST() {
  try {
    const settings = loadSettings();

    if (!settings?.shopify?.storeName || !settings?.shopify?.clientId || !settings?.shopify?.clientSecret) {
      return NextResponse.json(
        { error: "Shopify not configured. Set SHOPIFY_STORE_NAME, SHOPIFY_CLIENT_ID, and SHOPIFY_CLIENT_SECRET." },
        { status: 400 }
      );
    }

    console.log("[api/shopify] Manual sync triggered");

    const result = await runShopifySync({
      storeName: settings.shopify.storeName,
      clientId: settings.shopify.clientId,
      clientSecret: settings.shopify.clientSecret,
    });

    console.log(
      `[api/shopify] Sync complete: ${result.orderCount} orders, ` +
      `${result.productCount} products, ${result.customerCount} customers, ` +
      `${result.inventoryCount} inventory levels`
    );

    // Cloud backup after sync (non-fatal)
    try {
      const cloud = await uploadBackupToGitHub();
      console.log(`[api/shopify] Cloud backup: ${cloud.tag}`);
    } catch (backupErr) {
      console.warn("[api/shopify] Cloud backup failed (non-fatal):", backupErr instanceof Error ? backupErr.message : backupErr);
    }

    return NextResponse.json({ success: true, ...result });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Shopify sync failed";
    console.error("[api/shopify] Sync error:", message);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
