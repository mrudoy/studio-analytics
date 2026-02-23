/**
 * Core pipeline logic: parse CSVs → run analytics → save to database.
 *
 * Called from both:
 * - BullMQ worker (scraper-based pipeline)
 * - Upload API route (manual CSV upload)
 *
 * The database is the single source of truth. No Google Sheets export.
 */

import { parseCSV } from "../parser/csv-parser";
import {
  NewCustomerSchema,
  OrderSchema,
  FirstVisitSchema,
  RegistrationSchema,
  AutoRenewSchema,
  RevenueCategorySchema,
  FullRegistrationSchema,
} from "../parser/schemas";
import { analyzeFunnel } from "../analytics/funnel";
import { analyzeFunnelOverview } from "../analytics/funnel-overview";
import { analyzeChurn } from "../analytics/churn";
import { analyzeVolume } from "../analytics/volume";
import { computeSummary } from "../analytics/summary";
import { analyzeTrends } from "../analytics/trends";
import { analyzeRevenueCategories } from "../analytics/revenue-categories";
import { analyzeReturningNonMembers } from "../analytics/returning-non-members";
import type { PipelineResult, ValidationResult } from "@/types/pipeline";
import type {
  NewCustomer,
  Order,
  FirstVisit,
  Registration,
  AutoRenew,
  RevenueCategory,
  FullRegistration,
  DownloadedFiles,
} from "@/types/union-data";
import {
  saveRevenueCategories,
  savePipelineRun,
  lockPeriod,
} from "../db/revenue-store";
import {
  saveAutoRenews,
  type AutoRenewRow,
} from "../db/auto-renew-store";
import {
  saveRegistrations,
  saveFirstVisits,
  type RegistrationRow,
} from "../db/registration-store";
import { saveOrders, type OrderRow } from "../db/order-store";
import { saveCustomers, type CustomerRow } from "../db/customer-store";
import { setWatermark } from "../db/watermark-store";
import { writeFileSync } from "fs";
import { join } from "path";

type ProgressCallback = (step: string, percent: number) => void;

/**
 * Run the full analytics pipeline from pre-downloaded CSV files.
 * No Playwright, no BullMQ — pure parse → analyze → export.
 */
export async function runPipelineFromFiles(
  files: DownloadedFiles,
  analyticsSheetId?: string,
  options?: {
    rawDataSheetId?: string;
    dateRange?: string;
    onProgress?: ProgressCallback;
  }
): Promise<PipelineResult> {
  const startTime = Date.now();
  const allWarnings: string[] = [];
  const progress = options?.onProgress ?? (() => {});
  const dateRange = options?.dateRange ?? "";

  // ── Parse CSVs ──────────────────────────────────────────────
  progress("Parsing CSV data", 50);

  // Helper: parse a CSV file if it exists, otherwise return empty data
  const emptyResult = <T>() => ({ data: [] as T[], warnings: [] as string[] });

  const newCustomersResult = files.newCustomers
    ? parseCSV<NewCustomer>(files.newCustomers, NewCustomerSchema)
    : emptyResult<NewCustomer>();
  const ordersResult = files.orders
    ? parseCSV<Order>(files.orders, OrderSchema)
    : emptyResult<Order>();

  if (ordersResult.data.length > 0) {
    console.log(`[pipeline-core] First order sample:`, JSON.stringify(ordersResult.data[0]));
  } else {
    console.log(`[pipeline-core] WARNING: 0 orders parsed from CSV`);
  }
  if (ordersResult.warnings.length > 0) {
    console.log(`[pipeline-core] Order parse warnings (first 5):`, ordersResult.warnings.slice(0, 5));
  }

  const firstVisitsResult = files.firstVisits
    ? parseCSV<FirstVisit>(files.firstVisits, FirstVisitSchema)
    : emptyResult<FirstVisit>();
  const registrationsResult = files.allRegistrations
    ? parseCSV<Registration>(files.allRegistrations, RegistrationSchema)
    : emptyResult<Registration>();
  const canceledResult = files.canceledAutoRenews
    ? parseCSV<AutoRenew>(files.canceledAutoRenews, AutoRenewSchema)
    : emptyResult<AutoRenew>();
  const activeResult = files.activeAutoRenews
    ? parseCSV<AutoRenew>(files.activeAutoRenews, AutoRenewSchema)
    : emptyResult<AutoRenew>();
  const pausedResult = files.pausedAutoRenews
    ? parseCSV<AutoRenew>(files.pausedAutoRenews, AutoRenewSchema)
    : emptyResult<AutoRenew>();
  const trialingResult = files.trialingAutoRenews
    ? parseCSV<AutoRenew>(files.trialingAutoRenews, AutoRenewSchema)
    : emptyResult<AutoRenew>();
  const newAutoRenewsResult = files.newAutoRenews
    ? parseCSV<AutoRenew>(files.newAutoRenews, AutoRenewSchema)
    : emptyResult<AutoRenew>();
  const revenueCatResult = files.revenueCategories
    ? parseCSV<RevenueCategory>(files.revenueCategories, RevenueCategorySchema)
    : emptyResult<RevenueCategory>();

  const allCurrentSubs = [...activeResult.data, ...pausedResult.data, ...trialingResult.data];
  console.log(
    `[pipeline-core] Subscriber merge: ${activeResult.data.length} active + ${pausedResult.data.length} paused + ${trialingResult.data.length} trialing = ${allCurrentSubs.length} total`
  );

  allWarnings.push(
    ...newCustomersResult.warnings,
    ...ordersResult.warnings,
    ...firstVisitsResult.warnings,
    ...registrationsResult.warnings,
    ...canceledResult.warnings,
    ...activeResult.warnings,
    ...pausedResult.warnings,
    ...trialingResult.warnings,
    ...newAutoRenewsResult.warnings,
    ...revenueCatResult.warnings
  );

  const recordCounts = {
    newCustomers: newCustomersResult.data.length,
    orders: ordersResult.data.length,
    firstVisits: firstVisitsResult.data.length,
    registrations: registrationsResult.data.length,
    canceledAutoRenews: canceledResult.data.length,
    activeAutoRenews: activeResult.data.length,
    pausedAutoRenews: pausedResult.data.length,
    trialingAutoRenews: trialingResult.data.length,
    newAutoRenews: newAutoRenewsResult.data.length,
    revenueCategories: revenueCatResult.data.length,
  };

  progress("CSV parsing complete", 55);

  // ── Run Analytics ───────────────────────────────────────────
  progress("Running funnel analysis", 58);
  const funnelResults = analyzeFunnel(
    firstVisitsResult.data,
    newAutoRenewsResult.data,
    newCustomersResult.data
  );

  progress("Running funnel overview", 62);
  const funnelOverview = analyzeFunnelOverview(
    newCustomersResult.data,
    firstVisitsResult.data,
    newAutoRenewsResult.data
  );

  progress("Running churn analysis", 65);
  const churnResults = analyzeChurn(
    canceledResult.data,
    allCurrentSubs,
    newAutoRenewsResult.data
  );

  progress("Running volume analysis", 70);
  const volumeResults = analyzeVolume(
    newAutoRenewsResult.data,
    canceledResult.data,
    newCustomersResult.data
  );

  progress("Computing summary KPIs", 70);
  const summary = computeSummary(allCurrentSubs, ordersResult.data);

  console.log(`[pipeline-core] computeSummary input: ${allCurrentSubs.length} total subs`);
  console.log(
    `[pipeline-core] Category breakdown: MEMBER=${summary.activeMembers}, SKY3=${summary.activeSky3}, TV=${summary.activeSkyTingTv}, UNKNOWN=${summary.activeUnknown}, TOTAL=${summary.activeTotal}`
  );
  if (summary.activeUnknown > 0) {
    console.log(`[pipeline-core] UNKNOWN plan names:`, JSON.stringify(summary.unknownPlanNames));
  }
  if (Object.keys(summary.skippedStates).length > 0) {
    console.log(`[pipeline-core] Skipped states:`, JSON.stringify(summary.skippedStates));
  }

  // Dump unique plan names for debugging
  try {
    const planNameCounts: Record<string, number> = {};
    for (const ar of allCurrentSubs) {
      const key = `${ar.name} | state=${ar.state}`;
      planNameCounts[key] = (planNameCounts[key] || 0) + 1;
    }
    const debugPath = join(process.cwd(), "data", "debug-plan-names.json");
    writeFileSync(debugPath, JSON.stringify(planNameCounts, null, 2));
    console.log(`[pipeline-core] Wrote debug plan names to ${debugPath}`);
  } catch {
    /* ignore */
  }

  // Order type revenue breakdown
  const orderTypeRevenue: Record<string, number> = {};
  for (const order of ordersResult.data) {
    const t = order.type || "(blank)";
    orderTypeRevenue[t] = (orderTypeRevenue[t] || 0) + order.total;
  }
  console.log(`[pipeline-core] Order type revenue breakdown (${ordersResult.data.length} orders):`);
  const sortedTypes = Object.entries(orderTypeRevenue).sort((a, b) => b[1] - a[1]);
  for (const [type, revenue] of sortedTypes) {
    console.log(`  "${type}": $${revenue.toFixed(2)}`);
  }
  console.log(
    `[pipeline-core] Current month revenue: $${summary.currentMonthRevenue}, Previous month: $${summary.previousMonthRevenue}`
  );

  progress("Running trends analysis", 73);
  const trendsResults = analyzeTrends(
    newAutoRenewsResult.data,
    canceledResult.data,
    allCurrentSubs,
    summary,
    ordersResult.data,
    firstVisitsResult.data
  );

  // Returning non-members (requires fullRegistrations CSV)
  if (files.fullRegistrations) {
    progress("Analyzing returning non-members", 74);
    try {
      const fullRegsResult = parseCSV<FullRegistration>(
        files.fullRegistrations,
        FullRegistrationSchema
      );
      allWarnings.push(...fullRegsResult.warnings);
      console.log(`[pipeline-core] Full registrations: ${fullRegsResult.data.length} rows parsed`);

      const rnmStats = analyzeReturningNonMembers(
        fullRegsResult.data,
        firstVisitsResult.data
      );
      trendsResults.returningNonMembers = rnmStats;
    } catch (err) {
      console.warn(
        `[pipeline-core] Returning non-members analysis failed: ${err instanceof Error ? err.message : err}`
      );
      allWarnings.push(
        `Returning non-members analysis failed: ${err instanceof Error ? err.message : "unknown"}`
      );
    }
  } else {
    console.log(`[pipeline-core] fullRegistrations not available — skipping returning non-members analysis`);
  }

  // Save revenue categories to database
  if (revenueCatResult.data.length > 0) {
    progress("Saving revenue categories to database", 74);
    const drParts = (dateRange || "").split(" - ").map((s) => s.trim());
    // Normalize to YYYY-MM-DD (input can be M/D/YYYY from union.fit date range)
    const toISO = (s: string): string => {
      const d = new Date(s);
      return !isNaN(d.getTime()) ? d.toISOString().slice(0, 10) : s;
    };
    const periodStart = toISO(drParts[0] || new Date().toISOString().slice(0, 10));
    const periodEnd = toISO(drParts[1] || new Date().toISOString().slice(0, 10));
    try {
      await saveRevenueCategories(periodStart, periodEnd, revenueCatResult.data);
      console.log(`[pipeline-core] Saved ${revenueCatResult.data.length} revenue categories to database`);
      await setWatermark("revenueCategories", periodEnd, revenueCatResult.data.length, `pipeline run ${periodStart} to ${periodEnd}`);

      const currentMonthStart = new Date(new Date().getFullYear(), new Date().getMonth(), 1);
      const periodEndDate = new Date(periodEnd);
      if (!isNaN(periodEndDate.getTime()) && periodEndDate < currentMonthStart) {
        await lockPeriod(periodStart, periodEnd);
        console.log(`[pipeline-core] Auto-locked completed period ${periodStart} – ${periodEnd}`);
      }
    } catch (dbErr) {
      console.warn(
        `[pipeline-core] Failed to save revenue categories to database: ${dbErr instanceof Error ? dbErr.message : dbErr}`
      );
      allWarnings.push(`Database save failed: ${dbErr instanceof Error ? dbErr.message : "unknown error"}`);
    }
  }

  // Save auto-renews to database (all auto-renew reports merged)
  const allAutoRenewsForDb = [
    ...activeResult.data,
    ...pausedResult.data,
    ...trialingResult.data,
    ...newAutoRenewsResult.data,
    ...canceledResult.data,
  ];
  if (allAutoRenewsForDb.length > 0) {
    progress("Saving auto-renews to database", 74);
    try {
      const snapshotId = `pipeline-${Date.now()}`;
      const arRows: AutoRenewRow[] = allAutoRenewsForDb.map((ar) => ({
        planName: ar.name,
        planState: ar.state,
        planPrice: ar.price,
        customerName: ar.customer,
        customerEmail: ar.email || "",
        createdAt: ar.created || "",
        canceledAt: ar.canceledAt || undefined,
      }));
      await saveAutoRenews(snapshotId, arRows);
      console.log(`[pipeline-core] Saved ${arRows.length} auto-renews to database (snapshot: ${snapshotId})`);
      // Watermark: use the latest created_at from auto-renews
      const latestArDate = arRows.reduce((max, r) => (r.createdAt > max ? r.createdAt : max), "");
      if (latestArDate) {
        await setWatermark("autoRenews", latestArDate.slice(0, 10), arRows.length, `pipeline snapshot ${snapshotId}`);
      }
    } catch (dbErr) {
      console.warn(
        `[pipeline-core] Failed to save auto-renews to database: ${dbErr instanceof Error ? dbErr.message : dbErr}`
      );
      allWarnings.push(`Auto-renew Database save failed: ${dbErr instanceof Error ? dbErr.message : "unknown"}`);
    }
  }

  // Save full registrations to database
  if (files.fullRegistrations) {
    try {
      const fullRegsForDb = parseCSV<FullRegistration>(files.fullRegistrations, FullRegistrationSchema);
      if (fullRegsForDb.data.length > 0) {
        progress("Saving registrations to database", 74);
        const regRows: RegistrationRow[] = fullRegsForDb.data.map((r) => ({
          eventName: r.eventName,
          eventId: r.eventId || undefined,
          performanceId: r.performanceId || undefined,
          performanceStartsAt: r.performanceStartsAt || "",
          locationName: r.locationName,
          videoName: r.videoName || undefined,
          videoId: r.videoId || undefined,
          teacherName: r.teacherName,
          firstName: r.firstName,
          lastName: r.lastName,
          email: r.email,
          phone: r.phoneNumber || undefined,
          role: r.role || undefined,
          registeredAt: r.registeredAt || undefined,
          canceledAt: r.canceledAt || undefined,
          attendedAt: r.attendedAt,
          registrationType: r.registrationType,
          state: r.state,
          pass: r.pass,
          subscription: String(r.subscription),
          revenueState: r.revenueState || undefined,
          revenue: r.revenue,
        }));
        await saveRegistrations(regRows);
        console.log(`[pipeline-core] Saved ${regRows.length} registrations to database`);
        // Watermark: use the latest attended_at
        const latestRegDate = regRows.reduce((max, r) => (r.attendedAt > max ? r.attendedAt : max), "");
        if (latestRegDate) {
          await setWatermark("registrations", latestRegDate.slice(0, 10), regRows.length, "pipeline run");
        }
      }
    } catch (dbErr) {
      console.warn(
        `[pipeline-core] Failed to save registrations to database: ${dbErr instanceof Error ? dbErr.message : dbErr}`
      );
      allWarnings.push(`Registration Database save failed: ${dbErr instanceof Error ? dbErr.message : "unknown"}`);
    }
  }

  // Save first visits to database
  if (firstVisitsResult.data.length > 0 && files.firstVisits) {
    progress("Saving first visits to database", 74);
    try {
      // FirstVisit schema has different fields, map to RegistrationRow
      const fvRows: RegistrationRow[] = firstVisitsResult.data.map((fv) => ({
        eventName: fv.performance || "",
        performanceStartsAt: "",
        locationName: "",
        teacherName: "",
        firstName: "",
        lastName: "",
        email: "",
        attendedAt: fv.redeemedAt || "",
        registrationType: fv.type || "",
        state: fv.status || "",
        pass: fv.pass || "",
        subscription: "false",
        revenue: 0,
      }));
      await saveFirstVisits(fvRows);
      console.log(`[pipeline-core] Saved ${fvRows.length} first visits to database`);
      // Watermark: use the latest attended_at
      const latestFvDate = fvRows.reduce((max, r) => (r.attendedAt > max ? r.attendedAt : max), "");
      if (latestFvDate) {
        await setWatermark("firstVisits", latestFvDate.slice(0, 10), fvRows.length, "pipeline run");
      }
    } catch (dbErr) {
      console.warn(
        `[pipeline-core] Failed to save first visits to database: ${dbErr instanceof Error ? dbErr.message : dbErr}`
      );
      allWarnings.push(`First visits Database save failed: ${dbErr instanceof Error ? dbErr.message : "unknown"}`);
    }
  }

  // Save orders to database
  if (ordersResult.data.length > 0) {
    progress("Saving orders to database", 74);
    try {
      const orderRows: OrderRow[] = ordersResult.data.map((o) => ({
        created: o.created,
        code: o.code,
        customer: o.customer,
        type: o.type,
        payment: o.payment,
        total: o.total,
      }));
      await saveOrders(orderRows);
      console.log(`[pipeline-core] Saved ${orderRows.length} orders to database`);
      // Watermark: use the latest created date
      const latestOrderDate = orderRows.reduce((max, r) => (r.created > max ? r.created : max), "");
      if (latestOrderDate) {
        await setWatermark("orders", latestOrderDate.slice(0, 10), orderRows.length, "pipeline run");
      }
    } catch (dbErr) {
      console.warn(
        `[pipeline-core] Failed to save orders to database: ${dbErr instanceof Error ? dbErr.message : dbErr}`
      );
      allWarnings.push(`Orders Database save failed: ${dbErr instanceof Error ? dbErr.message : "unknown"}`);
    }
  }

  // Save new customers to database
  if (newCustomersResult.data.length > 0) {
    progress("Saving customers to database", 74);
    try {
      const custRows: CustomerRow[] = newCustomersResult.data.map((c) => ({
        name: c.name,
        email: c.email,
        role: c.role,
        orders: c.orders,
        created: c.created,
      }));
      await saveCustomers(custRows);
      console.log(`[pipeline-core] Saved ${custRows.length} customers to database`);
      // Watermark: use the latest created date
      const latestCustDate = custRows.reduce((max, r) => (r.created > max ? r.created : max), "");
      if (latestCustDate) {
        await setWatermark("newCustomers", latestCustDate.slice(0, 10), custRows.length, "pipeline run");
      }
    } catch (dbErr) {
      console.warn(
        `[pipeline-core] Failed to save customers to database: ${dbErr instanceof Error ? dbErr.message : dbErr}`
      );
      allWarnings.push(`Customers Database save failed: ${dbErr instanceof Error ? dbErr.message : "unknown"}`);
    }
  }

  // Revenue category analysis (log only)
  if (revenueCatResult.data.length > 0) {
    progress("Analyzing revenue categories", 85);
    console.log(`[pipeline-core] Revenue Categories (${revenueCatResult.data.length} categories):`);
    const sorted = [...revenueCatResult.data].sort((a, b) => b.netRevenue - a.netRevenue);
    for (const cat of sorted.slice(0, 10)) {
      console.log(
        `  "${cat.revenueCategory}": revenue=$${cat.revenue.toFixed(2)}, net=$${cat.netRevenue.toFixed(2)}`
      );
    }
  }

  // Save pipeline run to database
  progress("Saving pipeline run record", 90);
  try {
    const drParts = (dateRange || "").split(" - ").map((s) => s.trim());
    const toISO2 = (s: string): string => {
      const d = new Date(s);
      return !isNaN(d.getTime()) ? d.toISOString().slice(0, 10) : s;
    };
    await savePipelineRun(toISO2(drParts[0] || ""), toISO2(drParts[1] || ""), recordCounts, Date.now() - startTime);
  } catch {
    /* non-critical */
  }

  // Validate completeness
  const validation = validateCompleteness(recordCounts);
  if (!validation.passed) {
    const failed = validation.checks.filter((c) => c.status === "fail").map((c) => c.name);
    console.warn(`[pipeline] Completeness check failed: missing ${failed.join(", ")}`);
    allWarnings.push(`Data validation warning: ${failed.join(", ")} returned 0 records`);
  }

  progress("Pipeline complete!", 100);

  return {
    success: true,
    sheetUrl: "",
    rawDataSheetUrl: "",
    duration: Date.now() - startTime,
    recordCounts,
    warnings: allWarnings,
    validation,
  };
}

// ── Completeness Validation ──────────────────────────────────

function validateCompleteness(recordCounts: Record<string, number>): ValidationResult {
  const specs = [
    { name: "Active Auto-Renews", key: "activeAutoRenews", min: 100 },
    { name: "Canceled Auto-Renews", key: "canceledAutoRenews", min: 0 },
    { name: "New Auto-Renews", key: "newAutoRenews", min: 0 },
    { name: "Orders", key: "orders", min: 10 },
    { name: "New Customers", key: "newCustomers", min: 1 },
    { name: "First Visits", key: "firstVisits", min: 0 },
    { name: "Registrations", key: "registrations", min: 0 },
  ];

  const checks = specs.map((s) => {
    const count = recordCounts[s.key] || 0;
    let status: "ok" | "warn" | "fail";
    if (s.min === 0) {
      status = "ok"; // optional — any value is fine
    } else if (count >= s.min) {
      status = "ok";
    } else if (count > 0) {
      status = "warn"; // has data but less than expected
    } else {
      status = "fail"; // zero when we expected data
    }
    return { name: s.name, count, status };
  });

  return {
    passed: checks.every((c) => c.status !== "fail"),
    checks,
  };
}
