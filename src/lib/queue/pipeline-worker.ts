import { Worker, Job } from "bullmq";
import { getRedisConnection } from "./connection";
import { UnionClient } from "../scraper/union-client";
import { loadSettings } from "../crypto/credentials";
import { parseCSV } from "../parser/csv-parser";
import {
  NewCustomerSchema,
  OrderSchema,
  FirstVisitSchema,
  RegistrationSchema,
  AutoRenewSchema,
  RevenueCategorySchema,
} from "../parser/schemas";
import { analyzeFunnel } from "../analytics/funnel";
import { analyzeFunnelOverview } from "../analytics/funnel-overview";
import { analyzeChurn } from "../analytics/churn";
import { analyzeVolume } from "../analytics/volume";
import { computeSummary } from "../analytics/summary";
import { analyzeTrends } from "../analytics/trends";
import { analyzeRevenueCategories } from "../analytics/revenue-categories";
import { getSpreadsheet, getSheetUrl } from "../sheets/sheets-client";
import {
  writeDashboardTab,
  writeFunnelOverviewTab,
  writeFunnelTab,
  writeWeeklyVolumeTab,
  writeChurnTab,
  writeRunLogEntry,
  writeRawDataSheets,
  writeTrendsTab,
  writeRevenueCategoriesTab,
} from "../sheets/templates";
import { cleanupDownloads } from "../scraper/download-manager";
import { clearDashboardCache } from "../sheets/read-dashboard";
import type { PipelineJobData, PipelineResult } from "@/types/pipeline";
import type { NewCustomer, Order, FirstVisit, Registration, AutoRenew, RevenueCategory } from "@/types/union-data";
import { saveRevenueCategories, savePipelineRun, getLatestPeriod, lockPeriod } from "../db/revenue-store";
import { writeFileSync, copyFileSync } from "fs";
import { join } from "path";

/** Maximum total time the pipeline is allowed to run before being killed. */
const PIPELINE_TIMEOUT_MS = 20 * 60 * 1000; // 20 minutes

/**
 * Race a promise against a timeout. If the timeout fires first, throw.
 */
async function withTimeout<T>(promise: Promise<T>, ms: number, label: string): Promise<T> {
  let timer: ReturnType<typeof setTimeout>;
  const timeout = new Promise<never>((_, reject) => {
    timer = setTimeout(() => reject(new Error(`Timeout: ${label} (after ${ms / 1000}s)`)), ms);
  });
  try {
    return await Promise.race([promise, timeout]);
  } finally {
    clearTimeout(timer!);
  }
}

function updateProgress(job: Job, step: string, percent: number) {
  job.updateProgress({ step, percent });
}

async function runPipeline(job: Job): Promise<PipelineResult> {
  return withTimeout(
    runPipelineInner(job),
    PIPELINE_TIMEOUT_MS,
    "Pipeline timed out after 20 minutes — try again or check Union.fit connectivity"
  );
}

async function runPipelineInner(job: Job): Promise<PipelineResult> {
  const startTime = Date.now();
  const allWarnings: string[] = [];

  // Step 1: Load credentials
  updateProgress(job, "Loading credentials", 5);
  const settings = loadSettings();
  if (!settings?.credentials) {
    throw new Error("No Union.fit credentials configured. Go to Settings to configure.");
  }

  const analyticsSheetId = settings.analyticsSpreadsheetId || process.env.ANALYTICS_SPREADSHEET_ID;
  const rawDataSheetId = settings.rawDataSpreadsheetId || process.env.RAW_DATA_SPREADSHEET_ID;

  if (!analyticsSheetId) {
    throw new Error("Analytics Spreadsheet ID not configured.");
  }

  // Step 2: Scrape Union.fit
  updateProgress(job, "Launching browser", 10);
  const client = new UnionClient({
    onProgress: (step, percent) => updateProgress(job, step, percent),
  });

  let files;
  let dateRange = job.data.dateRangeStart && job.data.dateRangeEnd
    ? `${job.data.dateRangeStart} - ${job.data.dateRangeEnd}`
    : "";

  try {
    await client.initialize();
    updateProgress(job, "Logging into Union.fit", 15);
    await client.login(settings.credentials.email, settings.credentials.password);

    updateProgress(job, "Downloading CSV reports", 20);

    // Incremental mode: if no date range provided, check SQLite for last locked period.
    // If we have historical data, only fetch from the start of last month forward.
    // First run = full 12 months; subsequent runs = current + previous month only.
    if (!dateRange) {
      const now = new Date();
      const endStr = `${now.getMonth() + 1}/${now.getDate()}/${now.getFullYear()}`;
      let startDate: Date;

      try {
        const latestPeriod = getLatestPeriod();
        if (latestPeriod) {
          // We have data — only fetch from 1st of last month to catch any late entries
          const lastMonth = new Date(now.getFullYear(), now.getMonth() - 1, 1);
          startDate = lastMonth;
          console.log(`[pipeline] Incremental mode: have data through ${latestPeriod.periodEnd}, fetching from ${lastMonth.toISOString().slice(0, 10)}`);
        } else {
          // First run — fetch full 12 months
          startDate = new Date(now.getFullYear() - 1, now.getMonth(), now.getDate());
          console.log(`[pipeline] First run: no historical data, fetching last 12 months`);
        }
      } catch {
        // SQLite not available — full 12 month fallback
        startDate = new Date(now.getFullYear() - 1, now.getMonth(), now.getDate());
        console.log(`[pipeline] SQLite not available, defaulting to last 12 months`);
      }

      const startStr = `${startDate.getMonth() + 1}/${startDate.getDate()}/${startDate.getFullYear()}`;
      dateRange = `${startStr} - ${endStr}`;
      console.log(`[pipeline] Date range: ${dateRange}`);
    }

    files = await client.downloadAllReports(dateRange);
    updateProgress(job, "All CSVs downloaded", 45);
  } finally {
    await client.cleanup();
  }

  // Step 3: Parse CSVs
  updateProgress(job, "Parsing CSV data", 50);

  const newCustomersResult = parseCSV<NewCustomer>(files.newCustomers, NewCustomerSchema);
  const ordersResult = parseCSV<Order>(files.orders, OrderSchema);

  // Diagnostic: log first order to verify column mapping
  if (ordersResult.data.length > 0) {
    console.log(`[pipeline] First order sample:`, JSON.stringify(ordersResult.data[0]));
  } else {
    console.log(`[pipeline] WARNING: 0 orders parsed from CSV`);
  }
  if (ordersResult.warnings.length > 0) {
    console.log(`[pipeline] Order parse warnings (first 5):`, ordersResult.warnings.slice(0, 5));
  }

  const firstVisitsResult = parseCSV<FirstVisit>(files.firstVisits, FirstVisitSchema);
  const registrationsResult = parseCSV<Registration>(files.allRegistrations, RegistrationSchema);
  const canceledResult = parseCSV<AutoRenew>(files.canceledAutoRenews, AutoRenewSchema);
  const activeResult = parseCSV<AutoRenew>(files.activeAutoRenews, AutoRenewSchema);
  const pausedResult = parseCSV<AutoRenew>(files.pausedAutoRenews, AutoRenewSchema);
  const newAutoRenewsResult = parseCSV<AutoRenew>(files.newAutoRenews, AutoRenewSchema);
  const revenueCatResult = parseCSV<RevenueCategory>(files.revenueCategories, RevenueCategorySchema);

  // Merge active + paused for complete current subscriber picture
  const allCurrentSubs = [...activeResult.data, ...pausedResult.data];

  allWarnings.push(
    ...newCustomersResult.warnings,
    ...ordersResult.warnings,
    ...firstVisitsResult.warnings,
    ...registrationsResult.warnings,
    ...canceledResult.warnings,
    ...activeResult.warnings,
    ...pausedResult.warnings,
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
    newAutoRenews: newAutoRenewsResult.data.length,
    revenueCategories: revenueCatResult.data.length,
  };

  updateProgress(job, "CSV parsing complete", 55);

  // Step 4: Run analytics
  updateProgress(job, "Running funnel analysis", 58);
  const funnelResults = analyzeFunnel(
    firstVisitsResult.data,
    newAutoRenewsResult.data,
    newCustomersResult.data
  );

  updateProgress(job, "Running funnel overview", 62);
  const funnelOverview = analyzeFunnelOverview(
    newCustomersResult.data,
    firstVisitsResult.data,
    newAutoRenewsResult.data
  );

  updateProgress(job, "Running churn analysis", 65);
  const churnResults = analyzeChurn(
    canceledResult.data,
    allCurrentSubs,
    newAutoRenewsResult.data
  );

  updateProgress(job, "Running volume analysis", 70);
  const volumeResults = analyzeVolume(
    newAutoRenewsResult.data,
    canceledResult.data,
    newCustomersResult.data
  );

  updateProgress(job, "Computing summary KPIs", 70);
  const summary = computeSummary(allCurrentSubs, ordersResult.data);

  // Diagnostic: log category breakdown and dump debug data
  console.log(`[pipeline] computeSummary input: ${allCurrentSubs.length} total subs`);
  console.log(`[pipeline] Category breakdown: MEMBER=${summary.activeMembers}, SKY3=${summary.activeSky3}, TV=${summary.activeSkyTingTv}, UNKNOWN=${summary.activeUnknown}, TOTAL=${summary.activeTotal}`);
  if (summary.activeUnknown > 0) {
    console.log(`[pipeline] UNKNOWN plan names:`, JSON.stringify(summary.unknownPlanNames));
  }
  if (Object.keys(summary.skippedStates).length > 0) {
    console.log(`[pipeline] Skipped states:`, JSON.stringify(summary.skippedStates));
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
    console.log(`[pipeline] Wrote debug plan names to ${debugPath}`);
  } catch { /* ignore */ }

  // Diagnostic: log order type breakdown for revenue categorization
  const orderTypeRevenue: Record<string, number> = {};
  for (const order of ordersResult.data) {
    const t = order.type || "(blank)";
    orderTypeRevenue[t] = (orderTypeRevenue[t] || 0) + order.total;
  }
  console.log(`[pipeline] Order type revenue breakdown (${ordersResult.data.length} orders):`);
  const sortedTypes = Object.entries(orderTypeRevenue).sort((a, b) => b[1] - a[1]);
  for (const [type, revenue] of sortedTypes) {
    console.log(`  "${type}": $${revenue.toFixed(2)}`);
  }
  console.log(`[pipeline] Current month revenue: $${summary.currentMonthRevenue}, Previous month: $${summary.previousMonthRevenue}`);

  updateProgress(job, "Running trends analysis", 73);
  const trendsResults = analyzeTrends(
    newAutoRenewsResult.data,
    canceledResult.data,
    allCurrentSubs,
    summary,
    ordersResult.data,
    firstVisitsResult.data
  );

  // Step 4b: Save revenue categories to SQLite
  if (revenueCatResult.data.length > 0) {
    updateProgress(job, "Saving revenue categories to database", 74);
    // Parse date range to get period start/end
    const drParts = (dateRange || "").split(" - ").map((s) => s.trim());
    const periodStart = drParts[0] || new Date().toISOString().slice(0, 10);
    const periodEnd = drParts[1] || new Date().toISOString().slice(0, 10);
    try {
      saveRevenueCategories(periodStart, periodEnd, revenueCatResult.data);
      console.log(`[pipeline] Saved ${revenueCatResult.data.length} revenue categories to SQLite`);

      // Auto-lock completed months (any period that ends before the 1st of current month)
      const currentMonthStart = new Date(new Date().getFullYear(), new Date().getMonth(), 1);
      const periodEndDate = new Date(periodEnd);
      if (!isNaN(periodEndDate.getTime()) && periodEndDate < currentMonthStart) {
        lockPeriod(periodStart, periodEnd);
        console.log(`[pipeline] Auto-locked completed period ${periodStart} – ${periodEnd}`);
      }
    } catch (dbErr) {
      console.warn(`[pipeline] Failed to save revenue categories to SQLite: ${dbErr instanceof Error ? dbErr.message : dbErr}`);
      allWarnings.push(`SQLite save failed: ${dbErr instanceof Error ? dbErr.message : "unknown error"}`);
    }
  }

  // Step 4c: Analyze revenue categories
  const revenueCatAnalysis = revenueCatResult.data.length > 0
    ? analyzeRevenueCategories(revenueCatResult.data)
    : null;

  // Step 5: Export to Google Sheets
  updateProgress(job, "Connecting to Google Sheets", 75);
  const analyticsDoc = await getSpreadsheet(analyticsSheetId);

  const dateRangeStr = job.data.dateRangeStart && job.data.dateRangeEnd
    ? `${job.data.dateRangeStart} - ${job.data.dateRangeEnd}`
    : "All time";

  updateProgress(job, "Writing analytics tabs", 78);
  await Promise.all([
    writeDashboardTab(analyticsDoc, summary, dateRangeStr, recordCounts),
    writeFunnelOverviewTab(analyticsDoc, funnelOverview),
    writeFunnelTab(analyticsDoc, funnelResults),
    writeWeeklyVolumeTab(analyticsDoc, volumeResults),
    writeChurnTab(analyticsDoc, churnResults),
    writeTrendsTab(analyticsDoc, trendsResults),
    ...(revenueCatAnalysis ? [writeRevenueCategoriesTab(analyticsDoc, revenueCatAnalysis, dateRangeStr)] : []),
  ]);

  updateProgress(job, "Writing Run Log", 93);
  await writeRunLogEntry(analyticsDoc, {
    timestamp: new Date().toISOString(),
    duration: Date.now() - startTime,
    recordCounts,
    warnings: allWarnings,
  });

  // Step 6: Write raw data (if configured)
  let rawDataSheetUrl = "";
  if (rawDataSheetId) {
    updateProgress(job, "Writing raw data sheet", 95);
    const rawDoc = await getSpreadsheet(rawDataSheetId);

    await writeRawDataSheets(rawDoc, {
      newCustomers: newCustomersResult.data as unknown as Record<string, string | number>[],
      orders: ordersResult.data as unknown as Record<string, string | number>[],
      registrations: registrationsResult.data as unknown as Record<string, string | number>[],
      autoRenews: allCurrentSubs as unknown as Record<string, string | number>[],
    });

    rawDataSheetUrl = getSheetUrl(rawDataSheetId);
  }

  // Save debug copy of orders CSV before cleanup
  try {
    const debugPath = join(process.cwd(), "data", "debug-orders-sample.csv");
    copyFileSync(files.orders, debugPath);
    console.log(`[pipeline] Saved debug orders CSV to ${debugPath}`);
  } catch { /* ignore */ }

  // Log revenue categories summary
  if (revenueCatResult.data.length > 0) {
    console.log(`[pipeline] Revenue Categories (${revenueCatResult.data.length} categories):`);
    const sorted = [...revenueCatResult.data].sort((a, b) => b.netRevenue - a.netRevenue);
    for (const cat of sorted.slice(0, 10)) {
      console.log(`  "${cat.revenueCategory}": revenue=$${cat.revenue.toFixed(2)}, net=$${cat.netRevenue.toFixed(2)}`);
    }
  }

  // Save pipeline run to SQLite
  try {
    const drParts = (dateRange || "").split(" - ").map((s) => s.trim());
    savePipelineRun(drParts[0] || "", drParts[1] || "", recordCounts, Date.now() - startTime);
  } catch { /* non-critical */ }

  // Cleanup temp files
  cleanupDownloads();

  // Clear dashboard cache so next read fetches fresh data from sheets
  clearDashboardCache();

  updateProgress(job, "Pipeline complete!", 100);

  return {
    success: true,
    sheetUrl: getSheetUrl(analyticsSheetId),
    rawDataSheetUrl,
    duration: Date.now() - startTime,
    recordCounts,
    warnings: allWarnings,
  };
}

// Store on globalThis so the singleton survives Next.js HMR reloads.
// Without this, each hot reload resets `let worker = null` in the new module
// scope while the old Worker stays alive in Redis, orphaning the queue.
const g = globalThis as unknown as { __pipelineWorker?: Worker };

export function startPipelineWorker(): Worker {
  if (g.__pipelineWorker) return g.__pipelineWorker;

  const w = new Worker(
    "pipeline",
    async (job) => runPipeline(job),
    {
      connection: getRedisConnection(),
      concurrency: 1,
      lockDuration: 1_500_000, // 25 minutes — pipeline timeout is 20 min + margin
    }
  );

  w.on("completed", (job) => {
    console.log(`[worker] Pipeline job ${job.id} completed`);
  });

  w.on("failed", (job, err) => {
    console.error(`[worker] Pipeline job ${job?.id} failed:`, err.message);
  });

  w.on("error", (err) => {
    console.error("[worker] Worker error:", err.message);
  });

  w.on("stalled", (jobId) => {
    console.warn(`[worker] Job ${jobId} stalled — lock may have expired`);
  });

  g.__pipelineWorker = w;
  console.log("[worker] Pipeline worker started and listening for jobs");
  return w;
}
