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
} from "../parser/schemas";
import { analyzeFunnel } from "../analytics/funnel";
import { analyzeFunnelOverview } from "../analytics/funnel-overview";
import { analyzeChurn } from "../analytics/churn";
import { analyzeVolume } from "../analytics/volume";
import { computeSummary } from "../analytics/summary";
import { analyzeTrends } from "../analytics/trends";
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
} from "../sheets/templates";
import { cleanupDownloads } from "../scraper/download-manager";
import type { PipelineJobData, PipelineResult } from "@/types/pipeline";
import type { NewCustomer, Order, FirstVisit, Registration, AutoRenew } from "@/types/union-data";
import { writeFileSync } from "fs";
import { join } from "path";

function updateProgress(job: Job, step: string, percent: number) {
  job.updateProgress({ step, percent });
}

async function runPipeline(job: Job): Promise<PipelineResult> {
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
  try {
    await client.initialize();
    updateProgress(job, "Logging into Union.fit", 15);
    await client.login(settings.credentials.email, settings.credentials.password);

    updateProgress(job, "Downloading CSV reports", 20);
    const dateRange = job.data.dateRangeStart && job.data.dateRangeEnd
      ? `${job.data.dateRangeStart} - ${job.data.dateRangeEnd}`
      : undefined;

    files = await client.downloadAllReports(dateRange);
    updateProgress(job, "All CSVs downloaded", 45);
  } finally {
    await client.cleanup();
  }

  // Step 3: Parse CSVs
  updateProgress(job, "Parsing CSV data", 50);

  const newCustomersResult = parseCSV<NewCustomer>(files.newCustomers, NewCustomerSchema);
  const ordersResult = parseCSV<Order>(files.orders, OrderSchema);
  const firstVisitsResult = parseCSV<FirstVisit>(files.firstVisits, FirstVisitSchema);
  const registrationsResult = parseCSV<Registration>(files.allRegistrations, RegistrationSchema);
  const canceledResult = parseCSV<AutoRenew>(files.canceledAutoRenews, AutoRenewSchema);
  const activeResult = parseCSV<AutoRenew>(files.activeAutoRenews, AutoRenewSchema);
  const pausedResult = parseCSV<AutoRenew>(files.pausedAutoRenews, AutoRenewSchema);
  const newAutoRenewsResult = parseCSV<AutoRenew>(files.newAutoRenews, AutoRenewSchema);

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
    ...newAutoRenewsResult.warnings
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
    ordersResult.data
  );

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

  // Cleanup temp files
  cleanupDownloads();

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

let worker: Worker | null = null;

export function startPipelineWorker(): Worker {
  if (worker) return worker;

  worker = new Worker(
    "pipeline",
    async (job) => runPipeline(job),
    {
      connection: getRedisConnection(),
      concurrency: 1,
      lockDuration: 900000, // 15 minutes — pipeline takes ~8 min
    }
  );

  worker.on("completed", (job) => {
    console.log(`[worker] Pipeline job ${job.id} completed`);
  });

  worker.on("failed", (job, err) => {
    console.error(`[worker] Pipeline job ${job?.id} failed:`, err.message);
  });

  worker.on("error", (err) => {
    console.error("[worker] Worker error:", err.message);
  });

  worker.on("stalled", (jobId) => {
    console.warn(`[worker] Job ${jobId} stalled — lock may have expired`);
  });

  console.log("[worker] Pipeline worker started and listening for jobs");
  return worker;
}
