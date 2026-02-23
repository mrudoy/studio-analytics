import { NextResponse } from "next/server";
import { saveUploadedData, saveRevenueCategories } from "@/lib/db/revenue-store";
import { saveAutoRenews, type AutoRenewRow } from "@/lib/db/auto-renew-store";
import { saveFirstVisits, saveRegistrations, type RegistrationRow } from "@/lib/db/registration-store";
import { saveFullCustomers, type FullCustomerRow } from "@/lib/db/customer-store";
import { parseCSV } from "@/lib/parser/csv-parser";
import { RevenueCategorySchema, AutoRenewSchema, FullRegistrationSchema, CustomerExportSchema } from "@/lib/parser/schemas";
import { z } from "zod";
import { writeFileSync } from "fs";
import { join } from "path";
import { mkdirSync, existsSync } from "fs";
import type { RevenueCategory, AutoRenew } from "@/types/union-data";

const UPLOAD_DIR = join(process.cwd(), "data", "uploads");

/**
 * Auto-renew data types that can be uploaded.
 * Multiple files can be uploaded for different states (active, canceled, etc.)
 * and they'll all be merged into the auto_renews table.
 */
const AUTO_RENEW_TYPES = [
  "auto_renews",
  "active_auto_renews",
  "canceled_auto_renews",
  "paused_auto_renews",
  "trialing_auto_renews",
  "new_auto_renews",
];

export async function POST(request: Request) {
  try {
    const formData = await request.formData();
    const file = formData.get("file") as File | null;
    const dataType = formData.get("type") as string | null;
    const periodStart = formData.get("periodStart") as string | null;
    const periodEnd = formData.get("periodEnd") as string | null;

    if (!file) {
      return NextResponse.json({ error: "No file provided" }, { status: 400 });
    }

    if (!dataType) {
      return NextResponse.json({ error: "No data type specified" }, { status: 400 });
    }

    const content = await file.text();
    const filename = file.name || "upload.csv";

    // Ensure upload directory exists
    if (!existsSync(UPLOAD_DIR)) {
      mkdirSync(UPLOAD_DIR, { recursive: true });
    }

    // Save raw file
    const savedPath = join(UPLOAD_DIR, `${Date.now()}-${filename}`);
    writeFileSync(savedPath, content, "utf8");

    // Save to uploaded_data table
    const period = periodStart && periodEnd ? `${periodStart} - ${periodEnd}` : null;
    const uploadId = await saveUploadedData(filename, dataType, period, content);

    let parsedCount = 0;
    const warnings: string[] = [];

    // ── Revenue categories upload ────────────────────────────
    if (dataType === "revenue_categories" && periodStart && periodEnd) {
      const result = parseCSV<RevenueCategory>(savedPath, RevenueCategorySchema);
      if (result.data.length > 0) {
        await saveRevenueCategories(periodStart, periodEnd, result.data);
        parsedCount = result.data.length;
      }
      warnings.push(...result.warnings);
    }

    // ── Auto-renew CSV upload ────────────────────────────────
    if (AUTO_RENEW_TYPES.includes(dataType)) {
      const result = parseCSV<AutoRenew>(savedPath, AutoRenewSchema);
      if (result.data.length > 0) {
        const snapshotId = `upload-${Date.now()}`;
        const arRows: AutoRenewRow[] = result.data.map((ar) => ({
          planName: ar.name,
          planState: ar.state,
          planPrice: ar.price,
          customerName: ar.customer,
          customerEmail: ar.email || "",
          createdAt: ar.created || "",
          orderId: ar.orderId || undefined,
          salesChannel: ar.salesChannel || undefined,
          canceledAt: ar.canceledAt || undefined,
          canceledBy: ar.canceledBy || undefined,
          currentState: ar.currentState || undefined,
          currentPlan: ar.currentPlan || undefined,
        }));
        await saveAutoRenews(snapshotId, arRows);
        parsedCount = arRows.length;
        console.log(`[api/upload] Saved ${arRows.length} auto-renews from ${filename} (snapshot: ${snapshotId})`);
      }
      warnings.push(...result.warnings);
    }

    // ── First visits CSV upload ──────────────────────────────
    if (dataType === "first_visits" || dataType === "registrations") {
      const result = parseCSV<z.infer<typeof FullRegistrationSchema>>(savedPath, FullRegistrationSchema);
      if (result.data.length > 0) {
        const rows: RegistrationRow[] = result.data
          .filter((r) => r.email && r.attendedAt) // skip rows without email or attended date
          .map((r) => ({
            eventName: r.eventName,
            eventId: r.eventId || undefined,
            performanceId: r.performanceId || undefined,
            performanceStartsAt: r.performanceStartsAt || r.attendedAt,
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
        if (dataType === "first_visits") {
          await saveFirstVisits(rows);
        } else {
          await saveRegistrations(rows);
        }
        parsedCount = rows.length;
        console.log(`[api/upload] Saved ${rows.length} ${dataType} from ${filename}`);
      }
      warnings.push(...result.warnings);
    }

    // ── Customer export CSV upload ─────────────────────────────
    if (dataType === "customer_export") {
      const result = parseCSV<z.infer<typeof CustomerExportSchema>>(savedPath, CustomerExportSchema);
      if (result.data.length > 0) {
        const rows: FullCustomerRow[] = result.data
          .filter((r) => r.email)
          .map((r) => ({
            unionId: r.id,
            firstName: r.firstName,
            lastName: r.lastName,
            email: r.email,
            phone: r.phone || undefined,
            role: r.role || undefined,
            totalSpent: r.totalSpent,
            ltv: r.ltv,
            orderCount: r.orders,
            currentFreePass: r.currentFreeNonAutoRenewPass,
            currentFreeAutoRenew: r.currentFreeAutoRenewPass,
            currentPaidPass: r.currentPaidNonAutoRenewPass,
            currentPaidAutoRenew: r.currentPaidAutoRenewPass,
            currentPaymentPlan: r.currentPaymentPlan,
            livestreamRegistrations: r.livestreamRegistrations,
            inpersonRegistrations: r.inpersonRegistrations,
            replayRegistrations: r.replayRegistrations,
            livestreamRedeemed: r.livestreamRegistrationsRedeemed,
            inpersonRedeemed: r.inpersonRegistrationsRedeemed,
            replayRedeemed: r.replayRegistrationsRedeemed,
            twitter: r.twitter || undefined,
            instagram: r.instagram || undefined,
            facebook: r.facebook || undefined,
            notes: r.notes || undefined,
            birthday: r.birthday || undefined,
            howHeard: r.howDidYouHearAboutUs || undefined,
            goals: r.whatAreYourGoalsForJoiningSkyTing || undefined,
            neighborhood: r.whatNeighborhoodDoYouLiveIn || undefined,
            inspiration: r.whatInspiredYouToJoinSkyTing || undefined,
            practiceFrequency: r.howManyTimesPerWeekDoYouWantToPractice || undefined,
            createdAt: r.created,
          }));
        await saveFullCustomers(rows);
        parsedCount = rows.length;
        console.log(`[api/upload] Saved ${rows.length} customers from ${filename}`);
      }
      warnings.push(...result.warnings);
    }

    return NextResponse.json({
      success: true,
      uploadId,
      filename,
      dataType,
      parsedCount,
      warnings: warnings.slice(0, 10),
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Upload failed";
    console.error("[api/upload] Error:", message);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
