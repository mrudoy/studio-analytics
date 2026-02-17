import { NextResponse } from "next/server";
import { loadSettings, saveSettings, type ScheduleConfig } from "@/lib/crypto/credentials";
import { syncSchedule, getScheduleStatus } from "@/lib/queue/scheduler";

export async function GET() {
  try {
    const status = await getScheduleStatus();
    return NextResponse.json(status);
  } catch (err) {
    const message = err instanceof Error ? err.message : "Failed to get schedule status";
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

export async function PUT(request: Request) {
  try {
    const body = await request.json();

    // Validate cron pattern (basic check)
    if (body.cronPattern && typeof body.cronPattern !== "string") {
      return NextResponse.json({ error: "Invalid cron pattern" }, { status: 400 });
    }

    const existing = loadSettings() || {};
    const schedule: ScheduleConfig = {
      enabled: !!body.enabled,
      cronPattern: body.cronPattern || "0 10,16 * * *",
      timezone: body.timezone || "America/New_York",
    };

    const updated = { ...existing, schedule };
    saveSettings(updated);

    // Apply the schedule immediately
    await syncSchedule();

    const status = await getScheduleStatus();
    return NextResponse.json({ success: true, ...status });
  } catch (err) {
    const message = err instanceof Error ? err.message : "Failed to update schedule";
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
