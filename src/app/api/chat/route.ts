import { createAnthropic } from "@ai-sdk/anthropic";
import { streamText } from "ai";

const anthropic = createAnthropic({
  apiKey: process.env.ANTHROPIC_API_KEY,
});

const SYSTEM_PROMPT = `You are an analytics assistant for Sky Ting, a yoga studio business in New York City. You have access to the studio's full dashboard data which will be provided to you as context.

Sky Ting has these subscription tiers:
- **Member**: In-studio unlimited membership (~$199-249/month)
- **Sky3**: Premium tier with additional perks
- **Sky Ting TV**: Online/streaming membership (~$19-29/month)

Revenue comes from:
- Class subscriptions (auto-renewing memberships — the core business)
- Drop-in classes (single visits)
- Intro weeks (new customer trial passes)
- Workshops and retreats
- Merch (Shopify online store — apparel, accessories)
- Spa & Wellness (infrared sauna, contrast suite, treatment rooms, cupping)
- Studio rentals (renting space to teachers and external events)

When answering questions:
- Be concise and data-driven. Use specific numbers from the dashboard.
- Format currency as $X,XXX (no cents unless relevant).
- When showing trends, note the direction (up/down) and magnitude.
- If you don't have enough data to answer, say so clearly.
- Use markdown formatting: bold for emphasis, bullet points for lists, tables for comparisons.
- Round percentages to 1 decimal place.
- When referencing time periods, be specific (e.g. "January 2025" not "last month").
- If the user asks about something not in the data, suggest what data would be needed.
- Keep responses focused — don't dump all the data unless asked for a comprehensive overview.`;

async function fetchDashboardData(): Promise<string> {
  try {
    // Fetch from our own API to get the full dashboard data
    const baseUrl = process.env.NEXT_PUBLIC_BASE_URL
      || (process.env.RAILWAY_PUBLIC_DOMAIN ? `https://${process.env.RAILWAY_PUBLIC_DOMAIN}` : null)
      || (process.env.VERCEL_URL ? `https://${process.env.VERCEL_URL}` : null)
      || "http://localhost:3000";

    const res = await fetch(`${baseUrl}/api/stats`, {
      cache: "no-store",
      headers: { "Content-Type": "application/json" },
    });

    if (!res.ok) {
      return "Dashboard data unavailable. The API returned an error.";
    }

    const data = await res.json();

    // Build a structured summary for Claude
    const sections: string[] = [];

    // 1. Core subscription metrics
    if (data.mrr) {
      sections.push(`## Current MRR (Monthly Recurring Revenue)
- Member: $${data.mrr.member?.toLocaleString() ?? "N/A"}
- Sky3: $${data.mrr.sky3?.toLocaleString() ?? "N/A"}
- Sky Ting TV: $${data.mrr.skyTingTv?.toLocaleString() ?? "N/A"}
- **Total MRR: $${data.mrr.total?.toLocaleString() ?? "N/A"}**`);
    }

    if (data.activeSubscribers) {
      sections.push(`## Active Subscribers
- Member: ${data.activeSubscribers.member}
- Sky3: ${data.activeSubscribers.sky3}
- Sky Ting TV: ${data.activeSubscribers.skyTingTv}
- **Total: ${data.activeSubscribers.total}**`);
    }

    if (data.arpu) {
      sections.push(`## ARPU (Avg Revenue Per User)
- Member: $${data.arpu.member}
- Sky3: $${data.arpu.sky3}
- Sky Ting TV: $${data.arpu.skyTingTv}
- Overall: $${data.arpu.overall}`);
    }

    // 2. Revenue overview
    if (data.currentMonthRevenue !== undefined) {
      sections.push(`## Current Period Revenue
- Current Month Revenue (net): $${Math.round(data.currentMonthRevenue).toLocaleString()}
- Previous Month Revenue (net): $${Math.round(data.previousMonthRevenue).toLocaleString()}`);
    }

    // 3. Monthly revenue timeline
    if (data.monthlyRevenue?.length > 0) {
      const last12 = data.monthlyRevenue.slice(-12);
      sections.push(`## Monthly Revenue Timeline (last 12 months)
${last12.map((m: { month: string; gross: number; net: number; retreatGross?: number; retreatNet?: number }) =>
  `- ${m.month}: Gross $${Math.round(m.gross).toLocaleString()}, Net $${Math.round(m.net).toLocaleString()}${m.retreatGross ? ` (includes $${Math.round(m.retreatGross).toLocaleString()} retreat revenue)` : ""}`
).join("\n")}`);
    }

    // 4. Annual breakdown
    if (data.annualBreakdown?.length > 0) {
      sections.push(`## Annual Revenue Breakdown
${data.annualBreakdown.map((y: { year: number; totalGross: number; totalNet: number; segments: Array<{ segment: string; gross: number; net: number }> }) =>
  `### ${y.year}: Total Gross $${Math.round(y.totalGross).toLocaleString()}, Net $${Math.round(y.totalNet).toLocaleString()}
${y.segments.map((s) => `  - ${s.segment}: Gross $${Math.round(s.gross).toLocaleString()}, Net $${Math.round(s.net).toLocaleString()}`).join("\n")}`
).join("\n\n")}`);
    }

    // 5. Month-over-month
    if (data.monthOverMonth) {
      const mom = data.monthOverMonth;
      sections.push(`## Month-over-Month YoY Comparison (${mom.monthName})
- ${mom.current?.year ?? "Current"}: Gross $${Math.round(mom.current?.gross ?? 0).toLocaleString()}, Net $${Math.round(mom.current?.net ?? 0).toLocaleString()}
- ${mom.priorYear?.year ?? "Prior"}: Gross $${Math.round(mom.priorYear?.gross ?? 0).toLocaleString()}, Net $${Math.round(mom.priorYear?.net ?? 0).toLocaleString()}
- YoY Change: ${mom.yoyGrossPct != null ? `${mom.yoyGrossPct > 0 ? "+" : ""}${mom.yoyGrossPct}%` : "N/A"}`);
    }

    // 6. Trends data
    if (data.trends) {
      const t = data.trends;

      if (t.pacing) {
        sections.push(`## Current Month Pacing (${t.pacing.month})
- Day ${t.pacing.daysElapsed} of ${t.pacing.daysInMonth}
- New Members: ${t.pacing.newMembersActual} actual vs ${t.pacing.newMembersPaced} paced
- New Sky3: ${t.pacing.newSky3Actual} actual vs ${t.pacing.newSky3Paced} paced
- Revenue: $${Math.round(t.pacing.revenueActual).toLocaleString()} actual vs $${Math.round(t.pacing.revenuePaced).toLocaleString()} paced
- Member Cancellations: ${t.pacing.memberCancellationsActual} actual vs ${t.pacing.memberCancellationsPaced} paced`);
      }

      if (t.projection) {
        sections.push(`## ${t.projection.year} Revenue Projection
- Projected Annual Revenue: $${Math.round(t.projection.projectedAnnualRevenue).toLocaleString()}
- Current MRR: $${Math.round(t.projection.currentMRR).toLocaleString()}
- Monthly Growth Rate: ${(t.projection.monthlyGrowthRate * 100).toFixed(1)}%
- Prior Year Revenue: $${Math.round(t.projection.priorYearRevenue).toLocaleString()}`);
      }

      if (t.dropIns) {
        sections.push(`## Drop-In Activity
- Typical Week: ${t.dropIns.typicalWeekVisits} visits
- Trend: ${t.dropIns.trend} (${t.dropIns.trendDeltaPercent > 0 ? "+" : ""}${t.dropIns.trendDeltaPercent.toFixed(1)}%)
${t.dropIns.wtd ? `- Week-to-Date: ${t.dropIns.wtd.visits} visits (${t.dropIns.wtd.uniqueCustomers} unique)` : ""}`);
      }

      if (t.churnRates?.byCategory) {
        const cr = t.churnRates;
        sections.push(`## Churn Rates
- Member: ${cr.byCategory.member.avgUserChurnRate.toFixed(1)}% avg monthly churn (${cr.byCategory.member.atRiskCount} at-risk)
- Sky3: ${cr.byCategory.sky3.avgUserChurnRate.toFixed(1)}% avg monthly churn (${cr.byCategory.sky3.atRiskCount} at-risk)
- Sky Ting TV: ${cr.byCategory.skyTingTv.avgUserChurnRate.toFixed(1)}% avg monthly churn (${cr.byCategory.skyTingTv.atRiskCount} at-risk)
- Total At-Risk: ${cr.totalAtRisk}`);
      }

      // Weekly trends (last 4 weeks)
      if (t.weekly?.length > 0) {
        const last4 = t.weekly.slice(-4);
        sections.push(`## Weekly Growth Trends (last 4 weeks)
${last4.map((w: { period: string; newMembers: number; newSky3: number; memberChurn: number; sky3Churn: number; revenueAdded: number; revenueLost: number }) =>
  `- ${w.period}: +${w.newMembers} members, +${w.newSky3} Sky3, -${w.memberChurn} member churn, -${w.sky3Churn} Sky3 churn | Revenue +$${Math.round(w.revenueAdded).toLocaleString()} / -$${Math.round(w.revenueLost).toLocaleString()}`
).join("\n")}`);
      }

      // Conversion data
      if (t.conversionPool?.slices?.all) {
        const cp = t.conversionPool.slices.all;
        sections.push(`## Conversion Pool
- Average 7-day active pool: ${Math.round(cp.avgPool7d)} non-subscribers
- Average conversion rate: ${cp.avgRate.toFixed(2)}%
${cp.wtd ? `- WTD: ${cp.wtd.activePool7d} active, ${cp.wtd.converts} converts` : ""}`);
      }

      if (t.newCustomerCohorts?.cohorts?.length > 0) {
        sections.push(`## New Customer Conversion Cohorts
- Average 3-week conversion rate: ${t.newCustomerCohorts.avgConversionRate != null ? `${t.newCustomerCohorts.avgConversionRate.toFixed(1)}%` : "N/A"}
${t.newCustomerCohorts.cohorts.slice(-4).map((c: { cohortStart: string; newCustomers: number; total3Week: number }) =>
  `- ${c.cohortStart}: ${c.newCustomers} new → ${c.total3Week} converted (${c.newCustomers > 0 ? ((c.total3Week / c.newCustomers) * 100).toFixed(1) : 0}%)`
).join("\n")}`);
      }
    }

    // 7. Shopify Merch
    if (data.shopifyMerch) {
      const sm = data.shopifyMerch;
      sections.push(`## Merch (Shopify)
- MTD Revenue: $${Math.round(sm.mtdRevenue).toLocaleString()}
- Avg Monthly Revenue: $${Math.round(sm.avgMonthlyRevenue).toLocaleString()}
- Repeat Customer Rate: ${(sm.repeatCustomerRate * 100).toFixed(1)}%
- Total Customers: ${sm.totalCustomersWithOrders}

### Top Products:
${sm.topProducts.slice(0, 10).map((p: { title: string; revenue: number; unitsSold: number }, i: number) =>
  `${i + 1}. ${p.title}: $${Math.round(p.revenue).toLocaleString()} (${p.unitsSold} units)`
).join("\n")}

### Annual Revenue:
${sm.annualRevenue?.map((a: { year: number; gross: number; orderCount: number; avgOrderValue: number }) =>
  `- ${a.year}: $${Math.round(a.gross).toLocaleString()} (${a.orderCount} orders, $${Math.round(a.avgOrderValue)} AOV)`
).join("\n") ?? "N/A"}

### Category Breakdown:
${sm.categoryBreakdown?.map((c: { category: string; revenue: number; units: number; orders: number }) =>
  `- ${c.category}: $${Math.round(c.revenue).toLocaleString()} (${c.units} units, ${c.orders} orders)`
).join("\n") ?? "N/A"}`);
    }

    // 8. Spa
    if (data.spa) {
      const sp = data.spa;
      sections.push(`## Spa & Wellness
- MTD Revenue: $${Math.round(sp.mtdRevenue).toLocaleString()}
- Avg Monthly Revenue: $${Math.round(sp.avgMonthlyRevenue).toLocaleString()}
- Total Revenue (all time): $${Math.round(sp.totalRevenue).toLocaleString()}

### Service Breakdown:
${sp.serviceBreakdown.map((s: { category: string; totalRevenue: number }) =>
  `- ${s.category}: $${Math.round(s.totalRevenue).toLocaleString()}`
).join("\n")}

${sp.customerBehavior ? `### Customer Behavior:
- Unique Customers: ${sp.customerBehavior.uniqueCustomers}
- Also Take Classes: ${sp.customerBehavior.crossover.alsoTakeClasses} (${((sp.customerBehavior.crossover.alsoTakeClasses / sp.customerBehavior.crossover.total) * 100).toFixed(0)}%)
- Spa Only: ${sp.customerBehavior.crossover.spaOnly} (${((sp.customerBehavior.crossover.spaOnly / sp.customerBehavior.crossover.total) * 100).toFixed(0)}%)
- Are Subscribers: ${sp.customerBehavior.subscriberOverlap.areSubscribers} (${((sp.customerBehavior.subscriberOverlap.areSubscribers / sp.customerBehavior.subscriberOverlap.total) * 100).toFixed(0)}%)
- Visit Frequency: ${sp.customerBehavior.frequency.map((f: { bucket: string; customers: number }) => `${f.bucket}: ${f.customers}`).join(", ")}` : ""}`);
    }

    // 9. Rental Revenue
    if (data.rentalRevenue) {
      const rr = data.rentalRevenue;
      if (rr.annual?.length > 0) {
        sections.push(`## Studio Rentals
### Annual:
${rr.annual.map((a: { year: number; total: number; studioRental: number; teacherRentals: number }) =>
  `- ${a.year}: $${Math.round(a.total).toLocaleString()} (Studio: $${Math.round(a.studioRental).toLocaleString()}, Teacher: $${Math.round(a.teacherRentals).toLocaleString()})`
).join("\n")}`);
      }
    }

    // 10. Overview snapshots
    if (data.overviewData) {
      const ov = data.overviewData;
      const formatWindow = (w: { label: string; sublabel: string; subscriptions: { member: { new: number; churned: number }; sky3: { new: number; churned: number }; skyTingTv: { new: number; churned: number } }; activity: { dropIns: number; introWeeks: number }; revenue: { merch: number } }) =>
        `**${w.label}** (${w.sublabel}): +${w.subscriptions.member.new} members / -${w.subscriptions.member.churned} churned, +${w.subscriptions.sky3.new} Sky3 / -${w.subscriptions.sky3.churned} churned, ${w.activity.dropIns} drop-ins, ${w.activity.introWeeks} intro weeks, $${Math.round(w.revenue.merch)} merch`;

      sections.push(`## Overview Snapshots
${[ov.yesterday, ov.lastWeek, ov.thisMonth, ov.lastMonth].map(formatWindow).join("\n")}`);
    }

    // 11. Revenue Categories
    if (data.revenueCategories) {
      const rc = data.revenueCategories;
      sections.push(`## Revenue Categories (${rc.periodStart} to ${rc.periodEnd})
- Total Revenue: $${Math.round(rc.totalRevenue).toLocaleString()}
- Total Net Revenue: $${Math.round(rc.totalNetRevenue).toLocaleString()}
- Drop-In Revenue: $${Math.round(rc.dropInRevenue).toLocaleString()}
- Auto-Renew Revenue: $${Math.round(rc.autoRenewRevenue).toLocaleString()}
- Workshop Revenue: $${Math.round(rc.workshopRevenue).toLocaleString()}`);
    }

    return `# Sky Ting Studio Analytics Dashboard Data
Last updated: ${data.lastUpdated || "Unknown"}
Data range: ${data.dateRange || "Unknown"}

${sections.join("\n\n")}`;
  } catch (err) {
    console.error("[api/chat] Failed to fetch dashboard data:", err);
    return "Dashboard data unavailable due to an error fetching the stats API.";
  }
}

export async function POST(req: Request) {
  try {
    const { messages } = await req.json();

    if (!process.env.ANTHROPIC_API_KEY) {
      return new Response(
        JSON.stringify({ error: "ANTHROPIC_API_KEY not configured" }),
        { status: 500, headers: { "Content-Type": "application/json" } }
      );
    }

    // Fetch the latest dashboard data
    const dashboardContext = await fetchDashboardData();

    const result = streamText({
      model: anthropic("claude-sonnet-4-20250514"),
      // Falls back to claude-3-5-sonnet if the above is unavailable
      system: `${SYSTEM_PROMPT}\n\n---\n\nHere is the current dashboard data:\n\n${dashboardContext}`,
      messages,
      maxTokens: 2048,
    });

    return result.toDataStreamResponse();
  } catch (error) {
    console.error("[api/chat] Error:", error);
    return new Response(
      JSON.stringify({ error: "Failed to process chat request" }),
      { status: 500, headers: { "Content-Type": "application/json" } }
    );
  }
}
