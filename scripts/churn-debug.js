const https = require("https");

const url = "https://studio-analytics-production.up.railway.app/api/stats";

https.get(url, (res) => {
  let data = "";
  res.on("data", (d) => (data += d));
  res.on("end", () => {
    try {
      const j = JSON.parse(data);
      const cr = j.trends && j.trends.churnRates;
      if (!cr) {
        console.log("No churnRates found");
        return;
      }

      // Check byCategory structure
      const bc = cr.byCategory;
      if (bc) {
        console.log("byCategory keys:", Object.keys(bc));
        const mem = bc.member || bc.MEMBER;
        if (mem) {
          console.log("member keys:", Object.keys(mem));
          const monthly = mem.monthly || [];
          console.log("\nMEMBER Monthly Churn - " + monthly.length + " months:");
          console.log("");
          console.log("Month      | Mo.Active | Mo.Cancel | Elig.Churn% | AllActive | AllCancel | UserChurn%");
          console.log("-----------|-----------|-----------|-------------|-----------|-----------|----------");
          for (const m of monthly) {
            const pad = (v, n) => String(v).padStart(n);
            console.log(
              m.month + "    | " +
              pad(m.monthlyActiveAtStart != null ? m.monthlyActiveAtStart : "-", 9) + " | " +
              pad(m.monthlyCanceledCount != null ? m.monthlyCanceledCount : "-", 9) + " | " +
              pad(m.eligibleChurnRate != null ? m.eligibleChurnRate + "%" : "-", 11) + " | " +
              pad(m.activeAtStart != null ? m.activeAtStart : "-", 9) + " | " +
              pad(m.canceledCount != null ? m.canceledCount : "-", 9) + " | " +
              pad(m.userChurnRate != null ? m.userChurnRate + "%" : "-", 9)
            );
          }
          console.log("");
          console.log("Avg Eligible (Monthly) Churn:", mem.avgEligibleChurnRate + "%");
          console.log("Avg User Churn:", mem.avgUserChurnRate + "%");
          console.log("Avg MRR Churn:", mem.avgMrrChurnRate + "%");
        }
      }
    } catch (e) {
      console.log("Parse error:", e.message);
      console.log("Raw:", data.slice(0, 2000));
    }
  });
}).on("error", (e) => {
  console.error("Request error:", e.message);
});
