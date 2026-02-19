import XLSX from "xlsx";
import { writeFileSync } from "fs";

const wb = XLSX.readFile("/Users/mike.rudoy_old/Library/Application Support/Claude/local-agent-mode-sessions/18f4f7bc-3a94-49c3-9763-a27eb3a07f66/f0382eda-9a91-43f5-ae4b-7e44174ac215/local_0ca08d37-fce1-4f6d-b8ad-e8373ba43043/outputs/Revenue_2026.xlsx");
const ws = wb.Sheets[wb.SheetNames[0]];
const data = XLSX.utils.sheet_to_json(ws) as Record<string, unknown>[];

const rows = data.filter(r => r["Revenue Category"] !== "TOTAL");
console.log("Revenue categories:", rows.length);

const totalRevenue = rows.reduce((s, r) => s + (Number(r["Revenue"]) || 0), 0);
const totalNet = rows.reduce((s, r) => s + (Number(r["Net Revenue"]) || 0), 0);
console.log("Total gross revenue:", totalRevenue.toFixed(2));
console.log("Total net revenue:", totalNet.toFixed(2));

console.log("\nFirst 3 rows:");
for (const r of rows.slice(0, 3)) {
  console.log(" ", r["Revenue Category"], "- revenue:", r["Revenue"], "net:", r["Net Revenue"]);
}

// Write clean CSV
const header = "Revenue Category,Revenue,Union Fees,Stripe Fees,Other Fees,Refunded,Refunded Union Fees,Net Revenue";
const csvRows = rows.map(r => [
  '"' + (r["Revenue Category"] || "") + '"',
  r["Revenue"] || 0,
  r["Union Fees"] || 0,
  r["Stripe Fees"] || 0,
  r["Other Fees"] || 0,
  r["Refunded"] || 0,
  r["Refunded Union Fees"] || 0,
  r["Net Revenue"] || 0,
].join(","));
const csv = header + "\n" + csvRows.join("\n");
writeFileSync("/tmp/revenue-2026-corrected.csv", csv);
console.log("\nSaved to /tmp/revenue-2026-corrected.csv");
