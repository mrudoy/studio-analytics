import * as dotenv from "dotenv";
dotenv.config();

import { loadSettings } from "../src/lib/crypto/credentials";

const settings = loadSettings();
if (!settings) {
  console.log("No settings found");
} else {
  console.log("Union.fit email:", settings.credentials?.email || "(not set)");
  console.log("Union.fit password:", settings.credentials?.password ? "(set)" : "(not set)");
  console.log("Robot email:", settings.robotEmail?.address || "(not set)");
  console.log("Analytics sheet:", settings.analyticsSpreadsheetId || "(not set)");
  console.log("Raw data sheet:", settings.rawDataSpreadsheetId || "(not set)");
}
