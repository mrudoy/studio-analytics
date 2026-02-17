import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  serverExternalPackages: [
    "playwright",
    "playwright-extra",
    "puppeteer-extra-plugin-stealth",
    "puppeteer-extra-plugin",
    "clone-deep",
    "merge-deep",
    "bullmq",
    "ioredis",
    "pg",
  ],
};

export default nextConfig;
