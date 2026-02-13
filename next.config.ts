import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  serverExternalPackages: ["playwright", "bullmq", "ioredis"],
};

export default nextConfig;
