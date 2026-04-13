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
    "adm-zip",
    "googleapis",
    "google-auth-library",
    "googleapis-common",
    "gaxios",
    "gtoken",
    "jwa",
    "jws",
    "agent-base",
    "https-proxy-agent",
    "gcp-metadata",
    "pg-connection-string",
    "pgpass",
    "split2",
  ],
  webpack: (config, { isServer }) => {
    if (isServer) {
      // Mark Node.js builtins as external (bare & node: prefix)
      const builtins = [
        "fs", "fs/promises", "path", "crypto", "zlib", "stream",
        "http", "https", "net", "os", "string_decoder", "child_process",
        "tls", "dns", "http2", "querystring", "url", "util", "events",
        "buffer", "assert", "process", "stream/web", "perf_hooks",
        "worker_threads", "v8", "async_hooks", "diagnostics_channel",
      ];
      const externalsMap: Record<string, string> = {};
      for (const mod of builtins) {
        externalsMap[mod] = `commonjs ${mod}`;
        externalsMap[`node:${mod}`] = `commonjs ${mod}`;
      }
      config.externals = config.externals || [];
      config.externals.push(externalsMap);
      // Ignore optional native modules that aren't installed
      config.resolve = config.resolve || {};
      config.resolve.alias = config.resolve.alias || {};
      (config.resolve.alias as Record<string, boolean>)["pg-native"] = false;
      (config.resolve.alias as Record<string, boolean>)["@react-email/render"] = false;
    }
    return config;
  },
};

export default nextConfig;
