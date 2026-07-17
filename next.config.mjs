// Plain ESM config (not next.config.ts) so `next start` never needs the
// `typescript` package at RUNTIME to transpile it. Next 15.5.x transpiles a
// .ts config at boot; when `npm prune --omit=dev` stripped typescript from the
// runtime image, every route 502'd (see the 2026-07-17 outage). A .mjs config
// removes that runtime dependency entirely.

/** @type {import('next').NextConfig} */
const nextConfig = {
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
      const externalsMap = {};
      for (const mod of builtins) {
        externalsMap[mod] = `commonjs ${mod}`;
        externalsMap[`node:${mod}`] = `commonjs ${mod}`;
      }
      config.externals = config.externals || [];
      config.externals.push(externalsMap);
      // Ignore optional native modules that aren't installed
      config.resolve = config.resolve || {};
      config.resolve.alias = config.resolve.alias || {};
      config.resolve.alias["pg-native"] = false;
      config.resolve.alias["@react-email/render"] = false;
    }
    return config;
  },
};

export default nextConfig;
