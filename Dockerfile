# ── Stage 1: Build ────────────────────────────────────────
# Use a slim Node image for the build — much faster than Playwright image
FROM node:20-slim AS builder

WORKDIR /app

# Install dependencies (cached unless package.json/lock changes)
COPY package.json package-lock.json* ./
RUN npm ci

# Copy source and build
COPY . .
ENV NODE_ENV=production
RUN npm run build

# Prune to production-only deps
RUN npm prune --omit=dev

# ── Stage 2: Runtime ──────────────────────────────────────
# Playwright image only needed at runtime for the scraper
FROM mcr.microsoft.com/playwright:v1.58.2-noble

WORKDIR /app

# Install Xvfb for virtual display (headed Chrome needs a display)
RUN apt-get update && apt-get install -y xvfb && rm -rf /var/lib/apt/lists/*

# Copy only what we need from the builder
COPY --from=builder /app/node_modules ./node_modules
COPY --from=builder /app/.next ./.next
COPY --from=builder /app/package.json ./package.json
COPY --from=builder /app/next.config.ts ./next.config.ts
COPY --from=builder /app/tsconfig.json ./tsconfig.json
COPY --from=builder /app/src ./src

# Create data directory (will be mounted as a volume in production)
RUN mkdir -p data/downloads

EXPOSE 3000

# Startup script: starts Xvfb, then Next.js
COPY start.sh ./
RUN chmod +x start.sh
CMD ["./start.sh"]
