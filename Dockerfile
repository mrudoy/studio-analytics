FROM mcr.microsoft.com/playwright:v1.50.0-noble

WORKDIR /app

# Install Xvfb for virtual display (headed Chrome needs a display)
RUN apt-get update && apt-get install -y xvfb && rm -rf /var/lib/apt/lists/*

# Install ALL dependencies (devDeps needed for build)
COPY package.json package-lock.json* ./
RUN npm ci

# Copy source
COPY . .

# Build Next.js (needs TypeScript, Tailwind, etc. from devDeps)
ENV NODE_ENV=production
RUN npm run build

# Prune dev dependencies after build
RUN npm prune --omit=dev

# Create data directory (will be mounted as a volume in production)
RUN mkdir -p data/downloads

EXPOSE 3000

# Startup script: starts Xvfb, then Next.js with proper logging
COPY start.sh ./
RUN chmod +x start.sh
CMD ["./start.sh"]
