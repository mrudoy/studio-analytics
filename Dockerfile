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

# Use xvfb-run to provide a virtual display for headed Chrome
# Shell form so $PORT is expanded at runtime (Railway sets PORT dynamically)
CMD xvfb-run --auto-servernum --server-args="-screen 0 1280x720x24" npx next start -H 0.0.0.0 -p ${PORT:-3000}
