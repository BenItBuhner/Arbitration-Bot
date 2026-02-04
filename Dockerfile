FROM oven/bun:1.1.10

WORKDIR /app

# Install dependencies (frozen lockfile ensures reproducible builds)
COPY package.json bun.lock* ./
RUN bun install --frozen-lockfile

# Copy application code
COPY . .

# Create logs directory with write permissions for runtime logging
RUN mkdir -p logs && chmod 777 logs

# Configure entrypoint
# CLI arguments are passed directly: docker run image --fake-trade --auto
ENTRYPOINT ["bun", "run", "main.ts"]

# Default: run cross-platform analysis in headless summary mode
# Override with: docker run image --help (or any CLI args)
CMD ["--mode", "cross-platform-analysis", "--coins", "eth", "--headless-summary"]
