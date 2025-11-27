# Arc - High-Performance Time-Series Database (Go)
# Multi-stage build for minimal image size

ARG VERSION=dev

# Build stage
FROM golang:1.25-bookworm AS builder

WORKDIR /build

# Install build dependencies for DuckDB with Arrow support
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    make \
    && rm -rf /var/lib/apt/lists/*

# Copy go mod files first for better layer caching
COPY go.mod go.sum ./
RUN go mod download && go mod verify

# Copy source code
COPY . .

# Build with Arrow support
ARG VERSION
RUN CGO_ENABLED=1 go build -v -tags=duckdb_arrow \
    -ldflags="-s -w -X main.Version=${VERSION}" \
    -o arc ./cmd/arc

# Production stage
FROM debian:bookworm-slim

ARG VERSION

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd -m -u 1000 arc && \
    mkdir -p /app/data && \
    chown -R arc:arc /app

WORKDIR /app

# Copy binary from builder
COPY --from=builder --chown=arc:arc /build/arc .

# Copy default config
COPY --chown=arc:arc arc.toml .

# Create VERSION file
RUN echo "${VERSION}" > VERSION && chown arc:arc VERSION

# Switch to non-root user
USER arc

# Data volume
VOLUME ["/app/data"]

# Expose API port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Run Arc
ENTRYPOINT ["./arc"]
