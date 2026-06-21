# Arc - High-Performance Time-Series Database (Go)
# Multi-stage build for minimal image size

ARG VERSION=dev

# Build stage
FROM golang:1.26-bookworm AS builder

WORKDIR /build

# Copy go mod files first for better layer caching
COPY go.mod go.sum ./
RUN go mod download && go mod verify

# Copy source code
COPY . .

# Build. FIPS=1 selects the arc-fips variant: same source/commit/version,
# compiled with the fips build tag against the CMVP-certified Go Cryptographic
# Module (GOFIPS140=v1.0.0) and baking in GODEBUG=fips140=only. The binary is
# always named "arc" inside the image; the FIPS-ness is conveyed by the image
# TAG (e.g. :<version>-fips), per "FIPS is a build variant, not a version".
ARG VERSION
ARG FIPS=0
RUN if [ "$FIPS" = "1" ]; then \
        echo "Building FIPS variant (GOFIPS140=v1.0.0, -tags=duckdb_arrow,fips)"; \
        GOFIPS140=v1.0.0 CGO_ENABLED=1 go build -v \
            -tags=duckdb_arrow,fips \
            -ldflags="-s -w -X main.Version=${VERSION}" \
            -o arc ./cmd/arc; \
    else \
        CGO_ENABLED=1 go build -v \
            -tags=duckdb_arrow \
            -ldflags="-s -w -X main.Version=${VERSION}" \
            -o arc ./cmd/arc; \
    fi

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
