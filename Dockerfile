# Arc Core - Production Docker Image
# Multi-stage build for minimal image size

FROM python:3.11-slim as builder

# Install build dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    make \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Create virtual environment
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Production stage
FROM python:3.11-slim

# Install runtime dependencies only
RUN apt-get update && apt-get install -y \
    libpq5 \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy virtual environment from builder
COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Create app user (non-root for security)
RUN useradd -m -u 1000 arc && \
    mkdir -p /app /data && \
    chown -R arc:arc /app /data

# Set working directory
WORKDIR /app

# Copy application code
COPY --chown=arc:arc api/ ./api/
COPY --chown=arc:arc ingest/ ./ingest/
COPY --chown=arc:arc storage/ ./storage/
COPY --chown=arc:arc exporter/ ./exporter/
COPY --chown=arc:arc utils/ ./utils/
COPY --chown=arc:arc config.py config_loader.py ./
COPY --chown=arc:arc entrypoint.sh ./

# Make entrypoint executable
RUN chmod +x entrypoint.sh

# Switch to non-root user
USER arc

# Expose API port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Default environment variables
ENV PYTHONUNBUFFERED=1 \
    STORAGE_BACKEND=minio \
    QUERY_CACHE_ENABLED=true \
    LOG_LEVEL=INFO \
    WORKERS=4

# Entrypoint
ENTRYPOINT ["./entrypoint.sh"]
CMD ["api"]
