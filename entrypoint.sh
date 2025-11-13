#!/bin/bash
set -e

# Arc Core Entrypoint Script
# Handles different service modes: api, scheduler

# Auto-detect CPU cores for optimal worker count
if [ -z "$WORKERS" ]; then
    # Try to get CPU count (works on Linux and macOS)
    if command -v nproc > /dev/null 2>&1; then
        WORKERS=$(nproc)
    elif [ -f /proc/cpuinfo ]; then
        WORKERS=$(grep -c processor /proc/cpuinfo)
    elif command -v sysctl > /dev/null 2>&1; then
        WORKERS=$(sysctl -n hw.ncpu 2>/dev/null || echo 4)
    else
        WORKERS=4
    fi
    echo "Auto-detected $WORKERS CPU cores"
fi

# Default values
HOST=${HOST:-0.0.0.0}
PORT=${PORT:-8000}
TIMEOUT=${TIMEOUT:-300}
LOG_LEVEL=${LOG_LEVEL:-info}

# Ensure data directories exist
mkdir -p /app/data/arc

# Wait for dependencies (optional)
if [ ! -z "$WAIT_FOR_DB" ]; then
    echo "Waiting for database at $WAIT_FOR_DB..."
    timeout 60 bash -c "until curl -s $WAIT_FOR_DB > /dev/null 2>&1; do sleep 2; done"
    echo "Database is ready!"
fi

if [ ! -z "$WAIT_FOR_MINIO" ]; then
    echo "Waiting for MinIO at $WAIT_FOR_MINIO..."
    timeout 60 bash -c "until curl -s $WAIT_FOR_MINIO/minio/health/live > /dev/null 2>&1; do sleep 2; done"
    echo "MinIO is ready!"
fi

# Run the specified service
case "$1" in
    api)
        echo "Starting Arc API server..."
        echo "Workers: $WORKERS, Host: $HOST, Port: $PORT"
        exec gunicorn \
            -w $WORKERS \
            -b $HOST:$PORT \
            -k uvicorn.workers.UvicornWorker \
            --timeout $TIMEOUT \
            --log-level $LOG_LEVEL \
            --access-logfile - \
            --error-logfile - \
            api.main:app
        ;;

    scheduler)
        echo "Starting Arc export scheduler..."
        # Scheduler runs inside the API, but you can run it standalone if needed
        exec python3 -c "from api.scheduler import run_scheduler; run_scheduler()"
        ;;

    migrate)
        echo "Running database migrations..."
        # Add migration logic here if needed
        exec python3 -c "from api.database import init_db; init_db()"
        ;;

    shell)
        echo "Starting interactive shell..."
        exec /bin/bash
        ;;

    *)
        echo "Usage: $0 {api|scheduler|migrate|shell}"
        echo "  api            - Start the API server (default)"
        echo "  scheduler      - Start export scheduler"
        echo "  migrate        - Run database migrations"
        echo "  shell          - Interactive shell"
        exit 1
        ;;
esac
