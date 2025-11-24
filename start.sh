#!/bin/bash
set -e

# Arc Core - Quick Start Script
# Starts Arc Core locally with Docker or native Python

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MODE=${1:-docker}

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${GREEN}╔════════════════════════════════════════╗${NC}"
echo -e "${GREEN}║         Arc Core Quick Start           ║${NC}"
echo -e "${GREEN}╚════════════════════════════════════════╝${NC}"
echo ""

case "$MODE" in
    docker|rebuild)
        if [ "$MODE" = "rebuild" ]; then
            echo -e "${YELLOW}Rebuilding Arc Core with Docker (forced rebuild)...${NC}"
        else
            echo -e "${GREEN}Starting Arc Core with Docker...${NC}"
        fi
        echo ""

        # Check if Docker is installed
        if ! command -v docker &> /dev/null; then
            echo -e "${RED}Error: Docker is not installed${NC}"
            echo "Please install Docker from https://docs.docker.com/get-docker/"
            exit 1
        fi

        # Create .env if it doesn't exist
        if [ ! -f "$SCRIPT_DIR/.env" ]; then
            echo -e "${YELLOW}Creating .env from .env.example...${NC}"
            cp "$SCRIPT_DIR/.env.example" "$SCRIPT_DIR/.env"
        fi

        # Start services
        cd "$SCRIPT_DIR"

        if [ "$MODE" = "rebuild" ]; then
            echo -e "${YELLOW}Stopping existing containers...${NC}"
            docker-compose down
            echo -e "${YELLOW}Building images (no cache)...${NC}"
            docker-compose build --no-cache
            echo -e "${YELLOW}Starting containers...${NC}"
            docker-compose up -d
        else
            docker-compose up -d
        fi

        echo ""
        echo -e "${GREEN}Waiting for services to start...${NC}"
        sleep 10

        # Check health
        if curl -sf http://localhost:8000/health > /dev/null 2>&1; then
            echo -e "${GREEN}✓ Arc Core is running!${NC}"
            echo ""
            echo -e "API:           ${YELLOW}http://localhost:8000${NC}"
            echo -e "MinIO Console: ${YELLOW}http://localhost:9001${NC}"
            echo -e "  Username: minioadmin"
            echo -e "  Password: minioadmin"
            echo ""
            echo -e "View logs: ${YELLOW}docker-compose logs -f arc-api${NC}"
            echo -e "Stop:      ${YELLOW}docker-compose down${NC}"
        else
            echo -e "${RED}✗ Health check failed${NC}"
            echo "Check logs: docker-compose logs arc-api"
            exit 1
        fi
        ;;

    native)
        echo -e "${GREEN}Starting Arc Core natively...${NC}"
        echo ""

        # Check Python version (>= 3.9 required)
        PYTHON_BIN=""
        for py_cmd in python3.13 python3.12 python3.11 python3.10 python3.9 python3; do
            if command -v $py_cmd &> /dev/null; then
                # Get version
                PY_VERSION=$($py_cmd -c 'import sys; print(".".join(map(str, sys.version_info[:2])))')
                PY_MAJOR=$(echo $PY_VERSION | cut -d. -f1)
                PY_MINOR=$(echo $PY_VERSION | cut -d. -f2)

                # Check if >= 3.9
                if [ "$PY_MAJOR" -eq 3 ] && [ "$PY_MINOR" -ge 9 ]; then
                    PYTHON_BIN=$py_cmd
                    echo -e "${GREEN}✓ Found Python $PY_VERSION at $(which $py_cmd)${NC}"
                    break
                fi
            fi
        done

        if [ -z "$PYTHON_BIN" ]; then
            echo -e "${RED}Error: Python 3.9 or higher is required${NC}"
            echo "Please install Python 3.9+ from https://www.python.org/downloads/"
            exit 1
        fi

        # Check for build tools on Linux (required for psutil compilation)
        if [[ "$OSTYPE" == "linux-gnu"* ]]; then
            echo -e "${YELLOW}Checking system dependencies...${NC}"
            MISSING_DEPS=()

            if ! command -v gcc &> /dev/null; then
                MISSING_DEPS+=("gcc")
            fi

            # Check for python3-dev headers
            if ! $PYTHON_BIN -c "import sysconfig; print(sysconfig.get_config_var('INCLUDEPY'))" 2>/dev/null | xargs test -d 2>/dev/null; then
                MISSING_DEPS+=("python3-dev")
            fi

            if [ ${#MISSING_DEPS[@]} -gt 0 ]; then
                echo -e "${RED}✗ Missing system dependencies: ${MISSING_DEPS[*]}${NC}"
                echo ""
                echo "Install with:"
                echo -e "${YELLOW}  sudo apt-get update && sudo apt-get install -y gcc python3-dev${NC}"
                echo ""
                echo "Or on RHEL/Amazon Linux:"
                echo -e "${YELLOW}  sudo yum install -y gcc python3-devel${NC}"
                exit 1
            fi

            echo -e "${GREEN}✓ System dependencies installed${NC}"
        fi

        cd "$SCRIPT_DIR"

        # Create virtual environment if it doesn't exist
        if [ ! -d "venv" ]; then
            echo -e "${YELLOW}Creating virtual environment with $PYTHON_BIN...${NC}"
            if ! $PYTHON_BIN -m venv venv; then
                echo -e "${RED}✗ Failed to create virtual environment${NC}"
                exit 1
            fi
        fi

        # Verify venv was created
        if [ ! -f "venv/bin/activate" ]; then
            echo -e "${RED}✗ Virtual environment activation script not found${NC}"
            echo "venv directory exists but activate script is missing"
            exit 1
        fi

        # Activate and install dependencies
        echo -e "${YELLOW}Installing dependencies (this may take a few minutes)...${NC}"
        source venv/bin/activate

        # Upgrade pip quietly
        pip install --upgrade pip --quiet 2>&1 | grep -v "WARNING" || true

        # Install requirements with suppressed compilation warnings
        # Note: psycopg2-binary may show warnings on Python 3.13+ (safe to ignore)
        echo -e "${YELLOW}Installing packages...${NC}"
        if pip install -r requirements.txt --quiet 2>&1 | tee /tmp/arc_install.log | grep -E "(ERROR|Failed)" ; then
            echo -e "${RED}✗ Dependency installation failed${NC}"
            echo "Check logs: cat /tmp/arc_install.log"
            exit 1
        fi

        echo -e "${GREEN}✓ Dependencies installed${NC}"

        # Create .env if it doesn't exist
        if [ ! -f ".env" ]; then
            echo -e "${YELLOW}Creating .env from .env.example...${NC}"
            cp .env.example .env
            # Update for native deployment with MinIO
            sed -i.bak 's|MINIO_ENDPOINT=minio:9000|MINIO_ENDPOINT=http://localhost:9000|' .env
            rm .env.bak 2>/dev/null || true
        fi

        # Create directories
        mkdir -p data logs /tmp/arc-data

        # Check storage backend from arc.conf
        STORAGE_BACKEND="minio"  # default
        if [ -f "arc.conf" ]; then
            # Extract backend value from arc.conf (handles both quoted and unquoted values)
            STORAGE_BACKEND=$(grep -E "^\s*backend\s*=" arc.conf | head -1 | sed -E 's/.*=\s*"?([^"]+)"?.*/\1/' | tr -d ' ')
        fi
        # Allow environment variable override
        STORAGE_BACKEND="${STORAGE_BACKEND:-${STORAGE_BACKEND}}"

        echo -e "${YELLOW}Storage backend: $STORAGE_BACKEND${NC}"

        # Only setup MinIO if using minio backend
        if [ "$STORAGE_BACKEND" = "minio" ]; then
            mkdir -p "$SCRIPT_DIR/minio-data"

            # Check if MinIO binary is available
            if ! command -v minio &> /dev/null; then
            echo -e "${YELLOW}MinIO binary not found. Installing...${NC}"

            # Detect architecture
            ARCH=$(uname -m)
            case "$ARCH" in
                x86_64)
                    MINIO_ARCH="amd64"
                    ;;
                aarch64|arm64)
                    MINIO_ARCH="arm64"
                    ;;
                *)
                    echo -e "${RED}Unsupported architecture: $ARCH${NC}"
                    echo "Supported: x86_64, aarch64, arm64"
                    exit 1
                    ;;
            esac

            echo -e "${YELLOW}Detected architecture: $ARCH (using $MINIO_ARCH)${NC}"

            # Detect OS and install MinIO
            if [[ "$OSTYPE" == "darwin"* ]]; then
                # macOS
                if command -v brew &> /dev/null; then
                    echo -e "${YELLOW}Installing MinIO via Homebrew...${NC}"
                    brew install minio/stable/minio minio/stable/mc
                else
                    echo -e "${RED}Homebrew not found. Please install from https://brew.sh${NC}"
                    echo "Or download MinIO manually:"
                    echo "  wget https://dl.min.io/server/minio/release/darwin-${MINIO_ARCH}/minio"
                    echo "  chmod +x minio"
                    echo "  sudo mv minio /usr/local/bin/"
                    exit 1
                fi
            elif [[ "$OSTYPE" == "linux-gnu"* ]] || [[ "$OSTYPE" == "linux"* ]]; then
                # Linux
                echo -e "${YELLOW}Installing MinIO binary for linux-${MINIO_ARCH}...${NC}"

                # Download MinIO server
                MINIO_URL="https://dl.min.io/server/minio/release/linux-${MINIO_ARCH}/minio"
                if ! wget -q "$MINIO_URL" -O /tmp/minio; then
                    echo -e "${RED}Failed to download MinIO from $MINIO_URL${NC}"
                    exit 1
                fi
                chmod +x /tmp/minio

                # Try to install system-wide, fallback to local
                if sudo mv /tmp/minio /usr/local/bin/minio 2>/dev/null; then
                    echo -e "${GREEN}✓ MinIO installed to /usr/local/bin/minio${NC}"
                else
                    mv /tmp/minio "$SCRIPT_DIR/minio"
                    echo -e "${GREEN}✓ MinIO installed to $SCRIPT_DIR/minio${NC}"
                    export PATH="$SCRIPT_DIR:$PATH"
                fi

                # Download mc (MinIO client)
                echo -e "${YELLOW}Installing MinIO client (mc) for linux-${MINIO_ARCH}...${NC}"
                MC_URL="https://dl.min.io/client/mc/release/linux-${MINIO_ARCH}/mc"
                if ! wget -q "$MC_URL" -O /tmp/mc; then
                    echo -e "${RED}Failed to download mc from $MC_URL${NC}"
                    exit 1
                fi
                chmod +x /tmp/mc

                # Try to install system-wide, fallback to local
                if sudo mv /tmp/mc /usr/local/bin/mc 2>/dev/null; then
                    echo -e "${GREEN}✓ mc installed to /usr/local/bin/mc${NC}"
                else
                    mv /tmp/mc "$SCRIPT_DIR/mc"
                    echo -e "${GREEN}✓ mc installed to $SCRIPT_DIR/mc${NC}"
                    export PATH="$SCRIPT_DIR:$PATH"
                fi
            else
                echo -e "${RED}Unsupported OS: $OSTYPE${NC}"
                exit 1
            fi

            echo -e "${GREEN}✓ MinIO installation complete${NC}"
        fi

        # Check if MinIO is running
        echo -e "${YELLOW}Checking MinIO status...${NC}"
        if ! curl -sf http://localhost:9000/minio/health/live > /dev/null 2>&1; then
            echo -e "${YELLOW}Starting MinIO server...${NC}"

            # Start MinIO in background
            export MINIO_ROOT_USER=minioadmin
            export MINIO_ROOT_PASSWORD=minioadmin

            nohup minio server "$SCRIPT_DIR/minio-data" \
                --address ":9000" \
                --console-address ":9001" \
                > "$SCRIPT_DIR/logs/minio.log" 2>&1 &

            MINIO_PID=$!
            echo $MINIO_PID > "$SCRIPT_DIR/minio.pid"

            # Wait for MinIO to start
            echo -e "${YELLOW}Waiting for MinIO to start...${NC}"
            for i in {1..30}; do
                if curl -sf http://localhost:9000/minio/health/live > /dev/null 2>&1; then
                    echo -e "${GREEN}✓ MinIO is running (PID: $MINIO_PID)${NC}"
                    break
                fi
                sleep 1
            done

            if ! curl -sf http://localhost:9000/minio/health/live > /dev/null 2>&1; then
                echo -e "${RED}✗ MinIO failed to start${NC}"
                echo "Check logs: tail -f $SCRIPT_DIR/logs/minio.log"
                exit 1
            fi

            # Configure MinIO client and create bucket
            echo -e "${YELLOW}Configuring MinIO...${NC}"

            # Find mc binary
            MC_BIN=""
            if [ -f "$SCRIPT_DIR/mc" ]; then
                MC_BIN="$SCRIPT_DIR/mc"
                echo -e "${YELLOW}Using local mc: $MC_BIN${NC}"
            elif command -v mc &> /dev/null; then
                MC_BIN="mc"
                echo -e "${YELLOW}Using system mc: $(which mc)${NC}"
            else
                echo -e "${RED}✗ mc client not found${NC}"
                echo "mc should have been installed earlier. Check installation logs."
                echo "PATH: $PATH"
                echo "Looking for: $SCRIPT_DIR/mc or mc in PATH"
                exit 1
            fi

            echo -e "${YELLOW}Setting up mc alias...${NC}"
            # Run with background timeout (works on macOS and Linux)
            ( $MC_BIN alias set arc-local http://localhost:9000 minioadmin minioadmin --api S3v4 2>&1 ) &
            MC_PID=$!
            sleep 5
            if kill -0 $MC_PID 2>/dev/null; then
                kill $MC_PID 2>/dev/null || true
                echo -e "${YELLOW}⚠️  mc alias setup took too long, continuing anyway...${NC}"
            else
                wait $MC_PID 2>/dev/null
                echo -e "${GREEN}✓ mc alias configured${NC}"
            fi

            echo -e "${YELLOW}Creating bucket 'arc'...${NC}"
            if $MC_BIN mb arc-local/arc --ignore-existing 2>&1 | grep -v "already"; then
                echo -e "${GREEN}✓ Bucket 'arc' created${NC}"
            else
                echo -e "${GREEN}✓ Bucket 'arc' already exists${NC}"
            fi

            echo -e "${GREEN}✓ MinIO configured${NC}"
        else
            echo -e "${GREEN}✓ MinIO is already running${NC}"
        fi
        else
            echo -e "${GREEN}✓ Skipping MinIO (using $STORAGE_BACKEND backend)${NC}"
        fi

        echo ""
        echo -e "${YELLOW}Detecting system configuration...${NC}"

        # Auto-detect CPU cores BEFORE sourcing .env
        # Use 1.5-2x cores for I/O-bound workloads (MinIO writes)
        if command -v nproc > /dev/null 2>&1; then
            CORES=$(nproc)
        elif [ -f /proc/cpuinfo ]; then
            CORES=$(grep -c processor /proc/cpuinfo)
        elif command -v sysctl > /dev/null 2>&1; then
            CORES=$(sysctl -n hw.ncpu 2>/dev/null || echo 4)
        else
            CORES=4
        fi

        # Worker count formula based on workload type:
        # - Default: CORES * 3 (optimized for high-throughput ingestion, 4M+ RPS)
        # - Query-heavy (analytics): CORES * 1 (CPU-bound, DuckDB computation)
        # - Memory-constrained: CORES * 2 (lower memory footprint)
        # Override with: export ARC_WORKER_MULTIPLIER=2 before running start.sh

        WORKER_MULTIPLIER=${ARC_WORKER_MULTIPLIER:-3}
        WORKERS=$((CORES * WORKER_MULTIPLIER))

        # Export WORKERS before sourcing .env so it won't be overridden
        export WORKERS

        echo -e "${GREEN}✓ Detected $CORES CPU cores${NC}"
        echo -e "${GREEN}✓ Using $WORKERS workers (${WORKER_MULTIPLIER}x cores for workload optimization)${NC}"

        # Start Arc Core
        echo ""
        echo -e "${GREEN}Starting Arc Core API (native mode)...${NC}"

        # Load and export environment variables
        echo -e "${YELLOW}Loading environment configuration...${NC}"
        set -a
        source .env
        set +a
        echo -e "${GREEN}✓ Environment loaded${NC}"

        # Check if gunicorn is available
        if ! command -v gunicorn &> /dev/null; then
            echo -e "${RED}✗ gunicorn not found in virtual environment${NC}"
            echo "Try: pip install -r requirements.txt"
            exit 1
        fi

        echo ""
        echo -e "${GREEN}═══════════════════════════════════════${NC}"
        echo -e "${GREEN}Arc Core is starting...${NC}"
        echo ""
        echo -e "API:           ${YELLOW}http://localhost:${PORT:-8000}${NC}"
        echo -e "MinIO Console: ${YELLOW}http://localhost:9001${NC}"
        echo -e "  Username: minioadmin"
        echo -e "  Password: minioadmin"
        echo ""
        echo -e "Workers: $WORKERS"
        echo -e "${GREEN}═══════════════════════════════════════${NC}"
        echo ""

        # Start gunicorn with explicit error handling
        if ! gunicorn -w ${WORKERS} -b ${HOST:-0.0.0.0}:${PORT:-8000} \
            -k uvicorn.workers.UvicornWorker \
            --timeout ${TIMEOUT:-300} \
            --graceful-timeout 30 \
            --access-logfile /dev/null \
            --error-logfile - \
            --log-level warning \
            api.main:app; then
            echo -e "${RED}✗ Arc Core failed to start${NC}"
            echo "Check that all dependencies are installed: pip install -r requirements.txt"
            exit 1
        fi
        ;;

    stop)
        echo -e "${YELLOW}Stopping Arc Core...${NC}"
        cd "$SCRIPT_DIR"
        docker-compose down
        echo -e "${GREEN}✓ Stopped${NC}"
        ;;

    *)
        echo "Usage: $0 {docker|rebuild|native|stop}"
        echo ""
        echo "  docker  - Start with Docker Compose (recommended)"
        echo "  rebuild - Rebuild Docker images (no cache) and start"
        echo "  native  - Start with native Python"
        echo "  stop    - Stop Docker services"
        exit 1
        ;;
esac
