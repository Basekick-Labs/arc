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

        # Check Python
        if ! command -v python3.11 &> /dev/null; then
            echo -e "${RED}Error: Python 3.11 is not installed${NC}"
            exit 1
        fi

        cd "$SCRIPT_DIR"

        # Create virtual environment if it doesn't exist
        if [ ! -d "venv" ]; then
            echo -e "${YELLOW}Creating virtual environment...${NC}"
            python3.11 -m venv venv
        fi

        # Activate and install dependencies
        echo -e "${YELLOW}Installing dependencies...${NC}"
        source venv/bin/activate
        pip install --upgrade pip > /dev/null
        pip install -r requirements.txt > /dev/null

        # Create .env if it doesn't exist
        if [ ! -f ".env" ]; then
            echo -e "${YELLOW}Creating .env from .env.example...${NC}"
            cp .env.example .env
            # Update for native deployment with MinIO
            sed -i.bak 's|MINIO_ENDPOINT=minio:9000|MINIO_ENDPOINT=http://localhost:9000|' .env
            rm .env.bak 2>/dev/null || true
        fi

        # Create directories
        mkdir -p data logs /tmp/arc-data "$SCRIPT_DIR/minio-data"

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
            MC_BIN="mc"
            if [ -f "$SCRIPT_DIR/mc" ]; then
                MC_BIN="$SCRIPT_DIR/mc"
            fi

            $MC_BIN alias set arc-local http://localhost:9000 minioadmin minioadmin > /dev/null 2>&1
            $MC_BIN mb arc-local/arc --ignore-existing > /dev/null 2>&1 || true
            echo -e "${GREEN}✓ MinIO configured (bucket: arc)${NC}"
        else
            echo -e "${GREEN}✓ MinIO is already running${NC}"
        fi

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

        # For I/O-bound workloads, use more workers than cores
        # 3x cores provides optimal throughput without context switching overhead
        WORKERS=$((CORES * 3))

        # Export WORKERS before sourcing .env so it won't be overridden
        export WORKERS

        # Start Arc Core
        echo ""
        echo -e "${GREEN}Starting Arc Core API (native mode)...${NC}"
        echo -e "${GREEN}Auto-detected $WORKERS CPU cores${NC}"

        # Load and export environment variables
        set -a
        source .env
        set +a

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

        gunicorn -w ${WORKERS} -b ${HOST:-0.0.0.0}:${PORT:-8000} \
            -k uvicorn.workers.UvicornWorker \
            --timeout ${TIMEOUT:-300} \
            --graceful-timeout 30 \
            --access-logfile /dev/null \
            --error-logfile - \
            --log-level warning \
            api.main:app
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
