#!/bin/bash
set -e

# Arc Core - Stop Script
# Stops all Arc Core processes (Docker or native)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MODE=${1:-stop}

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

if [ "$MODE" = "clean" ]; then
    echo -e "${RED}╔════════════════════════════════════════╗${NC}"
    echo -e "${RED}║   Stopping & Cleaning Arc Core         ║${NC}"
    echo -e "${RED}╚════════════════════════════════════════╝${NC}"
else
    echo -e "${YELLOW}╔════════════════════════════════════════╗${NC}"
    echo -e "${YELLOW}║         Stopping Arc Core              ║${NC}"
    echo -e "${YELLOW}╚════════════════════════════════════════╝${NC}"
fi
echo ""

# Stop Docker containers
echo -e "${YELLOW}Checking for Docker containers...${NC}"
cd "$SCRIPT_DIR"
if [ -f "docker-compose.yml" ]; then
    if docker-compose ps -q 2>/dev/null | grep -q .; then
        if [ "$MODE" = "clean" ]; then
            # Full cleanup: remove containers, volumes, and images
            echo -e "${RED}Removing containers, volumes, and orphans...${NC}"
            docker-compose down -v --remove-orphans
            echo -e "${GREEN}✓ Docker containers and volumes removed${NC}"

            # Remove Arc images
            echo -e "${RED}Removing Arc Docker images...${NC}"
            ARC_IMAGES=$(docker images | grep -E "arc-core|arc-api" | awk '{print $3}')
            if [ -n "$ARC_IMAGES" ]; then
                echo "$ARC_IMAGES" | xargs docker rmi -f 2>/dev/null || true
                echo -e "${GREEN}✓ Arc Docker images removed${NC}"
            else
                echo "No Arc images to remove"
            fi
        else
            echo -e "${YELLOW}Stopping Docker containers...${NC}"
            docker-compose down
            echo -e "${GREEN}✓ Docker containers stopped${NC}"
        fi
    else
        echo "No running Docker containers found"
    fi
fi

# Stop native processes
echo ""
echo -e "${YELLOW}Checking for native Arc processes...${NC}"

# Find Arc API processes (gunicorn/uvicorn)
ARC_PIDS=$(ps aux | grep -E "(gunicorn|uvicorn).*api.main:app" | grep -v grep | awk '{print $2}')

if [ -z "$ARC_PIDS" ]; then
    echo "No running Arc processes found"
else
    echo -e "${YELLOW}Found Arc processes: $ARC_PIDS${NC}"
    echo -e "${YELLOW}Stopping Arc processes...${NC}"
    for PID in $ARC_PIDS; do
        echo "  Sending SIGTERM to process $PID"
        kill -15 $PID 2>/dev/null || true
    done

    # Wait for graceful shutdown
    sleep 3

    # Force kill if still running
    for PID in $ARC_PIDS; do
        if kill -0 $PID 2>/dev/null; then
            echo "  Force killing process $PID"
            kill -9 $PID 2>/dev/null || true
        fi
    done

    echo -e "${GREEN}✓ Arc processes stopped${NC}"
fi

# Check if MinIO is running locally
echo ""
echo -e "${YELLOW}Checking for MinIO processes...${NC}"
MINIO_PIDS=$(ps aux | grep "minio server" | grep -v grep | awk '{print $2}')

if [ -z "$MINIO_PIDS" ]; then
    echo "No running MinIO processes found"
else
    echo -e "${YELLOW}Found MinIO processes: $MINIO_PIDS${NC}"
    read -p "Stop MinIO as well? (y/n): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        for PID in $MINIO_PIDS; do
            echo "  Killing MinIO process $PID"
            kill -15 $PID 2>/dev/null || kill -9 $PID 2>/dev/null || true
        done
        echo -e "${GREEN}✓ MinIO processes stopped${NC}"
    else
        echo "Skipping MinIO shutdown"
    fi
fi

# Clean up native mode files if requested
if [ "$MODE" = "clean" ]; then
    echo ""
    echo -e "${RED}════════════════════════════════════════${NC}"
    echo -e "${RED}WARNING: This will remove ALL Arc data!${NC}"
    echo -e "${RED}════════════════════════════════════════${NC}"
    echo ""
    echo "The following will be deleted:"
    [ -d "$SCRIPT_DIR/data" ] && echo "  • Data directory ($SCRIPT_DIR/data)"
    [ -d "$SCRIPT_DIR/minio-data" ] && echo "  • MinIO data ($SCRIPT_DIR/minio-data)"
    [ -d "$SCRIPT_DIR/logs" ] && echo "  • Logs ($SCRIPT_DIR/logs)"
    [ -d "/tmp/arc-data" ] && echo "  • Temp data (/tmp/arc-data)"
    [ -d "$SCRIPT_DIR/venv" ] && echo "  • Python virtual environment (optional)"
    echo ""
    read -p "Continue with cleanup? (y/n): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "Cleanup cancelled"
        exit 0
    fi
    echo ""
    echo -e "${RED}Cleaning native mode data...${NC}"

    # Remove databases
    if [ -d "$SCRIPT_DIR/data" ]; then
        echo -e "${YELLOW}Removing data directory...${NC}"
        rm -rf "$SCRIPT_DIR/data"
        echo -e "${GREEN}✓ Data directory removed${NC}"
    fi

    # Remove MinIO data
    if [ -d "$SCRIPT_DIR/minio-data" ]; then
        echo -e "${YELLOW}Removing MinIO data...${NC}"
        rm -rf "$SCRIPT_DIR/minio-data"
        echo -e "${GREEN}✓ MinIO data removed${NC}"
    fi

    # Remove logs
    if [ -d "$SCRIPT_DIR/logs" ]; then
        echo -e "${YELLOW}Removing logs...${NC}"
        rm -rf "$SCRIPT_DIR/logs"
        echo -e "${GREEN}✓ Logs removed${NC}"
    fi

    # Remove temp data
    if [ -d "/tmp/arc-data" ]; then
        echo -e "${YELLOW}Removing temp data...${NC}"
        rm -rf /tmp/arc-data
        echo -e "${GREEN}✓ Temp data removed${NC}"
    fi

    # Remove PID files
    rm -f "$SCRIPT_DIR/minio.pid" 2>/dev/null || true

    # Remove installation log
    rm -f /tmp/arc_install.log 2>/dev/null || true

    # Remove virtual environment
    if [ -d "$SCRIPT_DIR/venv" ]; then
        read -p "Remove Python virtual environment? (y/n): " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            echo -e "${YELLOW}Removing virtual environment...${NC}"
            rm -rf "$SCRIPT_DIR/venv"
            echo -e "${GREEN}✓ Virtual environment removed${NC}"
        fi
    fi
fi

echo ""
if [ "$MODE" = "clean" ]; then
    echo -e "${GREEN}╔════════════════════════════════════════╗${NC}"
    echo -e "${GREEN}║  Arc Core Stopped & Cleaned            ║${NC}"
    echo -e "${GREEN}╚════════════════════════════════════════╝${NC}"
    echo ""
    echo -e "${YELLOW}Note: Run './start.sh rebuild' (Docker) or './start.sh native' (Native) to start fresh${NC}"
else
    echo -e "${GREEN}╔════════════════════════════════════════╗${NC}"
    echo -e "${GREEN}║       Arc Core Stopped                 ║${NC}"
    echo -e "${GREEN}╚════════════════════════════════════════╝${NC}"
    echo ""
    echo -e "${YELLOW}Usage: ./stop.sh clean - to remove all data, logs, and caches${NC}"
fi
