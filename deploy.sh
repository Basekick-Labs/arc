#!/bin/bash
set -e

# Arc Core - Standalone Deployment Script
# Deploys Arc Core to a remote server

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
REMOTE_USER=${REMOTE_USER:-ubuntu}
REMOTE_HOST=${REMOTE_HOST:-}
REMOTE_PATH=${REMOTE_PATH:-~/arc-core}
DEPLOYMENT_MODE=${DEPLOYMENT_MODE:-docker}  # docker or native

usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  -h HOST         Remote host to deploy to (required)"
    echo "  -u USER         Remote user (default: ubuntu)"
    echo "  -p PATH         Remote path (default: ~/arc-core)"
    echo "  -m MODE         Deployment mode: docker or native (default: docker)"
    echo "  --help          Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 -h 13.59.56.145 -u ubuntu -m docker"
    echo "  $0 -h my-server.com -u admin -m native"
    exit 1
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -h)
            REMOTE_HOST="$2"
            shift 2
            ;;
        -u)
            REMOTE_USER="$2"
            shift 2
            ;;
        -p)
            REMOTE_PATH="$2"
            shift 2
            ;;
        -m)
            DEPLOYMENT_MODE="$2"
            shift 2
            ;;
        --help)
            usage
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            usage
            ;;
    esac
done

# Validate required parameters
if [ -z "$REMOTE_HOST" ]; then
    echo -e "${RED}Error: Remote host is required${NC}"
    usage
fi

echo -e "${GREEN}╔════════════════════════════════════════╗${NC}"
echo -e "${GREEN}║      Arc Core Deployment Script       ║${NC}"
echo -e "${GREEN}╚════════════════════════════════════════╝${NC}"
echo ""
echo -e "Target: ${YELLOW}$REMOTE_USER@$REMOTE_HOST:$REMOTE_PATH${NC}"
echo -e "Mode:   ${YELLOW}$DEPLOYMENT_MODE${NC}"
echo ""

# Step 1: Copy files
echo -e "${GREEN}[1/4] Copying Arc Core files...${NC}"
rsync -avz --progress \
    --exclude='.git' \
    --exclude='__pycache__' \
    --exclude='*.pyc' \
    --exclude='data/' \
    --exclude='logs/' \
    --exclude='.env' \
    "$SCRIPT_DIR/" "$REMOTE_USER@$REMOTE_HOST:$REMOTE_PATH/"

echo -e "${GREEN}✓ Files copied successfully${NC}"
echo ""

# Step 2: Install dependencies
echo -e "${GREEN}[2/4] Installing dependencies on remote server...${NC}"

if [ "$DEPLOYMENT_MODE" == "docker" ]; then
    ssh "$REMOTE_USER@$REMOTE_HOST" << 'ENDSSH'
        # Install Docker if not present
        if ! command -v docker &> /dev/null; then
            echo "Installing Docker..."
            curl -fsSL https://get.docker.com -o get-docker.sh
            sudo sh get-docker.sh
            sudo usermod -aG docker $USER
            rm get-docker.sh
        fi

        # Install Docker Compose if not present
        if ! command -v docker-compose &> /dev/null; then
            echo "Installing Docker Compose..."
            sudo curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
            sudo chmod +x /usr/local/bin/docker-compose
        fi

        echo "Docker and Docker Compose installed"
ENDSSH
else
    ssh "$REMOTE_USER@$REMOTE_HOST" << 'ENDSSH'
        # Install Python 3.11 and dependencies
        sudo apt-get update
        sudo apt-get install -y python3.11 python3.11-venv python3-pip git curl wget build-essential

        echo "Python dependencies installed"
ENDSSH
fi

echo -e "${GREEN}✓ Dependencies installed${NC}"
echo ""

# Step 3: Setup and start
echo -e "${GREEN}[3/4] Setting up Arc Core on remote server...${NC}"

if [ "$DEPLOYMENT_MODE" == "docker" ]; then
    ssh "$REMOTE_USER@$REMOTE_HOST" << ENDSSH
        cd $REMOTE_PATH

        # Create .env file if it doesn't exist
        if [ ! -f .env ]; then
            cat > .env << 'EOF'
# Arc Core Configuration
STORAGE_BACKEND=minio
MINIO_ENDPOINT=minio:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
MINIO_BUCKET=historian
MINIO_USE_SSL=false

QUERY_CACHE_ENABLED=true
QUERY_CACHE_TTL=60

WORKERS=4
LOG_LEVEL=INFO
EOF
        fi

        # Build and start
        docker-compose down
        docker-compose build
        docker-compose up -d

        echo "Waiting for services to start..."
        sleep 10
        docker-compose ps
ENDSSH
else
    ssh "$REMOTE_USER@$REMOTE_HOST" << ENDSSH
        cd $REMOTE_PATH

        # Create virtual environment
        python3.11 -m venv venv
        source venv/bin/activate
        pip install --upgrade pip
        pip install -r requirements.txt

        # Create .env file
        if [ ! -f .env ]; then
            cat > .env << 'EOF'
STORAGE_BACKEND=local
LOCAL_STORAGE_PATH=/tmp/arc-data

QUERY_CACHE_ENABLED=true
QUERY_CACHE_TTL=60

WORKERS=4
LOG_LEVEL=INFO
EOF
        fi

        # Create directories
        mkdir -p data logs /tmp/arc-data

        # Create systemd service
        sudo tee /etc/systemd/system/arc-api.service > /dev/null << 'EOF'
[Unit]
Description=Arc Core API
After=network.target

[Service]
Type=simple
User=$USER
WorkingDirectory=$REMOTE_PATH
Environment="PATH=$REMOTE_PATH/venv/bin"
EnvironmentFile=$REMOTE_PATH/.env
ExecStart=$REMOTE_PATH/venv/bin/gunicorn -w 4 -b 0.0.0.0:8000 -k uvicorn.workers.UvicornWorker --timeout 300 api.main:app
Restart=on-failure

[Install]
WantedBy=multi-user.target
EOF

        # Start service
        sudo systemctl daemon-reload
        sudo systemctl enable arc-api
        sudo systemctl restart arc-api

        echo "Waiting for service to start..."
        sleep 5
        sudo systemctl status arc-api --no-pager
ENDSSH
fi

echo -e "${GREEN}✓ Arc Core setup complete${NC}"
echo ""

# Step 4: Verify deployment
echo -e "${GREEN}[4/4] Verifying deployment...${NC}"

sleep 5

if ssh "$REMOTE_USER@$REMOTE_HOST" "curl -sf http://localhost:8000/health > /dev/null"; then
    echo -e "${GREEN}✓ Arc Core is running and healthy!${NC}"
    echo ""
    echo -e "${GREEN}Deployment successful!${NC}"
    echo -e "Access Arc Core at: ${YELLOW}http://$REMOTE_HOST:8000${NC}"

    if [ "$DEPLOYMENT_MODE" == "docker" ]; then
        echo -e "MinIO Console at: ${YELLOW}http://$REMOTE_HOST:9001${NC}"
        echo -e "  Username: ${YELLOW}minioadmin${NC}"
        echo -e "  Password: ${YELLOW}minioadmin${NC}"
    fi
else
    echo -e "${RED}✗ Health check failed${NC}"
    echo -e "${YELLOW}Check logs on the remote server:${NC}"
    if [ "$DEPLOYMENT_MODE" == "docker" ]; then
        echo "  ssh $REMOTE_USER@$REMOTE_HOST 'cd $REMOTE_PATH && docker-compose logs -f'"
    else
        echo "  ssh $REMOTE_USER@$REMOTE_HOST 'sudo journalctl -u arc-api -f'"
    fi
    exit 1
fi
