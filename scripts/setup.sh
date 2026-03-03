#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

log()    { echo -e "${GREEN}[SETUP]${NC} $1"; }
warn()   { echo -e "${YELLOW}[WARN] ${NC} $1"; }
fail()   { echo -e "${RED}[ERROR]${NC} $1"; exit 1; }

echo ""
echo "╔══════════════════════════════════════════════════════╗"
echo "║     Crypto Real-Time Pipeline — Full Setup Script      ║"
echo "╚══════════════════════════════════════════════════════╝"
echo ""

command -v docker   >/dev/null 2>&1 || fail "Docker not found."
command -v python3  >/dev/null 2>&1 || fail "Python 3 not found."
command -v curl     >/dev/null 2>&1 || fail "curl not found."

mkdir -p logs

log "[1/5] Setting up Python virtual environment..."
if [ ! -d "venv" ]; then
    python3 -m venv venv
fi
source venv/bin/activate
pip install --quiet -r requirements.txt
log "      Python dependencies installed."

log "[2/5] Building and starting Docker services..."
docker compose up -d --build
log "      Services starting. Performing health checks..."

log "[3/5] Waiting for all services to be healthy..."
SERVICES=(
    "crypto_postgres:PostgreSQL"
    "crypto_kafka:Kafka"
    "crypto_cassandra:Cassandra"
    "crypto_connect:Kafka Connect"
    "crypto_grafana:Grafana"
)
MAX_WAIT=360
START_TIME=$(date +%s)

for ENTRY in "${SERVICES[@]}"; do
    CONTAINER="${ENTRY%%:*}"
    NAME="${ENTRY##*:}"
    echo -n "      Waiting for ${NAME} (${CONTAINER})..."
    while true; do
        NOW=$(date +%s)
        if [ $((NOW - START_TIME)) -gt $MAX_WAIT ]; then
            echo ""
            fail "Timeout waiting for ${NAME}."
        fi
        STATUS=$(docker inspect --format='{{.State.Health.Status}}' "$CONTAINER" 2>/dev/null || echo "missing")
        if [ "$STATUS" = "healthy" ]; then
            echo -e " ${GREEN}healthy${NC}"
            if [ "$CONTAINER" == "crypto_cassandra" ]; then
                echo -n "      Gossip stabilization sleep..."
                sleep 15
                echo -e " ${GREEN}done${NC}"
            fi
            break
        elif [ "$STATUS" = "unhealthy" ]; then
            echo ""
            warn "${NAME} is unhealthy."
            break
        fi
        echo -n "."
        sleep 5
    done
done

log "[4/5] Registering Kafka connectors and initializing Cassandra..."
chmod +x connectors/register_connectors.sh
./connectors/register_connectors.sh
log "      Connectors registered."

log "[5/5] Starting ingestion poller..."
source venv/bin/activate
if [ -f "logs/poller.pid" ]; then
    OLD_PID=$(cat logs/poller.pid)
    if kill -0 "$OLD_PID" 2>/dev/null; then
        warn "Poller already running with PID ${OLD_PID}. Skipping."
    else
        rm -f logs/poller.pid
    fi
fi

if [ ! -f "logs/poller.pid" ]; then
    nohup python3 ingestion/poller.py > logs/poller.log 2>&1 &
    echo $! > logs/poller.pid
    log "      Poller started with PID $(cat logs/poller.pid)"
fi

echo ""
echo "╔══════════════════════════════════════════════════════╗"
echo "║                   Setup Complete!                    ║"
echo "╠══════════════════════════════════════════════════════╣"
echo "║                                                      ║"
echo "║  Grafana Dashboard  →  http://localhost:3000         ║"
echo "║                        (admin / admin)               ║"
echo "║                                                      ║"
echo "║  Kafka Connect API  →  http://localhost:8083         ║"
echo "║  PostgreSQL         →  localhost:5432                ║"
echo "║  Cassandra (cqlsh)  →  localhost:9042                ║"
echo "║                                                      ║"
echo "║  Verify pipeline:   ./scripts/verify_pipeline.sh     ║"
echo "║  Stop poller:       kill \$(cat logs/poller.pid)      ║"
echo "║  Teardown:          docker compose down -v           ║"
echo "║                                                      ║"
echo "╚══════════════════════════════════════════════════════╝"
echo ""