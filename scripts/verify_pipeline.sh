#!/usr/bin/env bash
set -euo pipefail

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

pass()  { echo -e "  ${GREEN}✓${NC} $1"; }
fail()  { echo -e "  ${RED}✗${NC} $1"; }
info()  { echo -e "  ${YELLOW}→${NC} $1"; }

section() {
    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "  $1"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
}

echo ""
echo "╔══════════════════════════════════════════╗"
echo "║    Crypto Pipeline — Verification        ║"
echo "╚══════════════════════════════════════════╝"

section "Layer 0: Environment Setup"

if [ ! -d "venv" ]; then
    info "Virtual environment not found. Creating..."
    python3 -m venv venv || fail "Failed to create venv. Ensure python3-venv is installed."
fi

if ! ./venv/bin/pip show requests > /dev/null 2>&1; then
    info "Installing 'requests' in venv..."
    ./venv/bin/pip install requests > /dev/null 2>&1 || fail "Failed to install requests."
    pass "Dependencies installed."
else
    pass "Virtual environment and dependencies ready."
fi

PYTHON_EXEC="./venv/bin/python3"

section "Layer 1: PostgreSQL Staging"

PG_COUNT=$(docker exec crypto_postgres psql -U cryptouser -d cryptodb -tAc \
  "SELECT COUNT(*) FROM crypto_ticker_24h;" 2>/dev/null || echo "0")
if [ "$PG_COUNT" -gt 0 ]; then
    pass "crypto_ticker_24h has ${PG_COUNT} symbol rows"
else
    fail "crypto_ticker_24h is empty — is the poller running?"
fi

PG_HIST=$(docker exec crypto_postgres psql -U cryptouser -d cryptodb -tAc \
  "SELECT COUNT(*) FROM ticker_history_pg;" 2>/dev/null || echo "0")
if [ "$PG_HIST" -gt 0 ]; then
    pass "ticker_history_pg has ${PG_HIST} history rows"
else
    fail "ticker_history_pg is empty"
fi

info "Top 5 Gainers (24h %):"
docker exec crypto_postgres psql -U cryptouser -d cryptodb \
  -c "SELECT symbol, price_usdt, price_change_percent FROM crypto_ticker_24h ORDER BY price_change_percent DESC LIMIT 5;" \
  2>/dev/null || fail "Could not query PostgreSQL"

section "Layer 2: Kafka Connect Connectors"

for connector in "postgres-source-crypto" "cassandra-sink-crypto-v2"; do
    STATE=$(curl -sf http://localhost:8083/connectors/$connector/status 2>/dev/null | \
      $PYTHON_EXEC -c "import sys, json; d=json.load(sys.stdin); print(d.get('connector', {}).get('state', 'UNKNOWN'))" 2>/dev/null || echo "NOT_FOUND")
    
    if [ "$STATE" = "RUNNING" ]; then
        pass "$connector: RUNNING"
    else
        fail "$connector state: $STATE"
    fi
done

info "Kafka topics:"
docker exec crypto_kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --list 2>/dev/null | grep -v "^__" || true

section "Layer 3: Cassandra Sink"

CASS_COUNT=$(docker exec crypto_cassandra cqlsh -e \
  "SELECT COUNT(*) FROM crypto_market.ticker_history;" 2>/dev/null | \
  grep -E '^\s+[0-9]+' | tr -d ' ' || echo "0")
if [ "${CASS_COUNT:-0}" -gt 0 ]; then
    pass "crypto_market.ticker_history has ${CASS_COUNT} rows"
else
    fail "crypto_market.ticker_history is empty"
fi

info "Sample Cassandra rows:"
docker exec crypto_cassandra cqlsh -e \
  "SELECT symbol, ingested_at, price_usdt, price_change_percent FROM crypto_market.ticker_history LIMIT 5;" \
  2>/dev/null || fail "Could not query Cassandra"

section "Layer 4: Data Latency"

PG_LATEST=$(docker exec crypto_postgres psql -U cryptouser -d cryptodb -tAc \
  "SELECT EXTRACT(EPOCH FROM MAX(last_update))::int FROM crypto_ticker_24h;" 2>/dev/null || echo "0")

CASS_LATEST_RAW=$(docker exec crypto_cassandra cqlsh -e \
  "SELECT MAX(ingested_at) FROM crypto_market.ticker_history;" 2>/dev/null | grep -E '[0-9]{4}-[0-9]{2}' || echo "")

if [ -n "$CASS_LATEST_RAW" ] && [ "$PG_LATEST" -gt 0 ]; then
    CASS_LATEST=$(date -d "$CASS_LATEST_RAW" +%s 2>/dev/null || echo "0")
    LATENCY=$((PG_LATEST - CASS_LATEST))
    ABS_LATENCY=${LATENCY#-}
    pass "Pipeline Latency: ${ABS_LATENCY}s"
else
    fail "Latency check skipped (missing data)"
fi

section "Layer 5: Grafana"

GRAFANA_HEALTH=$(curl -sf http://localhost:3000/api/health 2>/dev/null | \
  $PYTHON_EXEC -c "import sys,json; print(json.load(sys.stdin).get('database','unknown'))" || echo "unreachable")
if [ "$GRAFANA_HEALTH" = "ok" ]; then
    pass "Grafana is healthy"
else
    fail "Grafana health check failed"
fi

section "Layer 6: Ingestion Poller"

RUNNING=0
if [ -f "logs/poller.pid" ]; then
    POLLER_PID=$(cat logs/poller.pid)
    if kill -0 "$POLLER_PID" 2>/dev/null; then RUNNING=1; fi
fi

if [ "$RUNNING" -eq 0 ] && pgrep -f "ingestion/poller.py" > /dev/null; then
    RUNNING=1
fi

if [ "$RUNNING" -eq 1 ]; then
    pass "Poller is running"
else
    info "Poller process not found. Restarting..."
    mkdir -p logs
    nohup $PYTHON_EXEC ingestion/poller.py > logs/poller.log 2>&1 &
    sleep 2
    pgrep -f "ingestion/poller.py" > /dev/null && pass "Poller restarted successfully." || fail "Poller failed to start."
fi

info "Last 5 poller log lines:"
tail -n 5 logs/poller.log 2>/dev/null || echo "    (no log file found)"

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Verification complete!"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
