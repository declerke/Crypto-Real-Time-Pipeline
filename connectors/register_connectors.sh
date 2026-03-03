#!/usr/bin/env bash
set -euo pipefail

CONNECT_URL="http://localhost:8083"
MAX_RETRIES=40
RETRY_DELAY=10
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

print_header() {
    echo ""
    echo "──────────────────────────────────────────"
    echo "  $1"
    echo "──────────────────────────────────────────"
}

wait_for_service() {
    local name="$1"
    local check_cmd="$2"
    echo "Waiting for ${name} to be ready..."
    for i in $(seq 1 "$MAX_RETRIES"); do
        if eval "$check_cmd" > /dev/null 2>&1; then
            echo "${name} is ready."
            return 0
        fi
        echo "  Attempt ${i}/${MAX_RETRIES} — ${name} not ready. Retrying in ${RETRY_DELAY}s..."
        sleep "$RETRY_DELAY"
    done
    echo "ERROR: ${name} never became ready."
    return 1
}

print_header "Step 1: Waiting for Kafka Connect REST API"
wait_for_service "Kafka Connect" "curl -sf ${CONNECT_URL}/connectors"

print_header "Step 2: Initializing Cassandra Keyspace and Table"
wait_for_service "Cassandra cqlsh" "docker exec crypto_cassandra cqlsh -e 'DESCRIBE KEYSPACES'"

echo "CREATE KEYSPACE IF NOT EXISTS crypto_market WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}; USE crypto_market; DROP TABLE IF EXISTS ticker_history; CREATE TABLE ticker_history (symbol text, ingested_at timestamp, price_usdt double, price_change_percent double, volume_usdt double, high_price_24h double, low_price_24h double, PRIMARY KEY (symbol, ingested_at)) WITH CLUSTERING ORDER BY (ingested_at DESC);" | docker exec -i crypto_cassandra cqlsh

echo "Waiting for keyspace propagation..."
until docker exec crypto_cassandra cqlsh -e "DESCRIBE KEYSPACE crypto_market" > /dev/null 2>&1; do
    echo -n "."
    sleep 2
done
echo " Keyspace is live."

print_header "Step 3: Removing any stale connectors"
for OLD_CONN in "postgres-source-crypto" "cassandra-sink-crypto" "cassandra-sink-crypto-v2" "cassandra-sink-crypto-v3"; do
    STATUS=$(curl -sf -o /dev/null -w "%{http_code}" "${CONNECT_URL}/connectors/${OLD_CONN}" || true)
    if [ "$STATUS" = "200" ]; then
        echo "Deleting existing connector: ${OLD_CONN}"
        curl -sf -X DELETE "${CONNECT_URL}/connectors/${OLD_CONN}"
        sleep 2
    fi
done

print_header "Step 4: Registering Debezium PostgreSQL Source Connector"
RESPONSE_SRC=$(curl -sf -X POST "${CONNECT_URL}/connectors" \
  -H "Content-Type: application/json" \
  -d "@${SCRIPT_DIR}/debezium-postgres-source.json")
# Use a unique variable name to avoid overwriting later
SRC_NAME=$(echo "$RESPONSE_SRC" | python3 -c "import sys,json; print(json.load(sys.stdin).get('name','ERROR'))")
echo "Registered: ${SRC_NAME}"

echo "Waiting 20s for source connector to initialize..."
sleep 20

print_header "Step 5: Registering DataStax Cassandra Sink Connector"
RESPONSE_SINK=$(curl -sf -X POST "${CONNECT_URL}/connectors" \
  -H "Content-Type: application/json" \
  -d "@${SCRIPT_DIR}/cassandra-sink.json")
SINK_NAME=$(echo "$RESPONSE_SINK" | python3 -c "import sys,json; print(json.load(sys.stdin).get('name','ERROR'))")
echo "Registered: ${SINK_NAME}"

echo "Waiting 15s for sink task validation..."
sleep 15

print_header "Connector Status Summary & Auto-Recovery"
# We loop through the names we JUST registered
for TARGET_CONN in "$SRC_NAME" "$SINK_NAME"; do
    echo "${TARGET_CONN} status:"
    STATUS_JSON=$(curl -sf "${CONNECT_URL}/connectors/${TARGET_CONN}/status" || echo "FAILED")
    
    if [ "$STATUS_JSON" != "FAILED" ]; then
        # Check if the first task failed
        STATE=$(echo "$STATUS_JSON" | python3 -c "import sys, json; print(json.load(sys.stdin).get('tasks', [{}])[0].get('state', 'UNKNOWN'))")
        if [ "$STATE" == "FAILED" ]; then
            echo "  Task failed. Attempting restart..."
            curl -sf -X POST "${CONNECT_URL}/connectors/${TARGET_CONN}/restart"
            sleep 5
        fi

        # Final Status Print
        curl -sf "${CONNECT_URL}/connectors/${TARGET_CONN}/status" | \
          python3 -c "
import sys, json
try:
    d = json.load(sys.stdin)
    print(f'  Connector state : {d.get(\"connector\", {}).get(\"state\", \"UNKNOWN\")}')
    for t in d.get(\"tasks\", []):
        print(f'  Task {t[\"id\"]} state    : {t[\"state\"]}')
except:
    print('  Failed to fetch status.')
"
    else
        echo "  Failed to fetch status for ${TARGET_CONN}."
    fi
done

echo "Pipeline setup complete."