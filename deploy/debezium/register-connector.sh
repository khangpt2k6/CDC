#!/usr/bin/env bash
#
# Register the Debezium Postgres source connector with Kafka Connect.
#
# Idempotent: removes any existing connector of the same name first, so it is
# safe to re-run after editing postgres-connector.json. Override the Connect
# address with CONNECT_URL (default http://localhost:8083).

set -euo pipefail

CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
NAME="cdc-postgres"
DIR="$(cd "$(dirname "$0")" && pwd)"
CONFIG="$DIR/postgres-connector.json"

echo "waiting for Kafka Connect at $CONNECT_URL ..."
until curl -sf "$CONNECT_URL/connectors" >/dev/null 2>&1; do
  sleep 2
done

echo "removing any existing connector named $NAME ..."
curl -s -X DELETE "$CONNECT_URL/connectors/$NAME" >/dev/null 2>&1 || true

echo "registering $NAME ..."
curl -s -X POST -H "Content-Type: application/json" \
  --data @"$CONFIG" "$CONNECT_URL/connectors" >/dev/null

# Give the connector a moment to transition to RUNNING.
sleep 3
echo "status:"
curl -s "$CONNECT_URL/connectors/$NAME/status"
echo
