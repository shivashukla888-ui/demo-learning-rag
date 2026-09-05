#!/usr/bin/env sh
set -eu

API_KEY="${AEGIS_API_KEY:-hackathon-local-change-me}"
COMPOSE="docker compose -f docker-compose.yml -f docker-compose.kafka.yml"
task_file=$(mktemp /tmp/trades-kafka-smoke.XXXXXX)
trap 'rm -f "$task_file"' EXIT

printf '%s\n' \
  'trade_id,order_id,event_time,instrument,side,quantity,price,account_id,client_id,venue' \
  "KAFKA-$(date +%s),ORDER-$(date +%s),2026-08-25T14:00:00Z,NOVA.L,BUY,1000,101.25,ACC-KAFKA,CLIENT-KAFKA,XLON" \
  >"$task_file"

status=$(
  curl -fsS -H "X-Aegis-Key: ${API_KEY}" http://localhost:8080/v1/integration/kafka
)
printf '%s' "$status" | grep -q '"enabled":true'
before=$(printf '%s' "$status" | python3 -c 'import json,sys; print(json.load(sys.stdin)["consumed"])')

upload=$(
  curl -fsS -H "X-Aegis-Key: ${API_KEY}" -F "file=@${task_file};filename=trades-kafka-smoke.csv" \
    http://localhost:8080/v1/ingestion/files/trades
)
printf '%s' "$upload" | grep -q '"deliveryMode":"KAFKA_EVENT_DRIVEN"'

attempt=0
while [ "$attempt" -lt 20 ]; do
  status=$(curl -fsS -H "X-Aegis-Key: ${API_KEY}" http://localhost:8080/v1/integration/kafka)
  consumed=$(printf '%s' "$status" | python3 -c 'import json,sys; print(json.load(sys.stdin)["consumed"])')
  if [ "$consumed" -gt "$before" ]; then break; fi
  attempt=$((attempt + 1))
  sleep 1
done

[ "$consumed" -gt "$before" ]
$COMPOSE exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:29092 --list \
  | grep -q 'surveillance.file.accepted.v1.dlt'
printf '%s\n' "Kafka event flow is healthy: published and consumed an accepted-file event."
