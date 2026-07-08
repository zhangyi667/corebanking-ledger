#!/bin/bash
# Layer 2 Kafka pipeline perf test — bypasses posting-manager.
# Injects N posting.transaction.received events directly into Kafka,
# measures how fast account-service applies them + publishes the
# account.balance.changed outbox rows.
#
# Prereqs:
#   - docker-compose.smoke.yml stack up
#   - the accounts referenced below (acc-perf-0..9) seeded in account-service

set -euo pipefail

N="${N:-100}"
COMPOSE_FILE="${COMPOSE_FILE:-$(dirname "$0")/../docker-compose.smoke.yml}"
KAFKA_CONTAINER="corebanking-smoke-kafka-1"
POSTGRES_ACC_CONTAINER="corebanking-smoke-postgres-acc-1"
TOPIC="posting.transaction"
ACCOUNT_SVC="http://localhost:8082"

echo "== Layer 2 perf test =="
echo "N=$N events, topic=$TOPIC"
echo

# ---------- 1. Seed 10 perf accounts if missing ----------
echo "seeding accounts…"
for i in $(seq 0 9); do
    curl -s -o /dev/null -w "" -X POST "$ACCOUNT_SVC/api/v1/accounts" \
        -H 'Content-Type: application/json' \
        -d "{\"accountId\":\"acc-perf-$i\",\"ownerId\":\"perf\",\"currency\":\"USD\"}" || true
done
echo "  → done"

# ---------- 2. Reset counters ----------
echo "resetting processed_event + outbox_event tables…"
docker exec "$POSTGRES_ACC_CONTAINER" psql -U account -d account_service -qtc \
    "TRUNCATE processed_event; DELETE FROM outbox_event; UPDATE balance SET amount=0, version=0 WHERE account_id LIKE 'acc-perf-%';" \
    > /dev/null

# ---------- 3. Generate N events into a file ----------
EVENTS_FILE=$(mktemp)
trap 'rm -f "$EVENTS_FILE"' EXIT

echo "generating $N events…"
for i in $(seq 0 $((N-1))); do
    debit=$((RANDOM % 10))
    credit=$((RANDOM % 10))
    while [ "$credit" -eq "$debit" ]; do credit=$((RANDOM % 10)); done

    posting_id=$(uuidgen | tr '[:upper:]' '[:lower:]')
    correlation_id="perf-corr-$i"
    idempotency_key="perf-key-$i"
    received_at=$(date -u +%Y-%m-%dT%H:%M:%S.000Z)

    envelope=$(cat <<EOF
{"eventId":$((i+1)),"eventType":"posting.transaction.received","aggregateId":"$posting_id","partitionKey":"$correlation_id","createdAt":"$received_at","payload":{"postingId":"$posting_id","transactionRef":"txn-perf-$i","correlationId":"$correlation_id","idempotencyKey":"$idempotency_key","currency":"USD","receivedAt":"$received_at","legs":[{"accountId":"acc-perf-$debit","type":"DEBIT","amount":"1.00"},{"accountId":"acc-perf-$credit","type":"CREDIT","amount":"1.00"}],"metadata":null}}
EOF
)
    # kafka-console-producer expects <key>|<value> per line
    echo "$correlation_id|$envelope" >> "$EVENTS_FILE"
done

echo "  → $EVENTS_FILE ($(wc -l < "$EVENTS_FILE") lines)"

# ---------- 4. Inject via kafka-console-producer ----------
echo "producing to Kafka…"
t0_ns=$(gdate +%s%N 2>/dev/null || python3 -c 'import time; print(int(time.time_ns()))')
docker exec -i "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server localhost:9092 \
    --topic "$TOPIC" \
    --property "parse.key=true" \
    --property "key.separator=|" \
    < "$EVENTS_FILE" > /dev/null
t_produced_ns=$(gdate +%s%N 2>/dev/null || python3 -c 'import time; print(int(time.time_ns()))')
produce_ms=$(( (t_produced_ns - t0_ns) / 1000000 ))
echo "  → produced $N messages in ${produce_ms} ms"

# ---------- 5. Wait for processed_event to reach N ----------
echo "waiting for account-service to apply all ${N}…"
deadline=$(( $(date +%s) + 600 ))
while :; do
    applied=$(docker exec "$POSTGRES_ACC_CONTAINER" psql -U account -d account_service -tAc \
        "SELECT COUNT(*) FROM processed_event;" | tr -d '[:space:]')
    if [ "$applied" -ge "$N" ]; then break; fi
    if [ "$(date +%s)" -gt "$deadline" ]; then
        echo "  → TIMEOUT: only $applied / $N applied after 120s"
        exit 1
    fi
    sleep 0.2
done
t_applied_ns=$(gdate +%s%N 2>/dev/null || python3 -c 'import time; print(int(time.time_ns()))')
apply_ms=$(( (t_applied_ns - t0_ns) / 1000000 ))
apply_rate=$(python3 -c "print(f'{$N * 1000 / $apply_ms:.1f}')")
echo "  → applied $N in ${apply_ms} ms → ${apply_rate} events/s"

# ---------- 6. Wait for outbox to drain (2N rows sent) ----------
expected_outbox=$((N * 2))
echo "waiting for outbox to drain all $expected_outbox rows…"
deadline=$(( $(date +%s) + 600 ))
while :; do
    sent=$(docker exec "$POSTGRES_ACC_CONTAINER" psql -U account -d account_service -tAc \
        "SELECT COUNT(*) FROM outbox_event WHERE sent_at IS NOT NULL;" | tr -d '[:space:]')
    if [ "$sent" -ge "$expected_outbox" ]; then break; fi
    if [ "$(date +%s)" -gt "$deadline" ]; then
        pending=$((expected_outbox - sent))
        echo "  → TIMEOUT: $sent / $expected_outbox drained; $pending still pending after 120s"
        exit 1
    fi
    sleep 0.2
done
t_drained_ns=$(gdate +%s%N 2>/dev/null || python3 -c 'import time; print(int(time.time_ns()))')
drain_ms=$(( (t_drained_ns - t0_ns) / 1000000 ))
drain_rate=$(python3 -c "print(f'{$expected_outbox * 1000 / $drain_ms:.1f}')")
echo "  → drained $expected_outbox in ${drain_ms} ms → ${drain_rate} events/s"

# ---------- 7. Summary ----------
echo
echo "== summary =="
printf "  N events            %10d\n" "$N"
printf "  produce             %10d ms\n" "$produce_ms"
printf "  apply (all N)       %10d ms  → %s events/s\n" "$apply_ms" "$apply_rate"
printf "  outbox drain (2N)   %10d ms  → %s events/s\n" "$drain_ms" "$drain_rate"

# ---------- 8. Balance sanity check ----------
echo
echo "final balances:"
docker exec "$POSTGRES_ACC_CONTAINER" psql -U account -d account_service -c \
    "SELECT account_id, amount FROM balance WHERE account_id LIKE 'acc-perf-%' ORDER BY account_id;"
