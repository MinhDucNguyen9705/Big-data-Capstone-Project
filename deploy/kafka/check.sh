#!/usr/bin/env bash
# Kafka health check. Owner: A (Platform).
#  1. the Kafka CR is Ready and every contract topic exists with the contract's partition count
#  2. round trip: the laptop produces to 127.0.0.1:9092 (external NodePort listener), an in-cluster pod consumes
#     from crypto-kafka-bootstrap:9092. Uses a temporary topic tmp.healthcheck (created and deleted here),
#     because the contracts define no health-check topic.
set -euo pipefail
. "$(dirname "$0")/../scripts/lib.sh"

PY="$(python_bin)"
BOOT="crypto-kafka-bootstrap:9092"
k -n kafka wait kafka/crypto --for=condition=Ready --timeout=600s >/dev/null
ok "Kafka CR crypto is Ready"

# 1. Contract topics vs cluster
expected="$(PYTHONPATH="$REPO_ROOT/src" "$PY" -c \
  'from cryptopipe.contracts import TOPICS; print("\n".join(f"{t.name} {t.partitions}" for t in TOPICS))' | tr -d '\r')"
actual="$(kafka_cli /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOT" --describe \
  | awk '/PartitionCount/ {for (i=1;i<=NF;i++) {if ($i=="Topic:") t=$(i+1); if ($i=="PartitionCount:") p=$(i+1)}; print t, p}')"
printf '%-18s %-10s %-10s\n' TOPIC CONTRACT CLUSTER
fail=0
while read -r name parts; do
  got="$(awk -v n="$name" '$1==n {print $2}' <<<"$actual")"
  printf '%-18s %-10s %-10s\n' "$name" "$parts" "${got:-missing}"
  [[ "$got" == "$parts" ]] || fail=1
done <<<"$expected"
((fail == 0)) || die "topics differ from contracts.py (is topics.generated.yaml applied and up to date?)"
ok "all contract topics present with the right partitions"

# 2. Round trip laptop -> cluster
TOPIC="tmp.healthcheck"
MSG="healthcheck-$(date +%s)-$RANDOM"
kafka_cli /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOT" --create --if-not-exists \
  --topic "$TOPIC" --partitions 1 --replication-factor 1 >/dev/null
trap 'kafka_cli /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOT" --delete --topic "$TOPIC" >/dev/null 2>&1 || true' EXIT

"$PY" - "$TOPIC" "$MSG" <<'EOF'
import sys
from confluent_kafka import Producer

topic, msg = sys.argv[1], sys.argv[2]
p = Producer({"bootstrap.servers": "127.0.0.1:9092", "message.timeout.ms": 15000})
errors = []
p.produce(topic, msg.encode(), callback=lambda err, _m: err and errors.append(err))
p.flush(20)
if errors:
    sys.exit(f"produce from laptop failed: {errors[0]}")
print(f"produced from laptop via 127.0.0.1:9092: {msg}")
EOF

got="$(kafka_cli /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server "$BOOT" --topic "$TOPIC" \
  --from-beginning --max-messages 1 --timeout-ms 30000 2>/dev/null | tr -d '\r' || true)"
[[ "$got" == *"$MSG"* ]] || die "in-cluster consumer did not receive the message (got: '$got')"
ok "round trip laptop -> Kafka -> in-cluster consumer"
