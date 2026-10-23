#!/usr/bin/env bash
# Smoke test: one PASS/FAIL/SKIP line per check, exit 1 if anything FAILs. Owner: A (Platform).
# Usage: deploy/scripts/smoke-test.sh          (SKIP_SPARK=1 skips the ~3 min sample SparkApplication)
set -uo pipefail
. "$(dirname "$0")/lib.sh"

PY="$(python_bin)"
RESULTS=()
FAILS=0
record() {  # record <PASS|FAIL|SKIP> <check> <detail>
  RESULTS+=("$(printf '%-5s %-32s %s' "$1" "$2" "$3")")
  [[ "$1" == FAIL ]] && FAILS=$((FAILS + 1))
  echo "$1  $2: $3"
}

# Nodes
not_ready="$(k get nodes --no-headers 2>/dev/null | awk '$2 != "Ready"' | wc -l | tr -d ' ')"
total="$(k get nodes --no-headers 2>/dev/null | wc -l | tr -d ' ')"
if [[ "$total" -gt 0 && "$not_ready" == 0 ]]; then record PASS "nodes Ready" "$total/$total"
else record FAIL "nodes Ready" "$((total - not_ready))/$total"; fi

# Pods per namespace (finished Job/sample pods are ignored)
for ns in kafka storage spark ingest serving monitoring; do
  pods="$(k -n "$ns" get pods --no-headers 2>/dev/null | awk '$3 != "Completed" && $3 != "Succeeded"')"
  if [[ -z "$pods" ]]; then record SKIP "pods $ns" "no pods"; continue; fi
  bad="$(awk '{split($2, r, "/"); if (r[1] != r[2] || $3 != "Running") print $1 "(" $3 ")"}' <<<"$pods" | tr '\n' ' ')"
  n="$(wc -l <<<"$pods" | tr -d ' ')"
  if [[ -z "$bad" ]]; then record PASS "pods $ns" "$n Ready"; else record FAIL "pods $ns" "not ready: $bad"; fi
done

# Kafka: topics vs contracts, message counts (one throwaway pod for both)
expected="$(PYTHONPATH="$REPO_ROOT/src" "$PY" -c \
  'from cryptopipe.contracts import TOPICS; print("\n".join(f"{t.name} {t.partitions}" for t in TOPICS))' | tr -d '\r')"
kout="$(kafka_cli sh -c '/opt/kafka/bin/kafka-topics.sh --bootstrap-server crypto-kafka-bootstrap:9092 --describe;
  echo "--offsets--"; /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server crypto-kafka-bootstrap:9092 --time -1' 2>/dev/null)"
if [[ -z "$kout" ]]; then
  record FAIL "kafka topics" "Kafka CLI pod could not reach crypto-kafka-bootstrap:9092"
else
  actual="$(sed '/--offsets--/,$d' <<<"$kout" | awk '/PartitionCount/ {for (i=1;i<=NF;i++) {if ($i=="Topic:") t=$(i+1); if ($i=="PartitionCount:") p=$(i+1)}; print t, p}')"
  offsets="$(sed '1,/--offsets--/d' <<<"$kout")"
  mismatch=""
  while read -r name parts; do
    got="$(awk -v n="$name" '$1==n {print $2}' <<<"$actual")"
    [[ "$got" == "$parts" ]] || mismatch+="$name(${got:-missing}/$parts) "
    count="$(awk -F: -v n="$name" '$1==n {s+=$3} END {print s+0}' <<<"$offsets")"
    record PASS "kafka messages $name" "$count"
  done <<<"$expected"
  if [[ -z "$mismatch" ]]; then record PASS "kafka topics vs contracts" "$(wc -l <<<"$expected" | tr -d ' ') topics, partitions match"
  else record FAIL "kafka topics vs contracts" "$mismatch"; fi
fi

# HDFS: live DataNodes and lake usage (from the NameNode pod, as the HDFS superuser)
nn() { k -n storage exec hdfs-namenode-0 -c namenode -- bash -c "$1" 2>/dev/null | tr -d '\r'; }
want_dn="$(k -n storage get statefulset hdfs-datanode -o jsonpath='{.spec.replicas}' 2>/dev/null)"
if [[ -z "$want_dn" ]]; then
  record FAIL "hdfs live DataNodes" "HDFS not deployed"
else
  live="$(nn 'hdfs dfsadmin -report -live' | sed -n 's/^Live datanodes (\([0-9]*\)).*/\1/p')"
  if [[ "$live" == "$want_dn" ]]; then record PASS "hdfs live DataNodes" "$live/$want_dn"
  else record FAIL "hdfs live DataNodes" "${live:-0}/$want_dn"; fi
  # `hdfs dfs -du -h` prints "<size> [unit] <size on disk> [unit] <path>"
  size_of='{u = ($2 ~ /^[KMGTP]$/) ? $2 : ""; sub("^/lake/", "", $NF); printf "%s%s%s ", ($NF == "/lake" ? "" : $NF "="), $1, u}'
  du="$(nn 'hdfs dfs -du -s -h /lake' | awk "$size_of")"
  if [[ -n "$du" ]]; then record PASS "hdfs /lake usage" "${du% } total (by folder: $(nn 'hdfs dfs -du -h /lake' | awk "$size_of" | sed 's/ $//'))"
  else record FAIL "hdfs /lake usage" "/lake missing (did hdfs-init-lake run?)"; fi
fi

# Spark sample job + History Server
if [[ "${SKIP_SPARK:-0}" == 1 ]]; then
  record SKIP "spark sample job" "SKIP_SPARK=1"
elif out="$(bash "$DEPLOY_DIR/spark/check.sh" 2>&1)"; then
  record PASS "spark sample job" "$(grep -oE 'COMPLETED in [0-9]+s' <<<"$out" | head -1), listed by History Server"
else
  record FAIL "spark sample job" "$(tail -1 <<<"$out")"
fi

# Cassandra (E), only if deployed
cpod="$(k -n storage get pods -o name 2>/dev/null | grep -m1 cassandra | sed 's|pod/||')"
if [[ -z "$cpod" ]]; then
  record SKIP "cassandra ohlcv_1m rows" "Cassandra not deployed"
else
  rows="$(k -n storage exec "$cpod" -- cqlsh -e 'SELECT count(*) FROM crypto.ohlcv_1m;' 2>/dev/null | tr -d '\r' | awk 'f && NF {print $1; exit} /^-+$/ {f=1}')"
  if [[ "$rows" =~ ^[0-9]+$ ]]; then record PASS "cassandra ohlcv_1m rows" "$rows"
  else record FAIL "cassandra ohlcv_1m rows" "query failed on $cpod"; fi
fi

# Grafana (E), only if deployed
if ! k -n monitoring get svc -o name 2>/dev/null | grep -q grafana; then
  record SKIP "grafana reachable" "monitoring not deployed"
elif curl -fsS --max-time 5 http://127.0.0.1:3000/api/health >/dev/null 2>&1; then
  record PASS "grafana reachable" "http://localhost:3000"
else
  record FAIL "grafana reachable" "http://localhost:3000/api/health did not answer"
fi

echo
printf '%-5s %-32s %s
' RESULT CHECK DETAIL
printf '%s\n' "${RESULTS[@]}"
echo
if ((FAILS)); then echo "${_C_RED}$FAILS check(s) FAILED${_C_RESET}"; exit 1; fi
echo "${_C_GREEN}smoke test passed${_C_RESET}"
