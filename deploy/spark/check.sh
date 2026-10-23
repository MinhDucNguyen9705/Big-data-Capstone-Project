#!/usr/bin/env bash
# Spark health check: submit the sample SparkApplication, wait for COMPLETED, print the driver log tail, and
# confirm the run is listed by the History Server API. Owner: A (Platform).
set -euo pipefail
. "$(dirname "$0")/../scripts/lib.sh"

APP=platform-hdfs-check
EXAMPLES="$DEPLOY_DIR/spark/examples"
HISTORY_URL="${HISTORY_URL:-http://127.0.0.1:18080}"

sync_hadoop_conf
k -n spark create configmap platform-hdfs-check --from-file="hdfs-check.py=$EXAMPLES/hdfs-check.py" \
  --dry-run=client -o yaml | k apply -f - >/dev/null
k -n spark delete sparkapplication "$APP" --ignore-not-found --wait=true >/dev/null
# Earlier runs' output: /lake/_platform_check is scratch space of this check only (not a contract path)
k -n storage exec hdfs-namenode-0 -c namenode -- hdfs dfs -rm -r -f -skipTrash /lake/_platform_check >/dev/null 2>&1 || true
k apply -f "$EXAMPLES/hdfs-check.yaml" >/dev/null
log "submitted SparkApplication $APP, waiting for it to finish (timeout 10 min)"

start=$(date +%s); state=""
while (( $(date +%s) - start < 600 )); do
  state="$(k -n spark get sparkapplication "$APP" -o jsonpath='{.status.applicationState.state}' 2>/dev/null || true)"
  case "$state" in
    COMPLETED|FAILED|SUBMISSION_FAILED|FAILING) break ;;
  esac
  sleep 5
done
elapsed=$(( $(date +%s) - start ))
driver="$(k -n spark get sparkapplication "$APP" -o jsonpath='{.status.driverInfo.podName}' 2>/dev/null || true)"
app_id="$(k -n spark get sparkapplication "$APP" -o jsonpath='{.status.sparkApplicationId}' 2>/dev/null || true)"

if [[ -n "$driver" ]]; then
  log "driver log tail ($driver)"
  k -n spark logs "$driver" --tail=400 2>/dev/null | grep -E 'HDFS:|Kafka:|PLATFORM CHECK|Exception|Error' | tail -15 || true
fi
if [[ "$state" != COMPLETED ]]; then
  k -n spark describe sparkapplication "$APP" | tail -20
  die "SparkApplication $APP ended in state '${state:-unknown}' after ${elapsed}s"
fi
ok "SparkApplication $APP COMPLETED in ${elapsed}s (app id $app_id)"

# History Server: the run must show up in /api/v1/applications (it rescans the event log dir every 10 s)
k -n spark rollout status deployment/spark-history-server --timeout=300s >/dev/null
for _ in $(seq 1 24); do
  if curl -fsS --max-time 5 "$HISTORY_URL/api/v1/applications?limit=50" 2>/dev/null | grep -q "\"$app_id\""; then
    ok "History Server lists $app_id ($HISTORY_URL)"
    exit 0
  fi
  sleep 5
done
die "History Server at $HISTORY_URL does not list $app_id after 2 min"
