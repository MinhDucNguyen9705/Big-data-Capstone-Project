#!/usr/bin/env bash
# Deploy everything onto the cluster created by cluster-up.sh, in dependency order, waiting at each step.
# Owner: A (Platform). Idempotent; never deletes data (only the finished hdfs-init-lake Job, to rerun it).
# Usage: PROFILE=local|lite|cloud WITH_JOBS=0|1 deploy/scripts/deploy-all.sh
#   WITH_JOBS=1 also applies C's deploy/spark/stream-*.yaml and D's deploy/spark/batch-*.yaml.
set -euo pipefail
. "$(dirname "$0")/lib.sh"

require_tools kubectl helm
WITH_JOBS="${WITH_JOBS:-0}"
PY="$(python_bin)"
start_all=$(date +%s)

# 1. Shared config (also done by cluster-up; repeated so `make deploy` alone picks up changes)
k apply -f "$DEPLOY_DIR/common/namespaces.yaml" >/dev/null
for ns in ingest spark serving; do k -n "$ns" apply -f "$DEPLOY_DIR/common/cryptopipe-env.yaml" >/dev/null; done
ok "namespaces and cryptopipe-env applied"

# 2. Kafka + topics generated from contracts.py
log "Kafka (overlay $PROFILE)"
k apply -k "$DEPLOY_DIR/kafka/overlays/$PROFILE" >/dev/null
"$PY" "$DEPLOY_DIR/kafka/gen_topics.py" --check >/dev/null \
  || die "deploy/kafka/topics.generated.yaml is out of date: run python deploy/kafka/gen_topics.py"
if [[ "$PROFILE" == cloud ]]; then
  "$PY" "$DEPLOY_DIR/kafka/gen_topics.py" --profile cloud -o - | k apply -f - >/dev/null
else
  k apply -f "$DEPLOY_DIR/kafka/topics.generated.yaml" >/dev/null
fi
k -n kafka wait kafka/crypto --for=condition=Ready --timeout=900s >/dev/null
k -n kafka wait kafkatopic --all --for=condition=Ready --timeout=300s >/dev/null
ok "Kafka crypto Ready, $(k -n kafka get kafkatopic --no-headers | wc -l | tr -d ' ') topics Ready"

# 3. HDFS + lake folders
log "HDFS (overlay $PROFILE)"
"$PY" "$DEPLOY_DIR/hdfs/gen_lake_dirs.py" --check >/dev/null \
  || die "deploy/hdfs/base/lake-dirs.generated.txt is out of date: run python deploy/hdfs/gen_lake_dirs.py"
k -n storage delete job hdfs-init-lake --ignore-not-found --wait=true >/dev/null
k apply -k "$DEPLOY_DIR/hdfs/overlays/$PROFILE" >/dev/null
wait_for_rollout storage statefulset/hdfs-namenode 600s
wait_for_rollout storage statefulset/hdfs-datanode 600s
k -n storage wait job/hdfs-init-lake --for=condition=Complete --timeout=600s >/dev/null
ok "HDFS ready, lake folders created"

# 4. Spark: HDFS client config for jobs, History Server (operator + ServiceAccount come from cluster-up)
sync_hadoop_conf
k apply -f "$DEPLOY_DIR/spark/history-server.yaml" >/dev/null
wait_for_rollout spark deployment/spark-history-server 600s

# 5. Other members' components, only if present
run_if_exists deploy/cassandra/install.sh
run_if_exists deploy/monitoring/install.sh
run_if_exists deploy/ingest/install.sh

if [[ "$WITH_JOBS" == 1 ]]; then
  shopt -s nullglob
  jobs=("$DEPLOY_DIR"/spark/stream-*.yaml "$DEPLOY_DIR"/spark/batch-*.yaml)
  shopt -u nullglob
  if ((${#jobs[@]})); then
    for f in "${jobs[@]}"; do k apply -f "$f"; done
  else
    SKIPPED+=("deploy/spark/stream-*.yaml, batch-*.yaml (none yet)")
  fi
else
  log "skipped Spark pipeline jobs (WITH_JOBS=1 to apply deploy/spark/stream-*.yaml and batch-*.yaml)"
fi

((${#SKIPPED[@]} == 0)) || warn "skipped (not present yet): ${SKIPPED[*]}"
ok "deploy done in $(( $(date +%s) - start_all )) s (PROFILE=$PROFILE). Next: make smoke"
