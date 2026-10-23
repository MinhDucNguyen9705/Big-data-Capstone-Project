#!/usr/bin/env bash
# Open the platform UIs on the laptop. Owner: A (Platform). Ctrl-C stops every forward.
# NodePort-mapped UIs (kind extraPortMappings) are used directly when they answer; otherwise this falls back to
# `kubectl port-forward` on the same local port. The Spark UI of a running driver always needs a port-forward.
set -euo pipefail
. "$(dirname "$0")/lib.sh"

PIDS=()
trap 'kill "${PIDS[@]}" 2>/dev/null || true' EXIT INT TERM

forward() {  # forward <namespace> <resource> <local:remote> <label>
  k -n "$1" port-forward "$2" "$3" >/dev/null 2>&1 &
  PIDS+=($!)
  ok "$4: http://localhost:${3%%:*}  (port-forward $1/$2)"
}

# expose <url> <namespace> <resource> <local:remote> <label>
expose() {
  if curl -fsS --max-time 3 "$1" >/dev/null 2>&1; then ok "$5: $1  (NodePort)"
  elif k -n "$2" get "$3" >/dev/null 2>&1; then forward "$2" "$3" "$4" "$5"
  else warn "$5: not deployed"
  fi
}

expose http://127.0.0.1:9870 storage svc/hdfs-namenode 9870:9870 "HDFS NameNode UI"
expose http://127.0.0.1:18080 spark svc/spark-history-server 18080:18080 "Spark History Server"

grafana="$(k -n monitoring get svc -o name 2>/dev/null | grep -m1 grafana || true)"
if [[ -n "$grafana" ]]; then
  expose http://127.0.0.1:3000/api/health monitoring "$grafana" 3000:80 "Grafana"
else
  warn "Grafana: not deployed"
fi

driver="$(k -n spark get pods -l spark-role=driver --field-selector=status.phase=Running -o name 2>/dev/null | head -1)"
if [[ -n "$driver" ]]; then forward spark "$driver" 4040:4040 "Spark UI (${driver#pod/})"
else warn "Spark UI: no running driver in namespace spark"
fi

if timeout 3 bash -c ': </dev/tcp/127.0.0.1/9092' 2>/dev/null; then
  ok "Kafka bootstrap: 127.0.0.1:9092  (NodePort)"
else
  warn "Kafka NodePort 127.0.0.1:9092 not answering. A port-forward cannot replace it (the broker advertises"
  warn "127.0.0.1:9093); check 'docker port $CLUSTER_NAME-control-plane' and deploy/README.md Troubleshooting."
fi

((${#PIDS[@]})) || { log "nothing to forward"; exit 0; }
log "forwarding; press Ctrl-C to stop"
wait
