#!/usr/bin/env bash
# Owner: E. Bind only to loopback, and stop both child processes on Ctrl-C.
set -euo pipefail
. "$(dirname "$0")/../scripts/lib.sh"
require_tools kubectl
pids=()
cleanup() { for pid in "${pids[@]}"; do kill "$pid" 2>/dev/null || true; done; }
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
k -n monitoring port-forward --address 127.0.0.1 svc/crypto-monitoring-grafana 3000:80 &
pids+=($!)
k -n monitoring port-forward --address 127.0.0.1 svc/crypto-monitoring-prometheus 9090:9090 &
pids+=($!)
ok "Grafana http://127.0.0.1:3000; Prometheus http://127.0.0.1:9090. Ctrl-C stops both."
# Detect either forward failing instead of leaving the other silently running.
while kill -0 "${pids[0]}" 2>/dev/null && kill -0 "${pids[1]}" 2>/dev/null; do sleep 1; done
exit 1
