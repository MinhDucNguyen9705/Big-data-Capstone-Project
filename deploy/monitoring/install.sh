#!/usr/bin/env bash
# Owner: E, A9. Called by the existing platform cluster-up/deploy hooks.
set -euo pipefail
. "$(dirname "$0")/../scripts/lib.sh"
require_tools kubectl helm
[[ "$PROFILE" != cloud ]] || die "Review checkpoint monitoring storage/retention and access before cloud deployment"
CHART_VERSION=69.8.2
PY="$(python_bin)"
"$PY" "$DEPLOY_DIR/monitoring/generate.py" --check
k get namespace monitoring >/dev/null 2>&1 || k create namespace monitoring
if ! k -n monitoring get secret cryptopipe-grafana-admin >/dev/null 2>&1; then
  # Stream the random password straight into kubectl; never print or commit it.
  "$PY" -c 'import json,secrets; print(json.dumps({"apiVersion":"v1","kind":"Secret","metadata":{"name":"cryptopipe-grafana-admin","namespace":"monitoring"},"type":"Opaque","stringData":{"admin-user":"admin","admin-password":secrets.token_urlsafe(24)}}))' \
    | k apply -f - >/dev/null
fi
k apply -f "$DEPLOY_DIR/monitoring/dashboards.generated.yaml"
h upgrade --install crypto-monitoring kube-prometheus-stack \
  --repo https://prometheus-community.github.io/helm-charts \
  --version "$CHART_VERSION" --namespace monitoring \
  --values "$DEPLOY_DIR/monitoring/values.yaml" \
  --wait --timeout 15m --history-max 5
k apply -f "$DEPLOY_DIR/monitoring/kafka-podmonitor.yaml"
ok "Monitoring deployed. Grafana: http://localhost:3000 (or bash deploy/monitoring/port-forward.sh)"
ok "Read the generated admin password with the command in deploy/monitoring/README.md"
