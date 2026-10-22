#!/usr/bin/env bash
# Install (or upgrade) the kubeflow spark-operator in namespace `spark`. Owner: A (Platform). Idempotent.
set -euo pipefail
. "$(dirname "$0")/../scripts/lib.sh"

log "installing spark-operator chart $SPARK_OPERATOR_CHART_VERSION"
h upgrade --install spark-operator spark-operator/spark-operator \
  --version "$SPARK_OPERATOR_CHART_VERSION" --namespace spark \
  -f "$DEPLOY_DIR/spark/operator-values.yaml" \
  --wait --timeout 5m >/dev/null
wait_for_rollout spark deployment/spark-operator-controller 300s
wait_for_rollout spark deployment/spark-operator-webhook 300s
ok "spark-operator ready (jobs run in namespace spark with ServiceAccount spark)"
