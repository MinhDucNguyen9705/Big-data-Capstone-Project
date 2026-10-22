#!/usr/bin/env bash
# Install (or upgrade) the kubeflow spark-operator in namespace `spark`. Owner: A (Platform). Idempotent.
set -euo pipefail
. "$(dirname "$0")/../scripts/lib.sh"

# Cluster-wide Spark defaults for every submitted job (mounted into the controller, see operator-values.yaml).
# The controller re-reads the directory on each submission, so a rerun picks up edits without a restart.
k -n spark create configmap spark-defaults \
  --from-file=spark-defaults.conf="$(native_path "$REPO_ROOT/conf/spark-defaults.conf")" \
  --dry-run=client -o yaml | k apply -f - >/dev/null
ok "ConfigMap spark-defaults (spark) from conf/spark-defaults.conf"

log "installing spark-operator chart $SPARK_OPERATOR_CHART_VERSION"
h upgrade --install spark-operator spark-operator/spark-operator \
  --version "$SPARK_OPERATOR_CHART_VERSION" --namespace spark \
  -f "$DEPLOY_DIR/spark/operator-values.yaml" \
  --wait --timeout 5m >/dev/null
wait_for_rollout spark deployment/spark-operator-controller 300s
wait_for_rollout spark deployment/spark-operator-webhook 300s
ok "spark-operator ready (jobs run in namespace spark with ServiceAccount spark)"
