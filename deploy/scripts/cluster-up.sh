#!/usr/bin/env bash
# Create the cluster and install the shared pieces: namespaces, the cryptopipe-env ConfigMap, operators.
# Owner: A (Platform). Idempotent: safe to rerun.
# Usage: PROFILE=local|lite|cloud deploy/scripts/cluster-up.sh
#   local: kind, 1 control plane + 2 workers     lite: kind, 1 control plane + 1 worker
#   cloud: no kind; uses the existing kubectl context named in KUBE_CONTEXT
set -euo pipefail
. "$(dirname "$0")/lib.sh"

require_tools kubectl helm
[[ "$PROFILE" == cloud ]] || require_tools docker kind

# 1. Cluster
if [[ "$PROFILE" == cloud ]]; then
  [[ -n "${KUBE_CONTEXT:-}" ]] || die "PROFILE=cloud needs KUBE_CONTEXT=<kubectl context of the managed cluster>"
  log "using existing cluster context $KUBE_CONTEXT"
elif cluster_exists; then
  ok "kind cluster '$CLUSTER_NAME' already exists"
else
  config="$DEPLOY_DIR/kind/cluster.yaml"
  [[ "$PROFILE" == lite ]] && config="$DEPLOY_DIR/kind/cluster-lite.yaml"
  log "creating kind cluster '$CLUSTER_NAME' from ${config#"$REPO_ROOT/"}"
  kind create cluster --name "$CLUSTER_NAME" --config "$config" --wait 180s
fi
k wait --for=condition=Ready nodes --all --timeout=180s >/dev/null
ok "nodes ready: $(k get nodes --no-headers | wc -l | tr -d ' ')"

# 2. Namespaces and shared config
k apply -f "$DEPLOY_DIR/common/namespaces.yaml" >/dev/null
ok "namespaces applied"
for ns in ingest spark serving; do
  k -n "$ns" apply -f "$DEPLOY_DIR/common/cryptopipe-env.yaml" >/dev/null
done
ok "ConfigMap cryptopipe-env applied to ingest, spark, serving"

# 3. Helm repos (OCI charts such as Strimzi need no repo)
helm repo add spark-operator https://kubeflow.github.io/spark-operator --force-update >/dev/null
helm repo update spark-operator >/dev/null
ok "helm repos ready"

# 4. Operators (A), then monitoring (E) if present
bash "$DEPLOY_DIR/kafka/install-operator.sh"
bash "$DEPLOY_DIR/spark/install-operator.sh"
run_if_exists deploy/monitoring/install.sh

((${#SKIPPED[@]} == 0)) || warn "skipped (not present yet): ${SKIPPED[*]}"
ok "cluster-up done (PROFILE=$PROFILE). Next: make build-images && make deploy"
