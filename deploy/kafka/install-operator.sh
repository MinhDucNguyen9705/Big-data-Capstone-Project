#!/usr/bin/env bash
# Install (or upgrade) the Strimzi cluster operator in namespace `kafka`. Owner: A (Platform). Idempotent.
# Strimzi 1.x is KRaft-only with node pools and the v1 CRD API. Installed in `kafka`, it watches only `kafka`.
# One replica, so leader election is off: on a busy kind cluster a slow lease renewal made the operator exit
# ("Stopped being a leader => exiting") mid-reconcile, and the Kafka CR never became Ready.
# The operator heap is 20% of the memory limit, so the limit is raised from the chart's 384Mi.
set -euo pipefail
. "$(dirname "$0")/../scripts/lib.sh"

log "installing Strimzi operator $STRIMZI_CHART_VERSION (Kafka $KAFKA_VERSION)"
h upgrade --install strimzi oci://quay.io/strimzi-helm/strimzi-kafka-operator \
  --version "$STRIMZI_CHART_VERSION" --namespace kafka \
  --set leaderElection.enable=false \
  --set resources.requests.memory=512Mi --set resources.limits.memory=1Gi \
  --set resources.requests.cpu=200m \
  --wait --timeout 5m >/dev/null
wait_for_rollout kafka deployment/strimzi-cluster-operator 300s
ok "Strimzi operator ready"
