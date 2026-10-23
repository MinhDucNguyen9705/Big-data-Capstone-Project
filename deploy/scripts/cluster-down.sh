#!/usr/bin/env bash
# Delete the local kind cluster (and every PVC in it). Owner: A (Platform).
# Usage: deploy/scripts/cluster-down.sh [--yes]
# Only ever deletes the local kind cluster named $CLUSTER_NAME (default crypto); never touches a cloud cluster.
set -euo pipefail
. "$(dirname "$0")/lib.sh"

[[ "${1:-}" == "--yes" ]] && ASSUME_YES=1
require_tools kind

if ! cluster_exists; then
  ok "kind cluster '$CLUSTER_NAME' does not exist, nothing to do"
  exit 0
fi
confirm "Delete kind cluster '$CLUSTER_NAME' and all its data (Kafka, HDFS, Cassandra volumes)?" || die "aborted"
kind delete cluster --name "$CLUSTER_NAME"
ok "cluster '$CLUSTER_NAME' deleted"
