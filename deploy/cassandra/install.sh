#!/usr/bin/env bash
# Owner: E, A7. Existing platform deploy-all.sh invokes this hook automatically.
set -euo pipefail
. "$(dirname "$0")/../scripts/lib.sh"
require_tools kubectl
[[ "$PROFILE" != cloud ]] || die "Cassandra checkpoint manifest is single-node; review an HA/auth configuration for cloud first"
k get namespace storage >/dev/null 2>&1 || k create namespace storage
k apply -k "$DEPLOY_DIR/cassandra"
wait_for_rollout storage statefulset/cassandra 1000s
k -n storage create configmap cassandra-schema --from-file="schema.cql=$REPO_ROOT/cql/schema.cql" \
  --dry-run=client -o yaml | k apply -f -
# Only the disposable schema Job is recreated; StatefulSet and PVCs are retained.
k -n storage delete job cassandra-schema --ignore-not-found --wait=true
k apply -f "$DEPLOY_DIR/cassandra/schema-job.yaml"
if ! k -n storage wait job/cassandra-schema --for=condition=Complete --timeout=620s; then
  k -n storage logs job/cassandra-schema --all-containers=true || true
  die "Cassandra schema failed; inspect storage/job/cassandra-schema"
fi
bash "$DEPLOY_DIR/cassandra/check.sh"
