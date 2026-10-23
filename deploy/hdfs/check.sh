#!/usr/bin/env bash
# HDFS health check, run from a client pod in namespace `spark` (proves cross-namespace DNS to the NameNode and
# to every DataNode hostname). Owner: A (Platform).
#  1. dfsadmin -report: expected number of live DataNodes
#  2. write a 50 MB file, read it back, compare checksums
#  3. restart drill: delete hdfs-datanode-0, wait, same DataNode hostnames live again, file still readable
#  4. with --namenode: also delete hdfs-namenode-0, check the cluster ID did not change (no re-format) and the
#     file is still readable
# Usage: deploy/hdfs/check.sh [--namenode] [--no-restart]
# Deleting pods is safe: StatefulSets recreate them on the same PVCs. No data is deleted except the test file.
set -euo pipefail
. "$(dirname "$0")/../scripts/lib.sh"

WITH_NN=0; RESTART=1
for arg in "$@"; do
  case "$arg" in
    --namenode) WITH_NN=1 ;;
    --no-restart) RESTART=0 ;;
    *) die "unknown argument $arg" ;;
  esac
done

POD=hdfs-client-check
TEST_DIR=/tmp/platform-check
TEST_FILE="$TEST_DIR/blob-$(date +%s)"
expected="$(k -n storage get statefulset hdfs-datanode -o jsonpath='{.spec.replicas}')"

sync_hadoop_conf
k -n spark delete pod "$POD" --ignore-not-found --wait=true >/dev/null
k -n spark apply -f - >/dev/null <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: $POD
  labels: { app.kubernetes.io/part-of: crypto-pipeline, app.kubernetes.io/component: platform-check }
spec:
  restartPolicy: Never
  securityContext: { runAsUser: 1000, runAsGroup: 100 }
  containers:
    - name: client
      image: $HADOOP_IMAGE
      imagePullPolicy: IfNotPresent
      command: ["sleep", "3600"]
      env:
        - { name: HADOOP_CONF_DIR, value: /opt/hadoop-conf }
        - { name: HADOOP_ROOT_LOGGER, value: "WARN,console" }
      resources:
        requests: { cpu: 100m, memory: 256Mi }
        limits: { memory: 768Mi }
      volumeMounts:
        - { name: hadoop-conf, mountPath: /opt/hadoop-conf }
  volumes:
    - name: hadoop-conf
      configMap: { name: hadoop-conf }
EOF
trap 'k -n spark delete pod "$POD" --ignore-not-found --wait=false >/dev/null' EXIT
k -n spark wait pod "$POD" --for=condition=Ready --timeout=300s >/dev/null
hdfs_exec() { k -n spark exec "$POD" -- bash -c "$1" 2>/dev/null | tr -d '\r'; }

k -n storage rollout status statefulset/hdfs-namenode --timeout=300s >/dev/null
k -n storage rollout status statefulset/hdfs-datanode --timeout=300s >/dev/null

live_count() { hdfs_exec 'hdfs dfsadmin -report -live' | sed -n 's/^Live datanodes (\([0-9]*\)).*/\1/p'; }
live_hosts() { hdfs_exec 'hdfs dfsadmin -report -live' | sed -n 's/^Hostname: //p' | sort | tr '\n' ' '; }

wait_live() {
  local n="" i
  for i in $(seq 1 60); do
    n="$(live_count)"
    [[ "$n" == "$expected" ]] && return 0
    sleep 5
  done
  die "expected $expected live DataNodes, have '${n:-0}' after 5 min"
}

# 1. Live DataNodes
wait_live
hosts_before="$(live_hosts)"
ok "$expected live DataNodes: $hosts_before"
grep -q "hdfs-datanode-0.hdfs-datanode.storage.svc.cluster.local" <<<"$hosts_before" \
  || die "DataNodes registered without their DNS names (dfs.datanode.hostname not applied?)"

# 2. 50 MB round trip
start=$(date +%s)
sum_local="$(hdfs_exec "set -e; dd if=/dev/urandom of=/tmp/blob bs=1M count=50 2>/dev/null; md5sum /tmp/blob | cut -d' ' -f1")"
hdfs_exec "set -e; hdfs dfs -mkdir -p $TEST_DIR; hdfs dfs -put -f /tmp/blob $TEST_FILE"
check_read() {
  local sum
  sum="$(hdfs_exec "set -e; rm -f /tmp/back; hdfs dfs -get $TEST_FILE /tmp/back; md5sum /tmp/back | cut -d' ' -f1")"
  [[ -n "$sum" && "$sum" == "$sum_local" ]] || die "checksum mismatch after $1: wrote $sum_local, read '${sum}'"
}
check_read "write"
ok "50 MB written and read back from the spark namespace, md5 $sum_local ($(( $(date +%s) - start )) s)"
cleanup_file() { hdfs_exec "hdfs dfs -rm -r -f -skipTrash $TEST_DIR" >/dev/null || true; }

if ((RESTART)); then
  # 3. DataNode restart
  start=$(date +%s)
  k -n storage delete pod hdfs-datanode-0 --wait=true >/dev/null
  k -n storage wait pod hdfs-datanode-0 --for=condition=Ready --timeout=300s >/dev/null
  wait_live
  hosts_after="$(live_hosts)"
  [[ "$hosts_after" == "$hosts_before" ]] || die "DataNode hostnames changed: before '$hosts_before', after '$hosts_after'"
  check_read "DataNode restart"
  ok "hdfs-datanode-0 restarted: same hostnames, file intact ($(( $(date +%s) - start )) s)"
fi

if ((WITH_NN)); then
  # 4. NameNode restart: no re-format
  cid() { k -n storage exec hdfs-namenode-0 -c namenode -- grep clusterID /data/namenode/current/VERSION | tr -d '\r'; }
  cid_before="$(cid)"
  start=$(date +%s)
  k -n storage delete pod hdfs-namenode-0 --wait=true >/dev/null
  k -n storage wait pod hdfs-namenode-0 --for=condition=Ready --timeout=300s >/dev/null
  hdfs_exec "hdfs dfsadmin -safemode wait" >/dev/null
  wait_live
  [[ "$(cid)" == "$cid_before" ]] || die "NameNode cluster ID changed ($cid_before -> $(cid)): it was re-formatted"
  check_read "NameNode restart"
  ok "hdfs-namenode-0 restarted: $cid_before unchanged, file intact ($(( $(date +%s) - start )) s)"
fi

cleanup_file
hdfs_exec "hdfs dfs -du -h /lake 2>/dev/null | head -20" || true
ok "HDFS check passed"
