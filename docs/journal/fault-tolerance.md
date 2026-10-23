# Fault tolerance

Scope: failure drills: broker, DataNode, Cassandra node, driver kills, recovery. Main contributors: A, C. Template: [README.md](README.md).

<!-- Add entries below, newest last. -->

## 2026-10-22: HDFS pod restarts on kind keep data and identity (first drill, deploy/hdfs/check.sh --namenode)

* **Context / symptom:** HDFS on Kubernetes has three classic restart failures: DataNodes re-registering under a
  new pod IP (clients then fail with "could not obtain block"), DataNodes losing the NameNode after it restarts
  with a new pod IP, and a NameNode that re-formats on restart (new cluster ID, every block orphaned).
* **What we tried:** design against all three, then drill: (1) each DataNode sets `dfs.datanode.hostname` to its
  StatefulSet DNS name (`hdfs-datanode-<i>.hdfs-datanode.storage.svc.cluster.local`, headless Service with
  `publishNotReadyAddresses`); (2) the NameNode sits behind a ClusterIP Service, whose virtual IP never changes;
  (3) the init container formats only when `/data/namenode/current/VERSION` is missing. The drill writes a 50 MB
  file from a pod in namespace `spark`, deletes `hdfs-datanode-0`, then `hdfs-namenode-0`, and re-reads it.
* **Numbers (local profile, 2 DataNodes, replication 1):** 50 MB put + get + md5: 8 s. DataNode pod delete → Ready
  and file re-read with the same md5: 20 s. NameNode pod delete → Ready, safe mode off, 2 live DataNodes, file
  re-read: 23 s; `clusterID=crypto-lake` before and after. Whole check: 61 s.
* **Fix / decision:** keep the three rules in `deploy/hdfs/base/` (comments explain each). Note that the
  DataNode drill does not wait for the NameNode to declare the old DataNode dead (default ~10.5 min); with
  replication 1 a block on a dead DataNode is unreadable until it returns. The full re-replication drill is Q3.
* **Lesson:** on Kubernetes, give every stateful peer a stable *name* (headless Service) and every server a stable
  *address* (ClusterIP Service), and make destructive init steps conditional on on-disk state, not on "first run".
