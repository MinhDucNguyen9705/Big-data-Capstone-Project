# deploy/

Kubernetes manifests, Helm values and scripts for the whole platform. Owner: **A (Platform)**, except
`cassandra/` and `monitoring/` (owner **E**), `ingest/` (owner **B**), `spark/stream-*.yaml` (**C**) and
`spark/batch-*.yaml` (**D**). Working plan: [docs/plans/member-a-platform.md](../docs/plans/member-a-platform.md).

```bash
make cluster-up        # kind cluster + namespaces + shared ConfigMap + Strimzi and spark-operator
make build-images      # cryptopipe-spark and cryptopipe-ingest images, loaded into kind
make deploy            # Kafka + topics, HDFS + lake folders, Spark History Server (+ others' install scripts)
make smoke             # PASS/FAIL/SKIP table: nodes, pods, topics, HDFS, Spark sample job, Cassandra
make port-forward      # NameNode UI, History Server, Spark driver UI, Grafana
make cluster-down      # delete the local kind cluster (asks first; YES=1 to skip)
```

All targets take `PROFILE=local|lite|cloud` (default `local`). The scripts are plain bash in `deploy/scripts/`
and run on Linux, macOS, WSL and Git Bash on Windows.

## Prerequisites

| Tool | Tested with | Minimum |
|---|---|---|
| Docker (Desktop) | 29.2 | 24, with **≥ 12 GB RAM and 6 CPUs** for the VM (16 GB recommended; 8 GB works for `lite` only, tight for `local`) |
| kind | 0.33.0 (node image Kubernetes 1.37) | 0.27 |
| kubectl | 1.34 | 1.31 |
| helm | 4.3.1 | 3.14 |
| GNU make | 4.4 | 3.81 (Windows: `winget install ezwinports.make`) |
| Python | 3.11 venv (`make install`) | used by the generators and checks |
| Java | not needed on the laptop (everything runs in images) | |

Windows: `winget install Kubernetes.kind Helm.Helm ezwinports.make`. Run `make` from Git Bash.

## Profiles

| | `local` (default) | `lite` | `cloud` (stub, Q2) |
|---|---|---|---|
| kind nodes | 1 control plane + 2 workers | 1 control plane + 1 worker | managed cluster (`KUBE_CONTEXT=...`) |
| Kafka | 1 dual-role node, RF 1 | same as local | 3 nodes, RF 3, `min.insync.replicas=2` |
| HDFS | 1 NameNode, 2 DataNodes, `dfs.replication=1` | 1 DataNode, replication 1 | 3 DataNodes, replication 3 |

## Endpoints

| What | Inside the cluster | From the laptop |
|---|---|---|
| Kafka bootstrap | `crypto-kafka-bootstrap.kafka:9092` | `127.0.0.1:9092` (NodePort 30092; broker 0 is advertised as `127.0.0.1:9093`) |
| HDFS | `hdfs://hdfs-namenode.storage:8020` | NameNode UI `http://localhost:9870` (NodePort 30870) |
| Lake root | `hdfs://hdfs-namenode.storage:8020/lake` | n/a |
| Spark event logs | `hdfs://hdfs-namenode.storage:8020/spark-events` | History Server `http://localhost:18080` (NodePort 31808) |
| Grafana (E) | | `http://localhost:3000` (NodePort 30300) |
| Spark images | `cryptopipe-spark:<git-sha>` and `:latest` | |
| Ingest image | `cryptopipe-ingest:<git-sha>` and `:latest` | |

Names follow `docs/contracts.md` section 6 exactly (short form `<svc>.<namespace>`). Do not mix in
`hdfs-namenode.storage.svc`: Hadoop treats a different authority as a different file system ("Wrong FS").

Use `127.0.0.1`, not `localhost`, for Kafka from a Windows laptop: `localhost` resolves to `::1` first and Docker
Desktop's IPv6 forwarding to kind hangs (librdkafka reports `Broker transport failure`). Put
`CRYPTO_KAFKA_BOOTSTRAP=127.0.0.1:9092` in your `.env`.

## Using the cluster (B, C, D, E)

* **Config:** every pod in `ingest`, `spark` and `serving` gets the cluster settings with
  ```yaml
  envFrom:
    - configMapRef: { name: cryptopipe-env }
  ```
  (source: [common/cryptopipe-env.yaml](common/cryptopipe-env.yaml); values from `docs/contracts.md`).
* **Kafka topics** are generated from `contracts.TOPICS` by [kafka/gen_topics.py](kafka/gen_topics.py); never
  create topics by hand (`auto.create.topics.enable=false`).
* **Lake folders** are generated from `contracts.LAKE_PATHS` by [hdfs/gen_lake_dirs.py](hdfs/gen_lake_dirs.py) and
  created by the `hdfs-init-lake` Job, owned by `spark`, mode 775.
* **Spark jobs** run in namespace `spark` with ServiceAccount `spark` (created by the spark-operator chart). Copy
  [spark/examples/hdfs-check.yaml](spark/examples/) to `deploy/spark/stream-*.yaml` (C) or `batch-*.yaml` (D).
* **Your own image:** `docker build ...` then `bash -c '. deploy/scripts/lib.sh; kind_load <image>'`
  (`kind load docker-image` fails on Docker Desktop's containerd image store, see Troubleshooting).
* **Install scripts:** `deploy/cassandra/install.sh`, `deploy/monitoring/install.sh` and `deploy/ingest/install.sh`
  are called by `make cluster-up` / `make deploy` when they exist, and skipped otherwise.

## Versions

| Component | Version | Pinned in |
|---|---|---|
| Strimzi operator (Helm OCI chart) | 1.3.0 (KRaft only, `kafka.strimzi.io/v1`) | `scripts/lib.sh` |
| Kafka | 4.3.1 | `scripts/lib.sh`, `kafka/base/kafka.yaml` |
| kubeflow spark-operator chart | 2.5.2 | `scripts/lib.sh` |
| Hadoop (HDFS) | `apache/hadoop:3.3.6` | `scripts/lib.sh`, `hdfs/base/*.yaml` |
| Spark | 3.5.9, Scala 2.12, Java 17, Python 3.11 | `docker/spark/Dockerfile` |

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `helm`: `open /d/...: The system cannot find the path specified` | Git Bash passes `/d/...` paths to native Windows tools | `lib.sh` uses `pwd -W` (`D:/...`) for `REPO_ROOT` |
| `kind load docker-image`: `ctr: content digest sha256:...: not found` | Docker Desktop containerd image store keeps multi-platform indexes | `kind_load` in `lib.sh` streams `docker save --platform linux/amd64` into each node |
| Laptop Kafka client: `Broker transport failure` / `Message timed out` | `localhost` → `::1` on Windows; IPv6 forwarding to kind hangs | use `127.0.0.1:9092`; the broker advertises `127.0.0.1:9093` |
| Strimzi operator restarts, `Stopped being a leader => exiting`, Kafka CR never Ready | lease renewal too slow on a busy laptop | single replica with `leaderElection.enable=false` (`kafka/install-operator.sh`) |
| `kubectl run ... timed out waiting for the condition` in check scripts | image pull / JVM start > 1 min on a busy node | `--pod-running-timeout=5m` in `kafka_cli` |
