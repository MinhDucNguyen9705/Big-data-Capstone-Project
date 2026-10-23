# Member A Implementation Plan for Claude Code: Platform

This is the working spec for Claude Code for **member A (Platform)**. It covers tasks A-1 to A-5 from [member-tasks.md](member-tasks.md) for Checkpoint 1, then the platform work after the checkpoint.

**How to use it**
- Run Claude Code **on your own machine** (CLI or desktop) inside your clone of the team repo. Every step needs Docker, kind, kubectl and Helm, which a cloud session usually cannot run.
- Copy this file into the repo as `docs/plans/member-a-platform.md` (in your first PR), then tell Claude Code:
  *"Read docs/plans/member-a-platform.md and CLAUDE.md. Do step P1 only. Follow section 1 (boundaries) strictly. Run the step's Done-when checks, then commit on a new branch and open a PR."*
- Do one step per branch and PR, in order P0 → P8. Steps P3 (HDFS) and P5 (Spark) are the hardest, so leave room for debugging.

---

## 1. Boundaries: do not conflict with other members' work

Claude Code must follow these rules in every step. Copy this section into the PR description of your first PR so reviewers know the rules.

### 1.1 Files A may create or change

```
deploy/kind/            deploy/kafka/        deploy/hdfs/
deploy/spark/           (except deploy/spark/stream-*.yaml → C, deploy/spark/batch-*.yaml → D)
deploy/common/          deploy/scripts/      docker/spark/       docker/ingest/
deploy/README.md        docs/adr/0002-*.md, docs/adr/0003-*.md (A's ADRs only)
docs/journal/{fault-tolerance,scaling,security,system-integration}.md  (append only)
docs/plans/member-a-platform.md
Makefile → only the targets: build-images, cluster-up, cluster-down, deploy, smoke, port-forward
```

### 1.2 Files A must never edit

| Path | Owner |
|---|---|
| `src/cryptopipe/**` (all Python package code), `pyproject.toml`, `tests/**` | B, C, D, E |
| `src/cryptopipe/contracts.py`, `docs/contracts.md`, `cql/schema.cql`, `conf/symbols.yaml` | everyone (contract PR only) |
| `deploy/ingest/**` | B |
| `deploy/spark/stream-*.yaml` | C |
| `deploy/spark/batch-*.yaml`, `docs/data.md` | D |
| `deploy/cassandra/**`, `deploy/monitoring/**`, `docs/architecture.md`, `README.md`, `CLAUDE.md` | E |
| `.github/**` | E |
| Makefile targets other than A's | their owners |
| Other members' journal files and ADRs | their owners |

### 1.3 Rules

1. **Read contracts, never change them.** Topic names, partitions, HDFS paths and env var names come from `src/cryptopipe/contracts.py` and `docs/contracts.md`. If the platform needs a contract change (for example a new topic, or a different Kafka bootstrap name), **stop, do not edit the file**, and write the proposed change into the PR description under "Contract change needed" so A can open a separate contract PR for the team to approve.
2. **Generate, don't duplicate.** Kafka topic manifests are generated from `contracts.py` by a script (P2), never typed by hand, so they cannot drift from what B, C and D use.
3. **Call other members' scripts, don't edit them.** `cluster-up.sh` and `deploy-all.sh` run `deploy/cassandra/install.sh`, `deploy/monitoring/install.sh` and `deploy/ingest/install.sh` **only if they exist**, and skip them with a message otherwise. If one of those scripts is broken, report it in the PR description; do not fix it.
4. **Templates for others are new files.** The sample SparkApplication goes in `deploy/spark/examples/`. C and D copy it into their own files; A never writes `stream-*.yaml` or `batch-*.yaml`.
5. **Python code is out of scope.** If a manifest or image needs a code change (for example a job entrypoint), describe it in the PR and tell the owner; do not change `src/`. The only Python A writes is under `deploy/` (helper scripts such as the topic generator).
6. **Makefile edits stay small.** Only add or change A's targets; keep other lines untouched. Rebase on `main` right before opening the PR, so Makefile and README conflicts show up early.
7. **One branch per step**, named `a/p<N>-<name>` (e.g. `a/p3-hdfs`). Before starting a step: `git fetch origin && git rebase origin/main`. Never force-push to `main`; never push to another member's branch.
8. **Namespaces are shared**: A creates them (P1) and owns the shared env ConfigMap (P1.4), but must not create or delete resources in `ingest`, `serving` or `monitoring` other than that ConfigMap.
9. **Do not delete data on a shared cluster.** `cluster-down` deletes only the local kind cluster. Any destructive command (`hdfs namenode -format`, deleting PVCs, deleting topics) needs an explicit confirmation flag and is never run automatically on an existing cluster.

---

## 2. Target result for Checkpoint 1

From a fresh clone, on any team laptop:

```bash
make cluster-up        # kind cluster + operators + namespaces + shared config
make build-images      # cryptopipe-spark and cryptopipe-ingest images, loaded into kind
make deploy            # Kafka, HDFS, Spark Operator, History Server (+ others' install scripts if present)
make smoke             # health report: pods, topics, HDFS, Spark sample job, Cassandra (if present)
```

Endpoints that other members rely on (written into `deploy/README.md` and the shared ConfigMap):

| What | Inside the cluster | From the laptop |
|---|---|---|
| Kafka bootstrap | `crypto-kafka-bootstrap.kafka.svc:9092` | `localhost:9092` (NodePort) |
| HDFS | `hdfs://hdfs-namenode.storage.svc:8020` | `make port-forward` → NameNode UI `localhost:9870` |
| Lake root | `hdfs://hdfs-namenode.storage.svc:8020/lake` | n/a |
| Spark event logs | `hdfs://hdfs-namenode.storage.svc:8020/spark-events` | History Server `localhost:18080` |
| Spark images | `cryptopipe-spark:<git-sha>` and `:latest` | |

---

## 3. Steps

### P0. Read the repo and prepare (no cluster yet) ★

- Read `CLAUDE.md`, `docs/contracts.md`, `src/cryptopipe/contracts.py`, `src/cryptopipe/config.py`, `docker/`, `Makefile`, `deploy/` (bootstrap stubs).
- Check the tools installed and print their versions: `docker`, `kind`, `kubectl`, `helm`, `java -version`. Write the minimum versions into `deploy/README.md` (do not install anything without asking).
- Write `deploy/README.md` skeleton: purpose, prerequisites (Docker with at least 12–16 GB RAM and 6 CPUs), profiles (`local`, `lite`, `cloud`), the endpoints table above, troubleshooting section (empty for now).
- **Done when:** PR with `deploy/README.md` and this plan file merged.

### P1. kind cluster, namespaces, shared config ★★

**Files:** `deploy/kind/cluster.yaml`, `deploy/kind/cluster-lite.yaml`, `deploy/common/namespaces.yaml`, `deploy/common/cryptopipe-env.yaml`, `deploy/scripts/{cluster-up.sh,cluster-down.sh,lib.sh}`, Makefile targets `cluster-up`, `cluster-down`.

1. `cluster.yaml`: kind cluster `crypto`, 1 control plane + 2 workers. `extraPortMappings` on the control plane for NodePorts: 30092 → 9092 (Kafka bootstrap), 30093 → 9093 (broker 0), 30094 → 9094 and 30095 → 9095 (spare for 3 brokers), 30300 → 3000 (Grafana, used by E), 30870 → 9870 (HDFS UI), 31808 → 18080 (History Server). Label the workers `crypto/role=worker`.
2. `cluster-lite.yaml`: 1 control plane + 1 worker for weak laptops (`PROFILE=lite`).
3. `namespaces.yaml`: `kafka`, `storage`, `spark`, `ingest`, `serving`, `monitoring`, each labelled `app.kubernetes.io/part-of=crypto-pipeline`.
4. `cryptopipe-env.yaml`: one ConfigMap `cryptopipe-env` **applied to the `ingest`, `spark` and `serving` namespaces** with the cluster values of the env vars defined in `config.py` (`CRYPTO_KAFKA_BOOTSTRAP`, `CRYPTO_LAKE_ROOT`, `CRYPTO_CHECKPOINT_ROOT`, `CRYPTO_CASSANDRA_HOSTS`, `CRYPTO_CASSANDRA_KEYSPACE`, `CRYPTO_LOG_LEVEL`). Use exactly the names in `config.py`; if a needed name is missing there, list it under "Contract change needed" instead of inventing one. Other members reference this ConfigMap with `envFrom`.
5. `cluster-up.sh` (idempotent; safe to rerun): check tools, create the cluster if it does not exist (`PROFILE=local|lite`), apply namespaces and the ConfigMap, add Helm repos, then call P2/P5 operator installs (added in later steps), then `deploy/monitoring/install.sh` if it exists. `set -euo pipefail`, coloured log helpers in `lib.sh`, `wait_for_rollout` helper.
6. `cluster-down.sh`: `kind delete cluster --name crypto` after a `--yes` flag or interactive confirmation.

**Done when:** `make cluster-up` twice in a row succeeds; `kubectl get nodes` shows 3 Ready nodes; `kubectl get cm cryptopipe-env -n spark -o yaml` shows the values.

### P2. Kafka with Strimzi ★★★

**Files:** `deploy/kafka/install-operator.sh`, `deploy/kafka/base/{kafka.yaml,nodepool.yaml,kustomization.yaml}`, `deploy/kafka/overlays/{local,lite,cloud}/`, `deploy/kafka/gen_topics.py`, `deploy/kafka/topics.generated.yaml`, `deploy/kafka/check.sh`.

1. Install the Strimzi operator with Helm (`oci://quay.io/strimzi-helm/strimzi-kafka-operator`), `watchNamespaces` limited to `kafka`. Pin the chart version in the script; use the newest release whose docs list KRaft node pools as the default. Use a Kafka version from that release's supported list.
2. `Kafka` CR named **`crypto`** in namespace `kafka` (gives bootstrap service `crypto-kafka-bootstrap`), KRaft mode, with `KafkaNodePool` `dual` (roles `controller` + `broker`).
   - `local`: 1 replica, 10 Gi PVC, JVM heap 512m–1g, `default.replication.factor=1`, `min.insync.replicas=1`, `offsets.topic.replication.factor=1`.
   - `cloud`: 3 replicas, RF 3, `min.insync.replicas=2`.
   - Listeners: `plain` 9092 internal; `external` type `nodeport`, bootstrap nodePort 30092, broker 0 nodePort 30093 with `advertisedHost: localhost` and `advertisedPort: 9093` (local only), so a producer on the laptop can reach `localhost:9092`.
   - `entityOperator` (topic + user operators) enabled; `kafkaExporter` enabled (topic and consumer-group regex `.*`) for E's lag dashboard.
3. `gen_topics.py`: imports `cryptopipe.contracts` (read only) and writes `topics.generated.yaml` with one `KafkaTopic` per topic: partitions and cleanup policy from the contract, `retention.ms` 7 days, `replicas` from a `--profile` flag. Commit the generated file and add a check (`gen_topics.py --check`) that fails if it is out of date. If `contracts.py` lacks partition or compaction info, use the values in `docs/contracts.md` and say so in the PR.
4. `check.sh`: list topics with partitions; produce a test message to `raw.trades`-independent topic `platform.healthcheck` **only if it exists in the contracts**; otherwise produce to and consume from a temporary topic `tmp.healthcheck` that the script creates and deletes. Run the Kafka CLI from a temporary pod using the Strimzi image.
5. Wait helpers: `kubectl wait kafka/crypto --for=condition=Ready -n kafka --timeout=600s`.

**Done when:** `deploy/kafka/check.sh` lists every contract topic with the right partitions, a message goes round trip, and `kcat -b localhost:9092 -L` (or a Python one-liner with confluent-kafka) works from the laptop.

**Journal (system-integration):** the internal vs external listener and advertised host problem.

### P3. HDFS on Kubernetes ★★★★★

**Files:** `deploy/hdfs/base/{configmap.yaml,namenode.yaml,datanode.yaml,services.yaml,init-lake-job.yaml,kustomization.yaml}`, `deploy/hdfs/overlays/{local,lite,cloud}/`, `deploy/hdfs/check.sh`, `deploy/hdfs/README.md`.

Image: `apache/hadoop:3.3.6` (or the newest 3.3.x/3.4.x tag available; the Hadoop client inside the Spark image must be compatible, so prefer 3.3.x to match Spark 3.5's bundled Hadoop 3.3). Check the image's user id with `docker run --rm apache/hadoop:3.3.6 id` and use it for `runAsUser` and `fsGroup`.

1. **ConfigMap `hdfs-config`** with:
   - `core-site.xml`: `fs.defaultFS=hdfs://hdfs-namenode.storage.svc:8020`, `hadoop.proxyuser.*` not needed yet.
   - `hdfs-site.xml`: `dfs.namenode.name.dir=/data/namenode`, `dfs.datanode.data.dir=/data/datanode`, `dfs.replication` (1 local / 2 lite / 3 cloud via overlay patch), `dfs.namenode.rpc-bind-host=0.0.0.0`, `dfs.namenode.http-bind-host=0.0.0.0`, `dfs.namenode.datanode.registration.ip-hostname-check=false`, `dfs.client.use.datanode.hostname=true`, `dfs.datanode.use.datanode.hostname=true`, `dfs.permissions.enabled=true`, `dfs.webhdfs.enabled=true`.
2. **NameNode**: StatefulSet `hdfs-namenode` (1 replica), PVC 10 Gi at `/data`, Service `hdfs-namenode` (ports 8020 RPC, 9870 HTTP) plus a NodePort 30870 for the UI.
   - Init container (same image): copy the ConfigMap files into an `emptyDir` used as `HADOOP_CONF_DIR`; **format only if `/data/namenode/current/VERSION` does not exist**: `hdfs namenode -format -nonInteractive -force -clusterId crypto-lake`. Never format when the file exists.
   - Main container: `hdfs namenode`; readiness probe HTTP `/` on 9870; resources request 512Mi, limit 1Gi; `HADOOP_HEAPSIZE_MAX=768m`.
3. **DataNodes**: StatefulSet `hdfs-datanode` (2 local, 1 lite, 3 cloud), headless Service `hdfs-datanode` (`clusterIP: None`, ports 9866 data, 9864 HTTP, 9867 IPC), `serviceName: hdfs-datanode`, PVC 20 Gi each.
   - Init container: copy config into the `emptyDir`, then insert a property `dfs.datanode.hostname=$(hostname).hdfs-datanode.storage.svc.cluster.local` into `hdfs-site.xml` (with `sed`), so each DataNode registers with a stable DNS name that Spark pods in other namespaces can resolve.
   - Init container waits for the NameNode RPC port (`nc -z hdfs-namenode.storage.svc 8020` loop).
   - Main container: `hdfs datanode`; readiness probe HTTP 9864; anti-affinity preferred across nodes.
4. **init-lake Job** (re-runnable): create `/lake` and its subfolders from the lake paths in `contracts.py` (generate the folder list with a small script like P2 or read `docs/contracts.md`), `/lake/_checkpoints`, `/spark-events`, `/user/spark`; set owner to the Spark user (`spark`, uid 185 in the apache/spark image) and permissions `775`. Use `hdfs dfs -mkdir -p` and `-chown`, run as the HDFS superuser.
5. `check.sh`: from a temporary pod **in the `spark` namespace** (this proves cross-namespace DNS works): `hdfs dfsadmin -report` (expect N live DataNodes), write a 50 MB file, read it back, compare checksums, delete it. Then delete one DataNode pod, wait for it to come back, and confirm the report shows the same DataNode hostnames.

**Done when:** `deploy/hdfs/check.sh` passes, including after `kubectl delete pod hdfs-datanode-0 -n storage` and after `kubectl delete pod hdfs-namenode-0 -n storage` (data still there, no re-format).

**Traps to watch and journal (storage, fault-tolerance):** DataNodes registered by pod IP; "could not obtain block" from clients that cannot resolve DataNode names; formatting twice and a cluster ID mismatch; PVC permission errors (fix with `fsGroup`).

### P4. Spark image ★★★

**Files:** `docker/spark/Dockerfile`, `docker/spark/jars.txt`, `docker/spark/spark-defaults.conf` (image-level defaults only), `docker/ingest/Dockerfile` (finalize the bootstrap draft), Makefile target `build-images`.

1. Base: the official `apache/spark` image, latest **3.5.x** tag with Scala 2.12, Java 17 and Python 3 (for example `apache/spark:3.5.x-scala2.12-java17-python3-ubuntu`; pick the exact tag and pin it).
2. **Check the Python version in the image** (`docker run --rm <image> python3 --version`). If it is not 3.11 while `pyproject.toml` requires 3.11, do not edit `pyproject.toml`: either install Python 3.11 in the image and set `PYSPARK_PYTHON`/`PYSPARK_DRIVER_PYTHON` to it, or report "Contract change needed: allow Python 3.10" in the PR. Driver and executors must use the same Python version as the package.
3. Download jars listed in `jars.txt` into `/opt/spark/jars` at build time (with checksums where Maven publishes them). All must be Scala 2.12 and match the pinned Spark version:
   - `org.apache.spark:spark-sql-kafka-0-10_2.12:<spark>`, `spark-token-provider-kafka-0-10_2.12:<spark>`, `org.apache.kafka:kafka-clients` (the version Spark 3.5 depends on), `org.apache.commons:commons-pool2`
   - `com.datastax.spark:spark-cassandra-connector-assembly_2.12:3.5.x`
   - GraphFrames for Spark 3.5 / Scala 2.12 (check whether the current release is on Maven Central under `io.graphframes` or on spark-packages).
4. Copy the `cryptopipe` wheel (built with `pip wheel . -w dist/` at build time in a builder stage) and install it with its runtime dependencies. Copy job entrypoints so they are at `local:///opt/cryptopipe/jobs/<name>.py` (read the package layout; if entrypoint files are missing, report it, do not create them under `src/`).
5. `HADOOP_CONF_DIR=/opt/hadoop-conf`; the directory is filled at runtime from the `hdfs-config` ConfigMap (mounted by the operator, P5). Image-level `spark-defaults.conf`: Prometheus servlet (`spark.ui.prometheus.enabled=true`, `spark.metrics.conf.*.sink.prometheusServlet.*`), `spark.eventLog.enabled=true`, `spark.eventLog.dir=hdfs://hdfs-namenode.storage.svc:8020/spark-events`, `spark.sql.session.timeZone=UTC`. Job-specific settings stay in the owners' SparkApplication YAMLs.
6. `make build-images`: build both images tagged with `$(git rev-parse --short HEAD)` and `latest`, then `kind load docker-image ... --name crypto`.
7. Image size: report it; remove apt caches and pip caches.

**Done when:** `docker run --rm cryptopipe-spark:latest /opt/spark/bin/spark-submit --version` works; a local `pyspark` run inside the image reads `--packages`-free Kafka and Cassandra classes (`spark._jvm.Class.forName(...)` check script `docker/spark/verify_jars.py`); `python -c "import cryptopipe"` works in both images.

### P5. Spark Operator, RBAC, sample job, History Server ★★★★

**Files:** `deploy/spark/install-operator.sh`, `deploy/spark/operator-values.yaml`, `deploy/spark/rbac.yaml`, `deploy/spark/hadoop-conf.yaml`, `deploy/spark/examples/{hdfs-check.py,hdfs-check.yaml,README.md}`, `deploy/spark/history-server.yaml`.

1. Install the **kubeflow spark-operator** chart (`https://kubeflow.github.io/spark-operator`), pinned version, `spark.jobNamespaces: [spark]`, webhook enabled (needed for volumes and ConfigMaps on driver/executor pods), operator metrics enabled.
2. `rbac.yaml`: ServiceAccount `spark` in `spark` namespace with a Role allowing pods, services, configmaps and PVCs (as in the operator docs). Skip if the chart already creates an equivalent account; use one name consistently and write it in `deploy/README.md`.
3. `hadoop-conf.yaml`: a copy of the HDFS `core-site.xml`/`hdfs-site.xml` as ConfigMap `hadoop-conf` in the `spark` namespace (generated from the P3 ConfigMap by the deploy script, not by hand), mounted at `/opt/hadoop-conf` via `spec.hadoopConfigMap` or a volume.
4. **Sample job** `examples/hdfs-check.py` (lives under `deploy/`, not `src/`): creates a small DataFrame, writes Parquet to `${CRYPTO_LAKE_ROOT}/_platform_check/<timestamp>`, reads it back, prints the count, and optionally reads one message from Kafka with the batch Kafka source to prove the jar works. `examples/hdfs-check.yaml`: `SparkApplication` v1beta2, `type: Python`, `mode: cluster`, image `cryptopipe-spark:latest`, `imagePullPolicy: IfNotPresent`, `envFrom` the `cryptopipe-env` ConfigMap, driver 1 core / 1 Gi, 1 executor 1 core / 1 Gi, `restartPolicy: Never`, Prometheus annotations. `examples/README.md` explains which fields C and D change when they copy it (name, mainApplicationFile, arguments, resources, restartPolicy, `ScheduledSparkApplication` wrapper for batch).
5. **History Server**: Deployment running the same image with `/opt/spark/bin/spark-class org.apache.spark.deploy.history.HistoryServer`, `spark.history.fs.logDirectory=hdfs://hdfs-namenode.storage.svc:8020/spark-events`, Service with NodePort 31808.
6. `deploy/spark/check.sh`: submit the sample, wait for `COMPLETED`, print driver log tail, confirm the run appears in the History Server API (`/api/v1/applications`).

**Done when:** `deploy/spark/check.sh` passes on a fresh cluster.

**Traps (spark-processing, system-integration journals):** driver cannot resolve HDFS (missing `HADOOP_CONF_DIR`); jar conflicts (`NoSuchMethodError`); executors pending because kind nodes are out of memory; webhook not ready so volumes are silently ignored.

### P6. One-command deploy and smoke test ★★

**Files:** `deploy/scripts/{deploy-all.sh,smoke-test.sh,port-forward.sh}`, Makefile targets `deploy`, `smoke`, `port-forward`.

1. `deploy-all.sh PROFILE=local|lite|cloud`: apply in order with waits: shared config → Kafka (+ generated topics) → HDFS (+ init-lake Job) → `hadoop-conf` → Spark RBAC + History Server → then, **only if present**, `deploy/cassandra/install.sh` (E), `deploy/monitoring/install.sh` (E), `deploy/ingest/install.sh` (B), and `kubectl apply` of `deploy/spark/stream-*.yaml` (C) and `deploy/spark/batch-*.yaml` (D) behind a flag `WITH_JOBS=1`. Print what was skipped.
2. `smoke-test.sh`: a table of checks with PASS/FAIL/SKIP: nodes Ready; pods Ready per namespace; Kafka topics vs contracts; message counts per topic (offsets via the Kafka CLI pod); HDFS live DataNodes and `hdfs dfs -du -h /lake`; Spark sample job; Cassandra `select count(*) from crypto.ohlcv_1m` if Cassandra exists (SKIP otherwise); Grafana reachable if monitoring exists. Exit non-zero on any FAIL.
3. `port-forward.sh`: NameNode UI, Spark UI of a running driver (by label), History Server, Kafka (if NodePort fails), Grafana (if present).

**Done when:** on a second laptop (ask a teammate), `make cluster-up build-images deploy smoke` from a fresh clone ends with no FAIL.

### P7. Documentation, ADRs, journal for the checkpoint ★★

- `deploy/README.md`: final version with the endpoints table, how others use the cluster (envFrom `cryptopipe-env`, copying the Spark example, `kind load` for their images), resource sizing, profile differences, and troubleshooting from the problems actually hit.
- `docs/adr/0002-kubernetes-instead-of-docker-compose.md` and `docs/adr/0003-hdfs-instead-of-minio.md` (context, options, decision, consequences). Check the ADR numbering in `docs/adr/` first and take the next free numbers; do not renumber others.
- Write the **deployment section text** in a new file `docs/sections/deployment.md` (E assembles it into `docs/architecture.md`; do not edit that file): a Mermaid diagram of namespaces, StatefulSets, Services, PVCs and NodePorts, and a table of resource requests.
- Journal: at least 2 entries (likely HDFS DataNode hostnames, Kafka advertised listeners, or Spark jar conflicts), in the course's lesson format with real error messages and timings.
- **Done when:** PR merged and E confirms the deployment section is usable.

### P8. Support integration day ★

- On integration day, run `make smoke` with everyone's components; collect failures into a checklist issue and assign each item to its owner. Fix only platform items yourself.
- Tag the cluster manifest versions used for the checkpoint demo in `docs/checkpoint-1.md` through a comment to E (E owns that file).

---

## 4. Order and timing for the checkpoint week

| Day | Steps | Unblocks |
|---|---|---|
| 1 | P0, P1, P2 | B can produce to Kafka from the laptop |
| 2 | P3 (HDFS) | |
| 3 | P3 finish, P4 | C and D can write to HDFS with local Spark through the port-forward, or wait for P5 |
| 4 | P5, P6 | C and D can run their jobs on the cluster |
| 5 | P8 integration | everyone |
| 6 | P7, fixes | |
| 7 | Rehearsal | |

If P3 stalls by the end of day 3, use the fallback below so the team is not blocked, and keep working on HDFS in parallel.

**Fallback (only if needed, and say so in the PR and ADR):** deploy MinIO as a temporary S3-compatible store with `CRYPTO_LAKE_ROOT=s3a://lake` in the shared ConfigMap, plus the `hadoop-aws` jar in the Spark image. Switch back to HDFS when P3 passes. This changes only A's files and the ConfigMap values, not the contracts.

---

## 4b. Fast track: finish the checkpoint work in one or two sessions

Use this instead of the day-by-day table when you want to get as far as possible, as fast as possible. The section 1 rules still apply in full.

### Prompt to start Claude Code

> Read docs/plans/member-a-platform.md (all of it) and CLAUDE.md. Work in **fast-track mode (section 4b)**: execute steps P0 to P6 continuously without asking me between steps. Follow the section 1 boundaries strictly. Use the 3 branches/PRs listed in 4b. After each step, run its Done-when checks and commit. Stop and ask me only when: (a) a command is destructive (format, delete PVCs or topics, delete the cluster), (b) a contract change is needed, (c) you need to install software on my machine, or (d) the same error survives 3 different fix attempts. When you finish, or stop, print a summary: what passed, what failed, what is left.

### How the fast track saves time

1. **3 PRs instead of 8**
   - PR 1 `a/fast-1-cluster-kafka`: P0 + P1 + P2 (cluster, namespaces, shared ConfigMap, Kafka, topics).
   - PR 2 `a/fast-2-hdfs`: P3.
   - PR 3 `a/fast-3-spark-deploy`: P4 + P5 + P6 (images, operator, sample job, History Server, deploy and smoke scripts).
   - P7 docs go in a final small PR; open PR 1 as soon as it passes so B can start producing to Kafka while you continue.
2. **Pre-pull everything first.** At the start, `docker pull` the kind node image, Strimzi operator and Kafka images, `apache/hadoop`, `apache/spark` and the spark-operator image in the background, then `kind load` them. Image downloads are usually the slowest part.
3. **Work while things roll out.** While waiting for a rollout (Kafka can take several minutes), Claude Code writes the next files: the Spark Dockerfile while HDFS starts, the README and ADR drafts while images build. It may use subagents for writing files, but only one agent runs `kubectl` or `helm` at a time.
4. **Local profile only for the checkpoint.** Write the `lite` and `cloud` overlays as small stubs with TODO notes; the full versions come after the checkpoint (Q2).
5. **Timeboxes.** Kafka: if not Ready after 2 fix attempts, simplify (drop the external NodePort listener, use `kubectl port-forward` and document it). HDFS: if `check.sh` still fails after about 3 hours of attempts, switch to the MinIO fallback from section 4 for now, open PR 2 as "HDFS work in progress", and continue with P4–P6 on MinIO so the team is unblocked.
6. **Optional parts deferred.** If time is short, skip the History Server (keep event logs on HDFS so it can be added later) and the second smoke-test laptop run; do everything else.
7. **Don't wait on the bootstrap.** If `contracts.py` is not merged yet, `gen_topics.py` reads a temporary `deploy/kafka/topics.seed.yaml` copied from `docs/contracts.md` (or from section S3 of the bootstrap plan) and prints a warning. Once `contracts.py` exists, regenerate and delete the seed file in the same PR. The same applies to the HDFS lake folders.

### Minimum to show at the checkpoint (if the fast track stops early)

In priority order: (1) `make cluster-up` + Kafka with all topics reachable from the laptop; (2) HDFS (or the MinIO fallback) reachable from the `spark` namespace; (3) the Spark sample job writing Parquet to the lake on the cluster; (4) `make smoke`. Items 1–3 are enough for B, C and D to integrate.

### The rest of the team can compress too

Every member's tasks are independent once the contracts are fixed, so the whole checkpoint can fit in about 3 days instead of 7:
- **Day 1:** E runs the bootstrap in the morning, the team reviews contracts for 30 minutes, then all five run their own fast-track sessions in parallel (you on P0–P6).
- **Day 2:** integration on your cluster (P8), D's full data load, fixes.
- **Day 3:** docs (P7 and the others' sections) and a demo rehearsal from a clean cluster.

---

## 5. After the checkpoint (same rules from section 1 apply)

| Step | Work | Difficulty |
|---|---|---|
| Q1. Security | Strimzi listeners with TLS + SCRAM-SHA-512; `KafkaUser` per component (B's producers, C's and D's jobs) with ACLs limited to their topics; Secrets mounted for clients (coordinate the env var names through a contract PR); K8s RBAC least privilege; NetworkPolicies per namespace (default deny + needed flows); HDFS permissions per user; K8s audit logging on the cloud cluster. Journal: security. | ★★★★ |
| Q2. Cloud cluster | Managed cluster (GKE/EKS/AKS), `cloud` overlays: 3 Kafka brokers RF 3 `min.insync.replicas=2`, 3 DataNodes `dfs.replication=3`, storage classes for SSD PVCs, images pushed to a registry (GHCR/Artifact Registry), budget alerts, a `make cloud-up/cloud-down` that scales node pools to zero between sessions. | ★★★★ |
| Q3. Fault-tolerance drills | Scripted experiments with timings: kill a Kafka broker during load (no data loss with `acks=all`), kill a DataNode (re-replication time), kill the NameNode pod (recovery, no format), kill a Cassandra node with E, kill a streaming driver with C. Backup and restore: HDFS snapshots of `/lake/silver`, Kafka topic config export, restore drill. Journal: fault-tolerance. | ★★★ |
| Q4. Scaling and cost | With D's replayer: throughput vs Kafka partitions and executor count; Spark dynamic allocation on K8s (`spark.dynamicAllocation.shuffleTracking.enabled`); HPA for B's producers or a KEDA autoscaler on consumer lag; vertical vs horizontal node sizes; cost per hour per profile. Journal: scaling. | ★★★ |
| Q5. Platform hardening | Pod disruption budgets, resource requests tuned from Prometheus data, liveness/readiness reviewed, Helm chart versions pinned and documented, a `make upgrade` path. | ★★ |
| Q6. Report | Deployment strategy, environment configuration (local vs cloud overlays), and the lessons for security, scaling, fault tolerance and system integration. | ★★ |
