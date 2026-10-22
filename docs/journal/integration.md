# Integration

Scope: making the layers talk to each other on the cluster (DNS, jars, configs). Main contributors: all. Template: [README.md](README.md).

<!-- Add entries below, newest last. -->

## 2026-10-20: local Spark on Windows needs winutils, and Python workers are slow to spawn

* **Context / symptom:** `df.write.parquet(...)` failed with `HADOOP_HOME and hadoop.home.dir are unset`. After
  fixing that, each tiny unit test that used `spark.createDataFrame([...])` took ~19 s.
* **What we tried:** `scripts/setup_winutils.py` downloads `winutils.exe` + `hadoop.dll` into `.tools/hadoop`,
  which `get_spark(local=True)` picks up. Timing showed `createDataFrame` from a Python list starts one Python
  worker per partition; on this laptop (12 cores, venv inside OneDrive) each spawn costs ~0.8 s and workers are
  not reused.
* **Numbers:** one small query: 10 s with `local[*]` (12 partitions), 3.4 s with `spark.default.parallelism=4`,
  0.9 s with 1. Full unit suite: 203 s -> 37 s.
* **Fix / decision:** the test `spark` fixture sets `spark.default.parallelism=1`; `get_spark` accepts `conf`
  overrides. Linux, CI and the Docker images need none of this.
* **Lesson:** measure where local test time goes before blaming Spark; most of it was process startup.

## 2026-10-20: Hadoop does not decode %20 in file: URIs

* **Context / symptom:** `spark.read.text(Path(...).as_uri())` failed with `PATH_NOT_FOUND .../OneDrive%20-%20...`.
* **Fix / decision:** `common.spark.to_spark_path()` builds `file:///D:/One Drive/...` with literal spaces; URIs
  (`hdfs://`) pass through unchanged. Use it for every local path given to Spark.
* **Lesson:** Hadoop paths are not standard URIs; never percent-encode them.

## 2026-10-22: Kafka on kind is reachable from pods but not from a Windows laptop (advertised listener)

* **Context / symptom:** Strimzi `Kafka` CR `crypto` with an `external` NodePort listener (bootstrap 30092 → host
  9092, broker 0 30093 → host 9093, `advertisedHost: localhost`). In-cluster clients worked. From the Windows
  laptop, `confluent_kafka` failed with `Failed to get metadata: Local: Broker transport failure` and produces
  timed out (`_MSG_TIMED_OUT`).
* **What we tried:** (1) `kafka-broker-api-versions.sh` from a pod against the external bootstrap Service and
  against `<node IP>:30092`: both returned metadata advertising `localhost:9093`, so the listener and NodePort
  were right. (2) The same from a container on the `kind` Docker network: OK. (3) Python socket test from Windows:
  `127.0.0.1:9092` open, `::1:9092` timed out. (4) librdkafka with `broker.address.family=v4`: metadata in 1 ms.
* **Numbers:** `localhost` bootstrap: no metadata after 15 s; `127.0.0.1` bootstrap: ApiVersion rtt 2.2 ms,
  Metadata rtt 0.9 ms. `deploy/kafka/check.sh` (6 topics + round trip) runs in ~4 min, mostly throwaway-pod start.
* **Fix / decision:** advertise `127.0.0.1:9093` instead of `localhost` (`deploy/kafka/base/kafka.yaml`), and use
  `CRYPTO_KAFKA_BOOTSTRAP=127.0.0.1:9092` in laptop `.env` files (`deploy/README.md`).
* **Lesson:** a Kafka client only uses the bootstrap address for the first metadata call; every later request
  goes to the advertised broker address, so that address must be reachable *from the client's network*. Test
  each hop separately (pod, Docker network, host) and prefer literal IPv4 over `localhost` on Windows.

## 2026-10-22: Windows tooling traps when scripting kind (paths, image loading, operator leases)

* **Context / symptom:** `make cluster-up` from Git Bash: (1) `helm` failed with `open /d/OneDrive - .../operator-values.yaml:
  The system cannot find the path specified`; (2) `kind load docker-image` failed with `ctr: content digest
  sha256:f0a2...: not found`; (3) the Strimzi operator restarted 6 times in 34 min (`Stopped being a leader =>
  exiting`) and `kubectl wait kafka/crypto --for=condition=Ready` timed out after 600 s, while Kafka itself ran.
* **What we tried:** (1) native Windows binaries do not understand MSYS paths; `pwd -W` gives `D:/...`, which both
  work with. (2) `docker save` to a temp file + `kind load image-archive` worked but filled the 0.4 GB left on C:;
  streaming `docker save --platform linux/amd64 | docker exec -i <node> ctr images import -` needs no disk.
  (3) the operator log showed a 409 on the lease PATCH, then exit; one replica needs no leader election.
* **Numbers:** first `cluster-up` 15 min (kind node image + operator image pulled inside kind); rerun 1m45s;
  streaming an 800 MB image into 3 nodes ~7 min. After disabling leader election: Kafka CR Ready in ~2 min, 0 restarts.
* **Fix / decision:** `REPO_ROOT` via `pwd -W`, `MSYS_NO_PATHCONV=1`, `kind_load` helper (`deploy/scripts/lib.sh`);
  `leaderElection.enable=false` and 1 Gi limit for the operator (`deploy/kafka/install-operator.sh`).
* **Lesson:** on a laptop cluster, single-replica controllers should not depend on lease renewals, and every
  script that mixes POSIX shells with native tools needs one place that normalises paths.
