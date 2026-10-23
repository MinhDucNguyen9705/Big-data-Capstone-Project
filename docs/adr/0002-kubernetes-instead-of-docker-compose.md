# ADR 0002: Kubernetes (kind locally) instead of Docker Compose

* Status: accepted (2026-10-22)
* Deciders: A (Platform), reviewed by the team
* Related: [deploy/README.md](../../deploy/README.md), [plans/member-a-platform.md](../plans/member-a-platform.md),
  [journal/integration.md](../journal/integration.md), ADR 0003

## Context

The pipeline runs Kafka, HDFS, Spark (streaming and batch), Cassandra, producers and monitoring. The course
grades deployment strategy, environment configuration (local vs cloud), scaling and fault tolerance. Five team
members must run the same stack on their laptops (Windows, macOS, Linux; 8 to 32 GB RAM) and later on a managed
cloud cluster. Spark jobs are owned by different members and must be submitted, restarted and observed the same
way.

## Options considered

1. **Docker Compose**: one YAML, fast start, low overhead (~1 GB less RAM than kind). But Spark runs in
   standalone mode, there is no operator for Kafka topics or Spark jobs, restarts and scaling are manual, and
   nothing carries over to the cloud deployment: we would write the deployment twice.
2. **Kubernetes with kind locally, a managed cluster in the cloud**: the same manifests in both places
   (kustomize overlays `local`, `lite`, `cloud`); operators do the hard parts: Strimzi (KRaft Kafka, topics as
   `KafkaTopic` CRs generated from `contracts.py`), kubeflow spark-operator (`SparkApplication`,
   `ScheduledSparkApplication`, restart policies), StatefulSets with PVCs for HDFS and Cassandra.
3. **minikube / k3d**: similar to kind; kind won because multi-node clusters and `extraPortMappings` (NodePorts
   on fixed laptop ports) are declarative in one file, and CI images exist for it.

## Decision

Kubernetes everywhere: kind for laptops (`make cluster-up`), the same manifests with a `cloud` overlay for the
managed cluster. Namespaces per layer (`kafka`, `storage`, `spark`, `ingest`, `serving`, `monitoring`), one shared
`cryptopipe-env` ConfigMap with the contract's env vars, operators for Kafka and Spark.

## Consequences

* + Fault-tolerance and scaling experiments are real: delete a pod, scale a StatefulSet, change executor counts.
* + Kafka topics and lake folders are generated from the contracts, so the cluster cannot drift from the code.
* + One submission path for every Spark job; finished runs in the History Server.
* - Heavier on laptops: the local profile needs ~12 GB for Docker; `lite` exists for 8 GB machines.
* - More moving parts on Windows (path conversion, image loading, IPv6 `localhost`), documented in
  `deploy/README.md` and the integration journal.
* - Slower first start (~15 min, mostly image pulls), ~2 min afterwards.
* Follow-ups: Q2 cloud overlays and registry; Q5 PodDisruptionBudgets and tuned requests.
