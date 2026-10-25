# Getting started: run crypto-stream-lake from zero

This guide starts from a laptop with nothing installed and covers **Windows 10/11**, **macOS** and **Linux**
(Ubuntu/Debian commands; other distributions have the same packages under similar names). Where the steps
differ, each OS has its own block. It has two parts:

| Part | What you get | Needs | Time (first run) |
|---|---|---|---|
| **A. Local mode** | Unit tests and the walking-skeleton demo in local Spark (fixture trades → 1-minute candles as Parquet). No cluster. | Python 3.11, Java 17, Git | ~20 min |
| **B. Full pipeline** | Kafka, HDFS, Spark, Cassandra, Prometheus and Grafana on a local Kubernetes (kind) cluster, fed by live Binance/Coinbase data. | Part A + Docker, kind, kubectl, helm, make, **≥ 12 GB RAM for Docker** | 1–1.5 h (mostly image downloads) |

Do Part A first even if you only want Part B: the cluster scripts use the Python virtual environment it creates.

---

## 0. Hardware check

| | Part A | Part B, `PROFILE=lite` | Part B, `PROFILE=local` (default) |
|---|---|---|---|
| RAM in the laptop | 8 GB | 16 GB | 16 GB+ (Docker gets 12 GB) |
| Free disk | 5 GB | 40 GB | 50 GB+ (images ~10 GB, volumes, archive data) |
| CPU | any | 4 cores, **x86-64** | 6+ cores, **x86-64** |

Put the free disk where Docker keeps its data (Windows: by default `C:`; Linux: `/var/lib/docker`). A nearly
full disk makes image loads into kind fail.

**Apple Silicon (M1–M4) and other ARM machines:** Part A works. Part B is built and tested for x86-64 only:
the Spark image and `kind_load` in `deploy/scripts/lib.sh` are pinned to `linux/amd64`, and the HDFS image
`apache/hadoop:3.3.6` is amd64-only. On an ARM Mac these run under emulation at best, which is slow and
untested; use an x86-64 machine for the cluster if you can.

---

## 1. Install the tools

### Windows

Open **PowerShell** (not as administrator unless a step asks) and install everything with `winget`:

```powershell
# Part A
winget install -e --id Git.Git
winget install -e --id Python.Python.3.11
winget install -e --id EclipseAdoptium.Temurin.17.JDK

# Part B (skip if you only want local mode)
winget install -e --id Docker.DockerDesktop
winget install -e --id Kubernetes.kind
winget install -e --id Kubernetes.kubectl
winget install -e --id Helm.Helm
winget install -e --id ezwinports.make
```

**Close every terminal and open a new one** so the new `PATH` is picked up. If Spark later says it cannot find
Java, set `JAVA_HOME` once (adjust the folder name to what Temurin installed):

```powershell
setx JAVA_HOME "C:\Program Files\Eclipse Adoptium\jdk-17.0.x-hotspot"
```

### macOS

Install the Xcode command line tools (they provide `git` and GNU `make`) and [Homebrew](https://brew.sh), then:

```bash
xcode-select --install

# Part A
brew install python@3.11 openjdk@17

# Part B (skip if you only want local mode)
brew install --cask docker              # Docker Desktop; start it once from Applications
brew install kind kubernetes-cli helm
```

Homebrew's JDK is not on the default path. Add these lines to `~/.zshrc` and open a new terminal:

```bash
export JAVA_HOME="$(brew --prefix openjdk@17)/libexec/openjdk.jdk/Contents/Home"
export PATH="$JAVA_HOME/bin:$PATH"
```

### Linux (Ubuntu/Debian)

```bash
# Part A
sudo apt update
sudo apt install -y git make curl openjdk-17-jdk
sudo apt install -y python3.11 python3.11-venv     # Debian 12, Ubuntu 22.04
# Ubuntu 24.04 ships Python 3.12 only; add the deadsnakes PPA first, then run the line above:
#   sudo add-apt-repository -y ppa:deadsnakes/ppa && sudo apt update

# Part B (skip if you only want local mode)
curl -fsSL https://get.docker.com | sh               # Docker Engine
sudo usermod -aG docker "$USER"                      # docker without sudo; log out and back in afterwards
curl -Lo kind https://kind.sigs.k8s.io/dl/v0.27.0/kind-linux-amd64
sudo install -m 0755 kind /usr/local/bin/kind && rm kind
curl -LO "https://dl.k8s.io/release/$(curl -Ls https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
sudo install -m 0755 kubectl /usr/local/bin/kubectl && rm kubectl
curl -fsSL https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash
```

kind runs several Kubernetes nodes as containers, which needs more inotify watches than the defaults allow.
Raise them (the second command keeps the setting after a reboot):

```bash
sudo sysctl fs.inotify.max_user_watches=524288 fs.inotify.max_user_instances=512
printf 'fs.inotify.max_user_watches=524288\nfs.inotify.max_user_instances=512\n' | sudo tee /etc/sysctl.d/99-kind.conf
```

### Check the tools (every OS)

```bash
git --version              # any recent version
python3.11 --version       # Windows: py -3.11 --version. Exactly 3.11: the package refuses 3.12+
java -version              # openjdk 17.x
docker --version           # Part B
docker run --rm hello-world    # Part B: Docker works for your user
kind version               # Part B, >= 0.27
kubectl version --client   # Part B, >= 1.31
helm version               # Part B, >= 3.14
make --version             # Part B, GNU Make
```

### Which terminal to use

* **Windows:** **PowerShell** for Part A. **Git Bash** (installed with Git; Start menu → "Git Bash") for every
  `make` and `bash deploy/...` command in Part B, because the deploy scripts are bash scripts.
* **macOS / Linux:** your normal terminal for everything. Where a step shows a `powershell` block, the bash
  block next to it is the one for you.

---

## 2. Get the code

Clone into a short path. On Windows, stay **outside OneDrive** if you can (OneDrive tries to sync `.venv`,
Spark output and downloaded data, which is slow and can lock files):

```powershell
# Windows
mkdir D:\dev -Force
cd D:\dev
git clone https://github.com/MinhDucNguyen9705/Big-data-Capstone-Project.git crypto-stream-lake
cd crypto-stream-lake
```

```bash
# macOS / Linux
mkdir -p ~/dev && cd ~/dev
git clone https://github.com/MinhDucNguyen9705/Big-data-Capstone-Project.git crypto-stream-lake
cd crypto-stream-lake
```

On Windows, paths with spaces still work; you will just see harmless `'-' is not recognized` /
`Missing Python executable` lines when Spark starts.

---

# Part A: local mode (no cluster)

## A1. Create the virtual environment and install the package

From the repo root.

Windows (PowerShell):

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -e ".[dev]"
```

If activation fails with "running scripts is disabled on this system", run once:
`Set-ExecutionPolicy -Scope CurrentUser RemoteSigned`, then activate again.

macOS / Linux:

```bash
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -e ".[dev]"
```

Equivalent on any OS with GNU make: `make install` (from Git Bash on Windows). The install downloads PySpark
(~320 MB), so it takes a few minutes.

Activate the environment again in every new terminal: `.\.venv\Scripts\Activate.ps1` (PowerShell),
`source .venv/Scripts/activate` (Git Bash) or `source .venv/bin/activate` (macOS/Linux). The rest of this guide
assumes it is active.

## A2. Windows only: winutils for Spark

Spark on Windows needs `winutils.exe` and `hadoop.dll` to write files. Download them once into `.tools/hadoop`
(the code finds them there automatically). macOS and Linux skip this step.

```powershell
python scripts/setup_winutils.py
```

## A3. Optional: a `.env` file

Every setting has a local default, so this is optional for Part A. To see or change them:

```powershell
Copy-Item .env.example .env      # Windows
```

```bash
cp .env.example .env             # macOS / Linux
```

All settings are `CRYPTO_*` environment variables (Kafka bootstrap, lake root, Cassandra hosts, ...), listed with
comments in `.env.example` and defined in `src/cryptopipe/config.py`.

## A4. Check the install

```powershell
crypto --help              # all commands
crypto contracts show      # topics, lake paths, Cassandra tables, active settings
crypto job list            # the Spark jobs
```

## A5. Run the tests

```powershell
python -m pytest -m "not integration"      # same as `make test`
ruff check . ; ruff format --check .       # same as `make lint`
```

Expect roughly 160 passed in under a minute. Integration tests are skipped; they need the cluster.

## A6. Run the demo

```powershell
crypto demo local
```

This runs the 500 fixture trades through parse → validate → dedupe → 1-minute OHLCV in a local SparkSession and
prints a summary: **500 rows in, 3 invalid, 5 duplicates removed, 60 candles out**. The Parquet output goes under
`CRYPTO_LAKE_ROOT` (default `file:///tmp/crypto-lake`: `/tmp/crypto-lake` on macOS/Linux, `\tmp\crypto-lake` on
the current drive on Windows).

## A7. Optional: the speed-layer jobs on files

The real streaming jobs can read a file instead of Kafka and write Parquet instead of Cassandra:

```powershell
# Windows (PowerShell)
$env:CRYPTO_LAKE_ROOT = "file:///tmp/crypto-speed-demo"
$env:CRYPTO_CHECKPOINT_ROOT = "file:///tmp/crypto-speed-demo/_checkpoints"
```

```bash
# macOS / Linux
export CRYPTO_LAKE_ROOT=file:///tmp/crypto-speed-demo
export CRYPTO_CHECKPOINT_ROOT=file:///tmp/crypto-speed-demo/_checkpoints
```

Then, on every OS:

```bash
crypto job run stream_bronze --local --source tests/fixtures/trades_normalized.jsonl --topic raw.trades --once
crypto job run stream_ohlcv  --local --source tests/fixtures/trades_normalized.jsonl --sink parquet --once
```

`--once` drains the file (Spark `AvailableNow` trigger) and exits. Output lands under `CRYPTO_LAKE_ROOT`: bronze
Parquet, quarantined bad trades, candle updates under `_preview/ohlcv_updates` and finalized candles in gold.
Other flags (`--trigger`, `--dedupe-watermark`, ...) are defined in `src/cryptopipe/jobs/streaming.py` and each
job's `main()`.

**Part A is done.** Everything below needs Docker.

---

# Part B: the full pipeline on Kubernetes (kind)

What gets deployed (one namespace per layer):

| Namespace | Components |
|---|---|
| `kafka` | Strimzi operator, Kafka `crypto` (KRaft, 1 node), 6 topics from `contracts.TOPICS` |
| `storage` | HDFS (1 NameNode, 2 DataNodes), Cassandra 5.0 (1 node) |
| `spark` | spark-operator, Spark History Server, the streaming and batch jobs |
| `monitoring` | Prometheus + Grafana with the market and pipeline dashboards |
| `ingest` | reserved for the producers (for now they run on your laptop, step B7) |

## B1. Configure Docker

**Windows (Docker Desktop):**

1. Start Docker Desktop once and accept the terms. Keep the default **WSL 2** backend.
2. Give the Docker VM enough memory. With WSL 2 the limit is set in `%UserProfile%\.wslconfig` (create it):

   ```ini
   [wsl2]
   memory=12GB
   processors=6
   swap=4GB
   ```

   Then in PowerShell run `wsl --shutdown` and restart Docker Desktop. (With the Hyper-V backend, use
   Settings → Resources instead.) 8 GB is only enough for `PROFILE=lite`.
3. Check: `docker info` shows "Total Memory" around 12 GiB, and `docker run --rm hello-world` works.

**macOS (Docker Desktop):** start Docker Desktop from Applications and accept the terms. In Settings →
Resources set Memory to 12 GB (8 GB only for `PROFILE=lite`), CPUs to 6 and the disk limit to 60 GB or more,
then Apply & restart. Check with `docker info | grep -i memory` and `docker run --rm hello-world`.

**Linux (Docker Engine):** nothing to size. Containers use the host's memory directly, so the laptop needs the
RAM from step 0 free. Make sure the daemon runs and your user is in the `docker` group (step 1; log out and
back in after `usermod`):

```bash
sudo systemctl enable --now docker
docker run --rm hello-world          # must work without sudo
```

## B2. Create the cluster and install the operators

Open a bash terminal in the repo root: on Windows **Git Bash** (right-click the folder → "Open Git Bash here",
or `cd /d/dev/crypto-stream-lake`), on macOS/Linux your normal terminal (`cd ~/dev/crypto-stream-lake`).
Make sure Part A's `.venv` exists; the scripts find it on their own.

```bash
make cluster-up                 # or: make cluster-up PROFILE=lite   (smaller laptops)
```

This creates the kind cluster `crypto` (1 control plane + 2 workers), the namespaces, the shared
`cryptopipe-env` ConfigMap, the Strimzi and spark-operator Helm releases, and Prometheus/Grafana.
First run: about 15 minutes (it downloads the Kubernetes node image and operator images). Reruns take ~2 minutes
and are safe.

Check:

```bash
kubectl get nodes                       # 3 nodes Ready (2 with lite)
kubectl get pods -A                     # operators Running in kafka, spark, monitoring
```

Use the **same `PROFILE` value for every make command** in Part B.

## B3. Build the application images

```bash
make build-images
```

Builds `cryptopipe-spark` (Spark 3.5.9 + Python 3.11 + Kafka/Cassandra connector jars + the package) and
`cryptopipe-ingest` (producers), tags each `:<git-sha>` and `:latest`, and copies them into every kind node.
First build: 15–20 minutes. Do not run it while `make deploy` is pulling images; both hammer the disk and the
control plane can stall (see Troubleshooting).

The streaming manifests use the tag `:dev`, so add that tag and load it too:

```bash
docker tag cryptopipe-spark:latest cryptopipe-spark:dev
bash -c '. deploy/scripts/lib.sh; kind_load cryptopipe-spark:dev'
```

Repeat these three commands whenever you change code under `src/`.

## B4. Deploy the platform

```bash
make deploy
```

In order, waiting for each: Kafka and its topics, HDFS and the lake folders (`hdfs-init-lake` Job), the Spark
History Server, Cassandra and its schema (from `cql/schema.cql`), monitoring. First run 20+ minutes (the Hadoop
image is ~1 GB per node); later runs ~6 minutes.

Check:

```bash
make smoke SKIP_SPARK=1      # PASS/FAIL/SKIP per check; drop SKIP_SPARK=1 to also run a ~3 min sample Spark job
```

At this stage the Kafka message counts are 0 and Cassandra is empty. That is expected.

## B5. Start the Spark jobs

Apply the streaming jobs' config and service account first, then the jobs:

```bash
kubectl apply -f deploy/spark/stream-rbac.yaml -f deploy/spark/stream-config.yaml
kubectl apply -f deploy/spark/stream-bronze.yaml -f deploy/spark/stream-ohlcv.yaml
kubectl apply -f deploy/spark/batch-silver.yaml          # hourly batch layer refresh at :20 UTC
kubectl -n spark get sparkapplications,scheduledsparkapplications
```

Wait until `stream-bronze` and `stream-ohlcv` show `RUNNING` (1–3 minutes). Follow a driver's log:

```bash
kubectl -n spark logs -f stream-ohlcv-driver
```

* `stream_bronze`: Kafka `raw.trades` / `raw.klines` → HDFS bronze Parquet (every raw record, for the batch layer).
* `stream_ohlcv`: Kafka `raw.trades` → validate → dedupe → live 1-minute candles and latest prices in Cassandra,
  finalized candles in HDFS gold.
* `batch-silver`: bronze → silver tables every hour; prints "nothing to do" while bronze is empty.

(`make deploy WITH_JOBS=1` also applies these files, but in alphabetical order, so the jobs can start before
their ServiceAccount exists and need a retry. Applying them by hand as above avoids that.)

## B6. Point your laptop at the cluster

Kafka is reachable from the laptop on `127.0.0.1:9092`. Use the literal IP, not `localhost` (on Windows
`localhost` resolves to IPv6 first and the connection hangs). Edit `.env` in the repo root (create it from
`.env.example` if needed):

```ini
CRYPTO_KAFKA_BOOTSTRAP=127.0.0.1:9092
```

## B7. Start the producers (live data)

Each WebSocket producer runs until you press Ctrl+C, so use one terminal per producer, each with the venv
activated (step A1). Try a dry run first; it prints validated messages without sending them:

```bash
crypto ingest binance --dry-run --symbols BTC-USDT     # Ctrl+C after a few lines
```

Then start them for real.

Windows (PowerShell):

```powershell
# terminal 1: Binance trades + 1m klines -> raw.trades, raw.klines
crypto ingest binance

# terminal 2: Coinbase trades -> raw.trades (different metrics port, 8000 is taken by terminal 1)
$env:CRYPTO_METRICS_PORT = "8001"; crypto ingest coinbase

# terminal 3: one poll each of the reference data (rerun when you want fresh values)
crypto ingest coingecko
crypto ingest fear-greed
```

macOS / Linux / Git Bash:

```bash
# terminal 1
crypto ingest binance

# terminal 2
CRYPTO_METRICS_PORT=8001 crypto ingest coinbase

# terminal 3
crypto ingest coingecko && crypto ingest fear-greed
```

`--symbols BTC-USDT,ETH-USDT` limits the symbols (default: all 10 in `conf/symbols.yaml`). Invalid messages go
to the `dlq.ingest` topic with a reason.

## B8. Look at the results

Open the UIs (NodePorts are mapped to the laptop by kind; this script also port-forwards what is missing and
the Spark driver UI):

```bash
make port-forward
```

| UI | URL |
|---|---|
| Grafana | http://localhost:3000 (user `admin`) |
| HDFS NameNode | http://localhost:9870 |
| Spark History Server | http://localhost:18080 |
| Spark driver UI (running job) | http://localhost:4040 |
| Prometheus | `bash deploy/monitoring/port-forward.sh`, then http://127.0.0.1:9090 |

Grafana admin password (generated at install, stored in a Secret):

```bash
# bash: macOS, Linux, Git Bash
kubectl -n monitoring get secret cryptopipe-grafana-admin -o jsonpath='{.data.admin-password}' | base64 --decode; echo
```

```powershell
# Windows PowerShell
$p = kubectl -n monitoring get secret cryptopipe-grafana-admin -o jsonpath='{.data.admin-password}'
[Text.Encoding]::UTF8.GetString([Convert]::FromBase64String($p))
```

In Grafana open **Crypto market** (`/d/crypto-market`): pick a symbol, exchange `binance`, dataset `crypto` and a
time range of the last hour. Candles appear within a minute or two of starting the producers. **Crypto pipeline**
(`/d/crypto-pipeline`) shows producer rates, Kafka lag and Spark batch metrics.

Check the data directly:

```bash
make smoke SKIP_SPARK=1                     # Kafka message counts are now > 0
kubectl -n storage exec cassandra-0 -- cqlsh -e \
  "SELECT * FROM crypto.latest_price WHERE symbol='BTC-USDT' AND exchange='binance';"
kubectl -n storage exec hdfs-namenode-0 -c namenode -- hdfs dfs -ls -R /lake/bronze | head
```

Full end-to-end check of Grafana (every panel query, data must be live):

```bash
export GRAFANA_PASSWORD="$(kubectl -n monitoring get secret cryptopipe-grafana-admin -o jsonpath='{.data.admin-password}' | base64 --decode)"
python -m cryptopipe.serving.check --require-live   # venv active
unset GRAFANA_PASSWORD
```

## B9. Optional: load historical data (batch layer)

Years of Binance trades and klines from data.binance.vision, loaded into the lake and turned into silver tables.
Run these one at a time; on a laptop, pause the streaming jobs first (`kubectl delete -f deploy/spark/stream-ohlcv.yaml -f deploy/spark/stream-bronze.yaml`; their checkpoints on HDFS keep their position).

1. Edit `SYMBOLS`, `KIND` (both containers) and `START`/`END` in
   `deploy/spark/backfill/archive-download-job.yaml`. Keep a range small (3–5 trade days); 30 days of trades for
   10 symbols is 15–20 GB.
2. Run the steps:

```bash
kubectl -n spark delete job archive-download --ignore-not-found
kubectl apply -f deploy/spark/backfill/archive-download-job.yaml
kubectl -n spark wait --for=condition=complete job/archive-download --timeout=2h

for app in load-archive silver-rebuild profile; do
  kubectl delete -f deploy/spark/backfill/$app.yaml --ignore-not-found
  kubectl apply -f deploy/spark/backfill/$app.yaml
  echo "wait until $app is COMPLETED:"; kubectl -n spark get sparkapplications -w
done
```

(`-w` watches; press Ctrl+C once the application shows `COMPLETED` to start the next one.) Details:
[deploy/spark/backfill/README.md](../deploy/spark/backfill/README.md).

To download archive files to the laptop instead: `make data-download` (defaults in the Makefile, override e.g.
`make data-download TRADES_START=2025-09-01 TRADES_END=2025-09-02 SYMBOLS=BTCUSDT`).

## B10. Optional: synthetic data for Grafana only

To see the dashboards without running producers, write 60 synthetic candles into the separate `crypto_demo`
keyspace (never into `crypto`):

```bash
python -m cryptopipe.serving.seed_demo > /tmp/crypto-serving-demo.cql   # venv active
kubectl -n storage exec -i cassandra-0 -- cqlsh < /tmp/crypto-serving-demo.cql
```

Then choose dataset **crypto_demo = SYNTHETIC** in the market dashboard.

## B11. Stop, restart, delete

| Goal | Command |
|---|---|
| Stop producers | Ctrl+C in their terminals |
| Stop the Spark jobs | `kubectl delete -f deploy/spark/stream-ohlcv.yaml -f deploy/spark/stream-bronze.yaml -f deploy/spark/batch-silver.yaml` |
| Pause everything | Windows/macOS: quit Docker Desktop. Linux: `docker stop $(kind get nodes --name crypto)`, later `docker start $(kind get nodes --name crypto)`. The kind nodes are containers; they come back when they start again (give it a few minutes, then `kubectl get pods -A`). |
| Redeploy after changes | `make deploy` (idempotent) |
| Delete the cluster and all its data | `make cluster-down` (asks first; `make cluster-down YES=1` skips the question) |
| Start over | `make cluster-down YES=1 && make cluster-up && make build-images && make deploy` |

---

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `pip install` says the package requires Python `<3.12` | venv created with another Python | delete `.venv`, recreate with `py -3.11 -m venv .venv` (Windows) or `python3.11 -m venv .venv` |
| `python3.11: command not found` on Ubuntu 24.04 | 24.04 ships only Python 3.12 | add the deadsnakes PPA (step 1, Linux) |
| Spark: `JAVA_HOME is not set` / `Java gateway process exited` | Java missing or not found | install JDK 17, set `JAVA_HOME` (step 1 for your OS), open a new terminal |
| Spark on Windows: `HADOOP_HOME and hadoop.home.dir are unset` or a winutils error when writing | winutils missing | `python scripts/setup_winutils.py` |
| `'-' is not recognized` / `Missing Python executable` at Spark start | space in the repo path | harmless |
| `make: command not found` in Git Bash | make installed after Git Bash was opened | close and reopen Git Bash |
| `missing tools: kind ...` from a make target | tool not on `PATH` | reopen the terminal; check `kind version` |
| `make cluster-up` or `make deploy` times out, pods `Pending` | Docker out of memory | raise the memory (`.wslconfig` on Windows, Settings → Resources on macOS, free RAM on Linux), or recreate with `PROFILE=lite` |
| Linux: `permission denied ... /var/run/docker.sock` | your user is not in the `docker` group | `sudo usermod -aG docker "$USER"`, then log out and back in |
| Linux: kind pods crash with `too many open files` | inotify limits too low | the two `sysctl` settings in step 1 (Linux) |
| Apple Silicon: pods fail with `exec format error`, or run very slowly | images are amd64-only | see step 0; run Part B on an x86-64 machine |
| Jobs stay `Running`, `kube-controller-manager` CrashLoopBackOff (`leaderelection lost`) | disk saturated by image builds/pulls | wait for builds/pulls to finish (recovers by itself in ~5 min); avoid building during `deploy` |
| `kind load ...: content digest ... not found` | Docker Desktop's containerd image store | load images with `kind_load` (as in B3), not `kind load docker-image` |
| Spark job pods `ErrImageNeverPull` / `ImagePullBackOff` for `cryptopipe-spark:dev` | `:dev` tag not loaded into kind | the two `:dev` commands in B3 |
| Producer: `Broker transport failure` / `Message timed out` | `localhost` used for Kafka on Windows | `CRYPTO_KAFKA_BOOTSTRAP=127.0.0.1:9092` in `.env` |
| Coinbase producer: `Address already in use` on start | Binance producer already uses metrics port 8000 | `CRYPTO_METRICS_PORT=8001` for the second producer |
| Grafana market dashboard empty | wrong dataset/symbol/time range, or no data yet | dataset `crypto`, range ≤ 24 h, check `latest_price` in Cassandra and the `stream-ohlcv` driver log |
| Grafana pipeline dashboard empty | Prometheus is not scraping your laptop producers | expected while producers run outside the cluster; Spark and Kafka panels still fill |
| First `make deploy` takes 20+ minutes | each node pulls the ~1 GB Hadoop image | normal; later runs ~6 min |

More: [deploy/README.md](../deploy/README.md) (Troubleshooting),
[deploy/cassandra/README.md](../deploy/cassandra/README.md) and [deploy/monitoring/README.md](../deploy/monitoring/README.md).

## Cheat sheet

```powershell
# Part A, Windows (PowerShell)
py -3.11 -m venv .venv; .\.venv\Scripts\Activate.ps1; pip install -e ".[dev]"
python scripts/setup_winutils.py
python -m pytest -m "not integration"
crypto demo local
```

```bash
# Part A, macOS / Linux
python3.11 -m venv .venv && source .venv/bin/activate && pip install -e ".[dev]"
python -m pytest -m "not integration"
crypto demo local
```

```bash
# Part B (bash: Git Bash on Windows; venv active)
make cluster-up
make build-images
docker tag cryptopipe-spark:latest cryptopipe-spark:dev
bash -c '. deploy/scripts/lib.sh; kind_load cryptopipe-spark:dev'
make deploy
kubectl apply -f deploy/spark/stream-rbac.yaml -f deploy/spark/stream-config.yaml
kubectl apply -f deploy/spark/stream-bronze.yaml -f deploy/spark/stream-ohlcv.yaml -f deploy/spark/batch-silver.yaml
echo CRYPTO_KAFKA_BOOTSTRAP=127.0.0.1:9092 >> .env
crypto ingest binance                        # one terminal per producer
make port-forward                            # Grafana http://localhost:3000
make smoke
make cluster-down
```
