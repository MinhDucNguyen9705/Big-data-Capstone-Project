# Shared helpers for deploy/ scripts. Owner: A (Platform). Source it: `. "$(dirname "$0")/../scripts/lib.sh"`.
# Works in bash on Linux, macOS, WSL and Git Bash on Windows.

# Git Bash rewrites arguments that look like POSIX paths (/lake -> C:/Program Files/Git/lake) before they reach
# kubectl or docker. Turn that off for every script that sources this file.
export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL='*'

# `pwd -W` (Git Bash) gives D:/path, which both bash and native Windows tools (helm.exe, kind.exe) accept.
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && { pwd -W 2>/dev/null || pwd; })"
DEPLOY_DIR="$REPO_ROOT/deploy"
CLUSTER_NAME="${CLUSTER_NAME:-crypto}"
PROFILE="${PROFILE:-local}"
KCTX="kind-$CLUSTER_NAME"

# Pinned versions (one place, so the README and the scripts agree).
STRIMZI_CHART_VERSION="1.3.0"
KAFKA_VERSION="4.3.1"
KAFKA_IMAGE="quay.io/strimzi/kafka:${STRIMZI_CHART_VERSION}-kafka-${KAFKA_VERSION}"
SPARK_OPERATOR_CHART_VERSION="2.5.2"
HADOOP_IMAGE="apache/hadoop:3.3.6"

if [[ -t 1 ]]; then
  _C_RESET=$'\033[0m'; _C_BLUE=$'\033[34m'; _C_GREEN=$'\033[32m'; _C_YELLOW=$'\033[33m'; _C_RED=$'\033[31m'
else
  _C_RESET=""; _C_BLUE=""; _C_GREEN=""; _C_YELLOW=""; _C_RED=""
fi

log()  { echo "${_C_BLUE}==>${_C_RESET} $*"; }
ok()   { echo "${_C_GREEN}ok${_C_RESET}  $*"; }
warn() { echo "${_C_YELLOW}warn${_C_RESET} $*" >&2; }
die()  { echo "${_C_RED}error${_C_RESET} $*" >&2; exit 1; }

case "$PROFILE" in
  local|lite|cloud) ;;
  *) die "PROFILE must be local, lite or cloud (got '$PROFILE')" ;;
esac

require_tools() {
  local missing=()
  for t in "$@"; do command -v "$t" >/dev/null 2>&1 || missing+=("$t"); done
  ((${#missing[@]} == 0)) || die "missing tools: ${missing[*]} (see deploy/README.md, Prerequisites)"
}

# Python with cryptopipe importable (the repo venv if present).
python_bin() {
  if [[ -x "$REPO_ROOT/.venv/Scripts/python.exe" ]]; then echo "$REPO_ROOT/.venv/Scripts/python.exe"
  elif [[ -x "$REPO_ROOT/.venv/bin/python" ]]; then echo "$REPO_ROOT/.venv/bin/python"
  elif command -v python3 >/dev/null 2>&1; then echo python3
  else echo python
  fi
}

cluster_exists() { kind get clusters 2>/dev/null | grep -qx "$CLUSTER_NAME"; }

# kubectl/helm always against the kind context unless KUBE_CONTEXT is set (cloud profile).
k() { kubectl --context "${KUBE_CONTEXT:-$KCTX}" "$@"; }
h() { helm --kube-context "${KUBE_CONTEXT:-$KCTX}" "$@"; }

# wait_for_rollout <namespace> <kind/name> [timeout]
wait_for_rollout() {
  local ns="$1" res="$2" timeout="${3:-300s}"
  log "waiting for $res in $ns (timeout $timeout)"
  k -n "$ns" rollout status "$res" --timeout="$timeout"
}

# run_if_exists <script> [args...]: run another member's install script only if it exists.
run_if_exists() {
  local script="$1"; shift
  if [[ -f "$REPO_ROOT/$script" ]]; then
    log "running $script"
    bash "$REPO_ROOT/$script" "$@"
  else
    log "skipped: $script does not exist yet"
    SKIPPED+=("$script")
  fi
}
SKIPPED=()

# kind_load <image...>: copy local images into every kind node. `kind load docker-image` fails with
# "content digest ... not found" when Docker Desktop uses the containerd image store, so stream a
# single-platform `docker save` into each node's containerd instead (no temp file on disk).
kind_load() {
  local img node
  for img in "$@"; do
    docker image inspect "$img" >/dev/null 2>&1 || docker pull -q "$img" >/dev/null
  done
  # One stream per node with every image: tags of the same image share layers, so they are sent once.
  for node in $(kind get nodes --name "$CLUSTER_NAME"); do
    docker save --platform linux/amd64 "$@" \
      | docker exec -i "$node" ctr --namespace=k8s.io images import --snapshotter=overlayfs - >/dev/null \
      || die "loading $* into $node failed"
    ok "loaded into $node: $*"
  done
}

# native_path <path>: a path that native Windows tools (kubectl.exe, helm.exe) can open; unchanged elsewhere.
native_path() { if command -v cygpath >/dev/null 2>&1; then cygpath -m "$1"; else echo "$1"; fi; }

# sync_hadoop_conf: copy core-site.xml/hdfs-site.xml from ConfigMap hdfs-config (storage) to ConfigMap
# hadoop-conf (spark), so Spark pods get the HDFS client config from the single source in deploy/hdfs.
sync_hadoop_conf() {
  local tmp f
  tmp="$(native_path "$(mktemp -d)")"
  for f in core-site.xml hdfs-site.xml; do
    k -n storage get configmap hdfs-config -o go-template="{{index .data \"$f\"}}" >"$tmp/$f"
  done
  k -n spark create configmap hadoop-conf --from-file="$tmp/core-site.xml" --from-file="$tmp/hdfs-site.xml" \
    --dry-run=client -o yaml | k -n spark label --local -f - app.kubernetes.io/part-of=crypto-pipeline -o yaml \
    | k apply -f - >/dev/null
  rm -rf "$tmp"
  ok "ConfigMap hadoop-conf (spark) synced from hdfs-config (storage)"
}

# confirm "<question>": true when --yes was passed (ASSUME_YES=1) or the user types y.
confirm() {
  [[ "${ASSUME_YES:-0}" == 1 ]] && return 0
  [[ -t 0 ]] || die "$1: refusing without --yes in a non-interactive shell"
  read -r -p "$1 [y/N] " reply
  [[ "$reply" == y || "$reply" == Y ]]
}

# kafka_cli <command...>: run a Kafka CLI command in a throwaway pod (Strimzi image) inside the kafka namespace.
kafka_cli() {
  k -n kafka run "kafka-cli-$RANDOM" --rm -i --restart=Never --quiet --pod-running-timeout=5m --image="$KAFKA_IMAGE" \
    --image-pull-policy=IfNotPresent --command -- "$@" </dev/null
}
