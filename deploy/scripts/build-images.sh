#!/usr/bin/env bash
# Build cryptopipe-spark and cryptopipe-ingest, tag them <git short sha> and latest, load them into kind.
# Owner: A (Platform). Usage: deploy/scripts/build-images.sh [--no-load]
set -euo pipefail
. "$(dirname "$0")/lib.sh"

require_tools docker git
LOAD=1
[[ "${1:-}" == "--no-load" ]] && LOAD=0

cd "$REPO_ROOT"
TAG="$(git rev-parse --short HEAD)"
git diff --quiet HEAD -- src docker conf pyproject.toml || TAG="${TAG}-dirty"

images=()
for name in spark ingest; do
  log "building cryptopipe-$name:$TAG"
  start=$(date +%s)
  docker build -f "docker/$name/Dockerfile" -t "cryptopipe-$name:$TAG" -t "cryptopipe-$name:latest" .
  ok "cryptopipe-$name:$TAG built in $(( $(date +%s) - start )) s, size $(docker image inspect -f '{{.Size}}' "cryptopipe-$name:$TAG" | awk '{printf "%.0f MB", $1/1e6}')"
  images+=("cryptopipe-$name:$TAG" "cryptopipe-$name:latest")
done

if ((LOAD)); then
  if [[ "$PROFILE" == cloud ]]; then
    warn "PROFILE=cloud: push the images to the registry instead of kind load (Q2)"
  elif cluster_exists; then
    kind_load "${images[@]}"
  else
    warn "kind cluster '$CLUSTER_NAME' does not exist; images built but not loaded (run make cluster-up)"
  fi
fi
