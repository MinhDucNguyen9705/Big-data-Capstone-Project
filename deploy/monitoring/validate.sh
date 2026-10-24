#!/usr/bin/env bash
# Owner: E. Render Helm/Kustomize and validate every resource, including Prometheus CRDs.
set -euo pipefail
. "$(dirname "$0")/../scripts/lib.sh"
require_tools helm kubectl kubeconform
PY="$(python_bin)"
tmp="$(native_path "$(mktemp -d)")"
trap 'rm -rf "$tmp"' EXIT
"$PY" "$DEPLOY_DIR/monitoring/generate.py" --check
if [[ -n "${MONITORING_CHART_DIR:-}" ]]; then
  chart="$MONITORING_CHART_DIR"
else
  helm pull kube-prometheus-stack --repo https://prometheus-community.github.io/helm-charts \
    --version 69.8.2 --destination "$tmp" --untar
  chart="$tmp/kube-prometheus-stack"
fi
helm lint "$chart" --values "$DEPLOY_DIR/monitoring/values.yaml"
helm template crypto-monitoring "$chart" --namespace monitoring --kube-version 1.32.0 \
  --values "$DEPLOY_DIR/monitoring/values.yaml" >"$tmp/monitoring.yaml"
kubectl kustomize "$DEPLOY_DIR/cassandra" >"$tmp/cassandra.yaml"
# Reuse the exact chart's OpenAPI schemas rather than silently skipping custom resources.
"$PY" - "$chart" "$tmp" <<'PY'
import json
import sys
from pathlib import Path

import yaml

# Prometheus's YAML contains an unquoted '=' enum value. PyYAML's YAML 1.1
# resolver tags it specially, while Helm/Kubernetes treat it as a string.
class CRDLoader(yaml.SafeLoader):
    pass

CRDLoader.add_constructor("tag:yaml.org,2002:value", lambda loader, node: loader.construct_scalar(node))

chart, dest = map(Path, sys.argv[1:])
for source in (chart / "charts/crds/crds").glob("*.yaml"):
    for crd in yaml.load_all(source.read_text(), Loader=CRDLoader):
        if not crd or crd.get("kind") != "CustomResourceDefinition":
            continue
        spec = crd["spec"]
        folder = dest / "schemas" / spec["group"]
        folder.mkdir(parents=True, exist_ok=True)
        for version in spec["versions"]:
            target = folder / f'{spec["names"]["kind"].lower()}_{version["name"]}.json'
            target.write_text(json.dumps(version["schema"]["openAPIV3Schema"]))
PY
kubeconform -strict -summary -kubernetes-version 1.32.0 \
  -schema-location default \
  -schema-location "$tmp/schemas/{{.Group}}/{{.ResourceKind}}_{{.ResourceAPIVersion}}.json" \
  "$tmp/monitoring.yaml" "$tmp/cassandra.yaml" \
  "$DEPLOY_DIR/cassandra/schema-job.yaml" "$DEPLOY_DIR/monitoring/kafka-podmonitor.yaml" \
  "$DEPLOY_DIR/monitoring/dashboards.generated.yaml"
for script in "$DEPLOY_DIR"/cassandra/*.sh "$DEPLOY_DIR"/monitoring/*.sh; do bash -n "$script"; done
ok "Serving manifests, shell syntax and generated dashboards validated"
