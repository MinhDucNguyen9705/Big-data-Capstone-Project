# Owner: D | Task: A6/A8. D's Kubernetes manifests stay consistent with the jobs they run (no cluster needed).
import importlib
from pathlib import Path

import pytest
import yaml

from cryptopipe.jobs import JOBS

DEPLOY = Path(__file__).resolve().parents[3] / "deploy" / "spark"
FILES = sorted(DEPLOY.glob("batch-*.yaml")) + sorted((DEPLOY / "backfill").glob("*.yaml"))
ENTRYPOINT = "local:///opt/cryptopipe/jobs/"


def _docs():
    for path in FILES:
        for doc in yaml.safe_load_all(path.read_text(encoding="utf-8")):
            yield path.relative_to(DEPLOY).as_posix(), doc


def _spark_apps():
    for name, doc in _docs():
        if doc["kind"] == "SparkApplication":
            yield f"{name}:{doc['metadata']['name']}", doc["spec"]
        elif doc["kind"] == "ScheduledSparkApplication":
            yield f"{name}:{doc['metadata']['name']}", doc["spec"]["template"]


SPARK_APPS = list(_spark_apps())


def test_manifests_exist():
    names = {n for n, _ in SPARK_APPS}
    assert any(n.startswith("batch-silver.yaml") for n in names)  # the scheduled refresh applied by make deploy
    assert len(SPARK_APPS) == 5


@pytest.mark.parametrize(("name", "spec"), SPARK_APPS, ids=[n for n, _ in SPARK_APPS])
def test_spark_app_runs_a_registered_job_with_valid_arguments(name, spec):
    assert spec["mainApplicationFile"].startswith(ENTRYPOINT)
    job = spec["mainApplicationFile"].removeprefix(ENTRYPOINT).removesuffix(".py")
    module, owner, _task = JOBS[job]
    assert owner == "D"
    args = importlib.import_module(module).build_parser().parse_args(spec["arguments"])
    assert not args.local  # cluster runs never use a local SparkSession
    assert not getattr(args, "lake_root", None)  # lake root comes from cryptopipe-env


@pytest.mark.parametrize(("name", "spec"), SPARK_APPS, ids=[n for n, _ in SPARK_APPS])
def test_spark_app_follows_the_platform_template(name, spec):
    # Fields deploy/spark/examples/README.md (A) says to keep as they are
    assert spec["image"] == "cryptopipe-spark:latest" and spec["hadoopConfigMap"] == "hadoop-conf"
    assert spec["driver"]["serviceAccount"] == "spark"
    for role in ("driver", "executor"):
        assert {"configMapRef": {"name": "cryptopipe-env"}} in spec[role]["envFrom"]
    if spec["mainApplicationFile"].endswith("batch_silver.py"):  # the bucketed table must outlive the pod
        assert spec["sparkConf"]["spark.sql.warehouse.dir"].startswith("hdfs://")


def test_download_job_kinds_match_and_arguments_parse(monkeypatch):
    from datetime import date

    from typer.testing import CliRunner

    from cryptopipe.cli import app
    from cryptopipe.ingest import archive

    (job,) = [doc for _name, doc in _docs() if doc["kind"] == "Job"]
    pod = job["spec"]["template"]["spec"]
    (download,) = pod["initContainers"]
    (put,) = pod["containers"]
    env = {e["name"]: e["value"] for e in download["env"]}
    assert {e["name"]: e.get("value") for e in put["env"]}["KIND"] == env["KIND"]  # put uploads what was fetched

    # Kubernetes expands $(VAR) from the container env; run the real CLI with the download stubbed out.
    args = download["args"]
    for key, value in env.items():
        args = [a.replace(f"$({key})", value) for a in args]
    calls = []
    monkeypatch.setattr(archive, "download", lambda *a, **kw: calls.append(a) or [])
    result = CliRunner().invoke(app, args)
    assert result.exit_code == 0, result.output
    symbols, kind, start, end, dest = calls[0]
    assert kind == env["KIND"] and str(dest) == "/staging"
    assert start == date.fromisoformat(env["START"]) <= end == date.fromisoformat(env["END"])
    assert len(symbols) == 10
