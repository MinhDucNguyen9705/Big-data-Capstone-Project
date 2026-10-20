"""SparkSession factory shared by jobs, tests and the local demo.

Local mode (`local=True`) is self-contained: `local[*]`, UTC, 4 shuffle partitions and a throw-away warehouse.
Cluster mode adds nothing: config comes from `conf/spark-defaults.conf` (baked into the Spark image) and
the SparkApplication YAML. Connector jars (Kafka, Cassandra) come from the image, never from code.
"""

from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

from pyspark.sql import SparkSession

# Repo root when running from a source checkout (src/cryptopipe/common/spark.py -> repo).
_REPO_ROOT = Path(__file__).resolve().parents[3]


def _ensure_hadoop_home() -> None:
    """On Windows, Spark needs winutils.exe/hadoop.dll to write files. Use `.tools/hadoop` if present.

    `python scripts/setup_winutils.py` downloads them. Linux, macOS, CI and the Docker images need nothing.
    """
    if sys.platform != "win32" or os.environ.get("HADOOP_HOME"):
        return
    candidate = _REPO_ROOT / ".tools" / "hadoop"
    if (candidate / "bin" / "winutils.exe").exists():
        os.environ["HADOOP_HOME"] = str(candidate)
        os.environ["PATH"] = str(candidate / "bin") + os.pathsep + os.environ.get("PATH", "")


def get_spark(app_name: str, local: bool = False) -> SparkSession:
    builder = SparkSession.builder.appName(app_name)
    if local:
        _ensure_hadoop_home()
        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
        os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
        warehouse = tempfile.mkdtemp(prefix="crypto-warehouse-")
        builder = (
            builder.master("local[*]")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.executor.extraJavaOptions", "-Duser.timezone=UTC")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.sql.warehouse.dir", Path(warehouse).as_uri())
            .config("spark.driver.extraJavaOptions", f"-Duser.timezone=UTC -Dderby.system.home={warehouse}")
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
            .config("spark.ui.enabled", "false")
            .config("spark.ui.showConsoleProgress", "false")
        )
    spark = builder.getOrCreate()
    if local:
        spark.sparkContext.setLogLevel("WARN")
    return spark
