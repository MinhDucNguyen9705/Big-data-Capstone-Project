"""Shared pytest fixtures. `spark` is one local SparkSession for the whole test run."""

from __future__ import annotations

from pathlib import Path

import pytest

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture(scope="session")
def fixtures_dir() -> Path:
    return FIXTURES


@pytest.fixture(scope="session")
def spark():
    from cryptopipe.common.spark import get_spark

    # One partition for DataFrames built from Python lists: on Windows every partition spawns a Python worker
    # (~1 s each), which dominated test time. Shuffles keep the default 4 partitions.
    session = get_spark("cryptopipe-tests", local=True, conf={"spark.default.parallelism": "1"})
    yield session
    session.stop()
