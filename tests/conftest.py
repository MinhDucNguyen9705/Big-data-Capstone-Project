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

    session = get_spark("cryptopipe-tests", local=True)
    yield session
    session.stop()
