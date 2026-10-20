"""docs/contracts.md stays in sync with the code: every topic has a valid example message."""

import re
from pathlib import Path

import pytest

from cryptopipe import contracts

DOC = Path(__file__).resolve().parents[2] / "docs" / "contracts.md"


def _examples() -> dict[str, str]:
    """Map topic name -> first ```json block under its `### \\`topic\\`` heading."""
    text = DOC.read_text(encoding="utf-8")
    out = {}
    for match in re.finditer(r"^### `([^`]+)`.*?```json\n(.*?)\n```", text, re.S | re.M):
        out[match.group(1)] = match.group(2)
    return out


@pytest.mark.parametrize("topic", contracts.TOPICS, ids=lambda t: t.name)
def test_every_topic_has_valid_example(topic):
    examples = _examples()
    assert topic.name in examples, f"docs/contracts.md has no example for {topic.name}"
    topic.value_model.model_validate_json(examples[topic.name])


def test_doc_lists_every_lake_path_and_table():
    text = DOC.read_text(encoding="utf-8")
    for path in contracts.LAKE_PATHS.values():
        assert f"`{path}" in text, path
    for table in contracts.CASSANDRA_TABLES:
        assert f"`{table}`" in text, table
