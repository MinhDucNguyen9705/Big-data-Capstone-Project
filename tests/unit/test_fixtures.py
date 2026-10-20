"""The committed fixtures match the seeded generator (tests/fixtures/make_fixtures.py --check)."""

import importlib.util
import json

from cryptopipe.schemas import Trade


def _load_generator(fixtures_dir):
    spec = importlib.util.spec_from_file_location("make_fixtures", fixtures_dir / "make_fixtures.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_fixtures_match_seed(fixtures_dir):
    gen = _load_generator(fixtures_dir)
    assert gen.check(gen.build()) == []


def test_normalized_trades_contents(fixtures_dir):
    lines = (fixtures_dir / "trades_normalized.jsonl").read_text(encoding="utf-8").splitlines()
    assert len(lines) == 500
    assert len(set(lines)) == 495  # 5 exact duplicates
    invalid = 0
    for line in lines:
        try:
            Trade.model_validate_json(line)
        except ValueError:
            invalid += 1
    assert invalid == 2  # negative price and unknown symbol; the future timestamp is a pipeline rule
    events = [json.loads(line)["event_ts"] for line in lines]
    assert sum(b < a for a, b in zip(events, events[1:], strict=False)) >= 8  # out-of-order arrivals
