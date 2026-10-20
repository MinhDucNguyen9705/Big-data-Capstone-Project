import json
import logging

from cryptopipe.common.logging import JsonFormatter


def test_json_formatter_includes_extra_fields():
    record = logging.LogRecord("x", logging.INFO, __file__, 1, "sent %d", (3,), None)
    record.topic = "raw.trades"
    out = json.loads(JsonFormatter().format(record))
    assert out["msg"] == "sent 3"
    assert out["level"] == "INFO"
    assert out["topic"] == "raw.trades"
