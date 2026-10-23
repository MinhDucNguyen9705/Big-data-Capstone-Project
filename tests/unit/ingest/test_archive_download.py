# Owner: D | Task: A4. Archive downloader (fake HTTP server) and replayer (fake Kafka producer).
import hashlib
import io
import json
import zipfile
from datetime import date
from pathlib import Path

import httpx
import pytest

from cryptopipe.contracts import TOPIC_DLQ, TOPIC_RAW_TRADES
from cryptopipe.ingest import archive, replay

# ----------------------------------------------------------------------------------------------- downloader


def test_archive_url_daily_trades():
    url = archive.archive_url("BTCUSDT", "trades", date(2025, 9, 1))
    assert url.endswith("/daily/trades/BTCUSDT/BTCUSDT-trades-2025-09-01.zip")


def test_archive_url_monthly_klines():
    url = archive.archive_url("btcusdt", "klines", date(2025, 9, 17))
    assert url == "https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/1m/BTCUSDT-1m-2025-09.zip"


def test_days_and_months_are_inclusive():
    assert archive.days(date(2025, 9, 30), date(2025, 10, 2)) == [
        date(2025, 9, 30),
        date(2025, 10, 1),
        date(2025, 10, 2),
    ]
    assert archive.months(date(2024, 12, 15), date(2025, 2, 1)) == [
        date(2024, 12, 1),
        date(2025, 1, 1),
        date(2025, 2, 1),
    ]


def test_verify_checksum(tmp_path):
    f = tmp_path / "x.zip"
    f.write_bytes(b"data")
    digest = hashlib.sha256(b"data").hexdigest()
    (tmp_path / "x.zip.CHECKSUM").write_text(f"{digest}  x.zip\n")
    assert archive.verify_checksum(f)
    f.write_bytes(b"tampered")
    assert not archive.verify_checksum(f)


def test_verify_checksum_missing_files(tmp_path):
    assert not archive.verify_checksum(tmp_path / "absent.zip")


class FakeArchive:
    """data.binance.vision stand-in: published zips with their .CHECKSUM, 404 for everything else."""

    def __init__(self):
        self.files: dict[str, bytes] = {}
        self.calls: list[str] = []

    def publish(self, url: str, csv_text: str, checksum: str | None = None) -> None:
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr(url.rsplit("/", 1)[-1].replace(".zip", ".csv"), csv_text)
        data = buf.getvalue()
        self.files[url] = data
        digest = checksum or hashlib.sha256(data).hexdigest()
        self.files[url + ".CHECKSUM"] = f"{digest}  {url.rsplit('/', 1)[-1]}\n".encode()

    def client(self) -> httpx.Client:
        def handler(request: httpx.Request) -> httpx.Response:
            self.calls.append(str(request.url))
            body = self.files.get(str(request.url))
            return httpx.Response(200, content=body) if body is not None else httpx.Response(404)

        return httpx.Client(transport=httpx.MockTransport(handler))


CSV_DAY1 = "1,100.0,0.5,50.0,1756684800000000,True,True\n2,101.0,0.5,50.5,1756684801000000,False,True\n"
CSV_DAY2 = "3,102.0,0.5,51.0,1756771200000000,True,True\n"


def test_download_verifies_unzips_skips_missing_and_is_idempotent(tmp_path):
    server = FakeArchive()
    server.publish(archive.archive_url("BTCUSDT", "trades", date(2025, 9, 1)), CSV_DAY1)
    server.publish(archive.archive_url("BTCUSDT", "trades", date(2025, 9, 2)), CSV_DAY2)
    # 2025-09-03 is not published -> skipped

    files = archive.download(["BTCUSDT"], "trades", date(2025, 9, 1), date(2025, 9, 3), tmp_path, server.client())

    folder = tmp_path / "trades" / "BTCUSDT"
    assert [f.name for f in files] == ["BTCUSDT-trades-2025-09-01.csv", "BTCUSDT-trades-2025-09-02.csv"]
    assert files[0].read_text() == CSV_DAY1
    assert not list(folder.glob("*.zip")) and not list(folder.glob("*.part"))  # zips removed after extraction
    assert len(list(folder.glob("*.CHECKSUM"))) == 2  # provenance kept

    calls = len(server.calls)
    again = archive.download(["BTCUSDT"], "trades", date(2025, 9, 1), date(2025, 9, 2), tmp_path, server.client())
    assert again == files
    assert len(server.calls) == calls  # nothing downloaded twice


def test_download_resumes_from_a_verified_zip(tmp_path):
    server = FakeArchive()
    url = archive.archive_url("ETHUSDT", "trades", date(2025, 9, 1))
    server.publish(url, CSV_DAY1)
    folder = tmp_path / "trades" / "ETHUSDT"
    folder.mkdir(parents=True)
    (folder / "ETHUSDT-trades-2025-09-01.zip").write_bytes(server.files[url])  # left by an interrupted run
    (folder / "ETHUSDT-trades-2025-09-01.zip.CHECKSUM").write_bytes(server.files[url + ".CHECKSUM"])

    files = archive.download(["ETHUSDT"], "trades", date(2025, 9, 1), date(2025, 9, 1), tmp_path, server.client())

    assert files[0].read_text() == CSV_DAY1
    assert server.calls == []


def test_download_checksum_mismatch_raises(tmp_path, monkeypatch):
    monkeypatch.setattr(archive.time, "sleep", lambda s: None)
    server = FakeArchive()
    server.publish(archive.archive_url("BTCUSDT", "trades", date(2025, 9, 1)), CSV_DAY1, checksum="0" * 64)

    with pytest.raises(archive.ArchiveError, match="not verified"):
        archive.download(["BTCUSDT"], "trades", date(2025, 9, 1), date(2025, 9, 1), tmp_path, server.client())
    assert not list((tmp_path / "trades" / "BTCUSDT").glob("*.csv"))
    assert sum(u.endswith(".zip") for u in server.calls) == archive.ATTEMPTS


def test_klines_month_without_monthly_file_falls_back_to_daily(tmp_path):
    server = FakeArchive()
    server.publish(archive.archive_url("BTCUSDT", "klines", date(2025, 9, 1)), "monthly\n")
    # October: monthly file not published yet, daily files are
    for d in (1, 2):
        server.publish(archive.daily_klines_url("BTCUSDT", date(2025, 10, d)), f"daily {d}\n")

    files = archive.download(["BTCUSDT"], "klines", date(2025, 9, 1), date(2025, 10, 2), tmp_path, server.client())

    assert [f.name for f in files] == [
        "BTCUSDT-1m-2025-09.csv",
        "BTCUSDT-1m-2025-10-01.csv",
        "BTCUSDT-1m-2025-10-02.csv",
    ]
    assert all(f.parent == tmp_path / "klines" / "BTCUSDT" for f in files)


def test_download_to_hdfs_puts_files_and_skips_existing(tmp_path, monkeypatch):
    server = FakeArchive()
    server.publish(archive.archive_url("BTCUSDT", "trades", date(2025, 9, 1)), CSV_DAY1)
    server.publish(archive.archive_url("BTCUSDT", "trades", date(2025, 9, 2)), CSV_DAY2)
    commands = []
    existing = {"hdfs://nn:8020/lake/raw/trades/BTCUSDT/BTCUSDT-trades-2025-09-02.csv"}

    def fake_hdfs(*args):
        commands.append(args)
        code = 0 if args[0] != "-test" or args[-1] in existing else 1
        return type("Result", (), {"returncode": code, "stderr": ""})()

    monkeypatch.setattr(archive, "_hdfs", fake_hdfs)
    files = archive.download(
        ["BTCUSDT"], "trades", date(2025, 9, 1), date(2025, 9, 2), Path("hdfs://nn:8020/lake/raw"), server.client()
    )

    assert [str(f) for f in files] == [
        "/lake/raw/trades/BTCUSDT/BTCUSDT-trades-2025-09-01.csv",
        "/lake/raw/trades/BTCUSDT/BTCUSDT-trades-2025-09-02.csv",
    ]
    puts = [c for c in commands if c[0] == "-put"]
    assert len(puts) == 1 and puts[0][-1] == "hdfs://nn:8020/lake/raw/trades/BTCUSDT/BTCUSDT-trades-2025-09-01.csv"
    assert not any("2025-09-02.zip" in u for u in server.calls)  # already on HDFS


def test_download_rejects_bad_arguments(tmp_path):
    with pytest.raises(ValueError):
        archive.download(["BTCUSDT"], "aggTrades", date(2025, 9, 1), date(2025, 9, 1), tmp_path)
    with pytest.raises(ValueError):
        archive.download(["BTCUSDT"], "trades", date(2025, 9, 2), date(2025, 9, 1), tmp_path)


# ------------------------------------------------------------------------------------------------- replayer


class FakeProducer:
    def __init__(self):
        self.messages: list[tuple[str, str, dict]] = []
        self.flushed = False

    def produce(self, topic, key=None, value=None):
        self.messages.append((topic, key, json.loads(value)))

    def poll(self, timeout=0):
        return 0

    def flush(self, timeout=None):
        self.flushed = True
        return 0


def test_replay_dry_run_counts_rows(fixtures_dir):
    assert replay.replay(fixtures_dir / "archive_trades_sample.csv", None, rate=1e9, dry_run=True) == 1000


def test_replay_dry_run_prints_trades_in_order(fixtures_dir, capsys):
    replay.replay(fixtures_dir / "archive_trades_sample.csv", None, rate=1e9, dry_run=True)
    trades = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert len(trades) == 1000
    assert trades[0]["event_ts"] == 1_756_684_800_097  # microseconds -> ms
    assert [t["event_ts"] for t in trades] == sorted(t["event_ts"] for t in trades)
    assert {t["symbol"] for t in trades} == {"BTC-USDT"}


def _write(path: Path, rows: list[str]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(r + "\n" for r in rows))
    return path


def test_replay_merges_files_in_time_order_and_sends_bad_rows_to_dlq(tmp_path):
    # BTC in microseconds (2025 archive), ETH in milliseconds (pre-2025 archive); one broken line
    _write(
        tmp_path / "BTCUSDT-trades-2025-09-01.csv",
        ["10,100.0,1.0,100.0,1756684800000000,True,True", "11,101.0,1.0,101.0,1756684802000000,False,True"],
    )
    _write(
        tmp_path / "eth" / "ETHUSDT-trades-2025-09-01.csv",
        ["20,4000.0,1.0,4000.0,1756684801000,False,True", "garbage", "21,-5,1.0,1.0,1756684803000,False,True"],
    )
    producer = FakeProducer()

    sent = replay.replay(tmp_path, producer, rate=1e9)

    trades = [(k, v) for t, k, v in producer.messages if t == TOPIC_RAW_TRADES.name]
    dlq = [v for t, _k, v in producer.messages if t == TOPIC_DLQ.name]
    assert sent == 3 and producer.flushed
    assert [v["trade_id"] for _k, v in trades] == ["10", "20", "11"]  # merged by event time
    assert [k for k, _v in trades] == ["BTC-USDT", "ETH-USDT", "BTC-USDT"]  # key = canonical symbol
    assert [v["side"] for _k, v in trades] == ["sell", "buy", "buy"]
    assert [d["source"] for d in dlq] == ["replay", "replay"]  # unparseable line + negative price
    assert "invalid trade" in dlq[-1]["reason"]


def test_replay_symbol_from_folder_and_override(tmp_path):
    _write(tmp_path / "trades" / "SOLUSDT" / "part.csv", ["1,200.0,1.0,200.0,1756684800000,True,True"])
    producer = FakeProducer()
    replay.replay(tmp_path, producer, rate=1e9)
    assert producer.messages[0][1] == "SOL-USDT"

    producer = FakeProducer()
    replay.replay(tmp_path, producer, rate=1e9, symbol="ETH-USDT")
    assert producer.messages[0][1] == "ETH-USDT"


def test_replay_rebase_time_moves_first_trade_to_now(fixtures_dir):
    producer = FakeProducer()
    before = replay.now_ms()
    replay.replay(fixtures_dir / "archive_trades_sample.csv", producer, rate=1e9, rebase_time=True)
    first, last = producer.messages[0][2], producer.messages[-1][2]
    assert before <= first["event_ts"] <= replay.now_ms()
    assert last["event_ts"] - first["event_ts"] == 1_756_684_859_118 - 1_756_684_800_097  # spacing kept


class FakeClock:
    def __init__(self):
        self.now = 0.0
        self.sleeps: list[float] = []

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds


@pytest.mark.parametrize(
    ("kwargs", "expected_sleeps"),
    [
        ({"speed": 2.0}, [0.5, 1.0]),  # trades 1 s and 3 s after the first, at 2x real time
        ({"rate": 4.0}, [0.25, 0.25]),  # 4 messages per second
    ],
)
def test_replay_pacing(tmp_path, monkeypatch, kwargs, expected_sleeps):
    clock = FakeClock()
    monkeypatch.setattr(replay.time, "monotonic", clock.monotonic)
    monkeypatch.setattr(replay.time, "sleep", clock.sleep)
    _write(
        tmp_path / "BTCUSDT-trades-2025-09-01.csv",
        [f"{i},100.0,1.0,100.0,{1756684800000 + s * 1000},True,True" for i, s in enumerate([0, 1, 3])],
    )
    replay.replay(tmp_path, FakeProducer(), **kwargs)
    assert clock.sleeps == pytest.approx(expected_sleeps)


def test_replay_argument_checks(fixtures_dir):
    path = fixtures_dir / "archive_trades_sample.csv"
    with pytest.raises(ValueError, match="producer"):
        replay.replay(path, None)
    with pytest.raises(ValueError, match="rate"):
        replay.replay(path, None, rate=0, dry_run=True)
    with pytest.raises(ValueError, match="symbol"):
        replay.replay(path, None, dry_run=True, symbol="FOO-USDT")
    with pytest.raises(FileNotFoundError):
        replay.replay(fixtures_dir / "missing", None, dry_run=True)


def test_to_ms_by_magnitude():
    assert replay.to_ms(1_756_684_800) == 1_756_684_800_000
    assert replay.to_ms(1_756_684_800_097) == 1_756_684_800_097
    assert replay.to_ms(1_756_684_800_097_640) == 1_756_684_800_097
    assert replay.to_ms(1_756_684_800_097_640_123) == 1_756_684_800_097
