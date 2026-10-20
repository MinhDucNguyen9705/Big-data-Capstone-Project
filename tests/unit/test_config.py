from cryptopipe.config import Settings


def test_defaults_need_no_env(monkeypatch):
    for var in ("CRYPTO_LAKE_ROOT", "CRYPTO_CHECKPOINT_ROOT", "CRYPTO_KAFKA_BOOTSTRAP"):
        monkeypatch.delenv(var, raising=False)
    s = Settings(_env_file=None)
    assert s.kafka_bootstrap == "localhost:9092"
    assert s.lake_root == "file:///tmp/crypto-lake"
    assert s.checkpoint_root == "file:///tmp/crypto-lake/_checkpoints"
    assert s.cassandra_keyspace == "crypto"
    assert s.metrics_port == 8000


def test_env_overrides_and_derived_checkpoint(monkeypatch):
    monkeypatch.setenv("CRYPTO_LAKE_ROOT", "hdfs://hdfs-namenode.storage:8020/lake/")
    monkeypatch.setenv("CRYPTO_CASSANDRA_HOSTS", "c1, c2")
    s = Settings(_env_file=None)
    assert s.lake_root == "hdfs://hdfs-namenode.storage:8020/lake"
    assert s.checkpoint_root == "hdfs://hdfs-namenode.storage:8020/lake/_checkpoints"
    assert s.cassandra_host_list == ["c1", "c2"]
    assert s.lake_path("silver/trades") == "hdfs://hdfs-namenode.storage:8020/lake/silver/trades"
