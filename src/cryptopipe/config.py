"""Runtime settings, read from `CRYPTO_*` environment variables (and `.env` for local runs).

Every field has a local default so tests and `crypto demo local` run with no `.env` and no cluster.
On Kubernetes the values come from the pod environment (see `deploy/`).
"""

from __future__ import annotations

from pydantic import model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="CRYPTO_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Kafka (B, C)
    kafka_bootstrap: str = "localhost:9092"
    kafka_security_protocol: str = "PLAINTEXT"

    # Data lake (C, D). Cluster: hdfs://hdfs-namenode.storage:8020/lake
    lake_root: str = "file:///tmp/crypto-lake"
    # Structured Streaming checkpoints (C). Defaults to `${lake_root}/_checkpoints`.
    checkpoint_root: str = ""

    # Cassandra serving layer (C, D, E)
    cassandra_hosts: str = "localhost"
    cassandra_keyspace: str = "crypto"

    # Shared
    symbols_file: str = "conf/symbols.yaml"
    log_level: str = "INFO"

    # Producers' Prometheus endpoint (B)
    metrics_port: int = 8000

    @model_validator(mode="after")
    def _derive_paths(self) -> Settings:
        self.lake_root = self.lake_root.rstrip("/")
        if not self.checkpoint_root:
            self.checkpoint_root = f"{self.lake_root}/_checkpoints"
        return self

    @property
    def cassandra_host_list(self) -> list[str]:
        return [h.strip() for h in self.cassandra_hosts.split(",") if h.strip()]

    def lake_path(self, *parts: str) -> str:
        """Join `lake_root` with relative lake paths, e.g. `lake_path(contracts.LAKE_SILVER_TRADES)`."""
        return "/".join([self.lake_root, *(p.strip("/") for p in parts)])


settings = Settings()
