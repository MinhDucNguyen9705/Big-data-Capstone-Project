# Developer entrypoints. Works on Linux, macOS, WSL and Git Bash on Windows (needs GNU make).
# Owners: bootstrap targets are shared; cluster targets belong to A (Platform), data-download to D.

VENV    ?= .venv
PYTHON  ?= python3
ifeq ($(OS),Windows_NT)
  BIN := $(VENV)/Scripts
  PYTHON := python
else
  BIN := $(VENV)/bin
endif
PY := $(BIN)/python

.DEFAULT_GOAL := help
.PHONY: help install lint fmt test test-integration demo fixtures fixtures-check \
        build-images cluster-up cluster-down deploy smoke port-forward data-download clean

help:  ## List targets
	@grep -E '^[a-zA-Z_-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-18s %s\n", $$1, $$2}'

$(PY):
	$(PYTHON) -m venv $(VENV)

install: $(PY)  ## Create .venv and install cryptopipe with dev extras
	$(PY) -m pip install --upgrade pip
	$(PY) -m pip install -e ".[dev]"

lint:  ## ruff check + format check
	$(BIN)/ruff check .
	$(BIN)/ruff format --check .

fmt:  ## Auto-fix lint and format
	$(BIN)/ruff check --fix .
	$(BIN)/ruff format .

test:  ## Unit tests (no cluster needed)
	$(PY) -m pytest -m "not integration"

test-integration:  ## Tests that need the cluster (Kafka, HDFS, Cassandra)
	$(PY) -m pytest -m integration

demo:  ## Walking skeleton: fixture trades -> 1m OHLCV Parquet in local Spark
	$(BIN)/crypto demo local

fixtures:  ## Regenerate the seeded fixtures (tests/fixtures/make_fixtures.py)
	$(PY) tests/fixtures/make_fixtures.py

fixtures-check:  ## Verify committed fixtures match the seed
	$(PY) tests/fixtures/make_fixtures.py --check

# ---- owner A (Platform): scripts in deploy/scripts, see deploy/README.md --------------------------------------
PROFILE ?= local

build-images:  ## [owner A] Build cryptopipe-spark and cryptopipe-ingest images, load into kind
	bash deploy/scripts/build-images.sh

cluster-up:  ## [owner A] Create the kind cluster, namespaces, shared ConfigMap, operators (PROFILE=local|lite)
	PROFILE=$(PROFILE) bash deploy/scripts/cluster-up.sh

cluster-down:  ## [owner A] Delete the local kind cluster (asks first; YES=1 to skip)
	bash deploy/scripts/cluster-down.sh $(if $(YES),--yes)

deploy:  ## [owner A] Kafka, HDFS, Spark, History Server + others' install scripts if present (WITH_JOBS=1)
	PROFILE=$(PROFILE) WITH_JOBS=$(WITH_JOBS) bash deploy/scripts/deploy-all.sh

smoke:  ## [owner A] Smoke test: pods ready, topic counts, HDFS usage, Spark job, Cassandra rows
	PROFILE=$(PROFILE) bash deploy/scripts/smoke-test.sh

port-forward:  ## [owner A] Port-forward NameNode UI, Spark UI, History Server, Grafana
	bash deploy/scripts/port-forward.sh

# ---- owner D (Batch and data), task A8 -----------------------------------------------------------------------
# Checkpoint dataset: 30 days of trades and 2 years of 1m klines for the tracked symbols (override on the command
# line, e.g. `make data-download TRADES_START=2025-09-01 TRADES_END=2025-09-02 SYMBOLS=BTCUSDT`).
DATA_DEST    ?= ./data/archive
SYMBOLS      ?= $(shell $(PY) -c "from cryptopipe.symbols import load_symbols; print(','.join(load_symbols().for_exchange('binance')))")
TRADES_START ?= 2025-09-01
TRADES_END   ?= 2025-09-30
KLINES_START ?= 2023-10-01
KLINES_END   ?= 2025-09-30

data-download:  ## [owner D] Download the checkpoint dataset from data.binance.vision
	$(BIN)/crypto backfill download --symbols $(SYMBOLS) --kind trades --start $(TRADES_START) --end $(TRADES_END) --dest $(DATA_DEST)
	$(BIN)/crypto backfill download --symbols $(SYMBOLS) --kind klines --start $(KLINES_START) --end $(KLINES_END) --dest $(DATA_DEST)

clean:  ## Remove caches and local Spark state
	rm -rf .pytest_cache .ruff_cache spark-warehouse metastore_db derby.log build dist src/*.egg-info
