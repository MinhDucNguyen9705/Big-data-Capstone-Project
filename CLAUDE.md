# Project rules

- Python 3.11, PySpark 3.5. Package code lives in `src/cryptopipe/`. Spark jobs are thin `main()` wrappers around pure functions
  (`DataFrame -> DataFrame`) in `src/cryptopipe/transforms/` so they can be unit tested without Kafka/HDFS/Cassandra.
- Never hardcode hosts, ports, paths or secrets. Read them from `cryptopipe.config.Settings` (env vars, `.env` for local).
  Topic names, lake paths and table names come from `cryptopipe.contracts`.
- Every Kafka message type has a pydantic model in `src/cryptopipe/schemas/`. Producers validate before sending;
  invalid records go to the `dlq.ingest` topic with an error reason. Spark reads JSON with `to_spark_schema(Model)`.
- Event time is the exchange timestamp (`event_ts`, UTC, milliseconds). Ingestion time is `ingest_ts`. Never mix them.
- Dedupe key for trades: `(exchange, symbol, trade_id)` (`contracts.TRADE_KEY`).
- Spark config lives in `deploy/spark/*.yaml` (SparkApplication) and `conf/spark-defaults.conf`, not in code,
  except settings a job truly needs (e.g. watermark length passed as an argument).
- Every new transform gets a unit test in `tests/unit/`. Run `make lint test` before committing.
  Use the shared fixtures in `tests/fixtures/` (rebuild the synthetic ones with `make fixtures`).
- K8s manifests go in `deploy/`. Use namespaces: `kafka`, `storage` (HDFS, Cassandra), `spark`, `monitoring`, `ingest`.
- When something breaks in an instructive way (or an experiment produces numbers), append an entry to
  `docs/journal/<category>.md`: date, symptom, what was tried, numbers, fix. These feed the report's Lessons Learned.
- Commit messages: conventional commits (`feat:`, `fix:`, `docs:`, `chore:`, `test:`).
- Implementation spec: `docs/implementation-plan.md`. Contracts: `docs/contracts.md`.
- **Only edit files your task owns** (see the header comment of each file, `.github/CODEOWNERS` and the team table in
  `README.md`); contracts change only through a PR that updates `docs/contracts.md`.
- Stubs raise `NotImplementedError("Task A3 – owner B")`; their tests in `tests/unit/<area>/` are skipped with
  `pytest.mark.skip(reason="TODO ...")`. Implementing a stub means removing that marker and making the test pass.
- Windows: run `python scripts/setup_winutils.py` once so local Spark can write files.
