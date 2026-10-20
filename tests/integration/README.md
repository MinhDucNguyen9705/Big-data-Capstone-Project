# tests/integration/

Tests that need a running cluster (Kafka, HDFS, Cassandra). Mark every test with `@pytest.mark.integration`;
they are excluded from `make test` and run with `make test-integration`. Each owner adds tests for their layer.
