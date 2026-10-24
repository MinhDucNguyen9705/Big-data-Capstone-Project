# ADR 0005: JSON contracts for the checkpoint, Avro later

Status: accepted checkpoint decision; Avro/Schema Registry is planned work.

## Context

Five team layers must integrate quickly and use the same fixtures. External exchanges already emit JSON
with different fields. A registry migration during initial integration would change ingestion and Spark parsers
simultaneously and make debugging harder.

## Decision

Keep normalized UTF-8 JSON with the existing shared Pydantic models and `schema_version=1`. Validate at
ingestion and processing boundaries; retain rejects with their reason and raw payload. Generate Spark schemas
from the same models. Use `docs/contracts.md` as the reviewed human-readable interface.

Plan Avro plus a Schema Registry and version-2 migration under milestone B1, including compatibility checks,
fixtures and coordinated producer/parser rollout. Do not describe the registry as deployed at this checkpoint.

## Consequences

JSON stays easy to inspect and replay, but is verbose and does not provide centralized registry compatibility
enforcement. Shared model/tests and affected-owner review are required until the later migration. Dashboard
queries consume Cassandra views and do not need to decode raw Kafka JSON.
