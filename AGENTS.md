# Project working context

Read [docs/project-context.md](docs/project-context.md) for the user's goals, architecture, source documents, checkpoint scope and unresolved differences between plans.

- Repository: `https://github.com/MinhDucNguyen9705/Big-data-Capstone-Project`.
- The user requested branch `speed_layer`, created from `main` at `77acea0` on 2026-10-21. Check the actual branch before edits; do not recreate or reset it automatically.
- The project uses Lambda Architecture. The current working focus is the speed layer; this does not authorize changes to every team's area.
- The user identified `docs/plans/implementation-plan.md` as the overall plan and `docs/plans/bootstrap-plan.md` as their immediate checkpoint preparation plan. These are preserved user-provided originals. Existing `docs/implementation-plan.md` and `docs/bootstrap-plan.md` are repository adaptations, with some differences.
- Prioritize the agreed checkpoint work before Part B features. The checkpoint is next week relative to 2026-10-21 (2026-10-26 through 2026-11-01, Vietnam time); the exact date is not confirmed.
- Read `CLAUDE.md`, `docs/contracts.md`, relevant source and ownership comments before implementation. Follow the repository's existing coding conventions and contracts. Plan descriptions are specifications, not proof that code or infrastructure works.
- Python 3.11, Java 17, PySpark 3.5; package under `src/cryptopipe/`. Keep Spark jobs thin and transforms testable as `DataFrame -> DataFrame` functions.
- Read endpoints and credentials from `cryptopipe.config.Settings`; read topics, lake paths, table names and trade keys from `cryptopipe.contracts`. Reuse shared Pydantic models and Spark schemas.
- Event time is `event_ts` in UTC milliseconds; ingestion time is `ingest_ts`. Dedupe trades by `(exchange, symbol, trade_id)`.
- Add meaningful tests for new transforms, reuse shared fixtures, and run `make lint test` for implementation changes. Record actual integration evidence separately from local fixture checks.
- Respect layer ownership. Contract changes require an accompanying `docs/contracts.md` update and review by affected owners through the normal PR workflow.
- Record real lessons and measurements under `docs/journal/`. Do not invent completed checks, dataset sizes or performance results.
- Saving these plans does not itself request executing S1-S10, creating issues, committing, pushing or opening PRs. Continue with the user's current task.
- Communicate with the user in Vietnamese by default.
