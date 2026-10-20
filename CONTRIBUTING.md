# Contributing

## Workflow

1. Pick your issue (one task from [docs/implementation-plan.md](docs/implementation-plan.md), labelled `owner:X`).
2. Branch from `main`: `<member>/<task-id>-<short-name>`, lower case, e.g. `b/a3-binance-producer`,
   `d/a4-archive-downloader`, `e/a9-market-dashboard`.
3. Only edit files your task owns (header comment of each file, [.github/CODEOWNERS](.github/CODEOWNERS)). If you
   need something from another layer, ask its owner or open an issue.
4. Before pushing: `make lint test` (and `make demo` if you touched transforms).
5. Open a PR using the template. **One task per PR.** Link the issue (`Closes #12`).
6. At least **one review** (CODEOWNERS adds the right reviewer); **CI must pass**.
7. **Squash merge**; delete the branch.

## Contract changes

Contracts are the interfaces between layers: `src/cryptopipe/contracts.py`, `src/cryptopipe/schemas/`,
`conf/symbols.yaml`, `cql/schema.cql`, `docs/contracts.md`, and the `Settings` env vars.

* A contract change is its own PR that updates `docs/contracts.md`, the code and the tests together.
* It needs approval from the owners of **every affected layer** (CODEOWNERS requests all five for `contracts.py`).
* Announce it in the daily sync; open it the day you find the need, so nobody builds on the old version for long.
* Breaking message changes bump `schema_version`.

## Commits

Conventional commits: `feat:`, `fix:`, `docs:`, `test:`, `chore:`, `build:`, `refactor:`. The squash commit title is
the PR title, so make it one of those.

## Journal

When something breaks in an instructive way or an experiment produces numbers, add an entry to
`docs/journal/<category>.md` in the same PR (the PR template asks).

## Branch protection (repo admin, once)

Settings → Branches → `main`: require a pull request with 1 approval, require review from Code Owners, require
status checks `ci / test` to pass, disallow force pushes.
