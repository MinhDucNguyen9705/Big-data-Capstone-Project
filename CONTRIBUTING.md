# Contributing

## Workflow

1. Open or pick an issue for the change.
2. Branch from `main`: `<member>/<short-name>`, lower case, e.g. `b/binance-producer`, `e/market-dashboard`.
3. Only edit files your area owns (header comment of each file, [.github/CODEOWNERS](.github/CODEOWNERS) and the
   team table in [README.md](README.md)). If you need something from another layer, ask its owner or open an issue.
4. Before pushing: `make lint test` (and `make demo` if you touched transforms).
5. Open a PR using the template. **One change per PR.** Link the issue (`Closes #12`).
6. At least **one review** (CODEOWNERS adds the right reviewer); **CI must pass**.
7. **Squash merge**; delete the branch.

## Contract changes

Contracts are the interfaces between layers: `src/cryptopipe/contracts.py`, `src/cryptopipe/schemas/`,
`conf/symbols.yaml`, `cql/schema.cql`, and the `Settings` env vars in `src/cryptopipe/config.py` / `.env.example`.

* A contract change is its own PR that updates the code, the tests and the summary in `docs/architecture.md`
  (section 6) together.
* It needs approval from the owners of **every affected layer** (CODEOWNERS requests all five for `contracts.py`).
* Breaking message changes bump `schema_version`.

## Commits

Conventional commits: `feat:`, `fix:`, `docs:`, `test:`, `chore:`, `build:`, `refactor:`. The squash commit title is
the PR title, so make it one of those.

## Branch protection (repo admin, once)

Settings → Branches → `main`: require a pull request with 1 approval, require review from Code Owners, require
status checks `ci / test` to pass, disallow force pushes.
