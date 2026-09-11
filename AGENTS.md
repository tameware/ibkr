# Intended audience

These instructions have been created to help Cursor but are expected to be useful more broadly.

# Deployment

The code runs on a remote GCP server. It's drive is mounted locally. Its git directory is mounted at
`/Users/adamw/mnt/sideswap/src/ibkr/`
Always let me know if this directory is not accessible.

# Code

The running version is at `/Users/adamw/mnt/sideswap/src/ibkr/src/`.

# Logs

Latest logs can be found at `/Users/adamw/mnt/sideswap/src/ibkr/logs`.

# Ledgers

Strategy ledgers live under `/Users/adamw/mnt/sideswap/src/ibkr/ledgers/`
(or `ledgers/` in the local checkout). Filename pattern:
`{strategy}_{symbol}.json` (e.g. `market_maker_OZ.json`, `mm_peg_best_OZ.json`).

Each file is a JSON object written by `PositionLedger` in `src/ibkr_app_support.py`.
Field meanings are embedded in the file under `_docs` (rewritten on every `save()`).

Notable keys:

- `strategy_qty` – strategy book (from fills); legacy `qty` is migrated on load.
- `avg_cost_per_share` – VWAP for `strategy_qty`; legacy `avg_cost` migrated on load.
- `ib_snapshot_*` – last IB `position` callback (sell cap / reconciliation).

Notes:

- With `ignore_ledger=true`, trading uses IB position in memory; the file may still get
  `ib_snapshot_*` updates while `strategy_qty` / `avg_cost_per_share` stay stale.
- Sellable size is `min(strategy_qty, ib_snapshot_qty)` when a snapshot exists, else
  `strategy_qty`.
- Removed/ignored on save: `ib_snapshot_avg_cost` (IB avg cost is not stored).

The market maker ledger file is
`/Users/adamw/mnt/sideswap/src/ibkr/ledgers/market_maker_OZ.json`

# Test-driven development

For every code change, follow strict TDD:

1. Write or update automated tests that initially fail and describe the desired behavior and edge cases.
2. Write the minimal production code needed to make the new tests pass. Do not add untested functionality.
3. After tests are green, refactor tests and implementation for clarity and to remove duplication.
4. Run the relevant tests after each refactor and keep them green.
5. Repeat this red-green-refactor cycle in small steps.

Structure tests with Arrange-Act-Assert and use descriptive names that capture intent, including negative and boundary cases.

Never write production code first and wrap tests around it afterward. Tests must be written first and must fail before implementation.

Tests are not necessary for changes to documentation, unless the change is coupled to a code or configuration change.