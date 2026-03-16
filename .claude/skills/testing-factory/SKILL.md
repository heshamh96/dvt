---
name: testing-factory
description: DVT Testing Factory — orchestrate all 6 test types
arguments:
  - name: type
    description: "Test type: unit, integration, cli, adapters, uat, os, all"
    required: true
  - name: subarg
    description: "Sub-argument: command name, distro, trial number, etc."
    required: false
---

# Skill: testing-factory

Master orchestrator for all DVT testing. Routes to the correct test workflow based on the requested test type.

## Usage

```
/testing-factory unit                  # Run unit tests (delegates to /test skill)
/testing-factory integration           # Run integration tests against local PG
/testing-factory cli                   # Run ALL CLI command tests
/testing-factory cli debug             # Run CLI tests for a specific command
/testing-factory adapters              # Run adapter tests against all 13 engines + federation
/testing-factory adapters postgres     # Run adapter tests for a specific engine
/testing-factory adapters docker       # Run adapter tests for 5 Docker engines only
/testing-factory adapters external     # Run adapter tests for external engines only
/testing-factory adapters federation   # Run cross-engine federation tests only
/testing-factory uat                   # Run full UAT E2E (delegates to /uat-e2e skill)
/testing-factory uat trial_18          # Run UAT on a specific trial
/testing-factory os                    # Run cross-OS tests against all distros
/testing-factory os ubuntu-24.04       # Run cross-OS test for a specific distro
/testing-factory all                   # Run everything in sequence
```

## Directory Layout

```
Testing_Factory/                                          # /Users/hex/Documents/My_Projects/DVT/Testing_Factory/
├── Testing_Playground/                                   # Type 5: UAT E2E trials
│   ├── trial_18_all_commands/                             #   Latest trial (all engines, 67 models)
│   ├── trial_17_sync_uat/                                #   Previous sync UAT
│   └── ...                                               #   Historical trials
├── Testing_DVT_cli_commands/                             # Type 3: CLI command tests
│   ├── conftest.py                                       #   Shared pytest fixtures
│   ├── run_all.sh                                        #   Run all CLI tests
│   └── test_<command>.py + test_<command>.sh              #   Per-command test pairs
├── Testing_adapters_docker/                              # Type 4: Adapter compatibility
│   ├── docker-compose.yml                                #   MySQL, PG, MSSQL, Oracle, MariaDB, Adminer
│   ├── .env                                              #   DB credentials
│   ├── test_project/                                     #   Minimal DVT project for adapter tests
│   └── run_adapter_tests.sh                              #   Orchestrator script
└── Testing_OSs_docker/                                   # Type 6: Cross-OS tests
    ├── docker-compose.yml                                #   All 7 distro containers
    ├── dvt_test_suite.sh                                 #   Test script copied into containers
    ├── ubuntu-22.04/Dockerfile                           #   Ubuntu 22.04 LTS
    ├── ubuntu-24.04/Dockerfile                           #   Ubuntu 24.04 LTS
    ├── debian-12/Dockerfile                              #   Debian Bookworm
    ├── rocky-9/Dockerfile                                #   Rocky Linux 9 (RHEL-compat)
    ├── alpine-3.19/Dockerfile                            #   Alpine (musl libc edge case)
    ├── fedora/Dockerfile                                 #   Fedora latest
    └── arch/Dockerfile                                   #   Arch Linux

dvt-ce/                                                 # /Users/hex/Documents/My_Projects/DVT/dvt-ce/
├── tests/unit/                                           # Type 1: Unit tests (pytest, no DB)
└── tests/functional/                                     # Type 2: Integration tests (pytest + PG)
```

## Path Constants

```
TESTING_FACTORY   = /Users/hex/Documents/My_Projects/DVT/Testing_Factory
TESTING_PLAYGROUND= /Users/hex/Documents/My_Projects/DVT/Testing_Factory/Testing_Playground
CLI_COMMANDS_DIR  = /Users/hex/Documents/My_Projects/DVT/Testing_Factory/Testing_DVT_cli_commands
ADAPTERS_DIR      = /Users/hex/Documents/My_Projects/DVT/Testing_Factory/Testing_adapters_docker
OS_DIR            = /Users/hex/Documents/My_Projects/DVT/Testing_Factory/Testing_OSs_docker
DVT_CORE          = /Users/hex/Documents/My_Projects/DVT/dvt-ce
DVT_ADAPTERS      = /Users/hex/Documents/My_Projects/DVT/dvt-adapters
UNIT_TESTS        = /Users/hex/Documents/My_Projects/DVT/dvt-ce/tests/unit
FUNCTIONAL_TESTS  = /Users/hex/Documents/My_Projects/DVT/dvt-ce/tests/functional
```

## The 6 Test Types

See `reference/test-types.md` for detailed explanations of each test type, what it catches, and when to run it.

### Type 1: Unit Tests (`/testing-factory unit`)

**Location**: `dvt-ce/tests/unit/`
**Runner**: `cd dvt-ce/core && .venv/bin/python -m pytest ../tests/unit/ -v`
**Delegates to**: `/test` skill

Fast, isolated, no DB required. Tests individual functions, classes, contracts, parsers.

Run after every code change. These are the first line of defense.

### Type 2: Integration Tests (`/testing-factory integration`)

**Location**: `dvt-ce/tests/functional/`
**Runner**: `cd dvt-ce/core && .venv/bin/python -m pytest ../tests/functional/ -v`
**Requires**: PostgreSQL running at localhost:5433

Tests the DVT runtime end-to-end in-process (not via CLI) using pytest fixtures. Inherited from upstream dbt-core. Tests model compilation, execution, and result verification against a real database.

Run after changes to core runtime (compilation, execution, graph resolution).

### Type 3: CLI Command Tests (`/testing-factory cli [command]`)

**Location**: `Testing_Factory/Testing_DVT_cli_commands/`
**Runner**: `cd Testing_DVT_cli_commands && pytest -v` or `bash run_all.sh`

Systematic test of every DVT CLI command with all flags and argument combinations. Each command gets a paired `.sh` (manual quick-run) + `.py` (pytest with assertions).

See `reference/cli-commands.md` for the full command/flag matrix.

**Commands tested** (19 total):
`build`, `clean`, `clone`, `compile`, `debug`, `deps`, `docs generate`, `init`, `list`, `parse`, `retry`, `run`, `run-operation`, `seed`, `show`, `snapshot`, `source freshness`, `sync`, `test`

Run after CLI changes, flag additions, or command wiring changes.

### Type 4: Adapter Compatibility Tests (`/testing-factory adapters [db]`)

**Location**: `Testing_Factory/Testing_adapters_docker/`
**Runner**: `bash run_adapter_tests.sh [engine|docker|external|federation|all]`
**Requires**: Docker (for Docker engines), network access (for cloud engines)

Tests DVT against 13 database engines (from dvt-adapters) to verify adapter compatibility AND cross-engine federation via Sling extraction into DuckDB cache.

**Docker Engines** (5, managed by docker-compose):
| Engine | Service | Target | Port | Adapter |
|--------|---------|--------|------|---------|
| PostgreSQL 16 | postgres | pg_docker | 5432 | dvt-adapters[postgres] |
| MySQL 8.0 | mysql | mysql_docker | 3306 | dvt-adapters[mysql] |
| SQL Server 2022 | sqlserver | mssql_docker | 1433 | dvt-adapters[sqlserver] |
| Oracle XE 21c | oracle | oracle_docker | 1521 | dvt-adapters[oracle] |
| MariaDB 11 | mariadb | mariadb_docker | 3307 | dvt-adapters[mysql] |

**External Engines** (3, already running outside Docker):
| Engine | Target | Type | Notes |
|--------|--------|------|-------|
| PostgreSQL (Homebrew) | pg_dev | Local | Port 5433, Homebrew-managed |
| Snowflake | disf_dev (write) / sf_dev (read) | Cloud | sf_dev is READ ONLY |
| Databricks | dbx_dev | Cloud | SQL warehouse |

**Phase A — Single-Engine Pushdown** (per engine):
1. `dvt debug` — verify connection
2. `dvt seed` — load test data
3. `dvt run` — execute pushdown models (SQL on target DB)
4. `dvt run --full-refresh` — verify DDL contract (DROP+CREATE+INSERT)

**Phase B — Cross-Engine Federation** (federation models):
All Docker containers + external engines must be accessible. Federation models read from one engine, Sling extracts to DuckDB cache, DuckDB executes the join, and Sling loads to the target.

Run after adapter upgrades, Sling updates, federation engine changes, or before adding support for a new database engine.

### Type 5: UAT E2E Tests (`/testing-factory uat [trial]`)

**Location**: `Testing_Factory/Testing_Playground/`
**Delegates to**: `/uat-e2e` skill

Comprehensive end-to-end test against real production-like databases (Postgres, Databricks, Snowflake). 10+ phases covering every DVT feature: federation via Sling+DuckDB, incremental strategies, extraction optimization, multi-target overrides, data validation.

This is the final quality gate before shipping. Produces a ship/no-ship recommendation.

**Targets**: pg_dev, dbx_dev, sf_dev (read-only), disf_dev (writable)

Run before any release or major merge to `uat`/`master`.

### Type 6: Cross-OS Tests (`/testing-factory os [distro]`)

**Location**: `Testing_Factory/Testing_OSs_docker/`
**Runner**: `bash run_os_tests.sh [distro]` or `docker compose up --build`
**Requires**: Docker, published dvt-ce + dvt-adapters on PyPI (or local wheels)

Tests the published packages on multiple Linux distributions to catch OS-specific issues (system library differences, Python version compatibility, Sling binary availability).

**Distros** (7 targets):
| Distro | Base | Python | libc | Notes |
|--------|------|--------|------|-------|
| Ubuntu 22.04 | Debian | 3.12+ (deadsnakes) | glibc | LTS server standard |
| Ubuntu 24.04 | Debian | 3.12 (system) | glibc | Latest LTS |
| Debian 12 | - | 3.12+ (deadsnakes) | glibc | Minimal, common Docker base |
| Rocky Linux 9 | RHEL | 3.12+ (AppStream) | glibc | Enterprise/RHEL compat |
| Alpine 3.19 | - | 3.12 (system) | **musl** | Edge case: musl libc |
| Fedora | - | 3.12+ (system) | glibc | Cutting-edge packages |
| Arch | - | 3.12+ (system) | glibc | Rolling release |

Each container:
1. Installs Python 3.12+, pip
2. `pip install dvt-ce dvt-adapters[postgres]` (from PyPI or local wheel)
3. Installs Sling binary
4. Copies test project
5. Connects to Testing_adapters_docker containers (shared Docker network)
6. Runs: `dvt debug`, `dvt seed`, `dvt run`, `dvt run --full-refresh`
7. Validates data matches macOS baseline

Run before publishing a new dvt-ce version to PyPI.

## Test Pyramid

```
                    /\
                   /  \          Type 5: UAT E2E (manual, 4 real targets)
                  / 5  \         Type 6: Cross-OS (Docker, published pkg)
                 /------\
                /   6    \       Type 4: Adapter Compat (Docker DBs)
               / 4     3  \     Type 3: CLI Commands (all flags)
              /------------\
             /    2         \    Type 2: Integration (pytest + real DB)
            /       1        \   Type 1: Unit Tests (fast, isolated)
           /------------------\
```

**Run frequency**:
- Type 1 (unit): Every code change
- Type 2 (integration): After runtime changes
- Type 3 (CLI): After CLI/flag changes
- Type 4 (adapters): After adapter upgrades
- Type 5 (UAT): Before releases
- Type 6 (cross-OS): Before PyPI publish

## Workflow: `/testing-factory all`

Runs all test types in order (bottom of pyramid first):

```bash
# 1. Unit tests
cd dvt-ce/core && .venv/bin/python -m pytest ../tests/unit/ -v

# 2. Integration tests
cd dvt-ce/core && .venv/bin/python -m pytest ../tests/functional/ -v

# 3. CLI command tests
cd Testing_Factory/Testing_DVT_cli_commands && pytest -v

# 4. Adapter compatibility tests
cd Testing_Factory/Testing_adapters_docker && bash run_adapter_tests.sh

# 5. UAT E2E (delegate to /uat-e2e skill with latest trial)
# Create new trial via /trial skill, then run /uat-e2e

# 6. Cross-OS tests
cd Testing_Factory/Testing_OSs_docker && docker compose up --build
```

Stop at the first type that fails. Fix before proceeding up the pyramid.

## Critical Rules

1. **sf_dev is READ ONLY** — never write to it. Use disf_dev for writable Snowflake.
2. **DDL contract**: `dvt run` = TRUNCATE+INSERT; `dvt run --full-refresh` = DROP+CREATE+INSERT.
3. **No column name sanitization** — use proper quoting for all dialects.
4. **No fallbacks** — one method, works right.
5. **Test data must be deterministic** — use ORDER BY + LIMIT for traceable verification.
6. **Record everything** — all findings go to `findings/` directories.
