# DVT Testing Factory: The 6 Test Types

## Test Pyramid Overview

| Level | Type | Speed | Isolation | What It Catches |
|-------|------|-------|-----------|----------------|
| 1 | Unit Tests | <1min | Full (mocks) | Logic bugs, contract violations, parsing errors |
| 2 | Integration Tests | ~5min | Partial (real PG) | Runtime errors, SQL generation bugs, graph resolution |
| 3 | CLI Command Tests | ~10min | Process-level | Flag handling, exit codes, help text, arg validation |
| 4 | Adapter Compatibility | ~15min | Docker containers | Adapter-specific SQL dialects, DDL differences, type mapping |
| 5 | UAT E2E | ~30min | Real production DBs | Federation via Sling+DuckDB, multi-target, incremental strategies, data integrity |
| 6 | Cross-OS | ~20min | Docker distros | Package install, system deps, libc compat, Sling binary availability |

## Type 1: Unit Tests

**Purpose**: Verify individual functions, classes, and contracts work correctly in isolation.

**Location**: `dvt-ce/tests/unit/`

**What it catches**:
- Logic bugs in parsers, config resolvers, graph builders
- Contract violations (dataclass schemas, manifest structure)
- Event serialization issues
- Edge cases in string handling, path resolution
- DVT-specific: federation event types, DuckDB cache management, extraction optimizer

**What it does NOT catch**:
- SQL generation correctness against real databases
- Cross-component integration issues
- CLI wiring problems
- Adapter-specific behavior

**When to run**: After every code change. Must pass before any commit.

**How to run**:
```bash
cd dvt-ce/core
.venv/bin/python -m pytest ../tests/unit/ -v                    # All
.venv/bin/python -m pytest ../tests/unit/federation/ -v          # Federation only
```

## Type 2: Integration Tests

**Purpose**: Test the DVT runtime end-to-end against a real database, but in-process (not via CLI).

**Location**: `dvt-ce/tests/functional/`

**What it catches**:
- SQL compilation errors that only manifest against a real DB
- Graph resolution and execution ordering bugs
- Materialization logic (table, view, incremental)
- Adapter connection lifecycle issues

**What it does NOT catch**:
- CLI flag handling
- Multi-database federation (tests run against single PG)
- Cross-OS packaging issues

**Current state**: Inherited from upstream dbt-core. Tests run against PostgreSQL (localhost:5433). Not yet extended for DVT-specific features (Sling extraction, DuckDB cache, multi-target).

**When to run**: After changes to core runtime (compilation, execution, graph resolution, materialization).

**How to run**:
```bash
cd dvt-ce/core
hatch run setup-db                                              # Start PG via docker-compose
.venv/bin/python -m pytest ../tests/functional/ -v              # All
```

## Type 3: CLI Command Tests

**Purpose**: Systematically test every DVT CLI command with all flags and argument combinations.

**Location**: `Testing_Factory/Testing_DVT_cli_commands/`

**What it catches**:
- Missing or broken CLI flags
- Wrong exit codes (success when should fail, vice versa)
- Help text regressions
- Argument validation gaps (invalid --target, missing --project-dir)
- Command wiring issues (Click decorator mistakes)

**What it does NOT catch**:
- Internal logic correctness (that's unit tests)
- Data integrity (that's UAT)
- Cross-database behavior (that's adapter tests)

**Format**: Dual format per command:
- `test_<command>.sh` — Shell script for quick manual verification
- `test_<command>.py` — pytest file with subprocess assertions for CI

**When to run**: After CLI changes, flag additions, command wiring changes, or Click decorator modifications.

**How to run**:
```bash
cd Testing_Factory/Testing_DVT_cli_commands
pytest -v                    # All commands via pytest
bash run_all.sh              # All commands via shell scripts
bash test_debug.sh           # Single command
pytest test_debug.py -v      # Single command via pytest
```

## Type 4: Adapter Compatibility Tests

**Purpose**: Verify DVT works correctly with each database engine's specific SQL dialect, DDL syntax, and type system. All 13 engines are in the dvt-adapters package.

**Location**: `Testing_Factory/Testing_adapters_docker/`

**What it catches**:
- Adapter-specific SQL generation bugs (quoting, type casting, DDL)
- Missing system dependencies (e.g., FreeTDS for SQL Server, Oracle client)
- Connection parameter handling differences
- Materialization differences across engines (e.g., TEMPORARY tables, CTAS syntax)
- Sling extraction compatibility per engine

**What it does NOT catch**:
- Cross-database federation (containers are isolated engines)
- Production-scale performance
- Cloud-specific auth (Databricks tokens, Snowflake key-pair)

**Engines**: PostgreSQL 16, MySQL 8.0, SQL Server 2022, Oracle XE 21c, MariaDB 11 (Docker) + pg_dev, Snowflake, Databricks (external)

**When to run**: After adapter upgrades, before adding support for a new database engine, after DVT runtime changes that affect SQL generation.

**How to run**:
```bash
cd Testing_Factory/Testing_adapters_docker
docker compose up -d                    # Start all engines
bash run_adapter_tests.sh               # Run DVT against all engines
bash run_adapter_tests.sh postgres      # Single engine
docker compose down                     # Cleanup
```

## Type 5: UAT E2E Tests

**Purpose**: Final quality gate. Exercises every DVT feature against real production-like databases including federation via Sling+DuckDB, incremental strategies, extraction optimization, and multi-target overrides.

**Location**: `Testing_Factory/Testing_Playground/`

**What it catches**:
- Federation path correctness (Sling extraction -> DuckDB cache -> DuckDB compute -> Sling loading)
- Incremental strategy correctness (merge, delete+insert, append)
- Cross-target data integrity (PG -> DBX -> SF data matches)
- DDL contract compliance (TRUNCATE+INSERT vs DROP+CREATE+INSERT)
- Extraction optimization (column pruning, predicate pushdown)
- `{{ this }}` resolution across federation paths
- Multi-target override via `--target` flag

**What it does NOT catch**:
- OS-specific packaging issues (that's cross-OS)
- Adapter-specific edge cases for engines not in the target set (that's adapter tests)

**Protocol**: 10+ phases defined in the uat-e2e skill. Produces a ship/no-ship recommendation.

**Targets**: pg_dev (Postgres), dbx_dev (Databricks), sf_dev (Snowflake read-only), disf_dev (Snowflake writable)

**When to run**: Before any release or major merge to `uat`/`master` branch.

**How to run**: Use the `/trial` skill to create a new trial, then the `/uat-e2e` skill to execute the protocol.

## Type 6: Cross-OS Tests

**Purpose**: Verify the published dvt-ce + dvt-adapters packages install and work correctly on multiple Linux distributions, catching OS-specific issues.

**Location**: `Testing_Factory/Testing_OSs_docker/`

**What it catches**:
- Package install failures on specific distros (dependency resolution, wheel availability)
- System library incompatibilities (glibc vs musl, OpenSSL versions)
- Python version differences (system Python vs deadsnakes/pyenv)
- Sling binary availability and compatibility per OS
- pip/uv behavior differences across package managers

**What it does NOT catch**:
- Logic bugs (that's unit tests)
- Database-specific issues (that's adapter tests)
- macOS-specific issues (that's local development)

**Distros**: Ubuntu 22.04, Ubuntu 24.04, Debian 12, Rocky 9, Alpine 3.19, Fedora, Arch

**When to run**: Before publishing a new dvt-ce version to PyPI.

**How to run**:
```bash
cd Testing_Factory/Testing_OSs_docker
docker compose up --build               # Build and test all distros
docker compose up --build ubuntu-24.04  # Single distro
```
