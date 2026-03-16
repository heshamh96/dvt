# DVT-CE — Claude Code Context

## What Is DVT

DVT (Data Virtualization Tool) is a cross-engine data transformation tool built on top of dbt-core 1.9.10. It lets users write SQL models that reference sources on any database, automatically handles cross-engine data movement, and materializes results to any target.

## Two Packages

- **dvt-ce** (`core/`) — provides `dbt.*` + `dvt.*` namespaces. The dbt-core internals are kept as `dbt.*` (not renamed) for adapter compatibility. DVT extensions live in `dvt.*`.
- **dvt-adapters** (separate repo at `../dvt-adapters`) — provides `dbt.adapters.*` + `dbt.include.*`. Contains ALL 13 engine adapters in one package: postgres, snowflake, bigquery, redshift, spark, databricks, duckdb, sqlserver, mysql, mysql5, mariadb, oracle, fabric.

## Architecture

```
Execution flow:
  1. dbt compiles model (Jinja → SQL in target dialect)
  2. DVT rewrites source refs → DuckDB cache table names
  3. Federation optimizer decomposes → per-source extraction queries
     (predicate pushdown + LIMIT, transpiled to source dialect via SQLGlot)
  4. Sling extracts each source → .dvt/cache.duckdb
  5. DuckDB executes rewritten model SQL
  6. Sling loads result → target (default or non-default)

Three execution paths:
  Default pushdown:     source.connection == default target → adapter SQL
  Non-default pushdown: source.connection == non-default target → DuckDB cache → target
  Extraction:           source.connection != model target → DuckDB cache → target
```

## Key Directories

```
core/
  dbt/                              # Upstream dbt-core 1.9.10 (mostly untouched)
  dvt/                              # DVT extension layer
    cli/
      __init__.py                   # Resilient entry point (sync works even when dbt broken)
      main.py                       # Full CLI: all dbt commands + dvt sync
    tasks/
      run.py                        # DvtRunTask — manages DvtCache, resolves execution paths
      build.py                      # DvtBuildTask
      seed.py                       # DvtSeedTask — Sling-based CSV loading
      show.py                       # DvtShowTask — DuckDB local queries
      debug.py                      # DvtDebugTask — multi-connection health check
      sync.py                       # DvtSyncTask — env bootstrap + self-healing
    config/
      source_connections.py         # Reads connection: from sources.yml
      target_resolver.py            # Classifies models into execution paths
    extraction/
      connection_mapper.py          # 16 adapter types → Sling connection URLs
      watermark_formatter.py        # 13 dialects × 4 types (timestamp, date, int, string)
      sling_client.py               # Sling Python wrapper (lazy import)
    runners/
      model_runner.py               # DvtModelRunner — pushdown vs extraction dispatch
      seed_runner.py                # DvtSeedRunner — Sling CSV loading
    federation/
      dvt_cache.py                  # Persistent DuckDB cache (.dvt/cache.duckdb)
      optimizer.py                  # Federation optimizer (decompose, predicate pushdown, SQLGlot)
    sync/
      profiles_reader.py            # Read profiles.yml
      adapter_installer.py          # Install database drivers
      duckdb_extensions.py          # Install DuckDB extensions
      cloud_deps.py                 # Install cloud SDKs
      sling_checker.py              # Verify/bootstrap Sling binary
    exceptions.py                   # DVT error codes (DVT001-DVT109)

hesham_mind/                        # Architecture docs + implementation plan
  IMPLEMENTATION_PLAN.md            # Shared memory — what's done, what's next
  DVT_RULES.md                      # Authoritative ruleset
  ARCHITECTURE.md                   # System design
  EXECUTION_PATHS.md                # The 3 execution paths
  CONFIGURATION.md                  # profiles.yml, sources.yml, model config
  SLING_INTEGRATION.md              # How Sling is used
  DUCKDB_ROLE.md                    # DuckDB's role (cache + dvt show)
  COMPETITIVE_CONTEXT.md            # How DVT relates to competitors
  CLI_COMMANDS.md                   # All dvt commands
```

## DVT Commands

| Command | What it does |
|---------|-------------|
| `dvt run` | Extract sources, transform models, load to targets |
| `dvt build` | Seeds + models + snapshots + tests in DAG order |
| `dvt seed` | Load CSVs via Sling (10-100x faster than dbt) |
| `dvt show` | Local DuckDB query (ATTACH to PG/MySQL/MariaDB) |
| `dvt debug` | Test all connections (or `--target X` for one) |
| `dvt sync` | Self-healing env bootstrap (drivers, DuckDB, Sling) |
| `dvt compile/test/snapshot/docs/deps/init/clean/list` | Stock dbt passthrough |

## DuckDB Cache

```
.dvt/cache.duckdb (persistent, per-project)
  Source tables: {source}__{table} (SELECT *, shared across models)
  Model results: __model__{name} (for incremental {{ this }})
  --full-refresh destroys it. dvt clean deletes .dvt/.
```

## Key Design Decisions

- Internal namespace: `dbt.*` (not renamed) — compatible with dbt adapters
- Adapter consolidation: `dvt-adapters` (one package, 13 engines)
- Extraction: Sling for data movement, DuckDB for compute
- Extraction models: DuckDB SQL dialect
- Incremental: watermark from TARGET, delta extraction via Sling
- Cache: per-source (shared), persistent between runs
- Federation optimizer: predicate pushdown + LIMIT transpilation via SQLGlot
- Profiles: `~/.dvt` then `~/.dbt` fallback

## Testing

- Test project: `Testing_Factory/Testing_Playground/trial_19_dvt_ce_pypi/Coke_DB`
- 5 Docker databases: pg, mysql, mssql, oracle, mariadb
- Remote: 2x Snowflake, 1x Postgres, 1x Databricks
- 12/12 models pass (5 pushdown + 6 extraction + 1 incremental)
- 27/27 nodes in dvt build
- 9/9 connections in dvt debug

## Git Workflow

- Main branch: `new_dev`
- Development: feature branches → PRs to `new_dev`
- Testing: trial projects in `Testing_Factory/Testing_Playground/`
- Trial setup: `pyproject.toml` with `[tool.uv.sources]` pointing to local editable packages
