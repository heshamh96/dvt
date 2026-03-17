# DVT Core Rules

DVT (Data Virtualization Tool) is a fork of dbt-core with federated query capabilities. It uses **Sling** for cross-engine data extraction and **DuckDB** as a local compute/cache engine.

## Execution Philosophy

**Primary Directive**: DVT prioritizes Adapter Pushdown (SQL execution on the target DB) whenever possible. Federation via Sling extraction into DuckDB cache is only used when cross-engine queries are required.

## Target Resolution Hierarchy

1. **CLI** `--target` argument (highest priority, forces global override)
2. **Model config** `target` setting
3. **profiles.yml** default target (lowest priority)

## Execution Path Decision

For each model in the DAG:

- **Default Pushdown Path**: When model target == all upstream targets
  - Uses adapter to execute SQL directly on the database
  - No extraction needed

- **Non-Default Pushdown Path**: When model references multiple targets but all resolve to the same engine type
  - Uses adapter pushdown with cross-database references
  - Engine handles cross-schema/cross-database queries natively

- **Extraction Path (Federation)**: When model target != at least one upstream target (different engines)
  - Sling extracts source data from remote engines
  - Data is cached in local DuckDB
  - DuckDB executes the cross-engine SQL query
  - Sling loads the result into the target database

## Materialization Rules

### Same-Target (Pushdown)
- `table` → Materialized as table via adapter
- `view` → Materialized as view via adapter
- `incremental` → Incremental via adapter
- `ephemeral` → CTE injected into downstream queries

### Cross-Target (Federation via Sling + DuckDB)
- `table` → Sling extracts sources → DuckDB joins → Sling loads to target
- `view` → **COERCED TO TABLE** (cannot create cross-DB views)
- `incremental` → DuckDB calculates delta, Sling loads to target
- `ephemeral` → Resolved in DuckDB memory, passed downstream

## Key File Locations

| File | Location | Purpose |
|------|----------|---------|
| `dbt_project.yml` | Project root | Project configuration |
| `profiles.yml` | `~/.dvt/` | Database connections |
| `mdm.duckdb` | `~/.dvt/data/` | MDM database |
| `dvt_cache.duckdb` | `~/.dvt/cache/` | DuckDB federation cache |

## Key Source Files

| File | Purpose |
|------|---------|
| `dvt/federation/dvt_cache.py` | DuckDB cache management |
| `dvt/federation/duckdb_compute.py` | DuckDB compute engine |
| `dvt/federation/optimizer.py` | Query optimization (column pruning, predicate pushdown) |
| `dvt/runners/model_runner.py` | Model execution orchestration |
| `dvt/runners/seed_runner.py` | Seed loading |
| `dvt/cli/main.py` | CLI entry point |

## Two Dialects in One Project

A DVT project naturally contains SQL in two dialects:

- **Pushdown models** → written in the **target's native SQL dialect** (Snowflake SQL, PostgreSQL, etc.). SQL runs on the target engine via the dbt adapter.
- **Extraction models** → written in **DuckDB SQL** (universal, Postgres-like). SQL runs in the local DuckDB cache after Sling extracts remote sources.

Both coexist in the same project. The dialect is determined by the execution path, not by config. This is the most important thing to understand about DVT model authoring.

## `--target` Philosophy

`--target` switches between **same-engine environments**, not between engines.

- **Correct**: `--target dev_snowflake` → `--target prod_snowflake` (both Snowflake)
- **Risky**: `--target dev_snowflake` → `--target mysql_docker` (different engine — pushdown models will break)

When `--target` changes the adapter type, DVT emits **DVT007** warning but does not block. Extraction models (DuckDB SQL) are unaffected. Pushdown models (target dialect) will fail if the new engine cannot parse the original dialect's syntax.

## Sling Invisibility

Sling output must **never** be shown to the user during normal operation. The extraction path (Sling → DuckDB → Sling) is a black box. DVT shows standard dbt-like output only. Sling errors are caught and re-raised as DVT errors with actionable messages.

## Seed Robustness

DVT's Sling-based seed loader must handle dirty CSV data at least as gracefully as dbt's agate-based loader: encoding issues, mixed types, null handling, schema inference. Seeds must "just work."

## dbt Backward Compatibility

When using only 1 adapter with no cross-target references, DVT works identically to dbt.
