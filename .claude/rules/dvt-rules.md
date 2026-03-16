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

## dbt Backward Compatibility

When using only 1 adapter with no cross-target references, DVT works identically to dbt.
