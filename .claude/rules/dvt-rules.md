# DVT Core Rules

DVT (Data Virtualization Tool) is a fork of dbt-core with federated query capabilities.

## Execution Philosophy

**Primary Directive**: DVT prioritizes Adapter Pushdown (SQL execution on the target DB) whenever possible. Federation via Spark is only used when cross-engine queries are required.

## Compute Resolution Hierarchy

1. **CLI** `--compute` argument (highest priority)
2. **Model config** `compute` setting
3. **computes.yml** default compute engine (lowest priority)

If a specified compute engine doesn't exist in the registry, DVT raises a compilation error.

## Target Resolution Hierarchy

1. **CLI** `--target` argument (highest priority, forces global override)
2. **Model config** `target` setting
3. **profiles.yml** default target (lowest priority)

## Execution Path Decision

For each model in the DAG:

- **Pushdown Path**: When model target == all upstream targets
  - Uses adapter to execute SQL directly on the database
  - Compute engine setting is ignored

- **Federation Path**: When model target != at least one upstream target
  - Uses Spark with JDBC connectors
  - Predicate pushdown optimizes network transfer

## Materialization Rules

### Same-Target (Pushdown)
- `table` → Materialized as table via adapter
- `view` → Materialized as view via adapter
- `incremental` → Incremental via adapter
- `ephemeral` → CTE injected into downstream queries

### Cross-Target (Federation)
- `table` → Spark writes table via JDBC
- `view` → **COERCED TO TABLE** (cannot create cross-DB views)
- `incremental` → Spark calculates delta, merges via JDBC
- `ephemeral` → Resolved in Spark memory, passed downstream

## Key File Locations

| File | Location | Purpose |
|------|----------|---------|
| `dbt_project.yml` | Project root | Project configuration |
| `profiles.yml` | `~/.dvt/` | Database connections |
| `computes.yml` | `~/.dvt/` | Spark compute configs |
| `mdm.duckdb` | `~/.dvt/data/` | MDM database |

## dbt Backward Compatibility

When using only 1 adapter with no cross-target references, DVT works identically to dbt.
