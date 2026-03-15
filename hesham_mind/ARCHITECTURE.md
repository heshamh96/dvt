# DVT-CE Architecture

## What DVT Is

DVT is a cross-engine data transformation tool built on top of dbt-core.
It combines three proven technologies:

- **dbt-core** — DAG orchestration, SQL models, Jinja, ref(), source(), testing, docs, adapters
- **Sling** — high-performance data movement (EL) across 30+ databases, file systems, and cloud buckets
- **DuckDB** — lightweight local analytics engine for `dvt show`, local dev, and virtual federation

The user experience is identical to dbt: write SQL models, declare sources, run `dvt run`.
The difference: DVT automatically handles cross-engine data movement so models can reference
sources on ANY database, and materialize results to ANY target (including cloud buckets).

## Core Principles

1. **dbt internals are NEVER modified.** DVT extends dbt via subclassing (DvtRunTask extends RunTask, DvtModelRunner extends ModelRunner). This allows rebasing on upstream dbt-core with minimal conflicts.

2. **The target database is the compute engine.** Models push down to the target via dbt adapters. DVT does NOT run model SQL in DuckDB (except for `dvt show`). The warehouse does the heavy lifting.

3. **Sling handles ALL data movement.** Cross-engine extraction, loading to targets, seeding CSV files, cross-target materialization, CDC — all via Sling. No custom JDBC code, no PySpark, no intermediate staging files managed by DVT.

4. **User-written extraction models, NOT auto-generated nodes.** Users write standard dbt models with a `connection` config to extract data from remote sources via Sling. This follows the dbt "base views" / "staging models" pattern. Users control naming, SQL, filtering, and incremental logic. DVT does NOT auto-generate hidden DAG nodes.

5. **Cross-engine incremental models work seamlessly.** dbt's `is_incremental()` macro works across engines. DVT pre-resolves the watermark value from the target, formats it as a dialect-specific literal for the source engine, and substitutes it into the extraction query before Sling executes it.

6. **DuckDB is scoped to three use cases.** `dvt show` (local queries), local file/API processing, and virtual federation. It is NOT the core transform engine.

## System Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           DVT PROJECT                                   │
│                                                                         │
│  profiles.yml          sources.yml           models/                    │
│  ┌──────────────┐     ┌──────────────┐      ┌──────────────┐           │
│  │ target: sf   │     │ crm:         │      │ stg_customers │           │
│  │ outputs:     │     │  conn: pg    │      │  connection:pg│           │
│  │  sf: {...}   │     │  tables:     │      │ stg_orders    │           │
│  │  pg: {...}   │     │   -customers │      │  connection:pg│           │
│  │  s3: {...}   │     │   -orders    │      │  incremental  │           │
│  │  gcs: {...}  │     │              │      │ dim_customers │           │
│  └──────────────┘     └──────────────┘      │ fct_orders    │           │
│                        (metadata only,       │  target=s3    │           │
│                         no sling config)     └──────────────┘           │
└─────────────────────────────────────────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                          DVT ENGINE                                     │
│                                                                         │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                     DAG Resolution                               │   │
│  │                                                                   │   │
│  │  1. Parse sources.yml (metadata: connection + tables)             │   │
│  │  2. Parse models/ → detect `connection` config on models         │   │
│  │  3. Build DAG: extraction models → pushdown models → tests       │   │
│  │  4. Resolve targets + execution paths per model                  │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                               │                                         │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                     Execution (per node in DAG order)            │   │
│  │                                                                   │   │
│  │  EXTRACTION MODEL (has `connection` config):                       │   │
│  │    DVT compiles model SQL (Jinja → SQL in source dialect)         │   │
│  │    For incremental: pre-resolves watermark, dialect-specific lit. │   │
│  │    Sling executes compiled SQL on SOURCE, streams to TARGET       │   │
│  │    Sling handles merge strategy for incremental models            │   │
│  │    Result: physical table on the model's target                   │   │
│  │                                                                   │   │
│  │  PUSHDOWN MODEL (no `connection` config, target == default):      │   │
│  │    dbt adapter pushes down SQL to the target                      │   │
│  │    Standard dbt materialization (table/view/incremental/ephemeral)│   │
│  │                                                                   │   │
│  │  CROSS-TARGET MODEL (no `connection`, target != default):         │   │
│  │    dbt adapter runs SQL on default target                         │   │
│  │    Sling streams result → model's configured target               │   │
│  │    Target may be another DB or a cloud bucket (Delta/Parquet/CSV) │   │
│  │                                                                   │   │
│  │  SEED NODE:                                                       │   │
│  │    Sling loads CSV → target (replaces dbt's slow Python seed)     │   │
│  │    Supports --full-refresh and --target overrides                 │   │
│  │                                                                   │   │
│  │  TEST NODE:                                                       │   │
│  │    Standard dbt test execution on the target                      │   │
│  └─────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────┘
                               │
                    ┌──────────┼──────────┐
                    ▼          ▼          ▼
              ┌──────────┐ ┌───────┐ ┌──────────┐
              │ Databases│ │Buckets│ │  DuckDB  │
              │          │ │       │ │  (local)  │
              │Snowflake │ │  S3   │ │dvt show   │
              │Postgres  │ │  GCS  │ │dvt debug  │
              │BigQuery  │ │ Azure │ │local files│
              │Redshift  │ │       │ │           │
              │MySQL     │ │       │ │           │
              │...       │ │       │ │           │
              └──────────┘ └───────┘ └──────────┘
```

## Project Structure

```
core/
  dbt/                              # Upstream dbt-core 1.9.10 (UNTOUCHED)
    cli/
    task/
    parser/
    adapters/
    ...

  dvt/                              # DVT extension layer (NEW)
    __init__.py
    cli/
      __init__.py
      main.py                       # Click CLI: dvt run, dvt build, etc.
      commands.py                   # Command definitions
    tasks/
      __init__.py
      run.py                        # DvtRunTask(RunTask)
      build.py                      # DvtBuildTask(BuildTask)
      compile.py                    # DvtCompileTask(CompileTask)
      test.py                       # DvtTestTask(TestTask)
      seed.py                       # DvtSeedTask — Sling-based seeding
      sync.py                       # DvtSyncTask — environment bootstrap
      show.py                       # DvtShowTask — DuckDB local query
      debug.py                      # DvtDebugTask — multi-connection checks
    runners/
      __init__.py
      model_runner.py               # DvtModelRunner(ModelRunner)
      extraction_runner.py          # Runs Sling extraction for models with `connection` config
      seed_runner.py                # Runs Sling seed loading
    extraction/
      __init__.py
      sling_client.py               # Python wrapper around Sling
      connection_mapper.py          # profiles.yml → Sling connection URLs
      watermark_formatter.py        # Dialect-specific watermark literal formatting
    loading/
      __init__.py
      sling_loader.py               # Load results to non-default targets via Sling
      bucket_materializer.py        # Delta/Parquet/CSV materialization to buckets
    config/
      __init__.py
      dvt_project.py                # DVT-specific project config
      target_resolver.py            # Resolves per-model target + connection overrides
    federation/
      __init__.py
      engine.py                     # DuckDB engine for dvt show / local dev
      optimizer.py                  # Predicate/column pushdown to extraction queries
    sync/
      __init__.py
      adapter_installer.py          # pip install dbt-<adapter> from profiles.yml
      duckdb_extensions.py          # Install DuckDB extensions
      cloud_deps.py                 # Install cloud SDKs (boto3, google-cloud, etc.)
      sling_checker.py              # Verify Sling binary availability
```

## Integration Points with dbt-core

DVT hooks into dbt-core at these specific points:

1. **CLI entry point** — `dvt.cli.main:cli` is a separate Click group that mirrors
   dbt's commands but routes to DvtXxxTask subclasses.

2. **Task subclasses** — DvtRunTask(RunTask), DvtBuildTask(BuildTask), etc.
   Override `get_runner()` to return DVT runners. Reuse ALL dbt lifecycle decorators.

3. **Runner subclasses** — DvtModelRunner(ModelRunner). Override `execute()` to
   detect `connection` config and route to Sling extraction or adapter pushdown.
   For incremental extraction models, pre-resolve watermarks and format as
   dialect-specific literals before sending to Sling.

4. **Config extension** — DVT reads `connection`, `target`, `format`, `sling`
   from model config(). These are passed through dbt's existing config system.

## Data Flow Examples

### Example 1: Simple pushdown (all sources on target)

```
Source: Snowflake.raw.customers (already on target)
Model: models/dim_customers.sql → config(materialized='table')
Target: Snowflake (default)

Flow:
  1. No extraction needed (source is on the target)
  2. Snowflake executes: CREATE TABLE dim_customers AS (SELECT ...)
  3. Done. This is stock dbt behavior.
```

### Example 2: User-written extraction model + pushdown

```
User writes:
  models/staging/stg_customers.sql → config(materialized='table', connection='source_postgres')
  models/staging/stg_orders.sql → config(materialized='incremental', connection='source_postgres', unique_key='id')
  models/marts/fct_orders.sql → config(materialized='table')  (no connection — pushdown)

Target: Snowflake (default)

Flow:
  1. Sling executes stg_customers SQL on Postgres, streams result → Snowflake.stg_customers
  2. Sling executes stg_orders SQL on Postgres (with watermark), merges → Snowflake.stg_orders
  3. Snowflake executes fct_orders SQL via pushdown (refs stg_customers + stg_orders)
  4. Done.
```

### Example 3: Cross-target materialization to bucket

```
User writes:
  models/archive/archive_orders.sql → config(target='data_lake', format='delta')

Target: S3 bucket (model-level override)

Flow:
  1. Snowflake executes model SQL → temp result (refs stg_orders which already exists)
  2. Sling streams result from Snowflake → S3 as Delta Lake format
  3. Temp table dropped
  4. Done.
```

### Example 4: Seed via Sling

```
Seed: seeds/country_codes.csv
Target: Snowflake (default, or overridden via --target)

Flow:
  1. Sling loads CSV → Snowflake.public.country_codes
  2. Much faster than dbt's Python-based INSERT batching.
  3. Supports --full-refresh (truncate + reload).
```
