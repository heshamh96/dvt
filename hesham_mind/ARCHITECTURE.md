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

4. **Data movement is FULLY TRANSPARENT.** Users write standard dbt models and sources — no `connection` config on models, no `sling:` blocks anywhere. DVT automatically detects when Sling is needed by comparing `source.connection` (from sources.yml) vs `model.target` (from profiles.yml default or model config). When a mismatch is detected, DVT extracts the source data into a `_dvt` staging schema on the target (`_dvt.{source_name}__{table_name}`). These staging tables are invisible in the DAG and lineage — they are implementation details managed entirely by DVT.

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
│  │ outputs:     │     │  conn: pg    │      │  (standard)   │           │
│  │  sf: {...}   │     │  tables:     │      │ stg_orders    │           │
│  │  pg: {...}   │     │   -customers │      │  (incremental)│           │
│  │  s3: {...}   │     │   -orders    │      │ dim_customers │           │
│  │  gcs: {...}  │     │              │      │ fct_orders    │           │
│  └──────────────┘     └──────────────┘      │  target=s3    │           │
│                        (metadata only,       └──────────────┘           │
│                         no sling config)                                │
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
│  │  2. Parse models/ → detect source references                     │   │
│  │  3. Compare source.connection vs model.target for each model     │   │
│  │  4. Auto-stage remote sources → _dvt schema, build DAG + tests  │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                               │                                         │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                     Execution (per node in DAG order)            │   │
│  │                                                                   │   │
│  │  AUTOMATIC EXTRACTION (source.connection != model.target):         │   │
│  │    DVT detects remote sources referenced by the model             │   │
│  │    Sling extracts source tables → _dvt staging schema on target  │   │
│  │    Staging tables: _dvt.{source_name}__{table_name}              │   │
│  │    Not visible in DAG — managed transparently by DVT             │   │
│  │    Model SQL then runs on target, referencing staged data         │   │
│  │                                                                   │   │
│  │  PUSHDOWN MODEL (all sources already on target):                  │   │
│  │    dbt adapter pushes down SQL to the target                      │   │
│  │    Standard dbt materialization (table/view/incremental/ephemeral)│   │
│  │                                                                   │   │
│  │  CROSS-TARGET MODEL (target != default):                          │   │
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
      extraction_runner.py          # Runs Sling extraction when source.connection != model.target
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
   detect when a model's sources live on a different connection than the model's
   target, and automatically extract via Sling before running the model SQL.
   For incremental sources, pre-resolve watermarks and format as dialect-specific
   literals before sending to Sling.

4. **Config extension** — DVT reads `target`, `format` from model config().
   Source `connection` metadata comes from sources.yml. These are passed through
   dbt's existing config system.

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

### Example 2: Automatic extraction + pushdown (remote source)

```
User writes:
  sources.yml → crm source with connection: source_postgres (customers, orders tables)
  models/staging/stg_customers.sql → config(materialized='table')
    SELECT * FROM {{ source('crm', 'customers') }}
  models/staging/stg_orders.sql → config(materialized='incremental', unique_key='id')
    SELECT * FROM {{ source('crm', 'orders') }} {% if is_incremental() %} WHERE ...{% endif %}
  models/marts/fct_orders.sql → config(materialized='table')
    SELECT ... FROM {{ ref('stg_customers') }} JOIN {{ ref('stg_orders') }}

Target: Snowflake (default)

Flow:
  1. DVT detects source_postgres != Snowflake (target) → automatic extraction needed
  2. Sling extracts crm.customers → Snowflake._dvt.crm__customers (staging, invisible)
  3. Sling extracts crm.orders → Snowflake._dvt.crm__orders (staging, invisible)
  4. Snowflake executes stg_customers SQL (refs _dvt staging tables transparently)
  5. Snowflake executes stg_orders SQL (incremental, refs _dvt staging)
  6. Snowflake executes fct_orders SQL via pushdown (refs stg_customers + stg_orders)
  7. Done. User never configured Sling or extraction.
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
