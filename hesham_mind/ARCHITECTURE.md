# DVT-CE Architecture

## What DVT Is

DVT is a cross-engine data transformation tool built on top of dbt-core.
It combines three proven technologies:

- **dbt-core** — DAG orchestration, SQL models, Jinja, ref(), source(), testing, docs, adapters
- **Sling** — high-performance data movement (EL) across 30+ databases, file systems, and cloud buckets
- **DuckDB** — lightweight analytics engine for multi-source extraction compute, `dvt show`, local dev, and virtual federation

The user experience is identical to dbt: write SQL models, declare sources, run `dvt run`.
The difference: DVT automatically handles cross-engine data movement so models can reference
sources on ANY database, and materialize results to ANY target (including cloud buckets).

## Core Principles

1. **dbt internals are NEVER modified.** DVT extends dbt via subclassing (DvtRunTask extends RunTask, DvtModelRunner extends ModelRunner). This allows rebasing on upstream dbt-core with minimal conflicts.

2. **The target database is the compute engine for pushdown models.** Models whose sources are all local push down to the target via dbt adapters. For extraction models (any model with remote sources), DuckDB is the compute engine — Sling streams sources into the persistent DuckDB cache, model SQL runs in DuckDB, Sling streams the result to the target.

3. **Sling handles ALL data streaming.** Cross-engine extraction (source → DuckDB cache), loading results (DuckDB cache → target), seeding CSV files, cross-target materialization, CDC — all via Sling. No custom JDBC code, no PySpark, no intermediate staging files managed by DVT.

4. **No hidden tables. Results land directly as model tables.** Users write standard dbt models and sources — no `connection` config on models, no `sling:` blocks anywhere. DVT automatically detects when extraction is needed by comparing `source.connection` (from sources.yml) vs `model.target` (from profiles.yml default or model config). When a mismatch is detected, DVT routes through the unified extraction path: Sling → DuckDB cache → model SQL → Sling → target. There is no `_dvt` staging schema and no hidden staging tables.

5. **Cross-engine incremental models work seamlessly.** dbt's `is_incremental()` macro works across engines via the persistent DuckDB cache. DVT checks the cache for model existence (fast, local), resolves the watermark value from the target, formats it in the source dialect, and extracts only delta rows via Sling. The DuckDB cache merges deltas and persists between runs.

6. **DuckDB is a persistent cache and compute engine.** (1) Persistent extraction cache at `.dvt/cache.duckdb` — stores extracted source tables and model results between runs for incremental support. (2) Extraction compute — ALL extraction model SQL runs in DuckDB. (3) `dvt show` — local queries without hitting the warehouse. (4) Local file/API processing. (5) Virtual federation (future).

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
│  │  4. Classify: pushdown / extraction (DuckDB cache)               │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                               │                                         │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                     Execution (per node in DAG order)            │   │
│  │                                                                   │   │
│  │  EXTRACTION (any remote source/ref — unified path):               │   │
│  │    Sling extracts each source → DuckDB cache (.dvt/cache.duckdb) │   │
│  │    Model SQL runs in DuckDB (user wrote DuckDB SQL)               │   │
│  │    Model result cached in DuckDB (for {{ this }} support)         │   │
│  │    Sling streams result → model's named table on target           │   │
│  │    Supports incremental via persistent cache                      │   │
│  │    No hidden tables on target, result lands as model table        │   │
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
.dvt/                                 # DVT runtime directory (gitignored)
  cache.duckdb                        # Persistent DuckDB cache for extraction

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
      duckdb_compute.py             # DuckDB extraction compute for multi-source models
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

3. **Runner subclasses** — DvtModelRunner(ModelRunner). Override `execute()` with
   two-way dispatch: (a) pushdown — all sources local, delegate to parent;
   (b) extraction — any remote source(s), Sling → DuckDB cache → model SQL → Sling → target.
   For incremental extraction models, check DuckDB cache for `is_incremental()`,
   resolve watermarks from target, format in source dialect, extract deltas via Sling.

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

### Example 2: Extraction (remote sources — DuckDB cache)

```
User writes:
  sources.yml → crm source with connection: source_postgres (customers, orders tables)
  models/staging/stg_customers.sql → config(materialized='table')
    -- Written in DuckDB SQL
    SELECT * FROM {{ source('crm', 'customers') }}
  models/staging/stg_orders.sql → config(materialized='incremental', unique_key='id')
    -- Written in DuckDB SQL
    SELECT * FROM {{ source('crm', 'orders') }} {% if is_incremental() %} WHERE ...{% endif %}
  models/marts/fct_orders.sql → config(materialized='table')
    -- Written in Snowflake SQL (pushdown model — all refs are local)
    SELECT ... FROM {{ ref('stg_customers') }} JOIN {{ ref('stg_orders') }}

Target: Snowflake (default)
DuckDB cache: .dvt/cache.duckdb

Flow:
  1. DVT detects source_postgres != Snowflake → extraction needed
  2. stg_customers: remote source → Extraction path
     Sling extracts pg.customers → DuckDB cache (crm__customers)
     Model SQL runs in DuckDB → result cached as stg_customers
     Sling loads stg_customers → Snowflake.stg_customers
  3. stg_orders: remote source, incremental → Extraction path
     DVT checks DuckDB cache → is_incremental() true (if stg_orders cached)
     Watermark from Snowflake.stg_orders → delta extraction via Sling
     DuckDB merges delta → model SQL → Sling loads delta → Snowflake.stg_orders
  4. fct_orders: all refs local → Pushdown
     Snowflake executes SQL via adapter (standard dbt)
  5. Done. No hidden tables on target. User never configured Sling.
```

### Example 2b: Extraction (multiple remote sources — DuckDB cache, shared)

```
User writes:
  sources.yml → crm (connection: source_postgres), erp (connection: source_sqlserver)
  models/staging/stg_combined.sql → config(materialized='incremental', unique_key='id')
    -- Written in DuckDB SQL. Incremental IS supported.
    SELECT c.id, c.name, i.invoice_total
    FROM {{ source('crm', 'customers') }} c
    JOIN {{ source('erp', 'invoices') }} i ON c.id = i.customer_id
    {% if is_incremental() %} WHERE i.updated_at > ... {% endif %}

Target: Snowflake (default)
DuckDB cache: .dvt/cache.duckdb

Flow:
  1. DVT detects 2 remote sources → Extraction path (unified)
  2. Sling extracts pg.customers → DuckDB cache (crm__customers)
     Note: if crm__customers already cached from stg_customers, extraction is shared
  3. Sling extracts mssql.invoices → DuckDB cache (erp__invoices)
  4. DuckDB executes model SQL (join) → result cached as stg_combined
  5. Sling streams result → Snowflake.stg_combined
  6. Done. No hidden tables on target. Cache persists for next run.
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

## DVT Philosophy

### Two Dialects in One Project

A DVT project naturally contains SQL in two dialects:

- **Pushdown models** use the **target's native dialect** (Snowflake SQL, PostgreSQL, BigQuery, etc.). These models' SQL runs directly on the target database via the dbt adapter.
- **Extraction models** use **DuckDB SQL** (a universal, Postgres-like dialect). These models' SQL runs in the local DuckDB cache engine after Sling extracts remote sources.

Both coexist in the same project. The dialect a model uses is determined by its execution path, not by any config flag. If all a model's sources are on the target, it's pushdown (target dialect). If any source is remote, it's extraction (DuckDB dialect).

### `--target` Switches Environments, Not Engines

The `--target` flag selects which named output from `profiles.yml` to use as the default target. It is designed for switching between **same-engine environments** (e.g., `--target dev_snowflake` vs `--target prod_snowflake`).

**`--target` should NOT change the adapter type.** If the default target is Snowflake and the user runs `--target mysql_docker`, all pushdown models written in Snowflake SQL will fail because MySQL cannot parse Snowflake-specific syntax. DVT emits a **DVT007** warning when the `--target` adapter type differs from the profile default, but does not block execution — the user may have a valid reason (e.g., running only extraction models that use DuckDB SQL anyway).

### Sling Is Invisible

Users should **never** see Sling output during normal operation. The extraction path (Sling → DuckDB → Sling) is a black box. DVT shows standard dbt-like output (model name, status, timing). Sling errors are caught and re-raised as DVT errors with actionable messages.

### Seeds Must Be Robust

DVT's Sling-based seed loader must handle dirty CSV data at least as gracefully as dbt's agate-based loader. This includes: encoding issues, mixed types, null handling, and schema inference. Seeds should "just work" for the same CSVs that dbt handles.

### Example 4: Seed via Sling

```
Seed: seeds/country_codes.csv
Target: Snowflake (default, or overridden via --target)

Flow:
  1. Sling loads CSV → Snowflake.public.country_codes
  2. Much faster than dbt's Python-based INSERT batching.
  3. Supports --full-refresh (truncate + reload).
```
