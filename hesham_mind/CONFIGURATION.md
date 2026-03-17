# DVT Configuration

## Overview

DVT extends dbt's configuration system with these additions:
1. **profiles.yml** — multi-adapter + bucket connections (extended from dbt)
2. **sources.yml** — metadata only: connection + schema + tables (extended from dbt, NO sling config)
3. **Model config** — `target` (materialization target override), `format` (bucket output format)
4. **`.dvt/` directory** — runtime cache directory (persistent DuckDB cache, gitignored)

All standard dbt configuration (dbt_project.yml, selectors.yml, packages.yml) works unchanged.

### Profiles Directory

DVT searches for `profiles.yml` in this order:
1. `DVT_PROFILES_DIR` environment variable (if set)
2. `~/.dvt/` — DVT-specific profiles directory
3. `~/.dbt/` — standard dbt profiles directory (VS Code dbt extension compatibility)

This allows DVT and dbt to coexist without conflicts. If you use the VS Code dbt extension,
keep your profiles in `~/.dbt/` — DVT will find them automatically.

## profiles.yml

DVT extends profiles.yml to support multiple adapter types per profile AND cloud bucket
connections as first-class targets.

```yaml
my_project:
  target: prod_snowflake                     # default target for dvt run
  outputs:

    # === Database Targets ===

    prod_snowflake:                           # default target
      type: snowflake
      account: xyz123.us-east-1
      user: dvt_service
      password: "{{ env_var('SNOWFLAKE_PASSWORD') }}"
      role: TRANSFORMER
      database: ANALYTICS
      warehouse: TRANSFORM_WH
      schema: PUBLIC
      threads: 8

    staging_postgres:                         # another database target
      type: postgres
      host: staging.internal
      port: 5432
      user: dvt_user
      password: "{{ env_var('PG_PASSWORD') }}"
      dbname: staging
      schema: public
      threads: 4

    # === Source Connections (read-only, used by extraction) ===

    source_postgres:                          # source database
      type: postgres
      host: prod-pg.internal
      port: 5432
      user: readonly
      password: "{{ env_var('SOURCE_PG_PASSWORD') }}"
      dbname: production
      schema: public

    source_mysql:                             # another source
      type: mysql
      host: mysql.internal
      port: 3306
      user: reader
      password: "{{ env_var('MYSQL_PASSWORD') }}"
      database: billing

    # === Bucket Targets ===

    data_lake:                                # S3 bucket as target
      type: s3
      bucket: company-data-lake
      region: us-east-1
      access_key_id: "{{ env_var('AWS_ACCESS_KEY_ID') }}"
      secret_access_key: "{{ env_var('AWS_SECRET_ACCESS_KEY') }}"
      prefix: analytics/                     # base path within bucket

    archive_gcs:                              # GCS bucket as target
      type: gcs
      project: my-gcp-project
      bucket: archive-data
      credentials_json: "{{ env_var('GCP_CREDENTIALS_JSON') }}"
      prefix: archive/

    landing_azure:                            # Azure Blob as target
      type: azure
      account: mystorageaccount
      container: raw-data
      access_key: "{{ env_var('AZURE_ACCESS_KEY') }}"
```

### Connection Types

**Database types** (require dbt adapter):
`postgres`, `snowflake`, `bigquery`, `redshift`, `mysql`, `sqlserver`,
`databricks`, `oracle`, `trino`, `clickhouse`, `duckdb`, etc.

**Bucket types** (require cloud SDK):
`s3`, `gcs`, `azure`

**File types** (no external dependencies):
`local` — local filesystem path

### How connections are used

| Connection role | How DVT uses it |
|----------------|----------------|
| Default target | dbt adapter pushes down model SQL (pushdown models) |
| Source connection (any remote) | Extraction: Sling → DuckDB cache → model SQL → Sling → model table on target |
| Non-default target (all sources local) | Non-default pushdown: adapter pushes down on non-default target |
| Alternate target (DB) | Sling loads model results to this DB |
| Alternate target (bucket) | Sling writes model results as Delta/Parquet/CSV |

## sources.yml

DVT extends dbt's sources.yml with a **required** `connection` property.
sources.yml is **metadata only** — it declares what tables exist on what connections.
It does NOT contain extraction config. All extraction behavior is controlled by the
**model** that references the source.

```yaml
version: 2

sources:
  - name: crm
    description: "CRM system on PostgreSQL"
    connection: source_postgres               # REQUIRED: profiles.yml output name
    schema: public
    tables:
      - name: customers
        description: "Customer master data"
        columns:
          - name: id
            data_tests:
              - unique
              - not_null
      - name: orders
      - name: ref_countries

  - name: erp
    description: "ERP system on SQL Server"
    connection: source_sqlserver
    schema: dbo
    tables:
      - name: invoices
      - name: products
      - name: line_items
```

**Key points:**
- `connection` is **required** on every source
- No `sling:` blocks — DVT manages extraction automatically
- Standard dbt source features (descriptions, tests, freshness) work unchanged

## Model Config

DVT extends dbt's model config with `target`, `format`, and `path` options.
Users write **standard dbt models** — no `connection` or `sling` config on models.
DVT automatically detects when Sling extraction is needed by comparing
`source.connection` (from sources.yml) vs `model.target` (from profiles.yml default or model config).

### Extraction model — single remote source

```sql
-- models/staging/stg_customers.sql
-- EXTRACTION MODEL: Written in DuckDB SQL.
-- DVT detects: source on source_postgres, target is prod_snowflake → Extraction path
-- Sling extracts source → DuckDB cache → model SQL → Sling → target
{{ config(materialized='table') }}
SELECT * FROM {{ source('crm', 'customers') }}
```

### Extraction model — single remote source, incremental

```sql
-- models/staging/stg_orders.sql
-- EXTRACTION MODEL: Written in DuckDB SQL. Incremental via persistent DuckDB cache.
-- DVT checks cache for is_incremental(), resolves watermark from target,
-- extracts delta via Sling, DuckDB merges, loads result to target.
{{ config(
    materialized='incremental',
    unique_key='id'
) }}
SELECT id, customer_id, order_date, total, updated_at
FROM {{ source('crm', 'orders') }}
{% if is_incremental() %}
WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}
```

### Extraction model — multiple remote sources (incremental supported)

```sql
-- models/staging/stg_combined.sql
-- EXTRACTION MODEL: Written in DuckDB SQL. DuckDB is the compute engine.
-- Incremental IS supported via persistent DuckDB cache.
-- Sling extracts each source → DuckDB cache, SQL runs in DuckDB, result → target.
{{ config(materialized='incremental', unique_key='id') }}
SELECT c.id, c.name, i.invoice_total
FROM {{ source('crm', 'customers') }} c
JOIN {{ source('erp', 'invoices') }} i ON c.id = i.customer_id
{% if is_incremental() %}
WHERE i.updated_at > (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}
```

### target (per-model target override)

```sql
-- models/archive_orders.sql
{{ config(
    materialized='table',
    target='data_lake',              -- materialize to S3 instead of default target
    format='delta',                  -- output format for bucket targets
    path='analytics/orders/',        -- path within the bucket
) }}
SELECT * FROM {{ ref('fct_orders') }}
```

### format (bucket materialization format)

Only applies when `target` is a bucket connection. Options:
- `delta` (default) — Delta Lake format
- `parquet` — plain Parquet files
- `csv` — CSV files
- `json` — JSON files
- `jsonlines` — JSON Lines files

### Standard dbt config (unchanged)

All standard dbt config works as-is:
- `materialized`: table, view, incremental, ephemeral, snapshot
- `schema`: target schema override
- `database`: target database override
- `tags`: model tags for selection
- `pre_hook` / `post_hook`: SQL hooks
- `unique_key`: for incremental merge (also maps to Sling primary_key)
- `strategy`: for snapshots (timestamp, check)
- etc.

### Config in dbt_project.yml

```yaml
# dbt_project.yml
name: my_project
version: '0.1.2'
profile: 'my_project'

model-paths: ["models"]
seed-paths: ["seeds"]
test-paths: ["tests"]
analysis-paths: ["analyses"]
macro-paths: ["macros"]

# DVT-specific configuration
dvt:
  cache_dir: ".dvt"                # directory for persistent DuckDB cache (default: .dvt)
  duckdb:
    memory_limit: "4GB"            # max DuckDB memory for extraction compute
    threads: 4                     # DuckDB parallel threads

models:
  my_project:
    staging:
      +materialized: table
    marts:
      +materialized: table
    archive:
      +materialized: table
      +target: data_lake           # all archive models go to S3
      +format: delta

seeds:
  my_project:
    +schema: seeds

# No dvt.staging_schema needed — DVT does not create hidden staging tables.
# Results land directly as model tables on the target.
# DuckDB cache at .dvt/cache.duckdb is for extraction compute only.
```

## Model Naming

Users name their models following standard dbt conventions. DVT manages extraction
transparently — no special naming or config required for models that reference remote sources:

```
models/
  staging/
    stg_customers.sql          ← standard model, refs source('crm', 'customers')
    stg_orders.sql             ← incremental, refs source('crm', 'orders')
    stg_invoices.sql           ← standard model, refs source('erp', 'invoices')
  marts/
    dim_customers.sql          ← pushdown (refs stg_customers)
    fct_orders.sql             ← pushdown (refs stg_orders + stg_customers)
```

DVT handles extraction automatically — no hidden staging tables. Extraction model results
land directly as model tables. Downstream models reference them via `{{ ref('stg_customers') }}`.

## .dvt/ Directory

DVT creates a `.dvt/` directory in the project root for runtime artifacts:

```
.dvt/
  cache.duckdb          # Persistent DuckDB cache for extraction
                        # Contains: cached source tables + model results
                        # Enables incremental extraction across runs
```

**IMPORTANT:** Add `.dvt/` to your `.gitignore`:

```gitignore
# DVT runtime cache
.dvt/
```

The cache is machine-specific and should not be committed to version control. Each
developer/environment will build its own cache on first `dvt run`.

### Cache management commands

| Command | Effect on cache |
|---------|----------------|
| `dvt run` | Creates/updates cache as needed |
| `dvt run --full-refresh` | **Deletes** cache, rebuilds from scratch |
| `dvt clean` | **Deletes** cache and other build artifacts |

## `--target` Usage Guidance

The `--target` CLI flag overrides the default target from `profiles.yml` for a single run. It is designed for switching between **same-engine environments**.

### Correct usage: same adapter type
```bash
# profiles.yml has default: prod_snowflake (type: snowflake)
dvt run --target dev_snowflake       # OK: snowflake → snowflake
dvt run --target staging_snowflake   # OK: snowflake → snowflake
dvt seed --target dev_snowflake      # OK: seeds work across same engine
```

### Risky usage: different adapter type
```bash
# profiles.yml has default: prod_snowflake (type: snowflake)
dvt run --target mysql_docker        # WARNING: pushdown models will fail
dvt seed --target mysql_docker       # OK: seeds don't depend on SQL dialect
dvt run --select extraction_model --target mysql_docker  # OK if only extraction models selected
```

### DVT007 Warning

When the `--target` adapter type differs from the profile's default target adapter type, DVT emits a warning:

```
DVT007: Target override 'mysql_docker' (mysql) differs in adapter type from
default target 'prod_snowflake' (snowflake). Pushdown models written in
snowflake SQL may fail on mysql. Extraction models (DuckDB SQL) are unaffected.
```

**DVT warns but does not block.** The user may have a valid reason:
- Running only extraction models (DuckDB SQL, engine-independent)
- Running only seeds (CSV loading, engine-independent)
- Running only tests (test SQL is typically simple/portable)
- Intentionally testing cross-engine compatibility

### Why this matters: two dialects in one project

A DVT project contains two SQL dialects:
- **Pushdown models**: target's native SQL dialect (e.g., Snowflake SQL). Switching `--target` to a different engine breaks these.
- **Extraction models**: DuckDB SQL (universal). These are unaffected by `--target` changes because DuckDB is always the compute engine.

## Environment Variables

DVT respects all dbt environment variables plus:

| Variable | Description |
|----------|-------------|
| `DVT_PROFILES_DIR` | Override profiles.yml location (checked first, before `~/.dvt` and `~/.dbt`) |
| `DVT_CACHE_DIR` | Override cache directory location (default: `.dvt` in project root) |
| `SLING_THREADS` | Number of parallel Sling extractions |
| `SLING_STATE` | State store for CDC (Sling native) |
| `DUCKDB_PATH` | Path to DuckDB binary (for extensions) |
