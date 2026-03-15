# DVT Configuration

## Overview

DVT extends dbt's configuration system with these additions:
1. **profiles.yml** — multi-adapter + bucket connections (extended from dbt)
2. **sources.yml** — metadata only: connection + schema + tables (extended from dbt, NO sling config)
3. **Model config** — `target` (materialization target override), `format` (bucket output format)

All standard dbt configuration (dbt_project.yml, selectors.yml, packages.yml) works unchanged.

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
| Default target | dbt adapter pushes down model SQL |
| Source connection | Sling extracts data to default target |
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

### Standard model referencing a remote source

```sql
-- models/staging/stg_customers.sql
-- This is a standard dbt model. DVT detects that source('crm', 'customers')
-- lives on source_postgres (remote) and auto-extracts it to _dvt staging.
{{ config(materialized='table') }}
SELECT * FROM {{ source('crm', 'customers') }}
```

### Incremental model referencing a remote source

```sql
-- models/staging/stg_orders.sql
-- Standard dbt incremental model. DVT handles cross-engine extraction transparently.
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

DVT pre-resolves the watermark and formats it in the source's dialect for the extraction query.

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

# DVT-specific project config (optional)
dvt:
  staging_schema: _dvt             # schema for auto-extracted source staging tables
                                   # default: _dvt. Tables: _dvt.{source_name}__{table_name}
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

DVT auto-extracts remote sources into `_dvt` staging tables before model execution.
Downstream models reference staging models via `{{ ref('stg_customers') }}` — standard dbt behavior.

## Environment Variables

DVT respects all dbt environment variables plus:

| Variable | Description |
|----------|-------------|
| `DVT_PROFILES_DIR` | Override profiles.yml location |
| `SLING_THREADS` | Number of parallel Sling extractions |
| `SLING_STATE` | State store for CDC (Sling native) |
| `DUCKDB_PATH` | Path to DuckDB binary (for extensions) |
