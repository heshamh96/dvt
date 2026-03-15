# Sling Integration

## Overview

Sling is DVT's data movement engine. It handles ALL data transfer between systems:
- Source extraction to default target
- Cross-target model materialization
- Seed loading (CSV → database)
- CDC (Change Data Capture)

DVT uses the `sling` Python wrapper (`pip install sling`) which calls the Sling Go binary.

## Sling Capabilities Summary

### What Sling Does
- Streams data between 30+ databases, file systems, and cloud buckets
- Supports custom SQL as source stream
- Handles incremental extraction via update_key watermarks
- Supports CDC via database transaction logs (MySQL binlog, Postgres WAL coming)
- Bulk loads using native database tools (COPY, bcp, sqlldr)
- Schema evolution (auto-adds new columns)
- Inline row-level transforms (trim, upper, hash, type casting)
- Configurable via CLI flags or YAML replication files
- Reads dbt profiles.yml natively for connection discovery
- Parallel stream execution

### What Sling Does NOT Do
- SQL-based transformations (no joins, aggregations, CTEs)
- DAG execution (no dependency ordering)
- Jinja templating
- Data testing

### Supported Connectors

**Databases:** PostgreSQL, MySQL, MariaDB, SQL Server, Oracle, Snowflake, BigQuery,
Redshift, Databricks, DuckDB, ClickHouse, SQLite, StarRocks, Trino, Exasol,
MongoDB, Elasticsearch, Prometheus, Bigtable, Azure SQL/DWH/Table, Fabric,
Cloudflare D1, Proton

**File Systems:** Local, S3, GCS, Azure Blob, Backblaze B2, DigitalOcean Spaces,
MinIO, Cloudflare R2, Wasabi, SFTP

**File Formats:** CSV, Parquet, JSON, JSONLines, Avro, XML, Excel, SAS, GeoJSON

**Data Lakes:** Athena, Iceberg, DuckLake, S3 Tables

## How DVT Uses Sling

### 1. Extraction Models (Models with `connection` Config)

When `dvt run` encounters a model with `connection` config:

```python
from sling import Replication, ReplicationStream

# DVT compiles the model SQL (Jinja → SQL in source dialect)
# For incremental: watermark is pre-resolved and substituted as dialect-specific literal
compiled_sql = dvt_compile_extraction_model(model, manifest)

replication = Replication(
    source=source_connection_url,       # from model's connection config → profiles.yml
    target=target_connection_url,       # model's target from profiles.yml
    streams={
        f"{model.name}.custom": ReplicationStream(
            sql=compiled_sql,                   # the model's compiled SQL
            object=f"{schema}.{model.name}",    # target table
            mode=sling_mode,                    # mapped from dbt materialization
            primary_key=model.unique_key,       # from model config
        ),
    },
)
replication.run()
```

The user controls the SQL — column selection, filtering, joins — by writing it
in their extraction model. DVT does NOT auto-generate extraction queries.

### 2. Seed Loading

When `dvt seed` is called:

```python
replication = Replication(
    source="file://.",                   # local filesystem
    target=target_connection_url,
    streams={
        f"file://{seed_path}": ReplicationStream(
            object=f"{schema}.{seed_name}",
            mode="full-refresh",         # or "truncate" for non-destructive
        ),
    },
)
replication.run()
```

**Advantages over dbt's native seed:**
- dbt uses Python agate library + batch INSERT statements
- Sling uses native bulk loading: COPY (Postgres), COPY INTO (Snowflake),
  bcp (SQL Server), etc.
- 10-100x faster for large seed files
- Better type inference
- Handles encoding issues (Latin-1, Windows-1252, etc.)

### 3. Cross-Target Loading

When a model has `config(target='other_db')`:

```python
# After model SQL executes on default target, result is in a temp table
# Sling streams from default target to model's target
replication = Replication(
    source=default_target_connection_url,
    target=model_target_connection_url,
    streams={
        "temp_schema.model_result_tmp": ReplicationStream(
            object=f"{schema}.{model_name}",
            mode="full-refresh",
        ),
    },
)
replication.run()
```

### 4. Bucket Materialization

When a model has `config(target='s3_bucket', format='delta')`:

```python
replication = Replication(
    source=default_target_connection_url,
    target=bucket_connection_url,
    streams={
        "temp_schema.model_result_tmp": ReplicationStream(
            object=f"s3://bucket/path/{model_name}/",
            target_options={
                "format": "delta",       # or parquet, csv, json
            },
        ),
    },
)
replication.run()
```

## Connection Mapper

DVT translates profiles.yml adapter configs to Sling connection URLs:

```python
ADAPTER_TO_SLING_URL = {
    "postgres":   "postgres://{user}:{password}@{host}:{port}/{dbname}?sslmode={sslmode}",
    "snowflake":  "snowflake://{user}:{password}@{account}/{database}?schema={schema}&warehouse={warehouse}&role={role}",
    "bigquery":   "bigquery://{project}?dataset={dataset}&location={location}",
    "redshift":   "redshift://{user}:{password}@{host}:{port}/{dbname}",
    "mysql":      "mysql://{user}:{password}@{host}:{port}/{database}",
    "sqlserver":  "sqlserver://{user}:{password}@{host}:{port}/{database}",
    "databricks": "databricks://{host}?token={token}&catalog={catalog}&schema={schema}",
    "oracle":     "oracle://{user}:{password}@{host}:{port}/{service}",
    "clickhouse": "clickhouse://{user}:{password}@{host}:{port}/{database}",
    "trino":      "trino://{user}@{host}:{port}/{catalog}?schema={schema}",
    "duckdb":     "duckdb://{path}",
    "sqlite":     "sqlite://{path}",
    # Bucket types
    "s3":         "s3://{bucket}?region={region}&access_key_id={access_key_id}&secret_access_key={secret_access_key}",
    "gcs":        "gs://{bucket}?project={project}",
    "azure":      "azure://{account}/{container}?access_key={access_key}",
}
```

This mapping lives in `core/dvt/extraction/connection_mapper.py`. It handles:
- URL construction from profile fields
- Environment variable resolution (Jinja rendering happens before mapping)
- Special auth methods (key-pair, OAuth, service accounts)

## Sling Modes Mapped from dbt Materializations

DVT maps dbt model materializations to Sling modes for extraction models:

### table → Sling full-refresh

```sql
{{ config(materialized='table', connection='source_postgres') }}
SELECT * FROM {{ source('crm', 'customers') }}
```

- Sling drops and recreates the target table every run
- Default write behavior (no `--full-refresh` needed)

### incremental → Sling incremental (with merge)

```sql
{{ config(materialized='incremental', connection='source_postgres', unique_key='id') }}
SELECT * FROM {{ source('crm', 'orders') }}
{% if is_incremental() %}
WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}
```

- DVT pre-resolves the watermark from the target in dialect-specific format
- Sling executes the filtered SQL on the source (only delta rows)
- Sling merges the result into the target table using the mapped merge strategy
- `unique_key` maps to Sling's `primary_key`
- dbt strategies map to Sling: `merge` → `update_insert`, `delete+insert` → `delete_insert`, `append` → no primary_key

### --full-refresh on any extraction model

- `is_incremental()` returns false → full SQL, no watermark
- Sling uses `full-refresh` mode → drop + create + full load
- Matches stock dbt `--full-refresh` behavior exactly

## Dialect-Specific Watermark Formatting

When DVT pre-resolves the watermark for cross-engine incremental models, it must
format the value as a valid SQL literal for the **source** database's dialect.

DVT maintains a `watermark_formatter.py` with dialect-specific templates:

| Source Engine | Timestamp Format | Date Format |
|---|---|---|
| PostgreSQL | `'{value}'::TIMESTAMP` | `'{value}'::DATE` |
| MySQL/MariaDB | `'{value}'` | `'{value}'` |
| SQL Server | `CONVERT(DATETIME2, '{value}', 121)` | `CONVERT(DATE, '{value}', 23)` |
| Oracle | `TO_TIMESTAMP('{value}', 'YYYY-MM-DD HH24:MI:SS.FF6')` | `TO_DATE('{value}', 'YYYY-MM-DD')` |
| Snowflake | `TO_TIMESTAMP('{value}')` | `TO_DATE('{value}')` |
| BigQuery | `TIMESTAMP '{value}'` | `DATE '{value}'` |
| Redshift | `'{value}'::TIMESTAMP` | `'{value}'::DATE` |
| Databricks | `TIMESTAMP '{value}'` | `DATE '{value}'` |
| ClickHouse | `toDateTime64('{value}', 6)` | `toDate('{value}')` |
| Trino | `TIMESTAMP '{value}'` | `DATE '{value}'` |

Integer/numeric watermarks: plain literals (`12345`).
String watermarks: `'value'` (SQL Server: `N'value'`).

## Sling Type Mapping

Sling uses a two-step type mapping: source native → Sling general → target native.

**General types:** `bigint`, `integer`, `smallint`, `float`, `decimal`, `string`,
`text`, `bool`, `date`, `datetime`, `timestamp`, `timestampz`, `time`, `json`,
`binary`, `uuid`.

Standard analytics types (integers, floats, strings, booleans, dates, timestamps,
decimals) map cleanly across all engines. No user intervention needed.

Exotic types (`GEOGRAPHY`, `HSTORE`, `TSVECTOR`, `INTERVAL`, nested ARRAY/STRUCT)
are mapped to `text` or `json` with a DVT002 warning.

Users can override column types via model `sling` config:
```sql
{{ config(
    connection='source_oracle',
    sling={'columns': {'amount': 'decimal(18,4)', 'notes': 'text'}}
) }}
```

## Sling Transforms (Inline Row-Level)

Sling supports 50+ built-in transform functions applied during extraction.
These are NOT SQL transforms — they're applied row-by-row in Go during streaming.

```sql
-- model config
{{ config(
    connection='source_postgres',
    sling={
        'transforms': {
            'email': ['lower', 'trim_space'],
            'phone': ['replace_non_printable'],
            'name': ['trim_space', 'replace_accents'],
        }
    }
) }}
```

Available functions: `upper`, `lower`, `trim_space`, `replace_accents`,
`replace_non_printable`, `hash` (md5/sha256), `cast`, `coalesce`,
`date_parse`, `date_format`, `int_parse`, `float_parse`, `uuid`, etc.

These are useful for data cleansing during extraction, before the data
reaches the target for SQL-based transformation.

## Sling Hooks in DVT Context

Sling's hook system (pre/post SQL, HTTP webhooks, checks) can be exposed
in model sling config for advanced extraction workflows:

```sql
-- models/staging/stg_orders.sql (future feature)
{{ config(
    connection='source_postgres',
    sling={
        'hooks': {
            'pre': [{'type': 'query', 'query': 'ANALYZE public.orders'}],
            'post': [{'type': 'check', 'check': 'run.total_rows > 0', 'on_failure': 'warn'}],
        }
    }
) }}
```

## Error Handling

When a Sling extraction fails:
1. The extraction node is marked as FAILED in the DVT DAG
2. All downstream models that depend on this source are SKIPPED
3. The error message from Sling is surfaced in DVT's run results
4. `dvt retry` will re-attempt failed extraction nodes and their downstream models

For incremental extractions, failure is safe — the watermark was not updated,
so the next run will re-attempt the same range.

For CDC, Sling's own retry logic handles transient failures (configurable
`retry_attempts` and `retry_delay`).
