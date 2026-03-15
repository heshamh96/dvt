# DVT Execution Paths

## Overview

Every model in the DVT DAG follows one of four execution paths, determined by
its `connection` and `target` config. Seeds and tests have their own fixed paths.

## Path 1: Extraction Model (Model with `connection` Config → Sling)

**When:** A model has `config(connection='xxx')` pointing to a remote source.

**What happens:**
1. DVT compiles the model (Jinja → SQL in the **source's** dialect)
2. For incremental models: DVT pre-resolves the watermark from the target
   and substitutes a dialect-specific literal into the compiled SQL (see below)
3. DVT calls Sling with the compiled SQL as the source stream
4. Sling executes the SQL on the source database
5. Sling streams the result to the model's target database
6. For `table` materialization: Sling does full-refresh (drop + create) or truncate + insert
7. For `incremental` materialization: Sling merges using the mapped strategy

**Sling call:**
```python
sling.run(
    src_conn=source_connection_url,          # from model's `connection` config
    src_stream=compiled_model_sql,           # the model's SQL, compiled by dbt
    tgt_conn=target_connection_url,          # model's target
    tgt_object=f"{schema}.{model_name}",     # materialization target
    mode=sling_mode,                         # mapped from dbt materialization
    primary_key=unique_key,                  # from model's unique_key config
)
```

**Incremental watermark resolution:**
For extraction models with `is_incremental()`:
```
1. DVT queries target: SELECT MAX(updated_at) FROM target_schema.stg_orders
2. Gets raw value: datetime(2024, 3, 14, 12, 0, 0)
3. Formats for source dialect:
   - PostgreSQL: '2024-03-14 12:00:00.000000'::TIMESTAMP
   - Oracle:     TO_TIMESTAMP('2024-03-14 12:00:00.000000', 'YYYY-MM-DD HH24:MI:SS.FF6')
   - SQL Server: CONVERT(DATETIME2, '2024-03-14 12:00:00.000000', 121)
4. Substitutes into compiled SQL:
   WHERE updated_at > TO_TIMESTAMP('2024-03-14 12:00:00.000000', 'YYYY-MM-DD HH24:MI:SS.FF6')
5. Sling executes this on the source, streams delta to target, merges
```

**Incremental strategy mapping:**

| dbt Strategy | Sling Behavior |
|---|---|
| `append` | Sling `incremental` mode, no primary_key (append-only insert) |
| `merge` | Sling `incremental` mode with primary_key (upsert: update + insert) |
| `delete+insert` | Sling `incremental` mode with `merge_strategy: delete_insert` |
| `insert_overwrite` | Sling partition overwrite via target_options |

**`--full-refresh` behavior:**
- `is_incremental()` returns false → no watermark filter, full SQL
- Sling uses `full-refresh` mode → drop + create + full load

**Example extraction model:**
```sql
-- models/staging/stg_orders.sql
{{ config(
    materialized='incremental',
    connection='source_postgres',
    unique_key='id'
) }}
SELECT id, customer_id, order_date, total, updated_at
FROM {{ source('crm', 'orders') }}
{% if is_incremental() %}
WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}
```

**DvtModelRunner logic:**
```python
class DvtModelRunner(ModelRunner):
    def execute(self, model, manifest):
        if model.config.get("connection"):
            return self.execute_extraction(model, manifest)  # Sling path
        elif model.config.get("target") != self.default_target:
            return self.execute_cross_target(model, manifest)
        else:
            return super().execute(model, manifest)  # stock dbt pushdown
```

## Path 2: Pushdown (Model SQL → Target via dbt Adapter)

**When:** A model has NO `connection` config, and its target is the default target.

**What happens:**
1. dbt compiles the model (Jinja → SQL)
2. `{{ ref('stg_customers') }}` resolves to the physical table on the target
   (the extraction model already materialized it there)
3. The dbt adapter executes the SQL on the target database
4. Materialization is handled by the adapter's macros (CREATE TABLE AS, MERGE INTO, etc.)

**This is stock dbt behavior.** No Sling, no DuckDB. DvtModelRunner delegates to
the parent ModelRunner.

**Example:**
```sql
-- models/marts/dim_customers.sql (no connection config — pushdown)
{{ config(materialized='table') }}
SELECT
    c.id, c.name, c.email,
    COUNT(o.id) AS order_count
FROM {{ ref('stg_customers') }} c
LEFT JOIN {{ ref('stg_orders') }} o ON c.id = o.customer_id
GROUP BY 1, 2, 3
```

## Path 3: Cross-Target Materialization (Model → Different Target via Sling)

**When:** A model has NO `connection` config, but `config(target='other_db')` or
`config(target='s3_bucket')` differs from the default target.

**What happens:**
1. The model SQL runs on the DEFAULT target via adapter pushdown
2. The result is written to a temporary table on the default target
3. Sling streams the result FROM the default target TO the model's configured target
4. If the target is a database: Sling creates/upserts the table
5. If the target is a bucket: Sling writes in the configured format (Delta, Parquet, CSV, etc.)
6. The temporary table on the default target is dropped

**Flow:**
```
Default target (Snowflake)           Model target (S3)
┌──────────────────────────┐        ┌──────────────────────────┐
│ 1. Execute model SQL     │        │                          │
│    → temp_result table   │───Sling──→ 3. Write as Delta     │
│ 2. Sling reads result    │        │    s3://bucket/path/     │
│ 4. Drop temp_result      │        │                          │
└──────────────────────────┘        └──────────────────────────┘
```

**Bucket materialization formats:**
- `delta` (default for buckets) — Delta Lake format, supports time travel and incremental
- `parquet` — plain Parquet files
- `csv` — CSV files
- `json` / `jsonlines` — JSON formats

**Model config:**
```sql
{{ config(
    materialized='table',
    target='data_lake',           -- profiles.yml output name
    format='delta',               -- output format for bucket targets
    path='analytics/orders/',     -- path within the bucket
) }}
```

## Path 4: Local Query (dvt show — DuckDB)

**When:** User runs `dvt show --select model_name` or `dvt show --inline "SELECT ..."`.

**What happens:**
1. DuckDB starts in-process (ephemeral, in-memory)
2. For sources that support ATTACH (Postgres, MySQL, SQLite): DuckDB ATTACHes directly
3. For other sources: DuckDB uses scanner extensions
4. Model SQL is transpiled from target dialect to DuckDB SQL via SQLGlot (if needed)
5. Query executes locally in DuckDB
6. Results displayed to terminal
7. No materialization, no data written anywhere

**This is for development and ad-hoc exploration only.** It does NOT replace `dvt run`.

```bash
# Show a model's output locally
dvt show --select dim_customers --limit 100

# Run inline SQL locally
dvt show --inline "SELECT COUNT(*) FROM {{ source('crm', 'customers') }}"
```

## Path 5: Seed Loading (dvt seed — Sling)

**When:** User runs `dvt seed` to load CSV files from the `seeds/` directory.

**What happens:**
1. DVT discovers CSV files in the seed paths
2. For each seed, Sling streams the CSV directly to the target database
3. Much faster than dbt's default Python-based INSERT batching
4. Default: truncate + insert. With `--full-refresh`: drop + create + load.
5. Supports `--target <target_name>` to redirect seeds to a specific target

**Why Sling for seeds:**
- dbt's default seed loader uses Python `agate` library + batch INSERTs
- For a 100K row CSV, dbt takes minutes; Sling takes seconds
- Sling uses native bulk loading (COPY for Postgres, COPY INTO for Snowflake, bcp for SQL Server)
- Sling handles type inference, encoding, and schema evolution automatically

## Execution Path Selection Logic

```python
def resolve_execution_path(node, manifest, config):
    """Determine which path a node takes."""

    if node.resource_type == "seed":
        return "seed_sling"

    if node.resource_type == "test":
        return "pushdown"  # tests always run on the target

    if node.resource_type == "model":
        # Extraction model: has `connection` config
        if node.config.get("connection"):
            return "extraction"

        # Cross-target: model target differs from default
        model_target = node.config.get("target", config.default_target)
        if model_target != config.default_target:
            return "cross_target"

        # Default: pushdown via adapter
        return "pushdown"
```

## The Two-Model Pattern (Filtering on Source)

When users need source-side filtering in a specific dialect, they use two models:

```
[stg_recent_orders_sqlserver]     ← pushdown on SQL Server (T-SQL dialect)
  config(materialized='view', target='source_sqlserver')
  SELECT TOP 1000000 ... WHERE order_date >= DATEADD(YEAR, -2, GETDATE())
         |
         | ref
         v
[stg_orders]                      ← extraction model via Sling
  config(materialized='incremental', connection='source_sqlserver', unique_key='id')
  SELECT * FROM {{ ref('stg_recent_orders_sqlserver') }}
  {% if is_incremental() %} WHERE order_date > ... {% endif %}
         |
         | ref
         v
[fct_orders]                      ← pushdown on target (Snowflake)
  config(materialized='table')
  SELECT ... FROM {{ ref('stg_orders') }} JOIN {{ ref('stg_customers') }}
```

This pattern:
- Keeps dialect-specific SQL in the source-side model
- Sling handles the data movement
- Incremental logic works across engines with dialect-aware watermarks
- No dialect mixing — each model speaks one dialect

## Summary Table

| Path | Trigger | Data Movement | Compute Engine | Sling | DuckDB |
|------|---------|---------------|----------------|-------|--------|
| Extraction | `connection` config on model | source → target | Source DB (query) + Sling (move + merge) | YES | no |
| Pushdown | no `connection`, target == default | none | Target DB (adapter) | no | no |
| Cross-target | no `connection`, target != default | target → other target/bucket | Target DB (adapter) + Sling (move) | YES | no |
| Local query | `dvt show` | streamed to memory | DuckDB | no | YES |
| Seed loading | `dvt seed` | CSV → target | Sling (bulk load) | YES | no |
