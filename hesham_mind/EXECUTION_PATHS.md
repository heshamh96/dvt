# DVT Execution Paths

## Overview

Every model in the DVT DAG follows one of four execution paths, determined by
whether its sources are remote and its `target` config. Seeds and tests have their own fixed paths.

## Path 1: Automatic Extraction (Remote Source Detection → Sling)

**When:** A model references sources whose `connection` (from sources.yml) differs from the model's target (from profiles.yml default or model config).

**What happens:**
1. DVT detects that one or more `source()` references point to a remote connection
2. For each remote source table, DVT extracts data via Sling into the `_dvt` staging schema on the target (`_dvt.{source_name}__{table_name}`)
3. These staging tables are NOT visible in the DAG or lineage — they are implementation details
4. DVT compiles the model SQL, rewriting `{{ source() }}` refs to point to `_dvt` staging tables
5. The model SQL runs on the target via the dbt adapter (standard pushdown)
6. For incremental models: DVT pre-resolves watermarks from the target with dialect-specific formatting

**Sling call (generated internally by DVT):**
```python
# DVT generates this automatically — the user never sees or configures it
sling.run(
    src_conn=source_connection_url,          # from source's `connection` in sources.yml
    src_stream=f"SELECT * FROM {source_schema}.{source_table}",  # auto-generated
    tgt_conn=target_connection_url,          # model's target from profiles.yml
    tgt_object=f"_dvt.{source_name}__{table_name}",  # staging table
    mode="full-refresh",                     # staging is always full-refresh
)
```

**Incremental watermark resolution:**
For models with `is_incremental()` that reference remote sources:
```
1. DVT queries target: SELECT MAX(updated_at) FROM target_schema.stg_orders
2. Gets raw value: datetime(2024, 3, 14, 12, 0, 0)
3. Formats for source dialect:
   - PostgreSQL: '2024-03-14 12:00:00.000000'::TIMESTAMP
   - Oracle:     TO_TIMESTAMP('2024-03-14 12:00:00.000000', 'YYYY-MM-DD HH24:MI:SS.FF6')
   - SQL Server: CONVERT(DATETIME2, '2024-03-14 12:00:00.000000', 121)
4. Uses watermark to filter the Sling extraction query for the staging table
5. Model SQL runs on target with incremental logic against staged data
```

**Incremental strategy mapping:**

| dbt Strategy | Sling Behavior |
|---|---|
| `append` | Sling `incremental` mode, no primary_key (append-only insert) |
| `merge` | Sling `incremental` mode with primary_key (upsert: update + insert) |
| `delete+insert` | Sling `incremental` mode with `merge_strategy: delete_insert` |
| `insert_overwrite` | Sling partition overwrite via target_options |

**`--full-refresh` behavior:**
- `is_incremental()` returns false → no watermark filter
- Sling re-extracts full source tables to `_dvt` staging
- Model runs full SQL on target

**Example model referencing a remote source:**
```sql
-- models/staging/stg_orders.sql
-- This is a STANDARD dbt model. No connection config. No sling config.
-- DVT detects that source('crm', 'orders') is on source_postgres (remote)
-- and automatically extracts it to _dvt.crm__orders before running this SQL.
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

**DvtModelRunner logic:**
```python
class DvtModelRunner(ModelRunner):
    def execute(self, model, manifest):
        # Check if any source references are on a different connection than model's target
        remote_sources = self.detect_remote_sources(model, manifest)
        if remote_sources:
            self.extract_sources_to_staging(remote_sources)  # Sling → _dvt schema

        if model.config.get("target") != self.default_target:
            return self.execute_cross_target(model, manifest)
        else:
            return super().execute(model, manifest)  # standard dbt pushdown

    def detect_remote_sources(self, model, manifest):
        """For each source() ref in model, check if source.connection != model.target"""
        model_target = model.config.get("target", self.default_target)
        remote = []
        for source_ref in model.source_references:
            source_conn = manifest.sources[source_ref].connection
            if source_conn != model_target:
                remote.append(source_ref)
        return remote
```

## Path 2: Pushdown (Model SQL → Target via dbt Adapter)

**When:** A model's sources are all on the same connection as its target (no remote sources), and its target is the default target.

**What happens:**
1. dbt compiles the model (Jinja → SQL)
2. `{{ ref('stg_customers') }}` resolves to the physical table on the target
3. The dbt adapter executes the SQL on the target database
4. Materialization is handled by the adapter's macros (CREATE TABLE AS, MERGE INTO, etc.)

**This is stock dbt behavior.** No Sling, no DuckDB. DvtModelRunner delegates to
the parent ModelRunner.

**Example:**
```sql
-- models/marts/dim_customers.sql (all sources on target — standard pushdown)
{{ config(materialized='table') }}
SELECT
    c.id, c.name, c.email,
    COUNT(o.id) AS order_count
FROM {{ ref('stg_customers') }} c
LEFT JOIN {{ ref('stg_orders') }} o ON c.id = o.customer_id
GROUP BY 1, 2, 3
```

## Path 3: Cross-Target Materialization (Model → Different Target via Sling)

**When:** A model has `config(target='other_db')` or `config(target='s3_bucket')`
that differs from the default target.

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
        model_target = node.config.get("target", config.default_target)

        # Check if any source references are on a remote connection
        has_remote_sources = any(
            manifest.sources[src].connection != model_target
            for src in node.source_references
        )
        if has_remote_sources:
            return "extraction"  # DVT auto-extracts remote sources to _dvt staging

        # Cross-target: model target differs from default
        if model_target != config.default_target:
            return "cross_target"

        # Default: pushdown via adapter
        return "pushdown"
```

## Summary Table

| Path | Trigger | Data Movement | Compute Engine | Sling | DuckDB |
|------|---------|---------------|----------------|-------|--------|
| Extraction | source.connection != model.target | source → _dvt staging → target | Sling (extract) + Target DB (model SQL) | YES | no |
| Pushdown | all sources on target, target == default | none | Target DB (adapter) | no | no |
| Cross-target | target != default | target → other target/bucket | Target DB (adapter) + Sling (move) | YES | no |
| Local query | `dvt show` | streamed to memory | DuckDB | no | YES |
| Seed loading | `dvt seed` | CSV → target | Sling (bulk load) | YES | no |
