# DVT Rules & Specifications

> **Document Version:** 4.0  
> **Last Updated:** March 2026  
> **Status:** Authoritative Reference

This document defines the complete ruleset for DVT (Data Virtualization Tool) behavior. All implementation decisions must conform to these rules.

---

## Table of Contents

1. [Core Principles](#1-core-principles)
2. [Target Rules](#2-target-rules)
3. [Source & Extraction Model Rules](#3-source--extraction-model-rules)
4. [Materialization Rules](#4-materialization-rules)
5. [DAG & Execution Rules](#5-dag--execution-rules)
6. [Writing Rules](#6-writing-rules)
7. [Seed Rules](#7-seed-rules)
8. [Test Rules](#8-test-rules)
9. [Hook Rules](#9-hook-rules)
10. [Edge Cases & Validation](#10-edge-cases--validation)
11. [Backward Compatibility](#11-backward-compatibility)

---

## 1. Core Principles

### 1.1 Pushdown Preference

DVT **always prioritizes Adapter Pushdown** over data movement:

```
IF model.target == ALL upstream.targets THEN
    USE Adapter Pushdown (SQL execution on target database)
ELSE
    USE Extraction via Sling (model has `connection` config pointing to remote source)
```

**Rationale:** Adapter pushdown is faster, requires no data movement, and maintains native database optimizations.

### 1.2 Sling as the Data Movement Engine

Sling handles **ALL** data movement in DVT:
- Cross-engine model extraction (database-to-database streaming)
- Cross-target model materialization (target-to-target streaming)
- Seed loading (CSV-to-database bulk loading)
- CDC (Change Data Capture via database transaction logs)

DVT does **NOT** use Spark, custom JDBC code, or any intermediate compute engine for data movement.

### 1.3 User-Written Extraction Models

DVT does **NOT** auto-generate extraction nodes. Instead, users write **regular dbt models** that use a `connection` config to indicate the data comes from a remote source.

This follows the established dbt "base views" / "staging models" pattern:

```sql
-- models/staging/stg_customers.sql
{{ config(
    materialized='table',
    connection='source_postgres'
) }}

SELECT * FROM {{ source('crm', 'customers') }}
```

**RULE 1.3.1:** The `connection` config on a model tells DVT:
- This model's `{{ source() }}` references live on the specified remote connection
- Sling should be used to extract the data and load it to the model's target
- The model's compiled SQL becomes Sling's source query (executed on the source database)

**RULE 1.3.2:** Users control everything:
- **Naming:** The model is named whatever the user wants (`stg_customers`, `raw_orders`, etc.)
- **SQL:** The user decides what columns, what filters, what logic
- **Materialization:** Standard dbt materializations apply (`table`, `incremental`, etc.)
- **Lineage:** The model is a standard DAG node, visible in lineage, selectable, testable

**RULE 1.3.3:** There are NO auto-generated extraction nodes, NO hidden DAG nodes, NO `dvt_raw` schema managed by DVT.

**Rationale:** Users should have full control over their extraction layer, just like they control their staging layer in dbt. This follows the principle of least surprise and leverages existing dbt patterns.

### 1.4 Filter Optimization (Future — P4)

When extracting data via Sling, DVT **may apply optimization** in future versions:
- Column pruning: only extract columns referenced in the model's SELECT
- Predicate pushdown: push WHERE clauses to the Sling source query

For v1, the user controls what gets extracted by writing the SQL in their extraction model.

### 1.5 Backward Compatibility

When using a single adapter (single target), DVT behaves **identically to dbt**:
- All models use adapter pushdown
- No Sling extraction needed
- No DuckDB involved
- Existing dbt projects work without modification

### 1.6 DuckDB is Scoped

DuckDB is **NOT** the core compute engine. It is used only for:
- `dvt show` — local ad-hoc queries without hitting the warehouse
- Local file/API processing — reading CSV/Parquet/JSON sources locally
- Virtual federation (future) — ephemeral cross-source queries via ATTACH

DuckDB is **never** used in the `dvt run` pipeline.

---

## 2. Target Rules

### 2.1 Target Selection Hierarchy

```
Priority (Highest to Lowest):
1. CLI: --target <name>            (FORCES global override)
2. Model Config: target: <name>    (model-specific)
3. profiles.yml: default target    (base default)
```

### 2.2 profiles.yml Requirements

**RULE 2.2.1:** profiles.yml supports **multiple output types** per profile:
- Database connections (postgres, snowflake, bigquery, redshift, mysql, etc.)
- Cloud bucket connections (s3, gcs, azure)
- All outputs are first-class targets

**RULE 2.2.2:** Each output is identified by its name and used via:
- `target:` in profile config (default target)
- `connection:` in sources.yml (source connections)
- `config(connection='name')` in models (remote source connection)
- `config(target='name')` in models (per-model target override)

### 2.3 Cross-Target References (ref)

**RULE 2.3.1:** When a model refs another model with a different target:
1. The ref'd model is **executed first** (respecting DAG order)
2. The ref'd model is **materialized to its own target**
3. The referencing model's target must have the ref'd data available
4. If the ref'd model's target differs, Sling extracts the result to the referencing model's target

```
Model A (target: postgres)
    |
    | refs
    v
Model B (target: snowflake)
    |
    | Sling streams Model A's result from postgres to snowflake
    | Model B's SQL executes on snowflake via adapter pushdown
    v
Result in Snowflake
```

### 2.4 Global Target Override (CLI)

**RULE 2.4.1:** When `--target` is specified via CLI:
- **ALL** models are forced to materialize in the specified target
- Any source not in this target triggers extraction via Sling
- Model-level target configs are **overridden**

**RULE 2.4.2:** Global target override implications:
```
dvt run --target snowflake_prod

Result:
- All models materialize to snowflake_prod
- Sources with connection != snowflake_prod → Sling extraction
- Sources with connection == snowflake_prod → Pushdown (no extraction)
```

### 2.5 Bucket Targets

**RULE 2.5.1:** Cloud buckets (S3, GCS, Azure) are first-class targets in profiles.yml.

**RULE 2.5.2:** When a model's target is a bucket:
- Model SQL executes on the **default target** (where upstream data lives)
- Sling streams the result to the bucket in the configured format
- Default format: `delta`. Configurable: `parquet`, `csv`, `json`, `jsonlines`

**RULE 2.5.3:** Bucket targets support `format` and `path` model config:
```sql
{{ config(target='data_lake', format='delta', path='analytics/orders/') }}
```

### 2.6 Target Resolution Summary

| Scenario | Model Target | Execution Path |
|----------|--------------|----------------|
| No config, no CLI | profiles.yml default | Depends on upstreams |
| Model config: target_x | target_x | Pushdown if sources on target_x, else extraction |
| CLI: --target target_y | target_y (forced) | Likely extraction for remote sources |
| Model config + CLI | CLI wins | Likely extraction for remote sources |
| Model config: s3_bucket | s3_bucket | SQL on default target, Sling loads to bucket |

---

## 3. Source & Extraction Model Rules

### 3.1 Source Connection Requirements

**RULE 3.1.1:** All sources **MUST** have an explicit `connection` property in `sources.yml`.

```yaml
# sources.yml
sources:
  - name: crm
    connection: source_postgres    # REQUIRED: output name from profiles.yml
    schema: public
    tables:
      - name: customers
      - name: orders
```

**RULE 3.1.2:** If a source does not have a `connection` property:
- DVT raises a **compilation error**: `"Source '<name>' must have a 'connection' property specifying its target"`
- DVT does **NOT** assume a default

**RULE 3.1.3:** `sources.yml` is **metadata only** — it declares what tables exist on what connections. It does NOT contain extraction config (no `sling:` blocks). All extraction behavior is controlled by the **model** that references the source.

**Rationale:** Explicit connections prevent ambiguity. Extraction config belongs on models, not sources.

### 3.2 Extraction Models (Models with `connection` Config)

**RULE 3.2.1:** A model with `config(connection='xxx')` is an **extraction model**:
- Its `{{ source() }}` references live on the specified remote connection
- DVT uses Sling to execute the model's compiled SQL on the **source** database
- Sling streams the result to the model's **target** database
- The model is materialized on the target as a physical table

**RULE 3.2.2:** The extraction model's compiled SQL is sent to Sling as the `src_stream`:
```python
sling.run(
    src_conn=source_connection_url,          # from connection config
    src_stream=compiled_model_sql,           # the model's SQL, compiled by dbt
    tgt_conn=target_connection_url,          # model's target
    tgt_object=f"{schema}.{model_name}",     # materialization target
    mode=sling_mode,                         # mapped from dbt materialization
)
```

**RULE 3.2.3:** The model's SQL runs on the **source** database in the **source's dialect**. Users must write SQL compatible with the source engine. This is intentional — it gives users full control over source-side filtering and logic.

**RULE 3.2.4:** To filter data on a source with complex logic, users write two models:
```sql
-- models/staging/stg_active_customers_pg.sql
-- This runs ON the source (Postgres), filtering in Postgres dialect
{{ config(materialized='view', target='source_postgres') }}
SELECT id, name, email, updated_at
FROM {{ source('crm', 'customers') }}
WHERE is_deleted = false AND region IN ('US', 'EU')

-- models/staging/stg_customers.sql
-- This extracts FROM source TO target via Sling
{{ config(materialized='table', connection='source_postgres') }}
SELECT * FROM {{ ref('stg_active_customers_pg') }}
```

**RULE 3.2.5:** Extraction models with no `connection` config are **standard dbt models** and execute via adapter pushdown. The `connection` config is the ONLY thing that triggers Sling extraction.

### 3.3 Cross-Engine Incremental Models

**RULE 3.3.1:** When an extraction model has `materialized='incremental'` AND `connection` config:
- dbt's `is_incremental()` macro works normally
- DVT maps the dbt incremental strategy to a Sling merge strategy
- Sling handles the merge/upsert on the target — dbt adapter materialization macros are **skipped** for cross-target incremental models

**RULE 3.3.2:** dbt incremental strategy → Sling mode mapping:

| dbt Incremental Strategy | Sling Mode / Merge Strategy |
|---|---|
| `append` | `incremental` with no `primary_key` (append-only) |
| `merge` | `incremental` with `primary_key` (upsert via `update_insert`) |
| `delete+insert` | `incremental` with `primary_key` (via `delete_insert`) |
| `insert_overwrite` | Sling partition overwrite (via `target_options`) |

**RULE 3.3.3:** The model's `unique_key` config maps to Sling's `primary_key`.

**RULE 3.3.4:** `--full-refresh` forces Sling to do `full-refresh` mode (no watermark, no merge — full re-extraction and table rebuild). This matches stock dbt `--full-refresh` behavior.

### 3.4 Watermark Resolution for Cross-Engine Incremental

**RULE 3.4.1:** In stock dbt, `is_incremental()` blocks generate SQL like:
```sql
WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})
```
This subquery works because `{{ this }}` is on the **same** database as the source. In DVT cross-engine models, `{{ this }}` is on the **target** but the WHERE clause runs on the **source**. A cross-database subquery is not possible.

**RULE 3.4.2:** DVT handles this by **pre-resolving** the watermark:
1. During compilation, DVT detects `{{ this }}` references inside `is_incremental()` blocks of extraction models
2. DVT queries the **target** database: `SELECT MAX(updated_at) FROM target_schema.model_table`
3. DVT gets the raw Python value (e.g., `datetime(2024, 3, 14, 12, 0, 0)`)
4. DVT formats the value as a **dialect-specific literal** for the source engine (see Rule 3.5)
5. DVT substitutes the literal into the compiled SQL before sending to Sling

**RULE 3.4.3:** Example resolution:
```sql
-- User writes (in their extraction model):
{% if is_incremental() %}
WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}

-- DVT compiles to (for an Oracle source):
WHERE updated_at > TO_TIMESTAMP('2024-03-14 12:00:00.000000', 'YYYY-MM-DD HH24:MI:SS.FF6')

-- DVT compiles to (for a SQL Server source):
WHERE updated_at > CONVERT(DATETIME2, '2024-03-14 12:00:00.000000', 121)

-- DVT compiles to (for a MySQL source):
WHERE updated_at > '2024-03-14 12:00:00.000000'
```

**RULE 3.4.4:** On first run (table does not exist on target), `is_incremental()` returns `false` (standard dbt behavior). The full query runs without a watermark filter. Sling does a full load.

**RULE 3.4.5:** On `--full-refresh`, `is_incremental()` returns `false` (standard dbt behavior). Full extraction, no watermark.

### 3.5 Dialect-Specific Watermark Formatting

**RULE 3.5.1:** DVT maintains a dialect-specific literal formatter for watermark values. The formatter produces valid SQL literals for the **source** database's dialect.

**RULE 3.5.2:** Timestamp literal formats by source engine:

| Source Engine | Format Template |
|---|---|
| PostgreSQL | `'{value}'::TIMESTAMP` |
| MySQL / MariaDB | `'{value}'` |
| SQL Server | `CONVERT(DATETIME2, '{value}', 121)` |
| Oracle | `TO_TIMESTAMP('{value}', 'YYYY-MM-DD HH24:MI:SS.FF6')` |
| Snowflake | `TO_TIMESTAMP('{value}')` |
| BigQuery | `TIMESTAMP '{value}'` |
| Redshift | `'{value}'::TIMESTAMP` |
| Databricks | `TIMESTAMP '{value}'` |
| ClickHouse | `toDateTime64('{value}', 6)` |
| Trino | `TIMESTAMP '{value}'` |
| DuckDB | `TIMESTAMP '{value}'` |
| SQLite | `'{value}'` |

**RULE 3.5.3:** Date literal formats by source engine:

| Source Engine | Format Template |
|---|---|
| PostgreSQL | `'{value}'::DATE` |
| MySQL / MariaDB | `'{value}'` |
| SQL Server | `CONVERT(DATE, '{value}', 23)` |
| Oracle | `TO_DATE('{value}', 'YYYY-MM-DD')` |
| Snowflake | `TO_DATE('{value}')` |
| BigQuery | `DATE '{value}'` |
| Redshift | `'{value}'::DATE` |
| Databricks | `DATE '{value}'` |
| ClickHouse | `toDate('{value}')` |
| Trino | `DATE '{value}'` |
| DuckDB | `DATE '{value}'` |
| SQLite | `'{value}'` |

**RULE 3.5.4:** Integer/numeric watermarks are formatted as plain literals: `12345`, `99.99`.

**RULE 3.5.5:** String watermarks use dialect-appropriate quoting:
- Most engines: `'value'`
- SQL Server (Unicode): `N'value'`
- All: single quotes are escaped as `''`

**RULE 3.5.6:** The timestamp value is always formatted as `YYYY-MM-DD HH:MM:SS.ffffff` internally before being wrapped in the dialect template. This is the canonical intermediate format.

**RULE 3.5.7:** If the watermark value cannot be formatted for the source dialect:
- DVT raises a **runtime error**: `"DVT109: Cannot format watermark value '<value>' for source dialect '<dialect>'. Column: '<column>', Type: '<type>'"`

### 3.6 Sling Type Mapping

**RULE 3.6.1:** Sling uses a two-step type mapping system:
1. **Source native type → Sling general type** (per source engine)
2. **Sling general type → Target native type** (per target engine)

**RULE 3.6.2:** Sling's general type system:

| General Type | Description |
|---|---|
| `bigint` | 64-bit integer |
| `integer` | 32-bit integer |
| `smallint` | 16-bit integer |
| `float` | Floating point |
| `decimal` | Exact numeric (preserves precision/scale) |
| `string` | Short string (with length) |
| `text` | Unbounded text |
| `bool` | Boolean |
| `date` | Date only |
| `datetime` | Timestamp without timezone |
| `timestamp` | Timestamp without timezone |
| `timestampz` | Timestamp with timezone |
| `time` / `timez` | Time with/without timezone |
| `json` | JSON/semi-structured (covers JSONB, VARIANT, ARRAY, MAP, STRUCT) |
| `binary` | Binary data (BYTEA, BLOB, VARBINARY) |
| `uuid` | UUID |

**RULE 3.6.3:** Standard analytics types (integers, floats, strings, booleans, dates, timestamps, decimals) map cleanly across all engines. No user intervention needed.

**RULE 3.6.4:** Exotic types without cross-engine equivalents (`GEOGRAPHY`, `GEOMETRY`, `HSTORE`, `TSVECTOR`, `INTERVAL`, `CIDR/INET`, nested `ARRAY<STRUCT<...>>`) are mapped to `text` or `json`. DVT emits warning: `"DVT002: Column '<col>' type '<type>' has no direct equivalent on target. Mapped to text/json."`

**RULE 3.6.5:** Users can override column types via Sling's `columns` config in the model:
```sql
{{ config(
    connection='source_oracle',
    materialized='table',
    sling={'columns': {'amount': 'decimal(18,4)', 'notes': 'text'}}
) }}
```

**RULE 3.6.6:** Users can control decimal precision, string length, JSON handling, and boolean casting via Sling's `column_typing` in the model's `sling` config:
```sql
{{ config(
    connection='source_sqlserver',
    materialized='table',
    sling={
        'target_options': {
            'column_typing': {
                'string': {'length_factor': 2},
                'decimal': {'min_precision': 18},
                'json': {'as_text': true},
                'boolean': {'cast_as': 'integer'},
            }
        }
    }
) }}
```

### 3.7 Filtering on Source (Two-Model Pattern)

**RULE 3.7.1:** When a user needs to filter or transform data **on the source** before extraction, they use two models:

1. **A pushdown model on the source** — runs SQL on the source database in the source's native dialect. Can filter, join, aggregate, use database-specific functions.
2. **An extraction model** — refs the pushdown model and extracts the result to the target via Sling.

**RULE 3.7.2:** Example:
```sql
-- Step 1: Filter on source (runs on SQL Server, in T-SQL)
-- models/staging/stg_recent_orders_sqlserver.sql
{{ config(materialized='view', target='source_sqlserver') }}
SELECT TOP 1000000 id, customer_id, order_date, total
FROM {{ source('erp', 'orders') }}
WHERE order_date >= DATEADD(YEAR, -2, GETDATE())
ORDER BY order_date DESC

-- Step 2: Extract filtered result to target (Sling streams SQL Server → Snowflake)
-- models/staging/stg_orders.sql
{{ config(materialized='incremental', connection='source_sqlserver', unique_key='id') }}
SELECT * FROM {{ ref('stg_recent_orders_sqlserver') }}
{% if is_incremental() %}
WHERE order_date > (SELECT MAX(order_date) FROM {{ this }})
{% endif %}
```

**RULE 3.7.3:** This pattern preserves:
- Full dialect support (T-SQL, PL/SQL, etc.) on the source
- Sling handles the data movement
- Incremental logic works across engines
- No dialect mixing — each model speaks one dialect

---

## 4. Materialization Rules

### 4.1 Defaults & Validation

**RULE 4.1.1:** If materialization is unspecified:
- Use project default from `dbt_project.yml`
- If no project default, use `view`

**RULE 4.1.2:** If materialization is invalid/unknown:
- DVT raises a **compilation error**: `"Unknown materialization '<name>'"`
- DVT does **NOT** fall back to a default

### 4.2 Same-Target Execution (Adapter Pushdown)

**Condition:** Model has no `connection` config (or connection matches target)

| Materialization | Behavior |
|-----------------|----------|
| `table` | Create/replace table via adapter SQL |
| `view` | Create/replace view via adapter SQL |
| `incremental` | Apply incremental strategy via adapter SQL |
| `ephemeral` | Compile as CTE, inject into downstream queries |

This is **stock dbt behavior**. DVT delegates entirely to the dbt adapter.

### 4.3 Cross-Target Execution (Extraction Model with `connection` Config)

**Condition:** Model has `connection` config pointing to a remote source

| Materialization | Behavior |
|-----------------|----------|
| `table` | Sling extracts model SQL result from source → target table |
| `view` | **COERCED TO TABLE** with warning DVT001 (cannot create a view from a remote extraction) |
| `incremental` | Sling extracts with watermark → merges into target table (see Section 3.3) |
| `ephemeral` | **NOT SUPPORTED** on extraction models (DVT110 error). Extraction models must materialize physically. |

**RULE 4.3.1:** After extraction, the model exists as a physical table on the target. Downstream models that `ref()` it run via normal adapter pushdown.

### 4.4 View Coercion (DVT001)

**RULE 4.4.1:** An extraction model (`connection` config) with `materialized='view'`:
- DVT emits warning: `"DVT001: Model '<name>' has connection config (extraction model) but is materialized as view. Views cannot be created from remote extraction. Coercing to table."`
- Model is materialized as **table**

**RULE 4.4.2:** A model with `target` config pointing to a bucket with `materialized='view'`:
- Same coercion to table with DVT001 warning

### 4.5 Ephemeral Models

**RULE 4.5.1:** Ephemeral models are compiled as CTEs and injected into downstream queries.

**RULE 4.5.2:** This works identically to stock dbt. Ephemeral models have no interaction with Sling or DuckDB.

**RULE 4.5.3:** Ephemeral models **cannot** have a `connection` config:
- DVT raises a **compilation error**: `"DVT110: Model '<name>' is ephemeral but has a connection config. Extraction models must materialize physically (table or incremental)."`

**RULE 4.5.4:** If an ephemeral model is referenced by **multiple** downstream models:
- The CTE is injected into each downstream model independently
- DVT emits a warning: `"DVT006: Ephemeral model '<name>' is referenced by multiple downstream models. Consider materializing it for performance."`

### 4.6 Incremental on Cross-Engine (Extraction Models)

**RULE 4.6.1:** Incremental extraction models work as follows:
1. DVT pre-resolves the watermark from the target (see Section 3.4)
2. DVT substitutes the dialect-specific literal into the compiled SQL
3. Sling executes the filtered SQL on the source (only delta rows)
4. Sling merges the result into the target table using the mapped merge strategy (see Section 3.3)
5. Sling handles the merge — dbt adapter materialization macros are **skipped**

**RULE 4.6.2:** `--full-refresh`:
- `is_incremental()` returns false (no watermark filter)
- Sling does `full-refresh` mode (drop + create + full load)

### 4.7 Snapshot Materialization

**RULE 4.7.1:** Snapshot models run via the target adapter. If a snapshot's source is on a remote connection, the user must first extract that source to the target via an extraction model, then snapshot the extracted table.

**RULE 4.7.2:** Snapshot models **cannot** have a `connection` config directly. Snapshots require same-target execution for SCD Type 2 logic.

### 4.8 Bucket Materialization

**RULE 4.8.1:** When `config(target='bucket_name')` is set:
- Model SQL executes on the **default target** via adapter pushdown
- Result is written to a temp table on the default target
- Sling streams the result from default target to the bucket
- Temp table is dropped

**RULE 4.8.2:** Default bucket format is `delta`. Configurable via `config(format='...')`:
- `delta` — Delta Lake format (supports versioning, time travel)
- `parquet` — plain Parquet files
- `csv` — CSV files
- `json` — JSON files
- `jsonlines` — JSON Lines files

**RULE 4.8.3:** Bucket path is configurable via `config(path='...')`.

### 4.9 Materialization Summary

| Materialization | Same-Target (no connection) | Extraction Model (connection) | Bucket Target |
|-----------------|---------------------------|-------------------------------|---------------|
| table | Adapter SQL | Sling extracts source SQL → target table | Adapter SQL → Sling to bucket |
| view | Adapter SQL | **Coerced to table** (DVT001) | **Coerced to table** (DVT001) |
| incremental | Adapter SQL | Sling incr extract → Sling merge on target | Adapter SQL → Sling to bucket |
| ephemeral | CTE injection | **NOT SUPPORTED** (DVT110) | CTE injection (no bucket write) |
| snapshot | Adapter SQL | **NOT SUPPORTED** directly (extract first, then snapshot) | Not applicable |

---

## 5. DAG & Execution Rules

### 5.1 Resolution Phase

DVT resolves the DAG in this order:

```
Step 1: Parse all models, sources, seeds, tests
Step 2: Resolve TARGET for every model node
        - Apply CLI --target override (if specified)
        - Apply model config target (if specified)
        - Fall back to profiles.yml default
Step 3: Resolve EXECUTION PATH for every model node
        - If model has `connection` config → Extraction (Sling)
        - If model target is a bucket → Pushdown + Sling load
        - Otherwise → Pushdown (adapter)
Step 4: Validate all configurations
        - Check source connections exist in profiles.yml
        - Check model targets exist in profiles.yml
        - Check model connection configs exist in profiles.yml
        - Check extraction models are not ephemeral or snapshot
        - Check for circular dependencies
```

### 5.2 Execution Phase

**RULE 5.2.1:** DVT respects DAG execution order. User-written extraction models run before their dependent downstream models.

```
User-written DAG:
  [stg_customers (connection: pg)] → [stg_orders (connection: pg)] → [dim_customers] → [fct_orders]

Execution:
1. Sling extracts stg_customers SQL result from pg → target
2. Sling extracts stg_orders SQL result from pg → target
3. Adapter runs dim_customers SQL on target (pushdown)
4. Adapter runs fct_orders SQL on target (pushdown)
5. Tests run on target
```

**RULE 5.2.2:** Extraction models at the same DAG level **MAY** run in parallel (controlled by `--threads`).

**RULE 5.2.3:** Models at the same DAG level **MAY** run in parallel (standard dbt behavior).

### 5.3 Parallel Execution

**RULE 5.3.1:** Parallel execution is controlled by `--threads` (like dbt).

**RULE 5.3.2:** Independent nodes (no DAG dependencies) **MAY** run in parallel:
- Extraction models: parallel Sling streams
- Pushdown models: parallel adapter connections

**RULE 5.3.3:** Default thread count: 4 (matches dbt default).

### 5.4 Error Handling

**RULE 5.4.1:** If an extraction model fails (Sling error):
- Mark the model as **Error**
- Mark all downstream models as **Skipped**
- Continue executing independent models

**RULE 5.4.2:** If a pushdown model fails (adapter error):
- Mark the model as **Error**
- Mark all downstream models as **Skipped**
- Continue executing independent models

**RULE 5.4.3:** `--fail-fast` flag:
- If specified: Stop entire run on first failure
- If not specified: Continue with independent models (default)

**RULE 5.4.4:** Error reporting includes:
- Which model failed
- Execution path used (extraction via Sling, pushdown via adapter, cross-target)
- Underlying error message (Sling error or adapter error)
- Skipped downstream models

**RULE 5.4.5:** For incremental extraction failures:
- The watermark is NOT updated on failure (Sling does not commit the merge)
- Next run re-attempts the same range (safe retry)

---

## 6. Writing Rules

### 6.1 Table Materialization Write Behavior

**RULE 6.1.1:** Default behavior (no `--full-refresh`):
- **Truncate** existing table
- **Insert** new data
- Preserves table structure, grants, etc.

**RULE 6.1.2:** With `--full-refresh`:
- **Drop** existing table (DROP CASCADE if needed)
- **Create** new table
- **Insert** data

**RULE 6.1.3:** This applies to both pushdown and extraction models. For extraction models, Sling's `full-refresh` mode = drop + create. Sling's `truncate` mode = truncate + insert.

### 6.2 Schema Evolution

**RULE 6.2.1:** If column types change between runs (without `--full-refresh`):
- DVT raises an **error**: `"Schema change detected for model '<name>'. Column '<col>' type changed from '<old>' to '<new>'. Use --full-refresh to apply schema changes."`
- Execution stops for this model

**RULE 6.2.2:** With `--full-refresh`:
- Table is dropped and recreated with new schema
- No error raised

**RULE 6.2.3:** Schema changes include:
- Column type changes
- Column additions (may work without full-refresh via Sling's `add_new_columns` option)
- Column removals
- Column renames

### 6.3 Incremental Write Behavior

**RULE 6.3.1:** For pushdown models: incremental strategy handled by dbt adapter (standard).

**RULE 6.3.2:** For extraction models: incremental strategy handled by Sling merge (see Section 3.3).

**RULE 6.3.3:** With `--full-refresh`:
- Incremental model treated as `table`
- Full data reload (no watermark, no merge)

---

## 7. Seed Rules

### 7.1 Sling-Based Loading

**RULE 7.1.1:** All seeds use **Sling** for CSV loading.

**RULE 7.1.2:** Sling seed loading provides:
- Native bulk loading (COPY, bcp, sqlldr) — 10-100x faster than dbt's Python INSERT batching
- Better type inference
- Encoding handling (UTF-8, Latin-1, Windows-1252, etc.)
- Schema evolution (auto-adds new columns)

### 7.2 Seed Write Behavior

**RULE 7.2.1:** Default seed behavior (no `--full-refresh`):
- **Truncate** existing table
- **Insert** all data from CSV
- Preserves table structure, grants, etc.

**RULE 7.2.2:** With `--full-refresh` (i.e., `dvt seed --full-refresh`):
- **Drop** existing table
- **Create** new table from CSV
- Full schema inference from CSV

### 7.3 Cross-Target Seeding

**RULE 7.3.1:** Seeds can be loaded to any target via CLI:
```bash
dvt seed --target snowflake_prod
dvt seed --target data_lake          # seeds as files to bucket
```

**RULE 7.3.2:** Target resolution for seeds:
```
Priority (Highest to Lowest):
1. CLI: --target <name>
2. profiles.yml: default target
```

### 7.4 Type Handling

**RULE 7.4.1:** Type inference:
- Sling infers types from CSV content
- Types are mapped to target database types via Sling's general type system

**RULE 7.4.2:** Type overrides via `column_types`:
```yaml
seeds:
  my_seed:
    +column_types:
      id: integer
      amount: numeric(10,2)
```

**RULE 7.4.3:** Unknown types default to STRING/VARCHAR.

### 7.5 Seed Configuration

```yaml
seeds:
  my_project:
    +delimiter: ","           # CSV delimiter
    +quote_columns: true      # Quote column names in DDL
    +column_types:            # Explicit type overrides
      id: integer
```

---

## 8. Test Rules

### 8.1 Test Execution Path

**RULE 8.1.1:** Tests **always** use the target adapter.

**RULE 8.1.2:** Tests **never** use Sling or DuckDB.

**RULE 8.1.3:** Rationale:
- Tests verify data in the target database
- The model has already been materialized to target
- No cross-database queries needed for testing

### 8.2 Test Configuration

**RULE 8.2.1:** Test severity and thresholds work identically to dbt.

**RULE 8.2.2:** Tests run after their dependent models complete.

### 8.3 Cross-Target Test Limitations

**RULE 8.3.1:** Tests that would require cross-target queries are **NOT supported**.

**RULE 8.3.2:** If a test references models/sources in different targets:
- DVT raises a **compilation error**: `"Test '<name>' references nodes in different targets. Tests must reference nodes in the same target."`

**Guidance:** Test models on their target database. If you need to verify cross-database consistency, create a model that joins the data and test that model.

---

## 9. Hook Rules

### 9.1 Hook Execution

**RULE 9.1.1:** `pre-hook` execution:
- Runs on the **model's target adapter** before model SQL executes
- For extraction models: runs on the **target** adapter BEFORE Sling extraction begins

**RULE 9.1.2:** `post-hook` execution:
- Runs on the **model's target adapter** after model is materialized
- For bucket-target models: runs AFTER model SQL on default target, BEFORE Sling loads to bucket

### 9.2 Hook Execution on Pushdown Path

**RULE 9.2.1:** Standard dbt behavior:
- `pre-hook` runs before model SQL
- `post-hook` runs after model SQL
- Both on the same adapter

### 9.3 Hook Configuration

```yaml
models:
  my_model:
    +pre-hook: "ANALYZE {{ this }}"
    +post-hook:
      - "GRANT SELECT ON {{ this }} TO reporting_role"
      - "CALL refresh_cache('{{ this }}')"
```

### 9.4 Hook Limitations

**RULE 9.4.1:** Hooks are SQL statements executed on the target adapter.

**RULE 9.4.2:** Hooks do NOT run on source connections. To run SQL on source databases, create a pushdown model on the source target.

---

## 10. Edge Cases & Validation

### 10.1 No Upstream Dependencies

**RULE 10.1.1:** Models with no upstream dependencies (sources or refs):
- Use **target adapter** (Pushdown path)
- No extraction needed
- Example: `SELECT 1 AS id, 'test' AS name`

### 10.2 Duplicate Source Definitions

**RULE 10.2.1:** If the same source is defined with different `connection` values in different schema.yml files:
- DVT raises a **compilation error**: `"Source '<name>' has conflicting connection values: '<conn1>' and '<conn2>'"`

### 10.3 Self-Referencing Models

**RULE 10.3.1:** Models that read from and write to the same table:
- **Allowed only** for `incremental` materialization
- Required for incremental logic (reading existing data via `{{ this }}`)

**RULE 10.3.2:** For non-incremental models:
- DVT raises a **compilation error**: `"Model '<name>' cannot read from itself unless using incremental materialization"`

**RULE 10.3.3:** For extraction models with `is_incremental()`:
- `{{ this }}` refers to the target table (where the model materializes)
- The watermark subquery is pre-resolved by DVT (see Section 3.4)
- The resolved literal is valid in the source's dialect (see Section 3.5)

### 10.4 Circular Target Dependencies

**RULE 10.4.1:** Circular dependencies across targets are **prevented by DAG resolution**.

**RULE 10.4.2:** The DAG ensures:
- Model A must complete before Model B (if B refs A)
- Even across targets, execution order is maintained

### 10.5 Empty Results

**RULE 10.5.1:** If a model produces zero rows:
- Table is still created (empty)
- Not treated as an error
- Downstream models may have zero rows as well

### 10.6 NULL Handling

**RULE 10.6.1:** NULL handling follows standard SQL semantics.

**RULE 10.6.2:** When Sling extracts between databases:
- NULL values are preserved
- Type coercion rules apply per Sling's general type mapping (see Section 3.6)

### 10.7 Source Connection Not in profiles.yml

**RULE 10.7.1:** If a source's `connection` value does not match any output in profiles.yml:
- DVT raises a **compilation error**: `"Source '<name>' references connection '<conn>' which does not exist in profiles.yml"`

### 10.8 Model Connection Not in profiles.yml

**RULE 10.8.1:** If a model's `connection` config does not match any output in profiles.yml:
- DVT raises a **compilation error**: `"Model '<name>' references connection '<conn>' which does not exist in profiles.yml"`

### 10.9 Sling Binary Not Available

**RULE 10.9.1:** If Sling binary is not installed and extraction models exist in the DAG:
- DVT raises a **runtime error**: `"Sling is required for cross-engine extraction but was not found. Run 'dvt sync' or install Sling manually."`
- If no extraction models exist (all models are pushdown), DVT runs normally without Sling.

### 10.10 Extraction Model with Multiple Source Connections

**RULE 10.10.1:** An extraction model can only reference sources from **one** connection:
- All `{{ source() }}` calls in an extraction model must resolve to the same `connection`
- If a model references sources from different connections:
  - DVT raises a **compilation error**: `"DVT111: Model '<name>' references sources from multiple connections ('<conn1>', '<conn2>'). Extraction models must use a single source connection. Create separate extraction models for each source."`

---

## 11. Backward Compatibility

### 11.1 Single-Adapter Projects

**RULE 11.1.1:** Projects using only one adapter work **identically to dbt**:
- All models use Pushdown path
- No Sling extraction needed
- No DuckDB involved
- All existing dbt functionality preserved

**RULE 11.1.2:** Sling is only required when models have `connection` config pointing to a remote source. Single-adapter projects do not need Sling installed.

### 11.2 Migration from dbt

**RULE 11.2.1:** Existing dbt projects can migrate to DVT:
1. Install DVT (`pip install dvt-ce`)
2. Add `connection` property to sources in `sources.yml`
3. Write extraction models for cross-engine sources (staging models with `connection` config)
4. Run `dvt sync` to install adapters and dependencies
5. Run `dvt run` — works like `dbt run` for single-adapter

### 11.3 dbt Compatibility Matrix

| dbt Feature | DVT Single-Adapter | DVT Multi-Adapter |
|-------------|-------------------|-------------------|
| Models | Identical | User-written extraction models + pushdown |
| Seeds | Sling-based (faster) | Sling-based + cross-target |
| Tests | Identical | Target adapter only |
| Snapshots | Identical | Extract first, then snapshot |
| Hooks | Identical | Target adapter (pre/post) |
| Macros | Identical | Identical |
| Packages | Identical | Identical |
| Incremental | Identical | Sling extraction + Sling merge (dialect-aware watermarks) |

---

## Appendix A: Warning & Error Codes

### Warnings

| Code | Message | Trigger |
|------|---------|---------|
| DVT001 | Materialization coerced | View → Table on extraction model or bucket target |
| DVT002 | Type precision loss | Exotic type mapped to text/json during extraction |
| DVT003 | Extraction slow | Full-refresh extraction on large table, suggest incremental |
| DVT004 | JSON handling | JSON semantics may differ across databases |
| DVT005 | Predicate not pushed | WHERE clause could not be optimized for extraction |
| DVT006 | Ephemeral multi-ref | Ephemeral model referenced by multiple downstream models |

### Errors

| Code | Message | Trigger |
|------|---------|---------|
| DVT100 | Connection required | Source missing connection property |
| DVT101 | Connection not found | Source or model connection not in profiles.yml |
| DVT102 | Conflicting connections | Same source, different connections |
| DVT103 | Cross-target test | Test references multiple targets |
| DVT104 | Self-reference | Non-incremental self-referencing model |
| DVT105 | Schema change | Column types changed without --full-refresh |
| DVT106 | Sling not found | Sling binary missing when extraction model exists |
| DVT107 | Invalid materialization | Unknown materialization type |
| DVT108 | Invalid incremental strategy | Unknown incremental strategy on extraction model |
| DVT109 | Watermark format error | Cannot format watermark for source dialect |
| DVT110 | Ephemeral extraction | Extraction model cannot be ephemeral |
| DVT111 | Multi-connection extraction | Extraction model references sources from multiple connections |

---

## Appendix B: Configuration Reference

### profiles.yml (Database + Bucket Connections)

```yaml
my_project:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost
      port: 5432
      user: dev_user
      password: "{{ env_var('DEV_PASSWORD') }}"
      database: dev_db
      schema: analytics

    prod:
      type: snowflake
      account: my_account
      user: prod_user
      password: "{{ env_var('PROD_PASSWORD') }}"
      database: PROD_DB
      warehouse: COMPUTE_WH
      schema: ANALYTICS

    source_sqlserver:
      type: sqlserver
      host: sqlserver.internal
      port: 1433
      user: reader
      password: "{{ env_var('MSSQL_PASSWORD') }}"
      database: ERP

    source_postgres:
      type: postgres
      host: pg.internal
      port: 5432
      user: readonly
      password: "{{ env_var('SOURCE_PG_PASSWORD') }}"
      dbname: production
      schema: public

    data_lake:
      type: s3
      bucket: company-data-lake
      region: us-east-1
      access_key_id: "{{ env_var('AWS_KEY') }}"
      secret_access_key: "{{ env_var('AWS_SECRET') }}"
```

### sources.yml (Metadata Only — No Extraction Config)

```yaml
version: 2

sources:
  - name: crm
    connection: source_postgres          # REQUIRED
    schema: public
    tables:
      - name: customers
      - name: orders

  - name: erp
    connection: source_sqlserver         # REQUIRED
    schema: dbo
    tables:
      - name: invoices
      - name: products
```

### Extraction Model Config

```sql
-- models/staging/stg_customers.sql
-- Simple extraction: full table
{{ config(
    materialized='table',
    connection='source_postgres'
) }}
SELECT * FROM {{ source('crm', 'customers') }}
```

```sql
-- models/staging/stg_orders.sql
-- Incremental extraction with cross-engine watermark
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

```sql
-- models/staging/stg_invoices.sql
-- Extraction with Sling type overrides
{{ config(
    materialized='table',
    connection='source_sqlserver',
    sling={
        'target_options': {
            'column_casing': 'snake',
            'column_typing': {
                'string': {'length_factor': 2},
                'decimal': {'min_precision': 18},
            }
        }
    }
) }}
SELECT * FROM {{ source('erp', 'invoices') }}
```

### Pushdown Model (Standard dbt — No Connection Config)

```sql
-- models/marts/dim_customers.sql
-- Runs on the target via adapter pushdown (standard dbt)
{{ config(materialized='table') }}
SELECT
    c.id,
    c.customer_name,
    c.email,
    COUNT(o.id) AS order_count,
    SUM(o.total) AS lifetime_value
FROM {{ ref('stg_customers') }} c
LEFT JOIN {{ ref('stg_orders') }} o ON c.id = o.customer_id
GROUP BY 1, 2, 3
```

### dbt_project.yml (DVT Extensions)

```yaml
name: my_project
version: '0.1.0'
profile: 'my_project'

model-paths: ["models"]
seed-paths: ["seeds"]
test-paths: ["tests"]

# DVT-specific config (optional)
dvt:
  sling:                                 # project-level sling defaults
    target_options:
      column_casing: snake
```

---

## Appendix C: Decision Summary

| Topic | Decision |
|-------|----------|
| Extraction approach | User-written models with `connection` config. NO auto-generated nodes. |
| computes.yml | **REMOVED.** No Spark. No intermediate compute engine. |
| Source without connection | Error (required) |
| sources.yml | Metadata only. No `sling:` blocks. No extraction config. |
| Extraction SQL dialect | Source's native dialect. User writes SQL for the source engine. |
| Cross-engine incremental | DVT pre-resolves watermark from target, formats in source dialect, Sling handles merge |
| Watermark formatting | Dialect-specific literals (Oracle: TO_TIMESTAMP, SQL Server: CONVERT, etc.) |
| Incremental merge | Sling handles (not dbt adapter macros) for extraction models |
| Type mapping | Sling's general type system. DVT002 warning for exotic types. |
| Filtering on source | Two-model pattern: pushdown filter model + extraction model |
| Ephemeral extraction | Not supported (DVT110 error). Must materialize physically. |
| Snapshot extraction | Not supported directly. Extract first, then snapshot the extracted table. |
| Multi-connection extraction | Not supported (DVT111 error). One connection per extraction model. |
| Cross-target refs | Execute ref'd first, Sling extracts to referencing model's target |
| Failed node handling | Skip downstream. Incremental watermark safe (not committed on failure). |
| Parallel execution | Configurable via --threads |
| Table write mode | Truncate+Insert by default. Drop+Create on --full-refresh |
| Schema evolution | Error without --full-refresh |
| Tests | Always use target adapter |
| Hooks (pre/post) | Run on target adapter |
| Seed loading | Sling (bulk load). --full-refresh drops table. --target overrides. |
| DuckDB usage | dvt show, local files, virtual federation (future). NOT dvt run. |
| Bucket targets | First-class. Delta default. Configurable format. |
| Sling not installed | Error only if extraction models exist. Optional otherwise. |

---

*End of DVT Rules Document*
