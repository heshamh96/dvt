# DVT Rules & Specifications

> **Document Version:** 7.0  
> **Last Updated:** March 2026  
> **Status:** Authoritative Reference

This document defines the complete ruleset for DVT (Data Virtualization Tool) behavior. All implementation decisions must conform to these rules.

---

## Table of Contents

1. [Core Principles](#1-core-principles)
2. [Target Rules](#2-target-rules)
3. [Source Rules](#3-source-rules)
4. [Data Movement Rules (Sling)](#4-data-movement-rules-sling)
5. [Materialization Rules](#5-materialization-rules)
6. [DAG & Execution Rules](#6-dag--execution-rules)
7. [Writing Rules](#7-writing-rules)
8. [Seed Rules](#8-seed-rules)
9. [Test Rules](#9-test-rules)
10. [Hook Rules](#10-hook-rules)
11. [Edge Cases & Validation](#11-edge-cases--validation)
12. [Backward Compatibility](#12-backward-compatibility)

---

## 1. Core Principles

### 1.1 Three Execution Paths

DVT automatically determines how to execute each model based on where its sources live relative to its target. The user writes standard dbt models — DVT handles the rest.

**Path 1 — Default Pushdown:** All sources on the default target → standard dbt. No Sling, no DuckDB. User writes in target dialect.

**Path 2 — Non-Default Pushdown:** Model targets a non-default adapter (e.g., `config(target='mysql_prod')`) but ALL its sources are on that same non-default target. Pure pushdown via the non-default adapter. User writes in that target's dialect.

**Path 3 — Extraction (DuckDB):** Model references source(s) whose `connection` != model's target. DVT detects this automatically. User writes in **DuckDB SQL** for all extraction models. ALL extraction models go through DuckDB as a persistent cache:

```
Source(s) → Sling → DuckDB (persistent cache at .dvt/cache.duckdb) → Sling → Target
```

There is ONE extraction path — no branching based on source count. Whether a model references 1 or 10 remote sources, the flow is the same: Sling extracts each remote source into DuckDB cache, model SQL runs in DuckDB, Sling loads the result to the target. Incremental materialization is fully supported via the persistent cache.

```
User writes:
  {{ source('crm', 'customers') }}    ← sources.yml says connection: source_postgres
  Model target: prod_snowflake        ← from profiles.yml default

DVT detects:
  source_postgres != prod_snowflake → Extraction path

User sees:
  Standard dbt run. No extra config needed.
  Result lands directly as the model's named table on prod_snowflake.
```

**RULE 1.1.1:** The user **NEVER** configures Sling directly. No `sling:` blocks in sources.yml, no `connection` config on models, no Sling YAML files. DVT handles all Sling orchestration internally based on the source's `connection` and the model's `target`.

**RULE 1.1.2:** There are **no hidden staging tables.** No `_dvt` schema. Results land directly as the model's named table on the target.

### 1.2 Pushdown Preference

DVT **always prioritizes Adapter Pushdown** over data movement:

```
IF source.connection == model.target THEN
    USE Adapter Pushdown (standard dbt — SQL execution on target database)
ELSE (any remote source/ref) THEN
    USE Extraction (Sling → DuckDB cache → Sling → target model table)
```

**Rationale:** Adapter pushdown is fastest. Extraction via DuckDB cache handles all cross-engine scenarios uniformly, with full incremental support.

### 1.3 Data Movement Engines

DVT uses **two** engines for data movement and cross-engine compute:

**Sling** handles all data streaming:
- Extracting remote sources into DuckDB cache (all extraction models)
- Loading results from DuckDB cache to the target (all extraction models)
- Cross-target ref resolution (when ref'd model's target != referencing model's target)
- Seed loading (CSV-to-database bulk loading)
- Cross-target model materialization (when model target != default target and target is a bucket)

**DuckDB** serves as a persistent cache and compute engine:
- **Persistent cache** at `.dvt/cache.duckdb` — stores extracted source tables and model results
- **Compute engine** — all extraction model SQL runs in DuckDB
- **Incremental support** — cache persists between runs, enabling `is_incremental()` and `{{ this }}` for extraction models
- **Local development** — `dvt show` (local ad-hoc queries)

DVT does **NOT** use Spark, custom JDBC code, or PySpark.

### 1.4 User Experience is dbt

The user writes:
- `profiles.yml` — with multiple outputs (databases + buckets). DVT checks `~/.dvt` in addition to `~/.dbt` for profiles.yml (VS Code dbt extension compatibility).
- `sources.yml` — with `connection:` property on each source
- `models/*.sql` — standard dbt SQL with `{{ source() }}`, `{{ ref() }}`, `{{ config() }}`
- `seeds/*.csv` — standard CSV seed files

**That's it.** No Sling config, no extraction models, no connection config on models, no hidden staging tables. DVT figures out the rest.

**Dialect rule for extraction models:** All models with remote sources (extraction path) must be written in **DuckDB SQL**. DuckDB executes the model SQL after all remote sources have been cached.

| Scenario | User Writes In | Runs On |
|---|---|---|
| All sources local (default pushdown) | Target dialect | Target DB via adapter |
| All sources on non-default target | That target's dialect | That target DB via non-default adapter |
| Any remote source(s) (extraction) | DuckDB SQL | DuckDB (persistent cache) |

### 1.5 DuckDB Role

DuckDB serves as a **persistent cache and compute engine** in DVT:

1. **Persistent extraction cache** (`.dvt/cache.duckdb`) — stores extracted source tables and model results between runs. Enables incremental extraction. Cache is per-source: if two models reference `crm.orders`, it's extracted once into DuckDB cache as `crm__orders`. Model results are also cached (e.g., `stg_orders`) for `{{ this }}` support.

2. **Extraction compute engine** (`dvt run`) — ALL extraction model SQL runs in DuckDB, regardless of how many remote sources the model references. This is a P1 feature.

3. **Local development** (`dvt show`) — run model queries locally without hitting the warehouse. SQLGlot transpiles target-dialect SQL to DuckDB SQL for local execution.

4. **Local file/API processing** — reading CSV/Parquet/JSON sources locally.

5. **Virtual federation (future)** — ephemeral cross-source queries via ATTACH.

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
- Database connections (postgres, snowflake, bigquery, redshift, mysql, sqlserver, oracle, etc.)
- Cloud bucket connections (s3, gcs, azure)
- All outputs are first-class: they can be source connections OR model targets

**RULE 2.2.2:** Each output is identified by its name and used via:
- `target:` in profile config (default target for models)
- `connection:` in sources.yml (where source data lives)
- `config(target='name')` in models (per-model target override)

### 2.3 Cross-Target References (ref)

**RULE 2.3.1:** When a model refs another model with a different target:
1. The ref'd model is **executed first** (respecting DAG order)
2. The ref'd model is **materialized to its own target**
3. DVT detects the target mismatch
4. Sling streams the ref'd model's result from its target to the referencing model's target
5. The referencing model's SQL executes on its target via adapter pushdown

```
Model A (target: postgres)    → materialized on postgres
    |
    | ref
    v
Model B (target: snowflake)   → DVT: Sling moves Model A result pg → sf
                              → Model B SQL runs on snowflake (pushdown)
```

### 2.4 Global Target Override (CLI)

**RULE 2.4.1:** When `--target` is specified via CLI:
- **ALL** models are forced to materialize in the specified target
- Any source whose connection differs from this target → Sling extraction
- Model-level target configs are **overridden**

**RULE 2.4.2:** Global target override implications:
```
dvt run --target snowflake_prod

Result:
- All models materialize to snowflake_prod
- Sources with connection != snowflake_prod → Sling extracts automatically
- Sources with connection == snowflake_prod → Pushdown (no data movement)
```

### 2.5 Bucket Targets

**RULE 2.5.1:** Cloud buckets (S3, GCS, Azure) are first-class targets in profiles.yml.

**RULE 2.5.2:** When a model's target is a bucket:
- Model SQL executes on the **default database target** (where upstream data lives)
- Sling streams the result to the bucket in the configured format
- Default format: `delta`. Configurable: `parquet`, `csv`, `json`, `jsonlines`

**RULE 2.5.3:** Bucket targets support `format` and `path` model config:
```sql
{{ config(target='data_lake', format='delta', path='analytics/orders/') }}
```

### 2.6 Target Resolution Summary

| Scenario | Model Target | Execution Path |
|----------|--------------|----------------|
| No config, no CLI | profiles.yml default | Pushdown if sources match, Sling if not |
| Model config: target_x | target_x | Pushdown if sources match, Sling if not |
| CLI: --target target_y | target_y (forced) | Likely Sling for remote sources |
| Model config + CLI | CLI wins | Likely Sling for remote sources |
| Model config: s3_bucket | s3_bucket | SQL on default target, Sling to bucket |

---

## 3. Source Rules

### 3.1 Source Connection Requirements

**RULE 3.1.1:** All sources **MUST** have an explicit `connection` property in `sources.yml`.

```yaml
sources:
  - name: crm
    connection: source_postgres    # REQUIRED: output name from profiles.yml
    schema: public
    tables:
      - name: customers
      - name: orders
```

**RULE 3.1.2:** If a source does not have a `connection` property:
- DVT raises a **compilation error**: `"DVT100: Source '<name>' must have a 'connection' property specifying its database connection"`
- DVT does **NOT** assume a default

**RULE 3.1.3:** The `connection` value must match an output name in profiles.yml:
- If it does not exist: DVT raises **compilation error**: `"DVT101: Source '<name>' references connection '<conn>' which does not exist in profiles.yml"`

**RULE 3.1.4:** `sources.yml` is **metadata only**. It declares:
- What sources exist (name, tables)
- Where they live (connection)
- Their schema/database
- Standard dbt source properties (descriptions, tests, freshness, columns)

It does **NOT** contain any extraction config, Sling config, or data movement config.

**Rationale:** Explicit connections let DVT automatically determine when data movement is needed. The user declares WHERE data lives; DVT handles HOW to get it.

### 3.2 Source Resolution

**RULE 3.2.1:** When a model references `{{ source('crm', 'customers') }}`:
1. DVT looks up the source's `connection` in sources.yml → `source_postgres`
2. DVT resolves the model's target → `prod_snowflake`
3. DVT counts how many remote sources/refs the model has
4. Comparison:
   - If `source_postgres == prod_snowflake` → source is local. Resolve to `schema.table` on target. Standard dbt.
   - If `source_postgres != prod_snowflake` → source is remote. DVT triggers extraction.

**RULE 3.2.2:** For remote sources, DVT uses the unified extraction path:
- Sling extracts each remote source into DuckDB persistent cache (`.dvt/cache.duckdb`)
- Model SQL runs in DuckDB
- Sling loads the result to the model's named table on the target
- Cache is per-source: if two models reference `crm.orders`, it's extracted once as `crm__orders`

**RULE 3.2.3:** The result lands directly as the model's named table. There are no hidden staging tables, no `_dvt` schema, no intermediate copies on the target.

### 3.3 Extraction Model SQL Dialect

**RULE 3.3.1:** All extraction models (any model with at least one remote source) must be written in **DuckDB SQL** syntax. This is the user-facing rule.

**RULE 3.3.2:** DuckDB executes the model SQL after all remote sources have been extracted into the DuckDB persistent cache.

**RULE 3.3.3:** For incremental extraction models that use `is_incremental()`, DVT resolves the watermark from the **target** database and formats it in the **source's dialect** for delta extraction via Sling (see Section 4.4).

---

## 4. Data Movement Rules

### 4.1 When Data Movement is Triggered

Data movement is triggered automatically in these scenarios:

| Scenario | Detection | Action |
|---|---|---|
| Any remote source(s)/ref(s) | `source.connection != model.target` | **Extraction:** Sling → DuckDB cache → model SQL in DuckDB → Sling → model table on target |
| Ref'd model target != this model target | `ref_model.target != model.target` | Move ref'd result → this model's target |
| Model target is a bucket | `model.target.type in (s3, gcs, azure)` | Move result from default target → bucket |
| Seed loading | `dvt seed` command | Load CSV → target via bulk loading |

**RULE 4.1.1:** Data movement is NEVER triggered when all sources and refs are on the model's target. In that case, pure adapter pushdown is used (standard dbt behavior).

### 4.2 DuckDB Cache

DVT uses a **persistent DuckDB cache** at `.dvt/cache.duckdb` in the project directory. This cache stores extracted source tables and model results, enabling incremental extraction across runs.

**RULE 4.2.1:** Cache location: `.dvt/cache.duckdb` in the project root directory. Configurable via `dvt.cache_dir` in `dbt_project.yml`.

**RULE 4.2.2:** Cache is **per-source, not per-model.** If two models reference `crm.orders`, it's extracted once into DuckDB cache as `crm__orders`. Both models query the same cached table. Model results are also cached (e.g., `stg_orders` for `{{ this }}` support).

**RULE 4.2.3:** Cache lifecycle:
- Created on first `dvt run` that needs extraction
- Persists between runs in `.dvt/cache.duckdb`
- `--full-refresh` deletes the cache file, starts from scratch
- `dvt clean` deletes the cache file
- `.gitignore` should include `.dvt/`

**RULE 4.2.4:** Cache naming convention:
- Source tables: `{source_name}__{table_name}` (e.g., `crm__orders`, `erp__invoices`)
- Model results: `{model_name}` (e.g., `stg_orders`, `stg_combined`)

### 4.3 Extraction Path (Unified)

**Condition:** Model references ANY remote `source()` or `ref()` (1 or more).

**RULE 4.3.1:** When a model references any remote source(s):
1. For each remote source/ref, Sling extracts data into DuckDB persistent cache
2. Model SQL executes in DuckDB (user wrote it in DuckDB SQL)
3. Model result is cached in DuckDB (for `{{ this }}` support in subsequent runs)
4. Sling streams the result from DuckDB to the model's named table on the target
5. No intermediate tables on the target, no staging schema

**RULE 4.3.2:** Extraction model materialization support:

| Materialization | Supported? |
|---|---|
| `table` | Yes — full extraction → DuckDB → Sling → model table on target |
| `incremental` | **Yes** — delta extraction → DuckDB merge → model SQL → Sling → target (see Section 4.4) |
| `view` | **Coerced to table** (DVT001 warning) |
| `ephemeral` | **Coerced to table** (DVT001 warning) |

**RULE 4.3.3:** The user writes DuckDB SQL for the model. DVT handles all Sling orchestration to load sources into DuckDB cache and stream the result to the target.

### 4.4 Cross-Engine Incremental Models

Incremental materialization is **fully supported** for all extraction models via the persistent DuckDB cache.

**RULE 4.4.1:** Incremental extraction flow:

```
First run:
  1. is_incremental() → false (model result not in DuckDB cache)
  2. Sling extracts full source(s) → DuckDB cache (e.g., crm__orders)
  3. Model SQL runs in DuckDB → result cached as model_name (e.g., stg_orders)
  4. Sling loads full result → target (full-refresh mode)

Subsequent runs:
  1. is_incremental() → true (stg_orders exists in DuckDB cache)
  2. Query TARGET: SELECT MAX(updated_at) FROM target_schema.stg_orders → watermark
  3. Format watermark in source dialect (see Section 4.5)
  4. Sling extracts delta: SELECT * FROM source.orders WHERE updated_at > <watermark>
  5. DuckDB merges delta into cached crm__orders
  6. Model SQL runs in DuckDB (with is_incremental() WHERE clause applied)
  7. Sling loads delta result → target (incremental merge mode)

--full-refresh:
  1. Delete .dvt/cache.duckdb
  2. Run as first run
```

**RULE 4.4.2:** `is_incremental()` detection: DVT checks whether the model result table exists in the DuckDB cache. This is fast and local — no network call needed.

**RULE 4.4.3:** Watermark resolution strategy:
- Watermark value is queried from the **TARGET** database (accurate, reflects what actually landed)
- Watermark is formatted in the **source's dialect** for delta extraction via Sling
- Sling extracts only delta rows from the source
- DuckDB merges the delta into the cached source table

**RULE 4.4.4:** dbt incremental strategy → Sling merge strategy mapping:

| dbt Strategy | Sling Behavior |
|---|---|
| `append` | Sling `incremental` mode, no primary_key (append-only) |
| `merge` | Sling `incremental` mode with primary_key (upsert) |
| `delete+insert` | Sling `incremental` mode with `merge_strategy: delete_insert` |
| `insert_overwrite` | Sling partition overwrite via target_options |

**RULE 4.4.5:** The model's `unique_key` maps to Sling's `primary_key` for the merge on the model's target table.

**RULE 4.4.6:** Watermark resolution flow:
```
1. Model SQL has: WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})
2. DVT queries target: SELECT MAX(updated_at) FROM target_schema.model_table
3. Gets raw value: datetime(2024, 3, 14, 12, 0, 0)
4. Formats for source dialect (see Section 4.5)
5. Generates Sling extraction query:
   SELECT * FROM source_schema.orders
   WHERE updated_at > TO_TIMESTAMP('2024-03-14 12:00:00.000000', 'YYYY-MM-DD HH24:MI:SS.FF6')
6. Sling extracts delta → DuckDB merges into cached source table
7. Model SQL runs in DuckDB → result cached
8. Sling loads delta to target (incremental merge)
```

**RULE 4.4.7:** On first run (model result not in DuckDB cache): `is_incremental()` returns false. Full extraction, no watermark. Standard dbt behavior.

**RULE 4.4.8:** On `--full-refresh`: `.dvt/cache.duckdb` is deleted. `is_incremental()` returns false. Full extraction, full model rebuild.

### 4.5 Dialect-Specific Watermark Formatting

**RULE 4.5.1:** DVT maintains a dialect-specific literal formatter. The formatter produces valid SQL literals for the **source** database's dialect.

**RULE 4.5.2:** Timestamp literal formats:

| Source Engine | Format |
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

**RULE 4.5.3:** Date literal formats follow the same pattern (e.g., Oracle: `TO_DATE(...)`, SQL Server: `CONVERT(DATE, ...)`, etc.).

**RULE 4.5.4:** Integer/numeric watermarks: plain literals (`12345`, `99.99`).

**RULE 4.5.5:** String watermarks: `'value'` (SQL Server Unicode: `N'value'`). Single quotes escaped as `''`.

**RULE 4.5.6:** If formatting fails: DVT raises **runtime error** `"DVT109: Cannot format watermark value for source dialect"`.

### 4.6 Sling Type Mapping

**RULE 4.6.1:** Sling maps types in two steps: source native → Sling general → target native.

**RULE 4.6.2:** Standard analytics types (integers, floats, strings, booleans, dates, timestamps, decimals) map cleanly across all engines. No user intervention needed.

**RULE 4.6.3:** Exotic types without cross-engine equivalents (`GEOGRAPHY`, `HSTORE`, `TSVECTOR`, `INTERVAL`, nested ARRAY/STRUCT) are mapped to `text` or `json`. DVT emits warning: `"DVT002: Column '<col>' type '<type>' has no direct equivalent on target. Mapped to text/json."`

---

## 5. Materialization Rules

### 5.1 Defaults & Validation

**RULE 5.1.1:** If materialization is unspecified:
- Use project default from `dbt_project.yml`
- If no project default, use `view`

**RULE 5.1.2:** If materialization is invalid/unknown:
- DVT raises a **compilation error**: `"DVT108: Unknown materialization '<name>'"`

### 5.2 Same-Target Execution (Adapter Pushdown)

**Condition:** All sources and refs are on the model's target (no data movement needed)

| Materialization | Behavior |
|-----------------|----------|
| `table` | Create/replace table via adapter SQL |
| `view` | Create/replace view via adapter SQL |
| `incremental` | Apply incremental strategy via adapter SQL |
| `ephemeral` | Compile as CTE, inject into downstream queries |

This is **stock dbt behavior**. DVT delegates entirely to the dbt adapter.

### 5.3 Extraction Execution (Cross-Engine)

**Condition:** At least one source or ref is on a different connection/target

| Materialization | Behavior |
|-----------------|----------|
| `table` | Sling → DuckDB cache (each source). DuckDB runs model SQL. Sling → model's table on target |
| `incremental` | Delta extraction → DuckDB cache merge → model SQL in DuckDB → Sling incremental load → target |
| `view` | **Coerced to table** (DVT001 warning) |
| `ephemeral` | **Coerced to table** (DVT001 warning) |

**Key points:**
- No hidden staging tables. Results land directly as the model's named table.
- Sling is the data movement engine, DuckDB is the compute engine (for ALL extraction models).
- Incremental is fully supported via the persistent DuckDB cache.

### 5.4 View on Extraction Models

**RULE 5.4.1:** Views are NOT supported for extraction models (models with remote sources). Views are coerced to tables:
- DVT emits warning: `"DVT001: Model '<name>' has remote sources and is materialized as view. Coercing to table."`

**RULE 5.4.2:** When a model's target is a **bucket**, views are also coerced to tables:
- DVT emits warning: `"DVT001: Model '<name>' targets a bucket but is materialized as view. Coercing to table."`

### 5.5 Ephemeral Models & Extraction Coercion

**RULE 5.5.1:** Ephemeral models with all local sources are compiled as CTEs and injected into downstream queries. Standard dbt behavior.

**RULE 5.5.2:** Ephemeral models with remote sources are coerced to `table`:
- DVT emits warning: `"DVT001: Model '<name>' has remote sources and is materialized as ephemeral. Coercing to table."`

**RULE 5.5.3:** If an ephemeral model (all local sources) is referenced by **multiple** downstream models:
- DVT emits a warning: `"DVT006: Ephemeral model '<name>' is referenced by multiple downstream models. Consider materializing it for performance."`

**RULE 5.5.4:** View/ephemeral coercion summary for extraction models:
- `view` → coerced to `table` (DVT001 warning)
- `ephemeral` → coerced to `table` (DVT001 warning)
- Rationale: extraction models require physical materialization in the DuckDB cache and on the target. Views and ephemeral CTEs cannot represent cross-engine data movement.

### 5.6 Snapshot Materialization

**RULE 5.6.1:** Snapshots work with remote sources. DVT extracts the source to the target staging area, then the snapshot runs via the adapter on the target database.

**RULE 5.6.2:** Snapshot SCD Type 2 logic executes entirely on the target adapter.

### 5.7 Bucket Materialization

**RULE 5.7.1:** When `config(target='bucket_name')`:
- Model SQL executes on the **default database target** via adapter pushdown
- Result is written to a temp table on the default target
- Sling streams the result → bucket in the configured format
- Temp table is dropped

**RULE 5.7.2:** Default bucket format: `delta`. Configurable via `config(format='...')`:
- `delta`, `parquet`, `csv`, `json`, `jsonlines`

**RULE 5.7.3:** Bucket path configurable via `config(path='...')`.

### 5.8 Materialization Summary

| Materialization | All Local (Pushdown) | Any Remote Source(s) (Extraction) | Bucket Target |
|-----------------|-----------|-------------------------------------|---------------|
| table | Adapter SQL | Sling → DuckDB cache → Sling → model table | Adapter SQL → Sling to bucket |
| view | Adapter SQL | **Coerced to table** (DVT001) | **Coerced to table** (DVT001) |
| incremental | Adapter SQL | Delta → DuckDB cache merge → Sling incremental → model table | Adapter SQL → Sling to bucket |
| ephemeral | CTE injection | **Coerced to table** (DVT001) | CTE (no bucket write) |
| snapshot | Adapter SQL | Sling → DuckDB cache → target, then snapshot | Not applicable |

---

## 6. DAG & Execution Rules

### 6.1 Resolution Phase

```
Step 1: Parse all models, sources, seeds, tests
Step 2: Resolve TARGET for every model
        - CLI --target > model config target > profiles.yml default
Step 3: Resolve DATA MOVEMENT for every model
        - For each source(): compare source.connection vs model.target
        - For each ref(): compare ref'd model.target vs this model.target
        - If any mismatch → mark model for extraction (DuckDB cache path)
Step 4: Validate
        - Check all source connections exist in profiles.yml
        - Check all model targets exist in profiles.yml
        - Check for circular dependencies
```

### 6.2 Execution Phase

**RULE 6.2.1:** For each model in DAG order:
1. **Extract** any remote sources/refs to the model's target (Sling, parallel where possible)
2. **Execute** model SQL on the target (adapter pushdown)
3. **Load** result to alternate target if needed (Sling, for bucket targets)

```
Example DAG:

sources.yml: crm (connection: pg), erp (connection: mssql)
Model target: snowflake (default)
DuckDB cache: .dvt/cache.duckdb

stg_customers (remote source: crm.customers):
  → Extraction: Sling pg.customers → DuckDB cache (crm__customers)
  → Model SQL in DuckDB → result cached as stg_customers
  → Sling loads stg_customers → sf.stg_customers

stg_orders (remote source: crm.orders, incremental):
  → Extraction: watermark from sf.stg_orders, delta via Sling → DuckDB cache merge
  → Model SQL in DuckDB (with WHERE clause) → result cached
  → Sling loads delta → sf.stg_orders (incremental merge)

stg_invoices (remote source: erp.invoices):
  → Extraction: Sling mssql.invoices → DuckDB cache (erp__invoices)
  → Model SQL in DuckDB → result cached as stg_invoices
  → Sling loads stg_invoices → sf.stg_invoices

(above three can run in parallel — cache is per-source)

combined_report (multiple remote sources: crm.customers + erp.products):
  → crm__customers already in DuckDB cache (shared with stg_customers)
  → Sling mssql.products → DuckDB cache (erp__products)
  → DuckDB runs model SQL (join) → result cached
  → Sling loads result → sf.combined_report

dim_customers (all local — refs stg_customers, stg_orders):
  → Pushdown: Snowflake runs SQL (standard dbt)

Tests run on snowflake.
```

**RULE 6.2.2:** Extractions for the same model **MAY** run in parallel.

**RULE 6.2.3:** Independent models **MAY** run in parallel (standard dbt, `--threads`).

### 6.3 Parallel Execution

**RULE 6.3.1:** Controlled by `--threads` (like dbt). Default: 4.

### 6.4 Error Handling

**RULE 6.4.1:** If a Sling extraction fails:
- Mark the extraction as **Error**
- Mark the dependent model as **Error**
- Mark all downstream models as **Skipped**
- Continue executing independent models

**RULE 6.4.2:** If a model fails (adapter error):
- Mark as **Error**, skip downstream, continue independent. Standard dbt.

**RULE 6.4.3:** `--fail-fast`: stop on first failure.

**RULE 6.4.4:** For incremental extraction failures: watermark not updated, next run retries safely.

---

## 7. Writing Rules

### 7.1 Table Materialization Write Behavior

**RULE 7.1.1:** Default (no `--full-refresh`): Truncate + Insert. Preserves structure, grants.

**RULE 7.1.2:** With `--full-refresh`: Drop + Create + Insert.

**RULE 7.1.3:** Applies to both pushdown and extraction-based paths.

### 7.2 Schema Evolution

**RULE 7.2.1:** Column type changes without `--full-refresh`:
- DVT raises **error** `"DVT105: Schema change detected. Use --full-refresh."`

**RULE 7.2.2:** With `--full-refresh`: table dropped and recreated. No error.

### 7.3 Incremental Write Behavior

**RULE 7.3.1:** For pushdown models: adapter handles incremental strategy (standard dbt).

**RULE 7.3.2:** For cross-engine models: Sling handles extraction merge, adapter handles model merge.

**RULE 7.3.3:** `--full-refresh`: full extraction + full model rebuild.

---

## 8. Seed Rules

### 8.1 Sling-Based Loading

**RULE 8.1.1:** All seeds use **Sling** for CSV loading. 10-100x faster than dbt's Python INSERT batching.

**RULE 8.1.2:** Sling uses native bulk loading (COPY, bcp, sqlldr) per target database.

### 8.2 Seed Write Behavior

**RULE 8.2.1:** Default (no `--full-refresh`): Truncate + Insert.

**RULE 8.2.2:** `dvt seed --full-refresh`: Drop + Create + Load.

### 8.3 Cross-Target Seeding

**RULE 8.3.1:** `dvt seed --target <name>` redirects all seeds to the specified target.

**RULE 8.3.2:** Target resolution: CLI --target > profiles.yml default.

### 8.4 Type Handling

**RULE 8.4.1:** Sling infers types from CSV. Standard dbt `column_types` overrides work.

**RULE 8.4.2:** Unknown types default to STRING/VARCHAR.

---

## 9. Test Rules

### 9.1 Test Execution Path

**RULE 9.1.1:** Tests **always** use the target adapter. Never Sling or DuckDB.

**RULE 9.1.2:** Rationale: models are already materialized on the target. Tests verify target data.

### 9.2 Cross-Target Test Limitations

**RULE 9.2.1:** Tests referencing models/sources in different targets:
- DVT raises **compilation error** `"DVT103: Test references nodes in different targets."`

---

## 10. Hook Rules

### 10.1 Hook Execution

**RULE 10.1.1:** `pre-hook`: runs on model's target adapter, BEFORE model SQL (AFTER any Sling extractions).

**RULE 10.1.2:** `post-hook`: runs on model's target adapter, AFTER model SQL (BEFORE any Sling load to bucket).

### 10.2 Pushdown Path

Standard dbt behavior. Both hooks on same adapter.

### 10.3 Limitations

**RULE 10.3.1:** Hooks are SQL on the target adapter. They do NOT run on source connections.

---

## 11. Edge Cases & Validation

### 11.1 No Upstream Dependencies
Models with no sources/refs → pushdown on target. No Sling.

### 11.2 Duplicate Source Definitions
Same source with different `connection` values → **compilation error** `"DVT102"`.

### 11.3 Self-Referencing Models
Allowed only for `incremental`. Otherwise → **compilation error** `"DVT104"`.

### 11.4 Empty Results
Zero rows → table/view still created. Not an error.

### 11.5 NULL Handling
Standard SQL semantics. NULLs preserved across Sling extraction.

### 11.6 Sling Binary Not Available
If extraction needed but Sling missing → **runtime error** `"DVT106"`.
If no extraction needed → DVT works without Sling.

---

## 12. Backward Compatibility

### 12.1 Single-Adapter Projects
**Identical to dbt.** No Sling, no DuckDB, no data movement. All pushdown.

### 12.2 Migration from dbt
1. `pip install dvt-ce`
2. Add `connection:` to sources in `sources.yml`
3. `dvt sync` to install adapters
4. `dvt run` — just works

### 12.3 Compatibility Matrix

| dbt Feature | DVT Single-Adapter | DVT Multi-Adapter |
|-------------|-------------------|-------------------|
| Models | Identical | Transparent Sling extraction + pushdown |
| Seeds | Sling-based (faster) | Sling-based + cross-target |
| Tests | Identical | Target adapter only |
| Snapshots | Identical | Sling extract + adapter snapshot |
| Hooks | Identical | Target adapter (pre/post) |
| Macros | Identical | Identical |
| Packages | Identical | Identical |
| Incremental | Identical | Fully supported via DuckDB persistent cache (dialect-aware watermarks, delta extraction). |

---

## Appendix A: Warning & Error Codes

### Warnings

| Code | Message | Trigger |
|------|---------|---------|
| DVT001 | Materialization coerced | View/ephemeral → Table on extraction model or bucket target |
| DVT002 | Type precision loss | Exotic type mapped to text/json during extraction |
| DVT003 | Extraction slow | Large table full-refresh, suggest incremental |
| DVT006 | Ephemeral multi-ref | Ephemeral referenced by multiple downstream models |

### Errors

| Code | Message | Trigger |
|------|---------|---------|
| DVT100 | Connection required | Source missing `connection` property |
| DVT101 | Connection not found | Source connection not in profiles.yml |
| DVT102 | Conflicting connections | Same source, different connections |
| DVT103 | Cross-target test | Test references multiple targets |
| DVT104 | Self-reference | Non-incremental self-referencing model |
| DVT105 | Schema change | Column types changed without --full-refresh |
| DVT106 | Sling not found | Sling binary missing when extraction needed |
| DVT108 | Unknown materialization | Invalid materialization type |
| DVT109 | Watermark format error | Cannot format watermark for source dialect |
| DVT110 | *(Removed)* | *(Ephemeral extraction models are now coerced to table with DVT001 warning)* |

---

## Appendix B: Configuration Reference

### profiles.yml

```yaml
my_project:
  target: prod_snowflake
  outputs:
    prod_snowflake:
      type: snowflake
      account: xyz123
      user: dvt_user
      password: "{{ env_var('SF_PASS') }}"
      database: ANALYTICS
      warehouse: COMPUTE_WH
      schema: PUBLIC

    source_postgres:
      type: postgres
      host: pg.internal
      port: 5432
      user: readonly
      password: "{{ env_var('PG_PASS') }}"
      dbname: production

    source_sqlserver:
      type: sqlserver
      host: mssql.internal
      port: 1433
      user: reader
      password: "{{ env_var('MSSQL_PASS') }}"
      database: ERP

    data_lake:
      type: s3
      bucket: company-data-lake
      region: us-east-1
      access_key_id: "{{ env_var('AWS_KEY') }}"
      secret_access_key: "{{ env_var('AWS_SECRET') }}"
```

### sources.yml (metadata only)

```yaml
version: 2
sources:
  - name: crm
    connection: source_postgres
    schema: public
    tables:
      - name: customers
      - name: orders

  - name: erp
    connection: source_sqlserver
    schema: dbo
    tables:
      - name: invoices
      - name: products
```

### Models

```sql
-- models/staging/stg_customers.sql
-- EXTRACTION MODEL: Remote source → Extraction path (DuckDB cache)
-- Written in DuckDB SQL. DVT detects: source crm on source_postgres, target is prod_snowflake
-- Sling extracts crm.customers → DuckDB cache → model SQL → Sling → target
{{ config(materialized='table') }}
SELECT * FROM {{ source('crm', 'customers') }}
```

```sql
-- models/staging/stg_orders.sql
-- EXTRACTION MODEL: Remote source, incremental → Extraction path (DuckDB cache)
-- Written in DuckDB SQL. DVT handles watermark formatting for source dialect.
-- Incremental supported: DuckDB cache persists between runs.
{{ config(materialized='incremental', unique_key='id') }}
SELECT * FROM {{ source('crm', 'orders') }}
{% if is_incremental() %}
WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}
```

```sql
-- models/staging/stg_combined.sql
-- EXTRACTION MODEL: Multiple remote sources → Extraction path (DuckDB cache)
-- Written in DuckDB SQL. DuckDB executes the join.
-- Sling extracts each source into DuckDB cache, SQL runs, result → target.
-- Incremental IS supported (DuckDB cache persists between runs).
{{ config(materialized='incremental', unique_key='id') }}
SELECT c.id, c.name, i.invoice_total
FROM {{ source('crm', 'customers') }} c
JOIN {{ source('erp', 'invoices') }} i ON c.id = i.customer_id
{% if is_incremental() %}
WHERE i.updated_at > (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}
```

```sql
-- models/marts/dim_customers.sql
-- PUSHDOWN MODEL: All refs are on target. Standard dbt. Written in target dialect.
{{ config(materialized='table') }}
SELECT c.*, COUNT(o.id) as order_count
FROM {{ ref('stg_customers') }} c
LEFT JOIN {{ ref('stg_orders') }} o ON c.id = o.customer_id
GROUP BY ALL
```

```sql
-- models/archive/archive_orders.sql
-- Cross-target: SQL runs on snowflake, result goes to S3 as Delta.
{{ config(materialized='table', target='data_lake', format='delta') }}
SELECT * FROM {{ ref('stg_orders') }}
```

### dbt_project.yml

```yaml
name: my_project
version: '0.1.0'
profile: 'my_project'

model-paths: ["models"]
seed-paths: ["seeds"]
test-paths: ["tests"]

# No dvt.staging_schema needed — DVT does not create hidden staging tables.
# Results land directly as model tables.
```

---

## Appendix C: Decision Summary

| Topic | Decision |
|-------|----------|
| Data movement trigger | Automatic. source.connection != model.target → extraction path. |
| Any remote source(s) | **Extraction (DuckDB cache)** — Sling → DuckDB cache → model SQL → Sling → model table. Supports incremental. |
| DuckDB cache | **Persistent** at `.dvt/cache.duckdb`. Per-source caching. Persists between runs. `--full-refresh` deletes it. |
| Cache strategy | **Per-source, not per-model.** `crm.orders` extracted once as `crm__orders`, shared by all models. |
| Incremental extraction | **Fully supported.** Watermark from target, delta extraction via Sling, DuckDB cache merge, incremental load to target. |
| User Sling configuration | **NONE.** User never configures Sling. DVT handles it. |
| Model connection config | **NONE.** No `connection` on models. Only on sources. |
| Extraction config on sources | **NONE.** No `sling:` blocks. sources.yml is metadata only. |
| Hidden staging tables | **NONE.** No `_dvt` schema. Results land directly as model tables. |
| Extraction model SQL dialect | **DuckDB SQL.** All models with remote sources are written in DuckDB SQL. |
| Watermark formatting | Dialect-specific literals (Oracle: TO_TIMESTAMP, SQL Server: CONVERT, etc.) |
| Type mapping | Sling general type system. DVT002 warning for exotic types. |
| Cross-target refs | DVT detects, Sling moves result. Transparent to user. |
| View/ephemeral with remote sources | **Coerced to table** (DVT001 warning). |
| Snapshots with remote sources | Supported via extraction path (DuckDB cache). |
| Failed extraction | Error model + skip downstream. Cache not corrupted (DuckDB transactions). |
| Parallel execution | --threads. Extractions for same model can parallelize. |
| Table write mode | Truncate+Insert default. Drop+Create on --full-refresh. |
| Seed loading | Sling bulk load. --full-refresh drops. --target redirects. |
| DuckDB | Persistent extraction cache + compute + dvt show + local files + virtual federation (future). |
| Bucket targets | First-class. Delta default. Configurable format. |
| Sling not installed | Error only if extraction needed. Optional otherwise. |
| Profiles directory | DVT checks `~/.dvt` in addition to `~/.dbt` for profiles.yml. |

---

*End of DVT Rules Document*
