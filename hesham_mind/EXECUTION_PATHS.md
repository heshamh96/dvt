# DVT Execution Paths

## Overview

Every model in the DVT DAG follows one of three execution paths. DVT determines the
path automatically based on where the model's sources live relative to its target.
Seeds and tests have their own fixed paths.

### Three Execution Paths

| Path | Condition | Engine | Incremental |
|------|-----------|--------|-------------|
| **Default Pushdown** | All sources on default target | Target DB via adapter | Yes |
| **Non-Default Pushdown** | All sources on non-default target (same as model target) | Non-default target DB via adapter | Yes |
| **Extraction (DuckDB)** | Any remote source/ref | Sling → DuckDB cache → Sling → Target | **Yes** (via persistent cache) |

There are NO sub-paths for extraction. Whether a model references 1 or 10 remote sources,
the flow is the same unified path through the persistent DuckDB cache.

## Path 1: Extraction (DuckDB Cache)

**When:** A model references source(s) whose `connection` (from sources.yml) differs from the model's target (from profiles.yml default or model config).

**User-facing rule:** All extraction models must be written in **DuckDB SQL** syntax. DuckDB SQL is a Postgres-like universal dialect. This is the key distinction from pushdown models which use the target's native dialect. Both dialects coexist in the same project.

### Unified Extraction Flow

**Condition:** Model references ANY remote `source()` or `ref()` (1 or more).

**What happens:**
1. DVT detects that source(s) or ref(s) are on remote connections
2. For each remote source, Sling extracts data into the persistent DuckDB cache (`.dvt/cache.duckdb`)
3. Model SQL executes in DuckDB (user wrote it in DuckDB SQL)
4. Model result is cached in DuckDB (for `{{ this }}` support in subsequent runs)
5. Sling streams the result from DuckDB to the model's named table on the target
6. No intermediate tables on the target, no staging schema

**DuckDB cache is persistent.** It lives at `.dvt/cache.duckdb` in the project directory and persists between runs. This enables:
- **Incremental extraction** — `is_incremental()` checks cache for model existence
- **Shared source caching** — if two models reference `crm.orders`, it's extracted once as `crm__orders`
- **Delta extraction** — on incremental runs, only new/changed rows are extracted from sources

**Flow:**
```
Source A (Postgres)     Source B (SQL Server)
    │                       │
    │ Sling                 │ Sling
    ▼                       ▼
┌────────────────────────────────────────┐
│   DuckDB Cache (.dvt/cache.duckdb)     │
│                                        │
│   crm__customers  (cached source)      │
│   crm__orders     (cached source)      │
│   erp__invoices   (cached source)      │
│                                        │
│   Model SQL runs here                  │
│   (DuckDB SQL syntax)                  │
│                                        │
│   stg_customers   (cached result)      │
│   stg_orders      (cached result)      │
│   stg_combined    (cached result)      │
└────────────────────────────────────────┘
                │
                │ Sling
                ▼
┌────────────────────────────────────────┐
│      Target (e.g., Snowflake)          │
│      model's named table               │
└────────────────────────────────────────┘
```

**Cache lifecycle:**
- Created on first `dvt run` that needs extraction
- Persists between runs in `.dvt/cache.duckdb`
- `--full-refresh` deletes the cache file, starts from scratch
- `dvt clean` deletes the cache file
- `.gitignore` should include `.dvt/`

### Incremental Extraction Flow

```
First run:
  1. is_incremental() → false (model result not in DuckDB cache)
  2. Sling extracts full source(s) → DuckDB cache (e.g., crm__orders)
  3. Model SQL runs in DuckDB → result cached as model_name (e.g., stg_orders)
  4. Sling loads full result → target (full-refresh mode)

Subsequent runs:
  1. is_incremental() → true (stg_orders exists in DuckDB cache)
  2. Query TARGET: SELECT MAX(updated_at) FROM target_schema.stg_orders → watermark
  3. Format watermark in source dialect:
     - PostgreSQL: '2024-03-14 12:00:00.000000'::TIMESTAMP
     - Oracle:     TO_TIMESTAMP('2024-03-14 12:00:00.000000', 'YYYY-MM-DD HH24:MI:SS.FF6')
     - SQL Server: CONVERT(DATETIME2, '2024-03-14 12:00:00.000000', 121)
  4. Sling extracts delta: SELECT * FROM source.orders WHERE updated_at > <watermark>
  5. DuckDB merges delta into cached crm__orders
  6. Model SQL runs in DuckDB (with is_incremental() WHERE clause)
  7. Sling loads delta result → target (incremental merge)

--full-refresh:
  1. Delete .dvt/cache.duckdb
  2. Run as first run
```

**Incremental strategy mapping:**

| dbt Strategy | Sling Behavior |
|---|---|
| `append` | Sling `incremental` mode, no primary_key (append-only insert) |
| `merge` | Sling `incremental` mode with primary_key (upsert: update + insert) |
| `delete+insert` | Sling `incremental` mode with `merge_strategy: delete_insert` |
| `insert_overwrite` | Sling partition overwrite via target_options |

**Materialization constraints (Extraction):**

| Materialization | Supported? |
|---|---|
| `table` | Yes |
| `incremental` | **Yes** (via persistent DuckDB cache) |
| `view` | Coerced to table (DVT001 warning) |
| `ephemeral` | Coerced to table (DVT001 warning) |

**Example model (single remote source, incremental):**
```sql
-- models/staging/stg_orders.sql
-- EXTRACTION MODEL: remote source → Extraction path (DuckDB cache)
-- Written in DuckDB SQL
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

**Example model (multiple remote sources, incremental):**
```sql
-- models/staging/stg_combined.sql
-- EXTRACTION MODEL: multiple remote sources → Extraction path (DuckDB cache)
-- Written in DuckDB SQL. Incremental IS supported.
{{ config(materialized='incremental', unique_key='id') }}
SELECT c.id, c.name, c.email, i.invoice_total, i.invoice_date
FROM {{ source('crm', 'customers') }} c
JOIN {{ source('erp', 'invoices') }} i ON c.id = i.customer_id
{% if is_incremental() %}
WHERE i.invoice_date > (SELECT MAX(invoice_date) FROM {{ this }})
{% endif %}
```

**DvtModelRunner logic:**
```python
class DvtModelRunner(ModelRunner):
    def execute(self, model, manifest):
        model_target = model.config.get("target", self.default_target)

        # Detect remote sources/refs
        remote_sources = self.detect_remote_sources(model, manifest)

        if not remote_sources:
            # Path 1/2: Pushdown — all sources local
            if model_target != self.default_target:
                return self.execute_cross_target(model, manifest)
            else:
                return super().execute(model, manifest)  # standard dbt pushdown

        else:
            # Path 3: Extraction — any remote source(s)
            # Unified path: Sling → DuckDB cache → model SQL → Sling → target
            return self.execute_extraction(model, manifest, remote_sources)

    def detect_remote_sources(self, model, manifest):
        """For each source()/ref() in model, check if connection != model.target"""
        model_target = model.config.get("target", self.default_target)
        remote = []
        for source_ref in model.source_references:
            source_conn = manifest.sources[source_ref].connection
            if source_conn != model_target:
                remote.append(source_ref)
        return remote
```

## Path 2: Pushdown (Default & Non-Default)

### Default Pushdown

**When:** A model's sources are all on the same connection as its target, and its target is the default target.

**Dialect:** Pushdown models are written in the **target's native SQL dialect** (e.g., Snowflake SQL, PostgreSQL, BigQuery SQL). The SQL runs directly on the target engine via the dbt adapter.

**What happens:**
1. dbt compiles the model (Jinja → SQL)
2. `{{ ref('stg_customers') }}` resolves to the physical table on the target
3. The dbt adapter executes the SQL on the target database
4. Materialization is handled by the adapter's macros (CREATE TABLE AS, MERGE INTO, etc.)

**This is stock dbt behavior.** No Sling, no DuckDB. User writes in target dialect.

### Non-Default Pushdown

**When:** Model targets a non-default adapter (e.g., `config(target='mysql_prod')`) but ALL its sources are on that same non-default target.

**Dialect:** Written in the **non-default target's native SQL dialect** (e.g., MySQL SQL if the non-default target is MySQL).

**What happens:**
1. DVT uses the non-default adapter instead of the global default
2. Pure pushdown — SQL runs on the non-default target
3. No Sling, no DuckDB
4. User writes in that target's dialect

**Example:**
```sql
-- models/marts/dim_customers.sql (all refs on target — standard pushdown)
-- Written in target dialect (e.g., Snowflake SQL)
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

        # Detect remote source references
        remote_sources = [
            src for src in node.source_references
            if manifest.sources[src].connection != model_target
        ]

        if remote_sources:
            return "extraction"  # Unified: Sling → DuckDB cache → Sling → target

        # Cross-target: model target differs from default (but all sources local)
        if model_target != config.default_target:
            return "pushdown_non_default"  # Non-default pushdown

        # Default: pushdown via adapter
        return "pushdown_default"
```

## Summary Table

| Path | Trigger | Data Movement | Compute Engine | Incremental | Sling | DuckDB |
|------|---------|---------------|----------------|-------------|-------|--------|
| Default Pushdown | all sources on default target | none | Target DB (adapter) | Yes | no | no |
| Non-Default Pushdown | all sources on non-default target | none | Non-default target DB (adapter) | Yes | no | no |
| Extraction (DuckDB) | any remote source(s)/ref(s) | sources → DuckDB cache → model table on target | DuckDB (persistent cache) + Sling (stream) | **Yes** | YES | YES |
| Cross-target output | target is bucket or other DB | result → other target/bucket | Target DB (adapter) + Sling (move) | Yes | YES | no |
| Local query | `dvt show` | streamed to memory | DuckDB | n/a | no | YES |
| Seed loading | `dvt seed` | CSV → target | Sling (bulk load) | n/a | YES | no |

## `--target` Behavior

The `--target` CLI flag overrides the default target from `profiles.yml`. It is designed for switching between **same-engine environments** (e.g., `--target dev_snowflake` vs `--target prod_snowflake`).

### Why `--target` should not change adapter type

Pushdown models are written in the target engine's native SQL dialect. If the default target is Snowflake and the user runs `--target mysql_docker`:
- All **pushdown models** written in Snowflake SQL will fail — MySQL cannot parse Snowflake-specific syntax (e.g., `FLATTEN()`, `QUALIFY`, `::VARIANT`).
- All **extraction models** will still work — they use DuckDB SQL which is engine-independent.
- **Seeds** and **tests** generally work across engines.

### DVT007 Warning

When the `--target` adapter type differs from the profile's default target adapter type, DVT emits:

```
DVT007: Target override 'mysql_docker' (mysql) differs in adapter type from
default target 'prod_snowflake' (snowflake). Pushdown models written in
snowflake SQL may fail on mysql. Extraction models (DuckDB SQL) are unaffected.
```

DVT warns but does **not** block execution. The user may have a valid reason (e.g., running only extraction models, or running only seeds/tests).
