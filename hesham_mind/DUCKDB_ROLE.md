# DuckDB Role in DVT

## Scope

DuckDB serves as DVT's **persistent cache and compute engine**, with these roles:

1. **Persistent extraction cache** (`.dvt/cache.duckdb`) — P1 feature
2. **Extraction compute engine** (`dvt run`) — P1 feature
3. **Local development queries** (`dvt show`) — P3 feature
4. **Local file/API processing** — development use
5. **Virtual federation** (future) — P4 feature

For pushdown models (all sources local), DuckDB is not involved. The target database
(via dbt adapters) handles pushdown.

## Purpose 1: Persistent Extraction Cache

**What:** DuckDB serves as a persistent cache at `.dvt/cache.duckdb` in the project
directory. It stores extracted source tables and model results between runs, enabling
incremental extraction across runs.

**This is a P1 feature** — part of the core `dvt run` pipeline.

**Cache contents:**
- **Source tables** — extracted via Sling, named `{source_name}__{table_name}` (e.g., `crm__orders`)
- **Model results** — output of model SQL, named `{model_name}` (e.g., `stg_orders`)

**Cache-per-source strategy:** If two models reference `crm.orders`, it's extracted once
into DuckDB cache as `crm__orders`. Both models query the same cached table. This avoids
redundant extraction and keeps sources consistent across models in the same run.

### Cache Lifecycle

| Event | Cache Behavior |
|-------|---------------|
| First `dvt run` with extraction | Cache file created at `.dvt/cache.duckdb` |
| Subsequent `dvt run` | Cache persists, enables `is_incremental()` and `{{ this }}` |
| `--full-refresh` | Cache file **deleted**, run proceeds as first run |
| `dvt clean` | Cache file **deleted** |
| `.gitignore` | `.dvt/` should be gitignored |

### is_incremental() Detection

DVT checks whether the model result table exists in the DuckDB cache to determine
`is_incremental()`. This is fast and local — no network call to the target database.

- Model result **exists** in cache → `is_incremental()` returns `true`
- Model result **not** in cache → `is_incremental()` returns `false` (first run)
- `--full-refresh` deletes cache → `is_incremental()` returns `false`

## Purpose 2: Extraction Compute Engine

**What:** ALL extraction model SQL runs in DuckDB, regardless of how many remote sources
the model references. Sling extracts each remote source into the DuckDB cache, the model
SQL executes in DuckDB, and Sling streams the result to the model's named table on the target.

**This is a P1 feature** — part of the core `dvt run` pipeline.

**How:**
1. DuckDB opens persistent cache (`.dvt/cache.duckdb`)
2. For each remote source, Sling extracts data into DuckDB cache
3. Model SQL executes in DuckDB (user wrote it in DuckDB SQL)
4. Model result is cached in DuckDB (for `{{ this }}` in subsequent runs)
5. Sling streams the result from DuckDB to the target

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
│   stg_customers   (cached model result)│
│   stg_orders      (cached model result)│
│   stg_combined    (cached model result)│
└────────────────────────────────────────┘
                │
                │ Sling
                ▼
         Target (Snowflake)
         model's named table
```

### Incremental Extraction Support

Because the DuckDB cache is persistent, incremental materialization is fully supported
for ALL extraction models (single-source and multi-source alike):

```
First run:
  1. is_incremental() → false (no model result in cache)
  2. Sling extracts full source(s) → DuckDB cache
  3. Model SQL runs in DuckDB → result cached
  4. Sling loads full result → target (full-refresh)

Subsequent runs:
  1. is_incremental() → true (model result exists in cache)
  2. Query TARGET: SELECT MAX(updated_at) FROM target.stg_orders → watermark
  3. Format watermark in source dialect
  4. Sling extracts delta from source → DuckDB merges into cached source table
  5. Model SQL runs in DuckDB (with is_incremental() WHERE clause)
  6. Sling loads delta result → target (incremental merge)

--full-refresh:
  1. Delete .dvt/cache.duckdb
  2. Run as first run
```

**Materialization constraints:**

| Materialization | Supported? |
|---|---|
| `table` | Yes |
| `incremental` | **Yes** (via persistent cache) |
| `view` | Coerced to `table` (DVT001 warning) |
| `ephemeral` | Coerced to `table` (DVT001 warning) |

**User-facing rule:** All extraction models (any model with remote sources) must be
written in **DuckDB SQL** syntax. DuckDB executes the model SQL after all remote sources
have been extracted into the cache.

**Memory management:** DuckDB memory is configurable. For large extractions,
monitor memory usage. See Configuration section below.

## Purpose 2: dvt show (Local Development)

**What:** Run model queries locally without hitting the warehouse.

**How:**
1. DuckDB starts in-process (ephemeral, in-memory)
2. Sources are accessed via:
   - DuckDB ATTACH (for Postgres, MySQL, SQLite families)
   - DuckDB scanner extensions (postgres_scanner, mysql_scanner)
   - Direct file reading (Parquet, CSV, JSON on local filesystem or S3/GCS)
3. Model SQL executes in DuckDB
4. Results displayed in terminal
5. DuckDB instance destroyed after query completes

**When SQLGlot is needed:**
If the model SQL is written in a target-specific dialect (e.g., Snowflake SQL),
SQLGlot transpiles it to DuckDB SQL before execution. This handles:
- Function name differences (DATE_TRUNC syntax, NVL vs COALESCE, etc.)
- Quoting style differences
- Type casting syntax

**Example:**
```bash
# Show model output locally
dvt show --select dim_customers --limit 50

# Run inline SQL locally with source references
dvt show --inline "SELECT COUNT(*) FROM {{ source('crm', 'customers') }}"

# Output as CSV
dvt show --select fct_orders --output csv > orders.csv
```

## Purpose 3: Local File and API Processing

**What:** Process local files (CSV, Parquet, JSON) and APIs as sources.

**How:**
When a source connection is a local filesystem or a cloud bucket,
DuckDB reads the files directly using its native readers:

```sql
-- DuckDB can read these natively:
SELECT * FROM 'data/customers.csv'
SELECT * FROM 'data/*.parquet'
SELECT * FROM 's3://bucket/path/*.json'    -- via httpfs extension
SELECT * FROM delta_scan('s3://bucket/delta_table/')  -- via delta extension
```

This is useful when:
- Source data is in files, not databases
- You want to prototype models against local data
- You're working with API responses saved as JSON

**Note:** For production runs (`dvt run`), file-based sources are still
extracted to the target via Sling. DuckDB is only used for `dvt show`
and local development.

## Purpose 4: Virtual Federation (Future)

**What:** Ephemeral cross-source queries without materializing to the target.

**How:**
A future materialization type (`materialized='virtual'` or `materialized='federated'`)
would execute the model in DuckDB instead of pushing down to the target.
DuckDB ATTACHes to multiple sources and runs the join locally.
The result is NOT materialized anywhere — it's computed on-demand.

This is the Denodo-style pattern: data stays in place, query goes to the data.

**When this is useful:**
- Real-time dashboards that need fresh cross-source data
- Ad-hoc exploration across multiple databases
- Small-volume cross-source queries where extraction overhead isn't worth it

**This is a future enhancement (P4).** Not part of the initial release.

## DuckDB Extensions

`dvt sync` installs DuckDB extensions based on the connections in profiles.yml:

| profiles.yml type | DuckDB Extension |
|-------------------|-----------------|
| postgres | postgres_scanner |
| mysql | mysql_scanner |
| sqlite | sqlite_scanner |
| s3 | httpfs |
| gcs | httpfs |
| azure | azure |
| any with Delta format | delta |
| any with Parquet | (built-in) |
| any with JSON | json |
| any with Excel | spatial (for xlsx) |

Extensions are installed to DuckDB's extension directory (usually `~/.duckdb/extensions/`).

## SQLGlot Role

SQLGlot is used ONLY for `dvt show` transpilation:

1. **dvt show transpilation** — when model SQL is in a target dialect (Snowflake, BigQuery,
   Redshift, etc.) and needs to run locally in DuckDB, SQLGlot transpiles it.

2. **Extraction query generation** (future, P4) — when the federation optimizer
   generates source-side queries, SQLGlot ensures they're valid for the source's dialect.

SQLGlot is **NOT** used for extraction models in `dvt run`. Extraction models are
written in DuckDB SQL by the user — no transpilation needed. DuckDB executes the
model SQL natively after all sources have been extracted into the persistent cache.

## Configuration

DuckDB configuration in dbt_project.yml (optional):

```yaml
# dbt_project.yml
dvt:
  cache_dir: ".dvt"              # directory for cache.duckdb (default: .dvt)
  duckdb:
    memory_limit: "4GB"          # max memory for extraction compute and dvt show
    threads: 4                   # parallel threads
    temp_directory: "/tmp/dvt"   # spill directory for large datasets
```

**Cache location:** The persistent DuckDB cache lives at `{cache_dir}/cache.duckdb`
(default: `.dvt/cache.duckdb`). Override via `dvt.cache_dir` in `dbt_project.yml`.

**Note on memory for extraction:** When DuckDB processes extraction models, source
data is stored in the persistent cache file (not purely in-memory). DuckDB manages
memory/disk spilling automatically, but for very large datasets, increase `memory_limit`
and configure `temp_directory` for optimal performance.

## Dependencies

- `duckdb>=0.9.0` — in setup.py install_requires
- `sqlglot>=20.0.0` — in setup.py install_requires (for transpilation)
- DuckDB extensions — installed by `dvt sync`, NOT bundled in the package
