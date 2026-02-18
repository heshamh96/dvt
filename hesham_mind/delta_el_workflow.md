# Delta EL Workflow — Smart Federation EL with Delta Lake Staging

## Current Session Summary

Hesham is building **DVT (Data Virtualization Tool)**, a fork of dbt-core with federated query capabilities across multiple databases using Apache Spark. The overall project involves implementing a **streaming EL pipeline enhancement** with Delta Lake staging for smart incremental federation. This session covered: fixing a critical data corruption bug (string literals instead of real data), comprehensive E2E testing, deep investigation of the EL layer architecture, and producing a detailed implementation plan for the next phase of work.

---

## Instructions

- **Commits go to `extraction_enhancement` branch** with descriptive conventional commit messages
- **Branch is local only — NOT pushed to origin yet**
- Test runner: `.venv/bin/python -m pytest ../tests/unit/federation/ -v` from `core/` directory
- Pre-existing LSP errors throughout the codebase (unresolved imports for optional DB drivers, dbt-core type issues) — **ignore them**
- **CRITICAL DDL CONTRACT** (user emphasized this is very important):
  - `dvt run` (default): **TRUNCATE + INSERT** (preserves table structure)
  - `dvt run --full-refresh`: **DROP + CREATE + INSERT** (rebuilds structure)
- **NO column name sanitization** — use proper quoting for all dialects via adapters
- **When a model targets a specific adapter (e.g., `config(target='dbx_dev')`), the SQL must be written in THAT adapter's dialect.** E.g., backticks for Databricks identifiers, not PG double-quotes. The compiler correctly transpiles between dialects.
- E2E test project is at `Coke_DB` — run commands from that directory using `uv run --project /Users/hex/Documents/My_Projects/DVT/dvt-core/core dvt <command>`
- Do NOT pass `--target pg_dev` for `transactions_pg_to_dbx` (it targets dbx_dev via model config)
- `delta-spark` should be added to the `dvt sync` logic, always coupled with pyspark — NOT as a separate optional dependency
- `{{ this }}` in incremental federation models should **always resolve to the staging Delta table** — scalar subqueries get pre-resolved to literal values before extraction
- Phase 6 is both **LIMIT pushdown AND column selection pushdown** — not just LIMIT
- **Loader simplification**: Remove all COPY/pipe loader paths. Use JDBC for INSERT + dbt adapter for DDL/MERGE. Buckets config stays for staging location only.

---

## Discoveries

### Architecture
- DVT has ~300+ files with ~87 DVT-specific files beyond dbt-core
- **Federation engine** (`core/dvt/federation/`) — 63 files including resolver, engine, EL layer, query optimizer, spark manager, 22 auth handlers, 23 extractors, 7 loaders
- `ExtractionConfig` has `batch_size: int = 100000` at `extractors/base.py:30`
- `LoadConfig` at `loaders/base.py:27-38` has `truncate: bool = True`, `full_refresh: bool = False`
- `--full-refresh` flag is accessible via `getattr(self.config.args, "FULL_REFRESH", False)`

### Cross-Dialect Compilation (DvtCompiler)
- `DvtCompiler` handles cross-dialect SQL transpilation via SQLGlot
- **Source dialect** = determined by `model.config.target` (the adapter the user writes SQL for)
- **Target dialect** = current compilation target (default adapter from profiles.yml)
- When source != target, SQLGlot transpiles the compiled SQL
- **CRITICAL BUG FIXED**: Models targeting `dbx_dev` had PG-style `"double quotes"` (identifiers in PG, but **string literals** in Databricks). This caused every row to contain literal column name text instead of actual data. Fix: use backtick identifiers in DBX-targeting models.

### Databricks Column Name Limitation
- **Databricks Delta does NOT allow spaces in column names** by default
- PG source tables have columns with spaces: `Customer Code`, `SKU Code`, etc.
- Column aliasing with backtick-quoted identifiers IS required when targeting Databricks
- The column ` Days For Delivery ` has a **trailing space** — must match exactly

### EL Layer Architecture — Comprehensive Investigation Results

**Staging is LOCAL-ONLY currently:**
- `get_bucket_path()` returns `None` for cloud buckets → `create_el_layer()` returns `None` → RuntimeError
- No `boto3`, `google.cloud.storage`, or upload code exists anywhere in federation
- Filesystem bucket stages to `.dvt/staging/`

**On-Prem vs Cloud adapter paths:**

| Adapter Type | Extraction Path | Load Path |
|---|---|---|
| **On-prem (PG, MySQL, SQL Server, Oracle, etc.)** | pipe/cursor/COPY → local Parquet (cloud bucket IGNORED) | pipe/COPY protocol/JDBC (no bulk load) |
| **Cloud DW (Snowflake, BQ, Redshift, Databricks, Athena)** | COPY INTO/UNLOAD/EXPORT DATA to cloud bucket (if configured), else cursor/JDBC → local | COPY INTO from cloud bucket (if configured), else JDBC |

**Cloud-native export → Spark read disconnect**: Cloud DW exports to `s3://...`, but engine does `spark.read.parquet(local_path)`. The bridge code is missing.

**Query Optimizer — mostly dead code:**
- Predicate pushdown: **WORKING** (only active optimization)
- Column pushdown: **DISABLED** at `engine.py:406` (`columns=None`) due to case-sensitivity bug (SQLGlot lowercases identifiers)
- LIMIT pushdown: Extracted by optimizer but **NOT WIRED** (no `limit` field on `SourceConfig`)
- SAMPLE/ORDER BY pushdown: Extracted but never used

**Incremental model handling gaps:**
- `is_incremental()` checks if target table exists + materialization is incremental + no full-refresh
- Federation engine sets `mode="append"` for incremental, `mode="overwrite"` for table
- No delta extraction — always full table extract
- No merge/upsert at loader — raw INSERT only
- `{{ this }}` passes through to Spark SQL untranslated (Spark can't resolve target DB table)
- Row hash infrastructure (`StateManager.get_changed_pks()`) is fully implemented but never called
- `column_pruning.py` module exists but is never imported

**Delta Lake vs Iceberg**: Delta Lake is the clear choice — simpler integration, path-based (no catalog server), `delta-spark` pip package, existing Parquet converts in-place, `deltalake` Python package for non-Spark ops.

**`dvt clean` already exists** (`core/dvt/task/clean.py`, 744 lines) with 5 storage backend support (filesystem, HDFS, S3, GCS, Azure). Can be extended for Delta OPTIMIZE/VACUUM.

**`dvt sync` installs pyspark** at `sync.py:609-620` from version in `computes.yml`. Delta-spark should be installed alongside.

### Column Pushdown Bug Details
- `query_optimizer.py:248`: `columns.add(col.name)` — SQLGlot lowercases to `customername`
- `extractors/base.py:221`: `", ".join(config.columns)` — no quoting applied
- Fix: match extracted names case-insensitively against real column metadata from `extractor.get_columns()`, then use properly-quoted real names

### Load Architecture Investigation

**Current load path has 7 loader files with ~2000 lines of code** across multiple tiers:
- `postgres.py` — 4 tiers: psql pipe / streaming COPY (`_SparkRowPipe`) / buffered COPY / JDBC
- `databricks.py` — COPY INTO from cloud (Parquet) / JDBC
- `snowflake.py` — COPY INTO from cloud (via native snowflake.connector) / JDBC
- `bigquery.py` — LOAD DATA from GCS (via bigquery.Client) / JDBC
- `redshift.py` — COPY from S3 (via native redshift_connector) / JDBC
- `generic.py` — CLI pipe detection (mysql/bcp/clickhouse-client) / JDBC

**Key finding**: All loaders ultimately fall back to `_load_jdbc()` from `BaseLoader`. The COPY/pipe paths are performance optimizations that add complexity. With Delta staging + Spark parallelized JDBC, the optimization is marginal for federation workloads.

**Key finding**: Cloud COPY paths (Snowflake, BigQuery, Redshift) cannot read Delta format — they require Parquet. Delta's underlying Parquet files include stale versions from updates/deletes, making direct COPY unsafe. Would need an intermediate export step, defeating the purpose.

**Decision**: Remove all COPY/pipe loader paths. Standardize on JDBC for INSERT + dbt adapter for DDL and MERGE. Buckets config stays for staging location only.

---

## Accomplished

### Committed (8 commits on `extraction_enhancement` branch):
1. `6f2e47000` — Plan A (streaming EL: fetchmany in 20 extractors, streaming COPY, _SparkRowPipe)
2. `54fec8e9e` — Plan B (pipe-based EL: CLI tools, pipe extractors/loaders for PG/MySQL/SQLServer/ClickHouse)
3. `cf4f59e75` — Parquet directory fix (pyarrow.dataset for Spark dirs)
4. `7b5dcf221` — DDL contract fix (main)
5. `d971c8f85` — Double IF NOT EXISTS bugfix
6. `6f582f4f9` — Wire --full-refresh to engine
7. `655d65cc4` — Cloud loader DDL cleanup (Redshift, BigQuery, Snowflake, Databricks)
8. `19ce883a3` — Comprehensive extractor cleanup across all 20 extractors

### Unit Tests: 192/192 passing

### E2E Tests ALL PASSING

| Test | Result |
|------|--------|
| `dvt seed --full-refresh` | 13/13 seeds |
| `dvt compile --select transactions_pg_to_dbx pg_to_databricks transactions_dbx_to_pg pushdown_pg_only` | All compiled correctly |
| `dvt run -s transactions_pg_to_dbx --full-refresh` | 100K rows, Drop+Create, JDBC |
| `dvt run -s transactions_pg_to_dbx` (default) | 100K rows, Truncate, JDBC |
| `dvt run -s pg_to_databricks --full-refresh` | 50 rows, Drop+Create, JDBC |
| `dvt run -s transactions_dbx_to_pg --full-refresh` | 100K rows, Drop+Create, pipe |
| `dvt run -s transactions_dbx_to_pg` (default) | 100K rows, Truncate, pipe |
| `dvt run -s pushdown_pg_only --full-refresh` | 100 rows, pushdown |
| `dvt run -s pushdown_pg_only` (default) | 100 rows, pushdown |
| `dvt run -s cross_pg_databricks --full-refresh` | 50 rows, Drop+Create, pipe |
| `dvt show -s transactions_pg_to_dbx --target dbx_dev` | Real data (Cs 83, Core S 42) |
| `dvt show -s transactions_pg_to_dbx` (default PG) | Real data |
| `dvt show -s transactions_dbx_to_pg --target pg_dev` | Real data |
| `dvt show -s pushdown_pg_only --target pg_dev` | Real data (Arabic names) |
| `dvt show -s pg_to_databricks --target dbx_dev` | Real data (Arabic names) |

### Investigation Completed
- Full EL layer architecture documented
- On-prem vs cloud adapter paths traced precisely
- All 20 extractors and 7 loaders categorized with exact fallback chains
- Query optimizer gaps identified
- Incremental model handling gaps identified
- Delta Lake vs Iceberg comparison done
- `dvt sync`, `dvt clean` existing functionality documented
- **Load architecture fully investigated** — all loader tiers, COPY paths, pipe paths, JDBC fallback documented

---

## 7-Phase Implementation Plan — Smart Federation EL with Delta Lake Staging

| Phase | Effort | Description | Status |
|---|---|---|---|
| **Phase 6: LIMIT + Column pushdown** | 2-3 days | Push LIMIT and column selections to extractors; fix case-sensitivity bug | NOT STARTED — do first |
| **Phase 1: Delta Lake in Spark** | 1 day | Add delta-spark to dvt sync; configure SparkSession with Delta extensions | NOT STARTED |
| **Phase 2: Delta staging** | 2-3 days | EL layer writes Delta tables; engine reads Delta; backward compat | NOT STARTED |
| **Phase 7: Loader simplification** | 2-3 days | Remove COPY/pipe loaders; single JDBC+adapter loader; remove bucket_config from LoadConfig | NOT STARTED |
| **Phase 5: dvt clean extension** | 1 day | Add --optimize flag for Delta OPTIMIZE/VACUUM | NOT STARTED |
| **Phase 4: Incremental load strategies** | 2-3 days | append/merge via temp table + adapter MERGE SQL; unique_key propagation | NOT STARTED |
| **Phase 3: Incremental-aware extraction** | 4-5 days | {{ this }} → staging view; pre-resolve scalars; delta extraction; MERGE into staging | NOT STARTED |

---

## Load Architecture — Simplified JDBC + Adapter Design

### Design Decision

**Remove all COPY/pipe loader paths.** Standardize on:
- **Spark JDBC** (`df.write.jdbc()`) for bulk INSERT (parallelized)
- **dbt adapter** for DDL (CREATE/DROP/TRUNCATE) and MERGE/UPSERT SQL

**Rationale:**
- COPY paths (Snowflake, BigQuery, Redshift) require Parquet on cloud — can't read Delta, adding export step defeats the purpose
- Pipe paths (psql, mysql, bcp) are CSV-based and not supported across all adapters
- JDBC with `numPartitions=4-8` is sufficient for federation workloads (100K-1M rows)
- Adapter MERGE is simpler and more portable than trying to make Spark do upserts
- Reduces ~2000 lines of loader code to ~200 lines

### Buckets Config — What Stays vs Goes

| Aspect | Status | Reason |
|--------|--------|--------|
| `buckets.yml` / bucket config in `user_config.py` | **KEEP** | Configures staging location (`.dvt/staging/` or cloud path for extracts) |
| `LoadConfig.bucket_config` field | **REMOVE** | No longer needed — loaders don't write to cloud buckets |
| `engine._write_to_target()` bucket check | **REMOVE** | No more `can_bulk_load` logic |
| `loader.supports_bulk_load()` | **REMOVE** | No bulk load path |

### New Load Flow

```
Result DataFrame (from Spark federation SQL)
    |
    +-- FULL REFRESH (dvt run --full-refresh):
    |   1. Adapter: DROP TABLE IF EXISTS target CASCADE
    |   2. Adapter: CREATE TABLE target (from DataFrame schema)
    |   3. Spark JDBC: df.write.jdbc(target, mode="append")
    |
    +-- DEFAULT (dvt run — truncate+insert):
    |   1. Adapter: CREATE TABLE IF NOT EXISTS target
    |   2. Adapter: TRUNCATE TABLE target
    |   3. Spark JDBC: df.write.jdbc(target, mode="append")
    |
    +-- INCREMENTAL APPEND:
    |   Spark JDBC: df.write.jdbc(target, mode="append")
    |
    +-- INCREMENTAL MERGE (new — Phase 4):
        1. Spark JDBC: df.write.jdbc(_dvt_staging_<model>, mode="overwrite")  <- temp table
        2. Adapter: MERGE INTO target USING _dvt_staging_<model> ON ...
        3. Adapter: DROP TABLE IF EXISTS _dvt_staging_<model>
```

### How MERGE Works — The Temp Table Pattern

The key insight: **MERGE executes entirely inside the target database.** Both the target table and the staging data must be in the same database for the MERGE SQL to work.

```
Delta staging (.dvt/staging/)     Spark               Target Database
       |                            |                        |
       +-- spark.read.delta ------->|                        |
                                    |-- spark.sql(model) --->|
                                    |   (result DataFrame)   |
                                    |                        |
                                    |-- df.write.jdbc ------>| _dvt_staging_<model> (temp table)
                                    |                        |
                                    |                        | MERGE INTO my_model AS target
                                    |                        | USING _dvt_staging_<model> AS stg
                                    |                        | ON target.id = stg.id
                                    |                        | WHEN MATCHED THEN UPDATE SET ...
                                    |                        | WHEN NOT MATCHED THEN INSERT ...
                                    |                        |
                                    |                        | DROP TABLE _dvt_staging_<model>
```

Step-by-step:
1. Spark writes the federation result into a **temp table inside the target DB** via JDBC
2. The dbt adapter executes a native **MERGE SQL** — both tables are in the same DB, so this runs natively
3. The dbt adapter drops the temp table

### Per-Adapter MERGE SQL (Phase 4)

```sql
-- Postgres (INSERT ON CONFLICT):
INSERT INTO public.my_model (id, name, amount)
SELECT id, name, amount FROM _dvt_staging_my_model
ON CONFLICT (id) DO UPDATE SET
    name = EXCLUDED.name,
    amount = EXCLUDED.amount;

-- Databricks:
MERGE INTO my_model AS target
USING _dvt_staging_my_model AS stg
ON target.id = stg.id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

-- Snowflake:
MERGE INTO my_model AS target
USING _dvt_staging_my_model AS stg
ON target.id = stg.id
WHEN MATCHED THEN UPDATE SET
    target.name = stg.name, target.amount = stg.amount
WHEN NOT MATCHED THEN INSERT (id, name, amount)
    VALUES (stg.id, stg.name, stg.amount);

-- MySQL (INSERT ON DUPLICATE KEY):
INSERT INTO my_model (id, name, amount)
SELECT id, name, amount FROM _dvt_staging_my_model
ON DUPLICATE KEY UPDATE
    name = VALUES(name),
    amount = VALUES(amount);

-- SQL Server:
MERGE INTO my_model AS target
USING _dvt_staging_my_model AS stg
ON target.id = stg.id
WHEN MATCHED THEN UPDATE SET
    target.name = stg.name, target.amount = stg.amount
WHEN NOT MATCHED THEN INSERT (id, name, amount)
    VALUES (stg.id, stg.name, stg.amount);

-- BigQuery:
MERGE INTO my_model AS target
USING _dvt_staging_my_model AS stg
ON target.id = stg.id
WHEN MATCHED THEN UPDATE SET
    target.name = stg.name, target.amount = stg.amount
WHEN NOT MATCHED THEN INSERT (id, name, amount)
    VALUES (stg.id, stg.name, stg.amount);

-- Redshift (no native MERGE — delete+insert pattern):
DELETE FROM my_model
USING _dvt_staging_my_model AS stg
WHERE my_model.id = stg.id;
INSERT INTO my_model SELECT * FROM _dvt_staging_my_model;
```

### New LoadConfig (after simplification)

```python
@dataclass
class LoadConfig:
    table_name: str                                    # schema.table
    mode: str = "overwrite"                            # 'overwrite', 'append'
    truncate: bool = True                              # TRUNCATE instead of DROP for overwrite
    full_refresh: bool = False                         # --full-refresh: DROP + CREATE + INSERT
    connection_config: Optional[Dict[str, Any]] = None # profiles.yml connection dict
    jdbc_config: Optional[Dict[str, Any]] = None       # jdbc_load settings from computes.yml
    # Phase 4 additions:
    incremental_strategy: Optional[str] = None         # 'append', 'delete+insert', 'merge'
    unique_key: Optional[List[str]] = None             # For merge/delete+insert
```

**Removed**: `bucket_config`, `streaming_batch_size`

### Files to Remove (Phase 7)

| File | Lines | Reason |
|------|-------|--------|
| `loaders/postgres.py` | ~540 | psql pipe, streaming COPY, buffered COPY, `_SparkRowPipe` — all replaced by JDBC |
| `loaders/databricks.py` | ~160 | COPY INTO from cloud — replaced by JDBC |
| `loaders/snowflake.py` | ~210 | COPY INTO from cloud via native connector — replaced by JDBC |
| `loaders/bigquery.py` | ~215 | LOAD DATA from GCS via BQ client — replaced by JDBC |
| `loaders/redshift.py` | ~212 | COPY from S3 via native connector — replaced by JDBC |
| `loaders/generic.py` | ~210 | CLI pipe detection (mysql/bcp/clickhouse-client) — replaced by JDBC |
| **Total** | **~1547** | All replaced by single JDBC + adapter pattern in `base.py` |

### Files to Keep and Modify (Phase 7)

| File | Change |
|------|--------|
| `loaders/base.py` | Keep `_load_jdbc()`, `_execute_ddl()`, `_create_table_with_adapter()`. Remove `_load_via_pipe()`. Add `_load_merge()` for temp table + MERGE pattern. |
| `loaders/__init__.py` | Simplify — single `FederationLoader` class, no per-adapter registry. `get_loader()` returns the one loader. |
| `engine.py` `_write_to_target()` | Remove bucket check, `can_bulk_load` logic. Simplify to: get adapter → build LoadConfig → call `loader.load()` |
| `LoadConfig` in `base.py` | Remove `bucket_config`, `streaming_batch_size`. Add `incremental_strategy`, `unique_key` (Phase 4). |

### New Loader Architecture (after Phase 7)

```python
class FederationLoader:
    """Single loader for all federation targets.
    
    Uses Spark JDBC for data transfer + dbt adapter for DDL/MERGE.
    """
    
    def load(self, df, config: LoadConfig, adapter) -> LoadResult:
        """Load DataFrame to target database."""
        if config.incremental_strategy == "merge":
            return self._load_merge(df, config, adapter)
        elif config.incremental_strategy == "delete+insert":
            return self._load_delete_insert(df, config, adapter)
        else:
            # Default: full refresh, truncate+insert, or append
            return self._load_jdbc(df, config, adapter)
    
    def _load_jdbc(self, df, config, adapter) -> LoadResult:
        """INSERT via Spark JDBC with adapter DDL."""
        # 1. DDL via adapter (DROP/TRUNCATE/CREATE)
        # 2. df.write.jdbc() for data
        ...
    
    def _load_merge(self, df, config, adapter) -> LoadResult:
        """MERGE via temp table pattern."""
        # 1. df.write.jdbc() to _dvt_staging_<model> (temp table)
        # 2. Adapter executes MERGE SQL
        # 3. Adapter drops temp table
        ...
    
    def _load_delete_insert(self, df, config, adapter) -> LoadResult:
        """DELETE+INSERT via temp table pattern."""
        # 1. df.write.jdbc() to _dvt_staging_<model> (temp table)
        # 2. Adapter executes DELETE WHERE unique_key IN (SELECT ... FROM staging)
        # 3. Adapter executes INSERT INTO target SELECT * FROM staging
        # 4. Adapter drops temp table
        ...
    
    def _build_merge_sql(self, adapter_type, target, staging, unique_key, columns):
        """Generate dialect-specific MERGE SQL."""
        ...
```

### Delta Lake Impact on Load Side

| Component | Impact |
|-----------|--------|
| Load-side loaders | **None** — Delta is only for extract-side staging |
| `engine._register_temp_views()` | Needs Delta auto-detect: `_delta_log/` → `spark.read.format("delta")` |
| `engine._write_to_target()` | **Simplified** — just JDBC + adapter, no bucket logic |
| Spark JDBC writes | **Unchanged** — `df.write.jdbc()` works the same regardless of source format |
| Adapter MERGE | **New** — executes inside target DB, unrelated to Delta |

**Delta is only used between Extract → Spark. The load side is always JDBC + native adapter SQL.**

---

## Phase 6: LIMIT + Column Selection Pushdown — Detailed Plan

### Problem Statement

The query optimizer correctly extracts column lists and LIMIT values from model SQL, but neither is wired through to the extraction layer. Every federation extraction currently does `SELECT * FROM schema.table` with no row limit — even when the model only uses 3 columns out of 50 and has `LIMIT 100`.

### Data Flow (Current → Target)

```
Current:  Optimizer → columns/limit extracted → DROPPED at engine.py → SELECT * (all rows)
Target:   Optimizer → columns/limit extracted → SourceConfig → ExtractionConfig → SELECT col1, col2 ... LIMIT N
```

### Current Pipeline Gaps

```
QueryOptimizer                    engine.py              el_layer.py         extractors/base.py
extract_all_pushable_operations   _extract_sources       _extract_source     build_export_query
   |                                  |                      |                    |
   v                                  v                      v                    v
PushableOperations -------> SourceConfig ----------> ExtractionConfig -----> SQL query
  .columns = ["id","name"]     .columns = None (!)      .columns = None       SELECT * FROM ...
  .limit = 100                 (no limit field!)         (no limit field!)     (no LIMIT!)
  .predicates = [...]          .predicates = [...]       .predicates = [...]   WHERE ...
```

### Design Decisions Made

1. **Column quoting strategy**: Route extraction SQL through `QueryOptimizer.build_extraction_query()` which uses SQLGlot for dialect-aware SQL generation (proper quoting, LIMIT syntax per dialect). No per-extractor `_quote_identifier()` needed.
2. **`column_pruning.py`**: Delete it. Build case resolution logic fresh in `el_layer.py`.

### Implementation Steps

#### Step 1: Add `limit` field to config dataclasses

**Files:** `el_layer.py`, `extractors/base.py`

Add `limit: Optional[int] = None` to both `SourceConfig` (line 44) and `ExtractionConfig` (line 31).

`SourceConfig` becomes:
```python
@dataclass
class SourceConfig:
    source_name: str
    adapter_type: str
    schema: str
    table: str
    connection: Any
    connection_config: Optional[Dict[str, Any]] = None
    columns: Optional[List[str]] = None
    predicates: Optional[List[str]] = None
    limit: Optional[int] = None          # <-- NEW
    pk_columns: Optional[List[str]] = None
    batch_size: int = 100000
```

`ExtractionConfig` becomes:
```python
@dataclass
class ExtractionConfig:
    source_name: str
    schema: str
    table: str
    columns: Optional[List[str]] = None
    predicates: Optional[List[str]] = None
    limit: Optional[int] = None          # <-- NEW
    pk_columns: Optional[List[str]] = None
    batch_size: int = 100000
    bucket_config: Optional[Dict[str, Any]] = None
    connection_config: Optional[Dict[str, Any]] = None
    jdbc_config: Optional[Dict[str, Any]] = None
```

#### Step 2: Wire columns + limit from optimizer through engine.py

**File:** `engine.py` (lines 392-409)

Replace the disabled column pushdown block:

```python
# BEFORE (current):
# NOTE: Column pushdown is disabled for now because of case sensitivity
# issues with PostgreSQL. SQLGlot lowercases unquoted column names,
# but PostgreSQL preserves case for quoted identifiers.
# TODO: Fix column case handling in QueryOptimizer
sources.append(
    SourceConfig(
        source_name=dep_id,
        adapter_type=connection_config.get("type", ""),
        schema=source.schema,
        table=source.name,
        connection=None,
        connection_config=connection_config,
        columns=None,  # Extract all columns to avoid case issues
        predicates=ops.predicates or None,
    )
)
```

```python
# AFTER:
sources.append(
    SourceConfig(
        source_name=dep_id,
        adapter_type=connection_config.get("type", ""),
        schema=source.schema,
        table=source.name,
        connection=None,
        connection_config=connection_config,
        columns=ops.columns or None,
        predicates=ops.predicates or None,
        limit=ops.limit,
    )
)
```

Column case resolution happens downstream in `el_layer.py` (Step 3), not here.

#### Step 3: Column case resolution in el_layer.py

**File:** `el_layer.py`

Add a module-level helper function:

```python
def _resolve_column_names(
    optimizer_columns: Optional[List[str]],
    real_columns: List[Dict[str, str]],
) -> Optional[List[str]]:
    """Match optimizer's potentially-lowercased column names to actual DB column names.

    SQLGlot normalizes unquoted identifiers to lowercase. This function
    resolves them against the real column metadata from the source database
    using case-insensitive matching.

    Args:
        optimizer_columns: Column names extracted by QueryOptimizer (may be lowercased)
        real_columns: Column metadata from extractor.get_columns() with 'name' and 'type' keys

    Returns:
        List of real column names with correct casing, or None for SELECT *
    """
    if not optimizer_columns:
        return None  # SELECT * — extract all columns

    # Build case-insensitive lookup: lowercased_name -> real_name
    real_name_map = {c["name"].lower(): c["name"] for c in real_columns}

    resolved = []
    for col in optimizer_columns:
        real_name = real_name_map.get(col.lower())
        if real_name:
            resolved.append(real_name)
        # else: column not found in source — skip (defensive, e.g. computed column)

    return resolved if resolved else None
```

Then in `_extract_source()`, after the `extractor.get_columns()` call (line 275) and before building the `ExtractionConfig` (line 301), resolve columns:

```python
# Resolve optimizer column names to real DB column names (case-insensitive)
resolved_columns = _resolve_column_names(source.columns, columns_info)
```

And update the `ExtractionConfig` construction:

```python
config = ExtractionConfig(
    source_name=source.source_name,
    schema=source.schema,
    table=source.table,
    columns=resolved_columns,        # <-- was source.columns
    predicates=source.predicates,
    limit=source.limit,              # <-- NEW
    pk_columns=pk_columns,
    batch_size=source.batch_size,
    bucket_config=self.bucket_config,
    connection_config=source.connection_config,
    jdbc_config=self.jdbc_config,
)
```

#### Step 4: Replace build_export_query() with SQLGlot-based implementation

**File:** `extractors/base.py`

Replace the current f-string `build_export_query()` (lines 209-228) with a SQLGlot-based version that delegates to `QueryOptimizer.build_extraction_query()`:

```python
def build_export_query(
    self,
    config: ExtractionConfig,
) -> str:
    """Build the SELECT query for extraction using SQLGlot.

    Uses QueryOptimizer.build_extraction_query() for dialect-aware SQL
    generation with proper identifier quoting and LIMIT syntax.

    Args:
        config: Extraction configuration

    Returns:
        SQL query string with columns, predicates, and LIMIT applied
    """
    from dvt.federation.query_optimizer import QueryOptimizer, PushableOperations

    ops = PushableOperations(
        source_id=config.source_name,
        source_alias=config.table,
        columns=config.columns or [],
        predicates=config.predicates or [],
        limit=config.limit,
    )

    optimizer = QueryOptimizer()
    return optimizer.build_extraction_query(
        schema=config.schema,
        table=config.table,
        operations=ops,
        target_dialect=self.dialect,
    )
```

**Why this works:**
- `self.dialect` is set to the adapter type on every extractor (e.g., `"postgres"`, `"databricks"`, `"sqlserver"`)
- `build_extraction_query()` calls `get_dialect_for_adapter()` to map to SQLGlot dialect names (e.g., `"sqlserver"` → `"tsql"`)
- SQLGlot handles identifier quoting per dialect: `"col"` for PG, `` `col` `` for MySQL/Databricks, `[col]` for SQL Server
- SQLGlot handles LIMIT syntax per dialect: `LIMIT N` for PG/MySQL, `TOP N` for SQL Server, `FETCH FIRST N ROWS ONLY` for Oracle/DB2
- On failure, `build_extraction_query()` falls back to `build_extraction_query_fallback()` from `dialect_fallbacks.py`
- No extractor overrides `build_export_query()` — this is a single replacement point for all 20+ extractors

#### Step 5: Pass limit through el_layer to ExtractionConfig

Already covered in Step 3 — `limit=source.limit` is passed to `ExtractionConfig`.

#### Step 6: Delete column_pruning.py

**File:** `core/dvt/federation/column_pruning.py` — DELETE

This file is dead code:
- Never imported anywhere in the codebase (verified by grep)
- Its functionality (column analysis, star detection, PK inclusion) is superseded by:
  - `QueryOptimizer._extract_columns()` for column extraction from SQL
  - `_resolve_column_names()` (new, Step 3) for case-insensitive resolution
  - `extractor.get_columns()` for real column metadata

#### Step 7: Unit tests

**New file:** `tests/unit/federation/test_column_limit_pushdown.py`

Test categories:

**A. Column case resolution (`_resolve_column_names`)**
- Lowercased optimizer names → real DB names with mixed case
- Columns with spaces (e.g., `"Customer Code"`)
- Columns not found in source (defensive skip)
- Empty optimizer columns → None (SELECT *)
- All columns found → full list returned

**B. `build_export_query()` with SQLGlot**
- Simple SELECT * (no columns, no predicates, no limit)
- SELECT with specific columns — properly quoted per dialect
- SELECT with predicates — WHERE clause generated
- SELECT with LIMIT — dialect-appropriate syntax (PG LIMIT, SQL Server TOP, Oracle FETCH)
- Combined: columns + predicates + LIMIT
- Columns with spaces and special characters — properly quoted

**C. End-to-end config propagation**
- `PushableOperations.columns` + `.limit` → `SourceConfig.columns` + `.limit` → `ExtractionConfig.columns` + `.limit` → SQL query
- SELECT * when optimizer extracts no columns (e.g., `SELECT *`)
- Predicates still work alongside new column/limit pushdown

#### Step 8: Run full test suite

- All 192 existing unit tests must pass
- E2E tests must show no regressions
- Specifically verify that extraction queries now include column lists and LIMIT when the model SQL has them

---

## Relevant Files / Directories

### Core federation engine & compiler:
- `core/dvt/federation/engine.py` — Federation orchestration; `_write_to_target()`, `_translate_to_spark()`, `_extract_sources()` (line 406 has column pushdown disabled)
- `core/dvt/federation/el_layer.py` — EL orchestration; `SourceConfig` (line 44 has `columns` field), `_extract_source()`, `create_el_layer()`
- `core/dvt/federation/resolver.py` — `ResolvedExecution`, execution path determination
- `core/dvt/federation/query_optimizer.py` — `PushableOperations`, `_extract_columns()` (line 248 has bug), `_extract_limit()`, `_extract_predicates()`
- `core/dvt/federation/column_pruning.py` — Never-imported column analysis module (TO BE DELETED in Phase 6)
- `core/dvt/federation/spark_manager.py` — SparkSession creation (needs Delta config in Phase 1)
- `core/dvt/federation/cloud_storage.py` — CloudStorageHelper (path/credential builder, no data transfer)
- `core/dvt/federation/state_manager.py` — `SourceState`, `should_extract()`, `get_changed_pks()` (row hash infrastructure)
- `core/dvt/federation/dialect_fallbacks.py` — `get_dialect_for_adapter()` canonical adapter→SQLGlot dialect mapping
- `core/dvt/dvt_compilation/dvt_compiler.py` — Cross-dialect transpilation

### Extractors (all under `core/dvt/federation/extractors/`):
- `base.py` — `ExtractionConfig`, `extract()` abstract, `_extract_via_pipe()`, `_extract_jdbc()`, `build_export_query()` (line 221-222 needs SQLGlot replacement)
- `postgres.py`, `mysql.py`, `sqlserver.py`, `clickhouse.py` — On-prem with pipe support
- `oracle.py`, `db2.py`, `teradata.py`, `trino.py`, `exasol.py`, `firebolt.py`, `hive.py`, `spark.py` — JDBC-only
- `snowflake.py`, `bigquery.py`, `redshift.py`, `databricks.py`, `athena.py` — Cloud with native export
- `duckdb.py` — Native COPY TO Parquet
- `generic.py` — Wildcard fallback

### Loaders (all under `core/dvt/federation/loaders/`) — TO BE SIMPLIFIED IN PHASE 7:
- `base.py` — **KEEP**: `LoadConfig`, `_load_jdbc()`, `_execute_ddl()`, `_create_table_with_adapter()`. **REMOVE**: `_load_via_pipe()`. **ADD**: `_load_merge()`, `_load_delete_insert()`, `_build_merge_sql()`
- `postgres.py` — **REMOVE** (4-tier pipe/COPY/JDBC → replaced by base JDBC)
- `databricks.py` — **REMOVE** (COPY INTO from cloud → replaced by base JDBC)
- `snowflake.py` — **REMOVE** (COPY INTO from cloud → replaced by base JDBC)
- `bigquery.py` — **REMOVE** (LOAD DATA from GCS → replaced by base JDBC)
- `redshift.py` — **REMOVE** (COPY from S3 → replaced by base JDBC)
- `generic.py` — **REMOVE** (CLI pipe detection → replaced by base JDBC)
- `__init__.py` — **SIMPLIFY** (single `FederationLoader`, no per-adapter registry)

### DVT tasks & CLI:
- `core/dvt/task/sync.py` — `SyncTask`, pyspark install at lines 609-620 (add delta-spark here in Phase 1)
- `core/dvt/task/clean.py` — `CleanTask`, 5 backend support (extend for Delta OPTIMIZE/VACUUM in Phase 5)
- `core/dvt/task/run.py` — `ModelRunner`, `MicrobatchModelRunner._is_incremental()`
- `core/dvt/cli/main.py` — CLI command definitions
- `core/dvt/cli/params.py` — CLI parameters

### Config & utilities:
- `core/dvt/config/user_config.py` — `get_bucket_path()`, `load_buckets_for_profile()`, `BUCKET_DEPENDENCIES`
- `core/dvt/utils/identifiers.py` — `build_create_table_sql()`, `quote_identifier()`
- `core/dvt/federation/adapter_manager.py` — `AdapterManager.get_adapter()`, `get_quoted_table_name()`
- `core/dvt/context/providers.py` — `this` property (line 1910-1943), Jinja context generation

### Test files:
- `tests/unit/federation/test_streaming_el.py` — 26 tests for Plan A
- `tests/unit/federation/test_pipe_el.py` — 62 tests for Plan B
- `tests/unit/federation/test_ddl_contract.py` — 18 tests for DDL contract (needs update after loader simplification)
- `tests/unit/federation/test_query_optimizer.py` — 32 tests for query optimizer

### E2E test project:
- `/Users/hex/Documents/My_Projects/DVT/Testing_Playground/trial_15_federation_e2e/Coke_DB/` — DVT project root
- `models/federation_tests/transactions_pg_to_dbx.sql` — Uses backtick identifiers (DBX dialect), `` ` Days For Delivery ` `` has trailing space
- `models/federation_tests/pg_to_databricks.sql` — Backtick identifiers
- `models/federation_tests/three_way_to_databricks.sql` — Explicit backtick-quoted PG column aliases
- `models/federation_tests/transactions_dbx_to_pg.sql` — `SELECT *` (DBX→PG, snake_case cols)
- `models/federation_tests/pushdown_pg_only.sql` — `c.*` (PG→PG pushdown)
- `models/federation_tests/cross_pg_databricks.sql` — `c.*` (multi-source→PG)
- `models/federation_tests/federation_sources.yml` — Source definitions with connections
- `seeds/_seeds.yml` — Seed configs (PG + DBX targets)

### Profiles:
- `~/.dvt/profiles.yml` — `pg_dev` (localhost:5433), `dbx_dev` (Databricks), `sf_dev` (Snowflake)

### Plan documents:
- `hesham_mind/Pipes_improvement.md` — Plan A
- `hesham_mind/3-Enhancements/pipes_&_cli_tools_extraction_enhancment.md` — Plan B
- `hesham_mind/1-unordered_thoughts/spark_optimizations.md` — Lists Delta/Iceberg as "Not Selected / Future"

---

## Risk Assessment

### Phase 6 Risks

| Risk | Mitigation |
|------|------------|
| SQLGlot may quote identifiers differently than expected | Existing E2E tests validate actual queries against PG + Databricks |
| LIMIT pushdown changes semantics for non-deterministic queries | Only pushed when model SQL has explicit LIMIT — user intent is clear |
| Column pruning may miss columns used in Spark SQL (post-extraction) | Optimizer extracts columns from the model SQL which is what Spark executes — columns referenced there are preserved |
| `SELECT *` expansion in optimizer | If no specific columns extracted, `columns=[]` → `None` → `SELECT *` (no regression) |
| Some adapters (Vertica, Teradata, DB2, Exasol, Firebolt, Athena) not in dialect map | They fall through to `adapter_type.lower()` which SQLGlot handles gracefully; plus `build_extraction_query()` has a fallback to `build_extraction_query_fallback()` |

### Phase 7 Risks (Loader Simplification)

| Risk | Mitigation |
|------|------------|
| JDBC slower than COPY/pipe for large datasets | Federation workloads are typically <1M rows; Spark parallelizes JDBC writes with `numPartitions` |
| Removing pipe loaders breaks E2E tests that currently use pipe path | E2E tests will naturally fall through to JDBC path — verify all still pass |
| Adapter MERGE SQL varies significantly across dialects | Build per-dialect MERGE templates; test against PG + Databricks first (our E2E targets) |
| Temp table naming collisions in target DB | Use `_dvt_staging_<model_hash>` with unique prefix |
| Temp table not cleaned up on failure | Wrap in try/finally; `dvt clean` can also clean orphaned staging tables |
