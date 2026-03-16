# Streaming EL Enhancement - Implementation Plan

## Overview

Implement streaming data movement enhancements for DVT-CE's EL pipeline. In the current architecture, **Sling handles ALL data movement** (extraction and loading) and **DuckDB (`DvtCache`) is the local compute layer** for cross-engine models.

```
Sling extracts → DuckDB cache (DvtCache) → model SQL in DuckDB → Sling loads → target
```

This plan focuses on optimizing the Sling-based pipeline: streaming configuration, chunked hash extraction from DuckDB, and ensuring constant-memory operation throughout.

## Branch

`extraction_enhancement` (already created from `dev`)

## Architecture Note

> The old architecture used per-database `extractors/` and `loaders/` with Spark JDBC.
> That has been replaced. Sling (`sling_client.py`) handles all data movement.
> DuckDB (`DvtCache`) handles local compute. Individual Python database drivers
> are only used for DDL operations (see `native-sql-execution-plan.md`).

---

## Phase 1: Chunked Hash Extraction from DuckDB Cache

In the new architecture, hash extraction queries run against **DuckDB** (`DvtCache`), not individual database cursors. Sling extracts source data into DuckDB first, then DVT queries DuckDB for hashes.

### `fetchall` → `fetchmany` in DuckDB hash queries

The DvtCache hash query path should use `fetchmany()` to avoid loading entire hash result sets into memory.

**File:** `core/dvt/cache/dvt_cache.py` (or equivalent DuckDB query path)

**Before:**
```python
hashes = {}
for row in cursor.fetchall():
    hashes[row[0]] = row[1]

cursor.close()
return hashes
```

**After:**
```python
hashes = {}
while True:
    batch = cursor.fetchmany(batch_size)
    if not batch:
        break
    for row in batch:
        hashes[row[0]] = row[1]

cursor.close()
return hashes
```

> **Note:** The old architecture had 20 per-database `extractors/` files each with their own
> `fetchall()` patterns. Those are removed. All data extraction is now via Sling (`sling_client.py`),
> and hash queries run in DuckDB.

---

## Phase 2: Sling Extraction Streaming Configuration

Sling (`sling_client.py`) now handles all extraction. Streaming behavior is configured via Sling's replication YAML and environment options.

### Ensure Sling streams to DuckDB cache efficiently

**File:** `core/dvt/sling/sling_client.py`

- Configure Sling's `SLING_LOADED_AT_COLUMN` and batch settings
- Ensure Sling writes to DuckDB (`DvtCache`) in streaming mode (not buffering full dataset)
- Sling natively streams — verify DVT's invocation passes through streaming-friendly options

### Sling replication config for streaming:

```yaml
source: postgres_source
target: duckdb_cache

defaults:
  mode: full-refresh
  target_options:
    batch_limit: 50000  # Rows per batch insert into DuckDB

streams:
  public.my_table:
    sql: "SELECT pk, hash_value FROM source_table"
```

> **Note:** The old plan had a custom `_extract_copy_streaming()` method in `extractors/postgres.py`.
> That is replaced by Sling's built-in streaming. Sling handles COPY-based extraction natively
> for PostgreSQL and other databases.

---

## Phase 3: DuckDB Cache Streaming Export

When DuckDB (`DvtCache`) exports model results for Sling to load into the target, ensure the export path uses streaming/chunked writes rather than materializing full result sets.

### DvtCache export to Parquet for Sling loading

**File:** `core/dvt/cache/dvt_cache.py`

```python
def export_model_result(self, model_sql: str, output_path: Path, batch_size: int = 50000):
    """Export DuckDB model result to Parquet in streaming batches.

    DuckDB executes the model SQL, then writes results in batches to Parquet.
    Sling then loads the Parquet file into the target.
    Memory: O(batch_size) instead of O(dataset).
    """
    conn = self._get_connection()
    result = conn.execute(model_sql)

    writer = None
    row_count = 0
    try:
        while True:
            batch = result.fetchmany(batch_size)
            if not batch:
                break
            # Convert to PyArrow and write
            arrow_batch = pa.record_batch(...)  # from DuckDB result
            if writer is None:
                writer = pq.ParquetWriter(str(output_path), arrow_batch.schema, compression="zstd")
            writer.write_batch(arrow_batch)
            row_count += len(batch)
    finally:
        if writer:
            writer.close()

    return row_count
```

> **Note:** The old plan had per-database streaming extractors (Databricks, etc.).
> All extraction is now via Sling. DuckDB is the only place DVT directly queries data.

---

## Phase 4: Sling Loading Configuration

Sling handles ALL loading. The old `loaders/` directory, `_SparkRowPipe`, and `toLocalIterator()` patterns are removed.

### Sling loading flow

```
DuckDB (model result) → Parquet file → Sling loads → target database
```

### Configure Sling for streaming loads

**File:** `core/dvt/sling/sling_client.py`

Ensure the Sling client passes streaming-friendly options when loading into targets:

```python
def load_to_target(self, source_path: Path, target_conn: str, target_table: str, mode: str = "full-refresh"):
    """Load data from Parquet/CSV into target via Sling.

    Sling natively streams data — no need for custom _SparkRowPipe or
    toLocalIterator() patterns. Sling handles COPY-based loading for
    PostgreSQL, bulk insert for SQL Server, etc.
    """
    replication = {
        "source": "LOCAL",
        "target": target_conn,
        "defaults": {"mode": mode},
        "streams": {
            str(source_path): {
                "object": target_table,
            }
        },
    }
    self._run_sling(replication)
```

> **Note:** The old `_SparkRowPipe` class, `_load_copy_streaming()`, `loaders/base.py` `LoadConfig`,
> and JDBC fallback chains are all removed. Sling handles streaming natively for all databases.

---

## Phase 5: Unit Tests

Create `tests/unit/test_streaming_el.py` with tests for:

1. **DvtCache `fetchmany()` loop correctness:**
   - Mock DuckDB cursor with 3 batches of data
   - Verify all hashes collected
   - Verify empty result set handled

2. **DvtCache streaming export:**
   - Mock DuckDB query result
   - Verify Parquet file written in batches
   - Verify row count matches

3. **Sling client invocation:**
   - Verify `sling_client.py` passes correct replication config
   - Verify streaming options propagated
   - Verify source/target connection mapping

---

## Phase 6: Lint + Existing Tests

```bash
cd core
hatch run unit-tests              # Verify no regressions
hatch run code-quality            # black, flake8, mypy
```

Fix any issues found.

---

## Phase 7: Commit

Commit all changes on `extraction_enhancement` branch with message:
```
feat(cache): streaming EL pipeline — eliminate RAM buffering

- Replace fetchall() with fetchmany() in DvtCache hash queries
- Add streaming DvtCache export (DuckDB → Parquet in batches)
- Configure Sling for streaming extraction and loading
- Memory: O(batch_size) instead of O(dataset) for hash extraction and model export
```

---

## Files Modified Summary

| File | Changes |
|------|---------|
| `core/dvt/cache/dvt_cache.py` | fetchmany in hash queries + streaming `export_model_result()` |
| `core/dvt/sling/sling_client.py` | Streaming-friendly Sling invocation for extract + load |
| `tests/unit/test_streaming_el.py` | New unit test file |

> **Removed from scope (old architecture):**
> - `extractors/*.py` (20 files) — replaced by Sling
> - `loaders/postgres.py` (`_SparkRowPipe`, `_load_copy_streaming`) — replaced by Sling
> - `loaders/base.py` (`LoadConfig.streaming_batch_size`) — replaced by Sling config

## Verification Checklist

- [ ] DvtCache `fetchmany` batch loop produces identical dict output to `fetchall`
- [ ] DvtCache `export_model_result()` produces valid Parquet
- [ ] Sling extraction streams data into DuckDB cache correctly
- [ ] Sling loading streams Parquet into target correctly
- [ ] End-to-end flow works: Sling extract → DuckDB cache → model SQL → Sling load → target
- [ ] `hatch run unit-tests` passes
- [ ] `hatch run code-quality` passes (black, flake8, mypy)
