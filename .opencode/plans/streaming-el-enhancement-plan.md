# Streaming EL Enhancement - Implementation Plan

## Overview

Implement Plan A (from `hesham_mind/Pipes_improvement.md`) across the `extraction_enhancement` branch. This eliminates RAM buffering in DVT's EL pipeline by replacing `fetchall()` with `fetchmany()`, adding streaming PyArrow extraction/loading, and introducing `_SparkRowPipe` for constant-memory Postgres loading.

## Branch

`extraction_enhancement` (already created from `dev`)

---

## Phase 1: `fetchall` → `fetchmany` in all 20 extractors' `extract_hashes()`

### Pattern A — Expanded loop (4 files: postgres, snowflake, bigquery, mysql)

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
    batch = cursor.fetchmany(config.batch_size)
    if not batch:
        break
    for row in batch:
        hashes[row[0]] = row[1]

cursor.close()
return hashes
```

**Files and lines:**
| File | fetchall line |
|------|--------------|
| `core/dvt/federation/extractors/postgres.py` | 219 |
| `core/dvt/federation/extractors/snowflake.py` | 216 |
| `core/dvt/federation/extractors/bigquery.py` | 261 |
| `core/dvt/federation/extractors/mysql.py` | 139 |

---

### Pattern B — Dict comprehension, no `str()` (11 files)

**Before:**
```python
hashes = {row[0]: row[1] for row in cursor.fetchall()}
cursor.close()
return hashes
```

**After:**
```python
hashes = {}
while True:
    batch = cursor.fetchmany(config.batch_size)
    if not batch:
        break
    hashes.update({row[0]: row[1] for row in batch})

cursor.close()
return hashes
```

**Files and lines:**
| File | fetchall line |
|------|--------------|
| `core/dvt/federation/extractors/databricks.py` | 277 |
| `core/dvt/federation/extractors/redshift.py` | 197 |
| `core/dvt/federation/extractors/athena.py` | 184 |
| `core/dvt/federation/extractors/clickhouse.py` | 130 |
| `core/dvt/federation/extractors/trino.py` | 102 |
| `core/dvt/federation/extractors/spark.py` | 55 |
| `core/dvt/federation/extractors/duckdb.py` | 142 |
| `core/dvt/federation/extractors/exasol.py` | 83 |
| `core/dvt/federation/extractors/vertica.py` | 83 |
| `core/dvt/federation/extractors/db2.py` | 83 |
| `core/dvt/federation/extractors/firebolt.py` | 83 |
| `core/dvt/federation/extractors/hive.py` | 86 |

---

### Pattern C — Dict comprehension with `str()` (3 files)

**sqlserver.py and oracle.py — Before:**
```python
hashes = {str(row[0]): row[1] for row in cursor.fetchall()}
cursor.close()
return hashes
```

**After:**
```python
hashes = {}
while True:
    batch = cursor.fetchmany(config.batch_size)
    if not batch:
        break
    hashes.update({str(row[0]): row[1] for row in batch})

cursor.close()
return hashes
```

**teradata.py — Before:**
```python
hashes = {str(row[0]).strip(): str(row[1]).strip() for row in cursor.fetchall()}
cursor.close()
return hashes
```

**After:**
```python
hashes = {}
while True:
    batch = cursor.fetchmany(config.batch_size)
    if not batch:
        break
    hashes.update({str(row[0]).strip(): str(row[1]).strip() for row in batch})

cursor.close()
return hashes
```

**Files and lines:**
| File | fetchall line |
|------|--------------|
| `core/dvt/federation/extractors/sqlserver.py` | 117 |
| `core/dvt/federation/extractors/oracle.py` | 106 |
| `core/dvt/federation/extractors/teradata.py` | 88 |

---

### Pattern D — Python-side hashing (1 file: generic.py)

**Before (lines 104-120):**
```python
for row in cursor.fetchall():
    row_dict = dict(zip(columns, row))

    # Build PK value
    if len(config.pk_columns) == 1:
        pk_value = str(row_dict.get(config.pk_columns[0], ""))
    else:
        pk_value = "|".join(
            str(row_dict.get(col, "")) for col in config.pk_columns
        )

    # Compute hash of all values
    values = [str(row_dict.get(col, "")) for col in sorted(columns)]
    row_str = "|".join(values)
    row_hash = hashlib.md5(row_str.encode()).hexdigest()

    hashes[pk_value] = row_hash
```

**After:**
```python
while True:
    batch = cursor.fetchmany(config.batch_size)
    if not batch:
        break
    for row in batch:
        row_dict = dict(zip(columns, row))

        # Build PK value
        if len(config.pk_columns) == 1:
            pk_value = str(row_dict.get(config.pk_columns[0], ""))
        else:
            pk_value = "|".join(
                str(row_dict.get(col, "")) for col in config.pk_columns
            )

        # Compute hash of all values
        values = [str(row_dict.get(col, "")) for col in sorted(columns)]
        row_str = "|".join(values)
        row_hash = hashlib.md5(row_str.encode()).hexdigest()

        hashes[pk_value] = row_hash
```

**File:** `core/dvt/federation/extractors/generic.py` line 104

---

## Phase 2: Streaming PostgresExtractor (`extractors/postgres.py`)

Add `_extract_copy_streaming()` alongside existing `_extract_copy()`.

### New method to add after `_extract_copy()` (after line ~174):

```python
def _extract_copy_streaming(
    self,
    config: ExtractionConfig,
    output_path: Path,
) -> ExtractionResult:
    """Extract using PostgreSQL COPY TO STDOUT with streaming PyArrow.

    Writes COPY output to a temp file (OS page cache handles buffering),
    then streams through PyArrow CSV reader in batches to Parquet.
    Memory: O(batch_size) instead of O(dataset).
    """
    start_time = time.time()

    conn = self._get_connection(config)
    query = self.build_export_query(config)
    copy_query = f"COPY ({query}) TO STDOUT WITH (FORMAT CSV, HEADER)"

    with tempfile.NamedTemporaryFile(mode="w+b", suffix=".csv") as tmp:
        # Phase A: PostgreSQL COPY -> temp file (OS page cache handles this)
        cursor = conn.cursor()
        cursor.copy_expert(copy_query, tmp)
        tmp.flush()
        tmp.seek(0)

        # Phase B: Stream-read CSV in batches -> write Parquet incrementally
        read_options = pa_csv.ReadOptions(
            block_size=config.batch_size * 512  # ~512 bytes/row estimate
        )
        streaming_reader = pa_csv.open_csv(tmp, read_options=read_options)

        writer = None
        row_count = 0
        try:
            for batch in streaming_reader:
                if writer is None:
                    writer = pq.ParquetWriter(
                        str(output_path), batch.schema, compression="zstd"
                    )
                writer.write_batch(batch)
                row_count += batch.num_rows
        finally:
            if writer:
                writer.close()

        cursor.close()

    duration = time.time() - start_time
    self._log(
        f"Streaming COPY extraction: {row_count:,} rows in {duration:.1f}s"
    )

    return ExtractionResult(
        success=True,
        row_count=row_count,
        extraction_method="copy_streaming",
        duration_seconds=duration,
    )
```

### Update `extract()` fallback chain (lines 102-118):

**Before:**
```python
def extract(self, config, output_path):
    try:
        return self._extract_copy(config, output_path)
    except Exception as e:
        self._log(f"COPY failed ({e}), falling back to Spark JDBC...")
    return self._extract_jdbc(config, output_path)
```

**After:**
```python
def extract(self, config, output_path):
    # Try streaming COPY first (constant memory)
    try:
        return self._extract_copy_streaming(config, output_path)
    except Exception as e:
        self._log(f"Streaming COPY failed ({e}), trying buffered COPY...")

    # Try buffered COPY (legacy, full dataset in memory)
    try:
        return self._extract_copy(config, output_path)
    except Exception as e:
        self._log(f"COPY failed ({e}), falling back to Spark JDBC...")

    # Fallback to Spark JDBC (parallel reads)
    return self._extract_jdbc(config, output_path)
```

### Required import addition at top of `postgres.py`:

```python
import tempfile
```

Note: `pyarrow.csv` and `pyarrow.parquet` imports should already exist. Verify and add if missing:
```python
import pyarrow.csv as pa_csv
import pyarrow.parquet as pq
```

---

## Phase 3: Streaming DatabricksExtractor (`extractors/databricks.py`)

Replace `_extract_native_cursor()` body (lines 112-176) with streaming variant.

### Replace the fetch + column-wise rebuild section (around lines 139-152):

**Before:**
```python
# Fetch all data and column names
rows = cursor.fetchall()
column_names = [desc[0] for desc in cursor.description]
cursor.close()

# ... column-wise pa.array() rebuild ...
```

**After:**
```python
column_names = [desc[0] for desc in cursor.description]

writer = None
row_count = 0

try:
    while True:
        rows = cursor.fetchmany(config.batch_size)
        if not rows:
            break

        # Build RecordBatch from chunk
        arrays = []
        for col_idx in range(len(column_names)):
            col_data = [row[col_idx] for row in rows]
            arrays.append(pa.array(col_data))
        batch = pa.record_batch(arrays, names=column_names)

        if writer is None:
            writer = pq.ParquetWriter(
                str(output_path), batch.schema, compression="zstd"
            )
        writer.write_batch(batch)
        row_count += len(rows)
finally:
    if writer:
        writer.close()
    cursor.close()
```

**Important:** Need to read the full `_extract_native_cursor()` method (lines 112-176) to see the complete structure including the ExtractionResult return and how columns are currently processed, then apply changes surgically.

---

## Phase 4: Streaming PostgresLoader + `_SparkRowPipe`

### 4a. Add `streaming_batch_size` to `LoadConfig` (`loaders/base.py`)

Add field to `LoadConfig` class at line ~37:
```python
streaming_batch_size: int = 10000  # Rows per batch for streaming load
```

### 4b. Add `_SparkRowPipe` class to `loaders/postgres.py`

Add before the `PostgresLoader` class definition:

```python
class _SparkRowPipe:
    """File-like object that streams Spark DataFrame rows as CSV.

    psycopg2's copy_expert() calls read() on this object repeatedly.
    Each read() returns a chunk of tab-separated rows.
    Uses toLocalIterator() so only one partition is in driver memory at a time.
    """

    def __init__(self, df, columns, batch_rows=10000):
        self.iterator = df.toLocalIterator()
        self.columns = columns
        self.batch_rows = batch_rows
        self.rows_written = 0
        self._buffer = b""
        self._exhausted = False

    def read(self, size=-1):
        """Read up to size bytes of TSV data."""
        if self._exhausted and not self._buffer:
            return b""

        # Fill buffer until we have enough or iterator exhausted
        while len(self._buffer) < size and not self._exhausted:
            try:
                row = next(self.iterator)
                values = []
                for v in row:
                    if v is None:
                        values.append("\\N")
                    else:
                        str_val = str(v)
                        str_val = str_val.replace("\\", "\\\\")
                        str_val = str_val.replace("\t", "\\t")
                        str_val = str_val.replace("\n", "\\n")
                        str_val = str_val.replace("\r", "\\r")
                        values.append(str_val)
                self._buffer += ("\t".join(values) + "\n").encode("utf-8")
                self.rows_written += 1
            except StopIteration:
                self._exhausted = True
                break

        if size > 0:
            result = self._buffer[:size]
            self._buffer = self._buffer[size:]
        else:
            result = self._buffer
            self._buffer = b""
        return result
```

### 4c. Add `_load_copy_streaming()` to PostgresLoader

Add after existing `_load_copy()` method:

```python
def _load_copy_streaming(
    self,
    df: Any,
    config: LoadConfig,
    adapter: Optional[Any] = None,
) -> LoadResult:
    """Load DataFrame to PostgreSQL using streaming COPY FROM.

    Uses toLocalIterator() + _SparkRowPipe for constant-memory loading.
    Memory: O(partition_size) instead of O(dataset).
    """
    start_time = time.time()

    conn = self._get_pg_connection(config)

    try:
        # DDL: create/truncate table (same as _load_copy)
        if config.full_refresh or config.mode == "overwrite":
            if adapter:
                self._execute_ddl(config, adapter, conn)

        # Check row count to decide streaming vs buffered
        # toLocalIterator() has per-partition overhead; not worth it for small data
        try:
            row_count_estimate = df.rdd.countApprox(timeout=5000, confidence=0.5)
        except Exception:
            row_count_estimate = 100000  # Assume large if can't estimate

        if row_count_estimate < 50000:
            self._log("Small dataset, falling back to buffered COPY")
            raise ValueError("Small dataset — use buffered path")

        # Stream via _SparkRowPipe
        columns = df.columns
        pipe = _SparkRowPipe(df, columns, batch_rows=config.streaming_batch_size)

        cursor = conn.cursor()
        copy_sql = f"COPY {config.table_name} FROM STDIN WITH (FORMAT TEXT)"
        cursor.copy_expert(copy_sql, pipe)
        conn.commit()

        row_count = pipe.rows_written
        cursor.close()

    except Exception:
        conn.rollback()
        raise

    duration = time.time() - start_time
    self._log(
        f"Streaming COPY load: {row_count:,} rows in {duration:.1f}s"
    )

    return LoadResult(
        success=True,
        row_count=row_count,
        load_method="copy_streaming",
        duration_seconds=duration,
    )
```

### 4d. Update `load()` fallback chain in PostgresLoader

**Before:**
```python
def load(self, df, config, adapter=None):
    try:
        return self._load_copy(df, config, adapter)
    except Exception as e:
        self._log(f"COPY failed ({e}), falling back to JDBC...")
        return self._load_jdbc(df, config, adapter)
```

**After:**
```python
def load(self, df, config, adapter=None):
    # Try streaming COPY first (constant memory)
    try:
        return self._load_copy_streaming(df, config, adapter)
    except Exception as e:
        self._log(f"Streaming COPY failed ({e}), trying buffered COPY...")

    # Try buffered COPY (legacy, full dataset in memory)
    try:
        return self._load_copy(df, config, adapter)
    except Exception as e:
        self._log(f"COPY failed ({e}), falling back to JDBC...")

    # Fallback to JDBC
    return self._load_jdbc(df, config, adapter)
```

---

## Phase 5: Unit Tests

Create `core/tests/unit/test_streaming_el.py` with tests for:

1. **`_SparkRowPipe.read()` correctness:**
   - NULL handling (`\\N` output)
   - Tab/newline/backslash escaping
   - Multiple `read()` calls with varying `size` values
   - Empty DataFrame
   - Unicode data

2. **Streaming extractor with mock cursor:**
   - Mock `cursor.fetchmany()` returning batches then empty
   - Verify ParquetWriter receives correct batches
   - Verify row count matches

3. **`fetchmany()` loop correctness:**
   - Mock cursor with 3 batches of data
   - Verify all hashes collected
   - Verify `str()` wrapping preserved for Pattern C
   - Verify `.strip()` preserved for teradata pattern

4. **Fallback chain:**
   - Streaming raises → buffered called
   - Both raise → JDBC called

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
feat(federation): streaming EL pipeline — eliminate RAM buffering

- Replace fetchall() with fetchmany() in all 20 extractors' extract_hashes()
- Add streaming PostgresExtractor using tempfile + PyArrow CSV streaming
- Add streaming DatabricksExtractor using fetchmany + PyArrow ParquetWriter
- Add streaming PostgresLoader with _SparkRowPipe + toLocalIterator()
- Add streaming_batch_size to LoadConfig
- Memory: O(batch_size) instead of O(dataset) for extraction and loading
```

---

## Files Modified Summary

| File | Changes |
|------|---------|
| `extractors/postgres.py` | fetchmany in extract_hashes + `_extract_copy_streaming()` + updated `extract()` fallback + `import tempfile` |
| `extractors/databricks.py` | fetchmany in extract_hashes + streaming `_extract_native_cursor()` |
| `extractors/snowflake.py` | fetchmany in extract_hashes |
| `extractors/bigquery.py` | fetchmany in extract_hashes |
| `extractors/redshift.py` | fetchmany in extract_hashes |
| `extractors/athena.py` | fetchmany in extract_hashes |
| `extractors/mysql.py` | fetchmany in extract_hashes |
| `extractors/sqlserver.py` | fetchmany in extract_hashes |
| `extractors/oracle.py` | fetchmany in extract_hashes |
| `extractors/clickhouse.py` | fetchmany in extract_hashes |
| `extractors/trino.py` | fetchmany in extract_hashes |
| `extractors/spark.py` | fetchmany in extract_hashes |
| `extractors/duckdb.py` | fetchmany in extract_hashes |
| `extractors/exasol.py` | fetchmany in extract_hashes |
| `extractors/vertica.py` | fetchmany in extract_hashes |
| `extractors/db2.py` | fetchmany in extract_hashes |
| `extractors/firebolt.py` | fetchmany in extract_hashes |
| `extractors/hive.py` | fetchmany in extract_hashes |
| `extractors/teradata.py` | fetchmany in extract_hashes |
| `extractors/generic.py` | fetchmany in extract_hashes |
| `loaders/postgres.py` | `_SparkRowPipe` class + `_load_copy_streaming()` + updated `load()` fallback |
| `loaders/base.py` | `streaming_batch_size` field on `LoadConfig` |
| `tests/unit/test_streaming_el.py` | New unit test file |

## Verification Checklist

- [ ] All 20 extractors compile (no syntax errors)
- [ ] `fetchmany` batch loop produces identical dict output to `fetchall` for all 4 patterns
- [ ] `_extract_copy_streaming()` produces valid Parquet readable by `spark.read.parquet()`
- [ ] `_SparkRowPipe` correctly escapes all special characters
- [ ] Fallback chains work: streaming → buffered → JDBC
- [ ] `hatch run unit-tests` passes
- [ ] `hatch run code-quality` passes (black, flake8, mypy)
