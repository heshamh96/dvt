# Streaming EL: Eliminate RAM Buffering via OS Storage Cache

## Context

DVT's EL pipeline currently buffers **entire datasets in Python RAM** during extraction and loading. For a 10GB table, the PostgresExtractor holds ~20GB in memory (CSV StringIO + PyArrow table), and the PostgresLoader holds another ~20GB (Spark df.collect() + CSV StringIO). Additionally, **every single extractor** (all 20+) uses `cursor.fetchall()` in `extract_hashes()`, loading entire hash result sets into memory.

This plan applies the Linux pipe/streaming concept: constant-memory processing where data flows through the OS page cache, never fully materializing in application RAM. Scope is **all native extractors** — not just PostgreSQL.

## The Core Idea: Linux Pipes Applied to EL

Unix pipes are a streaming framework with built-in backpressure, constant memory usage, and zero network overhead. The OS page cache handles all the smart buffering transparently:

| Layer | What happens |
|-------|-------------|
| **Application** | Reads/writes to pipe — sees ~64KB buffer |
| **Kernel pipe buffer** | Connects producer <-> consumer with backpressure |
| **Page cache** | OS transparently caches disk reads/writes in free RAM |
| **Storage** | Actual I/O happens asynchronously, in large sequential batches |

We apply this to DVT by:
1. Never holding entire datasets in Python RAM (StringIO, BytesIO, df.collect())
2. Using temp files + OS page cache instead of in-memory buffers
3. Using PyArrow streaming readers/writers that process data in batches
4. Using `cursor.fetchmany()` instead of `cursor.fetchall()` for DB cursors
5. Using `df.toLocalIterator()` instead of `df.collect()` for Spark DataFrames

---

## Memory Bottlenecks Audit

### Critical (2x dataset in RAM):

| Extractor/Loader | File | Method | Problem |
|------------------|------|--------|---------|
| PostgresExtractor | `extractors/postgres.py:139-153` | `_extract_copy()` | `StringIO` + `pa_csv.read_csv(buffer.getvalue().encode())` |
| DatabricksExtractor | `extractors/databricks.py:140-152` | `_extract_native_cursor()` | `cursor.fetchall()` + column-wise `pa.array()` rebuild |
| PostgresLoader | `loaders/postgres.py:146-176` | `_load_copy()` | `df.collect()` + `StringIO` CSV buffer |

### Universal (all extractors use fetchall in extract_hashes):

| Extractor | File:Line | `fetchall()` call |
|-----------|-----------|-------------------|
| PostgresExtractor | `postgres.py:219` | `cursor.fetchall()` |
| DatabricksExtractor | `databricks.py:277` | `cursor.fetchall()` |
| SnowflakeExtractor | `snowflake.py:216` | `cursor.fetchall()` |
| BigQueryExtractor | `bigquery.py:261` | `cursor.fetchall()` |
| RedshiftExtractor | `redshift.py:197` | `cursor.fetchall()` |
| AthenaExtractor | `athena.py:184` | `cursor.fetchall()` |
| MySQLExtractor | `mysql.py:139` | `cursor.fetchall()` |
| SQLServerExtractor | `sqlserver.py:117` | `cursor.fetchall()` |
| OracleExtractor | `oracle.py:106` | `cursor.fetchall()` |
| ClickHouseExtractor | `clickhouse.py:130` | `cursor.fetchall()` |
| TrinoExtractor | `trino.py:102` | `cursor.fetchall()` |
| SparkExtractor | `spark.py:55` | `cursor.fetchall()` |
| DuckDBExtractor | `duckdb.py:142` | `cursor.fetchall()` |
| ExasolExtractor | `exasol.py:83` | `cursor.fetchall()` |
| VerticaExtractor | `vertica.py:83` | `cursor.fetchall()` |
| DB2Extractor | `db2.py:83` | `cursor.fetchall()` |
| FireboltExtractor | `firebolt.py:83` | `cursor.fetchall()` |
| HiveExtractor | `hive.py:86` | `cursor.fetchall()` |
| TeradataExtractor | `teradata.py:88` | `cursor.fetchall()` |
| GenericExtractor | `generic.py:104` | `cursor.fetchall()` |

### Already Efficient (no fix needed for main extract path):

- **Snowflake, BigQuery, Redshift, Athena** — stream directly to cloud storage via SQL commands (COPY INTO / EXPORT DATA / UNLOAD). No Python RAM involved.
- **DuckDB** — `COPY TO` writes Parquet directly to disk. No Python RAM.
- **All JDBC-only extractors** — Spark handles streaming natively via partitioned reads.
- **Cloud loaders (Snowflake, BigQuery, Redshift, Databricks)** — stage Parquet to cloud -> COPY INTO. The `df.count()` is just for reporting.

---

## Plan

### Step 1: Streaming PostgresExtractor (`extractors/postgres.py`)

Add `_extract_copy_streaming()` method alongside existing `_extract_copy()`:

- **Write phase**: `cursor.copy_expert(COPY TO STDOUT)` writes CSV to a `tempfile.NamedTemporaryFile` (binary mode). The OS page cache handles this — no Python RAM needed for the full dataset.
- **Read phase**: `pyarrow.csv.open_csv(tmp)` returns a streaming `CSVStreamingReader` that yields `RecordBatch` objects. Each batch feeds into `pyarrow.parquet.ParquetWriter` incrementally.
- **Memory**: O(batch_size) instead of O(dataset). Uses `ExtractionConfig.batch_size` (already exists at `base.py:30`, currently unused by COPY path).
- **Fallback chain**: `extract()` tries streaming -> buffered -> JDBC.

**Key design decision**: `copy_expert()` is synchronous (blocks until COPY finishes), so a temp file is simpler than threading with `os.pipe()`. The sequential-write-then-sequential-read access pattern is exactly what the OS page cache is optimized for. The temp file is never fully resident in RAM — the kernel pages it in and out as needed.

**Pseudocode**:
```python
def _extract_copy_streaming(self, config, output_path):
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
                    writer = pq.ParquetWriter(output_path, batch.schema, compression="zstd")
                writer.write_batch(batch)
                row_count += batch.num_rows
        finally:
            if writer:
                writer.close()

    return ExtractionResult(success=True, row_count=row_count, ...)
```

### Step 2: Streaming DatabricksExtractor (`extractors/databricks.py`)

Replace `_extract_native_cursor()` (lines 112-176) with a streaming variant:

- Replace `cursor.fetchall()` with `cursor.fetchmany(batch_size)` loop
- Build PyArrow `RecordBatch` per chunk instead of full table
- Use `pyarrow.parquet.ParquetWriter` to write batches incrementally
- **Memory**: O(batch_size) instead of O(dataset)
- Preserves the existing native cloud COPY INTO path (`_extract_native_parallel`) unchanged since it already streams to cloud storage

**Pseudocode**:
```python
def _extract_native_cursor_streaming(self, config, output_path):
    conn = self._get_connection(config)
    query = self.build_export_query(config)
    cursor = conn.cursor()
    cursor.execute(query)

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
                writer = pq.ParquetWriter(output_path, batch.schema, compression="zstd")
            writer.write_batch(batch)
            row_count += len(rows)
    finally:
        if writer:
            writer.close()
        cursor.close()

    return ExtractionResult(success=True, row_count=row_count, ...)
```

### Step 3: Fix `extract_hashes()` fetchall -> fetchmany (ALL extractors)

Universal fix across **all 20 extractors**. The pattern is identical everywhere:

```python
# Before (every extractor):
hashes = {row[0]: row[1] for row in cursor.fetchall()}

# After:
hashes = {}
while True:
    batch = cursor.fetchmany(config.batch_size if config else 100000)
    if not batch:
        break
    for row in batch:
        hashes[row[0]] = row[1]
```

Peak memory drops from 2x (fetchall list + dict) to 1x+epsilon (dict + small batch list).

**Files to modify** (mechanical change, same pattern in each):
- `extractors/postgres.py` — line 219
- `extractors/databricks.py` — line 277
- `extractors/snowflake.py` — line 216
- `extractors/bigquery.py` — line 261
- `extractors/redshift.py` — line 197
- `extractors/athena.py` — line 184
- `extractors/mysql.py` — line 139
- `extractors/sqlserver.py` — line 117
- `extractors/oracle.py` — line 106
- `extractors/clickhouse.py` — line 130
- `extractors/trino.py` — line 102
- `extractors/spark.py` — line 55
- `extractors/duckdb.py` — line 142
- `extractors/exasol.py` — line 83
- `extractors/vertica.py` — line 83
- `extractors/db2.py` — line 83
- `extractors/firebolt.py` — line 83
- `extractors/hive.py` — line 86
- `extractors/teradata.py` — line 88
- `extractors/generic.py` — line 104

### Step 4: Streaming PostgresLoader (`loaders/postgres.py`)

Add `_load_copy_streaming()` method alongside existing `_load_copy()`:

- Replace `df.collect()` with `df.toLocalIterator()` — processes one Spark partition at a time instead of materializing the entire DataFrame
- Introduce `_SparkRowPipe` — a file-like object implementing `read(size)` that generates CSV on-the-fly from the Spark row iterator. psycopg2's `copy_expert(COPY FROM STDIN, pipe)` calls `read()` in a loop
- **Memory**: O(partition_size) instead of O(dataset)
- **Performance guard**: For small datasets (< 50k rows), fall back to `collect()` since `toLocalIterator()` has per-partition scheduling overhead
- **Fallback chain**: `load()` tries streaming -> buffered -> JDBC

**`_SparkRowPipe` design**:
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
        self._buffer = ""
        self._exhausted = False

    def read(self, size=-1):
        """Read up to size bytes of CSV data."""
        if self._exhausted and not self._buffer:
            return b""

        # Fill buffer until we have enough or iterator exhausted
        while len(self._buffer.encode("utf-8")) < size and not self._exhausted:
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
                self._buffer += "\t".join(values) + "\n"
                self.rows_written += 1
            except StopIteration:
                self._exhausted = True
                break

        encoded = self._buffer.encode("utf-8")
        if size > 0:
            result = encoded[:size]
            self._buffer = encoded[size:].decode("utf-8")
        else:
            result = encoded
            self._buffer = ""
        return result
```

**Why `toLocalIterator()` instead of `collect()`**: `toLocalIterator()` computes one partition at a time and yields rows lazily. Memory is O(partition_size) instead of O(dataset). For a 10GB table with 100 partitions, this uses ~100MB instead of 10GB.

### Step 5: Configuration

- Add `streaming_batch_size: int = 10000` field to `LoadConfig` in `loaders/base.py`
- `ExtractionConfig.batch_size` (already exists at `base.py:30`) now drives extraction batch sizes
- No `computes.yml` changes needed — existing `batch_size` in `ExtractionConfig` and new `streaming_batch_size` in `LoadConfig` are sufficient
- Fully backward compatible — defaults match current behavior

---

## Files to Modify

### Streaming extraction + loading (Steps 1, 2, 4):

| File | Changes |
|------|---------|
| `core/dvt/federation/extractors/postgres.py` | Add `_extract_copy_streaming()`, update `extract()` fallback chain |
| `core/dvt/federation/extractors/databricks.py` | Replace `_extract_native_cursor()` with streaming `fetchmany` + ParquetWriter |
| `core/dvt/federation/loaders/postgres.py` | Add `_load_copy_streaming()`, `_SparkRowPipe` class, update `load()` fallback chain |
| `core/dvt/federation/loaders/base.py` | Add `streaming_batch_size` to `LoadConfig` |

### fetchmany fix (Step 3) — 20 extractor files:

| File | Change |
|------|--------|
| `core/dvt/federation/extractors/postgres.py` | `extract_hashes()` fetchall -> fetchmany loop |
| `core/dvt/federation/extractors/databricks.py` | `extract_hashes()` fetchall -> fetchmany loop |
| `core/dvt/federation/extractors/snowflake.py` | `extract_hashes()` fetchall -> fetchmany loop |
| `core/dvt/federation/extractors/bigquery.py` | `extract_hashes()` fetchall -> fetchmany loop |
| `core/dvt/federation/extractors/redshift.py` | `extract_hashes()` fetchall -> fetchmany loop |
| `core/dvt/federation/extractors/athena.py` | `extract_hashes()` fetchall -> fetchmany loop |
| `core/dvt/federation/extractors/mysql.py` | `extract_hashes()` fetchall -> fetchmany loop |
| `core/dvt/federation/extractors/sqlserver.py` | `extract_hashes()` fetchall -> fetchmany loop |
| `core/dvt/federation/extractors/oracle.py` | `extract_hashes()` fetchall -> fetchmany loop |
| `core/dvt/federation/extractors/clickhouse.py` | `extract_hashes()` fetchall -> fetchmany loop |
| `core/dvt/federation/extractors/trino.py` | `extract_hashes()` fetchall -> fetchmany loop |
| `core/dvt/federation/extractors/spark.py` | `extract_hashes()` fetchall -> fetchmany loop |
| `core/dvt/federation/extractors/duckdb.py` | `extract_hashes()` fetchall -> fetchmany loop |
| `core/dvt/federation/extractors/exasol.py` | `extract_hashes()` fetchall -> fetchmany loop |
| `core/dvt/federation/extractors/vertica.py` | `extract_hashes()` fetchall -> fetchmany loop |
| `core/dvt/federation/extractors/db2.py` | `extract_hashes()` fetchall -> fetchmany loop |
| `core/dvt/federation/extractors/firebolt.py` | `extract_hashes()` fetchall -> fetchmany loop |
| `core/dvt/federation/extractors/hive.py` | `extract_hashes()` fetchall -> fetchmany loop |
| `core/dvt/federation/extractors/teradata.py` | `extract_hashes()` fetchall -> fetchmany loop |
| `core/dvt/federation/extractors/generic.py` | `extract_hashes()` fetchall -> fetchmany loop |

## Files NOT Modified

- `BaseExtractor` / `BaseLoader` ABCs — no new abstract methods
- Extractor/Loader registries — no new classes
- `ELLayer` / `FederationEngine` — call signatures unchanged
- Cloud extractors' main `extract()` paths (Snowflake, BigQuery, Redshift, Athena, DuckDB) — already stream to storage
- Cloud loaders' main `load()` paths (Snowflake, BigQuery, Redshift, Databricks) — already stage via cloud

---

## Implementation Order

1. **Step 3** (fetchmany across all 20 extractors) — widest impact, lowest risk, mechanical change
2. **Step 1** (Streaming PostgresExtractor) — highest single-extractor impact
3. **Step 2** (Streaming DatabricksExtractor) — second-highest single-extractor impact
4. **Step 4 + 5** (Streaming PostgresLoader + config) — completes the pipeline

## Verification

1. **Unit tests** (`tests/unit/test_streaming_el.py`):
   - `_SparkRowPipe.read()` correctness (CSV generation, NULL handling, escaping)
   - Streaming extractor with mock psycopg2 cursor
   - Streaming loader with mock Spark DataFrame
   - `fetchmany()` loop correctness for multiple extractors
   - Fallback chain (streaming -> buffered -> JDBC)

2. **Functional tests** (`tests/functional/test_streaming_postgres.py`, requires `hatch run setup-db`):
   - Round-trip: create Postgres table -> extract streaming -> load streaming -> verify match
   - Memory profile: extract 100MB+ table, verify Python RSS stays under 200MB

3. **Code quality**: `cd core && hatch run code-quality` (black, flake8, mypy)

---

## Future Enhancement: Direct Pipe Mode (deferred)

For simple Postgres->Postgres EL with **no Spark transformation**, skip Parquet staging and Spark entirely:

```
Source COPY TO STDOUT -> os.pipe() -> Target COPY FROM STDIN
```

- Two threads: writer thread runs `copy_expert(COPY TO, write_fd)`, reader thread runs `copy_expert(COPY FROM, read_fd)`
- Memory: O(pipe_buffer) = ~64KB on Linux, ~16KB on macOS
- Each thread creates its own `psycopg2.connect()` (connections are not thread-safe)
- Opt-in via config flag
- Requires both source and target to be PostgreSQL-compatible (7 adapter types)

This is deferred to a follow-up iteration after Steps 1-5 are stable.
