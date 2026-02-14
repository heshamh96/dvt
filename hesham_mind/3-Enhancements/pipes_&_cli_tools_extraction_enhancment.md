# Enhancement Plan: Pipe-Based EL with CLI Tools

## Overview

Replace Spark-based extraction/loading for on-prem databases with Linux pipe streaming. Memory drops from GBs to ~64KB kernel buffer + ~1-10MB PyArrow batch buffer. Cloud databases keep their existing bucket-based path. JDBC remains as universal fallback.

**Key design decision:** PyArrow streaming (already a DVT dependency) serves as the CSV-to-Parquet converter, eliminating the need for DuckDB CLI as an external dependency. The converter runs in-process — no extra binary required.

---

## Problem Statement

### Current Extraction (Source -> Staging Parquet)

The current extraction path uses Spark JDBC or PyArrow in-memory buffering:

- **Spark JDBC**: `spark.read.jdbc()` -> `df.write.parquet()` -- requires 4-8 GB JVM heap
- **PostgreSQL COPY**: `COPY TO STDOUT` -> `StringIO` buffer -> `PyArrow read_csv()` -> `pq.write_table()` -- holds 2x data in Python memory (CSV string + Arrow table)

### Current Loading (Spark Result -> Target DB)

- **PostgreSQL COPY FROM**: `df.collect()` pulls ENTIRE result to Spark driver JVM, then serializes to Python objects, then builds TSV in StringIO, then streams to psycopg2. Peak memory: ~3x result size.
- **Spark JDBC**: `df.repartition(N).write.jdbc()` -- triggers full data shuffle, plus redundant `df.count()` after write.
- **Cloud bulk load** (Snowflake/BQ/Redshift/Databricks): Already efficient -- Spark writes Parquet to cloud storage, target DB reads server-side. No change needed.

### The Linux Pipe Alternative

A Unix pipe is a kernel-managed buffer (~64KB on Linux). It has built-in backpressure: the writer blocks when the buffer is full, the reader blocks when it's empty. This means:

- Memory usage is constant regardless of data size (gigabytes, terabytes -- doesn't matter)
- No intermediate files in the pipe segment, no RAM bloat
- The OS page cache (storage cache) transparently handles disk I/O buffering
- Zero-copy data movement between processes

---

## 3-Tier Execution Model

```
Tier 1 -- PIPE (on-prem, CLI tool installed):
  Source DB CLI --> | (64KB kernel buffer) --> PyArrow streaming --> staging.parquet
  staging.parquet --> PyArrow streaming --> | (64KB kernel buffer) --> Target DB CLI

Tier 2 -- CLOUD BUCKET (cloud DW, unchanged):
  Spark --> S3/GCS/Azure --> COPY INTO target

Tier 3 -- JDBC FALLBACK (no CLI, no bucket):
  Spark JDBC read/write (current behavior, unchanged)
```

### Tier Selection at Runtime

```python
# Extraction
if cli_tool_available:
    _extract_pipe()       # Tier 1: ~64KB pipe + ~1-10MB PyArrow batch
elif bucket_configured:
    _extract_bulk()       # Tier 2: cloud-native export
else:
    _extract_jdbc()       # Tier 3: Spark JDBC

# Loading
if cli_tool_available:
    _load_pipe()          # Tier 1: ~64KB pipe + ~1-10MB PyArrow batch
elif bucket_configured and supports_bulk_load:
    _load_bulk()          # Tier 2: cloud COPY INTO
else:
    _load_jdbc()          # Tier 3: Spark JDBC
```

### Memory Comparison

| Phase | Current | Pipe-Based |
|-------|:-------:|:----------:|
| Extract (Postgres, 2GB table) | ~4 GB (StringIO + PyArrow full load) | ~1-10 MB (pipe + PyArrow batch) |
| Extract (Spark JDBC, 2GB table) | ~4-8 GB (JVM heap) | ~1-10 MB (pipe + PyArrow batch) |
| Load (Postgres, 2GB result) | ~6 GB (collect + objects + StringIO) | ~1-10 MB (pipe + PyArrow batch) |
| Load (Spark JDBC, 2GB result) | ~4 GB (shuffle + batch INSERT) | ~1-10 MB (pipe + PyArrow batch) |
| Cloud bulk load (any size) | Low (server-side) | Same (unchanged) |

---

## CSV-to-Parquet Conversion: PyArrow Streaming

PyArrow (already a DVT dependency) has a **streaming CSV reader** that reads in bounded batches without loading the full dataset into memory. This replaces the need for any external converter binary.

### How It Works

PyArrow's `open_csv()` returns an iterator of `RecordBatch` objects. Each batch is a small chunk (~1MB default). The `ParquetWriter` writes each batch incrementally to disk and releases it. Memory never exceeds one batch.

```python
import pyarrow.csv as pa_csv
import pyarrow.parquet as pq
import subprocess

# Spawn the DB CLI tool as a subprocess
proc = subprocess.Popen(
    ["psql", "-h", host, "-U", user, "-d", db,
     "-c", "COPY (SELECT ...) TO STDOUT WITH (FORMAT csv, HEADER)"],
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
)

# PyArrow reads CSV from the subprocess stdout in streaming batches
reader = pa_csv.open_csv(proc.stdout)
writer = pq.ParquetWriter(output_path, reader.schema, compression="zstd")

for batch in reader:              # ~1MB per batch, bounded memory
    writer.write_batch(batch)     # flush to Parquet on disk immediately

writer.close()
proc.wait()
```

### Memory Profile

```
psql process:                stdout --> kernel pipe buffer (64KB)
                                            |
Python process:              proc.stdout --> PyArrow open_csv() --> RecordBatch (~1MB)
                                                                        |
                                                                   ParquetWriter --> disk
                                                                        |
                                                                   batch released
                                                                        |
                                                                   next batch...
```

**Peak memory: ~1-10 MB** (one PyArrow RecordBatch in flight + Parquet write buffers). The 64KB kernel pipe buffer provides backpressure between psql and Python.

### Why PyArrow Streaming (Not DuckDB CLI)?

| Aspect | PyArrow Streaming | DuckDB CLI |
|--------|:-----------------:|:----------:|
| **Extra dependency** | None (already installed) | Separate binary (needs user install) |
| **Parquet compatibility** | Identical to current code (same library) | Standard but different writer |
| **Error handling** | Python exceptions (native integration) | Subprocess exit codes |
| **Integration** | In-process, direct access to subprocess pipe | Shell pipe composition |
| **Speed** | Fast (C++ engine) | Fast (C++ engine) |
| **Memory** | ~1-10 MB (batch size) | ~1-10 MB (internal buffers) |
| **Type inference** | Same as current extraction code | DuckDB auto-detect (may differ) |

PyArrow wins because it eliminates an external dependency while providing identical Parquet output to the current code.

### Loading Flow (Parquet -> CSV -> Target DB)

For loading, PyArrow reads the staged Parquet file in batches and streams CSV to the target DB's stdin:

```python
import pyarrow.parquet as pq
import pyarrow.csv as pa_csv
import subprocess

# Spawn the target DB CLI tool
proc = subprocess.Popen(
    ["psql", "-h", host, "-U", user, "-d", db,
     "-c", "COPY schema.table FROM STDIN WITH (FORMAT csv, HEADER)"],
    stdin=subprocess.PIPE,
    stderr=subprocess.PIPE,
)

# Read Parquet in batches, write CSV to subprocess stdin
parquet_file = pq.ParquetFile(result_parquet_path)
first_batch = True

for batch in parquet_file.iter_batches(batch_size=65536):
    # Convert Arrow batch to CSV bytes
    sink = pa.BufferOutputStream()
    write_options = pa_csv.WriteOptions(include_header=first_batch)
    pa_csv.write_csv(pa.Table.from_batches([batch]), sink, write_options=write_options)
    proc.stdin.write(sink.getvalue().to_pybytes())
    first_batch = False

proc.stdin.close()
proc.wait()
```

**Memory:** One Parquet row group read at a time (~1-10MB), converted to CSV bytes, streamed to the pipe. The kernel pipe buffer (64KB) provides backpressure if the target DB ingests slower than we read.

---

## Parquet Compatibility with StateManager

**No changes needed to StateManager or delta detection logic.**

Analysis of how StateManager interacts with staging Parquet files:

| StateManager Operation | What It Checks | Parquet Dependency |
|------------------------|---------------|-------------------|
| `staging_exists()` | `Path.exists()` on the Parquet file | File existence only -- never reads contents |
| `compute_schema_hash()` | Column names + types from `extractor.get_columns()` | Source DB metadata -- not Parquet schema |
| `should_extract()` | File existence + JSON state hash match | Never inspects Parquet internals |
| `save_row_hashes()` | Writes `_state/*.hashes.parquet` via PyArrow | Separate file -- unaffected by extraction method |
| `get_stored_hashes()` | Reads `_state/*.hashes.parquet` via PyArrow | Separate file -- unaffected by extraction method |
| `get_changed_pks()` | Compares stored hashes vs fresh DB query | Source DB query -- not Parquet |

**Key facts:**
- Schema hash is derived from source database metadata, not Parquet file schema
- Row-level hashing runs as SQL queries against the source DB
- Hash storage Parquet files are written/read by StateManager's own PyArrow code
- The staging Parquet file's only content consumer is `spark.read.parquet()` in the transform step, which reads any valid Parquet (PyArrow-produced, Spark-produced, etc.)
- PyArrow streaming produces the **exact same Parquet format** as the current `pq.write_table()` calls -- same library, same writer, same defaults

**Parquet format constraints (all satisfied by PyArrow streaming):**

| Property | Requirement | PyArrow Streaming Output |
|----------|-------------|--------------------------|
| Location | `{bucket_path}/{source_name}.parquet` | Controlled by DVT code, not the writer |
| Format | Standard Parquet | Yes (identical to current output) |
| Compression | Any standard codec | zstd (explicitly set) |
| Single file | Preferred (not directory) | Yes (single file via `ParquetWriter`) |
| Column names | Must match source table | Ensured by `SELECT *` in extraction query |
| Readable by Spark | `spark.read.parquet()` | Yes (same as current PyArrow output) |

---

## `dvt sync` Step 9: CLI Tool Detection

### Behavior

- Parse `profiles.yml` to determine which adapter types are in use
- For each on-prem adapter, check if the corresponding CLI tool is on PATH
- Print status table with found/missing tools
- For missing tools, print platform-specific install instructions
- **Never auto-install** -- notify user to install and add to PATH themselves

### Example Output

```
Step 9: Checking CLI tools for pipe-optimized data transfer

  Adapter          CLI Tool              Status
  ──────────────────────────────────────────────────
  postgres         psql                  found (/usr/local/bin/psql v16.1)
  mysql            mysql                 not found
  clickhouse       clickhouse-client     found (/usr/bin/clickhouse-client v24.1)

  Missing CLI tools for pipe-optimized transfer:

    mysql:
      macOS:   brew install mysql-client
      Linux:   sudo apt-get install mysql-client
      Docs:    https://dev.mysql.com/doc/refman/en/mysql.html

  Without these tools, DVT uses JDBC fallback (higher memory usage).
  PyArrow (built-in) handles CSV-to-Parquet conversion -- no extra tools needed.
```

### Tool Registry

| Adapter Type | CLI Tool | Version Check | macOS Install | Linux Install |
|-------------|----------|---------------|---------------|---------------|
| postgres (+6 compat) | `psql` | `psql --version` | `brew install libpq` | `apt install postgresql-client` |
| mysql (+5 compat) | `mysql` | `mysql --version` | `brew install mysql-client` | `apt install mysql-client` |
| sqlserver (+2 compat) | `bcp` | `bcp -v` | `brew install mssql-tools18` | MS docs link |
| clickhouse | `clickhouse-client` | `clickhouse-client --version` | `brew install clickhouse` | ClickHouse docs link |
| oracle | `sqlplus` | `sqlplus -v` | Oracle Instant Client link | Oracle Instant Client link |

**Compatible adapter families:**
- `postgres`: also covers greenplum, cockroachdb, alloydb, materialize, citus, timescaledb, neon
- `mysql`: also covers mariadb, tidb, singlestore, planetscale, vitess, aurora_mysql
- `sqlserver`: also covers synapse, fabric

**No converter binary needed.** PyArrow (already a DVT dependency) handles all CSV-to-Parquet and Parquet-to-CSV conversion in-process.

**Cloud adapters (snowflake, bigquery, redshift, databricks) are skipped** -- their CLI tools don't support pipe-based data movement. They use Tier 2 (cloud bucket) which is already efficient.

---

## Extractor Changes

### New Method: `_extract_pipe()`

Each on-prem extractor gains a `_extract_pipe()` method. The existing `extract()` method is updated with runtime detection:

```python
def extract(self, config, output_path):
    if shutil.which(self.cli_tool):
        return self._extract_pipe(config, output_path)     # Tier 1
    else:
        return self._extract_native(config, output_path)    # Existing path
```

### Core Pipe Extraction Pattern (shared in base.py)

```python
def _extract_pipe(self, config, output_path):
    """Extract data via CLI tool pipe + PyArrow streaming CSV-to-Parquet."""
    # Step 1: Build the CLI command with connection args + extraction SQL
    cmd = self._build_extraction_command(config)
    
    # Step 2: Spawn CLI tool as subprocess
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=self._build_env(config),  # PGPASSWORD, MYSQL_PWD, etc.
    )
    
    # Step 3: PyArrow reads CSV stream from subprocess stdout
    try:
        read_options = pa_csv.ReadOptions(block_size=1 << 20)  # 1MB blocks
        reader = pa_csv.open_csv(proc.stdout, read_options=read_options)
        writer = pq.ParquetWriter(str(output_path), reader.schema, compression="zstd")
        
        row_count = 0
        for batch in reader:
            writer.write_batch(batch)
            row_count += len(batch)
        
        writer.close()
    except Exception as e:
        proc.kill()
        raise
    
    # Step 4: Check subprocess exit
    proc.wait()
    if proc.returncode != 0:
        stderr = proc.stderr.read().decode()
        raise RuntimeError(f"CLI extraction failed: {stderr}")
    
    return ExtractionResult(
        row_count=row_count,
        extraction_method="pipe",
    )
```

### Per-Database CLI Commands

Each extractor implements `_build_extraction_command(config)` returning the CLI args:

**PostgreSQL:**
```python
def _build_extraction_command(self, config):
    query = self.build_export_query(config)  # SELECT ... WHERE predicates
    return [
        "psql",
        "-h", config.host, "-p", str(config.port),
        "-U", config.user, "-d", config.database,
        "-c", f"COPY ({query}) TO STDOUT WITH (FORMAT csv, HEADER)",
        "--no-psqlrc", "--quiet",
    ]

def _build_env(self, config):
    env = os.environ.copy()
    env["PGPASSWORD"] = config.password
    return env
```

**MySQL:**
```python
def _build_extraction_command(self, config):
    query = self.build_export_query(config)
    return [
        "mysql",
        "-h", config.host, "-P", str(config.port),
        "-u", config.user, config.database,
        "-e", query,
        "--batch", "--raw",  # tab-separated, no escaping
    ]

def _build_env(self, config):
    env = os.environ.copy()
    env["MYSQL_PWD"] = config.password
    return env
```
Note: MySQL `--batch` outputs tab-delimited. PyArrow `open_csv()` is configured with `parse_options=pa_csv.ParseOptions(delimiter='\t')`.

**SQL Server:**
```python
def _build_extraction_command(self, config):
    query = self.build_export_query(config)
    return [
        "bcp", query, "queryout", "/dev/stdout",
        "-S", config.host,
        "-U", config.user, "-P", config.password,
        "-c", "-t", ",",  # character mode, comma-separated
    ]
```

**ClickHouse:**
```python
def _build_extraction_command(self, config):
    query = self.build_export_query(config)
    return [
        "clickhouse-client",
        "-h", config.host, "--port", str(config.port),
        "-u", config.user, "--password", config.password,
        "-d", config.database,
        "--query", f"{query} FORMAT CSVWithNames",
    ]
```

### Predicate Pushdown Compatibility

Predicate pushdown still works -- the WHERE clause is embedded in the SQL query passed to the CLI tool. The query optimizer produces predicates in the source dialect, which is exactly what the CLI tool expects. Same predicates, same pushdown, different transport.

### Error Handling

- Check `proc.returncode` after `proc.wait()`
- If the CLI process fails, fall back to existing extraction method (`_extract_native`)
- Log warnings when falling back (so users know pipe extraction was attempted but failed)
- Capture stderr from the subprocess for error diagnostics
- If PyArrow fails mid-stream (malformed CSV, type inference error), kill the subprocess and fall back

---

## Loader Changes

### New Method: `_load_pipe()`

Each on-prem loader gains a `_load_pipe()` method. This **eliminates the `df.collect()` bottleneck**:

```python
def load(self, df, config, adapter):
    if shutil.which(self.cli_tool):
        return self._load_pipe(df, config, adapter)     # Tier 1
    elif config.bucket and self.supports_bulk_load(...):
        return self._load_bulk(df, config)              # Tier 2 (cloud)
    else:
        return self._load_jdbc(df, config)              # Tier 3 (JDBC)
```

### Core Pipe Loading Pattern

```python
def _load_pipe(self, df, config, adapter):
    """Load data via PyArrow streaming Parquet-to-CSV + CLI tool pipe."""
    
    # Step 1: DDL via dbt adapter (unchanged -- CREATE TABLE, DROP, etc.)
    self._execute_ddl(config, adapter)
    
    # Step 2: Write Spark result to temp Parquet
    #   Spark executors write to disk -- bounded memory per partition
    temp_parquet = f"{config.staging_path}/_result/{config.model_name}.parquet"
    df.write.mode("overwrite").option("compression", "zstd").parquet(temp_parquet)
    
    # Step 3: Spawn target DB CLI tool as subprocess
    cmd = self._build_load_command(config)
    proc = subprocess.Popen(
        cmd,
        stdin=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=self._build_env(config),
    )
    
    # Step 4: PyArrow reads Parquet in batches, writes CSV to subprocess stdin
    try:
        parquet_file = pq.ParquetFile(temp_parquet)
        first_batch = True
        row_count = 0
        
        for batch in parquet_file.iter_batches(batch_size=65536):
            sink = pa.BufferOutputStream()
            write_options = pa_csv.WriteOptions(include_header=first_batch)
            pa_csv.write_csv(
                pa.Table.from_batches([batch]),
                sink,
                write_options=write_options,
            )
            proc.stdin.write(sink.getvalue().to_pybytes())
            row_count += len(batch)
            first_batch = False
        
        proc.stdin.close()
    except Exception as e:
        proc.kill()
        raise
    
    # Step 5: Check subprocess exit
    proc.wait()
    if proc.returncode != 0:
        stderr = proc.stderr.read().decode()
        raise RuntimeError(f"CLI load failed: {stderr}")
    
    # Step 6: Clean up temp Parquet
    shutil.rmtree(temp_parquet, ignore_errors=True)
    
    return LoadResult(row_count=row_count, load_method="pipe")
```

### Per-Database CLI Load Commands

Each loader implements `_build_load_command(config)`:

**PostgreSQL:**
```python
def _build_load_command(self, config):
    return [
        "psql",
        "-h", config.host, "-p", str(config.port),
        "-U", config.user, "-d", config.database,
        "-c", f"COPY {config.quoted_table} FROM STDIN WITH (FORMAT csv, HEADER)",
        "--no-psqlrc", "--quiet",
    ]
```

**MySQL:**
```python
def _build_load_command(self, config):
    return [
        "mysql",
        "-h", config.host, "-P", str(config.port),
        "-u", config.user, config.database,
        "--local-infile=1",
        "-e", f"LOAD DATA LOCAL INFILE '/dev/stdin' INTO TABLE {config.table} "
              f"FIELDS TERMINATED BY ',' ENCLOSED BY '\"' "
              f"LINES TERMINATED BY '\\n' IGNORE 1 LINES",
    ]
```

**SQL Server:**
```python
def _build_load_command(self, config):
    return [
        "bcp", config.table, "in", "/dev/stdin",
        "-S", config.host,
        "-U", config.user, "-P", config.password,
        "-c", "-t", ",",
    ]
```

**ClickHouse:**
```python
def _build_load_command(self, config):
    return [
        "clickhouse-client",
        "-h", config.host, "--port", str(config.port),
        "-u", config.user, "--password", config.password,
        "-d", config.database,
        "--query", f"INSERT INTO {config.table} FORMAT CSVWithNames",
    ]
```

### DDL Handling

DDL (CREATE TABLE, DROP TABLE, TRUNCATE) continues to use the **dbt adapter** for dialect-aware identifier quoting. Only the data plane changes from `df.collect()` + in-memory buffer to pipe streaming.

### Note on Spark Result -> Temp Parquet

The loading side still requires Spark to write the result DataFrame to a temp Parquet file first (Step 2 above). This is because the Spark SQL transform step produces a Spark DataFrame, not a file. Writing to temp Parquet is already how cloud loaders work (Snowflake, BigQuery, etc.), so this is consistent. The key improvement is that the temp Parquet -> target DB step uses PyArrow batch streaming + pipe instead of `df.collect()`.

---

## What Doesn't Change

- **Cloud bucket path** (Tier 2): Snowflake, BigQuery, Redshift, Databricks -- untouched
- **JDBC fallback** (Tier 3): Always available when CLI tools aren't installed
- **StateManager**: No changes. Schema hash from source DB metadata. File existence checks only.
- **Delta detection**: Row hashing runs against source DB. Hash Parquet written by StateManager's own PyArrow code.
- **Query optimizer**: Predicate pushdown, column pruning -- same SQL, different transport.
- **SQLGlot transpilation**: Unchanged. Transform step still needs Spark.
- **`computes.yml` / `buckets.yml`**: No schema changes.
- **Federation resolver**: Unchanged. Still determines PUSHDOWN vs FEDERATION per model.
- **DVT tasks**: `DvtRunTask`, `DvtBuildTask`, etc. -- unchanged. Runner routing is the same.
- **Auth handlers**: Still used for credential resolution (building CLI connection strings + environment variables).
- **PyArrow dependency**: Already installed. No new dependencies added.

---

## Implementation Phases

### Phase 1: `dvt sync` CLI Tool Detection

**Files to create/modify:**
- `core/dvt/task/cli_tools.py` -- new module: tool registry, detection, install instructions
- `core/dvt/task/sync.py` -- add Step 9 calling the tool registry

**Scope:**
- Tool registry data structure (adapter type -> tool name -> check command -> install instructions per OS)
- Platform detection (macOS / Debian / RHEL / Alpine)
- `detect_cli_tools(adapter_types)` function
- Integration into `SyncTask.run()`
- Only checks tools for on-prem adapters found in `profiles.yml`
- Unit tests with mocked `shutil.which()`

### Phase 2: Pipe-Based Extractors

**Files to create/modify:**
- `core/dvt/federation/pipe_converter.py` -- new module: shared PyArrow streaming helpers
- `core/dvt/federation/extractors/postgres.py` -- add `_extract_pipe()`
- `core/dvt/federation/extractors/mysql.py` -- add `_extract_pipe()`
- `core/dvt/federation/extractors/sqlserver.py` -- add `_extract_pipe()`
- `core/dvt/federation/extractors/clickhouse.py` -- add `_extract_pipe()`
- `core/dvt/federation/extractors/base.py` -- add pipe detection helper + shared `_extract_via_pipe()` method

**Scope:**
- `_extract_pipe()` per extractor: builds CLI command, delegates to shared pipe extraction
- Shared `_extract_via_pipe(cmd, env, output_path)` in base: subprocess + PyArrow streaming
- Build CLI connection strings from auth handler credentials
- Pass credentials via environment variables (PGPASSWORD, MYSQL_PWD, etc.)
- Embed predicate pushdown WHERE clauses in extraction SQL
- Runtime detection: `shutil.which(cli_tool)`
- Silent fallback to existing methods on pipe failure
- Unit tests with mocked subprocess calls

### Phase 3: Pipe-Based Loaders

**Files to modify:**
- `core/dvt/federation/loaders/postgres.py` -- add `_load_pipe()`, eliminating `df.collect()`
- `core/dvt/federation/loaders/generic.py` -- add `_load_pipe()` for MySQL/SQLServer/ClickHouse
- `core/dvt/federation/loaders/base.py` -- add shared `_load_via_pipe()` method + pipe detection

**Scope:**
- `_load_pipe()` per loader: DDL via adapter, write temp Parquet, pipe to target
- Shared `_load_via_pipe(parquet_path, cmd, env)` in base: PyArrow Parquet batch reader -> CSV stream -> subprocess stdin
- Write Spark result to temp Parquet (reuse existing cloud loader pattern)
- DDL still via dbt adapter
- Error handling with fallback to JDBC
- Unit tests with mocked subprocess calls

### Phase 4: Testing & Validation

**Scope:**
- Verify PyArrow streaming Parquet output is identical to current `pq.write_table()` output
- Verify StateManager skip logic works with pipe-produced Parquet files
- Verify predicate pushdown in pipe extraction matches JDBC extraction results
- Performance benchmarks: pipe vs JDBC for various table sizes (1K, 100K, 1M, 10M rows)
- E2E test with mixed on-prem + cloud sources in one model
- Test fallback behavior when CLI tools are missing
- Test error handling when pipe processes fail mid-stream
- Test CSV edge cases: NULLs, special characters, newlines in values, unicode

---

## Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|------|:----------:|:------:|------------|
| DB CLI tool not installed | Medium | None | Silent fallback to existing JDBC/native path. No regression. `dvt sync` notifies user. |
| DB CLI tool version incompatibility | Low | Medium | Version check in `dvt sync` detection. Document minimum versions. |
| Pipe failure mid-stream (broken pipe) | Low | Medium | Catch subprocess errors, fall back to JDBC, log warning with stderr output. |
| CSV encoding edge cases (NULLs, special chars, newlines in values) | Medium | Medium | Use proper CSV escaping. PostgreSQL COPY and PyArrow both handle RFC 4180 CSV. Test with adversarial data. |
| PyArrow CSV type inference mismatch | Low | Low | PyArrow auto-detects types from CSV. Same library as current code -- behavior is identical. Can override with explicit `ConvertOptions` if needed. |
| Password exposure in process args | Medium | High | Use environment variables (PGPASSWORD, MYSQL_PWD) instead of CLI flags. Passwords never appear in `ps` output. |
| MySQL `--local-infile` disabled on server | Low | Medium | Detect and fall back to JDBC. Log informative warning. |

---

## Future Considerations

- **DuckDB as transform engine**: DuckDB could replace Spark for the SQL transform step (reading multiple Parquet files, executing JOINs). This would eliminate the Spark/JVM dependency entirely for single-machine workloads. Out of scope for this enhancement but a natural extension. DuckDB CLI detection could be added to `dvt sync` at that point.
- **Incremental delta extraction**: The existing StateManager hash infrastructure could be wired into pipe extraction -- extract only changed rows by building WHERE clauses from `get_changed_pks()`. Blocked on the partial implementation of delta loading in ELLayer.
- **Column projection pushdown**: Currently disabled due to case sensitivity issues. Pipe extraction doesn't change this -- same SQL queries, same column handling.
- **Named pipes (FIFOs)**: For tools that require file paths instead of stdin/stdout, named pipes (`mkfifo`) could provide the pipe semantics through a filesystem path. Not needed for current tool set but useful for future database CLIs.
