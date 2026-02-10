# EL Layer Implementation Plan

## Overview

This document outlines the plan for implementing DVT's Extract-Load (EL) Layer, which optimizes data extraction from source systems to buckets for Spark federation.

**Key Features:**
1. Native bulk export via dbt adapter connections
2. Predicate pushdown to source databases
3. Parquet output with compression
4. Consecutive run optimization (skip unchanged data)
5. Hash-based incremental extraction
6. State management in bucket

---

## Architecture

### High-Level Flow

```
┌─────────────────────────────────────────────────────────────────────┐
│                    Model SQL (User writes)                          │
│  SELECT o.*, c.name FROM {{ source('pg', 'orders') }} o             │
│  JOIN {{ source('mysql', 'customers') }} c ON o.cust_id = c.id      │
│  WHERE o.date > '2024-01-01'                                        │
└───────────────────────────────┬─────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    1. SQLGlot Parser                                │
│  - Parse SQL, identify source references                            │
│  - Extract predicates per source                                    │
│  - Identify required columns per source                             │
│  - Transpile to source dialect for export query                     │
└───────────────────────────────┬─────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    2. Extraction Planner                            │
│  For each source:                                                   │
│  - Check if staging exists (skip if unchanged)                      │
│  - Check hash state for incremental                                 │
│  - Build export query with pushed predicates                        │
│  - Determine extraction method (COPY, export, etc.)                 │
└───────────────────────────────┬─────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    3. Parallel Extraction                           │
│  Execute extractions in parallel:                                   │
│  - Source 1 → Parquet → Bucket                                      │
│  - Source 2 → Parquet → Bucket                                      │
│  - Source N → Parquet → Bucket                                      │
│  - Update hash state                                                │
└───────────────────────────────┬─────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    4. Spark Compute                                 │
│  - Read Parquet from bucket                                         │
│  - Execute JOINs, transforms                                        │
│  - Write to target                                                  │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Consecutive Run Optimization

### Run Behavior Matrix

| Scenario | Staging Exists? | Hash Changed? | Action |
|----------|-----------------|---------------|--------|
| First run | No | N/A | Full extraction |
| Normal run | Yes | No | Skip extraction, use existing |
| Normal run | Yes | Yes | Incremental extraction |
| `--full-refresh` | Any | Any | Clear staging, full extraction |
| Source schema changed | Yes | N/A | Full extraction |

### Decision Flow

```
┌─────────────────────────────────────────┐
│          Is --full-refresh?             │
└───────────────┬─────────────────────────┘
                │
        ┌───────┴───────┐
        │ Yes           │ No
        ▼               ▼
┌───────────────┐  ┌─────────────────────────────────┐
│ Clear staging │  │ Does staging Parquet exist?     │
│ + hash state  │  └───────────────┬─────────────────┘
│ Full extract  │                  │
└───────────────┘          ┌───────┴───────┐
                           │ No            │ Yes
                           ▼               ▼
                   ┌───────────────┐  ┌─────────────────────────────────┐
                   │ Full extract  │  │ Load hash state from bucket     │
                   └───────────────┘  │ Compare current source hashes   │
                                      └───────────────┬─────────────────┘
                                                      │
                                              ┌───────┴───────┐
                                              │ Changed?      │ No change
                                              ▼               ▼
                                      ┌───────────────┐  ┌───────────────┐
                                      │ Incremental   │  │ Skip, use     │
                                      │ extraction    │  │ existing      │
                                      └───────────────┘  └───────────────┘
```

---

## Hash-Based Incremental Extraction

### How It Works

1. **Initial extraction:** Extract all rows, compute hash for each row, store hashes
2. **Subsequent runs:** 
   - Query source for current row hashes (or sample)
   - Compare to stored hashes
   - Extract only changed/new rows
   - Update hash state

### Hash Computation

```python
def compute_row_hash(row: Dict, columns: List[str]) -> str:
    """
    Compute MD5 hash of entire row.
    
    Args:
        row: Row data as dict
        columns: Column names in consistent order
    
    Returns:
        MD5 hash string
    """
    import hashlib
    
    # Sort columns for consistency
    values = [str(row.get(col, '')) for col in sorted(columns)]
    row_str = '|'.join(values)
    return hashlib.md5(row_str.encode()).hexdigest()
```

### Hash State Storage

Stored in bucket alongside data:

```
.dvt/staging/
├── _state/
│   ├── postgres__orders.state.json         # Metadata
│   ├── postgres__orders.hashes.parquet     # PK → Hash mapping
│   ├── mysql__customers.state.json
│   └── mysql__customers.hashes.parquet
├── postgres__orders.parquet                 # Actual data
├── mysql__customers.parquet
└── oracle__products.parquet
```

**State JSON Schema:**

```json
{
  "source_name": "postgres__orders",
  "table_name": "orders",
  "schema_hash": "abc123",
  "row_count": 1000000,
  "last_extracted_at": "2024-01-15T10:30:00Z",
  "extraction_method": "incremental",
  "pk_columns": ["id"],
  "columns": ["id", "customer_id", "order_date", "total"]
}
```

**Hashes Parquet Schema:**

| Column | Type | Description |
|--------|------|-------------|
| `_pk` | `string` | Primary key value(s) concatenated |
| `_hash` | `string` | MD5 hash of entire row |
| `_extracted_at` | `timestamp` | When this row was extracted |

### Primary Key Detection

```python
def detect_primary_key(adapter, schema: str, table: str) -> List[str]:
    """
    Auto-detect primary key columns from database metadata.
    
    Uses adapter's get_columns() and database-specific PK queries.
    Falls back to unique index if no PK.
    Falls back to all columns if nothing found (full row = PK).
    """
    # PostgreSQL
    # SELECT a.attname FROM pg_index i
    # JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
    # WHERE i.indrelid = 'table'::regclass AND i.indisprimary;
    
    # MySQL
    # SHOW KEYS FROM table WHERE Key_name = 'PRIMARY';
    
    # Snowflake
    # SHOW PRIMARY KEYS IN TABLE schema.table;
    pass
```

---

## Native Bulk Export Methods

### Per-Database Export Strategies

| Database | Export Method | Via Adapter | Notes |
|----------|--------------|-------------|-------|
| PostgreSQL | `COPY (query) TO STDOUT` | psycopg2 cursor | Stream directly to Parquet |
| MySQL | Cursor streaming | mysql-connector | Batch fetch + write |
| Snowflake | `COPY INTO @stage` | snowflake-connector | Native to cloud stage |
| BigQuery | `EXPORT DATA` to GCS | google-cloud-bigquery | Native to GCS |
| Databricks | Unity Catalog export | databricks-connector | Native to cloud |
| Oracle | Cursor streaming | oracledb | Batch fetch + write |
| SQL Server | Cursor streaming | pyodbc | Batch fetch + write |
| Redshift | `UNLOAD` to S3 | redshift-connector | Native to S3 |

### Reusing Adapter Connections

```python
def get_adapter_connection(source_name: str):
    """
    Get the dbt adapter connection for a source.
    
    This reuses the connection dbt adapters already manage.
    No new dependencies needed!
    """
    # Get adapter from dbt's adapter registry
    adapter = get_adapter(config)
    connection = adapter.connections.get_thread_connection()
    return connection.handle  # The raw DB-API connection
```

### Export Implementation Example (Postgres)

```python
def export_postgres_to_parquet(
    connection,
    query: str,
    output_path: str,
    batch_size: int = 100000
) -> int:
    """
    Export Postgres query results to Parquet using COPY.
    
    Returns row count.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq
    from io import StringIO
    
    # Use COPY for speed
    copy_query = f"COPY ({query}) TO STDOUT WITH (FORMAT CSV, HEADER)"
    
    buffer = StringIO()
    with connection.cursor() as cursor:
        cursor.copy_expert(copy_query, buffer)
    
    buffer.seek(0)
    
    # Convert to Parquet via PyArrow
    table = pa.csv.read_csv(buffer)
    pq.write_table(table, output_path, compression='zstd')
    
    return table.num_rows
```

---

## Predicate Pushdown

### Extracting Predicates with SQLGlot

```python
import sqlglot
from sqlglot import exp

def extract_source_predicates(
    sql: str, 
    source_alias: str
) -> List[exp.Expression]:
    """
    Extract WHERE predicates that apply to a specific source table.
    
    Example:
        SQL: SELECT * FROM orders o JOIN customers c ON ...
             WHERE o.date > '2024-01-01' AND c.country = 'US'
        
        For source 'orders' (alias 'o'):
        Returns: [o.date > '2024-01-01']
    """
    parsed = sqlglot.parse_one(sql)
    
    predicates = []
    where = parsed.find(exp.Where)
    
    if where:
        for condition in where.find_all(exp.Condition):
            # Check if condition references only this source
            columns = condition.find_all(exp.Column)
            if all(col.table == source_alias for col in columns):
                predicates.append(condition)
    
    return predicates
```

### Building Export Query with Predicates

```python
def build_export_query(
    source_table: str,
    columns: List[str],
    predicates: List[exp.Expression],
    source_dialect: str
) -> str:
    """
    Build export query with pushed predicates.
    
    Example output:
        SELECT id, name, date 
        FROM orders 
        WHERE date > '2024-01-01'
    """
    import sqlglot
    
    # Build SELECT
    select = sqlglot.select(*columns).from_(source_table)
    
    # Add predicates
    for pred in predicates:
        select = select.where(pred)
    
    # Transpile to source dialect
    return select.sql(dialect=source_dialect)
```

---

## Column Pruning

### Extracting Required Columns

```python
def extract_required_columns(
    sql: str,
    source_alias: str,
    source_columns: List[str]
) -> List[str]:
    """
    Identify which columns from a source are actually used in the query.
    
    Handles:
    - SELECT columns
    - JOIN conditions
    - WHERE predicates
    - GROUP BY / ORDER BY
    """
    parsed = sqlglot.parse_one(sql)
    
    used_columns = set()
    
    for column in parsed.find_all(exp.Column):
        if column.table == source_alias:
            used_columns.add(column.name)
    
    # If SELECT *, we need all columns
    for star in parsed.find_all(exp.Star):
        if star.table == source_alias or star.table is None:
            return source_columns  # Need all
    
    return list(used_columns) if used_columns else source_columns
```

---

## Parallel Extraction

### Concurrent Source Extraction

```python
import concurrent.futures
from typing import List, Dict

def extract_sources_parallel(
    sources: List[SourceConfig],
    bucket_path: str,
    max_workers: int = 4
) -> Dict[str, ExtractionResult]:
    """
    Extract multiple sources in parallel.
    
    Returns dict of source_name -> ExtractionResult
    """
    results = {}
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_source = {
            executor.submit(extract_source, source, bucket_path): source
            for source in sources
        }
        
        for future in concurrent.futures.as_completed(future_to_source):
            source = future_to_source[future]
            try:
                result = future.result()
                results[source.name] = result
            except Exception as e:
                results[source.name] = ExtractionResult(
                    success=False,
                    error=str(e)
                )
    
    return results
```

---

## State Management

### State Manager Class

```python
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, List, Dict
from pathlib import Path
import json

@dataclass
class SourceState:
    """State for a single source table."""
    source_name: str
    table_name: str
    schema_hash: str
    row_count: int
    last_extracted_at: datetime
    extraction_method: str  # 'full' or 'incremental'
    pk_columns: List[str]
    columns: List[str]


class StateManager:
    """Manage extraction state in bucket."""
    
    def __init__(self, bucket_path: str):
        self.state_path = Path(bucket_path) / '_state'
        self.state_path.mkdir(parents=True, exist_ok=True)
    
    def get_source_state(self, source_name: str) -> Optional[SourceState]:
        """Load state for a source."""
        state_file = self.state_path / f"{source_name}.state.json"
        if not state_file.exists():
            return None
        with open(state_file) as f:
            data = json.load(f)
        return SourceState(**data)
    
    def save_source_state(self, state: SourceState):
        """Save state for a source."""
        state_file = self.state_path / f"{state.source_name}.state.json"
        with open(state_file, 'w') as f:
            json.dump(state.__dict__, f, default=str)
    
    def get_row_hashes(self, source_name: str) -> Dict[str, str]:
        """Load row hashes (pk -> hash) from Parquet."""
        import pandas as pd
        hash_file = self.state_path / f"{source_name}.hashes.parquet"
        if not hash_file.exists():
            return {}
        df = pd.read_parquet(hash_file)
        return dict(zip(df['_pk'], df['_hash']))
    
    def save_row_hashes(self, source_name: str, hashes: Dict[str, str]):
        """Save row hashes to Parquet."""
        import pandas as pd
        hash_file = self.state_path / f"{source_name}.hashes.parquet"
        df = pd.DataFrame({'_pk': list(hashes.keys()), '_hash': list(hashes.values())})
        df.to_parquet(hash_file)
    
    def clear_source_state(self, source_name: str):
        """Clear all state for a source (for --full-refresh)."""
        for pattern in [f"{source_name}.state.json", f"{source_name}.hashes.parquet"]:
            path = self.state_path / pattern
            if path.exists():
                path.unlink()
    
    def clear_all_state(self):
        """Clear all state (for dvt clean)."""
        import shutil
        if self.state_path.exists():
            shutil.rmtree(self.state_path)
            self.state_path.mkdir(parents=True, exist_ok=True)
```

---

## Integration with DVT Commands

### `dvt run` Flow

```python
def run_model_with_el(model, args):
    """
    Run a model with EL layer optimization.
    """
    # 1. Parse model SQL
    sources = extract_sources_from_sql(model.compiled_sql)
    
    # 2. Check if federation needed
    if all_sources_same_target(sources, model.target):
        # Pushdown path - no EL needed
        return run_with_adapter_pushdown(model)
    
    # 3. Federation path - use EL layer
    bucket_path = get_bucket_path()
    state_manager = StateManager(bucket_path)
    
    # 4. Handle --full-refresh
    if args.full_refresh:
        for source in sources:
            state_manager.clear_source_state(source.name)
            clear_staging_data(bucket_path, source.name)
    
    # 5. Plan extractions
    extractions = []
    for source in sources:
        state = state_manager.get_source_state(source.name)
        
        if state and staging_exists(bucket_path, source.name):
            # Check if incremental possible
            if should_extract_incremental(source, state):
                extractions.append(IncrementalExtraction(source, state))
            else:
                # Use existing staging - skip extraction
                continue
        else:
            extractions.append(FullExtraction(source))
    
    # 6. Execute extractions in parallel
    results = extract_sources_parallel(extractions, bucket_path)
    
    # 7. Update state
    for source_name, result in results.items():
        if result.success:
            state_manager.save_source_state(result.new_state)
            if result.row_hashes:
                state_manager.save_row_hashes(source_name, result.row_hashes)
    
    # 8. Run Spark computation
    return run_with_spark_federation(model, sources, bucket_path)
```

### `--full-refresh` Behavior

```python
def handle_full_refresh(sources: List[Source], bucket_path: str):
    """
    Clear staging and state for full refresh.
    """
    state_manager = StateManager(bucket_path)
    
    for source in sources:
        # Clear state (metadata + hashes)
        state_manager.clear_source_state(source.name)
        
        # Clear staging data
        staging_file = Path(bucket_path) / f"{source.name}.parquet"
        if staging_file.exists():
            staging_file.unlink()
    
    # Now all sources will do full extraction
```

---

## Incremental Extraction Flow

### Full Flow for Incremental

```
┌─────────────────────────────────────────────────────────────────────┐
│                    1. Load Previous State                           │
│  - Load row hashes from bucket/_state/source.hashes.parquet         │
│  - Get PK columns from state                                        │
└───────────────────────────────┬─────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    2. Query Current Hashes                          │
│  SELECT pk_col, MD5(CONCAT(col1, col2, ...)) as hash                │
│  FROM source_table                                                  │
│  (Only query PK + hash, not full rows)                              │
└───────────────────────────────┬─────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    3. Compare Hashes                                │
│  - New rows: PK in current but not in previous                      │
│  - Changed rows: Same PK, different hash                            │
│  - Deleted rows: PK in previous but not in current                  │
└───────────────────────────────┬─────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    4. Extract Changed Rows Only                     │
│  SELECT * FROM source_table                                         │
│  WHERE pk_col IN (new_pks + changed_pks)                            │
└───────────────────────────────┬─────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    5. Merge with Existing Staging                   │
│  - Read existing Parquet                                            │
│  - Remove rows with changed/deleted PKs                             │
│  - Append new/changed rows                                          │
│  - Write updated Parquet                                            │
└───────────────────────────────┬─────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    6. Update Hash State                             │
│  - Save new hash state to bucket                                    │
└─────────────────────────────────────────────────────────────────────┘
```

### Database-Side Hash Computation

For efficiency, compute hashes in the database:

```sql
-- PostgreSQL
SELECT 
    id as _pk,
    MD5(CONCAT_WS('|', col1::text, col2::text, col3::text)) as _hash
FROM orders;

-- MySQL
SELECT 
    id as _pk,
    MD5(CONCAT_WS('|', col1, col2, col3)) as _hash
FROM orders;

-- Snowflake
SELECT 
    id as _pk,
    MD5(CONCAT_WS('|', col1, col2, col3)) as _hash
FROM orders;

-- BigQuery
SELECT 
    id as _pk,
    TO_HEX(MD5(CONCAT(col1, '|', col2, '|', col3))) as _hash
FROM orders;

-- SQL Server
SELECT 
    id as _pk,
    CONVERT(VARCHAR(32), HASHBYTES('MD5', CONCAT(col1, '|', col2, '|', col3)), 2) as _hash
FROM orders;
```

---

## Files to Create/Modify

| File | Action | Description |
|------|--------|-------------|
| `core/dvt/federation/el_layer.py` | Create | Main EL layer orchestration |
| `core/dvt/federation/extractors/` | Create | Per-database extractor implementations |
| `core/dvt/federation/extractors/__init__.py` | Create | Extractor registry |
| `core/dvt/federation/extractors/base.py` | Create | Base extractor class |
| `core/dvt/federation/extractors/postgres.py` | Create | Postgres COPY extractor |
| `core/dvt/federation/extractors/mysql.py` | Create | MySQL streaming extractor |
| `core/dvt/federation/extractors/snowflake.py` | Create | Snowflake COPY extractor |
| `core/dvt/federation/extractors/generic.py` | Create | Generic cursor-based fallback |
| `core/dvt/federation/state_manager.py` | Create | Hash state management |
| `core/dvt/federation/predicate_pushdown.py` | Create | SQLGlot predicate extraction |
| `core/dvt/federation/column_pruning.py` | Create | SQLGlot column detection |
| `core/dvt/task/run.py` | Modify | Integrate EL layer into run flow |

---

## Configuration

### Source-Level Configuration (sources.yml)

```yaml
sources:
  - name: postgres_oltp
    database: production
    schema: public
    
    # NEW: Extraction configuration
    extraction:
      # Method: auto | full | incremental
      method: auto
      
      # For incremental
      incremental:
        # Strategy: hash (default)
        strategy: hash
        
        # Override auto-detected PK
        # primary_key: [id]
        
        # Columns to hash (default: all)
        # hash_columns: [col1, col2, col3]
      
      # Parallel extraction settings
      batch_size: 100000
      max_parallel: 4
    
    tables:
      - name: orders
        # Per-table overrides
        extraction:
          method: incremental
          incremental:
            primary_key: [order_id]
```

---

## Dependencies

No new dependencies! The EL layer uses:

| Dependency | Purpose | Already Have |
|------------|---------|--------------|
| `sqlglot` | SQL parsing, predicate extraction | ✅ Yes |
| `pyarrow` | Parquet read/write | ✅ Via dvt sync |
| `pyspark` | Spark compute | ✅ Via dvt sync |
| dbt adapters | Database connections | ✅ Yes |

---

## Testing Plan

### Unit Tests

| Test | Description |
|------|-------------|
| `test_predicate_extraction` | Extract predicates from SQL |
| `test_column_pruning` | Detect used columns |
| `test_hash_computation` | Compute row hashes |
| `test_state_manager_save_load` | State save/load |
| `test_state_manager_clear` | State clearing |
| `test_incremental_detection` | Detect changed rows |
| `test_pk_detection_postgres` | Auto-detect PK from Postgres |
| `test_pk_detection_mysql` | Auto-detect PK from MySQL |

### Integration Tests

| Test | Description |
|------|-------------|
| `test_postgres_extraction` | Extract from Postgres to Parquet |
| `test_consecutive_runs_skip` | Skip extraction on unchanged data |
| `test_consecutive_runs_incremental` | Only extract changed rows |
| `test_full_refresh_clears_state` | --full-refresh clears state + data |
| `test_parallel_extraction` | Extract multiple sources concurrently |

---

## Implementation Phases

### Phase 1: Basic EL Layer
- [ ] Full extraction via adapters
- [ ] Parquet output to bucket
- [ ] Skip extraction if staging exists
- [ ] `--full-refresh` clears staging + state
- [ ] State manager (metadata only, no hashes yet)

### Phase 2: Predicate Pushdown
- [ ] SQLGlot predicate extraction
- [ ] Build export query with filters
- [ ] Transpile to source dialect

### Phase 3: Column Pruning
- [ ] Detect used columns from SQL
- [ ] Export only needed columns
- [ ] Handle SELECT *

### Phase 4: Hash-Based Incremental
- [ ] PK auto-detection per database
- [ ] Database-side hash computation
- [ ] Hash state storage in bucket
- [ ] Incremental merge logic
- [ ] Handle deletes

### Phase 5: Parallel Extraction
- [ ] ThreadPoolExecutor for sources
- [ ] Progress reporting
- [ ] Error handling per source
- [ ] Configurable max_parallel

---

## Performance Expectations

| Scenario | Current (JDBC) | With EL Layer |
|----------|----------------|---------------|
| 100M rows, first run | 60 min | 10 min |
| 100M rows, no changes | 60 min | 0 min (skip) |
| 100M rows, 1% changed | 60 min | 1 min |
| 3 sources parallel | 180 min | 15 min |

---

## Interaction with `dvt clean`

The `dvt clean` command should also clean the `_state/` directory:

```python
def _clean_staging(self):
    """Clean DVT staging buckets including state."""
    # ... existing bucket cleaning logic ...
    
    # Also clean _state/ directory
    state_path = bucket_path / '_state'
    if state_path.exists():
        shutil.rmtree(state_path)
        state_path.mkdir(parents=True, exist_ok=True)
```

See `bucket_enhancement.md` for the updated `dvt clean` implementation.

---

## Future Enhancements

1. **CDC integration** - Use database logs for real-time incremental
2. **Partition pruning** - Only extract partitions touched by query
3. **Materialized staging** - Keep hot tables always staged
4. **Compression tuning** - Per-source compression settings
5. **Staging TTL** - Auto-expire staging after N days
6. **Sampling for hash check** - Sample rows for faster change detection on huge tables
