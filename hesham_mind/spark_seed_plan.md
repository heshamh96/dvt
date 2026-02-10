# Spark-Enhanced Seed Plan

> **Document Version:** 1.0  
> **Created:** February 2026  
> **Status:** Implementation Ready  
> **Related:** `federation_execution_plan.md`

## Executive Summary

This plan details the enhancement of DVT's seed functionality to use Apache Spark for all seed operations. This replaces the current agate-based CSV loading with a Spark-native approach that:

- Handles arbitrarily large files (distributed processing)
- Provides better type inference
- Supports cross-target seeding (seed to any database via JDBC)
- Enables parallel writes for faster loading
- Properly respects target database type systems

### Key Design Decisions

| Aspect | Decision | Rationale |
|--------|----------|-----------|
| When to use Spark | **Always** | Consistency, better type handling, no memory limits |
| Type specification | **Reuse `column_types`** | Backward compatible with existing configs |
| Type inference | **Spark inference + target mapping** | Best of both worlds |
| Load mode | **Always full refresh** | Matches current dbt behavior, simpler semantics |
| Cross-target | **Yes, via CLI/config** | Enables seeding reference data to any target |

---

## 1. Current Seed Implementation (What We're Replacing)

### Current Flow

```
CSV File
    |
    v
agate.from_csv()          <-- Memory-bound, loads entire file
    |
    v
agate.Table (in memory)   <-- Limited type inference
    |                         (Integer, Number, Date, DateTime, Boolean, Text)
    v
Adapter: create_csv_table()
    |
    v
Adapter: load_csv_rows()  <-- Batch INSERT statements (10,000 rows/batch)
    |
    v
Target Table (default target only)
```

### Current Limitations

| Limitation | Impact |
|------------|--------|
| **Memory-bound** | Fails on large files (100MB+) |
| **Limited type inference** | Only 6 types recognized |
| **Batch INSERTs** | Slow for large datasets |
| **Single target only** | Cannot seed to non-default targets |
| **No parallel processing** | Single-threaded CSV parsing |
| **Limited date formats** | Only `%Y-%m-%d` and `%Y-%m-%d %H:%M:%S` |
| **Boolean limitations** | Only "true"/"false" recognized |

### Current Configuration Options

```yaml
# dbt_project.yml
seeds:
  my_project:
    +quote_columns: true      # Quote column names in DDL
    +delimiter: ","           # CSV delimiter  
    +column_types:            # Force columns to specific types
      id: integer
      amount: numeric(10,2)
```

---

## 2. New Spark-Based Seed Flow

### Architecture

```
+-----------------------------------------------------------------------------+
|                           SPARK SEED EXECUTION                               |
+-----------------------------------------------------------------------------+
|                                                                              |
|  seeds/my_seed.csv                                                           |
|       |                                                                      |
|       v                                                                      |
|  +-------------------------------------------+                              |
|  |  SparkSeedRunner.execute()                |                              |
|  |                                           |                              |
|  |  1. Get SparkSession (from computes.yml)  |                              |
|  |  2. Resolve target (CLI > config > default)|                             |
|  |  3. Get JDBC credentials for target       |                              |
|  +-------------------+-----------------------+                              |
|                      |                                                       |
|                      v                                                       |
|  +-------------------------------------------+                              |
|  |  Spark CSV Reader                         |                              |
|  |                                           |                              |
|  |  spark.read                               |                              |
|  |    .option("header", "true")              |                              |
|  |    .option("inferSchema", "true")         | <-- If no column_types       |
|  |    .option("delimiter", config.delimiter) |                              |
|  |    .csv(path)                             |                              |
|  |                                           |                              |
|  |  OR with explicit schema:                 |                              |
|  |    .schema(spark_schema)                  | <-- If column_types provided |
|  +-------------------+-----------------------+                              |
|                      |                                                       |
|                      v                                                       |
|  +-------------------------------------------+                              |
|  |  Column Type Application                  |                              |
|  |                                           |                              |
|  |  For each column in column_types:         |                              |
|  |    df = df.withColumn(col,                |                              |
|  |           df[col].cast(spark_type))       |                              |
|  +-------------------+-----------------------+                              |
|                      |                                                       |
|                      v                                                       |
|  +-------------------------------------------+                              |
|  |  Type Mapping to Target                   |                              |
|  |                                           |                              |
|  |  For each column:                         |                              |
|  |    spark_type = df.schema[col].dataType   |                              |
|  |    target_type = map_to_target(           |                              |
|  |        spark_type, target_dialect)        |                              |
|  |                                           |                              |
|  |  Generate CREATE TABLE DDL                |                              |
|  +-------------------+-----------------------+                              |
|                      |                                                       |
|                      v                                                       |
|  +-------------------------------------------+                              |
|  |  JDBC Write                               |                              |
|  |                                           |                              |
|  |  df.write                                 |                              |
|  |    .format("jdbc")                        |                              |
|  |    .option("url", jdbc_url)               |                              |
|  |    .option("dbtable", "schema.seed_name") |                              |
|  |    .option("createTableOptions", ...)     |                              |
|  |    .mode("overwrite")                     | <-- Full refresh             |
|  |    .save()                                |                              |
|  +-------------------------------------------+                              |
|                      |                                                       |
|                      v                                                       |
|  +-------------------------------------------+                              |
|  |  Target Database                          |                              |
|  |  (Postgres, Snowflake, MySQL, etc.)       |                              |
|  +-------------------------------------------+                              |
|                                                                              |
+-----------------------------------------------------------------------------+
```

### Benefits of Spark Approach

| Benefit | Description |
|---------|-------------|
| **No memory limits** | Spark processes files in partitions, handles GB+ files |
| **Better type inference** | Spark's inferSchema is more sophisticated |
| **Parallel writes** | JDBC writes happen in parallel across partitions |
| **Cross-target support** | Can seed to any target database via JDBC |
| **Consistent with federation** | Uses same Spark infrastructure as model federation |
| **More type formats** | Better date/time parsing, JSON support, etc. |

---

## 3. Type System

### 3.1 Spark Type Inference

When `inferSchema=true`, Spark infers these types from CSV:

| CSV Content | Spark Inferred Type |
|-------------|---------------------|
| `123`, `-456` | IntegerType or LongType |
| `123.45`, `1.0E10` | DoubleType |
| `true`, `false` | BooleanType |
| `2024-01-15` | DateType (with dateFormat) |
| `2024-01-15 10:30:00` | TimestampType (with timestampFormat) |
| Everything else | StringType |

### 3.2 User-Specified Types (column_types)

Users can override inference with `column_types`:

```yaml
seeds:
  my_seed:
    +column_types:
      id: integer
      amount: numeric(10,2)
      price: decimal(12,4)
      created_at: timestamp
      is_active: boolean
      metadata: text         # Force as text, don't try to parse
      uuid_col: varchar(36)  # UUID as string with length
```

**Type String → Spark Type Mapping:**

| User Type String | Spark Type | Notes |
|------------------|------------|-------|
| `integer`, `int` | IntegerType | |
| `bigint`, `long` | LongType | |
| `smallint`, `short` | ShortType | |
| `float`, `real` | FloatType | |
| `double` | DoubleType | |
| `numeric(p,s)`, `decimal(p,s)` | DecimalType(p,s) | |
| `boolean`, `bool` | BooleanType | |
| `date` | DateType | |
| `timestamp`, `datetime` | TimestampType | |
| `text`, `string` | StringType | |
| `varchar(n)`, `char(n)` | StringType | Length stored as metadata |
| `json`, `jsonb` | StringType | Stored as string |
| `binary`, `bytea` | BinaryType | |

### 3.3 Spark → Target Type Mapping

After Spark processing, map to target database types:

```
Spark Type          Postgres              Snowflake            MySQL                BigQuery
==========          ========              =========            =====                ========
StringType      ->  TEXT              ->  VARCHAR          ->  TEXT             ->  STRING
IntegerType     ->  INTEGER           ->  INTEGER          ->  INT              ->  INT64
LongType        ->  BIGINT            ->  BIGINT           ->  BIGINT           ->  INT64
ShortType       ->  SMALLINT          ->  SMALLINT         ->  SMALLINT         ->  INT64
FloatType       ->  REAL              ->  FLOAT            ->  FLOAT            ->  FLOAT64
DoubleType      ->  DOUBLE PRECISION  ->  DOUBLE           ->  DOUBLE           ->  FLOAT64
DecimalType(p,s)->  NUMERIC(p,s)      ->  NUMBER(p,s)      ->  DECIMAL(p,s)     ->  NUMERIC(p,s)
BooleanType     ->  BOOLEAN           ->  BOOLEAN          ->  BOOLEAN          ->  BOOL
DateType        ->  DATE              ->  DATE             ->  DATE             ->  DATE
TimestampType   ->  TIMESTAMP         ->  TIMESTAMP_NTZ    ->  DATETIME         ->  TIMESTAMP
BinaryType      ->  BYTEA             ->  BINARY           ->  BLOB             ->  BYTES
ArrayType       ->  ARRAY             ->  ARRAY            ->  JSON             ->  ARRAY
```

---

## 4. Target Resolution

Seeds can be loaded to any target, not just the default.

### Resolution Priority

```
1. CLI: dvt seed --target snowflake_prod     (highest priority)
2. Seed config: (if we add target to SeedConfig)
3. Profile default target                     (lowest priority)
```

### Cross-Target Seeding Example

```bash
# Default target is postgres, but seed to snowflake
dvt seed --target snowflake_prod

# Seed specific seed to specific target
dvt seed --select my_reference_data --target bigquery_analytics
```

### JDBC Connection

For cross-target seeding, DVT uses JDBC (same as federation):

```python
# Get credentials for resolved target
target_creds = self._get_target_credentials(resolved_target)

# Build JDBC URL
jdbc_url = jdbc_builder.build_jdbc_url(target_creds['type'], target_creds)

# Write via Spark JDBC
df.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", f"{schema}.{seed_name}") \
    .mode("overwrite") \
    .save()
```

---

## 5. Module Structure

### Files to Create/Modify

```
core/dvt/
|-- task/
|   +-- seed.py                    # MODIFY: Replace SeedRunner with SparkSeedRunner
|
|-- federation/
|   |-- seed.py                    # CREATE: Spark seed loading logic
|   |-- types.py                   # MODIFY: Add seed-specific type mappings
|   +-- spark.py                   # REUSE: SparkSessionManager (from federation plan)
|
+-- artifacts/resources/v1/
    +-- seed.py                    # MODIFY: Potentially add new config options
```

---

## 6. Implementation Details

### 6.1 SparkSeedRunner (`core/dvt/task/seed.py`)

```python
from typing import Optional, Type
import time
import threading

from dvt.artifacts.schemas.results import NodeStatus, RunStatus
from dvt.artifacts.schemas.run import RunResult
from dvt.contracts.graph.manifest import Manifest
from dvt.contracts.graph.nodes import SeedNode
from dvt.events.types import LogSeedResult, LogStartLine
from dvt.task.run import ModelRunner
from dvt.task.base import BaseRunner

from dvt.federation.seed import SparkSeedLoader
from dvt.federation.spark import SparkSessionManager, SparkNotInstalledError
from dvt.federation.jdbc import JDBCBuilder
from dvt.federation.warnings import FederationWarningCollector

from dbt_common.events.base_types import EventLevel
from dbt_common.events.functions import fire_event


class SparkSeedRunner(BaseRunner):
    """
    Seed runner that uses Spark for CSV loading.
    
    Replaces the agate-based SeedRunner with a Spark-native approach that:
    - Handles arbitrarily large files (distributed processing)
    - Provides better type inference
    - Supports cross-target seeding (seed to any database via JDBC)
    - Enables parallel writes for faster loading
    
    Always performs full refresh (table recreation).
    """
    
    def __init__(self, config, adapter, node: SeedNode, node_index: int, num_nodes: int,
                 spark_manager: SparkSessionManager,
                 jdbc_builder: JDBCBuilder):
        super().__init__(config, adapter, node, node_index, num_nodes)
        self.spark_manager = spark_manager
        self.jdbc_builder = jdbc_builder
        self.seed_loader = SparkSeedLoader(spark_manager, jdbc_builder)
        
    def describe_node(self) -> str:
        return f"seed file {self.get_node_representation()}"
        
    def before_execute(self) -> None:
        fire_event(
            LogStartLine(
                description=self.describe_node(),
                index=self.node_index,
                total=self.num_nodes,
                node_info=self.node.node_info,
            )
        )
        
    def after_execute(self, result) -> None:
        level = EventLevel.ERROR if result.status == NodeStatus.Error else EventLevel.INFO
        fire_event(
            LogSeedResult(
                status=result.status,
                result_message=result.message,
                index=self.node_index,
                total=self.num_nodes,
                execution_time=result.execution_time,
                schema=self.node.schema,
                relation=self.node.alias,
                node_info=self.node.node_info,
            ),
            level=level,
        )
        
    def compile(self, manifest: Manifest):
        """Seeds don't need compilation - return node as-is."""
        return self.node
        
    def execute(self, seed: SeedNode, manifest: Manifest) -> RunResult:
        """
        Execute seed loading via Spark.
        
        Steps:
        1. Resolve target (CLI > config > default)
        2. Get CSV file path
        3. Read CSV with Spark (with type inference or explicit schema)
        4. Apply column_types overrides
        5. Write to target via JDBC
        """
        warning_collector = FederationWarningCollector()
        start_time = time.time()
        
        try:
            # Resolve target
            target_name = self._resolve_target(seed)
            target_credentials = self._get_target_credentials(target_name)
            
            # Get CSV path
            csv_path = self._get_csv_path(seed)
            
            # Get seed configuration
            seed_config = {
                'delimiter': seed.config.delimiter,
                'quote_columns': seed.config.quote_columns,
                'column_types': seed.config.column_types or {},
            }
            
            # Get target relation
            target_relation = f"{seed.schema}.{seed.alias}"
            
            # Execute via Spark
            row_count = self.seed_loader.load_seed(
                csv_path=csv_path,
                target_relation=target_relation,
                target_credentials=target_credentials,
                seed_config=seed_config,
                warning_collector=warning_collector,
                node_id=seed.unique_id,
            )
            
            elapsed = time.time() - start_time
            
            # Emit warnings
            warning_collector.emit_all()
            
            return RunResult(
                node=seed,
                status=RunStatus.Success,
                timing=[],
                thread_id=threading.current_thread().name,
                execution_time=elapsed,
                message=f"INSERT {row_count}",
                adapter_response={'rows_affected': row_count},
                failures=None,
                batch_results=None,
            )
            
        except SparkNotInstalledError as e:
            elapsed = time.time() - start_time
            return RunResult(
                node=seed,
                status=RunStatus.Error,
                timing=[],
                thread_id=threading.current_thread().name,
                execution_time=elapsed,
                message=str(e),
                adapter_response={},
                failures=1,
                batch_results=None,
            )
            
        except Exception as e:
            elapsed = time.time() - start_time
            return RunResult(
                node=seed,
                status=RunStatus.Error,
                timing=[],
                thread_id=threading.current_thread().name,
                execution_time=elapsed,
                message=f"Seed failed: {e}",
                adapter_response={},
                failures=1,
                batch_results=None,
            )
            
    def _resolve_target(self, seed: SeedNode) -> str:
        """
        Resolve target for this seed.
        Priority: CLI --target > profile default
        """
        # CLI override (highest priority)
        if hasattr(self.config, 'args') and self.config.args:
            if hasattr(self.config.args, 'target') and self.config.args.target:
                return self.config.args.target
                
        # Default from profiles.yml
        return self.config.target_name
        
    def _get_target_credentials(self, target_name: str) -> dict:
        """Get credentials for the specified target."""
        # Look up from config/profiles
        # This would use the actual profile lookup in real implementation
        creds = {}
        if hasattr(self.config, 'credentials'):
            creds = {
                'type': getattr(self.config.credentials, 'type', 'postgres'),
                'host': getattr(self.config.credentials, 'host', 'localhost'),
                'port': getattr(self.config.credentials, 'port', 5432),
                'database': getattr(self.config.credentials, 'database', ''),
                'user': getattr(self.config.credentials, 'user', ''),
                'password': getattr(self.config.credentials, 'password', ''),
                'schema': getattr(self.config.credentials, 'schema', 'public'),
            }
        return creds
        
    def _get_csv_path(self, seed: SeedNode) -> str:
        """Get the absolute path to the seed CSV file."""
        import os
        
        # Handle seeds in packages
        if seed.package_name != self.config.project_name:
            package_path = os.path.join(
                self.config.packages_install_path, 
                seed.package_name
            )
        else:
            package_path = "."
            
        path = os.path.join(
            self.config.project_root, 
            package_path, 
            seed.original_file_path
        )
        
        if not os.path.exists(path) and seed.root_path:
            path = os.path.join(seed.root_path, seed.original_file_path)
            
        return os.path.abspath(path)
```

### 6.2 SparkSeedLoader (`core/dvt/federation/seed.py`)

```python
"""
Spark-based seed loading for DVT.

This module provides the SparkSeedLoader class that handles:
- CSV parsing with Spark
- Type inference and mapping
- JDBC writes to target databases
"""

from pathlib import Path
from typing import Dict, List, Optional, Any
from dataclasses import dataclass

from dvt.federation.spark import SparkSessionManager
from dvt.federation.jdbc import JDBCBuilder
from dvt.federation.warnings import FederationWarningCollector


# User type string -> Spark type mapping
USER_TYPE_TO_SPARK = {
    # Integer types
    'integer': 'IntegerType',
    'int': 'IntegerType',
    'bigint': 'LongType',
    'long': 'LongType',
    'smallint': 'ShortType',
    'short': 'ShortType',
    'tinyint': 'ByteType',
    
    # Floating point types
    'float': 'FloatType',
    'real': 'FloatType',
    'double': 'DoubleType',
    'double precision': 'DoubleType',
    
    # Boolean
    'boolean': 'BooleanType',
    'bool': 'BooleanType',
    
    # Date/Time
    'date': 'DateType',
    'timestamp': 'TimestampType',
    'datetime': 'TimestampType',
    
    # String types
    'text': 'StringType',
    'string': 'StringType',
    'varchar': 'StringType',
    'char': 'StringType',
    'json': 'StringType',
    'jsonb': 'StringType',
    'uuid': 'StringType',
    
    # Binary
    'binary': 'BinaryType',
    'bytea': 'BinaryType',
    'blob': 'BinaryType',
}

# Spark type -> Target dialect type mapping
SPARK_TO_TARGET_TYPES = {
    'postgres': {
        'StringType': 'TEXT',
        'IntegerType': 'INTEGER',
        'LongType': 'BIGINT',
        'ShortType': 'SMALLINT',
        'ByteType': 'SMALLINT',
        'FloatType': 'REAL',
        'DoubleType': 'DOUBLE PRECISION',
        'DecimalType': 'NUMERIC',
        'BooleanType': 'BOOLEAN',
        'DateType': 'DATE',
        'TimestampType': 'TIMESTAMP',
        'BinaryType': 'BYTEA',
    },
    'snowflake': {
        'StringType': 'VARCHAR',
        'IntegerType': 'INTEGER',
        'LongType': 'BIGINT',
        'ShortType': 'SMALLINT',
        'ByteType': 'SMALLINT',
        'FloatType': 'FLOAT',
        'DoubleType': 'DOUBLE',
        'DecimalType': 'NUMBER',
        'BooleanType': 'BOOLEAN',
        'DateType': 'DATE',
        'TimestampType': 'TIMESTAMP_NTZ',
        'BinaryType': 'BINARY',
    },
    'mysql': {
        'StringType': 'TEXT',
        'IntegerType': 'INT',
        'LongType': 'BIGINT',
        'ShortType': 'SMALLINT',
        'ByteType': 'TINYINT',
        'FloatType': 'FLOAT',
        'DoubleType': 'DOUBLE',
        'DecimalType': 'DECIMAL',
        'BooleanType': 'BOOLEAN',
        'DateType': 'DATE',
        'TimestampType': 'DATETIME',
        'BinaryType': 'BLOB',
    },
    'bigquery': {
        'StringType': 'STRING',
        'IntegerType': 'INT64',
        'LongType': 'INT64',
        'ShortType': 'INT64',
        'ByteType': 'INT64',
        'FloatType': 'FLOAT64',
        'DoubleType': 'FLOAT64',
        'DecimalType': 'NUMERIC',
        'BooleanType': 'BOOL',
        'DateType': 'DATE',
        'TimestampType': 'TIMESTAMP',
        'BinaryType': 'BYTES',
    },
    'redshift': {
        'StringType': 'VARCHAR(MAX)',
        'IntegerType': 'INTEGER',
        'LongType': 'BIGINT',
        'ShortType': 'SMALLINT',
        'ByteType': 'SMALLINT',
        'FloatType': 'REAL',
        'DoubleType': 'DOUBLE PRECISION',
        'DecimalType': 'DECIMAL',
        'BooleanType': 'BOOLEAN',
        'DateType': 'DATE',
        'TimestampType': 'TIMESTAMP',
        'BinaryType': 'VARBYTE',
    },
}


@dataclass
class ColumnSchema:
    """Schema information for a single column."""
    name: str
    spark_type: str
    target_type: str
    user_specified: bool = False


class SparkSeedLoader:
    """
    Loads seed CSV files using Spark.
    
    Features:
    - Distributed CSV parsing (handles large files)
    - Intelligent type inference
    - User type overrides via column_types
    - JDBC writes to any target database
    - Full refresh (overwrite) semantics
    """
    
    def __init__(self, spark_manager: SparkSessionManager, jdbc_builder: JDBCBuilder):
        self.spark_manager = spark_manager
        self.jdbc_builder = jdbc_builder
        
    def load_seed(self,
                  csv_path: str,
                  target_relation: str,
                  target_credentials: dict,
                  seed_config: dict,
                  warning_collector: FederationWarningCollector,
                  node_id: str) -> int:
        """
        Load a seed CSV file to the target database.
        
        Args:
            csv_path: Absolute path to the CSV file
            target_relation: Target table name (schema.table)
            target_credentials: Database credentials
            seed_config: Seed configuration (delimiter, column_types, etc.)
            warning_collector: Warning collector for type conversion warnings
            node_id: Node ID for warning context
            
        Returns:
            Number of rows loaded
        """
        # Get Spark session
        spark = self.spark_manager.get_or_create()
        
        # Read CSV
        df = self._read_csv(spark, csv_path, seed_config)
        
        # Apply column_types overrides
        column_types = seed_config.get('column_types', {})
        if column_types:
            df = self._apply_column_types(df, column_types, warning_collector, node_id)
            
        # Get row count before write
        row_count = df.count()
        
        # Write to target
        target_dialect = target_credentials.get('type', 'postgres')
        self._write_to_target(df, target_relation, target_credentials, 
                              target_dialect, warning_collector, node_id)
        
        return row_count
        
    def _read_csv(self, spark, csv_path: str, seed_config: dict):
        """Read CSV file with Spark."""
        delimiter = seed_config.get('delimiter', ',')
        
        # Read with schema inference
        df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .option("delimiter", delimiter) \
            .option("quote", '"') \
            .option("escape", '"') \
            .option("multiLine", "true") \
            .option("dateFormat", "yyyy-MM-dd") \
            .option("timestampFormat", "yyyy-MM-dd HH:mm:ss") \
            .csv(csv_path)
            
        return df
        
    def _apply_column_types(self, df, column_types: Dict[str, str],
                           warning_collector: FederationWarningCollector,
                           node_id: str):
        """
        Apply user-specified column types.
        
        Args:
            df: Spark DataFrame
            column_types: Dict of column_name -> type_string
            warning_collector: Warning collector
            node_id: Node ID for warnings
            
        Returns:
            DataFrame with columns cast to specified types
        """
        from pyspark.sql import functions as F
        from pyspark.sql.types import (
            StringType, IntegerType, LongType, ShortType, ByteType,
            FloatType, DoubleType, DecimalType, BooleanType,
            DateType, TimestampType, BinaryType
        )
        
        type_map = {
            'StringType': StringType(),
            'IntegerType': IntegerType(),
            'LongType': LongType(),
            'ShortType': ShortType(),
            'ByteType': ByteType(),
            'FloatType': FloatType(),
            'DoubleType': DoubleType(),
            'BooleanType': BooleanType(),
            'DateType': DateType(),
            'TimestampType': TimestampType(),
            'BinaryType': BinaryType(),
        }
        
        for col_name, type_str in column_types.items():
            if col_name not in df.columns:
                warning_collector.add_warning(
                    f"DVT010: Column '{col_name}' specified in column_types not found in CSV",
                    node_id
                )
                continue
                
            # Parse type string
            spark_type_name = self._parse_user_type(type_str)
            
            # Handle decimal with precision
            if spark_type_name == 'DecimalType':
                precision, scale = self._parse_decimal_precision(type_str)
                spark_type = DecimalType(precision, scale)
            else:
                spark_type = type_map.get(spark_type_name)
                
            if spark_type is None:
                warning_collector.add_warning(
                    f"DVT011: Unknown type '{type_str}' for column '{col_name}', keeping inferred type",
                    node_id
                )
                continue
                
            # Cast column
            try:
                df = df.withColumn(col_name, F.col(col_name).cast(spark_type))
            except Exception as e:
                warning_collector.add_warning(
                    f"DVT012: Failed to cast column '{col_name}' to {type_str}: {e}",
                    node_id
                )
                
        return df
        
    def _parse_user_type(self, type_str: str) -> str:
        """Parse user type string to Spark type name."""
        # Normalize
        type_lower = type_str.lower().strip()
        
        # Check for decimal/numeric with precision
        if type_lower.startswith(('numeric', 'decimal')):
            return 'DecimalType'
            
        # Check for varchar/char with length
        if type_lower.startswith(('varchar', 'char')):
            return 'StringType'
            
        # Direct mapping
        return USER_TYPE_TO_SPARK.get(type_lower, 'StringType')
        
    def _parse_decimal_precision(self, type_str: str) -> tuple:
        """Parse precision and scale from decimal type string."""
        import re
        match = re.search(r'\((\d+)(?:,\s*(\d+))?\)', type_str)
        if match:
            precision = int(match.group(1))
            scale = int(match.group(2)) if match.group(2) else 0
            return precision, scale
        return 38, 10  # Default precision/scale
        
    def _write_to_target(self, df, target_relation: str, 
                         target_credentials: dict, target_dialect: str,
                         warning_collector: FederationWarningCollector,
                         node_id: str):
        """Write DataFrame to target database via JDBC."""
        # Build JDBC URL
        jdbc_url = self.jdbc_builder.build_jdbc_url(
            target_dialect, target_credentials
        )
        
        # Get JDBC properties
        jdbc_props = self.jdbc_builder.get_jdbc_properties(
            target_dialect, target_credentials
        )
        
        # Log type mappings
        self._log_type_mappings(df, target_dialect, warning_collector, node_id)
        
        # Write to target
        writer = df.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", target_relation) \
            .option("truncate", "true")  # Truncate before insert for clean overwrite
            
        # Add JDBC properties
        for key, value in jdbc_props.items():
            writer = writer.option(key, value)
            
        # Execute write with overwrite mode (full refresh)
        writer.mode("overwrite").save()
        
    def _log_type_mappings(self, df, target_dialect: str,
                          warning_collector: FederationWarningCollector,
                          node_id: str):
        """Log the type mappings being used."""
        type_mapping = SPARK_TO_TARGET_TYPES.get(target_dialect, {})
        
        for field in df.schema.fields:
            spark_type = type(field.dataType).__name__
            target_type = type_mapping.get(spark_type, 'VARCHAR')
            
            # Emit info-level message about type mapping
            # This is useful for debugging but not a warning
            pass
            
    def get_schema_preview(self, csv_path: str, seed_config: dict) -> List[ColumnSchema]:
        """
        Get a preview of the schema that would be inferred.
        Useful for debugging and schema documentation.
        """
        spark = self.spark_manager.get_or_create()
        df = self._read_csv(spark, csv_path, seed_config)
        
        column_types = seed_config.get('column_types', {})
        
        columns = []
        for field in df.schema.fields:
            spark_type = type(field.dataType).__name__
            user_specified = field.name in column_types
            
            columns.append(ColumnSchema(
                name=field.name,
                spark_type=spark_type,
                target_type='',  # Would be filled based on target dialect
                user_specified=user_specified,
            ))
            
        return columns
```

### 6.3 Modified SeedTask (`core/dvt/task/seed.py`)

Update `SeedTask` to use `SparkSeedRunner`:

```python
class SeedTask(RunTask):
    """Task for loading seed files via Spark."""
    
    def __init__(self, args, config, manifest=None):
        super().__init__(args, config, manifest)
        self._spark_seed_runner_deps = None
        
    def _get_spark_seed_dependencies(self):
        """Lazy initialization of Spark seed dependencies."""
        if self._spark_seed_runner_deps is None:
            from dvt.federation.spark import SparkSessionManager
            from dvt.federation.jdbc import JDBCBuilder
            from pathlib import Path
            
            # Get computes config
            computes_config = self._get_computes_config()
            
            # Get JDBC jars directory
            jdbc_jars_dir = Path.home() / ".dvt" / ".jdbc_jars"
            
            self._spark_seed_runner_deps = {
                'spark_manager': SparkSessionManager(computes_config, jdbc_jars_dir),
                'jdbc_builder': JDBCBuilder(),
            }
        return self._spark_seed_runner_deps
        
    def _get_computes_config(self) -> dict:
        """Get computes configuration from computes.yml."""
        # Would load from ~/.dvt/computes.yml
        # Simplified for now
        return {
            'default': {
                'type': 'spark',
                'master': 'local[*]',
                'config': {
                    'spark.driver.memory': '2g',
                }
            }
        }
        
    def raise_on_first_error(self) -> bool:
        return False

    def get_node_selector(self):
        if self.manifest is None or self.graph is None:
            raise DbtInternalError("manifest and graph must be set to get perform node selection")
        return ResourceTypeSelector(
            graph=self.graph,
            manifest=self.manifest,
            previous_state=self.previous_state,
            resource_types=[NodeType.Seed],
        )

    def get_runner_type(self, _) -> Optional[Type[BaseRunner]]:
        return SparkSeedRunner
        
    def get_runner(self, node) -> SparkSeedRunner:
        """Create a SparkSeedRunner for the node."""
        deps = self._get_spark_seed_dependencies()
        
        return SparkSeedRunner(
            config=self.config,
            adapter=self.adapter,
            node=node,
            node_index=self.node_index,
            num_nodes=self.num_nodes,
            spark_manager=deps['spark_manager'],
            jdbc_builder=deps['jdbc_builder'],
        )

    def task_end_messages(self, results) -> None:
        if self.args.show:
            self.show_tables(results)
        print_run_end_messages(results)

    def show_table(self, result):
        # Would need to update to work with Spark DataFrames
        # instead of agate tables
        pass

    def show_tables(self, results):
        for result in results:
            if result.status != RunStatus.Error:
                self.show_table(result)
```

---

## 7. Configuration

### 7.1 Existing Configuration (Preserved)

```yaml
# dbt_project.yml
seeds:
  my_project:
    +quote_columns: true
    +delimiter: ","
    +column_types:
      id: integer
      amount: numeric(10,2)
      created_at: timestamp
```

### 7.2 Potential New Configuration Options

```yaml
# dbt_project.yml
seeds:
  my_project:
    # Existing options
    +quote_columns: true
    +delimiter: ","
    +column_types:
      id: integer
      
    # Potential new options (optional enhancements)
    +encoding: "utf-8"                           # CSV encoding
    +null_value: ""                              # String to interpret as NULL
    +date_format: "yyyy-MM-dd"                   # Date parsing format
    +timestamp_format: "yyyy-MM-dd HH:mm:ss"     # Timestamp parsing format
    +multiline: false                            # Allow multiline CSV values
```

### 7.3 SeedConfig Update

```python
# core/dvt/artifacts/resources/v1/seed.py

@dataclass
class SeedConfig(NodeConfig):
    materialized: str = "seed"
    delimiter: str = ","
    quote_columns: Optional[bool] = None
    
    # New optional configurations
    encoding: str = "utf-8"
    null_value: Optional[str] = None
    date_format: str = "yyyy-MM-dd"
    timestamp_format: str = "yyyy-MM-dd HH:mm:ss"
    multiline: bool = False
```

---

## 8. CLI Usage

### Standard Usage (Same as Current)

```bash
# Seed all seeds
dvt seed

# Seed specific seed
dvt seed --select my_seed

# Show sample data after seeding
dvt seed --show
```

### Cross-Target Seeding (New Capability)

```bash
# Seed to non-default target
dvt seed --target snowflake_prod

# Seed specific seed to specific target
dvt seed --select reference_data --target bigquery_analytics

# Combine with other flags
dvt seed --select "tag:reference" --target warehouse
```

---

## 9. Error Handling

### Error Scenarios

| Scenario | Error Message | Recovery |
|----------|---------------|----------|
| Spark not installed | "PySpark is not installed. Run 'dvt sync' to install." | Run `dvt sync` |
| JDBC driver missing | "JDBC driver for {adapter} not found. Run 'dvt sync'." | Run `dvt sync` |
| CSV file not found | "Seed file not found: {path}" | Check file path |
| Invalid CSV format | "Failed to parse CSV: {error}" | Fix CSV format |
| Type cast failure | "Cannot cast column '{col}' to {type}: {error}" | Fix column_types |
| JDBC connection failed | "Failed to connect to {target}: {error}" | Check credentials |
| Table creation failed | "Failed to create table {relation}: {error}" | Check permissions |

### Warning Codes

| Code | Description |
|------|-------------|
| DVT010 | Column in column_types not found in CSV |
| DVT011 | Unknown type string, keeping inferred type |
| DVT012 | Type cast failed for column |
| DVT013 | Large file detected (>100MB), processing may take time |

---

## 10. Testing Strategy

### Unit Tests (`tests/unit/federation/test_seed.py`)

```python
class TestSparkSeedLoader:
    def test_parse_user_type_integer(self):
        """Test parsing 'integer' type string."""
        
    def test_parse_user_type_decimal_with_precision(self):
        """Test parsing 'numeric(10,2)' type string."""
        
    def test_type_mapping_postgres(self):
        """Test Spark -> Postgres type mapping."""
        
    def test_type_mapping_snowflake(self):
        """Test Spark -> Snowflake type mapping."""
        

class TestSparkSeedRunner:
    def test_resolve_target_cli_override(self):
        """Test that CLI --target overrides default."""
        
    def test_resolve_target_default(self):
        """Test default target resolution."""
        
    def test_csv_path_resolution(self):
        """Test CSV file path resolution."""
```

### Integration Tests (`tests/functional/federation/test_seed.py`)

```python
class TestSparkSeedIntegration:
    def test_seed_to_postgres(self):
        """Test seeding CSV to Postgres."""
        
    def test_seed_to_snowflake(self):
        """Test seeding CSV to Snowflake."""
        
    def test_seed_with_column_types(self):
        """Test seeding with explicit column_types."""
        
    def test_seed_large_file(self):
        """Test seeding a large CSV file (100MB+)."""
        
    def test_seed_cross_target(self):
        """Test seeding to non-default target."""
```

---

## 11. Migration Guide

### For Existing DVT Users

The new Spark-based seed is fully backward compatible:

1. **No changes required** to existing seed configurations
2. **Same CLI commands** work as before
3. **Same `column_types` syntax** is supported
4. **Better performance** for large files automatically

### Prerequisites

```bash
# Ensure Spark and JDBC drivers are installed
dvt sync
```

### Potential Differences to Be Aware Of

| Aspect | Old (agate) | New (Spark) |
|--------|-------------|-------------|
| Type inference | Limited (6 types) | More accurate |
| Boolean parsing | Only "true"/"false" | Also "1"/"0", "yes"/"no" |
| Date formats | Fixed formats | Configurable |
| Large files | Memory errors | Handles smoothly |
| Cross-target | Not supported | Supported |

---

## 12. Implementation Timeline

```
Week 1: Core Implementation
|-- Day 1-2: SparkSeedLoader class
|-- Day 3-4: SparkSeedRunner class
+-- Day 5: SeedTask modifications

Week 2: Type System & Testing
|-- Day 1-2: Type mapping system
|-- Day 3-4: Unit tests
+-- Day 5: Integration tests

Week 3: Polish & Documentation
|-- Day 1-2: Error handling
|-- Day 3-4: Cross-target testing
+-- Day 5: Documentation
```

---

## 13. Dependencies

### External Dependencies

| Package | Version | Purpose |
|---------|---------|---------|
| `pyspark` | `>=3.5.0` | CSV parsing, DataFrame operations |
| JDBC drivers | Various | Database connectivity |

### Internal Dependencies

| Component | Depends On |
|-----------|------------|
| `SparkSeedLoader` | `SparkSessionManager`, `JDBCBuilder` |
| `SparkSeedRunner` | `SparkSeedLoader`, `FederationWarningCollector` |
| `SeedTask` | `SparkSeedRunner` |

### Reused from Federation Plan

- `SparkSessionManager` (`federation/spark.py`)
- `JDBCBuilder` (`federation/jdbc.py`)
- `FederationWarningCollector` (`federation/warnings.py`)

---

## 14. Success Criteria

The Spark-enhanced seed is complete when:

**Functional:**
- [ ] All seeds use Spark for CSV loading
- [ ] Type inference works correctly
- [ ] `column_types` overrides work as before
- [ ] Cross-target seeding works via CLI
- [ ] Large files (100MB+) load without memory errors

**Compatibility:**
- [ ] Existing seed configurations work unchanged
- [ ] Existing CLI commands work unchanged
- [ ] Same output table schema as before (for simple cases)

**Performance:**
- [ ] 10x faster for files >10MB
- [ ] No memory errors for large files
- [ ] Parallel writes utilized

**Testing:**
- [ ] Unit tests for type mapping
- [ ] Integration tests for each supported database
- [ ] Large file test (100MB+ CSV)

---

## 15. References

- Federation Execution Plan: `hesham_mind/federation_execution_plan.md`
- DVT Rules: `hesham_mind/dvt_rules.md`
- Current Seed Implementation: `core/dvt/task/seed.py`
- Spark CSV Documentation: https://spark.apache.org/docs/latest/sql-data-sources-csv.html
- Spark JDBC Documentation: https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html
