# Spark Optimizations

## Overview

This document covers Spark-level optimizations for DVT federation: partitioned reading, retry strategies, compression, and Spark configuration tuning.

### The Problem

1. **Single-threaded JDBC reads**: Default Spark JDBC reads entire tables through one connection
2. **No retry on failure**: Network blip at 99% means starting over
3. **Memory pressure**: Large result sets overwhelm driver memory
4. **Throttling**: Cloud databases rate-limit aggressive connections
5. **Inefficient encoding**: Data transferred uncompressed

### Goals

- **Parallel transfers**: Partition reads across multiple connections (10-50x speedup)
- **Resilient transfers**: Retry failed partitions, not entire transfers
- **Efficient encoding**: Compress data in transit (2-5x reduction)
- **Optimal Spark config**: Tune memory, parallelism, and shuffle settings

---

## Selected Strategies

| Strategy | Benefit | Complexity |
|----------|---------|------------|
| Partitioned reading | 10-50x speedup via parallelism | Medium |
| Retry with backoff | Resilience to transient failures | Low |
| Compression | 2-5x reduction in transfer size | Low |

### Not Selected

| Strategy | Reason for Exclusion |
|----------|---------------------|
| Intermediate caching | Complexity outweighs benefit; Spark handles spill-to-disk |
| Checkpointing | Spark's internal checkpointing sufficient for our use case |
| Delta/Iceberg staging | Future enhancement; requires additional infrastructure |

---

## 1. Partitioned Reading Strategy

### Concept

Instead of reading an entire table through one JDBC connection, partition the read across multiple parallel connections. Spark JDBC supports this natively via:

```python
spark.read.jdbc(
    url=jdbc_url,
    table=table_name,
    column="id",           # Partition column
    lowerBound=1,          # Min value
    upperBound=1000000,    # Max value
    numPartitions=10,      # Parallel connections
    properties=props
)
```

### Auto-Detect Partition Column

Not all tables have obvious partition columns. DVT will auto-detect using this priority:

1. **Primary key** (if single-column, numeric or date)
2. **Unique index column** (numeric or date)
3. **`created_at` / `updated_at`** (common audit columns)
4. **Any indexed numeric column**
5. **Row number fallback** (computed, less efficient)

### Dynamic Partition Count

Formula based on estimated table size:

```
numPartitions = min(
    max(
        ceil(estimated_rows / rows_per_partition),
        min_partitions
    ),
    max_partitions
)
```

Default configuration:
- `rows_per_partition`: 500,000
- `min_partitions`: 4
- `max_partitions`: 200

### Implementation

```python
# core/dvt/federation/transfer/partition.py
"""Partitioned reading strategy for JDBC transfers."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Optional

from pyspark.sql import SparkSession, DataFrame


class PartitionColumnType(Enum):
    """Supported partition column types."""
    NUMERIC = "numeric"      # INT, BIGINT, DECIMAL
    DATE = "date"            # DATE
    TIMESTAMP = "timestamp"  # TIMESTAMP, DATETIME
    

@dataclass
class PartitionColumn:
    """Detected partition column information."""
    name: str
    column_type: PartitionColumnType
    min_value: Any
    max_value: Any
    distinct_count: Optional[int] = None
    is_primary_key: bool = False
    is_indexed: bool = False
    
    @property
    def range_size(self) -> int:
        """Calculate the range size for partitioning."""
        if self.column_type == PartitionColumnType.NUMERIC:
            return int(self.max_value - self.min_value)
        elif self.column_type in (PartitionColumnType.DATE, PartitionColumnType.TIMESTAMP):
            # Convert to epoch seconds for range calculation
            delta = self.max_value - self.min_value
            return int(delta.total_seconds())
        return 0


@dataclass
class PartitionConfig:
    """Configuration for partitioned reading."""
    rows_per_partition: int = 500_000
    min_partitions: int = 4
    max_partitions: int = 200
    fetch_size: int = 10_000
    
    # Timeouts
    connection_timeout_seconds: int = 30
    query_timeout_seconds: int = 3600  # 1 hour per partition


class PartitionColumnDetector:
    """Detects optimal partition column for a table."""
    
    # Common audit column names (case-insensitive)
    AUDIT_COLUMNS = {
        "created_at", "createdat", "created_date", "createddate",
        "updated_at", "updatedat", "updated_date", "updateddate",
        "modified_at", "modifiedat", "modified_date", "modifieddate",
        "inserted_at", "insertedat", "insert_date", "insertdate",
    }
    
    # Numeric types by dialect
    NUMERIC_TYPES = {
        "postgres": {"integer", "bigint", "smallint", "serial", "bigserial", "numeric", "decimal"},
        "snowflake": {"number", "integer", "bigint", "smallint", "decimal", "numeric"},
        "redshift": {"integer", "bigint", "smallint", "decimal", "numeric"},
        "bigquery": {"int64", "numeric", "bignumeric"},
        "mysql": {"int", "bigint", "smallint", "mediumint", "decimal", "numeric"},
        "spark": {"int", "bigint", "smallint", "decimal", "long"},
    }
    
    DATE_TYPES = {
        "postgres": {"date", "timestamp", "timestamptz", "timestamp with time zone"},
        "snowflake": {"date", "timestamp", "timestamp_ntz", "timestamp_ltz", "timestamp_tz"},
        "redshift": {"date", "timestamp", "timestamptz"},
        "bigquery": {"date", "datetime", "timestamp"},
        "mysql": {"date", "datetime", "timestamp"},
        "spark": {"date", "timestamp"},
    }
    
    def __init__(self, spark: SparkSession, jdbc_url: str, properties: dict[str, str]):
        self.spark = spark
        self.jdbc_url = jdbc_url
        self.properties = properties
        self._dialect = self._detect_dialect(jdbc_url)
    
    def _detect_dialect(self, jdbc_url: str) -> str:
        """Detect database dialect from JDBC URL."""
        url_lower = jdbc_url.lower()
        if "postgresql" in url_lower or "postgres" in url_lower:
            return "postgres"
        elif "snowflake" in url_lower:
            return "snowflake"
        elif "redshift" in url_lower:
            return "redshift"
        elif "bigquery" in url_lower:
            return "bigquery"
        elif "mysql" in url_lower:
            return "mysql"
        else:
            return "spark"  # Default fallback
    
    def detect(
        self,
        schema: str,
        table: str,
        prefer_column: Optional[str] = None,
    ) -> Optional[PartitionColumn]:
        """
        Detect the optimal partition column for a table.
        
        Args:
            schema: Database schema name
            table: Table name
            prefer_column: User-specified preferred column (if any)
            
        Returns:
            PartitionColumn if suitable column found, None otherwise
        """
        # If user specified a column, validate and use it
        if prefer_column:
            return self._validate_column(schema, table, prefer_column)
        
        # Try detection strategies in order
        strategies = [
            self._detect_primary_key,
            self._detect_unique_index,
            self._detect_audit_column,
            self._detect_any_numeric_indexed,
            self._detect_any_numeric,
        ]
        
        for strategy in strategies:
            result = strategy(schema, table)
            if result:
                return result
        
        return None
    
    def _validate_column(
        self,
        schema: str,
        table: str,
        column: str,
    ) -> Optional[PartitionColumn]:
        """Validate user-specified partition column."""
        query = self._build_column_stats_query(schema, table, column)
        
        try:
            df = self.spark.read.jdbc(
                url=self.jdbc_url,
                table=f"({query}) as stats",
                properties=self.properties
            )
            row = df.first()
            if row:
                return PartitionColumn(
                    name=column,
                    column_type=self._infer_column_type(row["data_type"]),
                    min_value=row["min_val"],
                    max_value=row["max_val"],
                    distinct_count=row.get("distinct_count"),
                )
        except Exception:
            pass
        return None
    
    def _detect_primary_key(self, schema: str, table: str) -> Optional[PartitionColumn]:
        """Detect single-column numeric/date primary key."""
        # Query varies by dialect
        if self._dialect == "postgres":
            query = f"""
                SELECT a.attname as column_name, format_type(a.atttypid, a.atttypmod) as data_type
                FROM pg_index i
                JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                WHERE i.indrelid = '{schema}.{table}'::regclass
                AND i.indisprimary
                AND array_length(i.indkey, 1) = 1
            """
        elif self._dialect == "snowflake":
            query = f"""
                SELECT column_name, data_type
                FROM {schema}.information_schema.columns
                WHERE table_schema = '{schema.upper()}'
                AND table_name = '{table.upper()}'
                AND is_identity = 'YES'
            """
        else:
            return None
        
        try:
            df = self.spark.read.jdbc(
                url=self.jdbc_url,
                table=f"({query}) as pk_info",
                properties=self.properties
            )
            row = df.first()
            if row and self._is_partitionable_type(row["data_type"]):
                return self._get_column_stats(schema, table, row["column_name"], is_primary_key=True)
        except Exception:
            pass
        return None
    
    def _detect_unique_index(self, schema: str, table: str) -> Optional[PartitionColumn]:
        """Detect single-column unique index with numeric/date type."""
        # Implementation similar to primary key detection
        # Queries information_schema for unique indexes
        pass
    
    def _detect_audit_column(self, schema: str, table: str) -> Optional[PartitionColumn]:
        """Detect common audit columns (created_at, updated_at, etc.)."""
        if self._dialect == "postgres":
            query = f"""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = '{schema}'
                AND table_name = '{table}'
                AND lower(column_name) IN ({','.join(f"'{c}'" for c in self.AUDIT_COLUMNS)})
            """
        elif self._dialect == "snowflake":
            query = f"""
                SELECT column_name, data_type
                FROM {schema}.information_schema.columns
                WHERE table_schema = '{schema.upper()}'
                AND table_name = '{table.upper()}'
                AND lower(column_name) IN ({','.join(f"'{c}'" for c in self.AUDIT_COLUMNS)})
            """
        else:
            return None
        
        try:
            df = self.spark.read.jdbc(
                url=self.jdbc_url,
                table=f"({query}) as audit_cols",
                properties=self.properties
            )
            row = df.first()
            if row:
                return self._get_column_stats(schema, table, row["column_name"])
        except Exception:
            pass
        return None
    
    def _detect_any_numeric_indexed(self, schema: str, table: str) -> Optional[PartitionColumn]:
        """Detect any indexed numeric column."""
        # Query for indexed columns, filter to numeric types
        pass
    
    def _detect_any_numeric(self, schema: str, table: str) -> Optional[PartitionColumn]:
        """Fallback: detect any numeric column (prefer larger range)."""
        pass
    
    def _is_partitionable_type(self, data_type: str) -> bool:
        """Check if column type is suitable for partitioning."""
        data_type_lower = data_type.lower()
        numeric_types = self.NUMERIC_TYPES.get(self._dialect, set())
        date_types = self.DATE_TYPES.get(self._dialect, set())
        
        for t in numeric_types | date_types:
            if t in data_type_lower:
                return True
        return False
    
    def _infer_column_type(self, data_type: str) -> PartitionColumnType:
        """Infer partition column type from database type."""
        data_type_lower = data_type.lower()
        date_types = self.DATE_TYPES.get(self._dialect, set())
        
        for t in date_types:
            if t in data_type_lower:
                if "timestamp" in data_type_lower or "datetime" in data_type_lower:
                    return PartitionColumnType.TIMESTAMP
                return PartitionColumnType.DATE
        
        return PartitionColumnType.NUMERIC
    
    def _get_column_stats(
        self,
        schema: str,
        table: str,
        column: str,
        is_primary_key: bool = False,
        is_indexed: bool = False,
    ) -> Optional[PartitionColumn]:
        """Get min/max/distinct statistics for a column."""
        query = f"""
            SELECT 
                MIN({column}) as min_val,
                MAX({column}) as max_val,
                COUNT(DISTINCT {column}) as distinct_count
            FROM {schema}.{table}
        """
        
        try:
            df = self.spark.read.jdbc(
                url=self.jdbc_url,
                table=f"({query}) as stats",
                properties=self.properties
            )
            row = df.first()
            if row and row["min_val"] is not None and row["max_val"] is not None:
                # Get column type
                col_type = self._get_column_type(schema, table, column)
                return PartitionColumn(
                    name=column,
                    column_type=col_type,
                    min_value=row["min_val"],
                    max_value=row["max_val"],
                    distinct_count=row["distinct_count"],
                    is_primary_key=is_primary_key,
                    is_indexed=is_indexed,
                )
        except Exception:
            pass
        return None
    
    def _get_column_type(self, schema: str, table: str, column: str) -> PartitionColumnType:
        """Get the type of a specific column."""
        if self._dialect == "postgres":
            query = f"""
                SELECT data_type
                FROM information_schema.columns
                WHERE table_schema = '{schema}'
                AND table_name = '{table}'
                AND column_name = '{column}'
            """
        else:
            # Generic fallback
            return PartitionColumnType.NUMERIC
        
        try:
            df = self.spark.read.jdbc(
                url=self.jdbc_url,
                table=f"({query}) as col_type",
                properties=self.properties
            )
            row = df.first()
            if row:
                return self._infer_column_type(row["data_type"])
        except Exception:
            pass
        return PartitionColumnType.NUMERIC
    
    def _build_column_stats_query(self, schema: str, table: str, column: str) -> str:
        """Build query to get column statistics."""
        return f"""
            SELECT 
                '{column}' as column_name,
                MIN({column}) as min_val,
                MAX({column}) as max_val,
                COUNT(DISTINCT {column}) as distinct_count,
                data_type
            FROM {schema}.{table}
            CROSS JOIN (
                SELECT data_type 
                FROM information_schema.columns 
                WHERE table_schema = '{schema}' 
                AND table_name = '{table}' 
                AND column_name = '{column}'
            ) as meta
            GROUP BY data_type
        """


class PartitionCalculator:
    """Calculates optimal partition boundaries."""
    
    def __init__(self, config: PartitionConfig):
        self.config = config
    
    def calculate(
        self,
        partition_column: PartitionColumn,
        estimated_rows: Optional[int] = None,
    ) -> tuple[int, Any, Any]:
        """
        Calculate partition parameters.
        
        Args:
            partition_column: Detected partition column info
            estimated_rows: Estimated row count (if known)
            
        Returns:
            Tuple of (num_partitions, lower_bound, upper_bound)
        """
        # Calculate number of partitions
        if estimated_rows:
            num_partitions = max(
                self.config.min_partitions,
                min(
                    (estimated_rows + self.config.rows_per_partition - 1) // self.config.rows_per_partition,
                    self.config.max_partitions
                )
            )
        else:
            # Use distinct count as proxy
            if partition_column.distinct_count:
                num_partitions = max(
                    self.config.min_partitions,
                    min(
                        partition_column.distinct_count // 100_000,
                        self.config.max_partitions
                    )
                )
            else:
                num_partitions = self.config.min_partitions
        
        # Handle date/timestamp bounds
        lower_bound = partition_column.min_value
        upper_bound = partition_column.max_value
        
        if partition_column.column_type in (PartitionColumnType.DATE, PartitionColumnType.TIMESTAMP):
            # Convert to epoch for Spark JDBC partitioning
            if hasattr(lower_bound, 'timestamp'):
                lower_bound = int(lower_bound.timestamp())
                upper_bound = int(upper_bound.timestamp())
            elif hasattr(lower_bound, 'toordinal'):
                # Date type
                lower_bound = lower_bound.toordinal()
                upper_bound = upper_bound.toordinal()
        
        return int(num_partitions), lower_bound, upper_bound


class PartitionedReader:
    """Reads data using partitioned JDBC strategy."""
    
    def __init__(
        self,
        spark: SparkSession,
        jdbc_url: str,
        properties: dict[str, str],
        config: Optional[PartitionConfig] = None,
    ):
        self.spark = spark
        self.jdbc_url = jdbc_url
        self.properties = properties
        self.config = config or PartitionConfig()
        self.detector = PartitionColumnDetector(spark, jdbc_url, properties)
        self.calculator = PartitionCalculator(self.config)
    
    def read(
        self,
        schema: str,
        table: str,
        query: Optional[str] = None,
        partition_column: Optional[str] = None,
        predicates: Optional[list[str]] = None,
    ) -> DataFrame:
        """
        Read data with automatic partitioning.
        
        Args:
            schema: Database schema
            table: Table name
            query: Optional custom query (overrides table)
            partition_column: User-specified partition column
            predicates: Optional WHERE predicates for pushdown
            
        Returns:
            Spark DataFrame
        """
        # Build read properties
        read_props = self.properties.copy()
        read_props["fetchsize"] = str(self.config.fetch_size)
        
        # Determine source (table or query)
        source = query if query else f"{schema}.{table}"
        
        # Try to detect partition column
        detected = self.detector.detect(schema, table, partition_column)
        
        if detected:
            # Calculate partitions
            num_partitions, lower, upper = self.calculator.calculate(detected)
            
            # Build partitioned read
            reader = self.spark.read.format("jdbc").options(
                url=self.jdbc_url,
                dbtable=source if not query else f"({query}) as subq",
                partitionColumn=detected.name,
                lowerBound=str(lower),
                upperBound=str(upper),
                numPartitions=str(num_partitions),
                **read_props
            )
        else:
            # Fallback to non-partitioned read
            reader = self.spark.read.format("jdbc").options(
                url=self.jdbc_url,
                dbtable=source if not query else f"({query}) as subq",
                **read_props
            )
        
        # Apply predicates if provided
        df = reader.load()
        if predicates:
            for predicate in predicates:
                df = df.filter(predicate)
        
        return df
```

---

## 2. Retry Strategy

### Concept

Network transfers fail. Instead of restarting from zero, DVT retries failed partitions with exponential backoff.

### Error Classification

| Category | Examples | Action |
|----------|----------|--------|
| Retryable | Connection timeout, socket reset, throttling (429), temporary unavailable (503) | Retry with backoff |
| Non-retryable | Auth failure (401), permission denied (403), schema mismatch, syntax error | Fail immediately |

### Backoff Formula

```
wait_time = min(base_delay * (2 ^ attempt) + jitter, max_delay)
jitter = random(0, base_delay * 0.1)
```

Default configuration:
- `base_delay`: 1 second
- `max_delay`: 60 seconds
- `max_retries`: 5

### Implementation

```python
# core/dvt/federation/transfer/retry.py
"""Retry strategy with exponential backoff for transfers."""

from __future__ import annotations

import random
import time
from dataclasses import dataclass, field
from enum import Enum
from functools import wraps
from typing import Any, Callable, Optional, TypeVar, ParamSpec
import re

from dvt.events.types import EventLevel
from dvt.events.functions import fire_event


P = ParamSpec("P")
T = TypeVar("T")


class ErrorCategory(Enum):
    """Classification of transfer errors."""
    RETRYABLE = "retryable"
    NON_RETRYABLE = "non_retryable"
    UNKNOWN = "unknown"


@dataclass
class RetryConfig:
    """Configuration for retry behavior."""
    max_retries: int = 5
    base_delay_seconds: float = 1.0
    max_delay_seconds: float = 60.0
    jitter_factor: float = 0.1
    
    # Retry on unknown errors (conservative default: yes)
    retry_on_unknown: bool = True


@dataclass
class RetryState:
    """Tracks retry state for an operation."""
    attempt: int = 0
    total_wait_time: float = 0.0
    last_error: Optional[Exception] = None
    errors: list[Exception] = field(default_factory=list)
    
    @property
    def has_failed(self) -> bool:
        return self.last_error is not None


class TransferErrorClassifier:
    """Classifies transfer errors as retryable or not."""
    
    # Retryable error patterns (case-insensitive regex)
    RETRYABLE_PATTERNS = [
        # Connection errors
        r"connection\s*(reset|refused|timed?\s*out|closed)",
        r"socket\s*(timeout|error|reset)",
        r"network\s*(unreachable|error)",
        r"broken\s*pipe",
        r"eof\s*reached",
        
        # HTTP status codes
        r"http\s*(429|500|502|503|504)",
        r"too\s*many\s*requests",
        r"service\s*unavailable",
        r"gateway\s*(timeout|error)",
        r"internal\s*server\s*error",
        
        # Database throttling
        r"throttl(ed|ing)",
        r"rate\s*limit",
        r"quota\s*exceeded",
        r"concurrent\s*(query|connection)\s*limit",
        r"too\s*many\s*(connections|queries)",
        
        # Transient errors
        r"temporary\s*(failure|error)",
        r"retry\s*later",
        r"deadlock",
        r"lock\s*timeout",
        r"statement\s*timeout",
    ]
    
    # Non-retryable error patterns
    NON_RETRYABLE_PATTERNS = [
        # Authentication/Authorization
        r"(authentication|login)\s*(failed|error|denied)",
        r"(access|permission)\s*denied",
        r"unauthorized",
        r"forbidden",
        r"invalid\s*(credentials|password|token)",
        r"http\s*(401|403)",
        
        # Schema/Syntax errors
        r"(table|column|schema|relation)\s*(not\s*found|does\s*not\s*exist)",
        r"syntax\s*error",
        r"invalid\s*(sql|query|identifier)",
        r"type\s*mismatch",
        r"data\s*type\s*(error|mismatch)",
        
        # Resource errors (won't help to retry)
        r"out\s*of\s*memory",
        r"disk\s*(full|space)",
        r"query\s*(cancelled|canceled)",
    ]
    
    def __init__(self):
        self._retryable_compiled = [
            re.compile(p, re.IGNORECASE) for p in self.RETRYABLE_PATTERNS
        ]
        self._non_retryable_compiled = [
            re.compile(p, re.IGNORECASE) for p in self.NON_RETRYABLE_PATTERNS
        ]
    
    def classify(self, error: Exception) -> ErrorCategory:
        """
        Classify an error as retryable or not.
        
        Args:
            error: The exception to classify
            
        Returns:
            ErrorCategory indicating whether to retry
        """
        error_str = str(error).lower()
        error_type = type(error).__name__.lower()
        full_text = f"{error_type}: {error_str}"
        
        # Check non-retryable first (more specific)
        for pattern in self._non_retryable_compiled:
            if pattern.search(full_text):
                return ErrorCategory.NON_RETRYABLE
        
        # Check retryable patterns
        for pattern in self._retryable_compiled:
            if pattern.search(full_text):
                return ErrorCategory.RETRYABLE
        
        # Check exception type hierarchy
        if self._is_connection_error(error):
            return ErrorCategory.RETRYABLE
        
        return ErrorCategory.UNKNOWN
    
    def _is_connection_error(self, error: Exception) -> bool:
        """Check if error is a connection-related exception type."""
        connection_types = (
            "ConnectionError",
            "TimeoutError", 
            "BrokenPipeError",
            "ConnectionResetError",
            "ConnectionRefusedError",
            "ConnectionAbortedError",
            "OSError",  # Often wraps socket errors
        )
        
        error_type = type(error).__name__
        for conn_type in connection_types:
            if conn_type in error_type:
                return True
        
        # Check cause chain
        cause = error.__cause__
        while cause:
            if type(cause).__name__ in connection_types:
                return True
            cause = cause.__cause__
        
        return False


class RetryHandler:
    """Handles retry logic with exponential backoff."""
    
    def __init__(
        self,
        config: Optional[RetryConfig] = None,
        classifier: Optional[TransferErrorClassifier] = None,
    ):
        self.config = config or RetryConfig()
        self.classifier = classifier or TransferErrorClassifier()
    
    def calculate_delay(self, attempt: int) -> float:
        """
        Calculate delay for a retry attempt.
        
        Args:
            attempt: Current attempt number (0-based)
            
        Returns:
            Delay in seconds
        """
        # Exponential backoff
        delay = self.config.base_delay_seconds * (2 ** attempt)
        
        # Add jitter
        jitter_range = self.config.base_delay_seconds * self.config.jitter_factor
        jitter = random.uniform(0, jitter_range)
        delay += jitter
        
        # Cap at max delay
        return min(delay, self.config.max_delay_seconds)
    
    def should_retry(self, error: Exception, state: RetryState) -> bool:
        """
        Determine if an operation should be retried.
        
        Args:
            error: The exception that occurred
            state: Current retry state
            
        Returns:
            True if should retry, False otherwise
        """
        # Check attempt limit
        if state.attempt >= self.config.max_retries:
            return False
        
        # Classify error
        category = self.classifier.classify(error)
        
        if category == ErrorCategory.NON_RETRYABLE:
            return False
        elif category == ErrorCategory.RETRYABLE:
            return True
        else:  # UNKNOWN
            return self.config.retry_on_unknown
    
    def execute_with_retry(
        self,
        operation: Callable[P, T],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> T:
        """
        Execute an operation with retry logic.
        
        Args:
            operation: Function to execute
            *args: Positional arguments for operation
            **kwargs: Keyword arguments for operation
            
        Returns:
            Result of successful operation
            
        Raises:
            Last exception if all retries exhausted
        """
        state = RetryState()
        
        while True:
            try:
                return operation(*args, **kwargs)
            except Exception as e:
                state.attempt += 1
                state.last_error = e
                state.errors.append(e)
                
                if not self.should_retry(e, state):
                    raise
                
                delay = self.calculate_delay(state.attempt - 1)
                state.total_wait_time += delay
                
                fire_event(
                    msg=f"Transfer failed (attempt {state.attempt}/{self.config.max_retries}), "
                        f"retrying in {delay:.1f}s: {e}",
                    level=EventLevel.WARN,
                )
                
                time.sleep(delay)
    
    def retry_decorator(
        self,
        max_retries: Optional[int] = None,
    ) -> Callable[[Callable[P, T]], Callable[P, T]]:
        """
        Decorator for adding retry logic to functions.
        
        Args:
            max_retries: Override max retries for this function
            
        Returns:
            Decorated function with retry logic
        """
        def decorator(func: Callable[P, T]) -> Callable[P, T]:
            @wraps(func)
            def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
                config = RetryConfig(
                    max_retries=max_retries or self.config.max_retries,
                    base_delay_seconds=self.config.base_delay_seconds,
                    max_delay_seconds=self.config.max_delay_seconds,
                    jitter_factor=self.config.jitter_factor,
                    retry_on_unknown=self.config.retry_on_unknown,
                )
                handler = RetryHandler(config, self.classifier)
                return handler.execute_with_retry(func, *args, **kwargs)
            return wrapper
        return decorator


# Convenience decorator with default config
retry_handler = RetryHandler()

def with_retry(max_retries: int = 5):
    """Decorator to add retry logic with default configuration."""
    return retry_handler.retry_decorator(max_retries)
```

---

## 3. Compression

### Multi-Layer Compression

DVT enables compression at multiple layers:

| Layer | Setting | Default |
|-------|---------|---------|
| JDBC fetch | `compress=true` in URL params | Enabled |
| Native connector | Format-specific (Parquet, Avro) | Snappy |
| Spark shuffle | `spark.shuffle.compress` | Enabled |

### Implementation

```python
# core/dvt/federation/transfer/compression.py
"""Compression configuration for data transfers."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Optional

from pyspark.sql import SparkSession


class CompressionCodec(Enum):
    """Available compression codecs."""
    NONE = "none"
    SNAPPY = "snappy"
    GZIP = "gzip"
    LZ4 = "lz4"
    ZSTD = "zstd"


@dataclass
class CompressionConfig:
    """Configuration for transfer compression."""
    
    # JDBC compression (connection-level)
    jdbc_compression: bool = True
    
    # Spark shuffle compression
    shuffle_compression: bool = True
    shuffle_codec: CompressionCodec = CompressionCodec.LZ4
    
    # Output file compression (for native connectors)
    output_codec: CompressionCodec = CompressionCodec.SNAPPY
    
    # Parquet-specific settings
    parquet_compression: CompressionCodec = CompressionCodec.SNAPPY
    parquet_block_size: int = 128 * 1024 * 1024  # 128MB


class CompressionManager:
    """Manages compression settings for transfers."""
    
    def __init__(self, config: Optional[CompressionConfig] = None):
        self.config = config or CompressionConfig()
    
    def configure_spark(self, spark: SparkSession):
        """Apply compression settings to Spark session."""
        conf = spark.conf
        
        # Shuffle compression
        conf.set("spark.shuffle.compress", str(self.config.shuffle_compression).lower())
        if self.config.shuffle_compression:
            conf.set("spark.io.compression.codec", self.config.shuffle_codec.value)
        
        # Parquet settings
        conf.set("spark.sql.parquet.compression.codec", self.config.parquet_compression.value)
        conf.set("parquet.block.size", str(self.config.parquet_block_size))
        
        # Broadcast compression
        conf.set("spark.broadcast.compress", "true")
        
        # RDD compression
        conf.set("spark.rdd.compress", "true")
    
    def get_jdbc_params(self, adapter_type: str) -> dict[str, str]:
        """Get JDBC URL parameters for compression."""
        if not self.config.jdbc_compression:
            return {}
        
        # Compression params vary by database
        params = {
            "postgres": {"compress": "true"},
            "mysql": {"useCompression": "true"},
            "sqlserver": {"compress": "true"},
            "snowflake": {},  # Compression handled at protocol level
            "redshift": {},
            "bigquery": {},
        }
        
        return params.get(adapter_type, {})
```

---

## 4. Spark Configuration Tuning

### Memory Settings

```python
# Recommended Spark configuration for federation workloads

SPARK_FEDERATION_CONFIG = {
    # Memory
    "spark.driver.memory": "4g",
    "spark.executor.memory": "4g",
    "spark.memory.fraction": "0.8",
    "spark.memory.storageFraction": "0.3",
    
    # Parallelism
    "spark.default.parallelism": "200",
    "spark.sql.shuffle.partitions": "200",
    
    # Network
    "spark.network.timeout": "600s",
    "spark.executor.heartbeatInterval": "60s",
    
    # Compression
    "spark.shuffle.compress": "true",
    "spark.io.compression.codec": "lz4",
    "spark.broadcast.compress": "true",
    "spark.rdd.compress": "true",
    
    # Parquet
    "spark.sql.parquet.compression.codec": "snappy",
    "parquet.block.size": str(128 * 1024 * 1024),
    
    # SQL
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.adaptive.skewJoin.enabled": "true",
}
```

### Dynamic Allocation

```python
# Enable dynamic allocation for variable workloads

DYNAMIC_ALLOCATION_CONFIG = {
    "spark.dynamicAllocation.enabled": "true",
    "spark.dynamicAllocation.minExecutors": "1",
    "spark.dynamicAllocation.maxExecutors": "10",
    "spark.dynamicAllocation.initialExecutors": "2",
    "spark.dynamicAllocation.executorIdleTimeout": "60s",
    "spark.dynamicAllocation.schedulerBacklogTimeout": "1s",
}
```

### JDBC-Specific Settings

```python
# JDBC optimization settings

JDBC_CONFIG = {
    # Fetch size (rows per network round-trip)
    "fetchsize": "10000",
    
    # Batch size for writes
    "batchsize": "10000",
    
    # Connection pool
    "numPartitions": "10",  # Default, overridden by PartitionCalculator
    
    # Timeouts
    "connectTimeout": "30",
    "socketTimeout": "3600",
}
```

---

## Module Architecture

```
core/dvt/federation/
├── __init__.py
└── transfer/
    ├── __init__.py
    ├── partition.py          # PartitionColumnDetector, PartitionedReader
    ├── retry.py              # RetryHandler, error classification
    └── compression.py        # CompressionConfig, CompressionManager
```

---

## Testing Strategy

### Unit Tests

```python
# tests/unit/federation/transfer/test_partition.py

import pytest
from dvt.federation.transfer.partition import (
    PartitionColumnType,
    PartitionColumn,
    PartitionConfig,
    PartitionCalculator,
)


class TestPartitionCalculator:
    """Tests for partition calculation logic."""
    
    def test_calculate_partitions_from_row_count(self):
        """Verify partition count scales with row count."""
        config = PartitionConfig(rows_per_partition=100_000)
        calculator = PartitionCalculator(config)
        
        column = PartitionColumn(
            name="id",
            column_type=PartitionColumnType.NUMERIC,
            min_value=1,
            max_value=1_000_000,
        )
        
        num_partitions, _, _ = calculator.calculate(column, estimated_rows=500_000)
        
        assert num_partitions == 5  # 500k / 100k = 5
    
    def test_respects_min_partitions(self):
        """Verify minimum partition count is respected."""
        config = PartitionConfig(min_partitions=4, rows_per_partition=1_000_000)
        calculator = PartitionCalculator(config)
        
        column = PartitionColumn(
            name="id",
            column_type=PartitionColumnType.NUMERIC,
            min_value=1,
            max_value=100,
        )
        
        num_partitions, _, _ = calculator.calculate(column, estimated_rows=100)
        
        assert num_partitions == 4  # Min partitions
    
    def test_respects_max_partitions(self):
        """Verify maximum partition count is respected."""
        config = PartitionConfig(max_partitions=50, rows_per_partition=10_000)
        calculator = PartitionCalculator(config)
        
        column = PartitionColumn(
            name="id",
            column_type=PartitionColumnType.NUMERIC,
            min_value=1,
            max_value=100_000_000,
        )
        
        num_partitions, _, _ = calculator.calculate(column, estimated_rows=10_000_000)
        
        assert num_partitions == 50  # Max partitions


# tests/unit/federation/transfer/test_retry.py

import pytest
from dvt.federation.transfer.retry import (
    ErrorCategory,
    TransferErrorClassifier,
    RetryConfig,
    RetryHandler,
)


class TestErrorClassifier:
    """Tests for error classification."""
    
    @pytest.mark.parametrize("error_msg,expected", [
        ("Connection reset by peer", ErrorCategory.RETRYABLE),
        ("Socket timeout after 30s", ErrorCategory.RETRYABLE),
        ("HTTP 429 Too Many Requests", ErrorCategory.RETRYABLE),
        ("Service temporarily unavailable", ErrorCategory.RETRYABLE),
        ("Authentication failed", ErrorCategory.NON_RETRYABLE),
        ("Permission denied", ErrorCategory.NON_RETRYABLE),
        ("Table does not exist", ErrorCategory.NON_RETRYABLE),
        ("Syntax error in SQL", ErrorCategory.NON_RETRYABLE),
        ("Unknown error xyz", ErrorCategory.UNKNOWN),
    ])
    def test_classify_errors(self, error_msg, expected):
        """Verify error classification."""
        classifier = TransferErrorClassifier()
        error = Exception(error_msg)
        
        result = classifier.classify(error)
        
        assert result == expected
    
    def test_backoff_calculation(self):
        """Verify exponential backoff formula."""
        config = RetryConfig(base_delay_seconds=1.0, jitter_factor=0.0)
        handler = RetryHandler(config)
        
        delays = [handler.calculate_delay(i) for i in range(5)]
        
        # Without jitter: 1, 2, 4, 8, 16
        assert delays[0] == pytest.approx(1.0)
        assert delays[1] == pytest.approx(2.0)
        assert delays[2] == pytest.approx(4.0)
        assert delays[3] == pytest.approx(8.0)
        assert delays[4] == pytest.approx(16.0)
```

### Integration Tests

```python
# tests/functional/federation/test_partitioned_read.py

import pytest
from pyspark.sql import SparkSession

from dvt.federation.transfer.partition import (
    PartitionConfig,
    PartitionedReader,
)


@pytest.fixture
def spark():
    """Create test Spark session."""
    return SparkSession.builder \
        .master("local[2]") \
        .appName("DVT Partition Tests") \
        .getOrCreate()


class TestPartitionedReader:
    """Integration tests for partitioned reading."""
    
    @pytest.mark.postgres
    def test_partitioned_read(self, spark, postgres_jdbc_url, postgres_props):
        """Test partitioned read from Postgres."""
        config = PartitionConfig(
            rows_per_partition=1000,
            min_partitions=2,
            max_partitions=10,
        )
        
        reader = PartitionedReader(
            spark=spark,
            jdbc_url=postgres_jdbc_url,
            properties=postgres_props,
            config=config,
        )
        
        df = reader.read(
            schema="public",
            table="large_test_table",
        )
        
        # Should have detected partition column and used multiple partitions
        assert df.rdd.getNumPartitions() >= 2
        assert df.count() > 0
    
    @pytest.mark.postgres
    def test_fallback_to_single_partition(self, spark, postgres_jdbc_url, postgres_props):
        """Test fallback when no partition column detected."""
        config = PartitionConfig()
        
        reader = PartitionedReader(
            spark=spark,
            jdbc_url=postgres_jdbc_url,
            properties=postgres_props,
            config=config,
        )
        
        # Table with no obvious partition column
        df = reader.read(
            schema="public",
            table="tiny_table_no_pk",
        )
        
        # Should still work, just with single partition
        assert df.count() > 0
```

---

## Summary

### Key Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Partitioned reading | Auto-detect column + dynamic count | Balance parallelism vs overhead |
| Retry strategy | Exponential backoff + error classification | Resilient without infinite loops |
| Compression | Multi-layer, enabled by default | Reduces transfer time |

### Implementation Priority

1. **Phase 1**: Partitioned reading + retry (immediate value, low complexity)
2. **Phase 2**: Compression configuration (quick win)
3. **Phase 3**: Spark config tuning (easy to add)

### Success Metrics

| Metric | Target |
|--------|--------|
| JDBC transfer speedup | 10-50x via partitioning |
| Retry success rate | >95% for transient failures |
| Compression ratio | 2-5x size reduction |
