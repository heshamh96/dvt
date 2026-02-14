# Connection Optimizations

## Overview

This document covers connection-level optimizations for DVT federation: JDBC connectors, native cloud connectors, staging bucket configuration, and CLI integration.

### The Problem

1. **Suboptimal path**: Using JDBC when native connectors exist (Snowflake -> S3 -> Spark is 5-20x faster)
2. **No unified configuration**: Native connectors require staging buckets but no standard way to configure them
3. **Manual JAR management**: Users must manually download native connector JARs

### Goals

- **Optimal paths**: Automatically use native connectors when available
- **Unified configuration**: New `buckets.yml` for staging bucket credentials
- **Automated setup**: `dvt sync` downloads required JARs, `dvt init` creates templates

---

## Connector Types

### JDBC Connector (Universal Fallback)

JDBC is the lowest-common-denominator approach. It works with any database but is slower than native connectors for cloud data warehouses.

**When to use**: 
- Database doesn't have a native connector
- No staging bucket configured
- Small data transfers where native connector overhead isn't worth it

**Supported databases**: All (Postgres, MySQL, SQL Server, Oracle, Snowflake, Redshift, BigQuery, etc.)

### Native Connectors (Cloud Data Warehouses)

Modern cloud data warehouses have optimized bulk export paths:

| Source | Native Path | Speedup | Required Bucket |
|--------|-------------|---------|-----------------|
| Snowflake | COPY INTO S3/Azure/GCS -> Spark read Parquet | 5-20x | S3, Azure, or GCS |
| BigQuery | Export to GCS -> Spark read Parquet | 10-50x | GCS only |
| Redshift | UNLOAD to S3 -> Spark read Parquet | 5-15x | S3 only |

**When to use**:
- Large data transfers (>100K rows)
- Staging bucket is configured
- Source is Snowflake, BigQuery, or Redshift

---

## Connector Selection Logic

DVT automatically selects the best connector:

```
1. If source adapter has native connector:
   a. Check if buckets.yml has matching bucket configured
   b. If yes, use native connector
   c. If no, warn and fall back to JDBC
2. If no native connector exists:
   a. Use JDBC with partitioned reading
```

### Decision Flow

```
Source: Snowflake
    |
    v
Native connector available? --> YES
    |
    v
Bucket configured in buckets.yml?
    |
    +-- YES --> Use SnowflakeNativeConnector (via S3/Azure/GCS staging)
    |
    +-- NO --> WARN: "Native connector available but no bucket configured"
               Fall back to JDBCConnector
```

### Implementation

```python
# core/dvt/federation/connectors/__init__.py
"""Connector registry and selection logic."""

from __future__ import annotations

from typing import Any, Optional, Type

from pyspark.sql import SparkSession

from dvt.federation.connectors.base import BaseConnector, BucketConfig
from dvt.federation.connectors.jdbc import JDBCConnector
from dvt.federation.connectors.snowflake import SnowflakeNativeConnector
from dvt.federation.connectors.bigquery import BigQueryNativeConnector
from dvt.federation.connectors.redshift import RedshiftNativeConnector
from dvt.events.functions import fire_event
from dvt.events.types import EventLevel


# Registry of native connectors (order matters: first match wins)
NATIVE_CONNECTORS: list[Type[BaseConnector]] = [
    SnowflakeNativeConnector,
    BigQueryNativeConnector,
    RedshiftNativeConnector,
]


class ConnectorFactory:
    """Factory for creating appropriate connectors."""
    
    def __init__(
        self,
        spark: SparkSession,
        bucket_configs: dict[str, BucketConfig],
    ):
        """
        Initialize connector factory.
        
        Args:
            spark: Spark session
            bucket_configs: Map of profile target -> BucketConfig
        """
        self.spark = spark
        self.bucket_configs = bucket_configs
    
    def get_connector(
        self,
        source_config: dict[str, Any],
        bucket_target: Optional[str] = None,
    ) -> BaseConnector:
        """
        Get the best available connector for a source.
        
        Selection logic:
        1. Check if native connector exists for adapter type
        2. If native connector requires bucket, check if bucket configured
        3. If bucket available, use native connector
        4. Otherwise, fall back to JDBC
        
        Args:
            source_config: Source connection configuration
            bucket_target: Specific bucket target to use (optional)
            
        Returns:
            Appropriate connector instance
        """
        adapter_type = source_config.get("type", "").lower()
        
        # Try native connectors first
        for connector_cls in NATIVE_CONNECTORS:
            if connector_cls.supports_source(adapter_type):
                # Check if bucket is required and available
                if connector_cls.requires_bucket():
                    bucket_config = self._get_bucket_config(adapter_type, bucket_target)
                    
                    if bucket_config:
                        fire_event(
                            msg=f"Using native connector for {adapter_type}",
                            level=EventLevel.DEBUG,
                        )
                        return connector_cls(
                            spark=self.spark,
                            source_config=source_config,
                            bucket_config=bucket_config,
                        )
                    else:
                        # Warn about falling back to JDBC
                        fire_event(
                            msg=f"Native connector available for {adapter_type} but no bucket configured. "
                                f"Configure buckets.yml for faster transfers. Falling back to JDBC.",
                            level=EventLevel.WARN,
                        )
                else:
                    # Native connector doesn't require bucket
                    return connector_cls(
                        spark=self.spark,
                        source_config=source_config,
                    )
        
        # Fall back to JDBC
        fire_event(
            msg=f"Using JDBC connector for {adapter_type}",
            level=EventLevel.DEBUG,
        )
        return JDBCConnector(
            spark=self.spark,
            source_config=source_config,
        )
    
    def _get_bucket_config(
        self,
        adapter_type: str,
        bucket_target: Optional[str] = None,
    ) -> Optional[BucketConfig]:
        """
        Get bucket configuration for an adapter type.
        
        Args:
            adapter_type: The database adapter type
            bucket_target: Specific bucket target (optional)
            
        Returns:
            BucketConfig if available, None otherwise
        """
        if bucket_target and bucket_target in self.bucket_configs:
            return self.bucket_configs[bucket_target]
        
        # Auto-select bucket based on adapter type
        type_preferences = {
            "snowflake": ["s3", "azure", "gcs"],
            "bigquery": ["gcs"],
            "redshift": ["s3"],
        }
        
        preferred_types = type_preferences.get(adapter_type, [])
        
        for bucket_config in self.bucket_configs.values():
            if bucket_config.type in preferred_types:
                return bucket_config
        
        return None
```

---

## Base Connector Interface

```python
# core/dvt/federation/connectors/base.py
"""Base connector interface for data transfer."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Any, Optional, TYPE_CHECKING

from pyspark.sql import DataFrame, SparkSession

if TYPE_CHECKING:
    from dvt.config.runtime import RuntimeConfig


class ConnectorType(Enum):
    """Available connector types."""
    JDBC = "jdbc"
    SNOWFLAKE_NATIVE = "snowflake_native"
    BIGQUERY_NATIVE = "bigquery_native"
    REDSHIFT_NATIVE = "redshift_native"


@dataclass
class TransferStats:
    """Statistics from a data transfer operation."""
    rows_transferred: int
    bytes_transferred: int
    duration_seconds: float
    partitions_used: int
    connector_type: ConnectorType
    
    @property
    def rows_per_second(self) -> float:
        if self.duration_seconds > 0:
            return self.rows_transferred / self.duration_seconds
        return 0.0
    
    @property
    def mb_per_second(self) -> float:
        if self.duration_seconds > 0:
            return (self.bytes_transferred / 1024 / 1024) / self.duration_seconds
        return 0.0


@dataclass
class BucketConfig:
    """Configuration for a staging bucket."""
    type: str  # s3, gcs, azure
    bucket: str  # bucket name or container
    prefix: str = ""
    region: Optional[str] = None  # AWS region
    project: Optional[str] = None  # GCP project
    storage_account: Optional[str] = None  # Azure storage account
    
    # S3 credentials
    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None
    aws_profile: Optional[str] = None
    role_arn: Optional[str] = None
    
    # GCS credentials
    credentials_path: Optional[str] = None
    credentials_json: Optional[str] = None
    
    # Azure credentials
    account_key: Optional[str] = None
    sas_token: Optional[str] = None
    connection_string: Optional[str] = None
    
    def get_uri(self, path: str = "") -> str:
        """Get full URI for a path within the bucket."""
        if self.type == "s3":
            return f"s3://{self.bucket}/{self.prefix}{path}".rstrip("/")
        elif self.type == "gcs":
            return f"gs://{self.bucket}/{self.prefix}{path}".rstrip("/")
        elif self.type == "azure":
            return f"wasbs://{self.bucket}@{self.storage_account}.blob.core.windows.net/{self.prefix}{path}".rstrip("/")
        else:
            raise ValueError(f"Unknown bucket type: {self.type}")


class BaseConnector(ABC):
    """Base class for data transfer connectors."""
    
    connector_type: ConnectorType
    
    def __init__(
        self,
        spark: SparkSession,
        source_config: dict[str, Any],
        bucket_config: Optional[BucketConfig] = None,
    ):
        self.spark = spark
        self.source_config = source_config
        self.bucket_config = bucket_config
    
    @abstractmethod
    def read(
        self,
        schema: str,
        table: str,
        query: Optional[str] = None,
        predicates: Optional[list[str]] = None,
    ) -> tuple[DataFrame, TransferStats]:
        """
        Read data from source.
        
        Args:
            schema: Source schema
            table: Source table
            query: Optional custom query
            predicates: WHERE predicates for pushdown
            
        Returns:
            Tuple of (DataFrame, TransferStats)
        """
        pass
    
    @abstractmethod
    def write(
        self,
        df: DataFrame,
        schema: str,
        table: str,
        mode: str = "overwrite",
    ) -> TransferStats:
        """
        Write data to target.
        
        Args:
            df: DataFrame to write
            schema: Target schema
            table: Target table
            mode: Write mode (overwrite, append)
            
        Returns:
            TransferStats
        """
        pass
    
    @classmethod
    def supports_source(cls, adapter_type: str) -> bool:
        """Check if connector supports this source adapter type."""
        return False
    
    @classmethod
    def requires_bucket(cls) -> bool:
        """Check if connector requires a staging bucket."""
        return False
```

---

## JDBC Connector

```python
# core/dvt/federation/connectors/jdbc.py
"""JDBC connector with partitioned reading."""

from __future__ import annotations

import time
from typing import Any, Optional

from pyspark.sql import DataFrame, SparkSession

from dvt.federation.connectors.base import (
    BaseConnector,
    BucketConfig,
    ConnectorType,
    TransferStats,
)
from dvt.federation.transfer.partition import (
    PartitionConfig,
    PartitionedReader,
)
from dvt.federation.transfer.retry import RetryHandler, RetryConfig


class JDBCConnector(BaseConnector):
    """JDBC connector with partitioned reading and retry support."""
    
    connector_type = ConnectorType.JDBC
    
    def __init__(
        self,
        spark: SparkSession,
        source_config: dict[str, Any],
        bucket_config: Optional[BucketConfig] = None,
        partition_config: Optional[PartitionConfig] = None,
        retry_config: Optional[RetryConfig] = None,
    ):
        super().__init__(spark, source_config, bucket_config)
        self.partition_config = partition_config or PartitionConfig()
        self.retry_config = retry_config or RetryConfig()
        self.retry_handler = RetryHandler(self.retry_config)
        
        # Build JDBC URL and properties
        self.jdbc_url = self._build_jdbc_url()
        self.jdbc_properties = self._build_jdbc_properties()
        
        # Initialize partitioned reader
        self.partitioned_reader = PartitionedReader(
            spark=spark,
            jdbc_url=self.jdbc_url,
            properties=self.jdbc_properties,
            config=self.partition_config,
        )
    
    def _build_jdbc_url(self) -> str:
        """Build JDBC URL from source config."""
        adapter_type = self.source_config.get("type", "")
        host = self.source_config.get("host", "localhost")
        port = self.source_config.get("port")
        database = self.source_config.get("database", "")
        
        # URL patterns by adapter
        patterns = {
            "postgres": f"jdbc:postgresql://{host}:{port or 5432}/{database}",
            "snowflake": f"jdbc:snowflake://{self.source_config.get('account')}.snowflakecomputing.com/",
            "redshift": f"jdbc:redshift://{host}:{port or 5439}/{database}",
            "bigquery": f"jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443;ProjectId={self.source_config.get('project')};",
            "mysql": f"jdbc:mysql://{host}:{port or 3306}/{database}",
            "sqlserver": f"jdbc:sqlserver://{host}:{port or 1433};databaseName={database}",
        }
        
        return patterns.get(adapter_type, f"jdbc:{adapter_type}://{host}:{port}/{database}")
    
    def _build_jdbc_properties(self) -> dict[str, str]:
        """Build JDBC connection properties."""
        props = {
            "user": self.source_config.get("user", ""),
            "password": self.source_config.get("password", ""),
            "driver": self._get_driver_class(),
        }
        
        # Add SSL if configured
        if self.source_config.get("ssl", False):
            props["ssl"] = "true"
            props["sslmode"] = self.source_config.get("sslmode", "require")
        
        return props
    
    def _get_driver_class(self) -> str:
        """Get JDBC driver class for adapter type."""
        adapter_type = self.source_config.get("type", "")
        
        drivers = {
            "postgres": "org.postgresql.Driver",
            "snowflake": "net.snowflake.client.jdbc.SnowflakeDriver",
            "redshift": "com.amazon.redshift.jdbc42.Driver",
            "bigquery": "com.simba.googlebigquery.jdbc.Driver",
            "mysql": "com.mysql.cj.jdbc.Driver",
            "sqlserver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        }
        
        return drivers.get(adapter_type, "")
    
    def read(
        self,
        schema: str,
        table: str,
        query: Optional[str] = None,
        predicates: Optional[list[str]] = None,
    ) -> tuple[DataFrame, TransferStats]:
        """Read data using partitioned JDBC with retry."""
        start_time = time.time()
        
        # Execute with retry
        df = self.retry_handler.execute_with_retry(
            self.partitioned_reader.read,
            schema=schema,
            table=table,
            query=query,
            predicates=predicates,
        )
        
        # Collect stats
        duration = time.time() - start_time
        row_count = df.count()
        
        # Estimate bytes (rough: assume 100 bytes per row average)
        estimated_bytes = row_count * 100
        
        stats = TransferStats(
            rows_transferred=row_count,
            bytes_transferred=estimated_bytes,
            duration_seconds=duration,
            partitions_used=df.rdd.getNumPartitions(),
            connector_type=self.connector_type,
        )
        
        return df, stats
    
    def write(
        self,
        df: DataFrame,
        schema: str,
        table: str,
        mode: str = "overwrite",
    ) -> TransferStats:
        """Write data using JDBC."""
        start_time = time.time()
        row_count = df.count()
        
        # Map mode to Spark mode
        spark_mode = "overwrite" if mode == "overwrite" else "append"
        
        # Execute with retry
        def do_write():
            df.write.jdbc(
                url=self.jdbc_url,
                table=f"{schema}.{table}",
                mode=spark_mode,
                properties=self.jdbc_properties,
            )
        
        self.retry_handler.execute_with_retry(do_write)
        
        duration = time.time() - start_time
        
        return TransferStats(
            rows_transferred=row_count,
            bytes_transferred=row_count * 100,  # Estimate
            duration_seconds=duration,
            partitions_used=df.rdd.getNumPartitions(),
            connector_type=self.connector_type,
        )
    
    @classmethod
    def supports_source(cls, adapter_type: str) -> bool:
        """JDBC supports all adapter types."""
        return True
    
    @classmethod
    def requires_bucket(cls) -> bool:
        """JDBC does not require a staging bucket."""
        return False
```

---

## Native Connectors

### Snowflake Native Connector

```python
# core/dvt/federation/connectors/snowflake.py
"""Snowflake native connector using bulk COPY operations."""

from __future__ import annotations

import time
import uuid
from typing import Any, Optional

from pyspark.sql import DataFrame, SparkSession

from dvt.federation.connectors.base import (
    BaseConnector,
    BucketConfig,
    ConnectorType,
    TransferStats,
)
from dvt.federation.transfer.retry import RetryHandler, RetryConfig


class SnowflakeNativeConnector(BaseConnector):
    """
    Snowflake native connector using bulk COPY operations.
    
    Transfer path:
    - Read: COPY INTO stage -> Download from S3/Azure/GCS -> Spark read
    - Write: Spark write to stage -> COPY INTO table
    """
    
    connector_type = ConnectorType.SNOWFLAKE_NATIVE
    
    # Supported storage types for Snowflake
    SUPPORTED_BUCKET_TYPES = {"s3", "azure", "gcs"}
    
    def __init__(
        self,
        spark: SparkSession,
        source_config: dict[str, Any],
        bucket_config: Optional[BucketConfig] = None,
        retry_config: Optional[RetryConfig] = None,
    ):
        super().__init__(spark, source_config, bucket_config)
        
        if not bucket_config:
            raise ValueError("SnowflakeNativeConnector requires bucket_config")
        
        if bucket_config.type not in self.SUPPORTED_BUCKET_TYPES:
            raise ValueError(
                f"Snowflake native connector supports bucket types: {self.SUPPORTED_BUCKET_TYPES}, "
                f"got: {bucket_config.type}"
            )
        
        self.retry_config = retry_config or RetryConfig()
        self.retry_handler = RetryHandler(self.retry_config)
        
        # Configure Spark for Snowflake connector
        self._configure_spark()
    
    def _configure_spark(self):
        """Configure Spark session for Snowflake connector."""
        # Set Snowflake options
        self.sf_options = {
            "sfURL": f"{self.source_config.get('account')}.snowflakecomputing.com",
            "sfUser": self.source_config.get("user"),
            "sfPassword": self.source_config.get("password"),
            "sfDatabase": self.source_config.get("database"),
            "sfSchema": self.source_config.get("schema", "PUBLIC"),
            "sfWarehouse": self.source_config.get("warehouse"),
        }
        
        # Add staging options based on bucket type
        if self.bucket_config.type == "s3":
            self.sf_options.update({
                "tempdir": self.bucket_config.get_uri("temp/"),
                "awsAccessKey": self.bucket_config.aws_access_key_id,
                "awsSecretKey": self.bucket_config.aws_secret_access_key,
            })
        elif self.bucket_config.type == "azure":
            self.sf_options.update({
                "tempdir": self.bucket_config.get_uri("temp/"),
                "temporary_azure_sas_token": self.bucket_config.sas_token,
            })
        elif self.bucket_config.type == "gcs":
            self.sf_options.update({
                "tempdir": self.bucket_config.get_uri("temp/"),
            })
    
    def read(
        self,
        schema: str,
        table: str,
        query: Optional[str] = None,
        predicates: Optional[list[str]] = None,
    ) -> tuple[DataFrame, TransferStats]:
        """
        Read data from Snowflake using native connector.
        
        Uses Snowflake's COPY INTO to export to cloud storage,
        then Spark reads the exported files.
        """
        start_time = time.time()
        
        # Build read options
        read_options = self.sf_options.copy()
        
        if query:
            read_options["query"] = query
        else:
            read_options["dbtable"] = f"{schema}.{table}"
        
        # Add predicates if provided
        if predicates and not query:
            # Construct query with predicates
            predicate_str = " AND ".join(predicates)
            read_options["query"] = f"SELECT * FROM {schema}.{table} WHERE {predicate_str}"
            del read_options["dbtable"]
        
        # Execute read with retry
        def do_read() -> DataFrame:
            return self.spark.read.format("snowflake").options(**read_options).load()
        
        df = self.retry_handler.execute_with_retry(do_read)
        
        # Collect stats
        duration = time.time() - start_time
        row_count = df.count()
        
        stats = TransferStats(
            rows_transferred=row_count,
            bytes_transferred=row_count * 100,  # Estimate
            duration_seconds=duration,
            partitions_used=df.rdd.getNumPartitions(),
            connector_type=self.connector_type,
        )
        
        return df, stats
    
    def write(
        self,
        df: DataFrame,
        schema: str,
        table: str,
        mode: str = "overwrite",
    ) -> TransferStats:
        """
        Write data to Snowflake using native connector.
        
        Spark writes to cloud storage, then Snowflake COPY INTO
        loads the data.
        """
        start_time = time.time()
        row_count = df.count()
        
        # Build write options
        write_options = self.sf_options.copy()
        write_options["dbtable"] = f"{schema}.{table}"
        
        # Execute write with retry
        def do_write():
            df.write.format("snowflake").options(**write_options).mode(mode).save()
        
        self.retry_handler.execute_with_retry(do_write)
        
        duration = time.time() - start_time
        
        return TransferStats(
            rows_transferred=row_count,
            bytes_transferred=row_count * 100,
            duration_seconds=duration,
            partitions_used=df.rdd.getNumPartitions(),
            connector_type=self.connector_type,
        )
    
    @classmethod
    def supports_source(cls, adapter_type: str) -> bool:
        """Check if this connector supports the adapter type."""
        return adapter_type.lower() == "snowflake"
    
    @classmethod
    def requires_bucket(cls) -> bool:
        """Snowflake native connector requires a staging bucket."""
        return True
```

### BigQuery Native Connector

```python
# core/dvt/federation/connectors/bigquery.py
"""BigQuery native connector using bulk export operations."""

from __future__ import annotations

import time
from typing import Any, Optional

from pyspark.sql import DataFrame, SparkSession

from dvt.federation.connectors.base import (
    BaseConnector,
    BucketConfig,
    ConnectorType,
    TransferStats,
)
from dvt.federation.transfer.retry import RetryHandler, RetryConfig


class BigQueryNativeConnector(BaseConnector):
    """
    BigQuery native connector using spark-bigquery connector.
    
    Transfer path:
    - Read: Export to GCS -> Spark read Avro/Parquet
    - Write: Spark write to GCS -> Load into BigQuery
    """
    
    connector_type = ConnectorType.BIGQUERY_NATIVE
    
    def __init__(
        self,
        spark: SparkSession,
        source_config: dict[str, Any],
        bucket_config: Optional[BucketConfig] = None,
        retry_config: Optional[RetryConfig] = None,
    ):
        super().__init__(spark, source_config, bucket_config)
        
        if not bucket_config:
            raise ValueError("BigQueryNativeConnector requires bucket_config")
        
        if bucket_config.type != "gcs":
            raise ValueError(
                f"BigQuery native connector requires GCS bucket, got: {bucket_config.type}"
            )
        
        self.retry_config = retry_config or RetryConfig()
        self.retry_handler = RetryHandler(self.retry_config)
        
        # Configure Spark for BigQuery
        self._configure_spark()
    
    def _configure_spark(self):
        """Configure Spark session for BigQuery connector."""
        # Set BigQuery options
        self.bq_options = {
            "project": self.source_config.get("project"),
            "temporaryGcsBucket": self.bucket_config.bucket,
            "materializationDataset": self.source_config.get("dataset", "dvt_temp"),
        }
        
        # Add credentials if specified
        if self.bucket_config.credentials_path:
            self.bq_options["credentialsFile"] = self.bucket_config.credentials_path
    
    def read(
        self,
        schema: str,
        table: str,
        query: Optional[str] = None,
        predicates: Optional[list[str]] = None,
    ) -> tuple[DataFrame, TransferStats]:
        """Read data from BigQuery using native connector."""
        start_time = time.time()
        
        read_options = self.bq_options.copy()
        
        if query:
            read_options["query"] = query
        else:
            # BigQuery uses project.dataset.table format
            project = self.source_config.get("project")
            read_options["table"] = f"{project}.{schema}.{table}"
        
        # Add filter if predicates provided
        if predicates and not query:
            read_options["filter"] = " AND ".join(predicates)
        
        def do_read() -> DataFrame:
            return self.spark.read.format("bigquery").options(**read_options).load()
        
        df = self.retry_handler.execute_with_retry(do_read)
        
        duration = time.time() - start_time
        row_count = df.count()
        
        stats = TransferStats(
            rows_transferred=row_count,
            bytes_transferred=row_count * 100,
            duration_seconds=duration,
            partitions_used=df.rdd.getNumPartitions(),
            connector_type=self.connector_type,
        )
        
        return df, stats
    
    def write(
        self,
        df: DataFrame,
        schema: str,
        table: str,
        mode: str = "overwrite",
    ) -> TransferStats:
        """Write data to BigQuery using native connector."""
        start_time = time.time()
        row_count = df.count()
        
        write_options = self.bq_options.copy()
        project = self.source_config.get("project")
        write_options["table"] = f"{project}.{schema}.{table}"
        
        def do_write():
            df.write.format("bigquery").options(**write_options).mode(mode).save()
        
        self.retry_handler.execute_with_retry(do_write)
        
        duration = time.time() - start_time
        
        return TransferStats(
            rows_transferred=row_count,
            bytes_transferred=row_count * 100,
            duration_seconds=duration,
            partitions_used=df.rdd.getNumPartitions(),
            connector_type=self.connector_type,
        )
    
    @classmethod
    def supports_source(cls, adapter_type: str) -> bool:
        return adapter_type.lower() == "bigquery"
    
    @classmethod
    def requires_bucket(cls) -> bool:
        return True
```

### Redshift Native Connector

```python
# core/dvt/federation/connectors/redshift.py
"""Redshift native connector using UNLOAD/COPY operations."""

from __future__ import annotations

import time
from typing import Any, Optional

from pyspark.sql import DataFrame, SparkSession

from dvt.federation.connectors.base import (
    BaseConnector,
    BucketConfig,
    ConnectorType,
    TransferStats,
)
from dvt.federation.transfer.retry import RetryHandler, RetryConfig


class RedshiftNativeConnector(BaseConnector):
    """
    Redshift native connector using spark-redshift connector.
    
    Transfer path:
    - Read: UNLOAD to S3 -> Spark read Avro
    - Write: Spark write to S3 -> COPY into Redshift
    """
    
    connector_type = ConnectorType.REDSHIFT_NATIVE
    
    def __init__(
        self,
        spark: SparkSession,
        source_config: dict[str, Any],
        bucket_config: Optional[BucketConfig] = None,
        retry_config: Optional[RetryConfig] = None,
    ):
        super().__init__(spark, source_config, bucket_config)
        
        if not bucket_config:
            raise ValueError("RedshiftNativeConnector requires bucket_config")
        
        if bucket_config.type != "s3":
            raise ValueError(
                f"Redshift native connector requires S3 bucket, got: {bucket_config.type}"
            )
        
        self.retry_config = retry_config or RetryConfig()
        self.retry_handler = RetryHandler(self.retry_config)
        
        self._configure_spark()
    
    def _configure_spark(self):
        """Configure Spark for Redshift connector."""
        host = self.source_config.get("host")
        port = self.source_config.get("port", 5439)
        database = self.source_config.get("database")
        
        self.rs_options = {
            "url": f"jdbc:redshift://{host}:{port}/{database}",
            "user": self.source_config.get("user"),
            "password": self.source_config.get("password"),
            "tempdir": self.bucket_config.get_uri("temp/"),
            "forward_spark_s3_credentials": "true",
        }
        
        # Set AWS credentials if provided
        if self.bucket_config.aws_access_key_id:
            self.rs_options["aws_iam_role"] = None  # Disable IAM role
            # Credentials passed via Spark config
            self.spark.conf.set(
                "fs.s3a.access.key", 
                self.bucket_config.aws_access_key_id
            )
            self.spark.conf.set(
                "fs.s3a.secret.key",
                self.bucket_config.aws_secret_access_key
            )
    
    def read(
        self,
        schema: str,
        table: str,
        query: Optional[str] = None,
        predicates: Optional[list[str]] = None,
    ) -> tuple[DataFrame, TransferStats]:
        """Read data from Redshift using native connector."""
        start_time = time.time()
        
        read_options = self.rs_options.copy()
        
        if query:
            read_options["query"] = query
        else:
            read_options["dbtable"] = f"{schema}.{table}"
        
        def do_read() -> DataFrame:
            return self.spark.read.format("io.github.spark_redshift_community.spark.redshift").options(**read_options).load()
        
        df = self.retry_handler.execute_with_retry(do_read)
        
        duration = time.time() - start_time
        row_count = df.count()
        
        stats = TransferStats(
            rows_transferred=row_count,
            bytes_transferred=row_count * 100,
            duration_seconds=duration,
            partitions_used=df.rdd.getNumPartitions(),
            connector_type=self.connector_type,
        )
        
        return df, stats
    
    def write(
        self,
        df: DataFrame,
        schema: str,
        table: str,
        mode: str = "overwrite",
    ) -> TransferStats:
        """Write data to Redshift using native connector."""
        start_time = time.time()
        row_count = df.count()
        
        write_options = self.rs_options.copy()
        write_options["dbtable"] = f"{schema}.{table}"
        
        def do_write():
            df.write.format("io.github.spark_redshift_community.spark.redshift").options(**write_options).mode(mode).save()
        
        self.retry_handler.execute_with_retry(do_write)
        
        duration = time.time() - start_time
        
        return TransferStats(
            rows_transferred=row_count,
            bytes_transferred=row_count * 100,
            duration_seconds=duration,
            partitions_used=df.rdd.getNumPartitions(),
            connector_type=self.connector_type,
        )
    
    @classmethod
    def supports_source(cls, adapter_type: str) -> bool:
        return adapter_type.lower() == "redshift"
    
    @classmethod
    def requires_bucket(cls) -> bool:
        return True
```

---

## `buckets.yml` Configuration

### Overview

A new configuration file that mirrors the structure of `profiles.yml`:

**Location**: `~/.dvt/buckets.yml`

**Purpose**: Configure cloud storage buckets for native connector staging

### Structure

```yaml
# ~/.dvt/buckets.yml
# Staging bucket configuration for native connectors
# Structure mirrors profiles.yml: profile -> outputs -> targets

# Default profile (used when no --profile specified)
default:
  target: s3_staging    # Default bucket target
  outputs:
    
    # S3 bucket for Snowflake/Redshift
    s3_staging:
      type: s3
      bucket: my-company-dvt-staging
      prefix: dvt/federation/       # Optional prefix within bucket
      region: us-east-1
      # Authentication (pick one):
      # Option 1: Access keys
      aws_access_key_id: "{{ env_var('AWS_ACCESS_KEY_ID') }}"
      aws_secret_access_key: "{{ env_var('AWS_SECRET_ACCESS_KEY') }}"
      # Option 2: IAM role (leave credentials empty)
      # Option 3: AWS profile
      # aws_profile: my-profile
    
    # GCS bucket for BigQuery
    gcs_staging:
      type: gcs
      bucket: my-company-dvt-staging
      prefix: dvt/federation/
      project: my-gcp-project
      # Authentication:
      credentials_path: "{{ env_var('GOOGLE_APPLICATION_CREDENTIALS') }}"
      # Or inline:
      # credentials_json: "{{ env_var('GCP_CREDENTIALS_JSON') }}"
    
    # Azure Blob for Snowflake on Azure
    azure_staging:
      type: azure
      storage_account: mystorageaccount
      container: dvt-staging
      prefix: federation/
      # Authentication:
      account_key: "{{ env_var('AZURE_STORAGE_KEY') }}"
      # Or SAS token:
      # sas_token: "{{ env_var('AZURE_SAS_TOKEN') }}"
      # Or connection string:
      # connection_string: "{{ env_var('AZURE_STORAGE_CONNECTION_STRING') }}"

# Production profile
production:
  target: prod_s3
  outputs:
    prod_s3:
      type: s3
      bucket: prod-dvt-staging
      prefix: dvt/
      region: us-west-2
      role_arn: arn:aws:iam::123456789:role/DVTFederationRole

# Profile for BigQuery-heavy workloads
bigquery_heavy:
  target: gcs_staging
  outputs:
    gcs_staging:
      type: gcs
      bucket: bq-staging-bucket
      project: my-analytics-project
      credentials_path: /secure/gcp-service-account.json
```

### Validation Rules

| Field | Required | Validation |
|-------|----------|------------|
| `type` | Yes | One of: `s3`, `gcs`, `azure` |
| `bucket` / `container` | Yes | Non-empty string |
| `region` | S3 only | Valid AWS region |
| `project` | GCS only | Non-empty string |
| `storage_account` | Azure only | Non-empty string |
| Credentials | Yes | At least one auth method |

### Configuration Loader

```python
# core/dvt/config/buckets.py
"""Bucket configuration loading and validation."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

import yaml

from dvt.exceptions import DbtProfileError
from dvt.federation.connectors.base import BucketConfig


@dataclass
class BucketProfile:
    """A bucket profile with target and outputs."""
    target: str
    outputs: dict[str, BucketConfig]


class BucketConfigLoader:
    """Loads and validates bucket configuration."""
    
    REQUIRED_FIELDS = {
        "s3": ["bucket", "region"],
        "gcs": ["bucket", "project"],
        "azure": ["storage_account", "container"],
    }
    
    def __init__(self, buckets_path: Optional[Path] = None):
        self.buckets_path = buckets_path or (Path.home() / ".dvt" / "buckets.yml")
        self._profiles: dict[str, BucketProfile] = {}
        self._loaded = False
    
    def load(self) -> dict[str, BucketProfile]:
        """Load bucket configuration from file."""
        if self._loaded:
            return self._profiles
        
        if not self.buckets_path.exists():
            self._loaded = True
            return self._profiles
        
        with open(self.buckets_path) as f:
            raw_config = yaml.safe_load(f) or {}
        
        for profile_name, profile_data in raw_config.items():
            if not isinstance(profile_data, dict):
                continue
            
            target = profile_data.get("target", "")
            outputs = profile_data.get("outputs", {})
            
            bucket_configs = {}
            for output_name, output_data in outputs.items():
                if not isinstance(output_data, dict):
                    continue
                
                bucket_config = self._parse_output(profile_name, output_name, output_data)
                if bucket_config:
                    bucket_configs[output_name] = bucket_config
            
            if bucket_configs:
                self._profiles[profile_name] = BucketProfile(
                    target=target,
                    outputs=bucket_configs,
                )
        
        self._loaded = True
        return self._profiles
    
    def _parse_output(
        self,
        profile_name: str,
        output_name: str,
        data: dict[str, Any],
    ) -> Optional[BucketConfig]:
        """Parse a single bucket output configuration."""
        bucket_type = data.get("type", "").lower()
        
        if bucket_type not in self.REQUIRED_FIELDS:
            raise DbtProfileError(
                f"Invalid bucket type '{bucket_type}' in {profile_name}.{output_name}. "
                f"Must be one of: s3, gcs, azure"
            )
        
        # Validate required fields
        missing = []
        for field in self.REQUIRED_FIELDS[bucket_type]:
            if not data.get(field):
                missing.append(field)
        
        if missing:
            raise DbtProfileError(
                f"Missing required fields for {bucket_type} bucket "
                f"'{profile_name}.{output_name}': {', '.join(missing)}"
            )
        
        # Validate credentials
        if bucket_type == "s3":
            has_creds = (
                data.get("aws_access_key_id") and data.get("aws_secret_access_key")
            ) or data.get("aws_profile") or data.get("role_arn")
            # IAM role from instance profile is also valid (no explicit creds)
        elif bucket_type == "gcs":
            has_creds = data.get("credentials_path") or data.get("credentials_json")
            # Default application credentials also valid
        elif bucket_type == "azure":
            has_creds = (
                data.get("account_key") or 
                data.get("sas_token") or 
                data.get("connection_string")
            )
            if not has_creds:
                raise DbtProfileError(
                    f"Azure bucket '{profile_name}.{output_name}' requires one of: "
                    "account_key, sas_token, or connection_string"
                )
        
        return BucketConfig(
            type=bucket_type,
            bucket=data.get("bucket") or data.get("container", ""),
            prefix=data.get("prefix", ""),
            region=data.get("region"),
            project=data.get("project"),
            storage_account=data.get("storage_account"),
            aws_access_key_id=data.get("aws_access_key_id"),
            aws_secret_access_key=data.get("aws_secret_access_key"),
            aws_profile=data.get("aws_profile"),
            role_arn=data.get("role_arn"),
            credentials_path=data.get("credentials_path"),
            credentials_json=data.get("credentials_json"),
            account_key=data.get("account_key"),
            sas_token=data.get("sas_token"),
            connection_string=data.get("connection_string"),
        )
    
    def get_bucket_for_profile(
        self,
        profile_name: str,
        target: Optional[str] = None,
    ) -> Optional[BucketConfig]:
        """
        Get bucket configuration for a profile.
        
        Args:
            profile_name: Profile name
            target: Specific bucket target (uses default if not specified)
            
        Returns:
            BucketConfig if found, None otherwise
        """
        profiles = self.load()
        
        if profile_name not in profiles:
            return None
        
        profile = profiles[profile_name]
        bucket_target = target or profile.target
        
        return profile.outputs.get(bucket_target)
    
    def get_all_buckets_for_profile(
        self,
        profile_name: str,
    ) -> dict[str, BucketConfig]:
        """Get all bucket configurations for a profile."""
        profiles = self.load()
        
        if profile_name not in profiles:
            return {}
        
        return profiles[profile_name].outputs
```

---

## CLI Integration

### `dvt sync` Enhancement

The `dvt sync` command downloads JDBC drivers. We enhance it to also download native connector JARs.

```python
# Addition to core/dvt/task/jdbc_drivers.py

# Native connector Maven coordinates
NATIVE_CONNECTOR_JARS: dict[str, list[tuple[str, str, str]]] = {
    # adapter_type -> list of (groupId, artifactId, version)
    "snowflake": [
        ("net.snowflake", "spark-snowflake_2.12", "2.12.0-spark_3.3"),
    ],
    "bigquery": [
        ("com.google.cloud.spark", "spark-bigquery-with-dependencies_2.12", "0.32.0"),
    ],
    "redshift": [
        ("io.github.spark-redshift-community", "spark-redshift_2.12", "5.1.0-spark_3.3"),
    ],
}


def get_native_connectors_for_profile(profile_adapters: set[str]) -> list[tuple[str, str, str]]:
    """
    Get native connector JARs needed for adapters in profile.
    
    Args:
        profile_adapters: Set of adapter types used in profiles.yml
        
    Returns:
        List of Maven coordinates (groupId, artifactId, version)
    """
    connectors = []
    for adapter in profile_adapters:
        adapter_lower = adapter.lower()
        if adapter_lower in NATIVE_CONNECTOR_JARS:
            connectors.extend(NATIVE_CONNECTOR_JARS[adapter_lower])
    return connectors
```

### `dvt init` Enhancement

The `dvt init` command creates initial configuration. We add `buckets.yml` template creation.

```python
# Template for buckets.yml created by dvt init

BUCKETS_YML_TEMPLATE = '''# DVT Staging Buckets Configuration
# Configure cloud storage buckets for native connector staging
# This enables faster data transfers for Snowflake, BigQuery, and Redshift
#
# Structure mirrors profiles.yml:
#   profile_name:
#     target: default_bucket_target
#     outputs:
#       bucket_target_name:
#         type: s3 | gcs | azure
#         ...credentials...

# Uncomment and configure the sections below based on your cloud environment

# default:
#   target: s3_staging
#   outputs:
#
#     # S3 bucket (for Snowflake on AWS, Redshift)
#     # s3_staging:
#     #   type: s3
#     #   bucket: my-company-dvt-staging
#     #   prefix: dvt/
#     #   region: us-east-1
#     #   aws_access_key_id: "{{ env_var('AWS_ACCESS_KEY_ID') }}"
#     #   aws_secret_access_key: "{{ env_var('AWS_SECRET_ACCESS_KEY') }}"
#
#     # GCS bucket (for BigQuery)
#     # gcs_staging:
#     #   type: gcs
#     #   bucket: my-company-dvt-staging
#     #   project: my-gcp-project
#     #   credentials_path: "{{ env_var('GOOGLE_APPLICATION_CREDENTIALS') }}"
#
#     # Azure Blob (for Snowflake on Azure)
#     # azure_staging:
#     #   type: azure
#     #   storage_account: mystorageaccount
#     #   container: dvt-staging
#     #   account_key: "{{ env_var('AZURE_STORAGE_KEY') }}"
'''
```

### `dvt debug` Enhancement

Add bucket configuration status to debug output:

```python
# Addition to debug output

def report_bucket_status(profile: str) -> dict[str, Any]:
    """Report bucket configuration status for a profile."""
    buckets_path = Path.home() / ".dvt" / "buckets.yml"
    
    status = {
        "buckets_yml_exists": buckets_path.exists(),
        "buckets_configured": [],
        "native_connectors_available": [],
    }
    
    if buckets_path.exists():
        # Parse and report configured buckets
        loader = BucketConfigLoader(buckets_path)
        buckets = loader.get_all_buckets_for_profile(profile)
        
        for name, config in buckets.items():
            status["buckets_configured"].append({
                "name": name,
                "type": config.type,
                "bucket": config.bucket,
            })
        
        # Check which native connectors could be used
        if any(b.type in ["s3", "azure", "gcs"] for b in buckets.values()):
            status["native_connectors_available"].append("snowflake")
        if any(b.type == "gcs" for b in buckets.values()):
            status["native_connectors_available"].append("bigquery")
        if any(b.type == "s3" for b in buckets.values()):
            status["native_connectors_available"].append("redshift")
    
    return status
```

---

## Module Architecture

```
core/dvt/federation/
├── __init__.py
├── connectors/
│   ├── __init__.py          # ConnectorFactory, registry
│   ├── base.py               # BaseConnector, BucketConfig, TransferStats
│   ├── jdbc.py               # JDBCConnector
│   ├── snowflake.py          # SnowflakeNativeConnector
│   ├── bigquery.py           # BigQueryNativeConnector
│   └── redshift.py           # RedshiftNativeConnector
└── config/
    └── buckets.py            # BucketConfigLoader
```

---

## Full `buckets.yml` Example

```yaml
# ~/.dvt/buckets.yml
# Complete example with all cloud providers

default:
  target: s3_staging
  outputs:
    
    # AWS S3 - For Snowflake (AWS), Redshift
    s3_staging:
      type: s3
      bucket: acme-corp-dvt-staging
      prefix: federation/
      region: us-east-1
      
      # Option 1: Access keys (not recommended for production)
      # aws_access_key_id: "{{ env_var('AWS_ACCESS_KEY_ID') }}"
      # aws_secret_access_key: "{{ env_var('AWS_SECRET_ACCESS_KEY') }}"
      
      # Option 2: IAM role (recommended)
      role_arn: arn:aws:iam::123456789012:role/DVTFederationRole
    
    # Google Cloud Storage - For BigQuery
    gcs_staging:
      type: gcs
      bucket: acme-corp-dvt-staging
      prefix: federation/
      project: acme-analytics-prod
      
      # Service account credentials
      credentials_path: "{{ env_var('GOOGLE_APPLICATION_CREDENTIALS') }}"
    
    # Azure Blob Storage - For Snowflake (Azure)
    azure_staging:
      type: azure
      storage_account: acmedvtstorage
      container: federation-staging
      prefix: data/
      
      # Option 1: Account key
      # account_key: "{{ env_var('AZURE_STORAGE_KEY') }}"
      
      # Option 2: SAS token (recommended - scoped permissions)
      sas_token: "{{ env_var('AZURE_SAS_TOKEN') }}"

# Production profile with stricter settings
production:
  target: prod_s3
  outputs:
    prod_s3:
      type: s3
      bucket: acme-prod-dvt-staging
      prefix: prod/
      region: us-west-2
      role_arn: arn:aws:iam::123456789012:role/DVTProdRole

# Development profile
development:
  target: dev_s3
  outputs:
    dev_s3:
      type: s3
      bucket: acme-dev-dvt-staging
      prefix: dev/
      region: us-east-1
      aws_access_key_id: "{{ env_var('DEV_AWS_ACCESS_KEY_ID') }}"
      aws_secret_access_key: "{{ env_var('DEV_AWS_SECRET_ACCESS_KEY') }}"
```

---

## Summary

### Key Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Native vs JDBC | Prefer native when bucket configured | Performance, with graceful fallback |
| Bucket config | New `buckets.yml` file | Mirrors `profiles.yml` structure |
| Connector selection | Auto-detect based on adapter + bucket | Zero configuration for users |
| CLI integration | Extend `dvt sync` and `dvt init` | Seamless setup experience |

### Performance Expectations

| Connector | Use Case | Expected Speedup |
|-----------|----------|------------------|
| JDBC | Small tables, unsupported DBs | Baseline |
| Snowflake Native | Large Snowflake tables | 5-20x |
| BigQuery Native | Large BigQuery tables | 10-50x |
| Redshift Native | Large Redshift tables | 5-15x |
