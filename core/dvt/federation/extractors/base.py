"""
Base extractor class for EL layer.

Extractors are responsible for extracting data from source databases
and writing to Parquet files in the staging bucket.

Extraction priority:
1. Native cloud export (e.g., Snowflake COPY INTO, BigQuery EXPORT)
2. Database-specific bulk export (e.g., PostgreSQL COPY)
3. Spark JDBC (default fallback - parallel reads via spark.read.jdbc)
"""

import os
import shutil
import subprocess
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple


@dataclass
class ExtractionConfig:
    """Configuration for a single extraction."""

    source_name: str  # e.g., 'postgres__orders'
    schema: str  # Database schema
    table: str  # Table name
    columns: Optional[List[str]] = None  # Columns to extract (None = all)
    predicates: Optional[List[str]] = None  # WHERE predicates to push down
    limit: Optional[int] = None  # LIMIT to push down (None = all rows)
    pk_columns: Optional[List[str]] = None  # Primary key columns for incremental
    batch_size: int = 100000  # Rows per batch
    bucket_config: Optional[Dict[str, Any]] = None  # Bucket type and credentials
    connection_config: Optional[Dict[str, Any]] = (
        None  # profiles.yml connection dict for JDBC
    )
    jdbc_config: Optional[Dict[str, Any]] = (
        None  # JDBC extraction settings from computes.yml
    )


@dataclass
class ExtractionResult:
    """Result of an extraction operation."""

    success: bool
    source_name: str
    row_count: int = 0
    output_path: Optional[Path] = None
    error: Optional[str] = None
    extraction_method: str = (
        "full"  # 'full', 'incremental', 'jdbc', 'native_parallel', 'skip'
    )
    elapsed_seconds: float = 0.0
    row_hashes: Optional[Dict[str, str]] = None  # pk -> hash for incremental


class BaseExtractor(ABC):
    """Base class for database extractors.

    Subclasses implement database-specific extraction logic.
    All extractors inherit _extract_jdbc() as the default fallback.

    Extraction methods (in priority order):
    1. Native cloud export - for cloud DWs exporting to cloud storage
    2. Database bulk export - e.g., PostgreSQL COPY, DuckDB COPY
    3. Spark JDBC - parallel reads via spark.read.jdbc() (default fallback)
    """

    # Adapter types this extractor handles (set by subclass)
    adapter_types: List[str] = []

    def __init__(
        self,
        connection: Any,
        dialect: str,
        on_progress: Optional[Callable[[str], None]] = None,
        connection_config: Optional[Dict[str, Any]] = None,
    ):
        """Initialize extractor.

        Args:
            connection: Raw database connection from dbt adapter
            dialect: SQL dialect name (e.g., 'postgres', 'mysql')
            on_progress: Optional callback for progress messages
            connection_config: Connection configuration dict for lazy connection creation
        """
        self.connection = connection
        self.dialect = dialect
        self.on_progress = on_progress or (lambda msg: None)
        self.connection_config = connection_config
        self._lazy_connection = None  # Created on demand if connection is None

    def _log(self, message: str) -> None:
        """Log a progress message."""
        self.on_progress(message)

    def supports_native_export(self, bucket_type: str) -> bool:
        """Check if this extractor supports native export to bucket type.

        Cloud-native extractors (Snowflake, BigQuery, Redshift, etc.) override
        this to return True for their supported cloud storage types.

        Args:
            bucket_type: Type of bucket ('filesystem', 's3', 'gcs', 'azure', 'hdfs')

        Returns:
            True if native export is supported for this bucket type
        """
        return False

    def get_native_export_bucket_types(self) -> List[str]:
        """Get list of bucket types this extractor can natively export to.

        Returns:
            List of supported bucket types (e.g., ['s3', 'gcs', 'azure'])
        """
        return []

    @abstractmethod
    def extract(
        self,
        config: ExtractionConfig,
        output_path: Path,
    ) -> ExtractionResult:
        """Extract data from source to Parquet file.

        Args:
            config: Extraction configuration
            output_path: Path to write Parquet file

        Returns:
            ExtractionResult with success status and metadata
        """
        pass

    @abstractmethod
    def extract_hashes(
        self,
        config: ExtractionConfig,
    ) -> Dict[str, str]:
        """Extract row hashes for incremental detection.

        Computes hash of each row using database functions (more efficient
        than extracting all data and hashing in Python).

        Args:
            config: Extraction configuration (must have pk_columns set)

        Returns:
            Dict mapping primary key values to row hashes
        """
        pass

    @abstractmethod
    def get_row_count(
        self,
        schema: str,
        table: str,
        predicates: Optional[List[str]] = None,
    ) -> int:
        """Get count of rows in source table.

        Args:
            schema: Database schema
            table: Table name
            predicates: Optional WHERE predicates

        Returns:
            Row count
        """
        pass

    @abstractmethod
    def get_columns(
        self,
        schema: str,
        table: str,
    ) -> List[Dict[str, str]]:
        """Get column metadata for a table.

        Args:
            schema: Database schema
            table: Table name

        Returns:
            List of dicts with 'name', 'type' keys
        """
        pass

    @abstractmethod
    def detect_primary_key(
        self,
        schema: str,
        table: str,
    ) -> List[str]:
        """Auto-detect primary key columns.

        Args:
            schema: Database schema
            table: Table name

        Returns:
            List of primary key column names
        """
        pass

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
        from dvt.federation.query_optimizer import PushableOperations, QueryOptimizer

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

    def build_hash_query(
        self,
        config: ExtractionConfig,
    ) -> str:
        """Build query to extract primary keys and row hashes.

        Args:
            config: Extraction configuration (must have pk_columns)

        Returns:
            SQL query that returns _pk, _hash columns
        """
        # Default implementation - subclasses override for database-specific hash functions
        if not config.pk_columns:
            raise ValueError("pk_columns required for hash query")

        # Build PK expression (concatenate if composite)
        if len(config.pk_columns) == 1:
            pk_expr = config.pk_columns[0]
        else:
            pk_expr = f"CONCAT({', '.join(config.pk_columns)})"

        # Build hash expression for all columns
        if config.columns:
            cols = config.columns
        else:
            cols = ["*"]  # Will need to be expanded by subclass

        # This is a placeholder - subclasses override with database-specific MD5
        hash_expr = f"MD5(CONCAT({', '.join(cols)}))"

        query = f"""
            SELECT
                CAST({pk_expr} AS VARCHAR) as _pk,
                {hash_expr} as _hash
            FROM {config.schema}.{config.table}
        """

        if config.predicates:
            where_clause = " AND ".join(config.predicates)
            query += f" WHERE {where_clause}"

        return query

    # =========================================================================
    # Pipe-Based Extraction - Tier 1 (CLI tool + PyArrow streaming)
    # =========================================================================

    # CLI tool name for pipe extraction (override in subclass, e.g., "psql")
    cli_tool: Optional[str] = None

    def _has_cli_tool(self) -> bool:
        """Check if the CLI tool for pipe extraction is available on PATH."""
        return self.cli_tool is not None and shutil.which(self.cli_tool) is not None

    def _build_extraction_command(self, config: ExtractionConfig) -> List[str]:
        """Build CLI command for pipe extraction.

        Override in subclass to provide database-specific CLI arguments.

        Args:
            config: Extraction configuration

        Returns:
            Command list for subprocess.Popen
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} does not implement _build_extraction_command"
        )

    def _build_extraction_env(self, config: ExtractionConfig) -> Dict[str, str]:
        """Build environment variables for pipe extraction subprocess.

        Override in subclass to set password env vars (PGPASSWORD, MYSQL_PWD, etc.).

        Args:
            config: Extraction configuration

        Returns:
            Environment dict for subprocess.Popen
        """
        return os.environ.copy()

    def _get_csv_parse_options(self) -> Any:
        """Get PyArrow CSV parse options for this extractor's CLI output.

        Override in subclass if CLI tool outputs non-standard CSV
        (e.g., MySQL --batch outputs tab-delimited).

        Returns:
            pyarrow.csv.ParseOptions or None for defaults
        """
        return None

    def _get_csv_read_options(self, config: ExtractionConfig) -> Any:
        """Get PyArrow CSV read options for this extractor's CLI output.

        Override in subclass if CLI tool does not emit column headers
        (e.g., bcp queryout for SQL Server).

        Args:
            config: Extraction configuration (for column name lookup)

        Returns:
            pyarrow.csv.ReadOptions or None for defaults
        """
        return None

    def _extract_via_pipe(
        self,
        config: ExtractionConfig,
        output_path: Path,
    ) -> ExtractionResult:
        """Extract data via CLI tool pipe + PyArrow streaming CSV-to-Parquet.

        Spawns the database CLI tool as a subprocess, reads its CSV stdout
        through PyArrow's streaming CSV reader in bounded batches, and writes
        each batch to a Parquet file incrementally.

        Memory: ~64KB kernel pipe buffer + ~1-10MB PyArrow batch buffer.

        Args:
            config: Extraction configuration
            output_path: Path to write Parquet file

        Returns:
            ExtractionResult with success status and metadata
        """
        start_time = time.time()

        try:
            import pyarrow.csv as pa_csv
            import pyarrow.parquet as pq
        except ImportError:
            raise ImportError("pyarrow required for pipe extraction. Run 'dvt sync'.")

        cmd = self._build_extraction_command(config)
        env = self._build_extraction_env(config)

        self._log(f"Extracting {config.source_name} via pipe ({self.cli_tool})...")

        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
        )

        try:
            # Check for subclass-specific read options (e.g., bcp no-header fix)
            read_options = self._get_csv_read_options(config)
            if read_options is None:
                read_options = pa_csv.ReadOptions(block_size=1 << 20)  # 1MB blocks

            parse_options = self._get_csv_parse_options()

            open_csv_kwargs: Dict[str, Any] = {"read_options": read_options}
            if parse_options is not None:
                open_csv_kwargs["parse_options"] = parse_options

            reader = pa_csv.open_csv(proc.stdout, **open_csv_kwargs)

            # Ensure output directory exists
            output_path.parent.mkdir(parents=True, exist_ok=True)

            writer = None
            row_count = 0
            try:
                for batch in reader:
                    if writer is None:
                        writer = pq.ParquetWriter(
                            str(output_path),
                            batch.schema,
                            compression="zstd",
                        )
                    writer.write_batch(batch)
                    row_count += batch.num_rows
            finally:
                if writer:
                    writer.close()

        except Exception:
            proc.kill()
            proc.wait()
            raise

        # Capture stderr before wait to avoid potential deadlock
        stderr_output = proc.stderr.read().decode("utf-8", errors="replace")
        proc.wait()
        if proc.returncode != 0:
            raise RuntimeError(
                f"{self.cli_tool} extraction failed (exit {proc.returncode}): "
                f"{stderr_output[:500]}"
            )

        elapsed = time.time() - start_time
        self._log(
            f"Extracted {row_count:,} rows from {config.source_name} "
            f"via pipe ({self.cli_tool}) in {elapsed:.1f}s"
        )

        return ExtractionResult(
            success=True,
            source_name=config.source_name,
            row_count=row_count,
            output_path=output_path,
            extraction_method="pipe",
            elapsed_seconds=elapsed,
        )

    # =========================================================================
    # Spark JDBC Extraction - Default Fallback for All Extractors
    # =========================================================================

    def _extract_jdbc(
        self,
        config: ExtractionConfig,
        output_path: Path,
    ) -> ExtractionResult:
        """Extract using Spark JDBC with parallel reads.

        This is the default fallback for all extractors when native
        export is not available or fails.

        Uses:
        - SparkManager for session and JDBC URL/driver
        - AuthHandler for JDBC authentication properties
        - Configurable partitioning from computes.yml

        Args:
            config: Extraction configuration (must have connection_config)
            output_path: Path to write Parquet files (will be a directory)

        Returns:
            ExtractionResult with success status and metadata
        """
        start_time = time.time()

        try:
            if not config.connection_config:
                raise ValueError(
                    "connection_config required for JDBC extraction. "
                    "Ensure SourceConfig includes the connection dict from profiles.yml."
                )

            # Import here to avoid circular imports and allow graceful failure
            from dvt.federation.spark_manager import SparkManager
            from dvt.federation.auth import get_auth_handler

            adapter_type = config.connection_config.get("type", self.dialect)

            # Get Spark session
            spark_manager = SparkManager.get_instance()
            spark = spark_manager.get_or_create_session()

            # Build JDBC URL and get driver
            jdbc_url = spark_manager.get_jdbc_url(config.connection_config)
            jdbc_driver = spark_manager.get_jdbc_driver(adapter_type)

            if not jdbc_driver:
                raise ValueError(
                    f"No JDBC driver configured for adapter type: {adapter_type}"
                )

            # Get auth handler and validate
            auth_handler = get_auth_handler(adapter_type)

            # Validate auth method (fail early for interactive auth)
            is_valid, error_msg = auth_handler.validate(config.connection_config)
            if not is_valid:
                raise ValueError(error_msg)

            # Get JDBC auth properties
            jdbc_props = auth_handler.get_jdbc_properties(config.connection_config)

            # Build query
            query = self.build_export_query(config)
            dbtable = f"({query}) AS dvt_extract"

            # Get JDBC extraction settings from config
            jdbc_settings = config.jdbc_config or {}
            num_partitions = jdbc_settings.get("num_partitions", 8)
            fetch_size = jdbc_settings.get("fetch_size", 10000)

            # Build JDBC options
            jdbc_options = {
                "url": jdbc_url,
                "driver": jdbc_driver,
                "dbtable": dbtable,
                "fetchsize": str(fetch_size),
                **jdbc_props,
            }

            # Add partitioning if numeric PK available (for parallel reads)
            partitioning_enabled = False
            if config.pk_columns and len(config.pk_columns) == 1:
                pk_col = config.pk_columns[0]
                bounds = self._get_partition_bounds(config.schema, config.table, pk_col)
                if bounds:
                    jdbc_options.update(
                        {
                            "partitionColumn": pk_col,
                            "lowerBound": str(bounds[0]),
                            "upperBound": str(bounds[1]),
                            "numPartitions": str(num_partitions),
                        }
                    )
                    partitioning_enabled = True
                    self._log(
                        f"Using {num_partitions} parallel JDBC readers "
                        f"on column '{pk_col}' (range: {bounds[0]} - {bounds[1]})"
                    )

            if not partitioning_enabled:
                self._log(
                    f"JDBC partitioning not available (no numeric PK), "
                    f"using single reader"
                )

            # Read via JDBC
            self._log(f"Extracting {config.source_name} via Spark JDBC...")
            df = spark.read.format("jdbc").options(**jdbc_options).load()

            # Ensure output directory exists
            output_path.parent.mkdir(parents=True, exist_ok=True)

            # Write to Parquet (directory with multiple part files)
            df.write.mode("overwrite").option("compression", "zstd").parquet(
                str(output_path)
            )

            # Get row count from written data
            row_count = spark.read.parquet(str(output_path)).count()

            elapsed = time.time() - start_time
            self._log(
                f"Extracted {row_count:,} rows from {config.source_name} "
                f"via JDBC in {elapsed:.1f}s"
            )

            # Note: Do NOT stop the Spark session here.
            # The session is shared across all parallel operations within a task.
            # SparkManager.reset() is called at task end (in seed.py, run.py, etc.)

            return ExtractionResult(
                success=True,
                source_name=config.source_name,
                row_count=row_count,
                output_path=output_path,
                extraction_method="jdbc",
                elapsed_seconds=elapsed,
            )

        except ImportError as e:
            elapsed = time.time() - start_time
            error_msg = (
                f"PySpark not available for JDBC extraction: {e}. "
                f"Run 'dvt sync' to install PySpark."
            )
            self._log(error_msg)
            return ExtractionResult(
                success=False,
                source_name=config.source_name,
                error=error_msg,
                elapsed_seconds=elapsed,
            )

        except Exception as e:
            elapsed = time.time() - start_time
            self._log(f"JDBC extraction failed: {e}")
            return ExtractionResult(
                success=False,
                source_name=config.source_name,
                error=str(e),
                elapsed_seconds=elapsed,
            )

    def _get_partition_bounds(
        self,
        schema: str,
        table: str,
        column: str,
    ) -> Optional[Tuple[int, int]]:
        """Get min/max values for JDBC partitioning.

        Only works for numeric columns. Returns None if:
        - Column is not numeric
        - Table is empty
        - Query fails

        Args:
            schema: Database schema
            table: Table name
            column: Column to get bounds for

        Returns:
            Tuple of (min, max) values or None
        """
        try:
            cursor = self.connection.cursor()
            cursor.execute(f"SELECT MIN({column}), MAX({column}) FROM {schema}.{table}")
            result = cursor.fetchone()
            cursor.close()

            if result and result[0] is not None and result[1] is not None:
                # Try to convert to int (works for int, bigint, serial, etc.)
                try:
                    return (int(result[0]), int(result[1]))
                except (ValueError, TypeError):
                    # Column might be numeric but not integer (float, decimal)
                    # Still try to use it for partitioning
                    return (int(float(result[0])), int(float(result[1])))
        except Exception:
            # Query failed or column not suitable for partitioning
            pass

        return None
