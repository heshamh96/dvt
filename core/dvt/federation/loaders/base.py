"""
Base loader class for EL layer.

Loaders are responsible for loading DataFrames into target databases.

Load priority:
1. DDL via dbt adapter (proper quoting, connection management)
2. Data via Spark JDBC (parallel writes)
3. Optional: Bulk load from cloud storage (COPY INTO)

Usage:
    from dvt.federation.loaders import get_loader
    from dvt.federation.adapter_manager import AdapterManager

    adapter = AdapterManager.get_adapter(profile, target, profiles_dir)
    loader = get_loader(adapter_type, on_progress=print)

    result = loader.load(df, config, adapter=adapter)
"""

import os
import shutil
import subprocess
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional


@dataclass
class LoadConfig:
    """Configuration for a single load operation."""

    table_name: str  # Fully qualified table name (schema.table or catalog.schema.table)
    mode: str = "overwrite"  # 'overwrite', 'append', 'ignore', 'error'
    truncate: bool = True  # Use TRUNCATE instead of DROP for overwrite
    full_refresh: bool = False  # --full-refresh: DROP + CREATE + INSERT
    connection_config: Optional[Dict[str, Any]] = None  # profiles.yml connection
    jdbc_config: Optional[Dict[str, Any]] = None  # jdbc_load settings from computes.yml
    bucket_config: Optional[Dict[str, Any]] = None  # For staging (bulk load)
    streaming_batch_size: int = 10000  # Rows per batch for streaming load


@dataclass
class LoadResult:
    """Result of a load operation."""

    success: bool
    table_name: str
    row_count: int = 0
    error: Optional[str] = None
    load_method: str = "jdbc"  # 'jdbc', 'copy', 'bulk_load'
    elapsed_seconds: float = 0.0


class BaseLoader(ABC):
    """Base class for database loaders.

    Provides:
    - DDL execution via dbt adapter (proper quoting per dialect)
    - Data loading via Spark JDBC (parallel writes)
    - Optional bulk load for cloud storage

    Subclasses override load() to add database-specific optimizations
    (e.g., PostgreSQL COPY FROM, Snowflake COPY INTO).
    """

    # Adapter types this loader handles (set by subclass)
    adapter_types: List[str] = []

    def __init__(
        self,
        on_progress: Optional[Callable[[str], None]] = None,
    ):
        """Initialize loader.

        Args:
            on_progress: Optional callback for progress messages
        """
        self.on_progress = on_progress or (lambda msg: None)

    def _log(self, message: str) -> None:
        """Log a progress message."""
        self.on_progress(message)

    def supports_bulk_load(self, bucket_type: str) -> bool:
        """Check if this loader supports bulk load from bucket type.

        Cloud loaders (Snowflake, BigQuery, Redshift, etc.) override
        this to return True for their supported cloud storage types.
        """
        return False

    def get_bulk_load_bucket_types(self) -> List[str]:
        """Get list of bucket types this loader can bulk load from."""
        return []

    @abstractmethod
    def load(
        self,
        df: Any,  # pyspark.sql.DataFrame
        config: LoadConfig,
        adapter: Optional[Any] = None,
    ) -> LoadResult:
        """Load DataFrame into target database.

        Args:
            df: PySpark DataFrame to load
            config: Load configuration
            adapter: Optional dbt adapter for DDL operations

        Returns:
            LoadResult with success status and metadata
        """
        pass

    # =========================================================================
    # DDL Operations via Adapter
    # =========================================================================

    def _safe_commit(self, adapter: Any) -> None:
        """Commit the adapter connection, tolerating aborted transaction state.

        On PostgreSQL, a failed SQL statement aborts the current transaction.
        Calling commit() on an aborted transaction raises an error or silently
        rolls back.  This helper catches that error and resets the connection
        via rollback so subsequent operations can proceed.
        """
        try:
            adapter.connections.commit()
        except Exception:
            try:
                adapter.connections.rollback()
            except Exception:
                pass  # Connection may already be clean

    def _execute_ddl(
        self,
        adapter: Any,
        config: LoadConfig,
    ) -> None:
        """Execute DDL operations via adapter.

        Handles table preparation (TRUNCATE or DROP+CREATE) using
        properly quoted identifiers from the adapter.

        Args:
            adapter: dbt adapter instance
            config: Load configuration
        """
        from dvt.federation.adapter_manager import get_quoted_table_name

        quoted_table = get_quoted_table_name(adapter, config.table_name)

        with adapter.connection_named("dvt_loader"):
            if config.full_refresh:
                # Full refresh: DROP + CREATE (Spark will create)
                self._log(f"Dropping {config.table_name}...")
                try:
                    adapter.execute(f"DROP TABLE IF EXISTS {quoted_table} CASCADE")
                except Exception:
                    # Try without CASCADE (some DBs don't support it)
                    try:
                        adapter.execute(f"DROP TABLE IF EXISTS {quoted_table}")
                    except Exception:
                        pass  # Table might not exist
            elif config.truncate:
                # Truncate: faster than DROP+CREATE, preserves structure
                self._log(f"Truncating {config.table_name}...")
                try:
                    adapter.execute(f"TRUNCATE TABLE {quoted_table}")
                except Exception:
                    # Table might not exist - will be created by Spark
                    pass
            # Commit DDL so it's visible to other connections (COPY, JDBC)
            self._safe_commit(adapter)

    def _ensure_schema_exists(
        self,
        adapter: Any,
        config: LoadConfig,
    ) -> None:
        """Ensure the target schema exists.

        Args:
            adapter: dbt adapter instance
            config: Load configuration
        """
        from dvt.federation.adapter_manager import parse_table_name

        parts = parse_table_name(config.table_name)
        schema = parts.get("schema")

        if schema:
            with adapter.connection_named("dvt_loader"):
                try:
                    adapter.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
                except Exception:
                    pass  # Schema might already exist or we might not have permissions
                self._safe_commit(adapter)

    def _create_table_with_adapter(
        self,
        adapter: Any,
        df: Any,  # pyspark.sql.DataFrame
        config: LoadConfig,
    ) -> None:
        """Create target table via adapter DDL with properly quoted columns.

        Generates a CREATE TABLE IF NOT EXISTS statement using dialect-aware
        quoting for both the table name and all column names. This preserves
        original column names (including spaces, special characters) by
        wrapping them in dialect-appropriate quotes.

        Args:
            adapter: dbt adapter instance
            df: PySpark DataFrame whose schema defines the table structure
            config: Load configuration with table_name
        """
        from dvt.federation.adapter_manager import get_quoted_table_name
        from dvt.utils.identifiers import build_create_table_sql

        adapter_type = adapter.type()
        quoted_table = get_quoted_table_name(adapter, config.table_name)
        create_sql = build_create_table_sql(df, adapter_type, quoted_table)

        self._log(f"Creating table {config.table_name} via adapter DDL...")
        with adapter.connection_named("dvt_loader"):
            try:
                adapter.execute(create_sql)
            except Exception as e:
                # Table might already exist (IF NOT EXISTS not supported everywhere)
                self._log(f"Create table note: {e}")
            # Commit DDL so it's visible to other connections (COPY, JDBC)
            self._safe_commit(adapter)

    # =========================================================================
    # Pipe-Based Loading - Tier 1 (CLI tool + PyArrow streaming)
    # =========================================================================

    # CLI tool name for pipe loading (override in subclass, e.g., "psql")
    cli_tool: Optional[str] = None

    def _has_cli_tool(self) -> bool:
        """Check if the CLI tool for pipe loading is available on PATH."""
        return self.cli_tool is not None and shutil.which(self.cli_tool) is not None

    def _build_load_command(self, config: LoadConfig) -> List[str]:
        """Build CLI command for pipe loading.

        Override in subclass to provide database-specific CLI arguments.

        Args:
            config: Load configuration

        Returns:
            Command list for subprocess.Popen
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} does not implement _build_load_command"
        )

    def _build_load_env(self, config: LoadConfig) -> Dict[str, str]:
        """Build environment variables for pipe loading subprocess.

        Override in subclass to set password env vars (PGPASSWORD, MYSQL_PWD, etc.).

        Args:
            config: Load configuration

        Returns:
            Environment dict for subprocess.Popen
        """
        return os.environ.copy()

    def _get_csv_write_delimiter(self) -> str:
        """Get CSV delimiter for pipe loading output.

        Override in subclass if target DB CLI expects non-standard delimiter
        (e.g., tab-delimited for MySQL LOAD DATA).

        Returns:
            Delimiter character
        """
        return ","

    def _load_via_pipe(
        self,
        parquet_path: str,
        config: LoadConfig,
    ) -> int:
        """Load data via PyArrow streaming Parquet-to-CSV + CLI tool pipe.

        Reads a Parquet file in batches using PyArrow, converts each batch
        to CSV, and streams it to the target database's CLI tool via stdin.

        Memory: ~1-10MB (one PyArrow RecordBatch + CSV write buffer).

        Args:
            parquet_path: Path to the Parquet file/directory to load
            config: Load configuration

        Returns:
            Number of rows loaded

        Raises:
            RuntimeError: If the CLI tool process exits with error
            ImportError: If pyarrow is not available
        """
        try:
            import pyarrow as pa
            import pyarrow.csv as pa_csv
            import pyarrow.parquet as pq
        except ImportError:
            raise ImportError("pyarrow required for pipe loading. Run 'dvt sync'.")

        cmd = self._build_load_command(config)
        env = self._build_load_env(config)
        delimiter = self._get_csv_write_delimiter()

        self._log(f"Loading {config.table_name} via pipe ({self.cli_tool})...")

        proc = subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
        )

        try:
            # Handle both single Parquet files and Spark-style directories
            parquet_path_obj = Path(parquet_path)
            if parquet_path_obj.is_dir():
                # Spark writes Parquet as a directory with part files.
                # Use ParquetDataset to stream batches across all part files.
                import pyarrow.dataset as ds

                dataset = ds.dataset(parquet_path, format="parquet")
                batches = dataset.to_batches(batch_size=65536)
            else:
                pf = pq.ParquetFile(parquet_path)
                batches = pf.iter_batches(batch_size=65536)

            first_batch = True
            row_count = 0

            for batch in batches:
                sink = pa.BufferOutputStream()
                write_options = pa_csv.WriteOptions(
                    include_header=first_batch,
                    delimiter=delimiter,
                )
                pa_csv.write_csv(
                    pa.Table.from_batches([batch]),
                    sink,
                    write_options=write_options,
                )
                proc.stdin.write(sink.getvalue().to_pybytes())
                row_count += len(batch)
                first_batch = False

            proc.stdin.close()

        except Exception:
            proc.kill()
            proc.wait()
            raise

        proc.wait()
        if proc.returncode != 0:
            stderr_output = proc.stderr.read().decode("utf-8", errors="replace")
            raise RuntimeError(
                f"{self.cli_tool} load failed (exit {proc.returncode}): "
                f"{stderr_output[:500]}"
            )

        return row_count

    # =========================================================================
    # Spark JDBC Load - Default Data Loading Method
    # =========================================================================

    def _load_jdbc(
        self,
        df: Any,  # pyspark.sql.DataFrame
        config: LoadConfig,
        adapter: Optional[Any] = None,
    ) -> LoadResult:
        """Load using Spark JDBC with parallel writes.

        If an adapter is provided, DDL is executed via adapter first,
        then data is loaded via Spark JDBC in append mode.

        If no adapter is provided, falls back to pure Spark JDBC
        which handles DDL internally.

        Args:
            df: PySpark DataFrame to load
            config: Load configuration
            adapter: Optional dbt adapter for DDL

        Returns:
            LoadResult with success status and metadata
        """
        start_time = time.time()

        try:
            if not config.connection_config:
                raise ValueError("connection_config required for JDBC load")

            from dvt.federation.auth import get_auth_handler
            from dvt.federation.spark_manager import SparkManager

            adapter_type = config.connection_config.get("type", "")

            spark_manager = SparkManager.get_instance()
            jdbc_url = spark_manager.get_jdbc_url(config.connection_config)
            jdbc_driver = spark_manager.get_jdbc_driver(adapter_type)

            if not jdbc_driver:
                raise ValueError(f"No JDBC driver for adapter: {adapter_type}")

            # Get auth handler and validate
            auth_handler = get_auth_handler(adapter_type)
            is_valid, error_msg = auth_handler.validate(config.connection_config)
            if not is_valid:
                raise ValueError(error_msg)

            # Get JDBC auth properties
            jdbc_props = auth_handler.get_jdbc_properties(config.connection_config)

            # Get JDBC load settings from config
            jdbc_settings = config.jdbc_config or {}
            num_partitions = jdbc_settings.get("num_partitions", 4)
            batch_size = jdbc_settings.get("batch_size", 10000)

            # Build JDBC properties
            properties = {
                **jdbc_props,
                "driver": jdbc_driver,
                "batchsize": str(batch_size),
            }

            # Determine mode and DDL handling
            if adapter and config.mode == "overwrite":
                # DDL via adapter (proper quoting), data via Spark JDBC append
                self._execute_ddl(adapter, config)
                # Create table with properly quoted column names via adapter
                self._create_table_with_adapter(adapter, df, config)
                write_mode = "append"  # Table cleared by DDL, just append
            else:
                # Pure Spark JDBC mode
                write_mode = config.mode
                # Set truncate property for Spark to handle
                if (
                    config.mode == "overwrite"
                    and config.truncate
                    and not config.full_refresh
                ):
                    properties["truncate"] = "true"

            # Repartition for parallel writes
            if num_partitions > 1:
                df = df.repartition(num_partitions)
                self._log(f"Using {num_partitions} parallel JDBC writers")

            self._log(f"Loading {config.table_name} via Spark JDBC...")
            df.write.jdbc(
                url=jdbc_url,
                table=config.table_name,
                mode=write_mode,
                properties=properties,
            )

            row_count = df.count()
            elapsed = time.time() - start_time

            self._log(f"Loaded {row_count:,} rows via JDBC in {elapsed:.1f}s")

            return LoadResult(
                success=True,
                table_name=config.table_name,
                row_count=row_count,
                load_method="jdbc",
                elapsed_seconds=elapsed,
            )

        except Exception as e:
            elapsed = time.time() - start_time
            self._log(f"JDBC load failed: {e}")
            return LoadResult(
                success=False,
                table_name=config.table_name,
                error=str(e),
                elapsed_seconds=elapsed,
            )
