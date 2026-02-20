# coding=utf-8
"""Spark session manager for DVT federation.

This module manages Spark sessions for DVT operations including:
- Federated query execution
- Spark-based seed loading
- Cross-target data transfer
- Cloud storage access (S3, GCS, Azure)

The SparkManager provides a singleton-style session management with
configurable Spark settings from computes.yml and cloud storage credentials
from buckets.yml.

Usage:
    # At task start (dvt seed, dvt run)
    SparkManager.initialize(compute_config, bucket_configs)
    SparkManager.get_instance().get_or_create_session("DVT-Seed")

    # During task execution (extractors, loaders, runners)
    spark_manager = SparkManager.get_instance()
    spark = spark_manager.get_or_create_session()

    # At task end
    SparkManager.reset()
"""

import os
import threading
from pathlib import Path
from typing import Any, Dict, List, Optional

from dvt.config.user_config import get_spark_jars_dir


class SparkManager:
    """Manages Spark sessions for DVT federation operations.

    Handles:
    - Session creation with appropriate JAR classpath (JDBC, native, cloud connectors)
    - Session configuration from computes.yml
    - Cloud storage credential configuration from buckets.yml
    - Session lifecycle (create, get, stop)

    Thread Safety:
    - One Spark session is shared across all threads within a task
    - A lock protects session creation and destruction
    - Spark internally handles concurrent read/write operations
    """

    _instance: Optional["SparkManager"] = None
    _session: Optional[Any] = None  # SparkSession
    _lock: threading.Lock = threading.Lock()

    def __init__(
        self,
        config: Optional[Dict[str, Any]] = None,
        bucket_configs: Optional[Dict[str, Dict[str, Any]]] = None,
    ):
        """Initialize SparkManager with optional configuration.

        Note: Do not call this directly. Use SparkManager.initialize() instead.

        Args:
            config: Spark configuration dict from computes.yml, e.g.:
                {
                    "master": "local[*]",
                    "version": "3.5.0",
                    "config": {
                        "spark.driver.memory": "2g",
                        "spark.sql.adaptive.enabled": "true"
                    }
                }
            bucket_configs: All bucket configs from buckets.yml for this profile, e.g.:
                {
                    "s3_staging": {"type": "s3", "bucket": "...", ...},
                    "gcs_staging": {"type": "gcs", ...},
                }
        """
        self.config = config or {}
        self.bucket_configs = bucket_configs or {}
        # Delta Lake table properties (resolved from user-friendly names in computes.yml)
        self._delta_config = self._resolve_delta_config()

    # --- User-friendly name -> Delta property mapping ---
    # These translate the simple names in computes.yml delta: section
    # to the actual Delta Lake property names used internally.
    _DELTA_CONFIG_MAP = {
        "optimize_on_write": "delta.autoOptimize.optimizeWrite",
        "cleanup_retention_hours": "delta.deletedFileRetentionDuration",
        "parallel_cleanup": "spark.databricks.delta.vacuum.parallelDelete.enabled",
    }

    def _resolve_delta_config(self) -> Dict[str, Any]:
        """Resolve user-friendly delta config names to Delta Lake properties.

        Reads the 'delta' section from the compute config and maps simple names
        (e.g., 'optimize_on_write') to actual Delta properties.

        Returns:
            Dict with two keys:
                'table_properties': dict of Delta table properties to apply on writes
                'session_properties': dict of Spark session-level properties
        """
        delta_section = self.config.get("delta", {})
        if not delta_section or not isinstance(delta_section, dict):
            return {"table_properties": {}, "session_properties": {}}

        table_props: Dict[str, str] = {}
        session_props: Dict[str, str] = {}

        for user_key, value in delta_section.items():
            if user_key not in self._DELTA_CONFIG_MAP:
                continue  # Unknown key, skip silently

            delta_key = self._DELTA_CONFIG_MAP[user_key]

            if user_key == "cleanup_retention_hours":
                # Convert hours to Delta interval format
                hours = int(value)
                table_props[delta_key] = f"interval {hours} hours"
            elif user_key == "parallel_cleanup":
                # Session-level property (not a table property)
                session_props[delta_key] = str(value).lower()
            else:
                # Boolean table properties
                table_props[delta_key] = str(value).lower()

        return {"table_properties": table_props, "session_properties": session_props}

    @property
    def delta_table_properties(self) -> Dict[str, str]:
        """Delta table properties to apply when writing Delta staging tables.

        Returns dict like:
            {"delta.autoOptimize.optimizeWrite": "true"}
        """
        return self._delta_config.get("table_properties", {})

    @property
    def delta_session_properties(self) -> Dict[str, str]:
        """Delta session-level Spark properties.

        Returns dict like:
            {"spark.databricks.delta.vacuum.parallelDelete.enabled": "true"}
        """
        return self._delta_config.get("session_properties", {})

    @property
    def cleanup_retention_hours(self) -> int:
        """Get cleanup retention in hours for VACUUM.

        Returns the user-configured value, or 0 if not set (delete immediately).
        """
        delta_section = self.config.get("delta", {})
        if isinstance(delta_section, dict):
            return int(delta_section.get("cleanup_retention_hours", 0))
        return 0

    @classmethod
    def initialize(
        cls,
        config: Optional[Dict[str, Any]] = None,
        bucket_configs: Optional[Dict[str, Dict[str, Any]]] = None,
    ) -> None:
        """Initialize the SparkManager singleton. Call once at task start.

        This must be called before any code uses get_instance().
        Typically called at the start of dvt seed or dvt run.

        Args:
            config: Spark configuration dict from computes.yml
            bucket_configs: Bucket configurations from buckets.yml

        Raises:
            RuntimeError: If already initialized (call reset() first)
        """
        with cls._lock:
            if cls._instance is not None:
                raise RuntimeError(
                    "SparkManager already initialized. "
                    "Call SparkManager.reset() before re-initializing."
                )
            cls._instance = SparkManager(config=config, bucket_configs=bucket_configs)

    @classmethod
    def get_instance(cls) -> "SparkManager":
        """Get the SparkManager singleton instance.

        Returns:
            SparkManager singleton instance

        Raises:
            RuntimeError: If not initialized (call initialize() first)
        """
        if cls._instance is None:
            raise RuntimeError(
                "SparkManager not initialized. Call SparkManager.initialize() first."
            )
        return cls._instance

    @classmethod
    def reset(cls) -> None:
        """Reset the SparkManager singleton. Call at task end.

        Stops the Spark session if running and clears the singleton instance.
        After calling reset(), initialize() can be called again.
        """
        with cls._lock:
            if cls._instance is not None:
                cls._instance._stop_session_internal()
                cls._instance = None

    @classmethod
    def is_initialized(cls) -> bool:
        """Check if SparkManager has been initialized.

        Returns:
            True if initialized, False otherwise
        """
        return cls._instance is not None

    def get_or_create_session(self, app_name: str = "DVT") -> Any:
        """Get existing or create new Spark session.

        Thread-safe: uses lock to prevent multiple threads from creating
        sessions simultaneously.

        Args:
            app_name: Name for the Spark application

        Returns:
            SparkSession instance
        """
        # Fast path: session already exists
        if SparkManager._session is not None:
            return SparkManager._session

        # Slow path: need to create session (with lock)
        with SparkManager._lock:
            # Double-check after acquiring lock
            if SparkManager._session is not None:
                return SparkManager._session

            # Set JAVA_HOME from compute config before JVM starts.
            # Must happen before any PySpark import triggers JVM initialization.
            java_home = self.config.get("java_home")
            if java_home:
                java_path = Path(java_home).expanduser().resolve()
                if java_path.is_dir():
                    os.environ["JAVA_HOME"] = str(java_path)
                else:
                    raise RuntimeError(
                        f"java_home in computes.yml points to a directory that does not exist: "
                        f"{java_path}\n"
                        f"Please fix the path or comment it out to use the system default."
                    )

            try:
                from pyspark.sql import SparkSession
            except ImportError:
                raise ImportError(
                    "PySpark is not installed. Run 'dvt sync' to install PySpark, "
                    "or install it manually with: pip install pyspark"
                )

            # Build Spark JARs classpath (JDBC drivers, native connectors, cloud connectors)
            spark_jars = self._get_spark_jars()
            extra_jars = ",".join(spark_jars) if spark_jars else ""

            # Get master from config or default to local
            master = self.config.get("master", "local[*]")

            # Build Spark session
            builder = SparkSession.builder.appName(app_name).master(master)

            # Add Spark JARs to classpath
            if extra_jars:
                builder = builder.config("spark.jars", extra_jars)

            # Apply additional config from computes.yml
            spark_config = self.config.get("config", {})
            for key, value in spark_config.items():
                builder = builder.config(key, str(value))

            # Configure cloud storage credentials for all buckets
            cloud_config = self._get_cloud_storage_config()
            for key, value in cloud_config.items():
                builder = builder.config(key, str(value))

            # Configure Delta Lake (if delta-spark is installed).
            # configure_spark_with_delta_pip() sets up the classpath for the
            # Delta JARs bundled with the pip package. We also need the extension
            # and catalog configs for Delta read/write operations.
            try:
                from delta import configure_spark_with_delta_pip

                builder = builder.config(
                    "spark.sql.extensions",
                    "io.delta.sql.DeltaSparkSessionExtension",
                ).config(
                    "spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog",
                )
                builder = configure_spark_with_delta_pip(builder)

                # Apply Delta session-level properties from computes.yml delta: section
                # (e.g., spark.databricks.delta.vacuum.parallelDelete.enabled)
                for key, value in self.delta_session_properties.items():
                    builder = builder.config(key, value)
            except ImportError:
                pass  # delta-spark not installed; Delta staging not available

            # Create session
            SparkManager._session = builder.getOrCreate()

            # Set log level to reduce noise
            SparkManager._session.sparkContext.setLogLevel("WARN")

            return SparkManager._session

    def _stop_session_internal(self) -> None:
        """Stop the current Spark session. Internal use only.

        Called by reset(). Do not call directly from extractors/loaders.
        """
        if SparkManager._session is not None:
            try:
                SparkManager._session.stop()
            except Exception:
                pass  # Ignore errors on cleanup
            SparkManager._session = None

    def _get_spark_jars(self) -> List[str]:
        """Get list of Spark JAR paths (JDBC drivers, native connectors, cloud connectors)."""
        jdbc_dir = get_spark_jars_dir(None)
        if not jdbc_dir.exists():
            return []

        jars = []
        for jar_file in jdbc_dir.glob("*.jar"):
            jars.append(str(jar_file))
        return jars

    def _get_cloud_storage_config(self) -> Dict[str, str]:
        """Get Spark Hadoop config for all configured cloud buckets.

        Reads bucket configurations and builds spark.hadoop.* config entries
        for S3, GCS, and Azure storage access.

        Returns:
            Dict of Spark config key-value pairs for cloud storage
        """
        if not self.bucket_configs:
            return {}

        from dvt.federation.cloud_storage import CloudStorageHelper

        config: Dict[str, str] = {}

        for bucket_name, bucket_config in self.bucket_configs.items():
            if not bucket_config or not isinstance(bucket_config, dict):
                continue
            bucket_type = bucket_config.get("type", "filesystem")
            if bucket_type in ("s3", "gcs", "azure"):
                helper = CloudStorageHelper(bucket_config)
                bucket_hadoop_config = helper.get_spark_hadoop_config()
                config.update(bucket_hadoop_config)

        return config

    def get_jdbc_url(self, connection: Dict[str, Any]) -> str:
        """Build JDBC URL from connection config.

        Args:
            connection: Database connection config from profiles.yml

        Returns:
            JDBC URL string
        """
        adapter_type = connection.get("type", "").lower()

        if adapter_type == "postgres":
            host = connection.get("host") or "localhost"
            port = connection.get("port") or 5432
            database = connection.get("database") or "postgres"
            return f"jdbc:postgresql://{host}:{port}/{database}"

        elif adapter_type == "snowflake":
            account = connection.get("account", "")
            database = connection.get("database", "")
            schema = connection.get("schema", "public")
            warehouse = connection.get("warehouse", "")
            url = f"jdbc:snowflake://{account}.snowflakecomputing.com/?db={database}&schema={schema}"
            if warehouse:
                url += f"&warehouse={warehouse}"
            return url

        elif adapter_type == "databricks":
            host = connection.get("host") or ""
            http_path = connection.get("http_path") or ""
            catalog = connection.get("catalog") or ""
            schema = connection.get("schema") or "default"
            # Build Databricks JDBC URL with all required parameters
            url = f"jdbc:databricks://{host}:443"
            params = [f"httpPath={http_path}"]
            if catalog:
                params.append(f"ConnCatalog={catalog}")
            if schema:
                params.append(f"ConnSchema={schema}")
            return url + ";" + ";".join(params)

        elif adapter_type == "bigquery":
            project = connection.get("project", "")
            return f"jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443;ProjectId={project}"

        elif adapter_type == "redshift":
            host = connection.get("host") or ""
            port = connection.get("port") or 5439
            database = connection.get("database") or ""
            return f"jdbc:redshift://{host}:{port}/{database}"

        elif adapter_type == "mysql":
            host = connection.get("host") or "localhost"
            port = connection.get("port") or 3306
            database = connection.get("database") or ""
            return f"jdbc:mysql://{host}:{port}/{database}"

        elif adapter_type == "sqlserver":
            host = connection.get("host") or ""
            port = connection.get("port") or 1433
            database = connection.get("database") or ""
            return f"jdbc:sqlserver://{host}:{port};databaseName={database}"

        elif adapter_type == "oracle":
            host = connection.get("host") or ""
            port = connection.get("port") or 1521
            database = connection.get("database") or ""
            return f"jdbc:oracle:thin:@{host}:{port}/{database}"

        elif adapter_type == "trino" or adapter_type == "starburst":
            host = connection.get("host", "")
            port = connection.get("port", 8080)
            catalog = connection.get("catalog", "")
            schema = connection.get("schema", "")
            return f"jdbc:trino://{host}:{port}/{catalog}/{schema}"

        elif adapter_type == "hive":
            host = connection.get("host") or "localhost"
            port = connection.get("port") or 10000
            database = (
                connection.get("database") or connection.get("schema") or "default"
            )
            return f"jdbc:hive2://{host}:{port}/{database}"

        elif adapter_type == "impala":
            host = connection.get("host") or "localhost"
            port = connection.get("port") or 21050
            database = (
                connection.get("database") or connection.get("schema") or "default"
            )
            # Impala auth mechanism in URL
            auth_mech = "3" if connection.get("password") else "0"
            return f"jdbc:impala://{host}:{port}/{database};AuthMech={auth_mech}"

        elif adapter_type == "clickhouse":
            host = connection.get("host") or "localhost"
            port = connection.get("port") or 8123
            database = connection.get("database") or "default"
            return f"jdbc:clickhouse://{host}:{port}/{database}"

        elif adapter_type == "duckdb":
            path = connection.get("path") or connection.get("database") or ":memory:"
            return f"jdbc:duckdb:{path}"

        elif adapter_type == "exasol":
            host = connection.get("host") or "localhost"
            port = connection.get("port") or 8563
            return f"jdbc:exa:{host}:{port}"

        elif adapter_type == "vertica":
            host = connection.get("host") or "localhost"
            port = connection.get("port") or 5433
            database = connection.get("database") or ""
            return f"jdbc:vertica://{host}:{port}/{database}"

        elif adapter_type == "firebolt":
            database = connection.get("database") or ""
            engine = connection.get("engine_name") or ""
            account = connection.get("account_name") or ""
            return f"jdbc:firebolt://?database={database}&engine={engine}&account={account}"

        elif adapter_type == "db2":
            host = connection.get("host") or "localhost"
            port = connection.get("port") or 50000
            database = connection.get("database") or ""
            return f"jdbc:db2://{host}:{port}/{database}"

        elif adapter_type == "athena":
            region = (
                connection.get("region_name") or connection.get("region") or "us-east-1"
            )
            s3_staging = connection.get("s3_staging_dir") or ""
            return f"jdbc:awsathena://athena.{region}.amazonaws.com:443?S3OutputLocation={s3_staging}"

        elif adapter_type in ("synapse", "fabric"):
            # Azure Synapse and Fabric use SQL Server driver
            host = connection.get("host") or ""
            port = connection.get("port") or 1433
            database = connection.get("database") or ""
            return f"jdbc:sqlserver://{host}:{port};databaseName={database}"

        elif adapter_type in (
            "greenplum",
            "materialize",
            "risingwave",
            "cratedb",
            "alloydb",
            "timescaledb",
        ):
            # PostgreSQL-compatible databases
            host = connection.get("host") or "localhost"
            port = connection.get("port") or 5432
            database = connection.get("database") or "postgres"
            return f"jdbc:postgresql://{host}:{port}/{database}"

        elif adapter_type in ("tidb", "singlestore"):
            # MySQL-compatible databases
            host = connection.get("host") or "localhost"
            port = connection.get("port") or (4000 if adapter_type == "tidb" else 3306)
            database = connection.get("database") or ""
            return f"jdbc:mysql://{host}:{port}/{database}"

        else:
            raise ValueError(f"Unsupported adapter type for JDBC: {adapter_type}")

    def get_jdbc_driver(self, adapter_type: str) -> str:
        """Get JDBC driver class name for an adapter type.

        Args:
            adapter_type: Database adapter type (e.g., 'postgres', 'snowflake')

        Returns:
            JDBC driver class name
        """
        drivers = {
            # Tier 1: Official dbt-labs adapters
            "postgres": "org.postgresql.Driver",
            "snowflake": "net.snowflake.client.jdbc.SnowflakeDriver",
            "databricks": "com.databricks.client.jdbc.Driver",
            "bigquery": "com.simba.googlebigquery.jdbc.Driver",
            "redshift": "com.amazon.redshift.jdbc42.Driver",
            # PostgreSQL-compatible
            "greenplum": "org.postgresql.Driver",
            "materialize": "org.postgresql.Driver",
            "risingwave": "org.postgresql.Driver",
            "cratedb": "org.postgresql.Driver",
            "alloydb": "org.postgresql.Driver",
            "timescaledb": "org.postgresql.Driver",
            # MySQL-compatible
            "mysql": "com.mysql.cj.jdbc.Driver",
            "tidb": "com.mysql.cj.jdbc.Driver",
            "singlestore": "com.singlestore.jdbc.Driver",
            # Microsoft
            "sqlserver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
            "synapse": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
            "fabric": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
            # Hadoop ecosystem
            "hive": "org.apache.hive.jdbc.HiveDriver",
            "impala": "com.cloudera.impala.jdbc.Driver",
            # Others
            "oracle": "oracle.jdbc.OracleDriver",
            "trino": "io.trino.jdbc.TrinoDriver",
            "starburst": "io.trino.jdbc.TrinoDriver",
            "clickhouse": "com.clickhouse.jdbc.ClickHouseDriver",
            "duckdb": "org.duckdb.DuckDBDriver",
            "exasol": "com.exasol.jdbc.EXADriver",
            "vertica": "com.vertica.jdbc.Driver",
            "firebolt": "com.firebolt.FireboltDriver",
            "db2": "com.ibm.db2.jcc.DB2Driver",
            "athena": "com.amazonaws.athena.jdbc.AthenaDriver",
        }
        return drivers.get(adapter_type.lower(), "")
