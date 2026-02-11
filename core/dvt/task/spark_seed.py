# coding=utf-8
"""Spark-based seed runner for DVT.

This module provides a Spark-based implementation for loading seed files.
DVT always uses Spark for seeding to provide:
- Multi-format support: CSV, Parquet, JSON, ORC
- Better performance with large files via bulk load or JDBC
- Cross-target seeding support (seed to any target in profiles.yml)
- Consistent federation architecture
- Column type overrides via seed config
- Native bulk load for Snowflake/BigQuery/Redshift/Databricks/PostgreSQL

Supported seed file formats:
- .csv      - Comma-separated values (default, with header)
- .parquet  - Apache Parquet columnar format
- .json     - JSON Lines format (one object per line)
- .jsonl    - JSON Lines format (alias for .json)
- .orc      - Apache ORC columnar format

Usage:
    dvt seed                          # Use default compute and target
    dvt seed --compute my_cluster     # Specify compute engine
    dvt seed --target prod            # Specify destination target
    dvt seed -c spark_local -t dev    # Both flags
"""

import sys
from pathlib import Path
from typing import Any, Dict, Optional

from dvt.artifacts.schemas.results import NodeStatus, RunStatus
from dvt.config import RuntimeConfig
from dvt.contracts.graph.manifest import Manifest
from dvt.contracts.graph.nodes import SeedNode
from dvt.events.types import LogSeedResult, LogStartLine
from dvt.task import group_lookup
from dvt.task.base import BaseRunner
from dbt_common.events.base_types import EventLevel
from dbt_common.events.functions import fire_event
from dbt_common.ui import green, red, yellow


def _log(msg: str) -> None:
    """Write message to stderr."""
    sys.stderr.write(msg + "\n")
    sys.stderr.flush()


def is_spark_available() -> bool:
    """Check if PySpark is available."""
    try:
        from pyspark.sql import SparkSession  # noqa: F401

        return True
    except ImportError:
        return False


# Supported seed file formats (built-in Spark formats only, no Avro/Delta)
SUPPORTED_SEED_FORMATS = {".csv", ".parquet", ".json", ".jsonl", ".orc"}


class SparkSeedRunner(BaseRunner):
    """Spark-based seed runner for DVT.

    Uses Spark to load seed files (CSV, Parquet, JSON, ORC) and write to target
    databases using bulk load (for supported adapters) or JDBC fallback.
    Supports --compute and --target CLI flags for flexible seeding.

    Load priority:
    1. Native bulk load (PostgreSQL COPY, Snowflake/BigQuery/Redshift/Databricks)
    2. Spark JDBC write (fallback for all other adapters)
    """

    def __init__(
        self,
        config: RuntimeConfig,
        adapter: Any,
        node: SeedNode,
        node_index: int,
        num_nodes: int,
    ):
        super().__init__(config, adapter, node, node_index, num_nodes)
        self.spark_manager = None
        self._compute_name = None
        self._target_name = None

    def get_node_representation(self) -> str:
        """Get the node representation for display."""
        display_quote_policy = {"database": False, "schema": False, "identifier": False}
        relation = self.adapter.Relation.create_from(
            self.config, self.node, quote_policy=display_quote_policy
        )
        # exclude the database from output if it's the default
        if self.node.database == self.config.credentials.database:
            relation = relation.include(database=False)
        return str(relation)

    def describe_node(self) -> str:
        return f"seed file {self.get_node_representation()}"

    def compile(self, manifest: Manifest) -> SeedNode:
        """Seeds don't need compilation - return node as-is."""
        return self.node

    def before_execute(self) -> None:
        fire_event(
            LogStartLine(
                description=self.describe_node(),
                index=self.node_index,
                total=self.num_nodes,
                node_info=self.node.node_info,
            )
        )

    def after_execute(self, result: Any) -> None:
        """Called after execute completes."""
        self.print_result_line(result)

    def execute(self, compiled_node: Any, manifest: Manifest) -> Any:
        """Execute seed using Spark.

        Resolution order for compute: CLI --compute > seed config > computes.yml default
        Resolution order for target: CLI --target > seed config > profiles.yml default

        Note: Spark session is initialized ONCE in SeedTask.run() and shared
        across all seed threads. This avoids race conditions from per-seed
        singleton resets.

        Returns:
            RunResult with execution status
        """
        from dvt.artifacts.schemas.run import RunResult
        from dvt.federation.spark_manager import SparkManager

        seed = self.node

        try:
            # Resolve compute engine (for logging purposes)
            self._compute_name = self._resolve_compute(seed)

            # Use the shared SparkManager instance (initialized by SeedTask.run())
            # Do NOT reset the singleton here - that causes race conditions
            self.spark_manager = SparkManager.get_instance()

            # Get existing Spark session (already created by SeedTask.run())
            spark = self.spark_manager.get_or_create_session()

            # Read seed file with Spark (supports CSV, Parquet, JSON, ORC)
            df = self._read_file_with_spark(spark, seed)
            row_count = df.count()

            # Apply schema overrides from seed config
            df = self._apply_schema_overrides(df, seed)

            # Determine target for this seed
            self._target_name = self._resolve_target(seed)

            # Get connection config for target
            connection = self._get_connection_for_target(self._target_name)

            # Build table name
            table_name = self._get_full_table_name(seed, connection)

            # Load to target using loader (bulk load with JDBC fallback)
            self._load_with_loader(df, seed, connection, table_name)

            return RunResult(
                status=RunStatus.Success,
                timing=[],
                thread_id="",
                execution_time=0.0,
                adapter_response={},
                message=f"Loaded {row_count} rows via Spark → {self._target_name}",
                failures=None,
                node=seed,
            )

        except Exception as e:
            _log(red(f"❌ Spark seed failed: {e}"))
            return RunResult(
                status=RunStatus.Error,
                timing=[],
                thread_id="",
                execution_time=0.0,
                adapter_response={},
                message=str(e),
                failures=None,
                node=seed,
            )

    def _resolve_compute(self, seed: SeedNode) -> Optional[str]:
        """Resolve which compute engine to use.

        Priority: CLI --compute > seed config compute > computes.yml default
        """
        # Check CLI flag (args object has uppercase attribute names)
        cli_compute = getattr(self.config.args, "COMPUTE", None)
        if not cli_compute:
            cli_compute = getattr(self.config.args, "compute", None)
        if cli_compute:
            return cli_compute

        # Check seed-level config
        if hasattr(seed, "config") and seed.config:
            seed_compute = seed.config.get("compute")
            if seed_compute:
                return seed_compute

        # Return None to use default from computes.yml
        return None

    def _resolve_target(self, seed: SeedNode) -> str:
        """Resolve which target to seed to.

        Priority: CLI --target > seed config target > profiles.yml default
        """
        # Check CLI flag
        cli_target = getattr(self.config.args, "TARGET", None)
        if not cli_target:
            cli_target = getattr(self.config.args, "target", None)
        if cli_target:
            return cli_target

        # Check seed-level config
        if hasattr(seed, "config") and seed.config:
            seed_target = seed.config.get("target")
            if seed_target:
                return seed_target

        # Default from profile
        return self.config.target_name

    def _get_profiles_dir(self) -> Optional[str]:
        """Get the profiles directory from config args.

        Returns:
            Profiles directory path or None for default
        """
        profiles_dir = getattr(self.config.args, "PROFILES_DIR", None)
        if not profiles_dir:
            profiles_dir = getattr(self.config.args, "profiles_dir", None)
        return profiles_dir

    def _get_connection_for_target(self, target_name: str) -> Dict[str, Any]:
        """Get ALL connection credentials for a target from profiles.yml.

        Loads profiles.yml and extracts the connection config for the specified
        target name. This allows seeding to targets other than the default.

        Args:
            target_name: The target name to get credentials for

        Returns:
            Connection config dict with all credentials
        """
        from dvt.config.user_config import load_profiles

        # Get profiles directory from config args
        profiles_dir = getattr(self.config.args, "PROFILES_DIR", None)
        if not profiles_dir:
            profiles_dir = getattr(self.config.args, "profiles_dir", None)

        # Load all profiles
        profiles = load_profiles(profiles_dir)
        if not profiles:
            # Fallback to current config's credentials if profiles not found
            if hasattr(self.config, "credentials"):
                from dvt.federation.credentials import extract_all_credentials

                return extract_all_credentials(self.config.credentials)
            return {}

        # Get profile for this project
        profile_name = self.config.profile_name
        profile = profiles.get(profile_name, {})
        outputs = profile.get("outputs", {})

        # Get connection config for the specified target
        connection = outputs.get(target_name, {})

        if not connection:
            # Fallback to current config if target not found
            if hasattr(self.config, "credentials"):
                from dvt.federation.credentials import extract_all_credentials

                return extract_all_credentials(self.config.credentials)

        return connection

    def _read_file_with_spark(self, spark: Any, seed: SeedNode) -> Any:
        """Read seed file using Spark (supports CSV, Parquet, JSON, ORC).

        Automatically detects file format based on extension and uses
        appropriate Spark reader with optimized settings.

        Supported formats:
        - .csv      - CSV with header, schema inference, quoted fields
        - .parquet  - Apache Parquet columnar format
        - .json     - JSON Lines (one JSON object per line)
        - .jsonl    - JSON Lines (alias for .json)
        - .orc      - Apache ORC columnar format

        Args:
            spark: SparkSession instance
            seed: SeedNode with file path information

        Returns:
            PySpark DataFrame with seed data

        Raises:
            ValueError: If file format is not supported
        """
        file_path = Path(seed.root_path) / seed.original_file_path
        extension = file_path.suffix.lower()

        if extension not in SUPPORTED_SEED_FORMATS:
            raise ValueError(
                f"Unsupported seed format: {extension}. "
                f"Supported: {', '.join(sorted(SUPPORTED_SEED_FORMATS))}"
            )

        _log(f"  Reading {extension}: {file_path}")

        if extension == ".csv":
            return spark.read.csv(
                str(file_path),
                header=True,
                inferSchema=True,
                nullValue="",
                quote='"',
                escape='"',
                multiLine=True,
            )
        elif extension == ".parquet":
            return spark.read.parquet(str(file_path))
        elif extension in (".json", ".jsonl"):
            return spark.read.json(str(file_path))
        elif extension == ".orc":
            return spark.read.orc(str(file_path))
        else:
            # Should never reach here due to check above
            raise ValueError(f"Unsupported seed format: {extension}")

    def _apply_schema_overrides(self, df: Any, seed: SeedNode) -> Any:
        """Apply column type overrides from seed config.

        Example seed config:
            seeds:
              my_seed:
                +column_types:
                  id: bigint
                  created_at: timestamp
        """
        if not hasattr(seed, "config"):
            return df

        column_types = seed.config.get("column_types", {})

        for col_name, col_type in column_types.items():
            if col_name in df.columns:
                df = df.withColumn(col_name, df[col_name].cast(col_type))

        return df

    def _get_full_table_name(self, seed: SeedNode, connection: Dict[str, Any]) -> str:
        """Get fully qualified table name for the seed.

        For most databases: schema.table
        For Databricks Unity Catalog: catalog.schema.table
        """
        schema = seed.schema
        alias = seed.alias or seed.name
        adapter_type = connection.get("type", "").lower()

        # Databricks Unity Catalog requires catalog.schema.table format
        if adapter_type == "databricks":
            catalog = connection.get("catalog", "")
            if catalog:
                return f"{catalog}.{schema}.{alias}"

        return f"{schema}.{alias}"

    def _load_with_loader(
        self,
        df: Any,
        seed: SeedNode,
        connection: Dict[str, Any],
        table_name: str,
    ) -> None:
        """Load DataFrame using appropriate loader (bulk load or JDBC fallback).

        Uses the loader registry to select the best loader for the target adapter.
        Loaders prioritize native bulk load (e.g., PostgreSQL COPY, Snowflake COPY INTO)
        and fall back to Spark JDBC if bulk load is not available.

        DDL operations (TRUNCATE, DROP) are executed via dbt adapter for proper
        quoting per dialect. Data loading uses Spark JDBC.

        Args:
            df: PySpark DataFrame to load
            seed: SeedNode for configuration
            connection: Target database connection config
            table_name: Fully qualified table name (schema.table)
        """
        from dvt.config.user_config import (
            load_buckets_for_profile,
            load_jdbc_load_config,
        )
        from dvt.federation.adapter_manager import AdapterManager
        from dvt.federation.loaders import get_loader
        from dvt.federation.loaders.base import LoadConfig

        adapter_type = connection.get("type", "").lower()

        # Get adapter for target (used for DDL with proper quoting)
        adapter = AdapterManager.get_adapter(
            profile_name=self.config.profile_name,
            target_name=self._target_name,
            profiles_dir=self._get_profiles_dir(),
        )

        # Create loader for this adapter
        loader = get_loader(adapter_type, on_progress=_log)

        # Load jdbc_load config from computes.yml
        profile_name = self.config.profile_name
        compute_name = self._compute_name
        jdbc_load_config = load_jdbc_load_config(profile_name, compute_name)

        # Load bucket config for bulk load (if available)
        bucket_config = None
        profile_buckets = load_buckets_for_profile(profile_name)
        if profile_buckets:
            target_bucket = profile_buckets.get("target", "local")
            buckets = profile_buckets.get("buckets", {})
            bucket_config = buckets.get(target_bucket)

        # Check CLI --full-refresh flag
        full_refresh = getattr(self.config.args, "FULL_REFRESH", False)
        if not full_refresh:
            full_refresh = getattr(self.config.args, "full_refresh", False)

        # Build load config - loader handles DROP/TRUNCATE/CREATE logic
        config = LoadConfig(
            table_name=table_name,
            mode="overwrite",
            truncate=not full_refresh,  # Use TRUNCATE unless full-refresh
            full_refresh=full_refresh,  # Pass to loader for proper handling
            connection_config=connection,
            jdbc_config=jdbc_load_config,
            bucket_config=bucket_config,
        )

        # Execute load with adapter for DDL
        _log(f"  Loading {table_name}...")
        result = loader.load(df, config, adapter=adapter)

        if not result.success:
            raise RuntimeError(f"Load failed: {result.error}")

        _log(green(f"  Loaded {result.row_count} rows via {result.load_method}"))

    def _write_with_jdbc(
        self,
        df: Any,
        seed: SeedNode,
        connection: Dict[str, Any],
        table_name: str,
    ) -> None:
        """Write DataFrame to target database using JDBC.

        DEPRECATED: This method is kept for backward compatibility.
        Use _load_with_loader() instead which uses the unified loader
        architecture with adapter-managed DDL.

        Normal mode: Use overwrite with truncate=True (TRUNCATE instead of DROP)
        --full-refresh mode: DROP CASCADE first, then recreate table
        """
        from dvt.federation.adapter_manager import AdapterManager
        from dvt.federation.auth import get_auth_handler
        from dvt.federation.spark_manager import SparkManager

        spark_manager = SparkManager.get_instance()

        # Build JDBC URL
        jdbc_url = spark_manager.get_jdbc_url(connection)

        # Get driver class
        adapter_type = connection.get("type", "").lower()
        driver = spark_manager.get_jdbc_driver(adapter_type)

        # Check CLI --full-refresh flag
        full_refresh = getattr(self.config.args, "FULL_REFRESH", False)
        if not full_refresh:
            full_refresh = getattr(self.config.args, "full_refresh", False)

        # Get batch size
        batch_size = 10000
        if hasattr(seed, "config") and seed.config:
            batch_size = seed.config.get("batch_size", 10000) or 10000

        # Get auth handler and validate credentials
        auth_handler = get_auth_handler(adapter_type)
        is_valid, error_msg = auth_handler.validate(connection)
        if not is_valid:
            _log(red(f"  Auth error: {error_msg}"))
            raise RuntimeError(error_msg)

        # Build JDBC properties using auth handler
        jdbc_props = auth_handler.get_jdbc_properties(connection)
        properties = {
            **jdbc_props,
            "driver": driver,
            "batchsize": str(batch_size),
        }

        # Get adapter for DDL operations
        adapter = AdapterManager.get_adapter(
            profile_name=self.config.profile_name,
            target_name=self._target_name,
            profiles_dir=self._get_profiles_dir(),
        )

        # Execute DDL via adapter for proper quoting
        from dvt.federation.adapter_manager import get_quoted_table_name

        quoted_table = get_quoted_table_name(adapter, table_name)

        with adapter.connection_named("dvt_seed"):
            if full_refresh:
                _log(f"  Dropping {table_name} (full refresh)...")
                try:
                    adapter.execute(f"DROP TABLE IF EXISTS {quoted_table} CASCADE")
                except Exception:
                    try:
                        adapter.execute(f"DROP TABLE IF EXISTS {quoted_table}")
                    except Exception:
                        pass
            else:
                _log(f"  Truncating {table_name}...")
                try:
                    adapter.execute(f"TRUNCATE TABLE {quoted_table}")
                except Exception:
                    pass  # Table might not exist

        # Write via Spark JDBC (table cleared by DDL, use append)
        _log(f"  Writing to {table_name} via JDBC...")
        df.write.jdbc(
            url=jdbc_url,
            table=table_name,
            mode="append",  # DDL handled above
            properties=properties,
        )

        row_count = df.count()
        _log(green(f"  Loaded {row_count} rows"))

    def _execute_sql_native(self, connection: Dict[str, Any], sql: str) -> bool:
        """Execute SQL using native Python driver. Returns True on success.

        Supports all major database adapters with their respective Python drivers.
        Falls back gracefully if driver is not installed.
        """
        adapter_type = connection.get("type", "").lower()

        # PostgreSQL and compatible databases
        if adapter_type in (
            "postgres",
            "greenplum",
            "materialize",
            "risingwave",
            "cratedb",
            "alloydb",
            "timescaledb",
        ):
            return self._execute_sql_postgres(connection, sql)

        elif adapter_type == "snowflake":
            return self._execute_sql_snowflake(connection, sql)

        elif adapter_type == "redshift":
            return self._execute_sql_redshift(connection, sql)

        # MySQL and compatible databases
        elif adapter_type in ("mysql", "tidb", "singlestore"):
            return self._execute_sql_mysql(connection, sql)

        elif adapter_type == "bigquery":
            return self._execute_sql_bigquery(connection, sql)

        elif adapter_type == "databricks":
            return self._execute_sql_databricks(connection, sql)

        elif adapter_type in ("sqlserver", "synapse", "fabric"):
            return self._execute_sql_sqlserver(connection, sql)

        elif adapter_type in ("trino", "starburst"):
            return self._execute_sql_trino(connection, sql)

        elif adapter_type == "clickhouse":
            return self._execute_sql_clickhouse(connection, sql)

        elif adapter_type == "oracle":
            return self._execute_sql_oracle(connection, sql)

        else:
            _log(f"    Native SQL exec not supported for {adapter_type}")
            return False

    def _execute_sql_postgres(self, connection: Dict[str, Any], sql: str) -> bool:
        """Execute SQL on Postgres using psycopg2 with full auth support.

        Supports: password, SSL certificates (sslmode, sslcert, sslkey, sslrootcert)
        Also used for PostgreSQL-compatible databases: Greenplum, Materialize,
        RisingWave, CrateDB, AlloyDB, TimescaleDB.
        """
        try:
            import psycopg2
        except ImportError:
            _log("    psycopg2 not installed, skipping native SQL")
            return False

        from dvt.federation.auth.postgres import PostgresAuthHandler

        handler = PostgresAuthHandler()
        is_valid, error_msg = handler.validate(connection)
        if not is_valid:
            _log(f"    {error_msg}")
            return False

        conn = None
        try:
            connect_kwargs = handler.get_native_connection_kwargs(connection)
            conn = psycopg2.connect(**connect_kwargs)
            conn.autocommit = True
            cur = conn.cursor()
            cur.execute(sql)
            cur.close()
            return True
        except Exception as e:
            _log(f"    Postgres SQL failed: {str(e)[:60]}")
            return False
        finally:
            if conn:
                conn.close()

    def _execute_sql_snowflake(self, connection: Dict[str, Any], sql: str) -> bool:
        """Execute SQL on Snowflake using all supported auth methods."""
        try:
            import snowflake.connector
        except ImportError:
            _log("    snowflake-connector not installed, skipping native SQL")
            return False

        from dvt.federation.auth.snowflake import SnowflakeAuthHandler

        handler = SnowflakeAuthHandler()

        # Validate auth method (block interactive SSO)
        is_valid, error_msg = handler.validate(connection)
        if not is_valid:
            _log(f"    {error_msg}")
            return False

        conn = None
        try:
            # Use auth handler to get connection kwargs
            connect_kwargs = handler.get_native_connection_kwargs(connection)
            conn = snowflake.connector.connect(**connect_kwargs)
            cur = conn.cursor()
            cur.execute(sql)
            cur.close()
            return True
        except Exception as e:
            _log(f"    Snowflake SQL failed: {str(e)[:60]}")
            return False
        finally:
            if conn:
                conn.close()

    def _execute_sql_redshift(self, connection: Dict[str, Any], sql: str) -> bool:
        """Execute SQL on Redshift using redshift_connector.

        Supports: password, IAM authentication.
        """
        try:
            import redshift_connector
        except ImportError:
            _log("    redshift_connector not installed, skipping native SQL")
            return False

        from dvt.federation.auth.redshift import RedshiftAuthHandler

        handler = RedshiftAuthHandler()
        is_valid, error_msg = handler.validate(connection)
        if not is_valid:
            _log(f"    {error_msg}")
            return False

        conn = None
        try:
            connect_kwargs = handler.get_native_connection_kwargs(connection)
            conn = redshift_connector.connect(**connect_kwargs)
            conn.autocommit = True
            cur = conn.cursor()
            cur.execute(sql)
            cur.close()
            return True
        except Exception as e:
            _log(f"    Redshift SQL failed: {str(e)[:60]}")
            return False
        finally:
            if conn:
                conn.close()

    def _execute_sql_mysql(self, connection: Dict[str, Any], sql: str) -> bool:
        """Execute SQL on MySQL using mysql-connector-python.

        Supports: password, SSL certificates.
        Also used for MySQL-compatible databases: TiDB, SingleStore.
        """
        try:
            import mysql.connector
        except ImportError:
            _log("    mysql-connector-python not installed, skipping native SQL")
            return False

        from dvt.federation.auth.mysql import MySQLAuthHandler

        handler = MySQLAuthHandler()
        is_valid, error_msg = handler.validate(connection)
        if not is_valid:
            _log(f"    {error_msg}")
            return False

        conn = None
        try:
            connect_kwargs = handler.get_native_connection_kwargs(connection)
            conn = mysql.connector.connect(**connect_kwargs)
            conn.autocommit = True
            cur = conn.cursor()
            cur.execute(sql)
            cur.close()
            return True
        except Exception as e:
            _log(f"    MySQL SQL failed: {str(e)[:60]}")
            return False
        finally:
            if conn:
                conn.close()

    def _execute_sql_bigquery(self, connection: Dict[str, Any], sql: str) -> bool:
        """Execute SQL on BigQuery using google-cloud-bigquery.

        Supports: oauth (application default), service_account, service_account_json.
        """
        try:
            from google.cloud import bigquery
            from google.oauth2 import service_account
        except ImportError:
            _log("    google-cloud-bigquery not installed, skipping native SQL")
            return False

        from dvt.federation.auth.bigquery import BigQueryAuthHandler

        handler = BigQueryAuthHandler()
        is_valid, error_msg = handler.validate(connection)
        if not is_valid:
            _log(f"    {error_msg}")
            return False

        try:
            native_kwargs = handler.get_native_connection_kwargs(connection)
            project = native_kwargs.get("project", "")

            # Build credentials based on auth method
            if native_kwargs.get("keyfile"):
                credentials = service_account.Credentials.from_service_account_file(
                    native_kwargs["keyfile"]
                )
                client = bigquery.Client(project=project, credentials=credentials)
            elif native_kwargs.get("keyfile_json"):
                credentials = service_account.Credentials.from_service_account_info(
                    native_kwargs["keyfile_json"]
                )
                client = bigquery.Client(project=project, credentials=credentials)
            else:
                # Use application default credentials (gcloud auth)
                client = bigquery.Client(project=project)

            query_job = client.query(sql)
            query_job.result()  # Wait for completion
            return True
        except Exception as e:
            _log(f"    BigQuery SQL failed: {str(e)[:60]}")
            return False

    def _execute_sql_databricks(self, connection: Dict[str, Any], sql: str) -> bool:
        """Execute SQL on Databricks using databricks-sql-connector.

        Supports: token, oauth_m2m (client credentials).
        Blocked: oauth_u2m (requires browser).
        """
        try:
            from databricks import sql as databricks_sql
        except ImportError:
            _log("    databricks-sql-connector not installed, skipping native SQL")
            return False

        from dvt.federation.auth.databricks import DatabricksAuthHandler

        handler = DatabricksAuthHandler()
        is_valid, error_msg = handler.validate(connection)
        if not is_valid:
            _log(f"    {error_msg}")
            return False

        conn = None
        try:
            connect_kwargs = handler.get_native_connection_kwargs(connection)
            conn = databricks_sql.connect(**connect_kwargs)
            cur = conn.cursor()
            cur.execute(sql)
            cur.close()
            return True
        except Exception as e:
            _log(f"    Databricks SQL failed: {str(e)[:60]}")
            return False
        finally:
            if conn:
                conn.close()

    def _execute_sql_sqlserver(self, connection: Dict[str, Any], sql: str) -> bool:
        """Execute SQL on SQL Server using pymssql.

        Supports: password, azure_ad.
        Blocked: windows (integrated auth requires domain context).
        Also used for: Azure Synapse, Microsoft Fabric.
        """
        try:
            import pymssql
        except ImportError:
            _log("    pymssql not installed, skipping native SQL")
            return False

        from dvt.federation.auth.sqlserver import SQLServerAuthHandler

        handler = SQLServerAuthHandler()
        is_valid, error_msg = handler.validate(connection)
        if not is_valid:
            _log(f"    {error_msg}")
            return False

        conn = None
        try:
            connect_kwargs = handler.get_native_connection_kwargs(connection)
            # pymssql uses 'server' not 'host', and port is separate
            server = connect_kwargs.get("server", "")
            port = connect_kwargs.get("port", "1433")
            conn = pymssql.connect(
                server=server,
                port=port,
                user=connect_kwargs.get("user", ""),
                password=connect_kwargs.get("password", ""),
                database=connect_kwargs.get("database", ""),
            )
            conn.autocommit(True)
            cur = conn.cursor()
            cur.execute(sql)
            cur.close()
            return True
        except Exception as e:
            _log(f"    SQL Server SQL failed: {str(e)[:60]}")
            return False
        finally:
            if conn:
                conn.close()

    def _execute_sql_trino(self, connection: Dict[str, Any], sql: str) -> bool:
        """Execute SQL on Trino/Starburst using trino Python driver.

        Supports: none, password (LDAP), jwt.
        """
        try:
            import trino
        except ImportError:
            _log("    trino not installed, skipping native SQL")
            return False

        from dvt.federation.auth.trino import TrinoAuthHandler

        handler = TrinoAuthHandler()
        is_valid, error_msg = handler.validate(connection)
        if not is_valid:
            _log(f"    {error_msg}")
            return False

        conn = None
        try:
            connect_kwargs = handler.get_native_connection_kwargs(connection)
            # Build auth object if password auth
            auth = None
            auth_tuple = connect_kwargs.pop("auth", None)
            if auth_tuple:
                auth = trino.auth.BasicAuthentication(*auth_tuple)

            conn = trino.dbapi.connect(
                host=connect_kwargs.get("host", "localhost"),
                port=connect_kwargs.get("port", 8080),
                user=connect_kwargs.get("user", "trino"),
                catalog=connect_kwargs.get("catalog", ""),
                schema=connect_kwargs.get("schema", ""),
                http_scheme=connect_kwargs.get("http_scheme", "http"),
                auth=auth,
            )
            cur = conn.cursor()
            cur.execute(sql)
            cur.fetchall()  # Consume results
            cur.close()
            return True
        except Exception as e:
            _log(f"    Trino SQL failed: {str(e)[:60]}")
            return False
        finally:
            if conn:
                conn.close()

    def _execute_sql_clickhouse(self, connection: Dict[str, Any], sql: str) -> bool:
        """Execute SQL on ClickHouse using clickhouse-connect.

        Supports: password, SSL.
        """
        try:
            import clickhouse_connect
        except ImportError:
            _log("    clickhouse-connect not installed, skipping native SQL")
            return False

        from dvt.federation.auth.clickhouse import ClickHouseAuthHandler

        handler = ClickHouseAuthHandler()
        is_valid, error_msg = handler.validate(connection)
        if not is_valid:
            _log(f"    {error_msg}")
            return False

        client = None
        try:
            connect_kwargs = handler.get_native_connection_kwargs(connection)
            client = clickhouse_connect.get_client(**connect_kwargs)
            client.command(sql)
            return True
        except Exception as e:
            _log(f"    ClickHouse SQL failed: {str(e)[:60]}")
            return False
        finally:
            if client:
                try:
                    client.close()
                except Exception:
                    pass

    def _execute_sql_oracle(self, connection: Dict[str, Any], sql: str) -> bool:
        """Execute SQL on Oracle using oracledb.

        Supports: password, wallet.
        """
        try:
            import oracledb
        except ImportError:
            _log("    oracledb not installed, skipping native SQL")
            return False

        from dvt.federation.auth.oracle import OracleAuthHandler

        handler = OracleAuthHandler()
        is_valid, error_msg = handler.validate(connection)
        if not is_valid:
            _log(f"    {error_msg}")
            return False

        conn = None
        try:
            connect_kwargs = handler.get_native_connection_kwargs(connection)
            conn = oracledb.connect(**connect_kwargs)
            conn.autocommit = True
            cur = conn.cursor()
            cur.execute(sql)
            cur.close()
            return True
        except Exception as e:
            _log(f"    Oracle SQL failed: {str(e)[:60]}")
            return False
        finally:
            if conn:
                conn.close()

    def _drop_table_cascade(
        self,
        connection: Dict[str, Any],
        table_name: str,
    ) -> bool:
        """Drop table with CASCADE where supported. Returns True on success.

        Different databases have different DROP TABLE syntax:
        - CASCADE supported: Postgres, Redshift, Snowflake, Trino, and PG-compatible DBs
        - CASCADE CONSTRAINTS: Oracle
        - No CASCADE: MySQL, BigQuery, Databricks, ClickHouse, SQL Server
        """
        adapter_type = connection.get("type", "").lower()

        # PostgreSQL and compatible (support CASCADE)
        if adapter_type in (
            "postgres",
            "redshift",
            "snowflake",
            "greenplum",
            "materialize",
            "risingwave",
            "cratedb",
            "alloydb",
            "timescaledb",
            "trino",
            "starburst",
        ):
            drop_sql = f"DROP TABLE IF EXISTS {table_name} CASCADE"

        # Oracle uses CASCADE CONSTRAINTS
        elif adapter_type == "oracle":
            # Oracle doesn't have IF EXISTS, but we handle errors gracefully
            drop_sql = f"DROP TABLE {table_name} CASCADE CONSTRAINTS"

        # MySQL and compatible (no CASCADE, but DROP works)
        elif adapter_type in ("mysql", "tidb", "singlestore", "clickhouse"):
            drop_sql = f"DROP TABLE IF EXISTS {table_name}"

        # BigQuery
        elif adapter_type == "bigquery":
            drop_sql = f"DROP TABLE IF EXISTS {table_name}"

        # Databricks/Spark SQL
        elif adapter_type == "databricks":
            drop_sql = f"DROP TABLE IF EXISTS {table_name}"

        # SQL Server / Synapse / Fabric (SQL Server 2016+ supports DROP IF EXISTS)
        elif adapter_type in ("sqlserver", "synapse", "fabric"):
            drop_sql = f"DROP TABLE IF EXISTS {table_name}"

        else:
            drop_sql = f"DROP TABLE IF EXISTS {table_name}"

        _log(f"    Executing: {drop_sql}")
        success = self._execute_sql_native(connection, drop_sql)
        if success:
            _log(f"    ✓ Table dropped with CASCADE")
        return success

    def print_result_line(self, result: Any) -> None:
        """Print result line for the seed."""
        model = result.node
        group = group_lookup.get(model.unique_id)
        level = (
            EventLevel.ERROR if result.status == NodeStatus.Error else EventLevel.INFO
        )
        fire_event(
            LogSeedResult(
                status=result.status,
                result_message=result.message,
                index=self.node_index,
                total=self.num_nodes,
                execution_time=result.execution_time,
                schema=self.node.schema,
                relation=model.alias,
                node_info=model.node_info,
                group=group,
            ),
            level=level,
        )
