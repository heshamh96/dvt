# coding=utf-8
"""Spark session manager for DVT federation.

This module manages Spark sessions for DVT operations including:
- Federated query execution
- Spark-based seed loading
- Cross-target data transfer

The SparkManager provides a singleton-style session management with
configurable Spark settings from computes.yml.
"""
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

from dvt.config.user_config import get_dvt_home, get_jdbc_drivers_dir


class SparkManager:
    """Manages Spark sessions for DVT federation operations.

    Handles:
    - Session creation with appropriate JDBC driver classpath
    - Session configuration from computes.yml
    - Session lifecycle (create, get, stop)
    """

    _instance: Optional["SparkManager"] = None
    _session: Optional[Any] = None  # SparkSession

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """Initialize SparkManager with optional configuration.

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
        """
        self.config = config or {}

    @classmethod
    def get_instance(cls) -> "SparkManager":
        """Get or create the singleton SparkManager instance."""
        if cls._instance is None:
            cls._instance = SparkManager()
        return cls._instance

    def get_or_create_session(self, app_name: str = "DVT") -> Any:
        """Get existing or create new Spark session.

        Args:
            app_name: Name for the Spark application

        Returns:
            SparkSession instance
        """
        if SparkManager._session is not None:
            return SparkManager._session

        try:
            from pyspark.sql import SparkSession
        except ImportError:
            raise ImportError(
                "PySpark is not installed. Run 'dvt sync' to install PySpark, "
                "or install it manually with: pip install pyspark"
            )

        # Build JDBC driver classpath
        jdbc_jars = self._get_jdbc_jars()
        extra_jars = ",".join(jdbc_jars) if jdbc_jars else ""

        # Get master from config or default to local
        master = self.config.get("master", "local[*]")

        # Build Spark session
        builder = SparkSession.builder.appName(app_name).master(master)

        # Add JDBC jars to classpath
        if extra_jars:
            builder = builder.config("spark.jars", extra_jars)

        # Apply additional config from computes.yml
        spark_config = self.config.get("config", {})
        for key, value in spark_config.items():
            builder = builder.config(key, str(value))

        # Create session
        SparkManager._session = builder.getOrCreate()

        # Set log level to reduce noise
        SparkManager._session.sparkContext.setLogLevel("WARN")

        return SparkManager._session

    def stop_session(self) -> None:
        """Stop the current Spark session."""
        if SparkManager._session is not None:
            try:
                SparkManager._session.stop()
            except Exception:
                pass  # Ignore errors on cleanup
            SparkManager._session = None

    def _get_jdbc_jars(self) -> List[str]:
        """Get list of JDBC driver JAR paths for Spark classpath."""
        jdbc_dir = get_jdbc_drivers_dir(None)
        if not jdbc_dir.exists():
            return []

        jars = []
        for jar_file in jdbc_dir.glob("*.jar"):
            jars.append(str(jar_file))
        return jars

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
            return f"jdbc:snowflake://{account}.snowflakecomputing.com/?db={database}&schema={schema}"

        elif adapter_type == "databricks":
            host = connection.get("host", "")
            http_path = connection.get("http_path", "")
            return f"jdbc:databricks://{host}:443;httpPath={http_path}"

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
            "postgres": "org.postgresql.Driver",
            "snowflake": "net.snowflake.client.jdbc.SnowflakeDriver",
            "databricks": "com.databricks.client.jdbc.Driver",
            "bigquery": "com.simba.googlebigquery.jdbc.Driver",
            "redshift": "com.amazon.redshift.jdbc42.Driver",
            "mysql": "com.mysql.cj.jdbc.Driver",
            "sqlserver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
            "oracle": "oracle.jdbc.OracleDriver",
            "trino": "io.trino.jdbc.TrinoDriver",
            "starburst": "io.trino.jdbc.TrinoDriver",
        }
        return drivers.get(adapter_type.lower(), "")
