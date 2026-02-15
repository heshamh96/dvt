"""
MySQL extractor for EL layer.

Extraction priority:
1. Pipe-based: mysql --batch | PyArrow streaming (if mysql CLI on PATH)
2. Spark JDBC: parallel reads (default fallback)
"""

import os
from pathlib import Path
from typing import Any, Dict, List, Optional

from dvt.federation.extractors.base import (
    BaseExtractor,
    ExtractionConfig,
    ExtractionResult,
)


class MySQLExtractor(BaseExtractor):
    """MySQL-specific extractor using Spark JDBC.

    Also works for MySQL-compatible databases:
    - MariaDB, TiDB, SingleStore, StarRocks, Doris
    """

    adapter_types = [
        "mysql",
        "mariadb",
        "tidb",
        "singlestore",
        "starrocks",
        "doris",
    ]

    cli_tool = "mysql"

    def _get_connection(self, config: ExtractionConfig = None) -> Any:
        """Get or create a MySQL database connection.

        If self.connection is None but connection_config is available,
        creates a new connection using mysql.connector.

        Args:
            config: Optional extraction config with connection_config

        Returns:
            MySQL database connection
        """
        if self.connection is not None:
            return self.connection

        # Return cached lazy connection if available
        if self._lazy_connection is not None:
            return self._lazy_connection

        # Try to get connection_config from config or instance
        conn_config = None
        if config and config.connection_config:
            conn_config = config.connection_config
        elif self.connection_config:
            conn_config = self.connection_config

        if not conn_config:
            raise ValueError(
                "No connection provided and no connection_config available. "
                "Either provide a connection to the extractor or include "
                "connection_config in ExtractionConfig."
            )

        # Try mysql-connector-python first, fall back to pymysql
        try:
            import mysql.connector

            driver = mysql.connector
        except ImportError:
            try:
                import pymysql

                driver = pymysql
            except ImportError:
                raise ImportError(
                    "mysql-connector-python or pymysql is required for MySQL extraction. "
                    "Install with: pip install mysql-connector-python"
                )

        from dvt.federation.auth.mysql import MySQLAuthHandler

        handler = MySQLAuthHandler()
        connect_kwargs = handler.get_native_connection_kwargs(conn_config)

        self._lazy_connection = driver.connect(**connect_kwargs)
        return self._lazy_connection

    def _build_extraction_command(self, config: ExtractionConfig) -> List[str]:
        """Build mysql CLI command for tab-delimited output."""
        conn_config = config.connection_config or self.connection_config or {}
        query = self.build_export_query(config)
        cmd = [
            "mysql",
            "-h",
            conn_config.get("host", "localhost"),
            "-P",
            str(conn_config.get("port", 3306)),
            "-u",
            conn_config.get("user", "root"),
        ]
        database = conn_config.get("database", "")
        if database:
            cmd.extend(["-D", database])
        cmd.extend(
            [
                "-e",
                query,
                "--batch",
                "--raw",  # tab-separated, no escaping
            ]
        )
        return cmd

    def _build_extraction_env(self, config: ExtractionConfig) -> Dict[str, str]:
        """Build env with MYSQL_PWD for mysql subprocess."""
        conn_config = config.connection_config or self.connection_config or {}
        env = os.environ.copy()
        password = conn_config.get("password", "")
        if password:
            env["MYSQL_PWD"] = str(password)
        return env

    def _get_csv_parse_options(self):
        """MySQL --batch outputs tab-delimited data."""
        try:
            import pyarrow.csv as pa_csv

            return pa_csv.ParseOptions(delimiter="\t")
        except ImportError:
            return None

    def extract(
        self,
        config: ExtractionConfig,
        output_path: Path,
    ) -> ExtractionResult:
        """Extract data from MySQL to Parquet.

        Tries pipe (mysql CLI) first, falls back to Spark JDBC.
        """
        if self._has_cli_tool():
            try:
                return self._extract_via_pipe(config, output_path)
            except Exception as e:
                self._log(f"Pipe extraction failed ({e}), falling back to JDBC...")

        return self._extract_jdbc(config, output_path)

    def extract_hashes(
        self,
        config: ExtractionConfig,
    ) -> Dict[str, str]:
        """Extract row hashes using MySQL MD5 function."""
        if not config.pk_columns:
            raise ValueError("pk_columns required for hash extraction")

        # Build MySQL-specific hash query
        pk_expr = (
            config.pk_columns[0]
            if len(config.pk_columns) == 1
            else f"CONCAT_WS('|', {', '.join(config.pk_columns)})"
        )

        # Get columns if not specified
        if config.columns:
            cols = config.columns
        else:
            col_info = self.get_columns(config.schema, config.table)
            cols = [c["name"] for c in col_info]

        # MySQL uses MD5() and CONCAT_WS(), IFNULL for null handling
        col_exprs = [f"IFNULL(CAST({c} AS CHAR), '')" for c in cols]
        hash_expr = f"MD5(CONCAT_WS('|', {', '.join(col_exprs)}))"

        query = f"""
            SELECT
                CAST({pk_expr} AS CHAR) as _pk,
                {hash_expr} as _hash
            FROM {config.schema}.{config.table}
        """

        if config.predicates:
            where_clause = " AND ".join(config.predicates)
            query += f" WHERE {where_clause}"

        cursor = self._get_connection(config).cursor()
        cursor.execute(query)

        hashes = {}
        while True:
            batch = cursor.fetchmany(config.batch_size)
            if not batch:
                break
            for row in batch:
                hashes[row[0]] = row[1]

        cursor.close()
        return hashes

    def get_row_count(
        self,
        schema: str,
        table: str,
        predicates: Optional[List[str]] = None,
        config: ExtractionConfig = None,
    ) -> int:
        """Get row count using COUNT(*)."""
        query = f"SELECT COUNT(*) FROM {schema}.{table}"
        if predicates:
            query += f" WHERE {' AND '.join(predicates)}"

        cursor = self._get_connection(config).cursor()
        cursor.execute(query)
        count = cursor.fetchone()[0]
        cursor.close()
        return count

    def get_columns(
        self,
        schema: str,
        table: str,
        config: ExtractionConfig = None,
    ) -> List[Dict[str, str]]:
        """Get column metadata from information_schema."""
        query = """
            SELECT COLUMN_NAME, DATA_TYPE
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
            ORDER BY ORDINAL_POSITION
        """
        cursor = self._get_connection(config).cursor()
        cursor.execute(query, (schema, table))

        columns = []
        for row in cursor.fetchall():
            columns.append({"name": row[0], "type": row[1]})

        cursor.close()
        return columns

    def detect_primary_key(
        self,
        schema: str,
        table: str,
        config: ExtractionConfig = None,
    ) -> List[str]:
        """Detect primary key from information_schema."""
        query = """
            SELECT COLUMN_NAME
            FROM information_schema.KEY_COLUMN_USAGE
            WHERE TABLE_SCHEMA = %s
            AND TABLE_NAME = %s
            AND CONSTRAINT_NAME = 'PRIMARY'
            ORDER BY ORDINAL_POSITION
        """
        cursor = self._get_connection(config).cursor()
        try:
            cursor.execute(query, (schema, table))
            pk_cols = [row[0] for row in cursor.fetchall()]
        except Exception:
            pk_cols = []
        finally:
            cursor.close()

        return pk_cols
