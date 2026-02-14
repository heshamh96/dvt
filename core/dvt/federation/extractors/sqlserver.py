"""
SQL Server extractor for EL layer.
Uses Spark JDBC for extraction.
Also handles Azure Synapse and Fabric.
"""

from pathlib import Path
from typing import Any, Dict, List, Optional

from dvt.federation.extractors.base import (
    BaseExtractor,
    ExtractionConfig,
    ExtractionResult,
)


class SQLServerExtractor(BaseExtractor):
    """SQL Server-specific extractor using Spark JDBC.

    Also works for Azure Synapse and Microsoft Fabric.
    """

    adapter_types = ["sqlserver", "synapse", "fabric"]

    def _get_connection(self, config: ExtractionConfig = None) -> Any:
        """Get or create a SQL Server database connection.

        If self.connection is None but connection_config is available,
        creates a new connection using pyodbc.

        Args:
            config: Optional extraction config with connection_config

        Returns:
            SQL Server database connection
        """
        if self.connection is not None:
            return self.connection

        if self._lazy_connection is not None:
            return self._lazy_connection

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

        try:
            import pyodbc
        except ImportError:
            raise ImportError(
                "pyodbc is required for SQL Server extraction. "
                "Install with: pip install pyodbc"
            )

        from dvt.federation.auth.sqlserver import SQLServerAuthHandler

        handler = SQLServerAuthHandler()
        kwargs = handler.get_native_connection_kwargs(conn_config)

        # Build connection string for pyodbc
        driver = conn_config.get("driver", "ODBC Driver 17 for SQL Server")
        conn_str = (
            f"DRIVER={{{driver}}};"
            f"SERVER={kwargs.get('server', '')};"
            f"PORT={kwargs.get('port', '1433')};"
            f"DATABASE={kwargs.get('database', '')};"
            f"UID={kwargs.get('user', '')};"
            f"PWD={kwargs.get('password', '')};"
        )
        if kwargs.get("encrypt"):
            conn_str += f"Encrypt={kwargs['encrypt']};"
        if conn_config.get("trust_cert"):
            conn_str += "TrustServerCertificate=yes;"

        self._lazy_connection = pyodbc.connect(conn_str)
        return self._lazy_connection

    def extract(self, config: ExtractionConfig, output_path: Path) -> ExtractionResult:
        """Extract data from SQL Server to Parquet using Spark JDBC."""
        return self._extract_jdbc(config, output_path)

    def extract_hashes(self, config: ExtractionConfig) -> Dict[str, str]:
        """Extract row hashes using SQL Server HASHBYTES."""
        if not config.pk_columns:
            raise ValueError("pk_columns required for hash extraction")

        pk_expr = (
            config.pk_columns[0]
            if len(config.pk_columns) == 1
            else f"CONCAT({', '.join(config.pk_columns)})"
        )

        cols = config.columns or [
            c["name"] for c in self.get_columns(config.schema, config.table)
        ]
        col_exprs = [f"ISNULL(CAST({c} AS NVARCHAR(MAX)), '')" for c in cols]
        hash_expr = f"LOWER(CONVERT(VARCHAR(32), HASHBYTES('MD5', CONCAT({', '.join(col_exprs)})), 2))"

        query = f"""
            SELECT CAST({pk_expr} AS NVARCHAR(MAX)) as _pk, {hash_expr} as _hash
            FROM {config.schema}.{config.table}
        """
        if config.predicates:
            query += f" WHERE {' AND '.join(config.predicates)}"

        cursor = self._get_connection(config).cursor()
        cursor.execute(query)
        hashes = {}
        while True:
            batch = cursor.fetchmany(config.batch_size)
            if not batch:
                break
            hashes.update({str(row[0]): row[1] for row in batch})
        cursor.close()
        return hashes

    def get_row_count(
        self,
        schema: str,
        table: str,
        predicates: Optional[List[str]] = None,
        config: ExtractionConfig = None,
    ) -> int:
        query = f"SELECT COUNT(*) FROM [{schema}].[{table}]"
        if predicates:
            query += f" WHERE {' AND '.join(predicates)}"
        cursor = self._get_connection(config).cursor()
        cursor.execute(query)
        count = cursor.fetchone()[0]
        cursor.close()
        return count

    def get_columns(
        self, schema: str, table: str, config: ExtractionConfig = None
    ) -> List[Dict[str, str]]:
        query = """
            SELECT COLUMN_NAME, DATA_TYPE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
            ORDER BY ORDINAL_POSITION
        """
        cursor = self._get_connection(config).cursor()
        cursor.execute(query, (schema, table))
        columns = [{"name": row[0], "type": row[1]} for row in cursor.fetchall()]
        cursor.close()
        return columns

    def detect_primary_key(
        self, schema: str, table: str, config: ExtractionConfig = None
    ) -> List[str]:
        query = """
            SELECT c.COLUMN_NAME
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
            JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE c
                ON tc.CONSTRAINT_NAME = c.CONSTRAINT_NAME
            WHERE tc.TABLE_SCHEMA = ?
            AND tc.TABLE_NAME = ?
            AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
            ORDER BY c.ORDINAL_POSITION
        """
        cursor = self._get_connection(config).cursor()
        try:
            cursor.execute(query, (schema, table))
            pk_cols = [row[0] for row in cursor.fetchall()]
        except Exception:
            pk_cols = []
        cursor.close()
        return pk_cols
