# coding=utf-8
"""DuckDB authentication handler for DVT federation.

Auth Methods:
- none: No authentication (local file or in-memory database)

DuckDB is typically used as a local embedded database,
so it doesn't require network authentication.
"""

from typing import Any, Dict

from .base import BaseAuthHandler


class DuckDBAuthHandler(BaseAuthHandler):
    """Handles DuckDB authentication methods."""

    SUPPORTED_AUTH_METHODS = ["none"]
    INTERACTIVE_AUTH_METHODS = []

    def detect_auth_method(self, creds: Dict[str, Any]) -> str:
        """Detect DuckDB auth method from credentials."""
        return "none"

    def get_jdbc_properties(self, creds: Dict[str, Any]) -> Dict[str, str]:
        """Build DuckDB JDBC properties."""
        # DuckDB JDBC doesn't require authentication
        props: Dict[str, str] = {}

        # Read-only mode if specified
        if creds.get("read_only"):
            props["duckdb.read_only"] = "true"

        return props

    def get_native_connection_kwargs(self, creds: Dict[str, Any]) -> Dict[str, Any]:
        """Get kwargs for duckdb.connect()."""
        database = creds.get("path") or creds.get("database") or ":memory:"
        kwargs: Dict[str, Any] = {
            "database": database,
        }

        if creds.get("read_only"):
            kwargs["read_only"] = True

        return kwargs
