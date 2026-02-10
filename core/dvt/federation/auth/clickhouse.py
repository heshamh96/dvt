# coding=utf-8
"""ClickHouse authentication handler for DVT federation.

Auth Methods:
- password: Basic user/password authentication
- ssl: SSL/TLS encrypted connection
"""

from typing import Any, Dict

from .base import BaseAuthHandler


class ClickHouseAuthHandler(BaseAuthHandler):
    """Handles ClickHouse authentication methods."""

    SUPPORTED_AUTH_METHODS = ["password", "ssl"]
    INTERACTIVE_AUTH_METHODS = []

    def detect_auth_method(self, creds: Dict[str, Any]) -> str:
        """Detect ClickHouse auth method from credentials."""
        if creds.get("secure") or creds.get("ssl"):
            return "ssl"
        return "password"

    def get_jdbc_properties(self, creds: Dict[str, Any]) -> Dict[str, str]:
        """Build ClickHouse JDBC properties."""
        props: Dict[str, str] = {
            "user": str(creds.get("user", "default")),
            "password": str(creds.get("password", "")),
        }

        # SSL settings
        if creds.get("secure") or creds.get("ssl"):
            props["ssl"] = "true"
        if creds.get("verify"):
            props["sslmode"] = "strict"

        return props

    def get_jdbc_url_params(self, creds: Dict[str, Any]) -> str:
        """Get ClickHouse JDBC URL params."""
        params = []
        if creds.get("secure") or creds.get("ssl"):
            params.append("ssl=true")
        return "&".join(params)

    def get_native_connection_kwargs(self, creds: Dict[str, Any]) -> Dict[str, Any]:
        """Get kwargs for clickhouse_connect or clickhouse_driver."""
        kwargs: Dict[str, Any] = {
            "host": creds.get("host", "localhost"),
            "port": creds.get("port", 8123),
            "database": creds.get("database") or creds.get("schema", "default"),
            "user": creds.get("user", "default"),
            "password": creds.get("password", ""),
        }

        # SSL settings
        if creds.get("secure") or creds.get("ssl"):
            kwargs["secure"] = True
        if creds.get("verify") is False:
            kwargs["verify"] = False

        return kwargs
