# coding=utf-8
"""Exasol authentication handler for DVT federation.

Auth Methods:
- password: Basic user/password authentication
"""

from typing import Any, Dict

from .base import BaseAuthHandler


class ExasolAuthHandler(BaseAuthHandler):
    """Handles Exasol authentication methods."""

    SUPPORTED_AUTH_METHODS = ["password"]
    INTERACTIVE_AUTH_METHODS = []

    def detect_auth_method(self, creds: Dict[str, Any]) -> str:
        """Detect Exasol auth method from credentials."""
        return "password"

    def get_jdbc_properties(self, creds: Dict[str, Any]) -> Dict[str, str]:
        """Build Exasol JDBC properties."""
        props: Dict[str, str] = {
            "user": str(creds.get("user", "")),
            "password": str(creds.get("password", "")),
        }

        # Connection settings
        if creds.get("encryption"):
            props["encryption"] = "1"

        return props

    def get_native_connection_kwargs(self, creds: Dict[str, Any]) -> Dict[str, Any]:
        """Get kwargs for pyexasol.connect()."""
        kwargs: Dict[str, Any] = {
            "dsn": f"{creds.get('host', 'localhost')}:{creds.get('port', 8563)}",
            "user": creds.get("user", ""),
            "password": creds.get("password", ""),
            "schema": creds.get("schema", ""),
        }

        if creds.get("encryption"):
            kwargs["encryption"] = True

        return kwargs
