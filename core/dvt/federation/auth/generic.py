# coding=utf-8
"""Generic authentication handler for DVT federation.

Fallback handler for adapters without specific auth handling.
Supports basic user/password authentication which works for most databases.
"""

from typing import Any, Dict

from .base import BaseAuthHandler


class GenericAuthHandler(BaseAuthHandler):
    """Generic fallback authentication handler.

    Used for adapters that don't have specific auth handling.
    Supports basic user/password authentication.
    """

    SUPPORTED_AUTH_METHODS = ["password"]
    INTERACTIVE_AUTH_METHODS = []

    def detect_auth_method(self, creds: Dict[str, Any]) -> str:
        """Detect auth method from credentials."""
        return "password"

    def get_jdbc_properties(self, creds: Dict[str, Any]) -> Dict[str, str]:
        """Build generic JDBC properties."""
        props: Dict[str, str] = {
            "user": str(creds.get("user", "") or creds.get("username", "")),
            "password": str(creds.get("password", "")),
        }
        return props

    def get_native_connection_kwargs(self, creds: Dict[str, Any]) -> Dict[str, Any]:
        """Get generic connection kwargs."""
        kwargs: Dict[str, Any] = {
            "host": creds.get("host", "localhost"),
            "port": creds.get("port"),
            "database": creds.get("database") or creds.get("schema", ""),
            "user": creds.get("user", "") or creds.get("username", ""),
            "password": creds.get("password", ""),
        }
        return kwargs
