# coding=utf-8
"""Hive authentication handler for DVT federation.

Auth Methods:
- none: No authentication (NOSASL)
- password: Username/password (LDAP)

Not supported (requires Kerberos):
- kerberos
"""

from typing import Any, Dict

from .base import BaseAuthHandler


class HiveAuthHandler(BaseAuthHandler):
    """Handles Hive authentication methods."""

    SUPPORTED_AUTH_METHODS = ["none", "password"]
    INTERACTIVE_AUTH_METHODS = []

    def detect_auth_method(self, creds: Dict[str, Any]) -> str:
        """Detect Hive auth method from credentials."""
        auth = str(creds.get("auth", "")).lower()
        if auth in ("ldap", "custom", "password"):
            return "password"
        elif creds.get("password"):
            return "password"
        return "none"

    def get_jdbc_properties(self, creds: Dict[str, Any]) -> Dict[str, str]:
        """Build Hive JDBC properties."""
        auth_method = self.detect_auth_method(creds)
        props: Dict[str, str] = {}

        if auth_method == "password":
            props["user"] = str(creds.get("user", ""))
            props["password"] = str(creds.get("password", ""))

        return props

    def get_jdbc_url_params(self, creds: Dict[str, Any]) -> str:
        """Get Hive JDBC URL params."""
        auth_method = self.detect_auth_method(creds)
        params = []

        if auth_method == "password":
            params.append("auth=ldap")
        else:
            params.append("auth=noSasl")

        return ";".join(params)

    def get_native_connection_kwargs(self, creds: Dict[str, Any]) -> Dict[str, Any]:
        """Get kwargs for PyHive connection."""
        auth_method = self.detect_auth_method(creds)
        kwargs: Dict[str, Any] = {
            "host": creds.get("host", "localhost"),
            "port": creds.get("port", 10000),
            "database": creds.get("database") or creds.get("schema", "default"),
        }

        if auth_method == "password":
            kwargs["username"] = creds.get("user", "")
            kwargs["password"] = creds.get("password", "")
            kwargs["auth"] = "LDAP"
        else:
            kwargs["auth"] = "NOSASL"

        return kwargs
