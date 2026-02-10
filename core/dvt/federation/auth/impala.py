# coding=utf-8
"""Impala authentication handler for DVT federation.

Auth Methods:
- none: No authentication (NOSASL)
- password: Username/password (LDAP)

Not supported (requires Kerberos):
- kerberos
"""

from typing import Any, Dict

from .base import BaseAuthHandler


class ImpalaAuthHandler(BaseAuthHandler):
    """Handles Impala authentication methods."""

    SUPPORTED_AUTH_METHODS = ["none", "password"]
    INTERACTIVE_AUTH_METHODS = []

    def detect_auth_method(self, creds: Dict[str, Any]) -> str:
        """Detect Impala auth method from credentials."""
        auth = str(creds.get("auth_mechanism", "")).lower()
        if auth == "ldap" or creds.get("password"):
            return "password"
        return "none"

    def get_jdbc_properties(self, creds: Dict[str, Any]) -> Dict[str, str]:
        """Build Impala JDBC properties."""
        auth_method = self.detect_auth_method(creds)
        props: Dict[str, str] = {}

        if auth_method == "password":
            props["user"] = str(creds.get("user", ""))
            props["password"] = str(creds.get("password", ""))
            props["AuthMech"] = "3"  # LDAP
        else:
            props["AuthMech"] = "0"  # No auth

        return props

    def get_jdbc_url_params(self, creds: Dict[str, Any]) -> str:
        """Get Impala JDBC URL params."""
        auth_method = self.detect_auth_method(creds)
        params = []

        if auth_method == "password":
            params.append("AuthMech=3")
        else:
            params.append("AuthMech=0")

        return ";".join(params)

    def get_native_connection_kwargs(self, creds: Dict[str, Any]) -> Dict[str, Any]:
        """Get kwargs for impyla connection."""
        auth_method = self.detect_auth_method(creds)
        kwargs: Dict[str, Any] = {
            "host": creds.get("host", "localhost"),
            "port": creds.get("port", 21050),
            "database": creds.get("database") or creds.get("schema", "default"),
        }

        if auth_method == "password":
            kwargs["user"] = creds.get("user", "")
            kwargs["password"] = creds.get("password", "")
            kwargs["auth_mechanism"] = "LDAP"
        else:
            kwargs["auth_mechanism"] = "NOSASL"

        return kwargs
