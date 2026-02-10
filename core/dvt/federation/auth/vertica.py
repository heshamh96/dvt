# coding=utf-8
"""Vertica authentication handler for DVT federation.

Auth Methods:
- password: Basic user/password authentication
- ssl: SSL/TLS encrypted connection

Not supported (requires Kerberos):
- kerberos
"""

from typing import Any, Dict

from .base import BaseAuthHandler


class VerticaAuthHandler(BaseAuthHandler):
    """Handles Vertica authentication methods."""

    SUPPORTED_AUTH_METHODS = ["password", "ssl"]
    INTERACTIVE_AUTH_METHODS = []

    def detect_auth_method(self, creds: Dict[str, Any]) -> str:
        """Detect Vertica auth method from credentials."""
        if creds.get("ssl") or creds.get("ssl_cert"):
            return "ssl"
        return "password"

    def get_jdbc_properties(self, creds: Dict[str, Any]) -> Dict[str, str]:
        """Build Vertica JDBC properties."""
        props: Dict[str, str] = {
            "user": str(creds.get("user", "")),
            "password": str(creds.get("password", "")),
        }

        # SSL settings
        if creds.get("ssl"):
            props["SSL"] = "true"
        if creds.get("ssl_cert"):
            props["SSLCertificatePath"] = str(creds["ssl_cert"])

        return props

    def get_native_connection_kwargs(self, creds: Dict[str, Any]) -> Dict[str, Any]:
        """Get kwargs for vertica_python.connect()."""
        kwargs: Dict[str, Any] = {
            "host": creds.get("host", "localhost"),
            "port": creds.get("port", 5433),
            "database": creds.get("database", ""),
            "user": creds.get("user", ""),
            "password": creds.get("password", ""),
        }

        # SSL settings
        if creds.get("ssl"):
            kwargs["ssl"] = True
        if creds.get("ssl_cert"):
            kwargs["ssl_options"] = {"ca_certs": creds["ssl_cert"]}

        return kwargs
