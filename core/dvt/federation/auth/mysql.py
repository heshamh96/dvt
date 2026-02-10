# coding=utf-8
"""MySQL authentication handler for DVT federation.

Also covers MySQL-compatible databases:
- TiDB
- SingleStore (MemSQL)

Auth Methods:
- password: Basic user/password authentication
- ssl: SSL/TLS encrypted connection
"""

from typing import Any, Dict

from .base import BaseAuthHandler


class MySQLAuthHandler(BaseAuthHandler):
    """Handles MySQL authentication methods."""

    SUPPORTED_AUTH_METHODS = ["password", "ssl"]
    INTERACTIVE_AUTH_METHODS = []

    def detect_auth_method(self, creds: Dict[str, Any]) -> str:
        """Detect MySQL auth method from credentials."""
        if creds.get("ssl_ca") or creds.get("ssl_cert") or creds.get("ssl_key"):
            return "ssl"
        return "password"

    def get_jdbc_properties(self, creds: Dict[str, Any]) -> Dict[str, str]:
        """Build MySQL JDBC properties."""
        props: Dict[str, str] = {
            "user": str(creds.get("user", "") or creds.get("username", "")),
            "password": str(creds.get("password", "")),
        }

        # SSL settings
        if creds.get("ssl_ca"):
            props["useSSL"] = "true"
            props["requireSSL"] = "true"
            props["verifyServerCertificate"] = "true"
            props["trustCertificateKeyStoreUrl"] = f"file:{creds['ssl_ca']}"
        if creds.get("ssl_cert"):
            props["clientCertificateKeyStoreUrl"] = f"file:{creds['ssl_cert']}"
        if creds.get("ssl_key"):
            props["clientCertificateKeyStorePassword"] = str(
                creds.get("ssl_key_password", "")
            )

        return props

    def get_jdbc_url_params(self, creds: Dict[str, Any]) -> str:
        """Get MySQL JDBC URL params."""
        params = []
        if creds.get("ssl_ca") or creds.get("ssl"):
            params.append("useSSL=true")
        return "&".join(params)

    def get_native_connection_kwargs(self, creds: Dict[str, Any]) -> Dict[str, Any]:
        """Get kwargs for mysql.connector or pymysql."""
        kwargs: Dict[str, Any] = {
            "host": creds.get("host", "localhost"),
            "port": creds.get("port", 3306),
            "database": creds.get("database") or creds.get("schema", ""),
            "user": creds.get("user", "") or creds.get("username", ""),
            "password": creds.get("password", ""),
        }

        # SSL settings
        if creds.get("ssl_ca"):
            kwargs["ssl"] = {
                "ca": creds["ssl_ca"],
            }
            if creds.get("ssl_cert"):
                kwargs["ssl"]["cert"] = creds["ssl_cert"]
            if creds.get("ssl_key"):
                kwargs["ssl"]["key"] = creds["ssl_key"]

        return kwargs
