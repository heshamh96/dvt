# coding=utf-8
"""IBM DB2 authentication handler for DVT federation.

Auth Methods:
- password: Basic user/password authentication

Not supported (requires Kerberos):
- kerberos
"""

from typing import Any, Dict

from .base import BaseAuthHandler


class DB2AuthHandler(BaseAuthHandler):
    """Handles IBM DB2 authentication methods."""

    SUPPORTED_AUTH_METHODS = ["password"]
    INTERACTIVE_AUTH_METHODS = []

    def detect_auth_method(self, creds: Dict[str, Any]) -> str:
        """Detect DB2 auth method from credentials."""
        return "password"

    def get_jdbc_properties(self, creds: Dict[str, Any]) -> Dict[str, str]:
        """Build DB2 JDBC properties."""
        props: Dict[str, str] = {
            "user": str(creds.get("user", "")),
            "password": str(creds.get("password", "")),
        }

        # SSL settings
        if creds.get("ssl"):
            props["sslConnection"] = "true"

        return props

    def get_native_connection_kwargs(self, creds: Dict[str, Any]) -> Dict[str, Any]:
        """Get kwargs for ibm_db.connect()."""
        # ibm_db uses connection string format
        host = creds.get("host", "localhost")
        port = creds.get("port", 50000)
        database = creds.get("database", "")
        user = creds.get("user", "")
        password = creds.get("password", "")

        conn_str = (
            f"DATABASE={database};"
            f"HOSTNAME={host};"
            f"PORT={port};"
            f"PROTOCOL=TCPIP;"
            f"UID={user};"
            f"PWD={password};"
        )

        if creds.get("ssl"):
            conn_str += "SECURITY=SSL;"

        return {"connection_string": conn_str}
