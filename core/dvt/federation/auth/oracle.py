# coding=utf-8
"""Oracle authentication handler for DVT federation.

Auth Methods:
- password: Basic user/password authentication
- wallet: Oracle Wallet authentication (for cloud databases)

Not supported (requires Kerberos):
- kerberos
"""

from typing import Any, Dict

from .base import BaseAuthHandler


class OracleAuthHandler(BaseAuthHandler):
    """Handles Oracle authentication methods."""

    SUPPORTED_AUTH_METHODS = ["password", "wallet"]
    INTERACTIVE_AUTH_METHODS = []

    def detect_auth_method(self, creds: Dict[str, Any]) -> str:
        """Detect Oracle auth method from credentials."""
        if creds.get("wallet_location") or creds.get("wallet_password"):
            return "wallet"
        return "password"

    def get_jdbc_properties(self, creds: Dict[str, Any]) -> Dict[str, str]:
        """Build Oracle JDBC properties."""
        auth_method = self.detect_auth_method(creds)
        props: Dict[str, str] = {}

        if auth_method == "password":
            props["user"] = str(creds.get("user", ""))
            props["password"] = str(creds.get("password", ""))

        elif auth_method == "wallet":
            # Oracle Wallet authentication
            if creds.get("wallet_location"):
                props["oracle.net.wallet_location"] = str(creds["wallet_location"])
            if creds.get("wallet_password"):
                props["oracle.net.wallet_password"] = str(creds["wallet_password"])

        return props

    def get_jdbc_url_params(self, creds: Dict[str, Any]) -> str:
        """Get Oracle JDBC URL params."""
        # Oracle uses TNS or easy connect string, not URL params typically
        return ""

    def get_native_connection_kwargs(self, creds: Dict[str, Any]) -> Dict[str, Any]:
        """Get kwargs for cx_Oracle or oracledb."""
        auth_method = self.detect_auth_method(creds)

        # Build DSN
        host = creds.get("host", "localhost")
        port = creds.get("port", 1521)
        database = creds.get("database") or creds.get("service", "")

        kwargs: Dict[str, Any] = {
            "dsn": f"{host}:{port}/{database}",
        }

        if auth_method == "password":
            kwargs["user"] = creds.get("user", "")
            kwargs["password"] = creds.get("password", "")

        elif auth_method == "wallet":
            if creds.get("wallet_location"):
                kwargs["wallet_location"] = creds["wallet_location"]
            if creds.get("wallet_password"):
                kwargs["wallet_password"] = creds["wallet_password"]

        return kwargs
