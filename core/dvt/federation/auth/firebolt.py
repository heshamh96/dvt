# coding=utf-8
"""Firebolt authentication handler for DVT federation.

Auth Methods:
- password: User/password authentication
- service_account: Service account authentication
"""

from typing import Any, Dict

from .base import BaseAuthHandler


class FireboltAuthHandler(BaseAuthHandler):
    """Handles Firebolt authentication methods."""

    SUPPORTED_AUTH_METHODS = ["password", "service_account"]
    INTERACTIVE_AUTH_METHODS = []

    def detect_auth_method(self, creds: Dict[str, Any]) -> str:
        """Detect Firebolt auth method from credentials."""
        if creds.get("client_id") and creds.get("client_secret"):
            return "service_account"
        return "password"

    def get_jdbc_properties(self, creds: Dict[str, Any]) -> Dict[str, str]:
        """Build Firebolt JDBC properties."""
        auth_method = self.detect_auth_method(creds)
        props: Dict[str, str] = {}

        if auth_method == "password":
            props["user"] = str(creds.get("user", ""))
            props["password"] = str(creds.get("password", ""))

        elif auth_method == "service_account":
            props["client_id"] = str(creds.get("client_id", ""))
            props["client_secret"] = str(creds.get("client_secret", ""))

        # Engine and database
        if creds.get("engine_name"):
            props["engine"] = str(creds["engine_name"])
        if creds.get("account_name"):
            props["account"] = str(creds["account_name"])

        return props

    def get_native_connection_kwargs(self, creds: Dict[str, Any]) -> Dict[str, Any]:
        """Get kwargs for firebolt.db.connect()."""
        auth_method = self.detect_auth_method(creds)
        kwargs: Dict[str, Any] = {
            "database": creds.get("database", ""),
        }

        if creds.get("engine_name"):
            kwargs["engine_name"] = creds["engine_name"]
        if creds.get("account_name"):
            kwargs["account_name"] = creds["account_name"]

        if auth_method == "password":
            kwargs["username"] = creds.get("user", "")
            kwargs["password"] = creds.get("password", "")

        elif auth_method == "service_account":
            kwargs["client_id"] = creds.get("client_id", "")
            kwargs["client_secret"] = creds.get("client_secret", "")

        return kwargs
