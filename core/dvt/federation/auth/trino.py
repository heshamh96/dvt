# coding=utf-8
"""Trino/Starburst authentication handler for DVT federation.

Auth Methods:
- password: Basic user/password authentication (LDAP)
- oauth: OAuth 2.0 authentication
- jwt: JWT token authentication
- none: No authentication (for local development)

Not supported (requires Kerberos):
- kerberos
"""

from typing import Any, Dict

from .base import BaseAuthHandler


class TrinoAuthHandler(BaseAuthHandler):
    """Handles Trino/Starburst authentication methods."""

    SUPPORTED_AUTH_METHODS = ["none", "password", "oauth", "jwt"]
    INTERACTIVE_AUTH_METHODS = []

    def detect_auth_method(self, creds: Dict[str, Any]) -> str:
        """Detect Trino auth method from credentials."""
        auth = str(creds.get("auth", "") or creds.get("method", "")).lower()

        if auth == "oauth":
            return "oauth"
        elif auth == "jwt" or creds.get("jwt_token"):
            return "jwt"
        elif creds.get("password"):
            return "password"
        else:
            return "none"

    def get_jdbc_properties(self, creds: Dict[str, Any]) -> Dict[str, str]:
        """Build Trino JDBC properties."""
        auth_method = self.detect_auth_method(creds)
        props: Dict[str, str] = {}

        if auth_method == "password":
            props["user"] = str(creds.get("user", ""))
            props["password"] = str(creds.get("password", ""))

        elif auth_method == "jwt":
            props["user"] = str(creds.get("user", ""))
            props["accessToken"] = str(creds.get("jwt_token", ""))

        elif auth_method == "oauth":
            props["user"] = str(creds.get("user", ""))
            # OAuth typically handled at connection level

        elif auth_method == "none":
            props["user"] = str(creds.get("user", "trino"))

        # SSL settings
        if creds.get("ssl") or creds.get("http_scheme") == "https":
            props["SSL"] = "true"

        return props

    def get_jdbc_url_params(self, creds: Dict[str, Any]) -> str:
        """Get Trino JDBC URL params."""
        params = []
        if creds.get("ssl") or creds.get("http_scheme") == "https":
            params.append("SSL=true")
        return "&".join(params)

    def get_native_connection_kwargs(self, creds: Dict[str, Any]) -> Dict[str, Any]:
        """Get kwargs for trino.dbapi.connect()."""
        auth_method = self.detect_auth_method(creds)
        kwargs: Dict[str, Any] = {
            "host": creds.get("host", "localhost"),
            "port": creds.get("port", 8080),
            "catalog": creds.get("catalog", ""),
            "schema": creds.get("schema", ""),
        }

        if auth_method == "password":
            kwargs["user"] = creds.get("user", "")
            kwargs["auth"] = (creds.get("user", ""), creds.get("password", ""))

        elif auth_method == "jwt":
            kwargs["user"] = creds.get("user", "")
            kwargs["jwt_token"] = creds.get("jwt_token", "")

        elif auth_method == "none":
            kwargs["user"] = creds.get("user", "trino")

        # SSL
        if creds.get("ssl") or creds.get("http_scheme") == "https":
            kwargs["http_scheme"] = "https"

        return kwargs
