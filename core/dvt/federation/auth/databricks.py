# coding=utf-8
"""Databricks authentication handler for DVT federation.

Supports all Databricks authentication methods except those requiring
browser interaction (OAuth U2M).

Auth Methods:
- token: Personal Access Token (most common)
- oauth_m2m: Machine-to-machine OAuth (client credentials)
- azure_oauth: Azure AD service principal

Blocked for federation:
- oauth_u2m: User-to-machine OAuth (requires browser)
"""

from typing import Any, Dict

from .base import BaseAuthHandler


class DatabricksAuthHandler(BaseAuthHandler):
    """Handles all Databricks authentication methods."""

    SUPPORTED_AUTH_METHODS = ["token", "oauth_m2m", "oauth_u2m", "azure_oauth"]
    INTERACTIVE_AUTH_METHODS = ["oauth_u2m"]

    def detect_auth_method(self, creds: Dict[str, Any]) -> str:
        """Detect Databricks auth method from credentials."""
        auth_type = str(creds.get("auth_type", "")).lower()

        if auth_type == "oauth":
            # Distinguish M2M vs U2M
            if creds.get("client_id") and creds.get("client_secret"):
                return "oauth_m2m"
            elif creds.get("azure_client_id") and creds.get("azure_client_secret"):
                return "azure_oauth"
            else:
                return "oauth_u2m"  # Browser-based
        elif creds.get("token"):
            return "token"
        else:
            return "token"  # Default to token

    def get_jdbc_properties(self, creds: Dict[str, Any]) -> Dict[str, str]:
        """Build Databricks JDBC properties."""
        auth_method = self.detect_auth_method(creds)
        props: Dict[str, str] = {}

        if auth_method == "token":
            # Databricks JDBC: user="token", password=<access_token>
            props["user"] = "token"
            props["password"] = str(creds.get("token", ""))

        elif auth_method == "oauth_m2m":
            props["Auth_Flow"] = "1"  # M2M flow
            props["OAuth2ClientId"] = str(creds.get("client_id", ""))
            props["OAuth2Secret"] = str(creds.get("client_secret", ""))

        elif auth_method == "azure_oauth":
            props["Auth_Flow"] = "1"
            props["OAuth2ClientId"] = str(creds.get("azure_client_id", ""))
            props["OAuth2Secret"] = str(creds.get("azure_client_secret", ""))

        return props

    def get_native_connection_kwargs(self, creds: Dict[str, Any]) -> Dict[str, Any]:
        """Get kwargs for databricks-sql-connector."""
        auth_method = self.detect_auth_method(creds)
        kwargs: Dict[str, Any] = {
            "server_hostname": creds.get("host", ""),
            "http_path": creds.get("http_path", ""),
        }

        if creds.get("catalog"):
            kwargs["catalog"] = creds["catalog"]
        if creds.get("schema"):
            kwargs["schema"] = creds["schema"]

        if auth_method == "token":
            kwargs["access_token"] = creds.get("token", "")
        elif auth_method in ("oauth_m2m", "azure_oauth"):
            kwargs["client_id"] = creds.get("client_id") or creds.get("azure_client_id")
            kwargs["client_secret"] = creds.get("client_secret") or creds.get(
                "azure_client_secret"
            )

        return kwargs
