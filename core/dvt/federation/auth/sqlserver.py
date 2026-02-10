# coding=utf-8
"""SQL Server authentication handler for DVT federation.

Also covers SQL Server-compatible databases:
- Azure Synapse Analytics
- Microsoft Fabric

Auth Methods:
- password: SQL Server authentication (user/password)
- azure_ad: Azure Active Directory authentication
- windows: Windows integrated authentication (blocked for federation)
"""

from typing import Any, Dict

from .base import BaseAuthHandler


class SQLServerAuthHandler(BaseAuthHandler):
    """Handles SQL Server authentication methods."""

    SUPPORTED_AUTH_METHODS = ["password", "azure_ad", "windows"]
    INTERACTIVE_AUTH_METHODS = ["windows"]  # Windows auth requires domain context

    def detect_auth_method(self, creds: Dict[str, Any]) -> str:
        """Detect SQL Server auth method from credentials."""
        auth = str(creds.get("authentication", "")).lower()

        if auth in ("activedirectorypassword", "activedirectoryintegrated", "azure"):
            return "azure_ad"
        elif auth in ("windows", "integrated"):
            return "windows"
        else:
            return "password"

    def get_jdbc_properties(self, creds: Dict[str, Any]) -> Dict[str, str]:
        """Build SQL Server JDBC properties."""
        auth_method = self.detect_auth_method(creds)
        props: Dict[str, str] = {}

        if auth_method == "password":
            props["user"] = str(creds.get("user", ""))
            props["password"] = str(creds.get("password", ""))

        elif auth_method == "azure_ad":
            props["user"] = str(creds.get("user", ""))
            props["password"] = str(creds.get("password", ""))
            props["authentication"] = "ActiveDirectoryPassword"
            if creds.get("tenant_id"):
                props["AADSecurePrincipalId"] = str(creds["tenant_id"])

        # Encryption settings
        if creds.get("encrypt"):
            props["encrypt"] = str(creds["encrypt"]).lower()
        if creds.get("trust_cert") or creds.get("TrustServerCertificate"):
            props["trustServerCertificate"] = "true"

        return props

    def get_jdbc_url_params(self, creds: Dict[str, Any]) -> str:
        """Get SQL Server JDBC URL params."""
        params = []
        if creds.get("encrypt"):
            params.append(f"encrypt={creds['encrypt']}")
        if creds.get("trust_cert"):
            params.append("trustServerCertificate=true")
        return ";".join(params)

    def get_native_connection_kwargs(self, creds: Dict[str, Any]) -> Dict[str, Any]:
        """Get kwargs for pymssql or pyodbc."""
        auth_method = self.detect_auth_method(creds)
        kwargs: Dict[str, Any] = {
            "server": creds.get("host", "") or creds.get("server", ""),
            "port": str(creds.get("port", 1433)),
            "database": creds.get("database", ""),
        }

        if auth_method in ("password", "azure_ad"):
            kwargs["user"] = creds.get("user", "")
            kwargs["password"] = creds.get("password", "")

        if auth_method == "azure_ad":
            # For pyodbc with Azure AD
            kwargs["Authentication"] = "ActiveDirectoryPassword"

        # Encryption
        if creds.get("encrypt"):
            kwargs["encrypt"] = creds["encrypt"]

        return kwargs
