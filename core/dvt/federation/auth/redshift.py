# coding=utf-8
"""Redshift authentication handler for DVT federation.

Supports all Redshift authentication methods.

Auth Methods:
- password: Database user/password
- iam: IAM-based authentication via AWS profile
"""

from typing import Any, Dict

from .base import BaseAuthHandler


class RedshiftAuthHandler(BaseAuthHandler):
    """Handles all Redshift authentication methods."""

    SUPPORTED_AUTH_METHODS = ["password", "iam"]
    INTERACTIVE_AUTH_METHODS = []

    def detect_auth_method(self, creds: Dict[str, Any]) -> str:
        """Detect Redshift auth method from credentials."""
        method = str(creds.get("method", "")).lower()
        if method == "iam":
            return "iam"
        return "password"

    def get_jdbc_properties(self, creds: Dict[str, Any]) -> Dict[str, str]:
        """Build Redshift JDBC properties."""
        auth_method = self.detect_auth_method(creds)
        props: Dict[str, str] = {}

        if auth_method == "password":
            props["user"] = str(creds.get("user", ""))
            props["password"] = str(creds.get("password", ""))

        elif auth_method == "iam":
            props["user"] = str(creds.get("user", ""))
            # IAM auth properties
            props["IAM"] = "true"
            if creds.get("cluster_id"):
                props["ClusterID"] = str(creds["cluster_id"])
            if creds.get("region"):
                props["Region"] = str(creds["region"])
            if creds.get("iam_profile"):
                props["Profile"] = str(creds["iam_profile"])
            if creds.get("db_groups"):
                # db_groups can be a list
                groups = creds["db_groups"]
                if isinstance(groups, list):
                    props["DbGroups"] = ",".join(groups)
                else:
                    props["DbGroups"] = str(groups)

        # SSL settings
        sslmode = creds.get("sslmode")
        if sslmode:
            props["ssl"] = "true" if sslmode != "disable" else "false"
            if sslmode in ("verify-ca", "verify-full"):
                props["sslMode"] = sslmode

        return props

    def get_jdbc_url_params(self, creds: Dict[str, Any]) -> str:
        """Get Redshift JDBC URL params."""
        params = []
        if creds.get("sslmode"):
            ssl_val = "true" if creds["sslmode"] != "disable" else "false"
            params.append(f"ssl={ssl_val}")
        return "&".join(params)

    def get_native_connection_kwargs(self, creds: Dict[str, Any]) -> Dict[str, Any]:
        """Get kwargs for redshift_connector."""
        auth_method = self.detect_auth_method(creds)
        kwargs: Dict[str, Any] = {
            "host": creds.get("host", ""),
            "port": creds.get("port", 5439),
            "database": creds.get("database") or creds.get("dbname", ""),
        }

        if auth_method == "password":
            kwargs["user"] = creds.get("user", "")
            kwargs["password"] = creds.get("password", "")
        elif auth_method == "iam":
            kwargs["iam"] = True
            kwargs["user"] = creds.get("user", "")
            if creds.get("cluster_id"):
                kwargs["cluster_identifier"] = creds["cluster_id"]
            if creds.get("region"):
                kwargs["region"] = creds["region"]
            if creds.get("iam_profile"):
                kwargs["profile"] = creds["iam_profile"]
            if creds.get("db_groups"):
                kwargs["db_groups"] = creds["db_groups"]

        # SSL
        if creds.get("sslmode"):
            kwargs["sslmode"] = creds["sslmode"]

        return kwargs
