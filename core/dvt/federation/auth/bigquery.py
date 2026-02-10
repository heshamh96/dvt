# coding=utf-8
"""BigQuery authentication handler for DVT federation.

Supports all BigQuery authentication methods with special handling
for the native Spark BigQuery Connector (preferred) with JDBC fallback.

Auth Methods:
- oauth: OAuth via gcloud CLI (uses application default credentials)
- service_account: Service account key file
- service_account_json: Inline service account JSON

Special:
- Prefers Spark BigQuery Connector (native, faster)
- Falls back to JDBC (Simba driver) if connector unavailable
"""

import json
from typing import Any, Dict

from .base import BaseAuthHandler


class BigQueryAuthHandler(BaseAuthHandler):
    """Handles all BigQuery authentication methods."""

    SUPPORTED_AUTH_METHODS = ["oauth", "service_account", "service_account_json"]
    INTERACTIVE_AUTH_METHODS = []  # oauth uses gcloud, not browser popup

    def detect_auth_method(self, creds: Dict[str, Any]) -> str:
        """Detect BigQuery auth method from credentials."""
        method = str(creds.get("method", "")).lower()

        if method == "service-account" or creds.get("keyfile"):
            return "service_account"
        elif method == "service-account-json" or creds.get("keyfile_json"):
            return "service_account_json"
        else:
            return "oauth"

    def get_jdbc_properties(self, creds: Dict[str, Any]) -> Dict[str, str]:
        """Build JDBC properties for Simba BigQuery driver."""
        auth_method = self.detect_auth_method(creds)
        props: Dict[str, str] = {}

        if auth_method == "service_account":
            props["OAuthType"] = "0"  # Service account
            keyfile = str(creds.get("keyfile", ""))
            props["OAuthServiceAcctEmail"] = self._get_service_account_email(keyfile)
            props["OAuthPvtKeyPath"] = keyfile

        elif auth_method == "service_account_json":
            # JDBC doesn't support inline JSON - write to temp file
            props["OAuthType"] = "0"
            keyfile_json = creds.get("keyfile_json", {})
            if keyfile_json:
                json_str = json.dumps(keyfile_json)
                temp_path = self.create_temp_credentials_file(json_str)
                props["OAuthPvtKeyPath"] = temp_path
                if isinstance(keyfile_json, dict):
                    props["OAuthServiceAcctEmail"] = keyfile_json.get(
                        "client_email", ""
                    )

        else:  # oauth
            props["OAuthType"] = "1"  # User authentication

        return props

    def _get_service_account_email(self, keyfile_path: str) -> str:
        """Extract service account email from keyfile."""
        try:
            with open(keyfile_path, "r") as f:
                data = json.load(f)
                return data.get("client_email", "")
        except Exception:
            return ""

    def get_native_connection_kwargs(self, creds: Dict[str, Any]) -> Dict[str, Any]:
        """Get kwargs for google-cloud-bigquery client."""
        auth_method = self.detect_auth_method(creds)
        kwargs: Dict[str, Any] = {
            "project": creds.get("project", ""),
        }

        if auth_method == "service_account":
            kwargs["keyfile"] = creds.get("keyfile")
        elif auth_method == "service_account_json":
            kwargs["keyfile_json"] = creds.get("keyfile_json")
        # oauth uses application default credentials

        return kwargs

    def get_spark_connector_options(self, creds: Dict[str, Any]) -> Dict[str, str]:
        """Get options for Spark BigQuery Connector (preferred over JDBC).

        The Spark BigQuery Connector is much faster than JDBC for large datasets.
        """
        auth_method = self.detect_auth_method(creds)
        options: Dict[str, str] = {
            "project": str(creds.get("project", "")),
            "parentProject": str(creds.get("project", "")),
        }

        if creds.get("dataset"):
            options["dataset"] = str(creds["dataset"])

        if auth_method == "service_account":
            options["credentialsFile"] = str(creds.get("keyfile", ""))

        elif auth_method == "service_account_json":
            # Write inline JSON to temp file, register for cleanup
            keyfile_json = creds.get("keyfile_json", {})
            if keyfile_json:
                json_str = json.dumps(keyfile_json)
                temp_path = self.create_temp_credentials_file(json_str)
                options["credentialsFile"] = temp_path

        # oauth uses application default credentials (no explicit file needed)

        return options

    def supports_native_connector(self) -> bool:
        """Check if Spark BigQuery Connector is available.

        Returns True - actual availability is checked at runtime.
        """
        return True

    def use_native_connector(self, creds: Dict[str, Any]) -> bool:
        """Determine if native connector should be used.

        BigQuery always prefers native connector when available,
        as it's significantly faster than JDBC.
        """
        return True
