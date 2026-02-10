# coding=utf-8
"""Snowflake authentication handler for DVT federation.

Supports all Snowflake authentication methods except those requiring
browser interaction (externalbrowser SSO).

Auth Methods:
- password: Basic user/password authentication
- keypair: Private key file or PEM string
- oauth: OAuth with refresh token

Blocked for federation:
- externalbrowser: Requires browser interaction (SSO)
"""

from typing import Any, Dict, Optional

from .base import BaseAuthHandler


class SnowflakeAuthHandler(BaseAuthHandler):
    """Handles all Snowflake authentication methods."""

    SUPPORTED_AUTH_METHODS = ["password", "keypair", "oauth", "externalbrowser"]
    INTERACTIVE_AUTH_METHODS = ["externalbrowser"]

    def detect_auth_method(self, creds: Dict[str, Any]) -> str:
        """Detect Snowflake auth method from credentials."""
        authenticator = str(creds.get("authenticator", "")).lower()

        if authenticator == "externalbrowser":
            return "externalbrowser"
        elif authenticator == "oauth":
            return "oauth"
        elif creds.get("private_key_path") or creds.get("private_key"):
            return "keypair"
        else:
            return "password"

    def get_jdbc_properties(self, creds: Dict[str, Any]) -> Dict[str, str]:
        """Build Snowflake JDBC properties."""
        auth_method = self.detect_auth_method(creds)
        props: Dict[str, str] = {}

        if auth_method == "password":
            props["user"] = str(creds.get("user", ""))
            props["password"] = str(creds.get("password", ""))

        elif auth_method == "keypair":
            props["user"] = str(creds.get("user", ""))
            # Snowflake JDBC supports private_key_file parameter
            if creds.get("private_key_path"):
                props["private_key_file"] = str(creds["private_key_path"])
            if creds.get("private_key_passphrase"):
                props["private_key_file_pwd"] = str(creds["private_key_passphrase"])
            # Note: inline private_key (PEM string) requires special handling
            # The JDBC driver expects a file path, not the key content

        elif auth_method == "oauth":
            props["authenticator"] = "oauth"
            props["token"] = str(creds.get("token", ""))

        # Common properties
        if creds.get("role"):
            props["role"] = str(creds["role"])
        if creds.get("warehouse"):
            props["warehouse"] = str(creds["warehouse"])

        return props

    def get_jdbc_url_params(self, creds: Dict[str, Any]) -> str:
        """Get Snowflake JDBC URL params."""
        params = []
        if creds.get("warehouse"):
            params.append(f"warehouse={creds['warehouse']}")
        if creds.get("role"):
            params.append(f"role={creds['role']}")
        return "&".join(params)

    def get_native_connection_kwargs(self, creds: Dict[str, Any]) -> Dict[str, Any]:
        """Get snowflake.connector.connect() kwargs."""
        auth_method = self.detect_auth_method(creds)
        kwargs: Dict[str, Any] = {
            "account": creds.get("account", ""),
            "database": creds.get("database"),
            "schema": creds.get("schema"),
        }

        if creds.get("warehouse"):
            kwargs["warehouse"] = creds["warehouse"]
        if creds.get("role"):
            kwargs["role"] = creds["role"]

        if auth_method == "password":
            kwargs["user"] = creds.get("user", "")
            kwargs["password"] = creds.get("password", "")

        elif auth_method == "keypair":
            kwargs["user"] = creds.get("user", "")
            if creds.get("private_key_path"):
                kwargs["private_key_path"] = creds["private_key_path"]
            if creds.get("private_key"):
                # PEM string - need to parse it
                kwargs["private_key"] = self._parse_private_key(
                    creds["private_key"], creds.get("private_key_passphrase")
                )
            if creds.get("private_key_passphrase") and not creds.get("private_key"):
                kwargs["private_key_passphrase"] = creds["private_key_passphrase"]

        elif auth_method == "oauth":
            kwargs["authenticator"] = "oauth"
            kwargs["token"] = creds.get("token", "")

        return kwargs

    def _parse_private_key(
        self, key_str: str, passphrase: Optional[str] = None
    ) -> bytes:
        """Parse private key from PEM string or Base64 DER.

        Args:
            key_str: Private key as PEM or Base64-encoded DER
            passphrase: Optional passphrase for encrypted keys

        Returns:
            Private key bytes in DER format
        """
        try:
            from cryptography.hazmat.backends import default_backend
            from cryptography.hazmat.primitives import serialization
        except ImportError:
            raise ImportError(
                "cryptography package required for Snowflake keypair auth. "
                "Install with: pip install cryptography"
            )

        key_bytes = key_str.encode("utf-8")
        pwd = passphrase.encode("utf-8") if passphrase else None

        try:
            # Try PEM format first
            private_key = serialization.load_pem_private_key(
                key_bytes, password=pwd, backend=default_backend()
            )
        except Exception:
            # Try Base64 DER format
            import base64

            der_bytes = base64.b64decode(key_str)
            private_key = serialization.load_der_private_key(
                der_bytes, password=pwd, backend=default_backend()
            )

        return private_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )
