# coding=utf-8
"""PostgreSQL authentication handler for DVT federation.

Also covers PostgreSQL-compatible databases:
- Greenplum
- Materialize
- RisingWave
- CrateDB
- AlloyDB
- TimescaleDB

Auth Methods:
- password: Basic user/password authentication
- ssl_cert: SSL certificate authentication
"""

from typing import Any, Dict

from .base import BaseAuthHandler


class PostgresAuthHandler(BaseAuthHandler):
    """Handles PostgreSQL authentication methods."""

    SUPPORTED_AUTH_METHODS = ["password", "ssl_cert"]
    INTERACTIVE_AUTH_METHODS = []

    def detect_auth_method(self, creds: Dict[str, Any]) -> str:
        """Detect PostgreSQL auth method from credentials."""
        if creds.get("sslcert") or creds.get("sslkey"):
            return "ssl_cert"
        return "password"

    def get_jdbc_properties(self, creds: Dict[str, Any]) -> Dict[str, str]:
        """Build PostgreSQL JDBC properties."""
        props: Dict[str, str] = {
            "user": str(creds.get("user", "")),
            "password": str(creds.get("password", "")),
        }

        # SSL settings
        sslmode = creds.get("sslmode")
        if sslmode:
            props["sslmode"] = str(sslmode)
            if sslmode != "disable":
                props["ssl"] = "true"
        if creds.get("sslcert"):
            props["sslcert"] = str(creds["sslcert"])
        if creds.get("sslkey"):
            props["sslkey"] = str(creds["sslkey"])
        if creds.get("sslrootcert"):
            props["sslrootcert"] = str(creds["sslrootcert"])

        return props

    def get_jdbc_url_params(self, creds: Dict[str, Any]) -> str:
        """Get PostgreSQL JDBC URL params."""
        params = []
        if creds.get("sslmode"):
            params.append(f"sslmode={creds['sslmode']}")
        return "&".join(params)

    def get_native_connection_kwargs(self, creds: Dict[str, Any]) -> Dict[str, Any]:
        """Get kwargs for psycopg2.connect()."""
        kwargs: Dict[str, Any] = {
            "host": creds.get("host", "localhost"),
            "port": creds.get("port", 5432),
            "dbname": creds.get("database") or creds.get("dbname", ""),
            "user": creds.get("user", ""),
            "password": creds.get("password", ""),
        }

        # SSL settings
        if creds.get("sslmode"):
            kwargs["sslmode"] = creds["sslmode"]
        if creds.get("sslcert"):
            kwargs["sslcert"] = creds["sslcert"]
        if creds.get("sslkey"):
            kwargs["sslkey"] = creds["sslkey"]
        if creds.get("sslrootcert"):
            kwargs["sslrootcert"] = creds["sslrootcert"]

        # Connection options
        if creds.get("connect_timeout"):
            kwargs["connect_timeout"] = creds["connect_timeout"]

        return kwargs
