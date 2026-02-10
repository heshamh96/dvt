# coding=utf-8
"""Universal credential extraction for DVT federation.

Dynamically extracts ALL credential fields from any dbt adapter's
Credentials dataclass, ensuring full auth method support for JDBC
and native SQL execution.

This module enables DVT to support all authentication methods
(password, keypair, OAuth, IAM, etc.) without hardcoding field names.
"""

from dataclasses import fields, is_dataclass
from typing import Any, Dict, Set

# Fields to exclude (internal/non-auth related)
EXCLUDE_FIELDS: Set[str] = {
    "type",
    "threads",
    "_ALIASES",
    "_events",
    "__dataclass_fields__",
}


def extract_all_credentials(creds: Any) -> Dict[str, Any]:
    """Extract all credential fields from an adapter's Credentials object.

    Dynamically reads ALL fields from the Credentials dataclass,
    ensuring we never miss auth-related fields like private_key_path,
    authenticator, oauth_client_id, etc.

    Args:
        creds: Adapter Credentials object (e.g., SnowflakeCredentials)

    Returns:
        Dict with all credential fields and their values
    """
    result: Dict[str, Any] = {"type": getattr(creds, "type", "")}

    if is_dataclass(creds) and not isinstance(creds, type):
        # Dataclass instance - iterate over defined fields
        for field in fields(creds):
            if field.name not in EXCLUDE_FIELDS:
                value = getattr(creds, field.name, None)
                if value is not None:
                    result[field.name] = value
    else:
        # Fallback for non-dataclass or class objects
        for attr in dir(creds):
            if attr.startswith("_") or attr in EXCLUDE_FIELDS:
                continue
            try:
                value = getattr(creds, attr)
                if not callable(value) and value is not None:
                    result[attr] = value
            except Exception:
                pass

    return result


def get_credential_field(
    creds: Dict[str, Any], *field_names: str, default: Any = None
) -> Any:
    """Get first available field from credentials.

    Useful when adapters have multiple names for the same concept
    (e.g., 'database' vs 'dbname' vs 'catalog').

    Args:
        creds: Credential dict
        field_names: Field names to try in order
        default: Default value if none found

    Returns:
        First non-None field value, or default
    """
    for name in field_names:
        value = creds.get(name)
        if value is not None:
            return value
    return default
