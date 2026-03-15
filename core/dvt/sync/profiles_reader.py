"""
Read profiles.yml and extract all output configurations.
Reuses dbt's profile reading logic.
"""

import os
from pathlib import Path
from typing import Any, Dict, List, Set, Tuple


def default_profiles_dir() -> str:
    """Return default profiles directory, same logic as dbt."""
    if (Path.cwd() / "profiles.yml").exists():
        return str(Path.cwd())
    return str(Path.home() / ".dbt")


def read_profiles_yml(profiles_dir: str) -> Dict[str, Any]:
    """Read and parse profiles.yml, returning the raw YAML dict."""
    import yaml

    path = os.path.join(profiles_dir, "profiles.yml")
    if not os.path.isfile(path):
        raise FileNotFoundError(f"profiles.yml not found at {path}")

    with open(path, "r") as f:
        content = yaml.safe_load(f)

    if not content:
        raise ValueError(f"profiles.yml at {path} is empty")

    return content


def extract_outputs(raw_profiles: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Extract all outputs from all profiles, returning list of output configs.

    Each output dict has at minimum: 'name', 'type', and the adapter-specific fields.
    """
    outputs = []
    for profile_name, profile_config in raw_profiles.items():
        if profile_name.startswith("config"):
            continue  # skip dbt config blocks
        if not isinstance(profile_config, dict):
            continue
        profile_outputs = profile_config.get("outputs", {})
        if not isinstance(profile_outputs, dict):
            continue
        for output_name, output_config in profile_outputs.items():
            if not isinstance(output_config, dict):
                continue
            output = {**output_config, "_name": output_name, "_profile": profile_name}
            outputs.append(output)
    return outputs


def get_adapter_types(outputs: List[Dict[str, Any]]) -> Set[str]:
    """Extract unique adapter types from outputs."""
    return {o["type"] for o in outputs if "type" in o}


def get_database_outputs(outputs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Filter to database outputs (not buckets)."""
    bucket_types = {"s3", "gcs", "azure", "local"}
    return [o for o in outputs if o.get("type") not in bucket_types]


def get_bucket_outputs(outputs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Filter to bucket outputs."""
    bucket_types = {"s3", "gcs", "azure"}
    return [o for o in outputs if o.get("type") in bucket_types]
