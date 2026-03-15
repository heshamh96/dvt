"""
Read source connection mappings directly from sources.yml files.

dbt's source parser doesn't preserve the `connection:` property on sources
(it's a DVT extension, not a standard dbt field). DVT reads sources.yml
files directly to build a mapping of source_name → connection_name.

This mapping is used by the target resolver to detect which sources are
remote (connection != model target) and need Sling extraction.
"""

import logging
import os
from typing import Any, Dict, List, Optional

import yaml

logger = logging.getLogger(__name__)


# Mapping: (source_name, table_name) → connection_name
SourceConnectionMap = Dict[str, str]  # source_name → connection_name


def load_source_connections(project_dir: str) -> SourceConnectionMap:
    """Scan the project for sources.yml / schema.yml files and extract connection mappings.

    Searches all YAML files under model-paths for source definitions with
    a `connection:` property.

    Args:
        project_dir: Root directory of the dbt project.

    Returns:
        Dict mapping source_name → connection_name.
    """
    connections: SourceConnectionMap = {}

    # Read dbt_project.yml to find model-paths
    project_yml = os.path.join(project_dir, "dbt_project.yml")
    model_paths = ["models"]  # default
    if os.path.isfile(project_yml):
        try:
            with open(project_yml, "r") as f:
                project_config = yaml.safe_load(f) or {}
            model_paths = project_config.get("model-paths", model_paths)
        except Exception:
            pass

    # Scan all YAML files under model-paths
    for model_path in model_paths:
        full_path = os.path.join(project_dir, model_path)
        if not os.path.isdir(full_path):
            continue

        for root, dirs, files in os.walk(full_path):
            for filename in files:
                if not filename.endswith((".yml", ".yaml")):
                    continue
                filepath = os.path.join(root, filename)
                _extract_connections_from_file(filepath, connections)

    if connections:
        logger.info(
            f"DVT: Found {len(connections)} source connections: "
            f"{', '.join(f'{k}={v}' for k, v in sorted(connections.items()))}"
        )

    return connections


def _extract_connections_from_file(
    filepath: str, connections: SourceConnectionMap
) -> None:
    """Extract source connection mappings from a single YAML file."""
    try:
        with open(filepath, "r") as f:
            content = yaml.safe_load(f)
    except Exception:
        return

    if not isinstance(content, dict):
        return

    sources = content.get("sources", [])
    if not isinstance(sources, list):
        return

    for source in sources:
        if not isinstance(source, dict):
            continue

        source_name = source.get("name")
        if not source_name:
            continue

        # DVT reads connection from top-level (DVT extension)
        connection = source.get("connection")

        # Fallback: meta.connection
        if not connection:
            meta = source.get("meta", {})
            if isinstance(meta, dict):
                connection = meta.get("connection")

        # Fallback: config.connection
        if not connection:
            config = source.get("config", {})
            if isinstance(config, dict):
                connection = config.get("connection")

        if connection:
            connections[source_name] = connection


def get_source_connection(
    source_name: str,
    source_connections: SourceConnectionMap,
) -> Optional[str]:
    """Look up the connection for a source by name.

    Args:
        source_name: The source name (e.g., 'crm', 'pg_source').
        source_connections: The connection map from load_source_connections().

    Returns:
        The connection name (profiles.yml output name) or None.
    """
    return source_connections.get(source_name)
