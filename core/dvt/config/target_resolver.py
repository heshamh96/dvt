"""
Source-target mismatch detection and execution path resolution.

For each model in the manifest, determines:
1. The model's target (CLI --target > model config target > profiles.yml default)
2. Which sources are remote (source.meta.connection != model target)
3. The execution path (default pushdown, non-default pushdown, sling direct, duckdb compute)
"""

import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Set

logger = logging.getLogger(__name__)


class ExecutionPath(Enum):
    """The execution path for a model."""

    DEFAULT_PUSHDOWN = "default_pushdown"
    NON_DEFAULT_PUSHDOWN = "non_default_pushdown"
    SLING_DIRECT = "sling_direct"
    DUCKDB_COMPUTE = "duckdb_compute"


@dataclass
class RemoteSource:
    """A source that lives on a different connection than the model's target."""

    source_unique_id: str
    source_name: str
    table_name: str
    connection: str  # profiles.yml output name where this source lives
    schema: Optional[str] = None
    identifier: Optional[str] = None


@dataclass
class ResolvedModel:
    """Resolution result for a single model."""

    model_unique_id: str
    model_name: str
    target: str  # resolved target name
    execution_path: ExecutionPath
    remote_sources: List[RemoteSource] = field(default_factory=list)
    remote_refs: List[str] = field(
        default_factory=list
    )  # ref'd model unique_ids on different targets


def get_source_connection(source_node: Any) -> Optional[str]:
    """Extract the connection property from a source node.

    DVT reads `connection` from the source's `meta` dict:
        sources:
          - name: crm
            meta:
              connection: source_postgres

    Falls back to checking config.connection for backward compat.
    """
    # Primary: meta.connection
    meta = getattr(source_node, "meta", {}) or {}
    if isinstance(meta, dict) and "connection" in meta:
        return meta["connection"]

    # Fallback: source_meta.connection (source-level meta)
    source_meta = getattr(source_node, "source_meta", {}) or {}
    if isinstance(source_meta, dict) and "connection" in source_meta:
        return source_meta["connection"]

    # Fallback: config.connection (AdditionalPropertiesAllowed)
    config = getattr(source_node, "config", None)
    if config:
        connection = getattr(config, "connection", None)
        if connection:
            return connection
        # Check _extra dict for AdditionalPropertiesAllowed
        extra = getattr(config, "_extra", {}) or {}
        if "connection" in extra:
            return extra["connection"]

    return None


def resolve_model_path(
    model_node: Any,
    manifest: Any,
    default_target: str,
    cli_target: Optional[str] = None,
) -> ResolvedModel:
    """Resolve the execution path for a single model.

    Args:
        model_node: The model node from the manifest.
        manifest: The full dbt manifest.
        default_target: The default target from profiles.yml.
        cli_target: Optional CLI --target override.

    Returns:
        ResolvedModel with the execution path and remote source details.
    """
    model_name = getattr(model_node, "name", str(model_node))
    model_uid = getattr(model_node, "unique_id", model_name)

    # 1. Resolve model's target
    if cli_target:
        model_target = cli_target
    else:
        config = getattr(model_node, "config", None)
        model_target = getattr(config, "target", None) if config else None
        if not model_target:
            model_target = default_target

    # 2. Find remote sources
    remote_sources: List[RemoteSource] = []
    depends_on = getattr(model_node, "depends_on", None)
    source_refs = []
    if depends_on:
        nodes = getattr(depends_on, "nodes", []) or []
        source_refs = [n for n in nodes if n.startswith("source.")]

    for source_uid in source_refs:
        source_node = manifest.sources.get(source_uid)
        if not source_node:
            continue

        connection = get_source_connection(source_node)
        if not connection:
            # DVT100: source without connection
            logger.warning(
                f"DVT100: Source '{source_uid}' has no connection property. "
                f"Add meta.connection to the source in sources.yml."
            )
            continue

        if connection != model_target:
            remote_sources.append(
                RemoteSource(
                    source_unique_id=source_uid,
                    source_name=getattr(source_node, "source_name", ""),
                    table_name=getattr(source_node, "name", ""),
                    connection=connection,
                    schema=getattr(source_node, "schema", None),
                    identifier=getattr(source_node, "identifier", None),
                )
            )

    # 3. Find remote refs (models on different targets)
    remote_refs: List[str] = []
    model_refs = []
    if depends_on:
        nodes = getattr(depends_on, "nodes", []) or []
        model_refs = [n for n in nodes if n.startswith("model.")]

    for ref_uid in model_refs:
        ref_node = manifest.nodes.get(ref_uid)
        if not ref_node:
            continue
        ref_config = getattr(ref_node, "config", None)
        ref_target = getattr(ref_config, "target", None) if ref_config else None
        if not ref_target:
            ref_target = default_target
        if cli_target:
            ref_target = cli_target

        if ref_target != model_target:
            remote_refs.append(ref_uid)

    # 4. Determine execution path
    total_remote = len(remote_sources) + len(remote_refs)

    if total_remote == 0:
        # No remote deps — pushdown
        if model_target == default_target:
            path = ExecutionPath.DEFAULT_PUSHDOWN
        else:
            path = ExecutionPath.NON_DEFAULT_PUSHDOWN
    elif total_remote == 1:
        # Single remote source/ref — Sling Direct (supports incremental)
        path = ExecutionPath.SLING_DIRECT
    else:
        # Multiple remote sources/refs — DuckDB Compute (table only)
        path = ExecutionPath.DUCKDB_COMPUTE

    return ResolvedModel(
        model_unique_id=model_uid,
        model_name=model_name,
        target=model_target,
        execution_path=path,
        remote_sources=remote_sources,
        remote_refs=remote_refs,
    )


def resolve_all_models(
    manifest: Any,
    default_target: str,
    cli_target: Optional[str] = None,
) -> Dict[str, ResolvedModel]:
    """Resolve execution paths for all models in the manifest.

    Returns:
        Dict mapping model unique_id → ResolvedModel.
    """
    resolutions: Dict[str, ResolvedModel] = {}

    for uid, node in manifest.nodes.items():
        resource_type = getattr(node, "resource_type", None)
        if resource_type and str(resource_type) in ("model", "ModelNode"):
            resolutions[uid] = resolve_model_path(
                node, manifest, default_target, cli_target
            )

    # Log summary
    path_counts = {}
    for r in resolutions.values():
        path_counts[r.execution_path.value] = (
            path_counts.get(r.execution_path.value, 0) + 1
        )

    logger.info(f"DVT resolved {len(resolutions)} models: {path_counts}")
    return resolutions
