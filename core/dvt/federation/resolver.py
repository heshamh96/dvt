"""
Federation resolver for DVT.

Determines execution path for each model in the DAG:
- ADAPTER_PUSHDOWN: All dependencies on same target -> execute on adapter
- SPARK_FEDERATION: Cross-target dependencies -> extract + transform in Spark

Resolution hierarchy (same for target, compute, bucket):
    CLI flag > model config > profiles.yml/computes.yml/buckets.yml default

Usage:
    from dvt.federation.resolver import FederationResolver, ExecutionPath

    resolver = FederationResolver(manifest, runtime_config, args)
    executions = resolver.resolve_all(selected_nodes)

    for model_id, resolution in executions.items():
        if resolution.execution_path == ExecutionPath.SPARK_FEDERATION:
            # Execute via FederationEngine
        else:
            # Execute via adapter pushdown
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Set

from dbt_common.events.functions import fire_event

from dvt.config.user_config import (
    load_buckets_for_profile,
    load_computes_for_profile,
)


class ExecutionPath(Enum):
    """Execution path for a model."""

    ADAPTER_PUSHDOWN = "adapter_pushdown"
    SPARK_FEDERATION = "spark_federation"


@dataclass
class ResolvedExecution:
    """Resolved execution details for a model.

    Contains all information needed to execute a model:
    - target: Which database to write results to
    - compute: Which Spark compute to use (federation only)
    - bucket: Which staging bucket to use (federation only)
    - execution_path: PUSHDOWN or FEDERATION
    - upstream_targets: Set of targets used by upstream dependencies
    - materialization coercion info (view -> table for federation)
    """

    model_id: str
    target: str
    compute: Optional[str] = None
    bucket: Optional[str] = None
    execution_path: ExecutionPath = ExecutionPath.ADAPTER_PUSHDOWN
    upstream_targets: Set[str] = field(default_factory=set)
    requires_materialization_coercion: bool = False
    original_materialization: str = "table"
    coerced_materialization: Optional[str] = None

    def is_federation(self) -> bool:
        """Check if this model requires federation."""
        return self.execution_path == ExecutionPath.SPARK_FEDERATION


class FederationResolver:
    """Resolves execution paths for all models in the DAG.

    This resolver determines:
    1. Which target each model writes to
    2. Whether federation is needed (cross-target dependencies)
    3. Which compute and bucket to use for federation
    4. If materialization needs to be coerced (view -> table)

    Resolution is done BEFORE compilation to enable:
    - Proper adapter selection for Jinja context
    - Execution plan summary for users
    - Spark initialization only when needed
    """

    def __init__(
        self,
        manifest: Any,  # Manifest
        runtime_config: Any,  # RuntimeConfig
        args: Any,  # Namespace with target, compute, bucket flags
    ):
        """Initialize the resolver.

        Args:
            manifest: Parsed project manifest
            runtime_config: Runtime configuration
            args: CLI arguments (may have target, compute, bucket)
        """
        self.manifest = manifest
        self.config = runtime_config
        self.args = args

        # Cache for profile configs
        self._computes_config: Optional[Dict[str, Any]] = None
        self._buckets_config: Optional[Dict[str, Any]] = None

        # Cache for resolved upstream targets
        self._resolved_targets: Dict[str, str] = {}

    def resolve_all(
        self,
        selected_nodes: List[str],
    ) -> Dict[str, ResolvedExecution]:
        """Pre-resolve execution for all selected models.

        This is called BEFORE compilation to determine execution paths.
        Results are used to:
        1. Select proper adapters for Jinja compilation
        2. Log execution plan summary
        3. Initialize Spark if needed

        Args:
            selected_nodes: List of unique_ids for selected nodes

        Returns:
            Dict mapping model_id to ResolvedExecution
        """
        results = {}

        for node_id in selected_nodes:
            node = self.manifest.nodes.get(node_id)
            if node is None:
                continue

            # Only resolve models (not seeds, tests, etc.)
            if not hasattr(node, "resource_type"):
                continue

            if node.resource_type.value == "model":
                resolution = self.resolve_model(node)
                results[node_id] = resolution

        return results

    def resolve_model(self, model: Any) -> ResolvedExecution:
        """Resolve execution for a single model.

        Args:
            model: ModelNode to resolve

        Returns:
            ResolvedExecution with all details
        """
        model_id = model.unique_id

        # 1. Resolve target (CLI > model config > default)
        target = self._resolve_target(model)
        self._resolved_targets[model_id] = target

        # 2. Get upstream targets
        upstream_targets = self._get_upstream_targets(model)

        # 3. Determine execution path
        execution_path = self._determine_execution_path(target, upstream_targets)

        # 4. Resolve compute and bucket (only for federation)
        compute = None
        bucket = None
        if execution_path == ExecutionPath.SPARK_FEDERATION:
            compute = self._resolve_compute(model)
            bucket = self._resolve_bucket(model)

        # 5. Check materialization coercion
        original_mat = self._get_materialization(model)
        requires_coercion = False
        coerced_mat = None

        if execution_path == ExecutionPath.SPARK_FEDERATION:
            if original_mat == "view":
                # Can't create cross-database view
                requires_coercion = True
                coerced_mat = "table"

        return ResolvedExecution(
            model_id=model_id,
            target=target,
            compute=compute,
            bucket=bucket,
            execution_path=execution_path,
            upstream_targets=upstream_targets,
            requires_materialization_coercion=requires_coercion,
            original_materialization=original_mat,
            coerced_materialization=coerced_mat,
        )

    def _resolve_target(self, model: Any) -> str:
        """Resolve target for a model.

        Priority: CLI --target > model config target: > profiles.yml default

        Args:
            model: ModelNode to resolve target for

        Returns:
            Target name
        """
        # CLI flag (highest priority)
        if hasattr(self.args, "target") and self.args.target:
            return self.args.target

        # Model config
        if hasattr(model, "config") and model.config:
            model_target = getattr(model.config, "target", None)
            if model_target:
                return model_target

        # Profile default
        if hasattr(self.config, "target"):
            return self.config.target

        # Fallback to profile's default target
        if hasattr(self.config, "profile") and hasattr(self.config.profile, "target"):
            return self.config.profile.target

        # Ultimate fallback
        return "default"

    def _resolve_compute(self, model: Any) -> str:
        """Resolve compute for a model.

        Priority: CLI --compute > model config compute: > computes.yml default

        Args:
            model: ModelNode to resolve compute for

        Returns:
            Compute name
        """
        # CLI flag (highest priority)
        if hasattr(self.args, "compute") and self.args.compute:
            return self.args.compute

        # Model config
        if hasattr(model, "config") and model.config:
            model_compute = getattr(model.config, "compute", None)
            if model_compute:
                return model_compute

        # computes.yml default
        computes = self._get_computes_config()
        if computes:
            return computes.get("target", "local_spark")

        return "local_spark"

    def _resolve_bucket(self, model: Any) -> str:
        """Resolve bucket for a model.

        Priority: CLI --bucket > model config bucket: > buckets.yml default

        Args:
            model: ModelNode to resolve bucket for

        Returns:
            Bucket name
        """
        # CLI flag (highest priority)
        if hasattr(self.args, "bucket") and self.args.bucket:
            return self.args.bucket

        # Model config
        if hasattr(model, "config") and model.config:
            model_bucket = getattr(model.config, "bucket", None)
            if model_bucket:
                return model_bucket

        # buckets.yml default
        buckets = self._get_buckets_config()
        if buckets:
            return buckets.get("target", "local")

        return "local"

    def _get_upstream_targets(self, model: Any) -> Set[str]:
        """Get targets of all upstream dependencies.

        Upstream dependencies include:
        - Sources (from source.connection property)
        - Upstream models (recursively resolved)

        Args:
            model: ModelNode to get upstream targets for

        Returns:
            Set of target names
        """
        targets = set()

        if not hasattr(model, "depends_on") or not model.depends_on:
            return targets

        nodes = getattr(model.depends_on, "nodes", []) or []

        for dep_id in nodes:
            if dep_id.startswith("source."):
                # Get source's connection property (stored in source.config.connection)
                source = self.manifest.sources.get(dep_id)
                if source:
                    # Connection is in the config, not at top level
                    connection = None
                    if hasattr(source, "config") and source.config:
                        connection = getattr(source.config, "connection", None)
                    # Fallback to top-level for backwards compatibility
                    if not connection:
                        connection = getattr(source, "connection", None)
                    if connection:
                        targets.add(connection)
                    else:
                        # Source without connection - use default target
                        # (This should be caught by parse validation)
                        targets.add(self._get_default_target())

            elif dep_id.startswith("model."):
                # Get upstream model's resolved target
                upstream_model = self.manifest.nodes.get(dep_id)
                if upstream_model:
                    # Check if already resolved (from previous call)
                    if dep_id in self._resolved_targets:
                        targets.add(self._resolved_targets[dep_id])
                    else:
                        # Resolve recursively
                        upstream_target = self._resolve_target(upstream_model)
                        self._resolved_targets[dep_id] = upstream_target
                        targets.add(upstream_target)

            # Seeds, snapshots, etc. use the default target
            elif dep_id.startswith("seed.") or dep_id.startswith("snapshot."):
                targets.add(self._get_default_target())

        return targets

    def _determine_execution_path(
        self,
        target: str,
        upstream_targets: Set[str],
    ) -> ExecutionPath:
        """Determine if pushdown or federation is needed.

        Federation is required when:
        - Model has dependencies on multiple different targets
        - Model's target differs from its upstream targets

        Args:
            target: Model's resolved target
            upstream_targets: Set of upstream dependency targets

        Returns:
            ExecutionPath.ADAPTER_PUSHDOWN or ExecutionPath.SPARK_FEDERATION
        """
        if not upstream_targets:
            # No upstream dependencies - just execute on target
            return ExecutionPath.ADAPTER_PUSHDOWN

        # Check if all upstreams are on the same target as the model
        if upstream_targets == {target}:
            return ExecutionPath.ADAPTER_PUSHDOWN

        # Check if all upstreams are on a single target (but different from model)
        if len(upstream_targets) == 1:
            # Single upstream target but different from model target
            # -> Need federation to move data
            return ExecutionPath.SPARK_FEDERATION

        # Multiple upstream targets -> definitely need federation
        return ExecutionPath.SPARK_FEDERATION

    def _get_materialization(self, model: Any) -> str:
        """Get model's materialization.

        Args:
            model: ModelNode

        Returns:
            Materialization name (table, view, incremental, ephemeral)
        """
        if hasattr(model, "config") and model.config:
            mat = getattr(model.config, "materialized", None)
            if mat:
                return mat
        return "table"

    def _get_default_target(self) -> str:
        """Get default target from profile."""
        if hasattr(self.config, "target"):
            return self.config.target
        if hasattr(self.config, "profile") and hasattr(self.config.profile, "target"):
            return self.config.profile.target
        return "default"

    def _get_computes_config(self) -> Optional[Dict[str, Any]]:
        """Load computes config for current profile."""
        if self._computes_config is not None:
            return self._computes_config

        profile_name = self._get_profile_name()
        profiles_dir = self._get_profiles_dir()

        try:
            self._computes_config = load_computes_for_profile(
                profile_name, profiles_dir
            )
        except Exception:
            self._computes_config = {}

        return self._computes_config

    def _get_buckets_config(self) -> Optional[Dict[str, Any]]:
        """Load buckets config for current profile."""
        if self._buckets_config is not None:
            return self._buckets_config

        profile_name = self._get_profile_name()
        profiles_dir = self._get_profiles_dir()

        try:
            self._buckets_config = load_buckets_for_profile(profile_name, profiles_dir)
        except Exception:
            self._buckets_config = {}

        return self._buckets_config

    def _get_profile_name(self) -> str:
        """Get current profile name."""
        if hasattr(self.config, "profile_name"):
            return self.config.profile_name
        if hasattr(self.config, "profile"):
            return getattr(self.config.profile, "profile_name", "default")
        return "default"

    def _get_profiles_dir(self) -> Optional[str]:
        """Get profiles directory."""
        if hasattr(self.args, "profiles_dir"):
            return self.args.profiles_dir
        return None

    def get_execution_summary(
        self,
        resolutions: Dict[str, ResolvedExecution],
    ) -> Dict[str, Any]:
        """Generate execution plan summary for logging.

        Args:
            resolutions: Dict of resolved executions

        Returns:
            Summary dict with counts and details
        """
        pushdown_count = 0
        federation_count = 0
        by_target: Dict[str, int] = {}
        coerced_models: List[str] = []

        for model_id, resolution in resolutions.items():
            if resolution.execution_path == ExecutionPath.ADAPTER_PUSHDOWN:
                pushdown_count += 1
            else:
                federation_count += 1

            target = resolution.target
            by_target[target] = by_target.get(target, 0) + 1

            if resolution.requires_materialization_coercion:
                coerced_models.append(model_id)

        return {
            "total": len(resolutions),
            "pushdown": pushdown_count,
            "federation": federation_count,
            "by_target": by_target,
            "coerced_models": coerced_models,
        }
