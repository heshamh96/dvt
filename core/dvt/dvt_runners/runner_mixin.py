"""Shared utilities for DVT runner subclasses.

FederationModelRunner and NonDefaultPushdownRunner share significant
boilerplate: node representation, logging, profiles dir resolution,
target-aware compilation, and DvtCompiler setup.  This mixin
centralizes that code.

Usage:
    class FederationModelRunner(DvtRunnerMixin, CompileRunner):
        ...
"""

from __future__ import annotations

from typing import Optional

from dvt.artifacts.schemas.results import RunStatus
from dvt.events.types import LogModelResult, LogStartLine
from dvt.task import group_lookup
from dvt.task.run import track_model_run
from dbt_common.events.base_types import EventLevel
from dbt_common.events.functions import fire_event


class DvtRunnerMixin:
    """Mixin providing shared DVT runner functionality.

    Expects the host class to have: config, adapter, node, node_index,
    num_nodes, resolution (ResolvedExecution), and manifest.
    Also expects a ``describe_node()`` method for runner-specific descriptions.
    """

    def _init_dvt_runner(self, resolution, manifest) -> None:
        """Initialize DVT-specific runner state. Call from __init__."""
        self.resolution = resolution
        self.manifest = manifest

        from dvt.dvt_compilation.dvt_compiler import DvtCompiler

        self.compiler = DvtCompiler(self.config)

    # -- Node display --

    def get_node_representation(self) -> str:
        display_quote_policy = {
            "database": False,
            "schema": False,
            "identifier": False,
        }
        relation = self.adapter.Relation.create_from(
            self.config, self.node, quote_policy=display_quote_policy
        )
        if self.node.database == self.config.credentials.database:
            relation = relation.include(database=False)
        return str(relation)

    # -- Logging --

    def print_start_line(self):
        fire_event(
            LogStartLine(
                description=self.describe_node(),
                index=self.node_index,
                total=self.num_nodes,
                node_info=self.node.node_info,
            )
        )

    def print_result_line(self, result):
        description = self.describe_node()
        group = group_lookup.get(self.node.unique_id)
        if result.status == RunStatus.Error:
            status = result.status
            level = EventLevel.ERROR
        else:
            status = result.message
            level = EventLevel.INFO
        fire_event(
            LogModelResult(
                description=description,
                status=status,
                index=self.node_index,
                total=self.num_nodes,
                execution_time=result.execution_time,
                node_info=self.node.node_info,
                group=group,
            ),
            level=level,
        )

    def before_execute(self) -> None:
        self.print_start_line()

    def after_execute(self, result) -> None:
        track_model_run(self.node_index, self.num_nodes, result, adapter=self.adapter)
        self.print_result_line(result)

    # -- Profile/config helpers --

    def _get_profiles_dir(self) -> Optional[str]:
        """Get profiles directory from config args."""
        profiles_dir = getattr(self.config.args, "PROFILES_DIR", None)
        if not profiles_dir:
            profiles_dir = getattr(self.config.args, "profiles_dir", None)
        return str(profiles_dir) if profiles_dir else None

    def _get_profile_name(self) -> str:
        """Get current profile name."""
        return self.config.profile_name

    # -- Target-aware compilation --

    def _compile_with_target_adapter(self, manifest):
        """Compile node using the target adapter for correct dialect.

        Used by both federation and pushdown runners when the model
        targets a non-default adapter.

        For incremental models on the federation path, is_incremental()
        normally calls adapter.get_relation() which requires adapter-specific
        macros (e.g., Databricks's 'get_uc_tables'). These macros aren't in
        the manifest when the default adapter is different. Instead of trying
        to make the remote adapter work for relation checks, we determine
        is_incremental() locally: the model's Delta staging exists in .dvt/
        AND materialization is 'incremental' AND no --full-refresh flag.
        """
        from dvt.federation.adapter_manager import AdapterManager

        target_adapter = AdapterManager.get_adapter(
            profile_name=self._get_profile_name(),
            target_name=self.resolution.target,
            profiles_dir=self._get_profiles_dir(),
        )

        # Determine is_incremental() locally using Delta staging presence.
        # This avoids calling adapter.get_relation() on the remote target,
        # which would fail for non-default adapters (missing macros in manifest).
        extra_context = self._resolve_is_incremental_for_federation(manifest)

        return self.compiler.compile_node(
            self.node, manifest, extra_context, adapter=target_adapter
        )

    def _resolve_is_incremental_for_federation(self, manifest) -> dict:
        """Determine is_incremental() for federation models using local Delta staging.

        Instead of querying the remote target database to check if the relation
        exists (which requires adapter-specific macros), we check if the model's
        Delta staging directory exists locally. This mirrors the target table
        state because _save_model_staging() writes a Delta copy after every
        successful load.

        Returns an extra_context dict with an 'is_incremental' override function.
        """
        node = self.node
        mat = getattr(getattr(node, "config", None), "materialized", None)
        if mat != "incremental":
            return {}

        full_refresh = getattr(
            getattr(self.config, "args", None), "FULL_REFRESH", False
        )
        if full_refresh:
            # --full-refresh: is_incremental() should return False
            return {"is_incremental": lambda: False}

        # Check if model's Delta staging exists locally
        try:
            from dvt.config.user_config import get_bucket_path, load_buckets_for_profile
            from dvt.federation.state_manager import StateManager

            profile_name = self._get_profile_name()
            profiles_dir = self._get_profiles_dir()
            profile_buckets = load_buckets_for_profile(profile_name, profiles_dir)

            if profile_buckets:
                target_bucket = profile_buckets.get("target", "local")
                buckets = profile_buckets.get("buckets", {})
                bucket_config = buckets.get(target_bucket)
                if bucket_config:
                    bucket_path = get_bucket_path(
                        bucket_config, profile_name, profiles_dir
                    )
                    if bucket_path:
                        state_mgr = StateManager(bucket_path)
                        # Check for model's own Delta staging
                        model_staging_id = node.unique_id
                        if state_mgr.staging_exists(model_staging_id):
                            return {"is_incremental": lambda: True}
        except Exception:
            pass

        # No staging exists (first run) — is_incremental() = False
        return {"is_incremental": lambda: False}
