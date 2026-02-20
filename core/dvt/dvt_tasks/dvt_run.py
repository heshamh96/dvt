"""DVT RunTask — extends dbt RunTask with federation routing and Spark lifecycle.

This is the DVT-specific extension of RunTask that adds:
- Federation path resolution (before_run)
- Three-way runner routing: FederationModelRunner / NonDefaultPushdownRunner / ModelRunner
- Spark session lifecycle management (init in before_run, cleanup in after_run)

The base RunTask in dvt.task.run remains identical to upstream dbt-core.
"""

from __future__ import annotations

from typing import AbstractSet, Dict, Optional, Type

from dvt.adapters.base import BaseAdapter
from dvt.adapters.factory import get_adapter
from dvt.config import RuntimeConfig
from dvt.config.user_config import load_buckets_for_profile, load_computes_for_profile
from dvt.contracts.graph.manifest import Manifest
from dvt.dvt_compilation.dvt_compiler import DvtCompiler
from dvt.dvt_runners.federation_runner import FederationModelRunner
from dvt.dvt_runners.pushdown_runner import NonDefaultPushdownRunner
from dvt.federation.el_layer import create_el_layer
from dvt.federation.resolver import ExecutionPath, FederationResolver, ResolvedExecution
from dvt.federation.spark_manager import SparkManager
from dvt.task.base import BaseRunner
from dvt.task.run import MicrobatchModelRunner, ModelRunner, RunTask
from dbt_common.events.functions import fire_event
from dbt_common.events.types import Formatting
from dbt_common.exceptions import DbtInternalError


class DvtRunTask(RunTask):
    """RunTask with DVT federation support.

    Routing logic (get_runner):
    - SPARK_FEDERATION        -> FederationModelRunner (Spark + JDBC)
    - ADAPTER_PUSHDOWN + non-default target -> NonDefaultPushdownRunner
    - ADAPTER_PUSHDOWN + default target     -> ModelRunner (standard dbt path)
    """

    def __init__(
        self,
        args: Flags,
        config: RuntimeConfig,
        manifest: Manifest,
        batch_map=None,
    ) -> None:
        super().__init__(args, config, manifest, batch_map)

        # DVT Federation state
        self._resolved_executions: Dict[str, ResolvedExecution] = {}
        self._spark_initialized: bool = False
        self._federation_count: int = 0

        # Use DVT compiler for target-aware compilation
        self.compiler = DvtCompiler(self.config)

    # ------------------------------------------------------------------
    # before_run — add federation resolution + Spark init
    # ------------------------------------------------------------------

    def before_run(
        self, adapter: BaseAdapter, selected_uids: AbstractSet[str]
    ) -> RunStatus:
        # DVT: Pre-resolve federation paths before the base before_run runs hooks.
        # We need federation info available before execution starts.
        self._resolve_federation_paths(selected_uids)

        # DVT: Initialize Spark if federation is needed
        if self._federation_count > 0:
            self._initialize_spark_for_federation()
            # Clear all source staging so every run re-extracts fresh data.
            # Model staging is preserved for incremental {{ this }} resolution.
            # Without this, stale staging from a previous run (with different
            # LIMIT, columns, or predicates) would silently produce wrong results.
            self._clear_source_staging()

        # Delegate to base RunTask.before_run (schema creation, cache, hooks)
        return super().before_run(adapter, selected_uids)

    # ------------------------------------------------------------------
    # after_run — add Spark cleanup
    # ------------------------------------------------------------------

    def after_run(self, adapter, results) -> None:
        try:
            super().after_run(adapter, results)
        finally:
            # DVT: Cleanup Spark if it was initialized
            self._cleanup_spark_for_federation()

    # ------------------------------------------------------------------
    # get_runner_type — signal federation models
    # ------------------------------------------------------------------

    def get_runner_type(self, node) -> Optional[Type[BaseRunner]]:
        if self.manifest is None:
            raise DbtInternalError(
                "manifest must be set prior to calling get_runner_type"
            )

        # DVT: Check if this model requires federation
        if node.unique_id in self._resolved_executions:
            resolution = self._resolved_executions[node.unique_id]
            if resolution.execution_path == ExecutionPath.SPARK_FEDERATION:
                return None  # Signal to use get_runner override

        if (
            node.config.materialized == "incremental"
            and node.config.incremental_strategy == "microbatch"
            and self.manifest.use_microbatch_batches(
                project_name=self.config.project_name
            )
        ):
            return MicrobatchModelRunner
        else:
            return ModelRunner

    # ------------------------------------------------------------------
    # get_runner — three-way routing
    # ------------------------------------------------------------------

    def get_runner(self, node) -> BaseRunner:
        """Route to the appropriate runner based on federation resolution."""
        adapter = get_adapter(self.config)

        # Check if this model has a resolved execution path
        if node.unique_id in self._resolved_executions:
            resolution = self._resolved_executions[node.unique_id]

            if resolution.execution_path == ExecutionPath.SPARK_FEDERATION:
                return FederationModelRunner(
                    config=self.config,
                    adapter=adapter,
                    node=node,
                    node_index=self.run_count,
                    num_nodes=self.num_nodes,
                    resolution=resolution,
                    manifest=self.manifest,
                )

            # Pushdown on a non-default target
            if (
                resolution.execution_path == ExecutionPath.ADAPTER_PUSHDOWN
                and resolution.target != self.config.target_name
            ):
                return NonDefaultPushdownRunner(
                    config=self.config,
                    adapter=adapter,
                    node=node,
                    node_index=self.run_count,
                    num_nodes=self.num_nodes,
                    resolution=resolution,
                    manifest=self.manifest,
                )

        # Default: standard dbt ModelRunner path
        runner_type = self.get_runner_type(node)
        if runner_type is None:
            runner_type = ModelRunner
        return runner_type(self.config, adapter, node, self.run_count, self.num_nodes)

    # ------------------------------------------------------------------
    # Federation helpers
    # ------------------------------------------------------------------

    def _resolve_federation_paths(self, selected_uids: AbstractSet[str]) -> None:
        """Pre-resolve execution paths for all selected models."""
        if self.manifest is None:
            return

        resolver = FederationResolver(
            manifest=self.manifest,
            runtime_config=self.config,
            args=self.args,
        )

        self._resolved_executions = resolver.resolve_all(list(selected_uids))

        # Count federation models
        self._federation_count = sum(
            1
            for r in self._resolved_executions.values()
            if r.execution_path == ExecutionPath.SPARK_FEDERATION
        )

        pushdown_count = len(self._resolved_executions) - self._federation_count

        # Log execution plan summary
        if self._resolved_executions:
            fire_event(
                Formatting(
                    msg=f"Execution plan: {pushdown_count} models via pushdown, "
                    f"{self._federation_count} models via federation"
                )
            )

    def _initialize_spark_for_federation(self) -> None:
        """Initialize Spark for federation operations."""
        if self._spark_initialized:
            return

        try:
            profile_name = self.config.profile_name
            profiles_dir = getattr(self.args, "profiles_dir", None)

            compute_config = {}
            bucket_configs = {}

            try:
                computes = load_computes_for_profile(profile_name, profiles_dir)
                if computes:
                    target_compute = computes.get("target", "local_spark")
                    all_computes = computes.get("computes", {})
                    compute_config = all_computes.get(target_compute, {})
            except Exception:
                pass

            try:
                buckets = load_buckets_for_profile(profile_name, profiles_dir)
                if buckets:
                    bucket_configs = buckets.get("buckets", {})
            except Exception:
                pass

            SparkManager.initialize(
                config=compute_config,
                bucket_configs=bucket_configs,
            )

            SparkManager.get_instance().get_or_create_session("DVT-Run")
            self._spark_initialized = True

            fire_event(
                Formatting(
                    msg=f"Initialized Spark session for federation "
                    f"({self._federation_count} models)"
                )
            )

        except Exception as e:
            fire_event(
                Formatting(
                    msg=f"Warning: Could not initialize Spark for federation: {e}"
                )
            )

    def _cleanup_spark_for_federation(self) -> None:
        """Cleanup Spark and adapters after run completes."""
        # Cleanup adapter connections from AdapterManager
        try:
            from dvt.federation.adapter_manager import AdapterManager

            AdapterManager.cleanup()
        except Exception:
            pass  # Ignore cleanup errors

        # Reset SparkManager
        if self._spark_initialized:
            try:
                SparkManager.reset()
                self._spark_initialized = False
            except Exception:
                pass  # Ignore cleanup errors

    def _clear_source_staging(self) -> None:
        """Clear all source staging at the start of each dvt run.

        Source data must always be re-extracted because model SQL may have
        changed since the last run (different LIMIT, columns, predicates).
        Model staging is preserved for incremental {{ this }} resolution.

        Within a single run, the EL layer's skip logic still deduplicates
        when multiple models share the same source — because those sources
        get re-extracted on the first model that needs them, and subsequent
        models see the fresh staging.
        """
        try:
            profile_name = self.config.profile_name
            profiles_dir = getattr(self.args, "profiles_dir", None)
            el_layer = create_el_layer(
                profile_name=profile_name,
                profiles_dir=profiles_dir,
            )
            if el_layer:
                el_layer.clear_source_staging()
        except Exception:
            pass  # Non-fatal: extraction will proceed normally
