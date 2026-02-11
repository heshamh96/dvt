"""DVT ShowTask — extends dbt ShowTask with target-aware routing.

For models targeting non-default adapters (pushdown or federation),
routes to ShowRunnerTargetAware which queries the materialized table
on the correct target adapter.
"""

from __future__ import annotations

from typing import Dict

from dvt.adapters.factory import get_adapter
from dvt.contracts.graph.nodes import SeedNode
from dvt.dvt_runners.show_runner_target_aware import ShowRunnerTargetAware
from dvt.federation.resolver import ExecutionPath, FederationResolver, ResolvedExecution
from dvt.task.seed import SeedRunner
from dvt.task.show import ShowRunner, ShowTask


class DvtShowTask(ShowTask):
    """ShowTask with DVT federation-aware runner routing."""

    def __init__(self, args, config, manifest=None):
        super().__init__(args, config, manifest)
        self._resolved_executions: Dict[str, ResolvedExecution] = {}

    def _runtime_initialize(self):
        # Call base (which includes the parent's inline query handling)
        super()._runtime_initialize()

        # DVT: Resolve federation paths for selected models so we can
        # route non-default target models to ShowRunnerTargetAware.
        if self.args.select and self.manifest and self._flattened_nodes:
            try:
                resolver = FederationResolver(
                    manifest=self.manifest,
                    runtime_config=self.config,
                    args=self.args,
                )
                selected_uids = [n.unique_id for n in self._flattened_nodes]
                self._resolved_executions = resolver.resolve_all(selected_uids)
            except Exception:
                # If federation resolution fails, fall back to default behavior
                self._resolved_executions = {}

    def get_runner(self, node):
        """Route to the correct runner based on federation resolution."""
        adapter = get_adapter(self.config)

        if node.is_ephemeral_model:
            run_count = 0
            num_nodes = 0
        else:
            self.run_count += 1
            run_count = self.run_count
            num_nodes = self.num_nodes

        # Check if this is a seed
        if isinstance(node, SeedNode):
            return SeedRunner(self.config, adapter, node, run_count, num_nodes)

        # Check federation resolution
        resolution = self._resolved_executions.get(node.unique_id)
        if resolution:
            is_federation = resolution.execution_path == ExecutionPath.SPARK_FEDERATION
            is_non_default = resolution.target != self.config.target_name

            if is_federation or is_non_default:
                # Federation or non-default target — query the materialized
                # table on the correct target adapter instead of re-compiling.
                try:
                    if is_non_default:
                        from dvt.federation.adapter_manager import AdapterManager

                        profiles_dir = getattr(self.config.args, "PROFILES_DIR", None)
                        if not profiles_dir:
                            profiles_dir = getattr(
                                self.config.args, "profiles_dir", None
                            )
                        profiles_dir = str(profiles_dir) if profiles_dir else None

                        profile_name = self.config.profile_name

                        target_adapter = AdapterManager.get_adapter(
                            profile_name=profile_name,
                            target_name=resolution.target,
                            profiles_dir=profiles_dir,
                        )
                    else:
                        # Federation to default target — materialized table
                        # is on the default adapter
                        target_adapter = adapter

                    return ShowRunnerTargetAware(
                        self.config,
                        adapter,
                        node,
                        run_count,
                        num_nodes,
                        target_adapter,
                        resolution,
                    )
                except Exception:
                    # If adapter creation fails, fall back to default ShowRunner
                    pass

        # Default: same-target pushdown — standard ShowRunner works
        return ShowRunner(self.config, adapter, node, run_count, num_nodes)
