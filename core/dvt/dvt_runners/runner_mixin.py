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
        """
        from dvt.federation.adapter_manager import AdapterManager

        target_adapter = AdapterManager.get_adapter(
            profile_name=self._get_profile_name(),
            target_name=self.resolution.target,
            profiles_dir=self._get_profiles_dir(),
        )
        return self.compiler.compile_node(
            self.node, manifest, {}, adapter=target_adapter
        )
