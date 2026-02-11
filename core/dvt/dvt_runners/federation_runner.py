"""DVT FederationModelRunner — executes models via cross-target Spark federation.

This runner is used when a model has dependencies on sources/models from different
targets than its own target.  The execution path is:
1. Compile (Jinja resolution) — handled by parent CompileRunner
2. Extract sources to staging (with predicate pushdown)
3. Transform in Spark (SQLGlot translation)
4. Load to target (via bucket or JDBC)
"""

from __future__ import annotations

import threading
import time
from typing import Any, Dict, Optional

from dvt.adapters.base import BaseAdapter
from dvt.artifacts.schemas.results import RunStatus
from dvt.artifacts.schemas.run import RunResult
from dvt.config import RuntimeConfig
from dvt.contracts.graph.manifest import Manifest
from dvt.contracts.graph.nodes import ModelNode
from dvt.events.types import LogModelResult, LogStartLine
from dvt.exceptions import DbtRuntimeError
from dvt.federation.engine import FederationEngine
from dvt.federation.resolver import ResolvedExecution
from dvt.task import group_lookup
from dvt.task.compile import CompileRunner
from dvt.task.run import track_model_run
from dbt_common.events.base_types import EventLevel
from dbt_common.events.functions import fire_event


class FederationModelRunner(CompileRunner):
    """Runner for models that require cross-target federation.

    This runner executes models via the FederationEngine when the model
    has dependencies on sources/models from different targets than its
    own target.
    """

    def __init__(
        self,
        config: RuntimeConfig,
        adapter: BaseAdapter,
        node: ModelNode,
        node_index: int,
        num_nodes: int,
        resolution: ResolvedExecution,
        manifest: Manifest,
    ):
        super().__init__(config, adapter, node, node_index, num_nodes)
        self.resolution = resolution
        self.manifest = manifest
        self._federation_engine: Optional[FederationEngine] = None

        # Use DvtCompiler for target-aware compilation (adapter= param)
        from dvt.dvt_compilation.dvt_compiler import DvtCompiler

        self.compiler = DvtCompiler(config)

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

    def describe_node(self) -> str:
        upstream_targets = ", ".join(sorted(self.resolution.upstream_targets))
        return (
            f"{self.node.language} federation model {self.get_node_representation()} "
            f"[{upstream_targets}] -> {self.resolution.target}"
        )

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

    def compile(self, manifest: Manifest):
        """Compile with target-aware adapter.

        DVT: For federation models targeting a non-default adapter, we compile
        using the target adapter so that Jinja resolves {{ ref() }} and
        {{ source() }} with the correct dialect (quoting, relation format).
        This eliminates the need for post-hoc SQL transpilation.

        If the model targets the default adapter, we fall back to the
        standard compile path.
        """
        if self.resolution.target == self.config.target_name:
            # Same as default — use standard compilation
            return super().compile(manifest)

        from dvt.federation.adapter_manager import AdapterManager

        target_adapter = AdapterManager.get_adapter(
            profile_name=self._get_profile_name(),
            target_name=self.resolution.target,
            profiles_dir=self._get_profiles_dir(),
        )
        return self.compiler.compile_node(
            self.node, manifest, {}, adapter=target_adapter
        )

    def _get_profiles_dir(self) -> Optional[str]:
        """Get profiles directory from config args."""
        profiles_dir = getattr(self.config.args, "PROFILES_DIR", None)
        if not profiles_dir:
            profiles_dir = getattr(self.config.args, "profiles_dir", None)
        return str(profiles_dir) if profiles_dir else None

    def _get_profile_name(self) -> str:
        """Get current profile name."""
        if hasattr(self.config, "profile_name"):
            return self.config.profile_name
        return "default"

    def execute(self, model: ModelNode, manifest: Manifest) -> RunResult:
        """Execute model via federation engine.

        Note: model.compiled_code has Jinja resolved (from compile phase).
        """
        # Log materialization coercion warning if needed
        if self.resolution.requires_materialization_coercion:
            fire_event(
                LogModelResult(
                    description=(
                        f"Materialization coerced: {self.resolution.original_materialization} "
                        f"-> {self.resolution.coerced_materialization} "
                        f"(cross-target federation cannot create views)"
                    ),
                    status="WARN",
                    index=self.node_index,
                    total=self.num_nodes,
                    execution_time=0,
                    node_info=model.node_info,
                    group=group_lookup.get(model.unique_id),
                ),
                level=EventLevel.WARN,
            )

        # Create federation engine
        def on_progress(msg: str) -> None:
            # Log progress to console for visibility
            print(f"  {msg}")

        engine = FederationEngine(
            runtime_config=self.config,
            manifest=manifest,
            on_progress=on_progress,
        )

        # Execute via federation
        result = engine.execute(
            model=model,
            resolution=self.resolution,
            compiled_sql=model.compiled_code,
        )

        # Build RunResult from federation result
        if result.get("success", False):
            return RunResult(
                node=model,
                status=RunStatus.Success,
                timing=[],
                thread_id=threading.current_thread().name,
                execution_time=result.get("execution_time", 0),
                message=result.get("message", "OK"),
                adapter_response={
                    "federation": True,
                    "method": result.get("load_method"),
                },
                failures=0,
                batch_results=None,
            )
        else:
            return RunResult(
                node=model,
                status=RunStatus.Error,
                timing=[],
                thread_id=threading.current_thread().name,
                execution_time=result.get("execution_time", 0),
                message=result.get("message", "FAILED"),
                adapter_response={"federation": True, "error": result.get("error")},
                failures=1,
                batch_results=None,
            )
