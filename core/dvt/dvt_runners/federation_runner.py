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
from typing import Optional

from dvt.adapters.base import BaseAdapter
from dvt.artifacts.schemas.results import RunStatus
from dvt.artifacts.schemas.run import RunResult
from dvt.config import RuntimeConfig
from dvt.contracts.graph.manifest import Manifest
from dvt.contracts.graph.nodes import ModelNode
from dvt.dvt_runners.runner_mixin import DvtRunnerMixin
from dvt.federation.engine import FederationEngine
from dvt.federation.resolver import ResolvedExecution
from dvt.task import group_lookup
from dvt.task.compile import CompileRunner
from dbt_common.events.base_types import EventLevel
from dbt_common.events.functions import fire_event


class FederationModelRunner(DvtRunnerMixin, CompileRunner):
    """Runner for models that require cross-target federation."""

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
        self._init_dvt_runner(resolution, manifest)
        self._federation_engine: Optional[FederationEngine] = None

    def describe_node(self) -> str:
        upstream_targets = ", ".join(sorted(self.resolution.upstream_targets))
        return (
            f"{self.node.language} federation model {self.get_node_representation()} "
            f"[{upstream_targets}] -> {self.resolution.target}"
        )

    def compile(self, manifest: Manifest):
        """Compile with target-aware adapter.

        For federation models targeting a non-default adapter, compile using
        the target adapter so Jinja resolves refs/sources with the correct
        dialect.  If same as default, fall back to standard compilation.
        """
        if self.resolution.target == self.config.target_name:
            return super().compile(manifest)
        return self._compile_with_target_adapter(manifest)

    def execute(self, model: ModelNode, manifest: Manifest) -> RunResult:
        """Execute model via federation engine."""
        from dvt.events.types import LogModelResult
        from dbt_common.events.types import Formatting

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

        def on_progress(msg: str) -> None:
            fire_event(Formatting(msg=f"  {msg}"))

        engine = FederationEngine(
            runtime_config=self.config,
            manifest=manifest,
            on_progress=on_progress,
        )

        result = engine.execute(
            model=model,
            resolution=self.resolution,
            compiled_sql=model.compiled_code,
        )

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
