"""DVT BuildTask — extends dbt BuildTask with federation runner routing.

Inherits from both BuildTask (multi-resource dispatch) and DvtRunTask
(federation routing).  For Model nodes, DvtRunTask.get_runner() handles
federation/pushdown routing.  For other resource types, BuildTask's
RUNNER_MAP is used.

Seed handling:
- Default: uses standard SeedRunner (adapter-based, same as dbt)
- With --compute: uses SparkSeedRunner (Spark JDBC bulk loading)
"""

from __future__ import annotations

from typing import AbstractSet, Optional, Type

from dvt.adapters.base import BaseAdapter
from dvt.dvt_compilation.dvt_compiler import DvtCompiler
from dvt.dvt_tasks.dvt_run import DvtRunTask
from dvt.node_types import NodeType
from dvt.task.base import BaseRunner
from dvt.task.build import BuildTask


class DvtBuildTask(DvtRunTask, BuildTask):
    """BuildTask with DVT federation support.

    MRO: DvtBuildTask -> DvtRunTask -> BuildTask -> RunTask -> ...
    - DvtRunTask provides: federation before_run/after_run, DvtCompiler, get_runner routing
    - BuildTask provides: RUNNER_MAP, unit test handling, compile_manifest with test edges
    """

    def __init__(self, args, config, manifest) -> None:
        # Call BuildTask.__init__ directly (skips DvtRunTask.__init__ which
        # passes batch_map to super() — BuildTask doesn't accept that param).
        BuildTask.__init__(self, args, config, manifest)

        # Apply DvtRunTask's federation state and compiler setup
        self._resolved_executions: dict = {}
        self._spark_initialized: bool = False
        self._federation_count: int = 0
        self.compiler = DvtCompiler(self.config)

        # Seed handling: default to adapter-based SeedRunner (same as dbt).
        # --spark (boolean) → use default compute; --compute <name> → named compute.
        # Neither → standard adapter-based seeding.
        use_spark = getattr(self.args, "SPARK", False) or getattr(
            self.args, "COMPUTE", None
        )
        self.RUNNER_MAP = dict(self.RUNNER_MAP)
        if use_spark:
            from dvt.dvt_tasks.lib.spark_seed import SparkSeedRunner

            self.RUNNER_MAP[NodeType.Seed] = SparkSeedRunner

        # Snapshot handling: use DvtSnapshotRunner for target-aware schema resolution
        from dvt.dvt_tasks.dvt_snapshot import DvtSnapshotRunner

        self.RUNNER_MAP[NodeType.Snapshot] = DvtSnapshotRunner

        # Test handling: use DvtTestRunner for target-aware test execution
        from dvt.dvt_tasks.dvt_test import DvtTestRunner

        self.RUNNER_MAP[NodeType.Test] = DvtTestRunner

    def before_run(
        self, adapter: BaseAdapter, selected_uids: AbstractSet[str]
    ) -> "RunStatus":
        """Extend DvtRunTask.before_run to also init Spark for seeds when --compute is set.

        DvtRunTask.before_run only initializes Spark when federation models
        are present.  When --compute is specified and the selection includes
        seed nodes, ensure Spark is ready before execution starts.
        Without --compute, seeds use the standard adapter path (no Spark needed).
        """
        # Let DvtRunTask resolve federation paths and conditionally init Spark
        result = DvtRunTask.before_run(self, adapter, selected_uids)

        # Only init Spark for seeds when --spark or --compute is specified
        use_spark = getattr(self.args, "SPARK", False) or getattr(
            self.args, "COMPUTE", None
        )
        if use_spark and not self._spark_initialized and self.manifest:
            has_seeds = any(
                self.manifest.nodes[uid].resource_type == NodeType.Seed
                for uid in selected_uids
                if uid in self.manifest.nodes
            )
            if has_seeds:
                self._initialize_spark_for_federation()

        return result

    def get_runner_type(self, node) -> Optional[Type[BaseRunner]]:
        # For Model nodes, use DvtRunTask routing (federation/pushdown/microbatch)
        if node.resource_type == NodeType.Model:
            return DvtRunTask.get_runner_type(self, node)
        # For non-Model nodes, use BuildTask's RUNNER_MAP
        return BuildTask.get_runner_type(self, node)

    def get_runner(self, node) -> BaseRunner:
        """For Model nodes, delegate to DvtRunTask (federation routing).
        For non-Model nodes, use BuildTask's standard dispatch."""
        if node.resource_type == NodeType.Model:
            return DvtRunTask.get_runner(self, node)
        return BuildTask.get_runner(self, node)
