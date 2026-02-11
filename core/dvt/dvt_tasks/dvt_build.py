"""DVT BuildTask — extends dbt BuildTask with federation runner routing.

Inherits from both BuildTask (multi-resource dispatch) and DvtRunTask
(federation routing).  For Model nodes, DvtRunTask.get_runner() handles
federation/pushdown routing.  For other resource types, BuildTask's
RUNNER_MAP is used, with SparkSeedRunner replacing the dbt SeedRunner.
"""

from __future__ import annotations

from typing import Optional, Type

from dvt.dvt_compilation.dvt_compiler import DvtCompiler
from dvt.dvt_tasks.dvt_run import DvtRunTask
from dvt.node_types import NodeType
from dvt.task.base import BaseRunner
from dvt.task.build import BuildTask
from dvt.task.spark_seed import SparkSeedRunner


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

        # Override seed runner to use Spark-based seeding
        self.RUNNER_MAP = dict(self.RUNNER_MAP)
        self.RUNNER_MAP[NodeType.Seed] = SparkSeedRunner

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
