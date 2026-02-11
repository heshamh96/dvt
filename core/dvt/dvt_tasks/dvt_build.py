"""DVT BuildTask — extends dbt BuildTask with federation runner routing.

Inherits from DvtRunTask (which provides federation routing) and BuildTask
(which provides multi-resource-type runner dispatching).

The MRO ensures that DvtRunTask.get_runner() is called for models, which
handles federation/pushdown routing, while BuildTask.RUNNER_MAP handles
other resource types (seeds, tests, snapshots, etc.).
"""

from __future__ import annotations

from typing import Dict, Iterable, List, Optional, Set, Type

from dvt.adapters.base import BaseRelation
from dvt.artifacts.schemas.results import NodeStatus
from dvt.artifacts.schemas.run import RunResult
from dvt.cli.flags import Flags
from dvt.config.runtime import RuntimeConfig
from dvt.contracts.graph.manifest import Manifest
from dvt.dvt_compilation.dvt_compiler import DvtCompiler
from dvt.dvt_tasks.dvt_run import DvtRunTask
from dvt.exceptions import DbtInternalError
from dvt.graph import Graph, GraphQueue, ResourceTypeSelector
from dvt.node_types import NodeType
from dvt.runners import ExposureRunner as exposure_runner
from dvt.runners import SavedQueryRunner as saved_query_runner
from dvt.task.base import BaseRunner, resource_types_from_args
from dvt.task.build import BuildTask
from dvt.task.run import MicrobatchModelRunner, ModelRunner


# Import runner aliases the same way build.py does
from dvt.task.run import ModelRunner as run_model_runner
from dvt.task.spark_seed import SparkSeedRunner as seed_runner
from dvt.task.snapshot import SnapshotRunner as snapshot_model_runner
from dvt.task.test import TestRunner as test_runner

try:
    from dvt.task.function import FunctionRunner as function_runner
except ImportError:
    function_runner = None  # type: ignore


class DvtBuildTask(DvtRunTask):
    """BuildTask with DVT federation support.

    Combines DvtRunTask's federation routing with BuildTask's multi-resource
    runner dispatch.  For Model nodes, federation routing applies; for other
    resource types, the RUNNER_MAP dispatch is used.
    """

    MARK_DEPENDENT_ERRORS_STATUSES = [
        NodeStatus.Error,
        NodeStatus.Fail,
        NodeStatus.Skipped,
        NodeStatus.PartialSuccess,
    ]

    RUNNER_MAP = {
        NodeType.Model: run_model_runner,
        NodeType.Snapshot: snapshot_model_runner,
        NodeType.Seed: seed_runner,
        NodeType.Test: test_runner,
        NodeType.Unit: test_runner,
        NodeType.SavedQuery: saved_query_runner,
        NodeType.Exposure: exposure_runner,
    }

    # Add FunctionRunner if available
    if function_runner is not None:
        RUNNER_MAP[NodeType.Function] = function_runner

    ALL_RESOURCE_VALUES = frozenset({x for x in RUNNER_MAP.keys()})

    def __init__(self, args: Flags, config: RuntimeConfig, manifest: Manifest) -> None:
        super().__init__(args, config, manifest)
        self.selected_unit_tests: Set = set()
        self.model_to_unit_test_map: Dict[str, List] = {}

    def resource_types(self, no_unit_tests: bool = False) -> List[NodeType]:
        resource_types = resource_types_from_args(
            self.args, set(self.ALL_RESOURCE_VALUES), set(self.ALL_RESOURCE_VALUES)
        )
        if no_unit_tests is True and NodeType.Unit in resource_types:
            resource_types.remove(NodeType.Unit)
        return list(resource_types)

    def get_model_schemas(
        self, adapter, selected_uids: Iterable[str]
    ) -> Set[BaseRelation]:
        model_schemas = super().get_model_schemas(adapter, selected_uids)

        # Get function schemas
        function_schemas: Set[BaseRelation] = set()
        for function in self.manifest.functions.values() if self.manifest else []:
            if function.unique_id in selected_uids:
                relation = adapter.Relation.create_from(self.config, function)
                function_schemas.add(relation.without_identifier())

        return model_schemas.union(function_schemas)

    def get_graph_queue(self) -> GraphQueue:
        spec = self.get_selection_spec()

        full_selector = self.get_node_selector(no_unit_tests=False)
        full_selected_nodes = full_selector.get_selected(
            spec=spec, warn_on_no_nodes=False
        )

        selector_wo_unit_tests = self.get_node_selector(no_unit_tests=True)
        selected_nodes_wo_unit_tests = selector_wo_unit_tests.get_selected(
            spec=spec, warn_on_no_nodes=False
        )

        selected_unit_tests = full_selected_nodes - selected_nodes_wo_unit_tests
        self.selected_unit_tests = selected_unit_tests
        self.build_model_to_unit_test_map(selected_unit_tests)

        return selector_wo_unit_tests.get_graph_queue(spec)

    def handle_job_queue(self, pool, callback):
        if self.run_count == 0:
            self.num_nodes = self.num_nodes + len(self.selected_unit_tests)
        node = self.job_queue.get()
        if (
            node.resource_type == NodeType.Model
            and self.model_to_unit_test_map
            and node.unique_id in self.model_to_unit_test_map
        ):
            self.handle_model_with_unit_tests_node(node, pool, callback)
        else:
            self.handle_job_queue_node(node, pool, callback)

    def handle_model_with_unit_tests_node(self, node, pool, callback):
        self._raise_set_error()
        args = [node, pool]
        if self.config.args.single_threaded:
            callback(self.call_model_and_unit_tests_runner(*args))
        else:
            pool.apply_async(
                self.call_model_and_unit_tests_runner, args=args, callback=callback
            )

    def call_model_and_unit_tests_runner(self, node, pool) -> RunResult:
        assert self.manifest
        for unit_test_unique_id in self.model_to_unit_test_map[node.unique_id]:
            unit_test_node = self.manifest.unit_tests[unit_test_unique_id]
            unit_test_runner = self.get_runner(unit_test_node)
            if node.unique_id in self._skipped_children:
                unit_test_runner.do_skip(cause=None)
            result = self.call_runner(unit_test_runner)
            self._handle_result(result)
            if result.status in self.MARK_DEPENDENT_ERRORS_STATUSES:
                self._skipped_children[node.unique_id] = None
        runner = self.get_runner(node)
        if runner.node.unique_id in self._skipped_children:
            cause = self._skipped_children.pop(runner.node.unique_id)
            runner.do_skip(cause=cause)

        if isinstance(runner, MicrobatchModelRunner):
            runner.set_parent_task(self)
            runner.set_pool(pool)

        return self.call_runner(runner)

    def handle_job_queue_node(self, node, pool, callback):
        self._raise_set_error()
        runner = self.get_runner(node)
        if runner.node.unique_id in self._skipped_children:
            cause = self._skipped_children.pop(runner.node.unique_id)
            runner.do_skip(cause=cause)

        if isinstance(runner, MicrobatchModelRunner):
            runner.set_parent_task(self)
            runner.set_pool(pool)

        args = [runner]
        self._submit(pool, args, callback)

    def build_model_to_unit_test_map(self, selected_unit_tests):
        dct = {}
        for unit_test_unique_id in selected_unit_tests:
            unit_test = self.manifest.unit_tests[unit_test_unique_id]
            model_unique_id = unit_test.depends_on.nodes[0]
            if model_unique_id not in dct:
                dct[model_unique_id] = []
            dct[model_unique_id].append(unit_test.unique_id)
        self.model_to_unit_test_map = dct

    def get_node_selector(self, no_unit_tests=False) -> ResourceTypeSelector:
        if self.manifest is None or self.graph is None:
            raise DbtInternalError(
                "manifest and graph must be set to get node selection"
            )

        resource_types = self.resource_types(no_unit_tests)

        return ResourceTypeSelector(
            graph=self.graph,
            manifest=self.manifest,
            previous_state=self.previous_state,
            resource_types=resource_types,
        )

    def get_runner_type(self, node) -> Optional[Type[BaseRunner]]:
        # For Model nodes, check if microbatch first
        if (
            node.resource_type == NodeType.Model
            and super().get_runner_type(node) == MicrobatchModelRunner
        ):
            return MicrobatchModelRunner

        # For Model nodes, check the parent (DvtRunTask) routing
        if node.resource_type == NodeType.Model:
            return super().get_runner_type(node)

        # For non-Model nodes, use RUNNER_MAP
        return self.RUNNER_MAP.get(node.resource_type)

    def get_runner(self, node) -> BaseRunner:
        """Route runner selection.

        For Model nodes, delegate to DvtRunTask.get_runner() which handles
        federation/pushdown routing.  For non-Model nodes, use RUNNER_MAP.
        """
        from dvt.adapters.factory import get_adapter

        if node.resource_type == NodeType.Model:
            # DvtRunTask handles federation routing for models
            return super().get_runner(node)

        # For non-model nodes, use the standard RUNNER_MAP dispatch
        adapter = get_adapter(self.config)
        runner_type = self.RUNNER_MAP.get(node.resource_type)
        if runner_type is None:
            raise DbtInternalError(
                f"No runner type found for resource type: {node.resource_type}"
            )
        return runner_type(self.config, adapter, node, self.run_count, self.num_nodes)

    def compile_manifest(self) -> None:
        if self.manifest is None:
            raise DbtInternalError("compile_manifest called before manifest was loaded")
        self.graph: Graph = self.compiler.compile(self.manifest, add_test_edges=True)
