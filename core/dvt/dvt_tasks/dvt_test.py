"""DVT TestTask -- extends dbt TestTask with target-aware test execution.

Data tests in DVT should execute against the adapter where their parent
model lives.  When a model has config(target='mssql_docker'), its tests
(schema tests and singular tests) should run on the MSSQL adapter, not
on the default adapter.

This task:
1. Resolves each test's target from its parent model's config.target
2. Executes tests on the correct adapter via AdapterManager
3. Falls through to standard TestRunner for default-target tests

Unit tests always run on the default adapter (they don't target real tables).
"""

from __future__ import annotations

import logging
from typing import Optional, Type

from dvt.contracts.graph.manifest import Manifest
from dvt.contracts.graph.nodes import GenericTestNode, SingularTestNode, TestNode
from dvt.task.base import BaseRunner
from dvt.task.test import TestRunner, TestTask, TestResultData

logger = logging.getLogger(__name__)


class DvtTestRunner(TestRunner):
    """TestRunner with target-aware execution for non-default adapter tests.

    For data tests whose parent model has config(target=X) where X differs
    from the default, this runner:
    1. Gets the target adapter via AdapterManager
    2. Fixes the test node's schema/database to match the target
    3. Executes the test SQL on the target adapter
    """

    def execute_data_test(
        self, data_test: TestNode, manifest: Manifest
    ) -> TestResultData:
        """Execute data test, routing to the correct target adapter."""
        target_name = self._resolve_test_target(data_test, manifest)

        if not target_name or target_name == self.config.target_name:
            # Default target — standard execution path
            return super().execute_data_test(data_test, manifest)

        # Non-default target — execute on the target adapter
        return self._execute_on_target_adapter(data_test, manifest, target_name)

    def _resolve_test_target(
        self, test_node: TestNode, manifest: Manifest
    ) -> Optional[str]:
        """Determine which target a test should execute on.

        Walks the test's depends_on.nodes to find a parent model with
        config.target set.  If multiple parents have different targets,
        uses the first one found (tests typically depend on one model).
        """
        if not hasattr(test_node, "depends_on") or not test_node.depends_on:
            return None

        for dep_uid in test_node.depends_on.nodes:
            dep_node = manifest.nodes.get(dep_uid)
            if dep_node is None:
                continue
            node_target = getattr(getattr(dep_node, "config", None), "target", None)
            if node_target:
                return node_target

        return None

    def _execute_on_target_adapter(
        self,
        data_test: TestNode,
        manifest: Manifest,
        target_name: str,
    ) -> TestResultData:
        """Execute a data test on a non-default target adapter."""
        from dvt.federation.adapter_manager import AdapterManager
        from dvt.clients.jinja import MacroGenerator
        from dvt.context.providers import generate_runtime_model_context
        from dvt.adapters.exceptions import MissingMaterializationError
        from dvt.exceptions import DbtInternalError
        from dvt.artifacts.schemas.catalog import PrimitiveDict
        from dvt.utils import _coerce_decimal

        profiles_dir = getattr(self.config.args, "PROFILES_DIR", None)
        if not profiles_dir:
            profiles_dir = getattr(self.config.args, "profiles_dir", None)

        try:
            target_adapter = AdapterManager.get_adapter(
                profile_name=self.config.profile_name,
                target_name=target_name,
                profiles_dir=str(profiles_dir) if profiles_dir else None,
            )
        except Exception as e:
            logger.warning(
                "Could not get adapter for target '%s': %s. "
                "Falling back to default adapter for test %s.",
                target_name,
                str(e),
                data_test.unique_id,
            )
            return super().execute_data_test(data_test, manifest)

        # Fix test node's schema/database to match the target adapter
        self._fix_test_schema(data_test, target_adapter)

        # Set macro resolver on target adapter so materialization macros work
        target_adapter.set_macro_resolver(manifest)

        # Generate context using the target adapter
        context = generate_runtime_model_context(
            data_test, self.config, manifest, adapter=target_adapter
        )

        hook_ctx = target_adapter.pre_model_hook(context["config"])

        materialization_macro = manifest.find_materialization_macro_by_name(
            self.config.project_name,
            data_test.get_materialization(),
            target_adapter.type(),
        )

        if materialization_macro is None:
            raise MissingMaterializationError(
                materialization=data_test.get_materialization(),
                adapter_type=target_adapter.type(),
            )

        if "config" not in context:
            raise DbtInternalError(
                "Invalid materialization context generated, missing config: {}".format(
                    context
                )
            )

        macro_func = MacroGenerator(materialization_macro, context)
        try:
            with target_adapter.connection_named(f"dvt_test_{data_test.name}"):
                macro_func()
        finally:
            target_adapter.post_model_hook(context, hook_ctx)

        result = context["load_result"]("main")
        table = result["table"]
        num_rows = len(table.rows)
        if num_rows != 1:
            raise DbtInternalError(
                f"dvt internally failed to execute {data_test.unique_id}: "
                f"Returned {num_rows} rows, but expected 1 row"
            )
        num_cols = len(table.columns)
        if num_cols != 3:
            raise DbtInternalError(
                f"dvt internally failed to execute {data_test.unique_id}: "
                f"Returned {num_cols} columns, but expected 3 columns"
            )

        test_result_dct: PrimitiveDict = dict(
            zip(
                [column_name.lower() for column_name in table.column_names],
                map(_coerce_decimal, table.rows[0]),
            )
        )
        test_result_dct["adapter_response"] = result["response"].to_dict(omit_none=True)
        TestResultData.validate(test_result_dct)
        return TestResultData.from_dict(test_result_dct)

    def _fix_test_schema(self, test_node: TestNode, target_adapter) -> None:
        """Fix test node's schema/database to match the target adapter.

        At parse time, test nodes inherit schema from the default target.
        When executing on a non-default adapter, we need the correct schema.
        """
        creds = target_adapter.config.credentials
        target_schema = getattr(creds, "schema", None)
        target_database = getattr(creds, "database", None)

        if target_schema:
            test_node.schema = target_schema
        if target_database is not None:
            test_node.database = target_database


class DvtTestTask(TestTask):
    """TestTask with DVT target-aware test execution.

    Uses DvtTestRunner to route data tests to the correct adapter
    based on their parent model's config.target.
    """

    __test__ = False

    def get_runner_type(self, _) -> Optional[Type[BaseRunner]]:
        return DvtTestRunner
