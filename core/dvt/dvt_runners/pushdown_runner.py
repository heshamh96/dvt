"""DVT NonDefaultPushdownRunner — executes models via pushdown on non-default target adapters.

When a model targets a non-default adapter (e.g., databricks when the default profile
is postgres), the standard ModelRunner cannot be used because it's bound to the default
adapter via the global FACTORY singleton.

This runner uses AdapterManager to get the correct target adapter and executes the
compiled SQL directly via CREATE TABLE AS / CREATE VIEW AS.

DVT Rule 1.1: Pushdown is correct when model.target == ALL upstream.targets.
DVT Rule 2.4.1: Compute engines are only for Federation Path — ignored here.
DVT Rule 4.2: Same-target execution uses adapter SQL.
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
from dvt.federation.resolver import ResolvedExecution
from dvt.task import group_lookup
from dvt.task.compile import CompileRunner
from dvt.task.run import track_model_run
from dbt_common.events.base_types import EventLevel
from dbt_common.events.functions import fire_event


class NonDefaultPushdownRunner(CompileRunner):
    """Runner for pushdown execution on non-default target adapters.

    The compiled SQL is already in the target dialect (Jinja resolved during
    the compile phase inherited from CompileRunner). We wrap it in the
    appropriate DDL and execute on the target adapter.
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
        return (
            f"{self.node.language} {self.node.get_materialization()} model "
            f"{self.get_node_representation()} "
            f"[pushdown -> {self.resolution.target}]"
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

    def compile(self, manifest: Manifest):
        """Compile with target-aware adapter.

        DVT: For non-default pushdown, we compile using the target adapter
        so that Jinja resolves {{ ref() }} and {{ source() }} with the
        correct dialect (quoting, relation format).  The compiled SQL is
        then in the target dialect and can be executed directly — no
        post-hoc SQLGlot transpilation needed.
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

    def execute(self, model: ModelNode, manifest: Manifest) -> RunResult:
        """Execute model via pushdown on a non-default target adapter.

        Uses AdapterManager to get the correct adapter for the resolved target,
        then executes the compiled SQL directly via DDL.

        DVT: The compiled SQL is already in the target dialect thanks to
        target-aware compilation — no transpilation needed.
        """
        from dvt.federation.adapter_manager import AdapterManager, get_quoted_table_name

        start_time = time.time()

        compiled_sql = model.compiled_code
        if not compiled_sql:
            raise DbtRuntimeError(f"Model {model.unique_id} has no compiled SQL")

        # Get the correct adapter for the non-default target
        target_adapter = AdapterManager.get_adapter(
            profile_name=self._get_profile_name(),
            target_name=self.resolution.target,
            profiles_dir=self._get_profiles_dir(),
        )

        materialization = model.config.materialized or "table"
        full_refresh = getattr(self.config.args, "FULL_REFRESH", False)

        # Build the quoted table name for the target dialect
        table_name = f"{model.schema}.{model.name}"
        quoted_table = get_quoted_table_name(target_adapter, table_name)

        try:
            with target_adapter.connection_named(f"dvt_pushdown_{model.name}"):
                if materialization == "view":
                    self._execute_view(target_adapter, quoted_table, compiled_sql)
                    message = f"CREATE VIEW {quoted_table}"
                elif materialization == "incremental" and not full_refresh:
                    self._execute_incremental(
                        target_adapter, quoted_table, compiled_sql, model
                    )
                    message = f"INSERT INTO {quoted_table}"
                else:
                    # table materialization (or incremental with --full-refresh)
                    self._execute_table(
                        target_adapter, quoted_table, compiled_sql, full_refresh
                    )
                    message = f"CREATE TABLE {quoted_table}"

            elapsed = time.time() - start_time
            return RunResult(
                node=model,
                status=RunStatus.Success,
                timing=[],
                thread_id=threading.current_thread().name,
                execution_time=elapsed,
                message=message,
                adapter_response={
                    "pushdown": True,
                    "target": self.resolution.target,
                },
                failures=0,
                batch_results=None,
            )

        except Exception as e:
            elapsed = time.time() - start_time
            return RunResult(
                node=model,
                status=RunStatus.Error,
                timing=[],
                thread_id=threading.current_thread().name,
                execution_time=elapsed,
                message=f"Pushdown failed: {e}",
                adapter_response={
                    "pushdown": True,
                    "target": self.resolution.target,
                    "error": str(e),
                },
                failures=1,
                batch_results=None,
            )

    def _execute_table(
        self,
        adapter: Any,
        quoted_table: str,
        compiled_sql: str,
        full_refresh: bool,
    ) -> None:
        """Execute table materialization on target adapter."""
        # Drop existing table/view
        try:
            adapter.execute(f"DROP TABLE IF EXISTS {quoted_table} CASCADE")
        except Exception:
            try:
                adapter.execute(f"DROP TABLE IF EXISTS {quoted_table}")
            except Exception:
                pass
        try:
            adapter.execute(f"DROP VIEW IF EXISTS {quoted_table} CASCADE")
        except Exception:
            try:
                adapter.execute(f"DROP VIEW IF EXISTS {quoted_table}")
            except Exception:
                pass

        # Create table from compiled SQL
        adapter.execute(f"CREATE TABLE {quoted_table} AS {compiled_sql}")

    def _execute_view(
        self,
        adapter: Any,
        quoted_table: str,
        compiled_sql: str,
    ) -> None:
        """Execute view materialization on target adapter."""
        adapter.execute(f"CREATE OR REPLACE VIEW {quoted_table} AS {compiled_sql}")

    def _execute_incremental(
        self,
        adapter: Any,
        quoted_table: str,
        compiled_sql: str,
        model: ModelNode,
    ) -> None:
        """Execute incremental materialization on target adapter."""
        sql_stripped = compiled_sql.strip().upper()
        if sql_stripped.startswith("SELECT") or sql_stripped.startswith("WITH"):
            adapter.execute(f"INSERT INTO {quoted_table} {compiled_sql}")
        else:
            # The SQL is already a DML statement (e.g., MERGE INTO)
            adapter.execute(compiled_sql)
