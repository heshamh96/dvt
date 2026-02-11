"""DVT NonDefaultPushdownRunner — executes models via pushdown on non-default target adapters.

When a model targets a non-default adapter (e.g., databricks when the default profile
is postgres), the standard ModelRunner cannot be used because it's bound to the default
adapter via the global FACTORY singleton.

This runner uses AdapterManager to get the correct target adapter and executes the
compiled SQL directly via CREATE TABLE AS / CREATE VIEW AS.
"""

from __future__ import annotations

import threading
import time
from typing import Any

from dvt.adapters.base import BaseAdapter
from dvt.artifacts.schemas.results import RunStatus
from dvt.artifacts.schemas.run import RunResult
from dvt.config import RuntimeConfig
from dvt.contracts.graph.manifest import Manifest
from dvt.contracts.graph.nodes import ModelNode
from dvt.dvt_runners.runner_mixin import DvtRunnerMixin
from dvt.exceptions import DbtRuntimeError
from dvt.federation.resolver import ResolvedExecution
from dvt.task.compile import CompileRunner


class NonDefaultPushdownRunner(DvtRunnerMixin, CompileRunner):
    """Runner for pushdown execution on non-default target adapters.

    The compiled SQL is already in the target dialect (Jinja resolved during
    the compile phase). We wrap it in the appropriate DDL and execute on
    the target adapter.
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
        self._init_dvt_runner(resolution, manifest)

    def describe_node(self) -> str:
        return (
            f"{self.node.language} {self.node.get_materialization()} model "
            f"{self.get_node_representation()} "
            f"[pushdown -> {self.resolution.target}]"
        )

    def compile(self, manifest: Manifest):
        """Compile with target-aware adapter for correct dialect."""
        return self._compile_with_target_adapter(manifest)

    def execute(self, model: ModelNode, manifest: Manifest) -> RunResult:
        """Execute model via pushdown on a non-default target adapter."""
        from dvt.federation.adapter_manager import AdapterManager, get_quoted_table_name

        start_time = time.time()

        compiled_sql = model.compiled_code
        if not compiled_sql:
            raise DbtRuntimeError(f"Model {model.unique_id} has no compiled SQL")

        target_adapter = AdapterManager.get_adapter(
            profile_name=self._get_profile_name(),
            target_name=self.resolution.target,
            profiles_dir=self._get_profiles_dir(),
        )

        materialization = model.config.materialized or "table"
        full_refresh = getattr(self.config.args, "FULL_REFRESH", False)

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

    # -- DDL helpers --

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
            adapter.execute(compiled_sql)
