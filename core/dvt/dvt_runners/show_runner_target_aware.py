"""DVT ShowRunnerTargetAware — previews models materialized on non-default adapters.

For models that target a non-default adapter (pushdown or federation), the standard
ShowRunner would try to execute the compiled SQL on the default adapter, which fails
for cross-database references.

This runner instead queries the already-materialized table on the correct target
adapter.  If the model hasn't been materialized yet, the database will raise an
error (the table won't exist).
"""

from __future__ import annotations

import threading
import time
from typing import Any

from dvt.artifacts.schemas.results import RunStatus
from dvt.artifacts.schemas.run import RunResult
from dvt.federation.resolver import ResolvedExecution
from dvt.task.compile import CompileRunner


class ShowRunnerTargetAware(CompileRunner):
    """Runner for previewing models that target a non-default adapter."""

    def __init__(
        self, config, adapter, node, node_index, num_nodes, target_adapter, resolution
    ) -> None:
        super().__init__(config, adapter, node, node_index, num_nodes)
        self.run_ephemeral_models = True
        self.target_adapter = target_adapter
        self.resolution: ResolvedExecution = resolution

    def execute(self, compiled_node, manifest):
        start_time = time.time()

        limit = None if self.config.args.limit < 0 else self.config.args.limit

        # Build a SELECT from the materialized table on the target adapter.
        from dvt.federation.adapter_manager import get_quoted_table_name

        table_name = f"{compiled_node.schema}.{compiled_node.name}"
        quoted_table = get_quoted_table_name(self.target_adapter, table_name)

        show_sql = f"SELECT * FROM {quoted_table}"
        if limit is not None:
            show_sql += f" LIMIT {limit}"

        # Execute on the target adapter
        conn_name = f"dvt_show_{self.resolution.target}"
        with self.target_adapter.connection_named(conn_name):
            adapter_response, execute_result = self.target_adapter.execute(
                show_sql, fetch=True
            )

        end_time = time.time()

        return RunResult(
            node=compiled_node,
            status=RunStatus.Success,
            timing=[],
            thread_id=threading.current_thread().name,
            execution_time=end_time - start_time,
            message=None,
            adapter_response=adapter_response.to_dict(),
            agate_table=execute_result,
            failures=None,
            batch_results=None,
        )
