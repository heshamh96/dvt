import io
import threading
import time

from dvt.adapters.factory import get_adapter
from dvt.artifacts.schemas.run import RunResult, RunStatus
from dvt.context.providers import generate_runtime_model_context
from dvt.contracts.graph.nodes import SeedNode
from dvt.events.types import ShowNode
from dvt.flags import get_flags
from dvt.task.base import ConfiguredTask
from dvt.task.compile import CompileRunner, CompileTask
from dvt.task.seed import SeedRunner
from dbt_common.events.base_types import EventLevel
from dbt_common.events.functions import fire_event
from dbt_common.events.types import Note
from dbt_common.exceptions import DbtRuntimeError


class ShowRunner(CompileRunner):
    def __init__(self, config, adapter, node, node_index, num_nodes) -> None:
        super().__init__(config, adapter, node, node_index, num_nodes)
        self.run_ephemeral_models = True

    def execute(self, compiled_node, manifest):
        start_time = time.time()

        # Allow passing in -1 (or any negative number) to get all rows
        limit = None if self.config.args.limit < 0 else self.config.args.limit

        model_context = generate_runtime_model_context(
            compiled_node, self.config, manifest
        )

        # Wrap compiled SQL in a subquery to avoid LIMIT clause conflicts.
        # The get_show_sql macro appends its own LIMIT, which produces invalid
        # SQL if the model already contains a LIMIT clause (e.g., "... LIMIT 100\nlimit 10").
        # Wrapping in a subquery makes the outer LIMIT apply cleanly.
        compiled_code = model_context["compiled_code"]
        compiled_code = f"SELECT * FROM ({compiled_code}) AS _dvt_show_subq"

        compiled_node.compiled_code = self.adapter.execute_macro(
            macro_name="get_show_sql",
            macro_resolver=manifest,
            context_override=model_context,
            kwargs={
                "compiled_code": compiled_code,
                "sql_header": model_context["config"].get("sql_header"),
                "limit": limit,
            },
        )
        adapter_response, execute_result = self.adapter.execute(
            compiled_node.compiled_code, fetch=True
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


class ShowRunnerTargetAware(CompileRunner):
    """Runner for previewing models that target a non-default adapter.

    DVT: For models that target a non-default adapter (pushdown or federation),
    the standard ShowRunner would try to execute the compiled SQL on the default
    adapter, which fails for cross-database references.

    This runner instead queries the already-materialized table on the correct
    target adapter.  If the model hasn't been materialized yet, it tells the
    user to run `dvt run` first.
    """

    def __init__(
        self, config, adapter, node, node_index, num_nodes, target_adapter, resolution
    ) -> None:
        super().__init__(config, adapter, node, node_index, num_nodes)
        self.run_ephemeral_models = True
        self.target_adapter = target_adapter
        self.resolution = resolution

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


class ShowTask(CompileTask):
    def __init__(self, args, config, manifest=None):
        super().__init__(args, config, manifest)
        self._resolved_executions = {}

    def _runtime_initialize(self):
        if not (self.args.select or getattr(self.args, "inline", None)):
            raise DbtRuntimeError("Either --select or --inline must be passed to show")
        super()._runtime_initialize()

        # DVT: Resolve federation paths for selected models so we can
        # route non-default target models to ShowRunnerTargetAware.
        # Only needed for non-inline queries (inline runs on default adapter).
        if self.args.select and self.manifest and self._flattened_nodes:
            try:
                from dvt.federation.resolver import FederationResolver

                resolver = FederationResolver(
                    manifest=self.manifest,
                    runtime_config=self.config,
                    args=self.args,
                )
                selected_uids = [n.unique_id for n in self._flattened_nodes]
                self._resolved_executions = resolver.resolve_all(selected_uids)
            except Exception:
                # If federation resolution fails, fall back to default behavior
                self._resolved_executions = {}

    def get_runner(self, node):
        """Route to the correct runner based on federation resolution.

        DVT: For models targeting a non-default adapter (pushdown or
        federation), use ShowRunnerTargetAware which queries the
        materialized table on the correct target adapter.
        """
        from dvt.federation.resolver import ExecutionPath

        adapter = get_adapter(self.config)
        run_count = 0
        num_nodes = 0

        if node.is_ephemeral_model:
            run_count = 0
            num_nodes = 0
        else:
            self.run_count += 1
            run_count = self.run_count
            num_nodes = self.num_nodes

        # Check if this is a seed
        if isinstance(node, SeedNode):
            return SeedRunner(self.config, adapter, node, run_count, num_nodes)

        # Check federation resolution
        resolution = self._resolved_executions.get(node.unique_id)
        if resolution:
            from dvt.federation.resolver import ExecutionPath

            is_federation = resolution.execution_path == ExecutionPath.SPARK_FEDERATION
            is_non_default = resolution.target != self.config.target_name

            if is_federation or is_non_default:
                # Federation or non-default target — query the materialized
                # table on the correct target adapter instead of re-compiling.
                try:
                    if is_non_default:
                        from dvt.federation.adapter_manager import AdapterManager

                        profiles_dir = getattr(self.config.args, "PROFILES_DIR", None)
                        if not profiles_dir:
                            profiles_dir = getattr(
                                self.config.args, "profiles_dir", None
                            )
                        profiles_dir = str(profiles_dir) if profiles_dir else None

                        profile_name = (
                            self.config.profile_name
                            if hasattr(self.config, "profile_name")
                            else "default"
                        )

                        target_adapter = AdapterManager.get_adapter(
                            profile_name=profile_name,
                            target_name=resolution.target,
                            profiles_dir=profiles_dir,
                        )
                    else:
                        # Federation to default target — materialized table
                        # is on the default adapter
                        target_adapter = adapter

                    return ShowRunnerTargetAware(
                        self.config,
                        adapter,
                        node,
                        run_count,
                        num_nodes,
                        target_adapter,
                        resolution,
                    )
                except Exception:
                    # If adapter creation fails, fall back to default ShowRunner
                    pass

        # Default: same-target pushdown — standard ShowRunner works
        return ShowRunner(self.config, adapter, node, run_count, num_nodes)

    def get_runner_type(self, node):
        if isinstance(node, SeedNode):
            return SeedRunner
        else:
            return ShowRunner

    def task_end_messages(self, results) -> None:
        is_inline = bool(getattr(self.args, "inline", None))

        if is_inline:
            matched_results = [
                result for result in results if result.node.name == "inline_query"
            ]
        else:
            matched_results = []
            for result in results:
                if result.node.name in self.selection_arg[0]:
                    matched_results.append(result)
                else:
                    fire_event(
                        Note(msg=f"Excluded node '{result.node.name}' from results"),
                        EventLevel.DEBUG,
                    )

        for result in matched_results:
            table = result.agate_table

            # Hack to get Agate table output as string
            output = io.StringIO()
            if self.args.output == "json":
                table.to_json(path=output)
            else:
                table.print_table(output=output, max_rows=None)

            node_name = result.node.name

            if hasattr(result.node, "version") and result.node.version:
                node_name += f".v{result.node.version}"

            fire_event(
                ShowNode(
                    node_name=node_name,
                    preview=output.getvalue(),
                    is_inline=is_inline,
                    output_format=self.args.output,
                    unique_id=result.node.unique_id,
                    quiet=get_flags().QUIET,
                )
            )

    def _handle_result(self, result) -> None:
        super()._handle_result(result)

        if (
            result.node.is_ephemeral_model
            and type(self) is ShowTask
            and (self.args.select or getattr(self.args, "inline", None))
        ):
            self.node_results.append(result)


class ShowTaskDirect(ConfiguredTask):
    def run(self):
        adapter = get_adapter(self.config)
        with adapter.connection_named("show", should_release_connection=False):
            limit = None if self.args.limit < 0 else self.args.limit
            response, table = adapter.execute(
                self.args.inline_direct, fetch=True, limit=limit
            )

            output = io.StringIO()
            if self.args.output == "json":
                table.to_json(path=output)
            else:
                table.print_table(output=output, max_rows=None)

            fire_event(
                ShowNode(
                    node_name="direct-query",
                    preview=output.getvalue(),
                    is_inline=True,
                    output_format=self.args.output,
                    unique_id="direct-query",
                    quiet=get_flags().QUIET,
                )
            )
