"""DVT RetryTask — extends dbt RetryTask with DVT-specific flag handling.

Adds DROP_FLAGS to strip DVT root-group-only flags (like show_resource_report)
from both previous_args and current_args before replaying through Flags.from_dict().
"""

from __future__ import annotations

from pathlib import Path

from click import get_current_context
from click.core import ParameterSource

from dvt.artifacts.schemas.results import NodeStatus
from dvt.cli.flags import Flags
from dvt.cli.types import Command as CliCommand
from dvt.config import RuntimeConfig
from dvt.constants import RUN_RESULTS_FILE_NAME
from dvt.contracts.state import load_result_state
from dvt.flags import get_flags, set_flags
from dvt.graph import GraphQueue
from dvt.parser.manifest import parse_manifest
from dvt.task.base import ConfiguredTask
from dbt_common.exceptions import DbtRuntimeError

RETRYABLE_STATUSES = {
    NodeStatus.Error,
    NodeStatus.Fail,
    NodeStatus.Skipped,
    NodeStatus.RuntimeErr,
    NodeStatus.PartialSuccess,
}

IGNORE_PARENT_FLAGS = {
    "log_path",
    "output_path",
    "profiles_dir",
    "profiles_dir_exists_false",
    "project_dir",
    "defer_state",
    "deprecated_state",
    "target_path",
    "warn_error",
}

# DVT: Flags that live on the root cli group only, not on subcommands.
# These must be stripped from BOTH previous_args AND current_args before
# replaying through Flags.from_dict(), otherwise Click raises NoSuchOption.
DROP_FLAGS = {
    "show_resource_report",
}

ALLOW_CLI_OVERRIDE_FLAGS = {"vars", "threads"}


# Import task classes — use DVT subclasses where they exist
def _get_task_dict():
    """Lazy import to avoid circular imports."""
    from dvt.dvt_tasks.dvt_build import DvtBuildTask
    from dvt.task.compile import CompileTask
    from dvt.task.clone import CloneTask
    from dvt.task.docs.generate import GenerateTask
    from dvt.dvt_tasks.dvt_seed import DvtSeedTask
    from dvt.task.snapshot import SnapshotTask
    from dvt.task.test import TestTask
    from dvt.dvt_tasks.dvt_run import DvtRunTask
    from dvt.task.run_operation import RunOperationTask

    return {
        "build": DvtBuildTask,
        "compile": CompileTask,
        "clone": CloneTask,
        "generate": GenerateTask,
        "seed": DvtSeedTask,
        "snapshot": SnapshotTask,
        "test": TestTask,
        "run": DvtRunTask,
        "run-operation": RunOperationTask,
    }


CMD_DICT = {
    "build": CliCommand.BUILD,
    "compile": CliCommand.COMPILE,
    "clone": CliCommand.CLONE,
    "generate": CliCommand.DOCS_GENERATE,
    "seed": CliCommand.SEED,
    "snapshot": CliCommand.SNAPSHOT,
    "test": CliCommand.TEST,
    "run": CliCommand.RUN,
    "run-operation": CliCommand.RUN_OPERATION,
}


class DvtRetryTask(ConfiguredTask):
    """Retry task with DVT-specific flag handling."""

    def __init__(self, args: Flags, config: RuntimeConfig) -> None:
        # load previous run results
        state_path = args.state or config.target_path
        self.previous_results = load_result_state(
            Path(config.project_root) / Path(state_path) / RUN_RESULTS_FILE_NAME
        )
        if not self.previous_results:
            raise DbtRuntimeError(
                f"Could not find previous run in '{state_path}' target directory"
            )
        self.previous_args = self.previous_results.args
        self.previous_command_name = self.previous_args.get("which")

        # Resolve flags and config
        if args.warn_error:
            RETRYABLE_STATUSES.add(NodeStatus.Warn)

        cli_command = CMD_DICT.get(self.previous_command_name)
        args_to_remove = {
            "show": lambda x: True,
            "resource_types": lambda x: x == [],
            "warn_error_options": lambda x: (
                x == {"warn": [], "error": [], "silence": []}
            ),
        }
        for k, v in args_to_remove.items():
            if k in self.previous_args and v(self.previous_args[k]):
                del self.previous_args[k]
        previous_args = {
            k: v
            for k, v in self.previous_args.items()
            if k not in IGNORE_PARENT_FLAGS and k not in DROP_FLAGS
        }
        click_context = get_current_context()
        current_args = {
            k: v
            for k, v in args.__dict__.items()
            if (
                k in IGNORE_PARENT_FLAGS
                or (
                    click_context.get_parameter_source(k) == ParameterSource.COMMANDLINE
                    and k in ALLOW_CLI_OVERRIDE_FLAGS
                )
            )
            and k not in DROP_FLAGS
        }
        combined_args = {**previous_args, **current_args}
        retry_flags = Flags.from_dict(cli_command, combined_args)
        set_flags(retry_flags)
        retry_config = RuntimeConfig.from_args(args=retry_flags)

        # Parse manifest using resolved config/flags
        manifest = parse_manifest(retry_config, False, True, retry_flags.write_json, [])
        super().__init__(args, retry_config, manifest)

        task_dict = _get_task_dict()
        self.task_class = task_dict.get(self.previous_command_name)

    def run(self):
        from dvt.dvt_tasks.dvt_run import DvtRunTask

        unique_ids = {
            result.unique_id
            for result in self.previous_results.results
            if result.status in RETRYABLE_STATUSES
            and not (
                self.previous_command_name != "run-operation"
                and result.unique_id.startswith("operation.")
            )
        }

        batch_map = {
            result.unique_id: result.batch_results
            for result in self.previous_results.results
            if result.batch_results is not None
            and len(result.batch_results.successful) != 0
            and len(result.batch_results.failed) > 0
            and not (
                self.previous_command_name != "run-operation"
                and result.unique_id.startswith("operation.")
            )
        }

        if not unique_ids and not hasattr(self.task_class, "get_graph_queue"):
            return self.previous_results

        class TaskWrapper(self.task_class):
            def get_graph_queue(self):
                new_graph = self.graph.get_subset_graph(unique_ids)
                return GraphQueue(
                    new_graph.graph,
                    self.manifest,
                    unique_ids,
                )

        task = TaskWrapper(
            get_flags(),
            self.config,
            self.manifest,
        )

        if self.task_class == DvtRunTask:
            task.batch_map = batch_map

        return_value = task.run()
        return return_value

    def interpret_results(self, *args, **kwargs):
        return self.task_class.interpret_results(*args, **kwargs)
