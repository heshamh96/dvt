"""
DVT Run Task — extends dbt's RunTask with cross-engine extraction.

Overrides get_runner_type() to return DvtModelRunner for models that
require cross-engine extraction (Sling Direct or DuckDB Compute paths).

For pushdown models (all sources on target), uses stock dbt ModelRunner.
"""

import logging
from typing import Dict, Optional, Type

from dbt.task.run import ModelRunner, RunTask

from dvt.config.source_connections import load_source_connections
from dvt.config.target_resolver import (
    ExecutionPath,
    ResolvedModel,
    resolve_all_models,
)
from dvt.runners.model_runner import DvtModelRunner
from dvt.sync.profiles_reader import default_profiles_dir, read_profiles_yml

logger = logging.getLogger(__name__)


class DvtRunTask(RunTask):
    """Extends RunTask with DVT three-path execution.

    Before execution:
    1. Reads profiles.yml for output configs
    2. Reads sources.yml for source→connection mappings
    3. Resolves execution paths for all models

    During execution:
    4. Returns DvtModelRunner for extraction models, stock ModelRunner for pushdown
    5. DvtModelRunner handles Sling/DuckDB orchestration transparently
    """

    def __init__(self, args, config, manifest):
        super().__init__(args, config, manifest)
        self._resolutions: Dict[str, ResolvedModel] = {}
        self._raw_profiles: Optional[dict] = None
        self._source_connections: Optional[Dict[str, str]] = None

    def _resolve_models(self) -> None:
        """Resolve execution paths for all models in the manifest."""
        if self._resolutions:
            return  # already resolved

        # Read profiles.yml for output configs
        profiles_dir = (
            getattr(self.args, "PROFILES_DIR", None) or default_profiles_dir()
        )
        try:
            self._raw_profiles = read_profiles_yml(profiles_dir)
        except Exception as e:
            logger.warning(f"DVT: Could not read profiles.yml: {e}")
            self._raw_profiles = {}

        # Read source connections from sources.yml files
        project_dir = getattr(self.args, "PROJECT_DIR", None) or "."
        self._source_connections = load_source_connections(project_dir)

        # Get default target and CLI target
        default_target = self.config.target_name
        cli_target = getattr(self.args, "TARGET", None)

        # Resolve all models
        self._resolutions = resolve_all_models(
            self.manifest,
            default_target,
            self._source_connections,
            cli_target,
        )

        # Log extraction summary
        extraction_count = sum(
            1
            for r in self._resolutions.values()
            if r.execution_path
            in (ExecutionPath.SLING_DIRECT, ExecutionPath.DUCKDB_COMPUTE)
        )
        if extraction_count > 0:
            logger.info(
                f"DVT: {extraction_count} model(s) require cross-engine extraction"
            )

    def get_runner_type(self, node) -> Optional[Type]:
        """Return DvtModelRunner for extraction models, stock runner for pushdown."""
        self._resolve_models()

        uid = getattr(node, "unique_id", None)
        if uid and uid in self._resolutions:
            resolution = self._resolutions[uid]
            if resolution.execution_path in (
                ExecutionPath.SLING_DIRECT,
                ExecutionPath.DUCKDB_COMPUTE,
            ):
                return DvtModelRunner

        # Pushdown — use stock dbt runner selection
        return super().get_runner_type(node)

    def get_runner(self, node):
        """Get runner and inject DVT context for extraction models."""
        runner = super().get_runner(node)

        # If it's a DvtModelRunner, inject the resolution and profiles
        if isinstance(runner, DvtModelRunner):
            uid = getattr(node, "unique_id", None)
            if uid and uid in self._resolutions:
                runner.set_dvt_context(
                    resolution=self._resolutions[uid],
                    profiles=self._raw_profiles or {},
                )

        return runner
