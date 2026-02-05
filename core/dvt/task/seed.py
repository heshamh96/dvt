import sys
from typing import Any, Dict, Optional, Type

from dvt.artifacts.schemas.run import RunExecutionResult
from dvt.exceptions import PySparkNotInstalledError
from dvt.graph import ResourceTypeSelector
from dvt.node_types import NodeType
from dvt.task.base import BaseRunner
from dvt.task.printer import print_run_end_messages
from dvt.task.run import RunTask
from dbt_common.exceptions import DbtInternalError
from dbt_common.ui import green


def _load_compute_config(compute_name: Optional[str], profiles_dir: Optional[str] = None) -> Dict[str, Any]:
    """Load compute configuration from computes.yml.

    Args:
        compute_name: Name of the compute to load, or None for default
        profiles_dir: Optional profiles directory override

    Returns:
        Compute configuration dict with master, config, etc.
    """
    try:
        from dvt.config.user_config import load_computes_config
    except ImportError:
        return {}

    computes = load_computes_config(profiles_dir)
    if not computes:
        return {}

    # If no compute specified, look for 'default' or first available
    if not compute_name:
        if "default" in computes:
            return computes["default"]
        # Return first compute if available
        if computes:
            first_key = next(iter(computes))
            return computes[first_key]
        return {}

    # Return specified compute or empty dict
    return computes.get(compute_name, {})


class SeedTask(RunTask):
    """Seed task that uses Spark JDBC for loading CSV files.

    DVT always uses Spark-based seeding for:
    - Better performance with large files
    - Cross-target seeding support
    - Consistent federation architecture

    Use --compute to specify compute engine, --target for destination.

    Thread Safety:
    - ONE Spark session is shared across all seed threads
    - Session is initialized ONCE before any seeds run
    - This avoids race conditions in singleton reset
    """

    def raise_on_first_error(self) -> bool:
        return False

    def run(self) -> RunExecutionResult:
        """Run the seed task, checking for PySpark first.

        Initializes a single Spark session before running any seeds.
        All seed threads share this session (Spark handles concurrency internally).
        """
        from dvt.task.spark_seed import is_spark_available

        if not is_spark_available():
            raise PySparkNotInstalledError()

        # Initialize Spark session ONCE before any seeds run
        # All seed threads will share this session
        from dvt.federation.spark_manager import SparkManager

        compute_name = getattr(self.args, "COMPUTE", None) or getattr(self.args, "compute", None)
        compute_config = _load_compute_config(compute_name)

        # Create and store the singleton instance with the compute config
        SparkManager._instance = SparkManager(compute_config)
        app_name = f"DVT-Seed-{compute_name or 'default'}"
        SparkManager._instance.get_or_create_session(app_name)

        try:
            return super().run()
        finally:
            # Clean up Spark session to prevent resource leaks
            try:
                SparkManager.get_instance().stop_session()
            except Exception:
                pass  # Ignore cleanup errors

    def get_node_selector(self):
        if self.manifest is None or self.graph is None:
            raise DbtInternalError("manifest and graph must be set to get perform node selection")
        return ResourceTypeSelector(
            graph=self.graph,
            manifest=self.manifest,
            previous_state=self.previous_state,
            resource_types=[NodeType.Seed],
        )

    def get_runner_type(self, _) -> Optional[Type[BaseRunner]]:
        """Get the Spark-based seed runner."""
        from dvt.task.spark_seed import SparkSeedRunner

        # Log compute/target info
        compute_name = getattr(self.args, "COMPUTE", None) or getattr(self.args, "compute", None)
        target_name = getattr(self.args, "TARGET", None) or getattr(self.args, "target", None)

        info_parts = ["🚀 Spark seed"]
        if compute_name:
            info_parts.append(f"compute={compute_name}")
        if target_name:
            info_parts.append(f"target={target_name}")

        sys.stderr.write(green(" ".join(info_parts) + "\n"))
        return SparkSeedRunner

    def task_end_messages(self, results) -> None:
        print_run_end_messages(results)


# Alias for backward compatibility with build.py and other imports
from dvt.task.spark_seed import SparkSeedRunner as SeedRunner  # noqa: E402, F401
