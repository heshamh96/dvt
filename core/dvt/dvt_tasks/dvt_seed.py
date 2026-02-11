"""DVT SeedTask — extends dbt SeedTask with Spark-based seeding.

DVT always uses Spark-based seeding for:
- Better performance with large files
- Cross-target seeding support
- Multi-format support (CSV, Parquet, JSON, ORC)
- Consistent federation architecture

Use --compute to specify compute engine, --target for destination.
"""

from __future__ import annotations

from typing import Any, Dict, Optional, Type

from dvt.artifacts.schemas.run import RunExecutionResult
from dvt.exceptions import PySparkNotInstalledError
from dvt.graph import ResourceTypeSelector
from dvt.node_types import NodeType
from dvt.task.base import BaseRunner
from dvt.task.printer import print_run_end_messages
from dvt.task.run import RunTask
from dbt_common.events.functions import fire_event
from dbt_common.events.types import Formatting
from dbt_common.exceptions import DbtInternalError


def _load_compute_config(
    compute_name: Optional[str], profiles_dir: Optional[str] = None
) -> Dict[str, Any]:
    """Load compute configuration from computes.yml."""
    try:
        from dvt.config.user_config import load_computes_config
    except ImportError:
        return {}

    computes = load_computes_config(profiles_dir)
    if not computes:
        return {}

    if not compute_name:
        if "default" in computes:
            return computes["default"]
        if computes:
            first_key = next(iter(computes))
            return computes[first_key]
        return {}

    return computes.get(compute_name, {})


class DvtSeedTask(RunTask):
    """Seed task that uses Spark JDBC for loading seed files.

    Thread Safety:
    - ONE Spark session is shared across all seed threads
    - Session is initialized ONCE before any seeds run
    """

    def raise_on_first_error(self) -> bool:
        return False

    def run(self) -> RunExecutionResult:
        """Run the seed task, checking for PySpark first."""
        from dvt.task.spark_seed import is_spark_available

        if not is_spark_available():
            raise PySparkNotInstalledError()

        # Initialize Spark session ONCE before any seeds run
        from dvt.federation.spark_manager import SparkManager
        from dvt.config.user_config import load_buckets_for_profile

        compute_name = getattr(self.args, "COMPUTE", None) or getattr(
            self.args, "compute", None
        )
        compute_config = _load_compute_config(compute_name)

        # Load bucket configs for cloud storage credentials
        profile_name = self.config.profile_name
        profile_buckets = load_buckets_for_profile(profile_name)
        bucket_configs = profile_buckets.get("buckets", {}) if profile_buckets else {}

        SparkManager.initialize(
            config=compute_config,
            bucket_configs=bucket_configs,
        )

        app_name = f"DVT-Seed-{compute_name or 'default'}"
        SparkManager.get_instance().get_or_create_session(app_name)

        try:
            return super().run()
        finally:
            try:
                from dvt.federation.adapter_manager import AdapterManager

                AdapterManager.cleanup()
            except Exception:
                pass
            try:
                SparkManager.reset()
            except Exception:
                pass

    def get_node_selector(self):
        if self.manifest is None or self.graph is None:
            raise DbtInternalError(
                "manifest and graph must be set to get perform node selection"
            )
        return ResourceTypeSelector(
            graph=self.graph,
            manifest=self.manifest,
            previous_state=self.previous_state,
            resource_types=[NodeType.Seed],
        )

    def get_runner_type(self, _) -> Optional[Type[BaseRunner]]:
        """Get the Spark-based seed runner."""
        from dvt.task.spark_seed import SparkSeedRunner

        compute_name = getattr(self.args, "COMPUTE", None) or getattr(
            self.args, "compute", None
        )
        target_name = getattr(self.args, "TARGET", None) or getattr(
            self.args, "target", None
        )

        info_parts = ["Spark seed"]
        if compute_name:
            info_parts.append(f"compute={compute_name}")
        if target_name:
            info_parts.append(f"target={target_name}")

        fire_event(Formatting(msg=" ".join(info_parts)))
        return SparkSeedRunner

    def task_end_messages(self, results) -> None:
        print_run_end_messages(results)
