"""
DVT Seed Runner — loads CSV seeds via Sling instead of dbt's agate-based loader.

Sling uses native bulk loading (COPY, bcp, sqlldr) for 10-100x faster seeds.
"""

import logging
import os
import time
import threading
from typing import Any, Dict, Optional

from dbt.artifacts.schemas.results import RunStatus
from dbt.artifacts.schemas.run.v5.run import RunResult
from dbt.contracts.graph.manifest import Manifest
from dbt.events.types import LogSeedResult, LogStartLine
from dbt.task.seed import SeedRunner
from dbt_common.events.base_types import EventLevel
from dbt_common.events.functions import fire_event

logger = logging.getLogger(__name__)


class DvtSeedRunner(SeedRunner):
    """Seed runner that uses Sling for CSV loading."""

    _profiles: Optional[dict] = None
    _target_name: Optional[str] = None

    def set_dvt_context(self, profiles: dict, target_name: str) -> None:
        self._profiles = profiles
        self._target_name = target_name

    def compile(self, manifest: Manifest):
        # Seeds don't need compilation — just return the node
        return self.node

    def execute(self, model, manifest):
        """Load seed CSV via Sling instead of dbt's agate loader."""
        from dvt.extraction.sling_client import SlingClient

        start = time.time()

        try:
            # Find the CSV file path
            csv_path = self._find_seed_csv(model)
            if not csv_path:
                raise RuntimeError(f"Seed CSV not found for {model.name}")

            # Get target connection config
            target_name = self._target_name or "default"
            target_config = self._get_output_config(target_name)

            # Target table
            target_table = (
                f"{model.schema}.{model.name}" if model.schema else model.name
            )

            full_refresh = getattr(self.config.args, "FULL_REFRESH", False)
            # Always use 'truncate' for seeds — Sling's 'full-refresh' mode
            # has quoting issues with DROP TABLE on some engines.
            # 'truncate' clears and reloads the data, which achieves the
            # same result for seeds. If the table doesn't exist, Sling
            # creates it automatically.
            mode = "truncate"

            client = SlingClient()
            client.load_seed(
                csv_path=csv_path,
                target_config=target_config,
                target_table=target_table,
                mode=mode,
            )

            elapsed = time.time() - start
            msg = f"Sling Seed → {target_table}"
            logger.info(f"DVT [{model.name}]: {msg} ({elapsed:.2f}s)")

            return RunResult(
                status=RunStatus.Success,
                thread_id=threading.current_thread().name,
                execution_time=elapsed,
                timing=[],
                message=msg,
                node=model,
                adapter_response={"_message": msg},
                failures=0,
                batch_results=None,
            )

        except Exception as e:
            elapsed = time.time() - start
            logger.error(f"DVT [{model.name}]: Seed failed: {e}")
            return RunResult(
                status=RunStatus.Error,
                thread_id=threading.current_thread().name,
                execution_time=elapsed,
                timing=[],
                message=str(e),
                node=model,
                adapter_response={"_message": str(e)},
                failures=1,
                batch_results=None,
            )

    def _find_seed_csv(self, model) -> Optional[str]:
        """Find the CSV file for a seed node."""
        # dbt stores the original file path on the node
        original_path = getattr(model, "original_file_path", None)
        if original_path:
            # Try relative to project root
            root = getattr(model, "root_path", None) or getattr(
                self.config, "project_root", "."
            )
            full_path = os.path.join(root, original_path)
            if os.path.isfile(full_path):
                return full_path

        # Fallback: search seed paths
        seed_paths = getattr(self.config, "seed_paths", ["seeds"])
        project_root = getattr(self.config, "project_root", ".")
        for seed_dir in seed_paths:
            csv_file = os.path.join(project_root, seed_dir, f"{model.name}.csv")
            if os.path.isfile(csv_file):
                return csv_file

        return None

    def _get_output_config(self, target_name: str) -> dict:
        """Get profiles.yml output config by name."""
        if not self._profiles:
            raise RuntimeError("DVT: profiles not set on seed runner")

        for profile_name, profile_data in self._profiles.items():
            if not isinstance(profile_data, dict):
                continue
            outputs = profile_data.get("outputs", {})
            if target_name in outputs:
                config = outputs[target_name]
                if isinstance(config, dict):
                    return {**config, "type": config.get("type", "")}

        raise RuntimeError(
            f"DVT101: Connection '{target_name}' not found in profiles.yml"
        )
