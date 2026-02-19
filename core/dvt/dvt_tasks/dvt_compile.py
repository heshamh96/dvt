"""DVT CompileTask — extends dbt CompileTask with cross-dialect SQL transpilation.

Wires DvtCompiler into the compile path so that ``dvt compile`` transpiles
compiled SQL from the source dialect to the target dialect (e.g., Postgres
double-quotes → Databricks backticks) when they differ.

The base CompileTask/CompileRunner use the standard dbt Compiler which has
no transpilation support.  DvtCompileRunner overrides the runner-level
compiler with DvtCompiler, and DvtCompileTask returns DvtCompileRunner.

For models targeting non-default adapters (e.g., ``config(target='dbx_dev')``
when the default is ``pg_dev``), DvtCompileRunner additionally:
1. Resolves ``is_incremental()`` via local Delta staging presence
2. Sets the macro resolver on the non-default adapter
3. Passes the resolved extra_context and adapter to compile_node()

This makes ``dvt compile`` produce correct compiled SQL for all models,
regardless of which adapter they target.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional, Type

from dvt.contracts.graph.manifest import Manifest
from dvt.dvt_compilation.dvt_compiler import DvtCompiler
from dvt.task.base import BaseRunner
from dvt.task.compile import CompileRunner, CompileTask

logger = logging.getLogger(__name__)


class DvtCompileRunner(CompileRunner):
    """CompileRunner that uses DvtCompiler for cross-dialect transpilation.

    For non-default adapter models, resolves is_incremental() locally via
    Delta staging and sets the macro resolver on the adapter so that
    adapter-native macros (like Databricks's get_uc_tables) don't crash.
    """

    def __init__(self, config, adapter, node, node_index, num_nodes) -> None:
        super().__init__(config, adapter, node, node_index, num_nodes)
        self.compiler = DvtCompiler(config)

    def compile(self, manifest: Manifest):
        # Check if this model targets a non-default adapter
        model_target = getattr(getattr(self.node, "config", None), "target", None)
        if not model_target or model_target == self.config.target_name:
            # Same adapter — standard compile path
            return self.compiler.compile_node(self.node, manifest, {})

        # Non-default adapter model — need special handling
        try:
            from dvt.federation.adapter_manager import AdapterManager

            profiles_dir = self._get_profiles_dir()
            target_adapter = AdapterManager.get_adapter(
                profile_name=self.config.profile_name,
                target_name=model_target,
                profiles_dir=profiles_dir,
            )

            # Set macro resolver so adapter-native macros work
            target_adapter.set_macro_resolver(manifest)

            # Resolve is_incremental() via local Delta staging
            extra_context = self._resolve_is_incremental(manifest)

            return self.compiler.compile_node(
                self.node, manifest, extra_context, adapter=target_adapter
            )
        except Exception as e:
            logger.warning(
                "Non-default adapter compile setup failed for %s (target=%s): %s. "
                "Falling back to default compile path.",
                self.node.unique_id,
                model_target,
                str(e),
            )
            return self.compiler.compile_node(self.node, manifest, {})

    def _get_profiles_dir(self) -> Optional[str]:
        """Get profiles directory from config args."""
        profiles_dir = getattr(self.config.args, "PROFILES_DIR", None)
        if not profiles_dir:
            profiles_dir = getattr(self.config.args, "profiles_dir", None)
        return str(profiles_dir) if profiles_dir else None

    def _resolve_is_incremental(self, manifest: Manifest) -> Dict[str, Any]:
        """Determine is_incremental() for non-default adapter models using Delta staging.

        Same logic as DvtRunnerMixin._resolve_is_incremental_for_federation():
        check if model's Delta staging directory exists locally instead of
        querying the remote target database.
        """
        node = self.node
        mat = getattr(getattr(node, "config", None), "materialized", None)
        if mat != "incremental":
            return {}

        full_refresh = getattr(
            getattr(self.config, "args", None), "FULL_REFRESH", False
        )
        if full_refresh:
            return {"is_incremental": lambda: False}

        # Check if model's Delta staging exists locally
        try:
            from dvt.config.user_config import get_bucket_path, load_buckets_for_profile
            from dvt.federation.state_manager import StateManager

            profile_name = self.config.profile_name
            profiles_dir = self._get_profiles_dir()
            profile_buckets = load_buckets_for_profile(profile_name, profiles_dir)

            if profile_buckets:
                target_bucket = profile_buckets.get("target", "local")
                buckets = profile_buckets.get("buckets", {})
                bucket_config = buckets.get(target_bucket)
                if bucket_config:
                    bucket_path = get_bucket_path(
                        bucket_config, profile_name, profiles_dir
                    )
                    if bucket_path:
                        state_mgr = StateManager(bucket_path)
                        model_staging_id = node.unique_id
                        if state_mgr.staging_exists(model_staging_id):
                            return {"is_incremental": lambda: True}
        except Exception:
            pass

        # No staging exists (first run) — is_incremental() = False
        return {"is_incremental": lambda: False}


class DvtCompileTask(CompileTask):
    """CompileTask that uses DvtCompileRunner for cross-dialect transpilation."""

    def get_runner_type(self, _) -> Optional[Type[BaseRunner]]:
        return DvtCompileRunner
