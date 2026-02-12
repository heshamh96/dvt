"""DVT DocsGenerateTask — extends GenerateTask with multi-target catalog generation.

Standard dbt GenerateTask queries catalog metadata using only the default adapter.
In a DVT project with multiple targets (e.g., postgres, snowflake, databricks),
this fails because:

1. Manifest.get_used_schemas() returns (database, schema) tuples from ALL targets
2. PostgresAdapter._get_catalog_schemas() calls SchemaSearchMap.flatten() which
   rejects multiple databases (CrossDbReferenceProhibitedError)
3. Even if it didn't, a single adapter can't query another engine's information_schema

DvtDocsGenerateTask fixes this by:
- Partitioning manifest nodes/sources by their resolved target
- Creating an adapter per target via AdapterManager
- Loading secondary adapter macros (get_catalog, etc.) into the manifest
- Querying each target's catalog independently (no cross-db conflicts)
- Merging all per-target agate.Table results into one unified catalog
- Enriching connection_metadata with adapter type info from each target

The result is a complete catalog.json with metadata from ALL targets.
"""

from __future__ import annotations

import logging
import os
import shutil
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, FrozenSet, List, Optional, Set, Tuple

import agate

import dvt.utils
from dvt.adapters.events.types import (
    BuildingCatalog,
    CannotGenerateDocs,
    CatalogWritten,
    WriteCatalogFailure,
)
from dvt.adapters.factory import get_adapter
from dvt.artifacts.schemas.catalog import CatalogArtifact, PrimitiveDict
from dvt.artifacts.schemas.results import NodeStatus
from dvt.constants import CATALOG_FILENAME, MANIFEST_FILE_NAME
from dvt.contracts.graph.manifest import Manifest
from dvt.graph.graph import UniqueId
from dvt.parser.manifest import write_manifest
from dvt.task.compile import CompileTask
from dvt.task.docs import DOCS_INDEX_FILE_PATH
from dvt.task.docs.generate import Catalog, GenerateTask
from dvt.utils.artifact_upload import add_artifact_produced
from dbt_common.clients.system import load_file_contents
from dbt_common.events.functions import fire_event
from dbt_common.events.types import Formatting
from dbt_common.exceptions import DbtInternalError

logger = logging.getLogger(__name__)


class DvtDocsGenerateTask(GenerateTask):
    """GenerateTask with multi-target catalog generation.

    Overrides run() to query each target's adapter separately, avoiding
    cross-database errors when the manifest contains nodes/sources from
    different database engines.
    """

    # ------------------------------------------------------------------
    # Profile / target helpers (same pattern as DvtCompiler)
    # ------------------------------------------------------------------

    def _get_profiles_dir(self) -> str:
        """Resolve the profiles directory from args or default ~/.dvt."""
        profiles_dir = getattr(self.config.args, "PROFILES_DIR", None)
        if not profiles_dir:
            profiles_dir = getattr(self.config.args, "profiles_dir", None)
        if not profiles_dir:
            from pathlib import Path

            profiles_dir = str(Path.home() / ".dvt")
        return profiles_dir

    def _read_raw_profile(self) -> Optional[Dict[str, Any]]:
        """Read the raw profile dict from profiles.yml (cached)."""
        if hasattr(self, "_cached_raw_profile"):
            return self._cached_raw_profile

        try:
            from dvt.config.profile import read_profile

            raw_profiles = read_profile(self._get_profiles_dir())
            profile_name = self.config.profile_name
            self._cached_raw_profile = raw_profiles.get(profile_name)
        except Exception as e:
            logger.debug("Could not read profiles.yml: %s", str(e))
            self._cached_raw_profile = None

        return self._cached_raw_profile

    def _get_adapter_type_for_target(self, target_name: str) -> Optional[str]:
        """Look up the adapter type for a target name.

        Fast path: if target matches current config, use credentials.type.
        Slow path: reads from cached raw profile.
        """
        if target_name == self.config.target_name:
            return getattr(getattr(self.config, "credentials", None), "type", None)

        try:
            raw_profile = self._read_raw_profile()
            if raw_profile:
                outputs = raw_profile.get("outputs", {})
                if target_name in outputs:
                    return outputs[target_name].get("type")
        except Exception as e:
            logger.debug(
                "Could not look up adapter type for target '%s': %s",
                target_name,
                str(e),
            )

        return None

    # ------------------------------------------------------------------
    # Load secondary adapter macros into the manifest
    # ------------------------------------------------------------------

    def _load_secondary_adapter_macros(
        self,
        adapter_types: Set[str],
    ) -> None:
        """Load macros from secondary adapter packages into the manifest.

        The manifest is parsed with only the default adapter's macros.
        Secondary adapters (e.g., snowflake, databricks) need their own
        macros (get_catalog, current_catalog, etc.) to be available in
        the manifest for execute_macro() to work.

        This method:
        1. Loads each secondary adapter plugin (registers include paths)
        2. Gets the adapter's include paths from the FACTORY
        3. Uses config.load_projects() to create project configs for each path
        4. Parses macros from those projects into the manifest

        Args:
            adapter_types: Set of adapter type names to load
                           (e.g., {"snowflake", "databricks"}).
                           The default adapter type should NOT be included.
        """
        if not adapter_types or self.manifest is None:
            return

        from pathlib import Path

        from dvt.adapters.factory import get_include_paths, load_plugin
        from dvt.contracts.files import ParseFileType
        from dvt.parser.macros import MacroParser
        from dvt.parser.read_files import load_source_file
        from dvt.parser.search import FileBlock

        default_adapter_type = getattr(
            getattr(self.config, "credentials", None), "type", None
        )

        for adapter_type in adapter_types:
            if adapter_type == default_adapter_type:
                continue  # Already loaded

            try:
                # Ensure plugin is registered (populates FACTORY.packages)
                load_plugin(adapter_type)

                # Get include paths for this adapter
                include_paths = get_include_paths(adapter_type)

                macros_loaded = 0
                for include_path in include_paths:
                    # Skip the global project — already loaded with default adapter
                    if "global_project" in str(include_path):
                        continue

                    # Load this include path as a project
                    try:
                        for project_name, project in self.config.load_projects(
                            [Path(include_path)]
                        ):
                            # Check if we already have macros from this package
                            has_existing = any(
                                m.package_name == project_name
                                for m in self.manifest.macros.values()
                            )
                            if has_existing:
                                continue

                            # Parse macros from this project into the manifest
                            macro_parser = MacroParser(project, self.manifest)
                            for path in macro_parser.get_paths():
                                source_file = load_source_file(
                                    path, ParseFileType.Macro, project_name, {}
                                )
                                block = FileBlock(source_file)
                                macro_parser.parse_file(block)
                                macros_loaded += 1

                    except Exception as e:
                        logger.debug(
                            "Could not load project from include path '%s': %s",
                            include_path,
                            str(e),
                        )

                if macros_loaded > 0:
                    fire_event(
                        Formatting(
                            msg=f"  Loaded {macros_loaded} macro files for "
                            f"'{adapter_type}' adapter"
                        )
                    )

            except Exception as e:
                logger.warning(
                    "Could not load macros for adapter '%s': %s",
                    adapter_type,
                    str(e),
                )

    # ------------------------------------------------------------------
    # Partition nodes/sources by target
    # ------------------------------------------------------------------

    def _partition_by_target(
        self,
        manifest: Manifest,
        selected_node_ids: Optional[Set[UniqueId]] = None,
    ) -> Dict[str, List[Any]]:
        """Partition manifest nodes and sources by their resolved target.

        For sources: uses config.connection or falls back to default target.
        For models/nodes: uses config.target or falls back to default target.

        Returns:
            Dict mapping target_name -> list of nodes/sources for that target.
        """
        default_target = self.config.target_name
        partitions: Dict[str, List[Any]] = defaultdict(list)

        # Partition sources
        for unique_id, source in manifest.sources.items():
            if selected_node_ids is not None and unique_id not in selected_node_ids:
                continue
            conn = getattr(source.config, "connection", None) if source.config else None
            target = conn if conn else default_target
            partitions[target].append(source)

        # Partition relational, non-ephemeral nodes
        for unique_id, node in manifest.nodes.items():
            if not (node.is_relational and not node.is_ephemeral_model):
                continue
            if selected_node_ids is not None and unique_id not in selected_node_ids:
                continue
            model_target = getattr(node.config, "target", None) if node.config else None
            target = model_target if model_target else default_target
            partitions[target].append(node)

        return dict(partitions)

    def _get_used_schemas_for_nodes(
        self, nodes: List[Any]
    ) -> FrozenSet[Tuple[Optional[str], str]]:
        """Build the used_schemas frozenset for a subset of nodes.

        Same format as Manifest.get_used_schemas() but scoped to specific nodes.
        """
        return frozenset({(node.database, node.schema) for node in nodes})

    # ------------------------------------------------------------------
    # Per-target catalog query
    # ------------------------------------------------------------------

    def _query_catalog_for_target(
        self,
        target_name: str,
        nodes: List[Any],
        adapter: Any,
    ) -> Tuple[Optional["agate.Table"], List[Exception]]:
        """Query catalog metadata for a single target's nodes using its adapter.

        Args:
            target_name: The target name being queried.
            nodes: The nodes/sources belonging to this target.
            adapter: The adapter instance for this target.

        Returns:
            (agate.Table or None, list of exceptions)
        """
        if not nodes:
            return None, []

        used_schemas = self._get_used_schemas_for_nodes(nodes)

        try:
            with adapter.connection_named(f"generate_catalog_{target_name}"):
                catalog_table, exceptions = adapter.get_filtered_catalog(
                    nodes,
                    used_schemas,
                    None,  # No relation filtering per-target
                )
                return catalog_table, exceptions
        except Exception as e:
            logger.warning(
                "Failed to query catalog for target '%s' (%s): %s",
                target_name,
                type(e).__name__,
                str(e),
            )
            logger.debug("Traceback for target '%s':", target_name, exc_info=True)
            return None, [e]

    # ------------------------------------------------------------------
    # run() — multi-target catalog generation
    # ------------------------------------------------------------------

    def run(self) -> CatalogArtifact:
        compile_results = None
        if self.args.compile:
            compile_results = CompileTask.run(self)
            if any(r.status == NodeStatus.Error for r in compile_results):
                fire_event(CannotGenerateDocs())
                return CatalogArtifact.from_results(
                    nodes={},
                    sources={},
                    generated_at=datetime.now(timezone.utc).replace(tzinfo=None),
                    errors=None,
                    compile_results=compile_results,
                )

        shutil.copyfile(
            DOCS_INDEX_FILE_PATH,
            os.path.join(self.config.project_target_path, "index.html"),
        )

        for asset_path in self.config.asset_paths:
            to_asset_path = os.path.join(self.config.project_target_path, asset_path)
            if os.path.exists(to_asset_path):
                shutil.rmtree(to_asset_path)
            from_asset_path = os.path.join(self.config.project_root, asset_path)
            if os.path.exists(from_asset_path):
                shutil.copytree(from_asset_path, to_asset_path)

        if self.manifest is None:
            raise DbtInternalError("self.manifest was None in run!")

        # ------------------------------------------------------------------
        # DVT: Multi-target catalog generation
        # ------------------------------------------------------------------

        selected_node_ids: Optional[Set[UniqueId]] = None

        if self.args.empty_catalog:
            # Preserve existing behavior for --empty-catalog
            catalog_table: agate.Table = agate.Table([])
            all_exceptions: List[Exception] = []
            selected_node_ids = set()
        else:
            fire_event(BuildingCatalog())

            # Resolve selected nodes if running with --select
            if self.job_queue is not None:
                selected_node_ids = self.job_queue.get_selected_nodes()
                selected_source_ids = self._get_selected_source_ids()
                selected_node_ids.update(selected_source_ids)

            # Partition nodes/sources by target
            partitions = self._partition_by_target(self.manifest, selected_node_ids)

            default_target = self.config.target_name
            target_count = len(partitions)
            if target_count > 1:
                target_list = ", ".join(sorted(partitions.keys()))
                fire_event(
                    Formatting(
                        msg=f"DVT: Generating catalog across {target_count} targets: "
                        f"{target_list}"
                    )
                )

                # Load macros from secondary adapter packages into the manifest
                # so execute_macro() can find adapter-specific catalog macros
                # (e.g., snowflake__get_catalog, databricks-specific macros)
                secondary_adapter_types: Set[str] = set()
                for target_name in partitions:
                    if target_name != default_target:
                        adapter_type = self._get_adapter_type_for_target(target_name)
                        if adapter_type:
                            secondary_adapter_types.add(adapter_type)

                if secondary_adapter_types:
                    self._load_secondary_adapter_macros(secondary_adapter_types)

            # Query each target's catalog independently
            catalog_tables: List[agate.Table] = []
            all_exceptions: List[Exception] = []

            for target_name, target_nodes in partitions.items():
                node_count = len(target_nodes)

                # Get or create adapter for this target
                if target_name == default_target:
                    # Use the default adapter (already configured)
                    adapter = get_adapter(self.config)
                else:
                    # Use AdapterManager for non-default targets
                    try:
                        from dvt.federation.adapter_manager import AdapterManager

                        adapter = AdapterManager.get_adapter(
                            profile_name=self.config.profile_name,
                            target_name=target_name,
                            profiles_dir=self._get_profiles_dir(),
                        )

                        # Set the RuntimeConfig proxy on MinimalAdapterConfig
                        # so the macro context system can access RuntimeConfig
                        # methods (load_dependencies, vars, etc.) while keeping
                        # adapter-specific fields (credentials, target_name).
                        if hasattr(adapter.config, "_runtime_config_proxy"):
                            adapter.config._runtime_config_proxy = self.config

                        # Set macro resolver + context generator so the adapter
                        # can execute catalog macros (get_catalog, etc.).
                        from dvt.context.providers import generate_runtime_macro_context

                        adapter.set_macro_resolver(self.manifest)
                        adapter.set_macro_context_generator(
                            generate_runtime_macro_context
                        )

                        # Register the adapter in the FACTORY so that
                        # get_adapter(config) can find it when the macro
                        # context generator calls get_adapter() internally.
                        # MacroContext.__init__ does:
                        #   self.adapter = get_adapter(self.config)
                        # which looks up FACTORY.adapters[credentials.type].
                        from dvt.adapters.factory import FACTORY

                        adapter_type = adapter.type()
                        with FACTORY.lock:
                            if adapter_type not in FACTORY.adapters:
                                FACTORY.adapters[adapter_type] = adapter
                    except Exception as e:
                        logger.warning(
                            "Could not create adapter for target '%s': %s. "
                            "Skipping %d nodes.",
                            target_name,
                            str(e),
                            node_count,
                        )
                        all_exceptions.append(e)
                        continue

                # Query catalog for this target
                fire_event(
                    Formatting(
                        msg=f"  Querying catalog for target '{target_name}' "
                        f"({node_count} nodes)..."
                    )
                )

                table, exceptions = self._query_catalog_for_target(
                    target_name, target_nodes, adapter
                )

                if exceptions:
                    for exc in exceptions:
                        logger.warning(
                            "Catalog exception for target '%s': %s: %s",
                            target_name,
                            type(exc).__name__,
                            str(exc),
                        )
                    all_exceptions.extend(exceptions)

                if table is not None and len(table) > 0:
                    catalog_tables.append(table)
                    fire_event(
                        Formatting(
                            msg=f"  Got {len(table)} catalog entries "
                            f"from '{target_name}'"
                        )
                    )
                elif not exceptions:
                    fire_event(
                        Formatting(
                            msg=f"  No catalog entries returned from '{target_name}'"
                        )
                    )

            # Merge catalog data from all targets.
            # Instead of merging agate.Table objects (which fails when adapters
            # return columns with different types), we convert each table to
            # dicts and concatenate. The Catalog() constructor handles dicts.
            merged_catalog_data: List[PrimitiveDict] = []
            skipped_rows = 0
            for t in catalog_tables:
                for row in t:
                    row_dict = dict(
                        zip(
                            t.column_names,
                            map(dvt.utils._coerce_decimal, row),
                        )
                    )
                    # Filter out rows with missing required fields.
                    # Some adapters (e.g., Databricks) may return catalog rows
                    # where table_name or table_schema is None — these produce
                    # invalid CatalogTable entries that crash make_unique_id_map().
                    if (
                        row_dict.get("table_name") is None
                        or row_dict.get("table_schema") is None
                    ):
                        skipped_rows += 1
                        continue
                    merged_catalog_data.append(row_dict)

            if skipped_rows > 0:
                logger.debug(
                    "Skipped %d catalog rows with missing table_name or table_schema",
                    skipped_rows,
                )

            # Cleanup non-default adapters
            try:
                from dvt.federation.adapter_manager import AdapterManager

                AdapterManager.cleanup()
            except Exception:
                pass

            # Remove secondary adapters from FACTORY.adapters
            # (they were temporarily registered for macro context generation)
            try:
                from dvt.adapters.factory import FACTORY

                default_adapter_type = getattr(
                    getattr(self.config, "credentials", None), "type", None
                )
                with FACTORY.lock:
                    for key in list(FACTORY.adapters.keys()):
                        if key != default_adapter_type:
                            del FACTORY.adapters[key]
            except Exception:
                pass

        # ------------------------------------------------------------------
        # Build catalog dict from merged table (same as base GenerateTask)
        # ------------------------------------------------------------------

        if self.args.empty_catalog:
            merged_catalog_data = []

        catalog = Catalog(merged_catalog_data)

        errors: Optional[List[str]] = None
        if all_exceptions:
            errors = [str(e) for e in all_exceptions]

        nodes, sources = catalog.make_unique_id_map(self.manifest, selected_node_ids)

        # ------------------------------------------------------------------
        # DVT: Enhanced source_connections and connection_metadata
        # ------------------------------------------------------------------

        source_connections, connection_metadata = (
            self._get_source_connections_and_metadata(selected_node_ids)
        )

        # Enrich connection_metadata with adapter_type for each target we queried
        if not self.args.empty_catalog:
            for target_name in partitions:
                adapter_type = self._get_adapter_type_for_target(target_name)
                if adapter_type:
                    connection_metadata.setdefault(target_name, {})["adapter_type"] = (
                        adapter_type
                    )

        results = self.get_catalog_results(
            nodes=nodes,
            sources=sources,
            generated_at=datetime.now(timezone.utc).replace(tzinfo=None),
            compile_results=compile_results,
            errors=errors,
            source_connections=source_connections,
            connection_metadata=connection_metadata,
        )

        catalog_path = os.path.join(self.config.project_target_path, CATALOG_FILENAME)
        results.write(catalog_path)
        add_artifact_produced(catalog_path)

        from dvt.events.types import ArtifactWritten

        fire_event(
            ArtifactWritten(
                artifact_type=results.__class__.__name__,
                artifact_path=catalog_path,
            )
        )

        if self.args.compile:
            write_manifest(self.manifest, self.config.project_target_path)

        if self.args.static:
            read_manifest_data = load_file_contents(
                os.path.join(self.config.project_target_path, MANIFEST_FILE_NAME)
            )
            read_catalog_data = load_file_contents(catalog_path)

            index_data = load_file_contents(DOCS_INDEX_FILE_PATH)
            index_data = index_data.replace(
                '"MANIFEST.JSON INLINE DATA"', read_manifest_data
            )
            index_data = index_data.replace(
                '"CATALOG.JSON INLINE DATA"', read_catalog_data
            )

            static_index_path = os.path.join(
                self.config.project_target_path, "static_index.html"
            )
            with open(static_index_path, "wb") as static_index_file:
                static_index_file.write(bytes(index_data, "utf8"))

        if all_exceptions:
            fire_event(WriteCatalogFailure(num_exceptions=len(all_exceptions)))
        fire_event(CatalogWritten(path=os.path.abspath(catalog_path)))
        return results
