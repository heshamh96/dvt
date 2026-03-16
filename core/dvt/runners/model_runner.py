"""
DVT Model Runner — extends dbt's ModelRunner with cross-engine extraction.

Two-way dispatch:
1. Pushdown → delegates to stock dbt ModelRunner (all sources local)
2. Extraction → Sling → DuckDB cache → model SQL → Sling → target

ALL extraction models go through the persistent DuckDB cache (.dvt/cache.duckdb).
There is no "Sling Direct" sub-path. This ensures:
- Consistent DuckDB SQL dialect for all cross-engine models
- Incremental support via persistent cache
- Cache per source (shared across models)
"""

import logging
import time
import threading
from typing import Any, Dict, List, Optional, Tuple

from dbt.artifacts.schemas.results import RunStatus
from dbt.artifacts.schemas.run.v5.run import RunResult
from dbt.task.run import ModelRunner

from dvt.config.target_resolver import ExecutionPath, ResolvedModel

logger = logging.getLogger(__name__)


def _make_run_result(
    node: Any,
    status: RunStatus,
    message: str,
    execution_time: float,
    rows_affected: int = 0,
) -> RunResult:
    """Build a RunResult for extraction execution paths."""
    return RunResult(
        status=status,
        thread_id=threading.current_thread().name,
        execution_time=execution_time,
        timing=[],
        message=message,
        node=node,
        adapter_response={"_message": message, "rows_affected": rows_affected},
        failures=0 if status == RunStatus.Success else 1,
        batch_results=None,
    )


class DvtModelRunner(ModelRunner):
    """Extends ModelRunner with DVT extraction via DuckDB cache.

    Two-way dispatch:
    - Pushdown: super().execute() (stock dbt)
    - Extraction: Sling→DuckDB cache→Sling→target
    """

    _resolution: Optional[ResolvedModel] = None
    _profiles: Optional[dict] = None
    _cache = None  # DvtCache instance, shared across runners

    def set_dvt_context(
        self,
        resolution: ResolvedModel,
        profiles: dict,
        cache: Any = None,
    ) -> None:
        """Set DVT-specific context for this runner."""
        self._resolution = resolution
        self._profiles = profiles
        self._cache = cache

    def execute(self, model, manifest):
        """Two-way dispatch: pushdown vs extraction."""
        if self._resolution is None:
            return super().execute(model, manifest)

        path = self._resolution.execution_path

        if path in (ExecutionPath.DEFAULT_PUSHDOWN, ExecutionPath.NON_DEFAULT_PUSHDOWN):
            logger.info(f"DVT [{model.name}]: {path.value}")
            return super().execute(model, manifest)

        elif path in (ExecutionPath.SLING_DIRECT, ExecutionPath.DUCKDB_COMPUTE):
            # Both old sub-paths now route to unified extraction
            logger.info(f"DVT [{model.name}]: extraction via DuckDB cache")
            return self._execute_extraction(model, manifest)

        else:
            return super().execute(model, manifest)

    # ------------------------------------------------------------------
    # Unified Extraction: Sling → DuckDB cache → Sling → Target
    # ------------------------------------------------------------------

    def _execute_extraction(self, model, manifest) -> RunResult:
        """Unified extraction path for ALL cross-engine models.

        1. Extract each remote source → DuckDB cache (Sling)
        2. Rewrite compiled SQL (source refs → cache table names)
        3. Execute model SQL in DuckDB
        4. Store model result in DuckDB cache (for incremental {{ this }})
        5. Close DuckDB (release file lock)
        6. Sling loads result from DuckDB → target
        """
        from dvt.extraction.sling_client import SlingClient
        from dvt.federation.dvt_cache import DvtCache
        from dvt.config.source_connections import load_source_connections

        resolution = self._resolution
        start = time.time()

        try:
            client = SlingClient()
            cache = self._cache
            if cache is None:
                project_dir = getattr(self.config.args, "PROJECT_DIR", None) or "."
                cache = DvtCache(project_dir=project_dir)

            # Acquire file lock — DuckDB has exclusive file locking, and Sling
            # is an external process that also needs it. Serialize all extraction.
            DvtCache._file_lock.acquire()
            try:
                return self._execute_extraction_locked(
                    model, manifest, resolution, client, cache
                )
            finally:
                DvtCache._file_lock.release()

        except Exception as e:
            elapsed = time.time() - start
            logger.error(f"DVT [{model.name}]: extraction failed: {e}")
            return _make_run_result(model, RunStatus.Error, str(e), elapsed)

    def _execute_extraction_locked(
        self, model, manifest, resolution, client, cache
    ) -> RunResult:
        """Inner extraction logic — called while holding the file lock."""
        from dvt.config.source_connections import load_source_connections

        start = time.time()

        try:
            # Ensure cache file exists before any Sling operations
            cache.ensure_created()

            compiled_sql = getattr(model, "compiled_code", "") or ""
            if not compiled_sql:
                raise RuntimeError(f"Model {model.name} has no compiled SQL")

            # Collect ALL source dependencies
            depends_on = getattr(model, "depends_on", None)
            all_source_uids = []
            if depends_on:
                all_source_uids = [
                    n
                    for n in (getattr(depends_on, "nodes", []) or [])
                    if n.startswith("source.")
                ]

            project_dir = getattr(self.config.args, "PROJECT_DIR", None) or "."
            source_connections = load_source_connections(project_dir)

            # Check incremental state
            materialization = model.get_materialization()
            is_incremental = (
                materialization == "incremental" and cache.has_model_result(model.name)
            )

            # Resolve watermark if incremental
            watermark_value = None
            watermark_column = None
            if is_incremental:
                watermark_value, watermark_column = self._resolve_watermark_from_target(
                    model
                )

            # ----------------------------------------------------------
            # Step 1: Extract each source → DuckDB cache
            # ----------------------------------------------------------
            ref_replacements = {}

            for source_uid in all_source_uids:
                source_node = manifest.sources.get(source_uid)
                if not source_node:
                    continue

                src_name = getattr(source_node, "source_name", "")
                tbl_name = getattr(source_node, "name", "")
                src_schema = getattr(source_node, "schema", "")
                src_identifier = getattr(source_node, "identifier", "") or tbl_name
                src_db = getattr(source_node, "database", "")

                connection = source_connections.get(src_name, resolution.target)
                source_config = self._get_output_config(connection)

                cache_table = cache.source_table_name(src_name, tbl_name)

                # Build extraction query
                extraction_query = (
                    f"SELECT * FROM {src_schema}.{src_identifier}"
                    if src_schema
                    else f"SELECT * FROM {src_identifier}"
                )

                # For incremental: add watermark filter to extraction
                if is_incremental and watermark_value is not None and watermark_column:
                    from dvt.extraction.watermark_formatter import format_watermark

                    source_type = source_config.get("type", "")
                    formatted = format_watermark(
                        watermark_value, source_type, "timestamp"
                    )
                    extraction_query += f" WHERE {watermark_column} > {formatted}"

                # Extract via Sling → DuckDB cache
                # Close DuckDB first so Sling can write to the file
                cache.close_and_release()

                client.extract_to_duckdb(
                    source_config=source_config,
                    duckdb_path=cache.db_path,
                    source_query=extraction_query,
                    duckdb_table=cache_table,
                )

                # Reopen after Sling is done
                cache.reopen()

                # Collect ref replacements for SQL rewriting
                candidates = []
                if src_db and src_schema:
                    candidates.append(f'"{src_db}"."{src_schema}"."{src_identifier}"')
                    candidates.append(f"{src_db}.{src_schema}.{src_identifier}")
                if src_schema:
                    candidates.append(f'"{src_schema}"."{src_identifier}"')
                    candidates.append(f"{src_schema}.{src_identifier}")

                for candidate in candidates:
                    if candidate in compiled_sql:
                        ref_replacements[candidate] = cache_table
                        break

            # Also extract remote refs
            for ref_uid in resolution.remote_refs or []:
                ref_node = manifest.nodes.get(ref_uid)
                if not ref_node:
                    continue
                ref_target = (
                    getattr(ref_node.config, "target", None) or self.config.target_name
                )
                ref_config = self._get_output_config(ref_target)
                ref_schema = getattr(ref_node, "schema", "")
                ref_name = getattr(ref_node, "name", "")
                query = (
                    f"SELECT * FROM {ref_schema}.{ref_name}"
                    if ref_schema
                    else f"SELECT * FROM {ref_name}"
                )

                cache.close_and_release()
                client.extract_to_duckdb(
                    source_config=ref_config,
                    duckdb_path=cache.db_path,
                    source_query=query,
                    duckdb_table=ref_name,
                )
                cache.reopen()

            # ----------------------------------------------------------
            # Step 2: Rewrite compiled SQL (source refs → cache tables)
            # ----------------------------------------------------------
            for old_ref, new_ref in sorted(
                ref_replacements.items(), key=lambda x: len(x[0]), reverse=True
            ):
                compiled_sql = compiled_sql.replace(old_ref, new_ref)

            # Also rewrite {{ this }} for incremental models.
            # {{ this }} compiles to the target table ref (e.g., "devdb"."public"."model_name").
            # In DuckDB cache, it's __model__model_name.
            if is_incremental:
                model_cache_table = cache.model_table_name(model.name)
                target_table_fqn = self._model_table_name(model)
                # Try various formats dbt might have generated for {{ this }}
                this_candidates = []
                if model.database and model.schema:
                    this_candidates.append(
                        f'"{model.database}"."{model.schema}"."{model.name}"'
                    )
                    this_candidates.append(
                        f"{model.database}.{model.schema}.{model.name}"
                    )
                if model.schema:
                    this_candidates.append(f'"{model.schema}"."{model.name}"')
                    this_candidates.append(f"{model.schema}.{model.name}")
                this_candidates.append(f'"{model.name}"')
                this_candidates.append(model.name)

                for candidate in this_candidates:
                    if candidate in compiled_sql:
                        compiled_sql = compiled_sql.replace(
                            candidate, model_cache_table
                        )
                        break

            # ----------------------------------------------------------
            # Step 3: Execute model SQL in DuckDB + cache the result
            # ----------------------------------------------------------
            logger.info(f"DVT [{model.name}]: DuckDB SQL: {compiled_sql[:200]}...")
            row_count = cache.save_model_result(model.name, compiled_sql)

            # ----------------------------------------------------------
            # Step 4: Close DuckDB, Sling loads result → target
            # ----------------------------------------------------------
            result_table = cache.model_table_name(model.name)
            db_path = cache.db_path
            cache.close_and_release()

            target_config = self._get_output_config(resolution.target)
            target_table = self._model_table_name(model)

            # Determine Sling mode for target
            if is_incremental:
                sling_mode = "incremental"
                unique_key = model.config.get("unique_key")
                primary_key = (
                    [unique_key] if isinstance(unique_key, str) else (unique_key or [])
                )
            else:
                sling_mode = "full-refresh"
                primary_key = None

            client.load_from_duckdb(
                duckdb_path=db_path,
                target_config=target_config,
                source_query=f"SELECT * FROM {result_table}",
                target_table=target_table,
                mode=sling_mode,
                primary_key=primary_key,
            )

            # Reopen cache for potential next model
            cache.reopen()

            elapsed = time.time() - start
            incr_label = " (incremental)" if is_incremental else ""
            msg = f"DuckDB Cache → {target_table} ({row_count} rows{incr_label})"
            return _make_run_result(model, RunStatus.Success, msg, elapsed, row_count)

        except Exception as e:
            elapsed = time.time() - start
            logger.error(f"DVT [{model.name}]: extraction failed: {e}")
            return _make_run_result(model, RunStatus.Error, str(e), elapsed)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _resolve_watermark_from_target(
        self, model
    ) -> Tuple[Optional[Any], Optional[str]]:
        """Query the TARGET for the incremental watermark value.

        Scans the compiled SQL for common update_key column names.
        Returns (watermark_value, column_name) or (None, None).
        """
        compiled = getattr(model, "compiled_code", "") or ""

        # Find update_key column from common patterns
        update_key = None
        for candidate in [
            "updated_at",
            "modified_at",
            "_updated_at",
            "last_modified",
            "update_date",
            "modified_date",
            "created_at",
            "loaded_at",
        ]:
            if candidate in compiled.lower():
                update_key = candidate
                break

        if not update_key:
            return None, None

        try:
            target_table = self._model_table_name(model)
            sql = f"SELECT MAX({update_key}) FROM {target_table}"
            _, result = self.adapter.execute(sql, fetch=True)
            if result and len(result) > 0 and result[0][0] is not None:
                return result[0][0], update_key
        except Exception:
            pass

        return None, None

    def _model_table_name(self, model) -> str:
        """Get the fully-qualified target table name for a model."""
        if model.schema:
            return f"{model.schema}.{model.name}"
        return model.name

    def _get_output_config(self, output_name: str) -> dict:
        """Get a profiles.yml output config by name."""
        if not self._profiles:
            raise RuntimeError("DVT: profiles not set on runner")

        for profile_name, profile_data in self._profiles.items():
            if not isinstance(profile_data, dict):
                continue
            outputs = profile_data.get("outputs", {})
            if output_name in outputs:
                config = outputs[output_name]
                if isinstance(config, dict):
                    return {**config, "type": config.get("type", "")}

        raise RuntimeError(
            f"DVT101: Connection '{output_name}' not found in profiles.yml"
        )
