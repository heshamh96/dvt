"""
DVT Model Runner — extends dbt's ModelRunner with cross-engine extraction.

Implements the three-way dispatch:
1. Default/Non-Default Pushdown → delegates to stock dbt ModelRunner
2. Sling Direct (single remote source) → Sling streams source → target model table
3. DuckDB Compute (multi remote sources) → Sling → DuckDB → Sling → target

The user writes standard dbt models. DVT detects remote sources automatically
and routes to the appropriate execution path.
"""

import logging
import threading
import time
from typing import Any, Dict, List, Optional

from dbt.artifacts.schemas.results import RunStatus, TimingInfo
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
    """Build a RunResult for Sling/DuckDB execution paths."""
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
    """Extends ModelRunner with DVT cross-engine extraction paths."""

    # Set by DvtRunTask before execution
    _resolution: Optional[ResolvedModel] = None
    _profiles: Optional[dict] = None

    def set_dvt_context(
        self,
        resolution: ResolvedModel,
        profiles: dict,
    ) -> None:
        """Set DVT-specific context for this runner."""
        self._resolution = resolution
        self._profiles = profiles

    def execute(self, model, manifest):
        """Three-way dispatch based on resolved execution path."""
        if self._resolution is None:
            return super().execute(model, manifest)

        path = self._resolution.execution_path

        if path in (ExecutionPath.DEFAULT_PUSHDOWN, ExecutionPath.NON_DEFAULT_PUSHDOWN):
            logger.info(f"DVT [{model.name}]: {path.value}")
            return super().execute(model, manifest)

        elif path == ExecutionPath.SLING_DIRECT:
            logger.info(f"DVT [{model.name}]: sling_direct")
            return self._execute_sling_direct(model, manifest)

        elif path == ExecutionPath.DUCKDB_COMPUTE:
            logger.info(f"DVT [{model.name}]: duckdb_compute")
            return self._execute_duckdb_compute(model, manifest)

        else:
            return super().execute(model, manifest)

    # ------------------------------------------------------------------
    # Sling Direct — single remote source, streams to target model table
    # ------------------------------------------------------------------

    def _execute_sling_direct(self, model, manifest) -> RunResult:
        """Sling Direct — extract remote source(s) to staging, run model SQL on target.

        Steps:
        1. For each remote source: Sling extracts to a staging table on the target
        2. Rewrite model's compiled SQL to replace remote source refs with staging tables
        3. Execute rewritten SQL on the target via adapter (CREATE TABLE AS)
        4. Drop staging tables
        """
        from dvt.extraction.sling_client import SlingClient

        resolution = self._resolution
        start = time.time()
        staging_tables = []

        try:
            client = SlingClient()
            target_config = self._get_output_config(resolution.target)
            compiled_sql = getattr(model, "compiled_code", "") or ""

            # Step 1: Extract each remote source to a staging table
            for remote in resolution.remote_sources or []:
                source_config = self._get_output_config(remote.connection)
                src_schema = remote.schema or ""
                src_table = remote.identifier or remote.table_name
                extraction_query = (
                    f"SELECT * FROM {src_schema}.{src_table}"
                    if src_schema
                    else f"SELECT * FROM {src_table}"
                )

                staging_name = f"_dvt_stg_{remote.source_name}__{remote.table_name}"
                staging_fqn = (
                    f"{model.schema}.{staging_name}" if model.schema else staging_name
                )
                staging_tables.append(staging_fqn)

                client.extract_to_target(
                    source_config=source_config,
                    target_config=target_config,
                    source_query=extraction_query,
                    target_table=staging_fqn,
                    mode="full-refresh",
                    primary_key=None,
                )

                # Step 2: Rewrite compiled SQL — replace remote source ref with staging table
                # The compiled SQL has the source ref as resolved by dbt, e.g.:
                #   "devdb"."devdb"."test_seed"  (database.schema.table with quotes)
                # We need to replace it with the staging table on the target.
                source_node = manifest.sources.get(remote.source_unique_id)
                if source_node:
                    # Build the ref string that dbt generated for this source
                    src_db = getattr(source_node, "database", "")
                    src_sch = getattr(source_node, "schema", "")
                    src_id = getattr(source_node, "identifier", "") or getattr(
                        source_node, "name", ""
                    )

                    # Try various formats dbt might have generated
                    candidates = []
                    if src_db and src_sch:
                        candidates.append(f'"{src_db}"."{src_sch}"."{src_id}"')
                        candidates.append(f"{src_db}.{src_sch}.{src_id}")
                    if src_sch:
                        candidates.append(f'"{src_sch}"."{src_id}"')
                        candidates.append(f"{src_sch}.{src_id}")
                    candidates.append(f'"{src_id}"')
                    candidates.append(src_id)

                    # Target staging ref — unquoted for simplicity
                    target_ref = staging_fqn

                    for candidate in candidates:
                        if candidate in compiled_sql:
                            compiled_sql = compiled_sql.replace(candidate, target_ref)
                            break

            # Step 3: Execute rewritten model SQL on target via adapter
            target_table = self._model_table_name(model)
            create_sql = f"DROP TABLE IF EXISTS {target_table};\nCREATE TABLE {target_table} AS (\n{compiled_sql}\n)"

            for stmt in create_sql.split(";\n"):
                stmt = stmt.strip()
                if stmt:
                    self.adapter.execute(stmt, auto_begin=True)
            self.adapter.commit_if_has_connection()

            # Step 4: Drop staging tables
            for stg in staging_tables:
                try:
                    self.adapter.execute(f"DROP TABLE IF EXISTS {stg}", auto_begin=True)
                    self.adapter.commit_if_has_connection()
                except Exception:
                    pass

            elapsed = time.time() - start
            msg = f"Sling Direct → {target_table}"
            return _make_run_result(model, RunStatus.Success, msg, elapsed)

        except Exception as e:
            elapsed = time.time() - start
            logger.error(f"DVT [{model.name}]: Sling Direct failed: {e}")
            # Cleanup staging on error
            for stg in staging_tables:
                try:
                    self.adapter.execute(f"DROP TABLE IF EXISTS {stg}", auto_begin=True)
                    self.adapter.commit_if_has_connection()
                except Exception:
                    pass
            return _make_run_result(model, RunStatus.Error, str(e), elapsed)

    # ------------------------------------------------------------------
    # DuckDB Compute — multiple remote sources, process in DuckDB
    # ------------------------------------------------------------------

    def _execute_duckdb_compute(self, model, manifest) -> RunResult:
        from dvt.extraction.sling_client import SlingClient
        from dvt.federation.duckdb_compute import DuckDBCompute

        resolution = self._resolution
        start = time.time()

        try:
            client = SlingClient()

            with DuckDBCompute() as duckdb_engine:
                # Step 1: Extract each remote source into DuckDB
                for remote in resolution.remote_sources:
                    source_config = self._get_output_config(remote.connection)
                    src_schema = remote.schema or ""
                    src_table = remote.identifier or remote.table_name
                    query = (
                        f"SELECT * FROM {src_schema}.{src_table}"
                        if src_schema
                        else f"SELECT * FROM {src_table}"
                    )
                    duckdb_table = f"{remote.source_name}__{remote.table_name}"

                    client.extract_to_duckdb(
                        source_config=source_config,
                        duckdb_path=duckdb_engine.db_path,
                        source_query=query,
                        duckdb_table=duckdb_table,
                    )

                # Step 2: Extract remote refs into DuckDB
                for ref_uid in resolution.remote_refs:
                    ref_node = manifest.nodes.get(ref_uid)
                    if not ref_node:
                        continue
                    ref_target = (
                        getattr(ref_node.config, "target", None)
                        or self.config.target_name
                    )
                    ref_config = self._get_output_config(ref_target)
                    ref_schema = getattr(ref_node, "schema", "")
                    ref_name = getattr(ref_node, "name", "")
                    query = (
                        f"SELECT * FROM {ref_schema}.{ref_name}"
                        if ref_schema
                        else f"SELECT * FROM {ref_name}"
                    )
                    duckdb_table = ref_name

                    client.extract_to_duckdb(
                        source_config=ref_config,
                        duckdb_path=duckdb_engine.db_path,
                        source_query=query,
                        duckdb_table=duckdb_table,
                    )

                # Step 3: Run model SQL in DuckDB
                compiled_sql = model.compiled_code
                if not compiled_sql:
                    raise RuntimeError(f"Model {model.name} has no compiled SQL")

                result_table = f"__dvt_result_{model.name}"
                row_count = duckdb_engine.create_result_table(
                    compiled_sql, result_table
                )

                # Step 4: Load result → target
                target_config = self._get_output_config(resolution.target)
                target_table = self._model_table_name(model)

                client.load_from_duckdb(
                    duckdb_path=duckdb_engine.db_path,
                    target_config=target_config,
                    source_query=f"SELECT * FROM {result_table}",
                    target_table=target_table,
                    mode="full-refresh",
                )

            elapsed = time.time() - start
            msg = f"DuckDB Compute → {target_table} ({row_count} rows)"
            return _make_run_result(model, RunStatus.Success, msg, elapsed, row_count)

        except Exception as e:
            elapsed = time.time() - start
            logger.error(f"DVT [{model.name}]: DuckDB Compute failed: {e}")
            return _make_run_result(model, RunStatus.Error, str(e), elapsed)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _build_single_source_query(
        self, model, manifest, resolution: ResolvedModel
    ) -> tuple:
        """Build the extraction query for Sling Direct path.

        For incremental models, pre-resolves the watermark from the target
        and formats it as a dialect-specific literal for the source engine.

        Returns:
            (source_connection_name, extraction_query)
        """
        if resolution.remote_sources:
            remote = resolution.remote_sources[0]
            source_connection = remote.connection
            src_schema = remote.schema or ""
            src_table = remote.identifier or remote.table_name
            fqn = f"{src_schema}.{src_table}" if src_schema else src_table
        elif resolution.remote_refs:
            ref_uid = resolution.remote_refs[0]
            ref_node = manifest.nodes.get(ref_uid)
            if not ref_node:
                raise RuntimeError(f"Cannot resolve remote ref {ref_uid}")
            source_connection = (
                getattr(ref_node.config, "target", None) or self.config.target_name
            )
            ref_schema = getattr(ref_node, "schema", "")
            ref_name = getattr(ref_node, "name", "")
            fqn = f"{ref_schema}.{ref_name}" if ref_schema else ref_name
        else:
            raise RuntimeError(f"No remote sources or refs found for {model.name}")

        # Base query
        query = f"SELECT * FROM {fqn}"

        # For incremental: pre-resolve watermark
        materialization = model.get_materialization()
        if materialization == "incremental" and self._is_incremental(model):
            watermark_clause = self._resolve_watermark(
                model, source_connection, resolution.target
            )
            if watermark_clause:
                query += f" WHERE {watermark_clause}"

        return source_connection, query

    def _is_incremental(self, model) -> bool:
        """Check if this is an incremental run (table exists on target)."""
        try:
            relation = self.adapter.get_relation(
                database=model.database,
                schema=model.schema,
                identifier=model.name,
            )
            return relation is not None
        except Exception:
            return False

    def _resolve_watermark(
        self, model, source_connection: str, target_name: str
    ) -> Optional[str]:
        """Pre-resolve the incremental watermark from the target table.

        Queries the target for MAX(update_key), formats the value as a
        dialect-specific literal for the source engine.

        Returns:
            A WHERE clause string like "updated_at > TO_TIMESTAMP('...')"
            or None if no watermark can be resolved.
        """
        from dvt.extraction.watermark_formatter import format_watermark

        unique_key = model.config.get("unique_key")
        if not unique_key:
            return None

        # Try to find an update_key column — common patterns
        # Look for columns named updated_at, modified_at, _updated_at, etc.
        # In the compiled SQL for hints
        compiled = getattr(model, "compiled_code", "") or ""
        update_key = None
        for candidate in [
            "updated_at",
            "modified_at",
            "_updated_at",
            "last_modified",
            "update_date",
        ]:
            if candidate in compiled.lower():
                update_key = candidate
                break

        if not update_key:
            # No recognizable update key — Sling will do full merge on primary_key
            return None

        # Query target for max watermark
        try:
            target_table = (
                f"{model.schema}.{model.name}" if model.schema else model.name
            )
            sql = f"SELECT MAX({update_key}) FROM {target_table}"
            _, result = self.adapter.execute(sql, fetch=True)
            if result and len(result) > 0 and result[0][0] is not None:
                watermark_value = result[0][0]
            else:
                return None  # No data on target yet — full load
        except Exception:
            return None  # Table doesn't exist yet — full load

        # Format for source dialect
        source_config = self._get_output_config(source_connection)
        source_type = source_config.get("type", "")

        formatted = format_watermark(
            value=watermark_value,
            source_type=source_type,
            column_type="timestamp",
        )

        return f"{update_key} > {formatted}"

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
