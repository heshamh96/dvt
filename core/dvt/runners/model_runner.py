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
from typing import Any, Optional

from dbt.task.run import ModelRunner

from dvt.config.target_resolver import ExecutionPath, ResolvedModel

logger = logging.getLogger(__name__)


class DvtModelRunner(ModelRunner):
    """Extends ModelRunner with DVT cross-engine extraction paths.

    For DEFAULT_PUSHDOWN and NON_DEFAULT_PUSHDOWN: delegates to super().execute()
    For SLING_DIRECT: uses Sling to stream single source → target model table
    For DUCKDB_COMPUTE: Sling → DuckDB → Sling → target model table
    """

    # Set by DvtRunTask before execution
    _resolution: Optional[ResolvedModel] = None
    _profiles: Optional[dict] = None

    def set_dvt_context(
        self,
        resolution: ResolvedModel,
        profiles: dict,
    ) -> None:
        """Set DVT-specific context for this runner.

        Called by DvtRunTask.get_runner() after instantiation.
        """
        self._resolution = resolution
        self._profiles = profiles

    def execute(self, model, manifest):
        """Three-way dispatch based on resolved execution path."""
        if self._resolution is None:
            # No DVT resolution — fall back to stock dbt
            return super().execute(model, manifest)

        path = self._resolution.execution_path

        if path in (ExecutionPath.DEFAULT_PUSHDOWN, ExecutionPath.NON_DEFAULT_PUSHDOWN):
            logger.info(f"DVT [{model.name}]: {path.value} — standard dbt adapter")
            return super().execute(model, manifest)

        elif path == ExecutionPath.SLING_DIRECT:
            logger.info(f"DVT [{model.name}]: sling_direct — single remote source")
            return self._execute_sling_direct(model, manifest)

        elif path == ExecutionPath.DUCKDB_COMPUTE:
            logger.info(
                f"DVT [{model.name}]: duckdb_compute — {len(self._resolution.remote_sources)} remote sources"
            )
            return self._execute_duckdb_compute(model, manifest)

        else:
            logger.warning(
                f"DVT [{model.name}]: unknown path {path}, falling back to dbt"
            )
            return super().execute(model, manifest)

    def _execute_sling_direct(self, model, manifest):
        """Sling Direct path — single remote source, stream to target model table.

        1. Get the single remote source
        2. Build extraction query (SELECT * FROM source.schema.table)
        3. Call Sling to stream from source → target model table
        4. For incremental: Sling handles the watermark + merge
        """
        from dvt.extraction.sling_client import SlingClient

        resolution = self._resolution
        assert resolution is not None
        assert len(resolution.remote_sources) == 1 or len(resolution.remote_refs) == 1

        # Get source/ref details
        if resolution.remote_sources:
            remote = resolution.remote_sources[0]
            source_connection = remote.connection
            source_schema = remote.schema or ""
            source_table = remote.identifier or remote.table_name
            extraction_query = (
                f"SELECT * FROM {source_schema}.{source_table}"
                if source_schema
                else f"SELECT * FROM {source_table}"
            )
        else:
            # Remote ref — the ref'd model is already materialized on its target
            # We need to read from that target
            ref_uid = resolution.remote_refs[0]
            ref_node = manifest.nodes.get(ref_uid)
            if ref_node:
                source_connection = (
                    getattr(ref_node.config, "target", None) or self.config.target_name
                )
                source_schema = getattr(ref_node, "schema", "")
                source_table = getattr(ref_node, "name", "")
                extraction_query = (
                    f"SELECT * FROM {source_schema}.{source_table}"
                    if source_schema
                    else f"SELECT * FROM {source_table}"
                )
            else:
                raise RuntimeError(f"DVT: Cannot resolve remote ref {ref_uid}")

        # Get connection configs from profiles
        source_config = self._get_output_config(source_connection)
        target_config = self._get_output_config(resolution.target)

        # Determine target table name
        target_table = f"{model.schema}.{model.name}" if model.schema else model.name

        # Determine Sling mode from materialization
        materialization = model.get_materialization()
        if materialization == "incremental":
            mode = "incremental"
            pk = model.config.get("unique_key")
            primary_key = [pk] if isinstance(pk, str) else pk
        else:
            mode = "full-refresh"
            primary_key = None

        # Execute via Sling
        client = SlingClient()
        result = client.extract_to_target(
            source_config=source_config,
            target_config=target_config,
            source_query=extraction_query,
            target_table=target_table,
            mode=mode,
            primary_key=primary_key,
        )

        logger.info(f"DVT [{model.name}]: Sling Direct complete → {target_table}")

        # Return a RunResult-compatible response
        # For now, delegate to super for the result format but skip the SQL execution
        # TODO: Build proper RunResult in P1.8
        return super().execute(model, manifest)

    def _execute_duckdb_compute(self, model, manifest):
        """DuckDB Compute path — multi-source, stream via DuckDB.

        1. For each remote source: Sling streams source → DuckDB table
        2. Run the model SQL in DuckDB
        3. Sling streams DuckDB result → target model table
        """
        from dvt.extraction.sling_client import SlingClient
        from dvt.federation.duckdb_compute import DuckDBCompute

        resolution = self._resolution
        assert resolution is not None

        client = SlingClient()

        with DuckDBCompute() as duckdb:
            # Step 1: Extract each remote source into DuckDB
            for remote in resolution.remote_sources:
                source_config = self._get_output_config(remote.connection)
                source_schema = remote.schema or ""
                source_table = remote.identifier or remote.table_name
                extraction_query = (
                    f"SELECT * FROM {source_schema}.{source_table}"
                    if source_schema
                    else f"SELECT * FROM {source_table}"
                )
                duckdb_table = f"{remote.source_name}__{remote.table_name}"

                client.extract_to_duckdb(
                    source_config=source_config,
                    duckdb_path=duckdb.db_path,
                    source_query=extraction_query,
                    duckdb_table=duckdb_table,
                )

            # Step 2: Run model SQL in DuckDB
            # The model SQL references source tables — we need to rewrite
            # {{ source('crm', 'customers') }} → crm__customers (DuckDB table name)
            compiled_sql = model.compiled_code
            if not compiled_sql:
                raise RuntimeError(f"DVT: Model {model.name} has no compiled SQL")

            result_table = f"__dvt_result_{model.name}"
            row_count = duckdb.create_result_table(compiled_sql, result_table)

            # Step 3: Load result from DuckDB → target
            target_config = self._get_output_config(resolution.target)
            target_table = (
                f"{model.schema}.{model.name}" if model.schema else model.name
            )

            client.load_from_duckdb(
                duckdb_path=duckdb.db_path,
                target_config=target_config,
                source_query=f"SELECT * FROM {result_table}",
                target_table=target_table,
                mode="full-refresh",
            )

        logger.info(
            f"DVT [{model.name}]: DuckDB compute complete → {target_table} ({row_count} rows)"
        )

        # TODO: Build proper RunResult in P1.8
        return super().execute(model, manifest)

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
