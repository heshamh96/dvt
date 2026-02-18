"""
EL Layer - Extract-Load orchestration for DVT federation.

The EL layer optimizes data extraction from source systems:
1. Native bulk export via dbt adapter connections (e.g., Snowflake COPY INTO)
2. Spark JDBC extraction with parallel reads (default fallback)
3. Parquet output to staging bucket
4. Skip extraction if staging exists (consecutive run optimization)
5. Hash-based incremental extraction for changed rows
6. Parallel extraction across sources
"""

import concurrent.futures
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from dvt.config.user_config import (
    get_bucket_path,
    load_buckets_for_profile,
    load_computes_for_profile,
)
from dvt.federation.extractors import ExtractionConfig, ExtractionResult, get_extractor
from dvt.federation.state_manager import (
    SourceState,
    StateManager,
    compute_schema_hash,
)


@dataclass
class SourceConfig:
    """Configuration for a source to extract."""

    source_name: str  # e.g., 'postgres__orders'
    adapter_type: str  # e.g., 'postgres'
    schema: str
    table: str
    connection: Any  # Raw DB-API connection
    connection_config: Optional[Dict[str, Any]] = (
        None  # profiles.yml connection dict for JDBC
    )
    columns: Optional[List[str]] = None
    predicates: Optional[List[str]] = None
    limit: Optional[int] = None
    pk_columns: Optional[List[str]] = None
    batch_size: int = 100000


@dataclass
class ELResult:
    """Result of EL layer processing."""

    success: bool
    sources_extracted: int
    sources_skipped: int
    sources_failed: int
    total_rows: int
    elapsed_seconds: float
    results: Dict[str, ExtractionResult]
    errors: Dict[str, str]


def _resolve_column_names(
    optimizer_columns: Optional[List[str]],
    real_columns: List[Dict[str, str]],
) -> Optional[List[str]]:
    """Match optimizer's potentially-lowercased column names to actual DB column names.

    SQLGlot normalizes unquoted identifiers to lowercase. This function
    resolves them against the real column metadata from the source database
    using case-insensitive matching.

    Args:
        optimizer_columns: Column names extracted by QueryOptimizer (may be lowercased)
        real_columns: Column metadata from extractor.get_columns() with 'name' key

    Returns:
        List of real column names with correct casing, or None for SELECT *
    """
    if not optimizer_columns:
        return None  # SELECT * — extract all columns

    # Build case-insensitive lookup: lowercased_name -> real_name
    real_name_map = {c["name"].lower(): c["name"] for c in real_columns}

    resolved = []
    for col in optimizer_columns:
        real_name = real_name_map.get(col.lower())
        if real_name:
            resolved.append(real_name)
        # else: column not found in source — skip (defensive, e.g. computed column)

    return resolved if resolved else None


class ELLayer:
    """Extract-Load layer for DVT federation.

    Orchestrates extraction from multiple sources to staging bucket.
    Handles consecutive run optimization and incremental extraction.

    Extraction priority:
    1. Native cloud export (for cloud DWs with cloud bucket)
    2. Database-specific bulk export (e.g., PostgreSQL COPY)
    3. Spark JDBC with parallel reads (default fallback)
    """

    def __init__(
        self,
        bucket_path: Path,
        profile_name: str = "default",
        bucket_config: Optional[Dict[str, Any]] = None,
        jdbc_config: Optional[Dict[str, Any]] = None,
        on_progress: Optional[Callable[[str], None]] = None,
        max_workers: int = 4,
    ):
        """Initialize EL layer.

        Args:
            bucket_path: Path to staging bucket directory
            profile_name: Profile name for bucket config
            bucket_config: Bucket configuration dict
            jdbc_config: JDBC extraction settings from computes.yml
            on_progress: Optional callback for progress messages
            max_workers: Maximum parallel extractions
        """
        self.bucket_path = Path(bucket_path)
        self.bucket_path.mkdir(parents=True, exist_ok=True)
        self.profile_name = profile_name
        self.bucket_config = bucket_config or {}
        self.jdbc_config = jdbc_config or {}
        self.on_progress = on_progress or (lambda msg: None)
        self.max_workers = max_workers
        self.state_manager = StateManager(self.bucket_path)

    def _log(self, message: str) -> None:
        """Log a progress message."""
        self.on_progress(message)

    def extract_sources(
        self,
        sources: List[SourceConfig],
        full_refresh: bool = False,
    ) -> ELResult:
        """Extract multiple sources to staging bucket.

        Args:
            sources: List of source configurations
            full_refresh: If True, clear staging and do full extraction

        Returns:
            ELResult with extraction summary
        """
        import time

        start_time = time.time()
        results: Dict[str, ExtractionResult] = {}
        errors: Dict[str, str] = {}
        sources_extracted = 0
        sources_skipped = 0
        sources_failed = 0
        total_rows = 0

        for source in sources:
            try:
                result = self._extract_source(source, full_refresh)
                results[source.source_name] = result

                if result.success:
                    if result.extraction_method == "skip":
                        sources_skipped += 1
                        self._log(f"Skipped {source.source_name} (staging exists)")
                    else:
                        sources_extracted += 1
                        total_rows += result.row_count
                else:
                    sources_failed += 1
                    if result.error:
                        errors[source.source_name] = result.error

            except Exception as e:
                sources_failed += 1
                errors[source.source_name] = str(e)
                results[source.source_name] = ExtractionResult(
                    success=False,
                    source_name=source.source_name,
                    error=str(e),
                )

        elapsed = time.time() - start_time
        success = sources_failed == 0

        return ELResult(
            success=success,
            sources_extracted=sources_extracted,
            sources_skipped=sources_skipped,
            sources_failed=sources_failed,
            total_rows=total_rows,
            elapsed_seconds=elapsed,
            results=results,
            errors=errors,
        )

    def extract_sources_parallel(
        self,
        sources: List[SourceConfig],
        full_refresh: bool = False,
    ) -> ELResult:
        """Extract multiple sources in parallel.

        Args:
            sources: List of source configurations
            full_refresh: If True, clear staging and do full extraction

        Returns:
            ELResult with extraction summary
        """
        import time

        start_time = time.time()
        results: Dict[str, ExtractionResult] = {}
        errors: Dict[str, str] = {}
        sources_extracted = 0
        sources_skipped = 0
        sources_failed = 0
        total_rows = 0

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=self.max_workers
        ) as executor:
            future_to_source = {
                executor.submit(self._extract_source, source, full_refresh): source
                for source in sources
            }

            for future in concurrent.futures.as_completed(future_to_source):
                source = future_to_source[future]
                try:
                    result = future.result()
                    results[source.source_name] = result

                    if result.success:
                        if result.extraction_method == "skip":
                            sources_skipped += 1
                            self._log(f"Skipped {source.source_name} (staging exists)")
                        else:
                            sources_extracted += 1
                            total_rows += result.row_count
                    else:
                        sources_failed += 1
                        if result.error:
                            errors[source.source_name] = result.error

                except Exception as e:
                    sources_failed += 1
                    errors[source.source_name] = str(e)
                    results[source.source_name] = ExtractionResult(
                        success=False,
                        source_name=source.source_name,
                        error=str(e),
                    )

        elapsed = time.time() - start_time
        success = sources_failed == 0

        return ELResult(
            success=success,
            sources_extracted=sources_extracted,
            sources_skipped=sources_skipped,
            sources_failed=sources_failed,
            total_rows=total_rows,
            elapsed_seconds=elapsed,
            results=results,
            errors=errors,
        )

    def _extract_source(
        self,
        source: SourceConfig,
        full_refresh: bool,
    ) -> ExtractionResult:
        """Extract a single source.

        Args:
            source: Source configuration
            full_refresh: If True, clear staging and do full extraction

        Returns:
            ExtractionResult
        """
        # Handle full refresh - clear staging and state
        if full_refresh:
            self.state_manager.clear_source_state(source.source_name)
            self.state_manager.clear_staging(source.source_name)

        # Get extractor for this adapter type
        # Pass connection_config for lazy connection creation if connection is None
        extractor = get_extractor(
            source.adapter_type,
            source.connection,
            source.adapter_type,  # dialect same as adapter for now
            self.on_progress,
            source.connection_config,
        )

        # Get column metadata for schema hash
        columns_info = extractor.get_columns(source.schema, source.table)
        columns = [c["name"] for c in columns_info]
        column_types = {c["name"]: c["type"] for c in columns_info}
        current_schema_hash = compute_schema_hash(columns, column_types)

        # Check if extraction needed
        should_extract, reason = self.state_manager.should_extract(
            source.source_name,
            current_schema_hash,
            full_refresh,
        )

        if not should_extract and reason == "skip":
            # Staging exists and schema unchanged - skip extraction
            return ExtractionResult(
                success=True,
                source_name=source.source_name,
                extraction_method="skip",
            )

        # Auto-detect primary key if not provided
        pk_columns = source.pk_columns
        if not pk_columns:
            pk_columns = extractor.detect_primary_key(source.schema, source.table)

        # Resolve optimizer column names to real DB column names (case-insensitive).
        # SQLGlot may have lowercased the names; we match against real metadata.
        resolved_columns = _resolve_column_names(source.columns, columns_info)

        # Build extraction config with connection info for JDBC fallback
        config = ExtractionConfig(
            source_name=source.source_name,
            schema=source.schema,
            table=source.table,
            columns=resolved_columns,
            predicates=source.predicates,
            limit=source.limit,
            pk_columns=pk_columns,
            batch_size=source.batch_size,
            bucket_config=self.bucket_config,
            connection_config=source.connection_config,
            jdbc_config=self.jdbc_config,
        )

        # Get final output path (Delta format for new extractions)
        final_path = self.state_manager.get_staging_path(source.source_name)

        # Extractors write Parquet natively; we convert to Delta after.
        # Use a temporary Parquet path for extraction, then convert.
        parquet_tmp = final_path.parent / f"tmp_{source.source_name}.parquet"

        # Perform extraction (always writes Parquet)
        self._log(f"Extracting {source.source_name}...")
        result = extractor.extract(config, parquet_tmp)

        if result.success:
            # Convert Parquet staging to Delta format
            result = self._convert_to_delta(
                parquet_tmp, final_path, source.source_name, result
            )
            # Save state
            state = SourceState(
                source_name=source.source_name,
                table_name=source.table,
                schema_hash=current_schema_hash,
                row_count=result.row_count,
                last_extracted_at=datetime.now().isoformat(),
                extraction_method=result.extraction_method,
                pk_columns=pk_columns or [],
                columns=columns,
            )
            self.state_manager.save_source_state(state)

            # Optionally save row hashes for future incremental
            if pk_columns and result.row_count > 0:
                try:
                    hashes = extractor.extract_hashes(config)
                    self.state_manager.save_row_hashes(source.source_name, hashes)
                except Exception as e:
                    self._log(
                        f"Warning: Could not save row hashes for {source.source_name}: {e}"
                    )

        return result

    def _convert_to_delta(
        self,
        parquet_path: Path,
        delta_path: Path,
        source_name: str,
        extraction_result: ExtractionResult,
    ) -> ExtractionResult:
        """Convert extracted Parquet staging data to Delta format.

        All extractors write Parquet natively (PyArrow, Spark JDBC, COPY, etc.).
        This method converts the output to Delta format using Spark, providing
        a single conversion point for all extraction paths.

        If conversion fails (e.g., delta-spark not installed), falls back to
        keeping the Parquet file as-is and logs a warning.

        Args:
            parquet_path: Path to the temporary Parquet file/directory
            delta_path: Path for the final Delta table directory
            source_name: Source identifier for logging
            extraction_result: The ExtractionResult from the extractor

        Returns:
            Updated ExtractionResult with the final output path
        """
        import shutil

        try:
            from dvt.federation.spark_manager import SparkManager

            spark_manager = SparkManager.get_instance()
            spark = spark_manager.get_or_create_session()

            # Read the Parquet data.
            # PyArrow pipe/COPY extraction writes a single file; Spark JDBC writes
            # a directory. spark.read.parquet() can fail on single files in Spark 4.x
            # (UNABLE_TO_INFER_SCHEMA), so wrap single files in a temp directory.
            if parquet_path.is_file():
                # Single file from PyArrow: wrap in a directory so Spark can read it.
                # IMPORTANT: Don't use '.' or '_' prefix — Spark/Hadoop ignores hidden paths.
                parquet_dir = parquet_path.parent / f"tmp_parquet_{source_name}"
                parquet_dir.mkdir(parents=True, exist_ok=True)
                moved_file = parquet_dir / "part-00000.parquet"
                parquet_path.rename(moved_file)
                parquet_path = parquet_dir  # Now it's a directory

            df = spark.read.parquet(str(parquet_path))

            # Write as Delta (overwrites any previous Delta staging)
            # Enable column mapping to support column names with spaces/special chars
            df.write.format("delta").mode("overwrite").option(
                "delta.columnMapping.mode", "name"
            ).option("delta.minReaderVersion", "2").option(
                "delta.minWriterVersion", "5"
            ).save(str(delta_path))

            # Clean up temp Parquet
            if parquet_path.is_dir():
                shutil.rmtree(parquet_path)
            elif parquet_path.exists():
                parquet_path.unlink()

            # Update result with final Delta path
            extraction_result.output_path = delta_path
            return extraction_result

        except Exception as e:
            # Delta conversion failed — fall back to keeping Parquet as-is.
            # Move the temp Parquet to a permanent location so staging_exists()
            # can find it (legacy Parquet backward compat).
            self._log(
                f"Warning: Delta conversion failed for {source_name} ({e}). "
                f"Keeping Parquet staging."
            )
            permanent_parquet = delta_path.parent / f"{source_name}.parquet"
            if parquet_path != permanent_parquet:
                if permanent_parquet.exists():
                    if permanent_parquet.is_dir():
                        shutil.rmtree(permanent_parquet)
                    else:
                        permanent_parquet.unlink()
                parquet_path.rename(permanent_parquet)
            extraction_result.output_path = permanent_parquet
            return extraction_result

    def clear_all_staging(self) -> None:
        """Clear all staging data and state."""
        import shutil

        if self.bucket_path.exists():
            shutil.rmtree(self.bucket_path)
            self.bucket_path.mkdir(parents=True, exist_ok=True)
        self.state_manager.clear_all_state()

    def get_staging_path(self, source_name: str) -> Path:
        """Get the staging file path for a source."""
        return self.state_manager.get_staging_path(source_name)

    def staging_exists(self, source_name: str) -> bool:
        """Check if staging exists for a source."""
        return self.state_manager.staging_exists(source_name)


def create_el_layer(
    profile_name: str = "default",
    profiles_dir: Optional[str] = None,
    on_progress: Optional[Callable[[str], None]] = None,
    max_workers: int = 4,
) -> Optional[ELLayer]:
    """Create an EL layer instance from bucket configuration.

    Args:
        profile_name: Profile name to use
        profiles_dir: Optional profiles directory
        on_progress: Optional progress callback
        max_workers: Maximum parallel extractions

    Returns:
        ELLayer instance or None if bucket not configured
    """
    # Load bucket config for profile
    profile_buckets = load_buckets_for_profile(profile_name, profiles_dir)
    if not profile_buckets:
        return None

    # Get default bucket
    target_bucket = profile_buckets.get("target", "local")
    buckets = profile_buckets.get("buckets", {})
    bucket_config = buckets.get(target_bucket)

    if not bucket_config:
        return None

    # Get bucket path
    bucket_path = get_bucket_path(bucket_config, profile_name, profiles_dir)
    if not bucket_path:
        return None

    # Load JDBC extraction settings from computes.yml
    jdbc_config = _load_jdbc_config(profile_name, profiles_dir)

    return ELLayer(
        bucket_path=bucket_path,
        profile_name=profile_name,
        bucket_config=bucket_config,
        jdbc_config=jdbc_config,
        on_progress=on_progress,
        max_workers=max_workers,
    )


def _load_jdbc_config(
    profile_name: str,
    profiles_dir: Optional[str] = None,
) -> Dict[str, Any]:
    """Load JDBC extraction settings from computes.yml.

    Args:
        profile_name: Profile name to load settings for
        profiles_dir: Optional profiles directory

    Returns:
        Dict with jdbc_extraction settings (num_partitions, fetch_size, etc.)
    """
    try:
        profile_computes = load_computes_for_profile(profile_name, profiles_dir)
        if not profile_computes:
            return _default_jdbc_config()

        # Get target compute
        target_compute = profile_computes.get("target")
        computes = profile_computes.get("computes", {})

        if not target_compute or target_compute not in computes:
            return _default_jdbc_config()

        compute_config = computes[target_compute]
        jdbc_extraction = compute_config.get("jdbc_extraction", {})

        # Merge with defaults
        return {
            "num_partitions": jdbc_extraction.get("num_partitions", 8),
            "fetch_size": jdbc_extraction.get("fetch_size", 10000),
        }

    except Exception:
        return _default_jdbc_config()


def _default_jdbc_config() -> Dict[str, Any]:
    """Return default JDBC extraction settings."""
    return {
        "num_partitions": 8,
        "fetch_size": 10000,
    }
