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
import dataclasses
import threading
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
    extraction_sql: Optional[str] = (
        None  # Pre-built extraction SQL from FederationOptimizer
    )


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

    # Class-level per-source locks for thread-safe concurrent extraction.
    # When multiple models run in parallel and reference the same source,
    # these locks serialize extraction of each source so only one thread
    # does the should_extract() check + extract + convert-to-Delta sequence.
    # Must be class-level because each model creates its own ELLayer instance.
    _source_locks: Dict[str, threading.Lock] = {}
    _source_locks_guard: threading.Lock = threading.Lock()

    # Track sources already extracted in this run (for full_refresh dedup).
    # When --full-refresh is used, the first thread clears and re-extracts.
    # Subsequent threads see the source is already refreshed and skip the
    # clear+extract, avoiding deletion of staging that other models need.
    _refreshed_sources: set = set()
    _refreshed_sources_guard: threading.Lock = threading.Lock()

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

    @classmethod
    def _get_source_lock(cls, source_name: str) -> threading.Lock:
        """Get or create a per-source lock for thread-safe extraction.

        Uses double-checked locking: fast path (no guard) for existing locks,
        guard only for creating new ones.
        """
        lock = cls._source_locks.get(source_name)
        if lock is not None:
            return lock
        with cls._source_locks_guard:
            # Re-check under guard (another thread may have created it)
            if source_name not in cls._source_locks:
                cls._source_locks[source_name] = threading.Lock()
            return cls._source_locks[source_name]

    def _extract_source(
        self,
        source: SourceConfig,
        full_refresh: bool,
    ) -> ExtractionResult:
        """Extract a single source (thread-safe via per-source lock).

        Acquires a per-source lock before doing anything, so concurrent
        threads extracting the same source are serialized. This prevents:
        - TOCTOU races on should_extract() check
        - Multiple threads writing to the same tmp Parquet path
        - FileNotFoundError on _convert_to_delta() rename

        Args:
            source: Source configuration
            full_refresh: If True, clear staging and do full extraction

        Returns:
            ExtractionResult
        """
        source_lock = self._get_source_lock(source.source_name)
        with source_lock:
            return self._extract_source_locked(source, full_refresh)

    def _extract_source_locked(
        self,
        source: SourceConfig,
        full_refresh: bool,
    ) -> ExtractionResult:
        """Extract a single source (caller holds the per-source lock).

        Args:
            source: Source configuration
            full_refresh: If True, clear staging and do full extraction

        Returns:
            ExtractionResult
        """
        # Handle full refresh - clear staging and state.
        # Only clear once per source per run to avoid deleting staging
        # that another thread's model is about to read.
        if full_refresh:
            already_refreshed = source.source_name in self._refreshed_sources
            if not already_refreshed:
                self.state_manager.clear_source_state(source.source_name)
                self.state_manager.clear_staging(source.source_name)
                # Mark as refreshed AFTER clear so only one thread clears
                with self._refreshed_sources_guard:
                    self._refreshed_sources.add(source.source_name)

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

        # Determine if another model already extracted this source in the
        # current run (relevant for --full-refresh dedup).
        already_refreshed_this_run = (
            full_refresh and source.source_name in self._refreshed_sources
        )

        # Union-of-columns check applies in two scenarios:
        # 1. Normal skip (staging exists, schema unchanged)
        # 2. Full-refresh when source was already extracted by another model
        #    in this run — we must NOT blindly overwrite with a smaller
        #    column set; instead, union the columns and re-extract.
        needs_union_check = (not should_extract and reason == "skip") or (
            already_refreshed_this_run
            and self.state_manager.staging_exists(source.source_name)
        )

        if needs_union_check:
            # Staging exists — check if column pushdown requires re-extraction.
            if source.columns is not None:
                existing_state = self.state_manager.get_source_state(source.source_name)
                existing_cols = list(existing_state.columns) if existing_state else []
                requested_cols = list(source.columns)

                # Case-insensitive comparison: optimizer may lowercase names
                # while state stores real DB-cased names.
                existing_lower = {c.lower() for c in existing_cols}
                requested_lower = {c.lower() for c in requested_cols}

                if not requested_lower.issubset(existing_lower):
                    # Staging is missing columns this model needs.
                    # Re-extract with the union of existing + requested columns.
                    # Use existing (real-cased) names as the base, then add any
                    # genuinely new columns from the requested set.
                    union_cols = list(existing_cols)  # Start with real-cased existing
                    for col in requested_cols:
                        if col.lower() not in existing_lower:
                            union_cols.append(col)
                    union_cols.sort(key=lambda c: c.lower())
                    self._log(
                        f"  Union-of-columns: {source.source_name} needs "
                        f"{len(requested_lower - existing_lower)} additional columns, "
                        f"re-extracting with {len(union_cols)} total"
                    )
                    # Update source columns and rebuild extraction_sql with union
                    source = dataclasses.replace(
                        source,
                        columns=union_cols,
                        extraction_sql=None,  # Force rebuild from parts
                    )
                    # Fall through to extraction below
                else:
                    # Staging has all columns we need
                    return ExtractionResult(
                        success=True,
                        source_name=source.source_name,
                        extraction_method="skip",
                    )
            else:
                # SELECT * — check whether staging actually has all source columns.
                # A previous model may have extracted only a subset (column pushdown),
                # so we cannot assume staging covers everything.
                existing_state = self.state_manager.get_source_state(source.source_name)
                existing_cols = list(existing_state.columns) if existing_state else []
                all_source_cols_lower = {c.lower() for c in columns}
                existing_lower = {c.lower() for c in existing_cols}

                if not all_source_cols_lower.issubset(existing_lower):
                    # Staging is missing columns — re-extract with all source columns
                    self._log(
                        f"  Union-of-columns: {source.source_name} SELECT * needs "
                        f"{len(all_source_cols_lower - existing_lower)} additional columns, "
                        f"re-extracting with all {len(columns)} columns"
                    )
                    # Fall through to extraction below (columns=None → SELECT *)
                else:
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
            extraction_sql=source.extraction_sql,
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
            # Save state — record the columns that were ACTUALLY extracted,
            # not the full source table schema. This is critical for the
            # union-of-columns check: when a second model needs different
            # columns from the same source, we must know what's already in
            # the Delta staging to decide whether re-extraction is needed.
            actually_extracted = resolved_columns if resolved_columns else columns
            state = SourceState(
                source_name=source.source_name,
                table_name=source.table,
                schema_hash=current_schema_hash,
                row_count=result.row_count,
                last_extracted_at=datetime.now().isoformat(),
                extraction_method=result.extraction_method,
                pk_columns=pk_columns or [],
                columns=actually_extracted,
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
            # Enable column mapping to support column names with spaces/special chars.
            # overwriteSchema=true ensures schema changes (e.g., different column
            # sets from re-extraction) are accepted rather than rejected.
            writer = (
                df.write.format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .option("delta.columnMapping.mode", "name")
                .option("delta.minReaderVersion", "2")
                .option("delta.minWriterVersion", "5")
            )

            # Apply user Delta table properties from computes.yml delta: section
            try:
                from dvt.federation.spark_manager import SparkManager

                if SparkManager.is_initialized():
                    for (
                        key,
                        value,
                    ) in SparkManager.get_instance().delta_table_properties.items():
                        writer = writer.option(key, value)
            except Exception:
                pass  # SparkManager not available; skip user config

            writer.save(str(delta_path))

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
