"""
State manager for EL layer extraction state.

Manages extraction metadata and row hashes for incremental extraction.
State is stored in the bucket's _state/ directory.

Extended for federation to track:
- Per-source extraction state (schema, row count, timestamps)
- Row-level hashes for incremental extraction
- Model-level dependencies (which sources each model uses)
- Run-level statistics for reporting
"""

import hashlib
import json
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

try:
    import pyarrow as pa
    import pyarrow.parquet as pq

    PYARROW_AVAILABLE = True
except ImportError:
    PYARROW_AVAILABLE = False


@dataclass
class SourceState:
    """State for a single source table."""

    source_name: str
    table_name: str
    schema_hash: str
    row_count: int
    last_extracted_at: str  # ISO format datetime string
    extraction_method: str  # 'full' or 'incremental'
    pk_columns: List[str]
    columns: List[str]

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "SourceState":
        """Create from dictionary."""
        return cls(**data)


@dataclass
class ExtractionStats:
    """Statistics for a single extraction operation."""

    source_id: str  # e.g., 'postgres__orders'
    extraction_method: str  # 'full', 'incremental', 'skipped'
    row_count: int
    new_rows: int
    changed_rows: int
    deleted_rows: int
    elapsed_seconds: float
    started_at: str
    completed_at: str
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ExtractionStats":
        """Create from dictionary."""
        return cls(**data)


@dataclass
class ModelFederationState:
    """State for a federated model execution."""

    model_unique_id: str
    target: str
    compute: str
    bucket: str
    sources_used: List[str]  # List of source_ids
    execution_path: str  # 'spark_federation' or 'adapter_pushdown'
    last_run_at: str
    last_run_status: str  # 'success', 'error', 'skipped'
    elapsed_seconds: float
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ModelFederationState":
        """Create from dictionary."""
        return cls(**data)


@dataclass
class FederationRunStats:
    """Statistics for a complete federation run."""

    run_id: str
    started_at: str
    completed_at: str
    total_models: int
    federation_models: int
    pushdown_models: int
    sources_extracted: int
    total_rows_extracted: int
    errors: int
    extraction_stats: List[ExtractionStats] = field(default_factory=list)
    model_stats: List[ModelFederationState] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        result = asdict(self)
        result["extraction_stats"] = [
            s.to_dict() if hasattr(s, "to_dict") else s for s in self.extraction_stats
        ]
        result["model_stats"] = [
            s.to_dict() if hasattr(s, "to_dict") else s for s in self.model_stats
        ]
        return result

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "FederationRunStats":
        """Create from dictionary."""
        extraction_stats = [
            ExtractionStats.from_dict(s) if isinstance(s, dict) else s
            for s in data.pop("extraction_stats", [])
        ]
        model_stats = [
            ModelFederationState.from_dict(s) if isinstance(s, dict) else s
            for s in data.pop("model_stats", [])
        ]
        return cls(**data, extraction_stats=extraction_stats, model_stats=model_stats)


def compute_row_hash(row: Dict[str, Any], columns: List[str]) -> str:
    """Compute MD5 hash of entire row.

    Args:
        row: Row data as dict
        columns: Column names in consistent order

    Returns:
        MD5 hash string
    """
    # Sort columns for consistency
    values = [str(row.get(col, "")) for col in sorted(columns)]
    row_str = "|".join(values)
    return hashlib.md5(row_str.encode()).hexdigest()


def compute_schema_hash(
    columns: List[str], column_types: Optional[Dict[str, str]] = None
) -> str:
    """Compute hash of table schema for change detection.

    Args:
        columns: List of column names
        column_types: Optional dict of column name -> type string

    Returns:
        MD5 hash of schema
    """
    schema_str = "|".join(sorted(columns))
    if column_types:
        type_str = "|".join(f"{k}:{v}" for k, v in sorted(column_types.items()))
        schema_str += "|" + type_str
    return hashlib.md5(schema_str.encode()).hexdigest()


class StateManager:
    """Manage extraction state in bucket."""

    def __init__(self, bucket_path: Path):
        """Initialize state manager.

        Args:
            bucket_path: Path to the bucket directory (e.g., .dvt/staging/)
        """
        self.bucket_path = Path(bucket_path)
        self.state_path = self.bucket_path / "_state"
        self.state_path.mkdir(parents=True, exist_ok=True)

    def get_source_state(self, source_name: str) -> Optional[SourceState]:
        """Load state for a source.

        Args:
            source_name: Identifier like 'postgres__orders'

        Returns:
            SourceState if exists, None otherwise
        """
        state_file = self.state_path / f"{source_name}.state.json"
        if not state_file.exists():
            return None
        try:
            with open(state_file) as f:
                data = json.load(f)
            return SourceState.from_dict(data)
        except (json.JSONDecodeError, KeyError, TypeError):
            return None

    def save_source_state(self, state: SourceState) -> None:
        """Save state for a source.

        Args:
            state: SourceState to save
        """
        state_file = self.state_path / f"{state.source_name}.state.json"
        with open(state_file, "w") as f:
            json.dump(state.to_dict(), f, indent=2, default=str)

    def get_row_hashes(self, source_name: str) -> Dict[str, str]:
        """Load row hashes (pk -> hash) from Parquet.

        Args:
            source_name: Identifier like 'postgres__orders'

        Returns:
            Dict mapping primary key values to row hashes
        """
        if not PYARROW_AVAILABLE:
            return {}

        hash_file = self.state_path / f"{source_name}.hashes.parquet"
        if not hash_file.exists():
            return {}

        try:
            table = pq.read_table(hash_file)
            df = table.to_pandas()
            return dict(zip(df["_pk"], df["_hash"]))
        except Exception:
            return {}

    def save_row_hashes(self, source_name: str, hashes: Dict[str, str]) -> None:
        """Save row hashes to Parquet.

        Args:
            source_name: Identifier like 'postgres__orders'
            hashes: Dict mapping primary key values to row hashes
        """
        if not PYARROW_AVAILABLE:
            return

        hash_file = self.state_path / f"{source_name}.hashes.parquet"
        table = pa.table(
            {
                "_pk": list(hashes.keys()),
                "_hash": list(hashes.values()),
                "_extracted_at": [datetime.now().isoformat()] * len(hashes),
            }
        )
        pq.write_table(table, hash_file, compression="zstd")

    def clear_source_state(self, source_name: str) -> None:
        """Clear all state for a source (for --full-refresh).

        Args:
            source_name: Identifier like 'postgres__orders'
        """
        for pattern in [f"{source_name}.state.json", f"{source_name}.hashes.parquet"]:
            path = self.state_path / pattern
            if path.exists():
                path.unlink()

    def clear_all_state(self) -> None:
        """Clear all state (for dvt clean)."""
        import shutil

        if self.state_path.exists():
            shutil.rmtree(self.state_path)
            self.state_path.mkdir(parents=True, exist_ok=True)

    def staging_exists(self, source_name: str) -> bool:
        """Check if staging data exists for a source.

        Checks for Delta format first (directory with _delta_log/),
        then falls back to legacy Parquet file for backward compatibility.

        Args:
            source_name: Identifier like 'postgres__orders'

        Returns:
            True if staging data exists (Delta or legacy Parquet)
        """
        # Delta format: directory with _delta_log/ subdirectory
        delta_path = self.bucket_path / f"{source_name}.delta"
        if delta_path.is_dir() and (delta_path / "_delta_log").is_dir():
            return True
        # Legacy Parquet file (backward compat)
        parquet_path = self.bucket_path / f"{source_name}.parquet"
        return parquet_path.exists() or (
            parquet_path.is_dir() and any(parquet_path.iterdir())
        )

    def get_staging_path(self, source_name: str) -> Path:
        """Get the staging path for a source.

        Returns the Delta directory path for new extractions.
        If a legacy Parquet file exists (and no Delta version), returns
        the Parquet path for backward compatibility.

        Args:
            source_name: Identifier like 'postgres__orders'

        Returns:
            Path to the Delta staging directory (or legacy Parquet file)
        """
        delta_path = self.bucket_path / f"{source_name}.delta"
        if delta_path.is_dir() and (delta_path / "_delta_log").is_dir():
            return delta_path
        # Legacy Parquet — return it if it exists, otherwise return Delta path
        # (new extractions will create Delta)
        parquet_path = self.bucket_path / f"{source_name}.parquet"
        if parquet_path.exists():
            return parquet_path
        return delta_path

    def clear_staging(self, source_name: str) -> None:
        """Clear staging data for a source.

        Removes both Delta and legacy Parquet staging data.

        Args:
            source_name: Identifier like 'postgres__orders'
        """
        import shutil

        # Clear Delta directory
        delta_path = self.bucket_path / f"{source_name}.delta"
        if delta_path.is_dir():
            shutil.rmtree(delta_path)
        # Clear legacy Parquet file or directory
        parquet_path = self.bucket_path / f"{source_name}.parquet"
        if parquet_path.is_dir():
            shutil.rmtree(parquet_path)
        elif parquet_path.exists():
            parquet_path.unlink()

    def should_extract(
        self,
        source_name: str,
        current_schema_hash: str,
        full_refresh: bool = False,
    ) -> tuple[bool, str]:
        """Determine if extraction is needed and what type.

        Args:
            source_name: Identifier like 'postgres__orders'
            current_schema_hash: Hash of current source schema
            full_refresh: Whether --full-refresh was specified

        Returns:
            Tuple of (should_extract: bool, reason: str)
            Reason is one of: 'full_refresh', 'no_staging', 'schema_changed', 'skip', 'incremental'
        """
        if full_refresh:
            return True, "full_refresh"

        if not self.staging_exists(source_name):
            return True, "no_staging"

        state = self.get_source_state(source_name)
        if not state:
            return True, "no_state"

        if state.schema_hash != current_schema_hash:
            return True, "schema_changed"

        # Staging exists and schema unchanged - could skip or do incremental
        # For now, return skip. Caller can check row hashes for incremental.
        return False, "skip"

    def get_changed_pks(
        self,
        source_name: str,
        current_hashes: Dict[str, str],
    ) -> tuple[List[str], List[str], List[str]]:
        """Compare current row hashes with stored hashes to find changes.

        Args:
            source_name: Identifier like 'postgres__orders'
            current_hashes: Dict of pk -> hash from current source

        Returns:
            Tuple of (new_pks, changed_pks, deleted_pks)
        """
        stored_hashes = self.get_row_hashes(source_name)

        if not stored_hashes:
            # No stored hashes means all are "new"
            return list(current_hashes.keys()), [], []

        stored_keys = set(stored_hashes.keys())
        current_keys = set(current_hashes.keys())

        new_pks = list(current_keys - stored_keys)
        deleted_pks = list(stored_keys - current_keys)

        changed_pks = []
        for pk in current_keys & stored_keys:
            if current_hashes[pk] != stored_hashes[pk]:
                changed_pks.append(pk)

        return new_pks, changed_pks, deleted_pks

    # =========================================================================
    # Federation Run State
    # =========================================================================

    def get_model_state(self, model_unique_id: str) -> Optional[ModelFederationState]:
        """Load federation state for a model.

        Args:
            model_unique_id: Model's unique_id (e.g., 'model.my_project.my_model')

        Returns:
            ModelFederationState if exists, None otherwise
        """
        # Sanitize unique_id for filesystem
        safe_id = model_unique_id.replace(".", "__")
        state_file = self.state_path / f"model__{safe_id}.state.json"
        if not state_file.exists():
            return None
        try:
            with open(state_file) as f:
                data = json.load(f)
            return ModelFederationState.from_dict(data)
        except (json.JSONDecodeError, KeyError, TypeError):
            return None

    def save_model_state(self, state: ModelFederationState) -> None:
        """Save federation state for a model.

        Args:
            state: ModelFederationState to save
        """
        safe_id = state.model_unique_id.replace(".", "__")
        state_file = self.state_path / f"model__{safe_id}.state.json"
        with open(state_file, "w") as f:
            json.dump(state.to_dict(), f, indent=2, default=str)

    def clear_model_state(self, model_unique_id: str) -> None:
        """Clear federation state for a model.

        Args:
            model_unique_id: Model's unique_id
        """
        safe_id = model_unique_id.replace(".", "__")
        state_file = self.state_path / f"model__{safe_id}.state.json"
        if state_file.exists():
            state_file.unlink()

    def get_run_stats(self, run_id: str) -> Optional[FederationRunStats]:
        """Load statistics for a previous run.

        Args:
            run_id: Unique identifier for the run

        Returns:
            FederationRunStats if exists, None otherwise
        """
        stats_file = self.state_path / f"run__{run_id}.stats.json"
        if not stats_file.exists():
            return None
        try:
            with open(stats_file) as f:
                data = json.load(f)
            return FederationRunStats.from_dict(data)
        except (json.JSONDecodeError, KeyError, TypeError):
            return None

    def save_run_stats(self, stats: FederationRunStats) -> None:
        """Save statistics for a run.

        Args:
            stats: FederationRunStats to save
        """
        stats_file = self.state_path / f"run__{stats.run_id}.stats.json"
        with open(stats_file, "w") as f:
            json.dump(stats.to_dict(), f, indent=2, default=str)

    def get_latest_run_id(self) -> Optional[str]:
        """Get the most recent run ID.

        Returns:
            Run ID string if any runs exist, None otherwise
        """
        run_files = list(self.state_path.glob("run__*.stats.json"))
        if not run_files:
            return None

        # Sort by modification time, newest first
        run_files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
        # Extract run_id from filename: run__{run_id}.stats.json
        filename = run_files[0].stem  # run__{run_id}.stats
        return filename.replace("run__", "").replace(".stats", "")

    def list_run_ids(self, limit: int = 10) -> List[str]:
        """List recent run IDs.

        Args:
            limit: Maximum number of run IDs to return

        Returns:
            List of run ID strings, newest first
        """
        run_files = list(self.state_path.glob("run__*.stats.json"))
        run_files.sort(key=lambda p: p.stat().st_mtime, reverse=True)

        run_ids = []
        for f in run_files[:limit]:
            filename = f.stem
            run_id = filename.replace("run__", "").replace(".stats", "")
            run_ids.append(run_id)
        return run_ids

    def clear_old_runs(self, keep_count: int = 10) -> int:
        """Clean up old run statistics, keeping the most recent ones.

        Args:
            keep_count: Number of recent runs to keep

        Returns:
            Number of runs deleted
        """
        run_files = list(self.state_path.glob("run__*.stats.json"))
        run_files.sort(key=lambda p: p.stat().st_mtime, reverse=True)

        deleted = 0
        for f in run_files[keep_count:]:
            f.unlink()
            deleted += 1
        return deleted

    # =========================================================================
    # Extraction Statistics Helpers
    # =========================================================================

    def record_extraction(
        self,
        source_id: str,
        method: str,
        row_count: int,
        new_rows: int = 0,
        changed_rows: int = 0,
        deleted_rows: int = 0,
        elapsed: float = 0.0,
        error: Optional[str] = None,
    ) -> ExtractionStats:
        """Create an ExtractionStats record.

        Args:
            source_id: Source identifier
            method: 'full', 'incremental', or 'skipped'
            row_count: Total rows extracted
            new_rows: New rows in incremental
            changed_rows: Changed rows in incremental
            deleted_rows: Deleted rows in incremental
            elapsed: Seconds elapsed
            error: Error message if failed

        Returns:
            ExtractionStats instance
        """
        now = datetime.now().isoformat()
        return ExtractionStats(
            source_id=source_id,
            extraction_method=method,
            row_count=row_count,
            new_rows=new_rows,
            changed_rows=changed_rows,
            deleted_rows=deleted_rows,
            elapsed_seconds=elapsed,
            started_at=now,
            completed_at=now,
            error=error,
        )

    def get_sources_for_model(self, model_unique_id: str) -> List[str]:
        """Get list of source IDs that a model depends on.

        Args:
            model_unique_id: Model's unique_id

        Returns:
            List of source IDs from the last successful run
        """
        state = self.get_model_state(model_unique_id)
        if state:
            return state.sources_used
        return []

    def get_extraction_summary(self) -> Dict[str, Any]:
        """Get summary of all extracted sources.

        Returns:
            Dict with source_id -> last extraction info
        """
        summary = {}
        for state_file in self.state_path.glob("*.state.json"):
            # Skip model and run state files
            if state_file.name.startswith("model__") or state_file.name.startswith(
                "run__"
            ):
                continue
            try:
                with open(state_file) as f:
                    data = json.load(f)
                source_id = state_file.stem.replace(".state", "")
                summary[source_id] = {
                    "row_count": data.get("row_count", 0),
                    "last_extracted_at": data.get("last_extracted_at"),
                    "extraction_method": data.get("extraction_method"),
                    "schema_hash": data.get("schema_hash"),
                }
            except (json.JSONDecodeError, KeyError):
                continue
        return summary

    def get_stale_sources(self, max_age_hours: int = 24) -> List[str]:
        """Get sources that haven't been extracted recently.

        Args:
            max_age_hours: Maximum age in hours before considered stale

        Returns:
            List of stale source IDs
        """
        stale = []
        cutoff = datetime.now().timestamp() - (max_age_hours * 3600)

        for state_file in self.state_path.glob("*.state.json"):
            if state_file.name.startswith("model__") or state_file.name.startswith(
                "run__"
            ):
                continue
            try:
                with open(state_file) as f:
                    data = json.load(f)
                last_extracted = data.get("last_extracted_at")
                if last_extracted:
                    extracted_ts = datetime.fromisoformat(last_extracted).timestamp()
                    if extracted_ts < cutoff:
                        source_id = state_file.stem.replace(".state", "")
                        stale.append(source_id)
            except (json.JSONDecodeError, KeyError, ValueError):
                continue
        return stale
