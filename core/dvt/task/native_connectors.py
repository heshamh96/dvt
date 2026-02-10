# coding=utf-8
"""Native connector JAR registry for DVT.

Native connectors provide optimized data transfer using cloud storage
instead of JDBC for specific adapters (Snowflake, BigQuery, Redshift).
These are used for:
- Faster seed loading via Spark
- Optimized federation queries
- Better performance for large data transfers
"""
import hashlib
import os
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Dict, List, Optional
from urllib.request import urlretrieve

from dvt.config.user_config import get_native_connectors_dir, load_buckets_config


@dataclass
class NativeConnectorSpec:
    """Specification for a native connector JAR."""

    adapter: str
    artifact_id: str
    group_id: str
    version: str
    jar_name: str
    requires_bucket_type: str  # s3, gcs, azure
    checksum_sha256: Optional[str] = None
    dependencies: List[str] = field(default_factory=list)  # Additional JARs needed

    @property
    def maven_url(self) -> str:
        """Construct Maven Central URL for the JAR."""
        group_path = self.group_id.replace(".", "/")
        return f"https://repo1.maven.org/maven2/{group_path}/{self.artifact_id}/{self.version}/{self.jar_name}"


# Registry of native connectors for supported adapters
NATIVE_CONNECTORS: Dict[str, NativeConnectorSpec] = {
    "snowflake": NativeConnectorSpec(
        adapter="snowflake",
        artifact_id="spark-snowflake_2.12",
        group_id="net.snowflake",
        version="2.12.0-spark_3.4",
        jar_name="spark-snowflake_2.12-2.12.0-spark_3.4.jar",
        requires_bucket_type="s3",  # or gcs/azure depending on Snowflake account
        dependencies=[
            # snowflake-jdbc is already downloaded as part of JDBC drivers
        ],
    ),
    "bigquery": NativeConnectorSpec(
        adapter="bigquery",
        artifact_id="spark-bigquery-with-dependencies_2.12",
        group_id="com.google.cloud.spark",
        version="0.32.2",
        jar_name="spark-bigquery-with-dependencies_2.12-0.32.2.jar",
        requires_bucket_type="gcs",
        dependencies=[],  # Self-contained uber JAR
    ),
    "redshift": NativeConnectorSpec(
        adapter="redshift",
        artifact_id="spark-redshift_2.12",
        group_id="io.github.spark-redshift-community",
        version="6.0.0",
        jar_name="spark-redshift_2.12-6.0.0.jar",
        requires_bucket_type="s3",
        dependencies=[
            # These may be needed for S3 access - check if Spark has them bundled
            # "hadoop-aws-3.3.4.jar",
            # "aws-java-sdk-bundle-1.12.262.jar",
        ],
    ),
}


def get_native_connector(adapter: str) -> Optional[NativeConnectorSpec]:
    """Get native connector spec for an adapter, if available."""
    return NATIVE_CONNECTORS.get(adapter.lower())


def list_native_connectors() -> List[str]:
    """List all adapters with native connector support."""
    return list(NATIVE_CONNECTORS.keys())


def get_native_connectors_for_adapters(adapter_types: List[str]) -> List[NativeConnectorSpec]:
    """Get native connector specs for a list of adapter types."""
    connectors = []
    for adapter in adapter_types:
        spec = get_native_connector(adapter)
        if spec:
            connectors.append(spec)
    return connectors


def _verify_checksum(file_path: Path, expected_sha256: str) -> bool:
    """Verify SHA256 checksum of a file."""
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest() == expected_sha256


def download_native_connector_jars(
    connectors: List[NativeConnectorSpec],
    target_dir: Path,
    on_event: Optional[Callable[[str], None]] = None,
    buckets_config: Optional[Dict] = None,
) -> Dict[str, bool]:
    """Download native connector JARs for the specified connectors.

    Args:
        connectors: List of native connector specs to download
        target_dir: Directory to store JARs (e.g. ~/.dvt/lib/native/)
        on_event: Optional callback for progress messages
        buckets_config: Parsed buckets.yml config for bucket type validation

    Returns:
        Dict mapping adapter name -> success status
    """

    def log(msg: str) -> None:
        if on_event:
            on_event(msg)
        else:
            sys.stderr.write(msg + "\n")

    target_dir = Path(target_dir)
    target_dir.mkdir(parents=True, exist_ok=True)

    results = {}

    for spec in connectors:
        jar_path = target_dir / spec.jar_name

        # Check if bucket type matches (warn if mismatch)
        if buckets_config:
            configured_bucket_type = _get_bucket_type_for_adapter(spec.adapter, buckets_config)
            if configured_bucket_type and configured_bucket_type != spec.requires_bucket_type:
                log(
                    f"  ⚠️  {spec.adapter}: Bucket type '{configured_bucket_type}' configured, "
                    f"but native connector requires '{spec.requires_bucket_type}'"
                )

        # Skip if already downloaded
        if jar_path.exists():
            log(f"  ✓ {spec.adapter}: {spec.jar_name} (already exists)")
            results[spec.adapter] = True
            continue

        # Download the JAR
        log(f"  📥 {spec.adapter}: Downloading {spec.jar_name}...")
        try:
            urlretrieve(spec.maven_url, jar_path)

            # Verify checksum if available
            if spec.checksum_sha256:
                if not _verify_checksum(jar_path, spec.checksum_sha256):
                    log(f"  ❌ {spec.adapter}: Checksum mismatch for {spec.jar_name}")
                    os.remove(jar_path)
                    results[spec.adapter] = False
                    continue

            log(f"  ✓ {spec.adapter}: {spec.jar_name}")
            results[spec.adapter] = True

        except Exception as e:
            log(f"  ❌ {spec.adapter}: Failed to download {spec.jar_name}: {e}")
            results[spec.adapter] = False

    return results


def _get_bucket_type_for_adapter(adapter: str, config: Dict) -> Optional[str]:
    """Get configured bucket type for an adapter from buckets.yml."""
    # Check adapter-specific config first
    adapters_config = config.get("adapters", {})
    if adapters_config and adapter in adapters_config:
        adapter_cfg = adapters_config[adapter]
        if isinstance(adapter_cfg, dict):
            return adapter_cfg.get("type")

    # Fall back to default
    default = config.get("default", {})
    if isinstance(default, dict):
        return default.get("type")

    return None


def sync_native_connectors(
    adapter_types: List[str],
    profiles_dir: Optional[str] = None,
    on_event: Optional[Callable[[str], None]] = None,
) -> Dict[str, bool]:
    """Sync native connector JARs for the given adapter types.

    Args:
        adapter_types: List of adapter types (e.g., ['snowflake', 'bigquery'])
        profiles_dir: DVT profiles directory (default ~/.dvt)
        on_event: Optional callback for progress messages

    Returns:
        Dict mapping adapter name -> success status
    """
    connectors = get_native_connectors_for_adapters(adapter_types)

    if not connectors:
        return {}

    native_dir = get_native_connectors_dir(profiles_dir)
    buckets_config = load_buckets_config(profiles_dir)

    return download_native_connector_jars(
        connectors,
        native_dir,
        on_event=on_event,
        buckets_config=buckets_config,
    )
