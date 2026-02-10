# DVT CLI Command: `dvt sync`

## Phase: 1 (Foundation)
## Type: Delta (modifications to existing implementation)

---

## Current Implementation Summary

**File**: `core/dvt/task/jdbc_drivers.py` (driver registry)
**CLI**: `core/dvt/cli/main.py` (sync command)

The current `dvt sync` command:
1. Reads `profiles.yml` to identify configured adapters
2. Downloads required JDBC drivers from Maven Central
3. Stores drivers in `~/.dvt/lib/jdbc/`
4. Validates driver checksums

**Supported Adapters** (24 total - see `jdbc_drivers.py`):
- PostgreSQL, MySQL, SQL Server, Oracle
- Snowflake, BigQuery, Redshift, Databricks
- Trino, Presto, Athena, Spark
- And more...

---

## Required Changes

### Delta: Add Native Connector JAR Downloads

Extend sync to also download native connector JARs for Snowflake, BigQuery, and Redshift when those adapters are configured.

### Reference Document
- `connection_optimizations.md` - Section: "Native Connector JARs"

---

## Implementation Details

### 1. Native Connector Registry

**File**: `core/dvt/task/native_connectors.py` (new file)

```python
"""Native connector JAR registry for DVT.

Native connectors provide optimized data transfer using cloud storage
instead of JDBC for specific adapters (Snowflake, BigQuery, Redshift).
"""

from dataclasses import dataclass
from typing import Dict, List, Optional

@dataclass
class NativeConnectorSpec:
    """Specification for a native connector JAR."""
    adapter: str
    artifact_id: str
    group_id: str
    version: str
    jar_name: str
    checksum_sha256: str
    requires_bucket_type: str  # s3, gcs, azure
    dependencies: List[str]  # Additional JARs needed


NATIVE_CONNECTORS: Dict[str, NativeConnectorSpec] = {
    "snowflake": NativeConnectorSpec(
        adapter="snowflake",
        artifact_id="spark-snowflake_2.12",
        group_id="net.snowflake",
        version="2.12.0-spark_3.4",
        jar_name="spark-snowflake_2.12-2.12.0-spark_3.4.jar",
        checksum_sha256="abc123...",  # TODO: actual checksum
        requires_bucket_type="s3",  # or gcs/azure
        dependencies=[
            "snowflake-jdbc-3.14.3.jar",  # Already in JDBC drivers
        ],
    ),
    "bigquery": NativeConnectorSpec(
        adapter="bigquery",
        artifact_id="spark-bigquery-with-dependencies_2.12",
        group_id="com.google.cloud.spark",
        version="0.32.2",
        jar_name="spark-bigquery-with-dependencies_2.12-0.32.2.jar",
        checksum_sha256="def456...",  # TODO: actual checksum
        requires_bucket_type="gcs",
        dependencies=[],  # Self-contained uber JAR
    ),
    "redshift": NativeConnectorSpec(
        adapter="redshift",
        artifact_id="spark-redshift_2.12",
        group_id="io.github.spark-redshift-community",
        version="6.0.0",
        jar_name="spark-redshift_2.12-6.0.0.jar",
        checksum_sha256="ghi789...",  # TODO: actual checksum
        requires_bucket_type="s3",
        dependencies=[
            "redshift-jdbc42-2.1.0.12.jar",  # Already in JDBC drivers
            "hadoop-aws-3.3.4.jar",
            "aws-java-sdk-bundle-1.12.262.jar",
        ],
    ),
}


def get_native_connector(adapter: str) -> Optional[NativeConnectorSpec]:
    """Get native connector spec for an adapter, if available."""
    return NATIVE_CONNECTORS.get(adapter.lower())


def list_native_connectors() -> List[str]:
    """List all adapters with native connector support."""
    return list(NATIVE_CONNECTORS.keys())
```

### 2. Extended Sync Logic

**File**: `core/dvt/task/sync.py` (modifications)

```python
from dvt.task.native_connectors import (
    get_native_connector,
    NativeConnectorSpec,
)
from dvt.config.buckets import load_buckets_config


def sync_native_connectors(
    adapters: List[str],
    lib_dir: str,
    buckets_config: Optional[dict],
) -> Dict[str, bool]:
    """Download native connector JARs for configured adapters.
    
    Args:
        adapters: List of adapter names from profiles.yml
        lib_dir: Directory to store JARs (~/.dvt/lib/native/)
        buckets_config: Parsed buckets.yml config
        
    Returns:
        Dict mapping adapter -> success status
    """
    native_dir = os.path.join(lib_dir, "native")
    os.makedirs(native_dir, exist_ok=True)
    
    results = {}
    
    for adapter in adapters:
        spec = get_native_connector(adapter)
        if spec is None:
            continue  # No native connector for this adapter
            
        # Check if bucket is configured for this adapter
        if buckets_config:
            bucket_type = _get_bucket_type_for_adapter(adapter, buckets_config)
            if bucket_type and bucket_type != spec.requires_bucket_type:
                fire_event(NativeConnectorBucketMismatch(
                    adapter=adapter,
                    required=spec.requires_bucket_type,
                    configured=bucket_type,
                ))
        
        # Download main JAR
        success = _download_native_jar(spec, native_dir)
        
        # Download dependencies
        for dep in spec.dependencies:
            if not _jar_exists(dep, native_dir) and not _jar_exists(dep, os.path.join(lib_dir, "jdbc")):
                _download_dependency(dep, native_dir)
        
        results[adapter] = success
        
    return results


def _get_bucket_type_for_adapter(adapter: str, config: dict) -> Optional[str]:
    """Get configured bucket type for an adapter."""
    # Check adapter-specific config first
    adapters_config = config.get("adapters", {})
    if adapter in adapters_config:
        return adapters_config[adapter].get("type")
    
    # Fall back to default
    default = config.get("default", {})
    return default.get("type")
```

### 3. CLI Integration

**File**: `core/dvt/cli/main.py`

Modify the sync command to include native connectors:

```python
@click.command("sync")
@click.option("--jdbc/--no-jdbc", default=True, help="Sync JDBC drivers")
@click.option("--native/--no-native", default=True, help="Sync native connectors")
@click.pass_context
def sync(ctx, jdbc: bool, native: bool):
    """Sync JDBC drivers and native connectors for configured adapters."""
    
    profiles = load_profiles()
    adapters = get_configured_adapters(profiles)
    
    if jdbc:
        click.echo("Syncing JDBC drivers...")
        sync_jdbc_drivers(adapters)
    
    if native:
        click.echo("Syncing native connectors...")
        buckets_config = load_buckets_config()
        results = sync_native_connectors(adapters, get_lib_dir(), buckets_config)
        
        for adapter, success in results.items():
            if success:
                click.echo(f"  {adapter}: OK")
            else:
                click.echo(f"  {adapter}: FAILED", err=True)
```

### 4. New Events

**File**: `core/dvt/events/types.py`

```python
@dataclass
class NativeConnectorDownloaded(InfoLevel):
    adapter: str
    jar_name: str
    path: str
    
    def message(self) -> str:
        return f"Downloaded native connector for {self.adapter}: {self.jar_name}"


@dataclass
class NativeConnectorBucketMismatch(WarnLevel):
    adapter: str
    required: str
    configured: str
    
    def message(self) -> str:
        return (
            f"Native connector for {self.adapter} requires {self.required} bucket "
            f"but {self.configured} is configured. Native connector may not work."
        )


@dataclass
class NativeConnectorSkipped(DebugLevel):
    adapter: str
    reason: str
    
    def message(self) -> str:
        return f"Skipped native connector for {self.adapter}: {self.reason}"
```

---

## CLI Output Changes

Before:
```
$ dvt sync
Syncing JDBC drivers...
  postgresql: OK
  snowflake: OK
  bigquery: OK
Done!
```

After:
```
$ dvt sync
Syncing JDBC drivers...
  postgresql: OK
  snowflake: OK
  bigquery: OK
Syncing native connectors...
  snowflake: OK (spark-snowflake_2.12-2.12.0-spark_3.4.jar)
  bigquery: OK (spark-bigquery-with-dependencies_2.12-0.32.2.jar)
Done!
```

---

## Directory Structure

```
~/.dvt/
├── lib/
│   ├── jdbc/                    # JDBC drivers (existing)
│   │   ├── postgresql-42.6.0.jar
│   │   ├── snowflake-jdbc-3.14.3.jar
│   │   └── ...
│   └── native/                  # Native connectors (NEW)
│       ├── spark-snowflake_2.12-2.12.0-spark_3.4.jar
│       ├── spark-bigquery-with-dependencies_2.12-0.32.2.jar
│       ├── spark-redshift_2.12-6.0.0.jar
│       ├── hadoop-aws-3.3.4.jar
│       └── aws-java-sdk-bundle-1.12.262.jar
```

---

## Testing Requirements

### Unit Tests

**File**: `tests/unit/test_native_connectors.py`

```python
def test_get_native_connector_snowflake():
    spec = get_native_connector("snowflake")
    assert spec is not None
    assert spec.adapter == "snowflake"
    assert spec.requires_bucket_type == "s3"


def test_get_native_connector_unknown():
    spec = get_native_connector("postgres")
    assert spec is None  # No native connector for postgres


def test_sync_native_connectors_with_matching_bucket(tmp_path, mocker):
    """Test sync succeeds when bucket type matches."""
    mocker.patch("dvt.task.sync._download_native_jar", return_value=True)
    
    buckets_config = {
        "default": {"type": "s3"},
    }
    
    results = sync_native_connectors(
        ["snowflake"],
        str(tmp_path),
        buckets_config,
    )
    
    assert results["snowflake"] is True
```

### Integration Tests

**File**: `tests/functional/test_sync_command.py`

```python
def test_sync_downloads_native_connectors(project, dbt_project_dir):
    """Test dvt sync downloads native connector JARs."""
    # Setup: Configure snowflake in profiles.yml
    
    result = run_dbt(["sync"])
    
    assert result.success
    assert (Path.home() / ".dvt/lib/native/spark-snowflake_2.12-2.12.0-spark_3.4.jar").exists()
```

---

## Dependencies

- `01_init.md` - buckets.yml must exist for bucket type validation

## Blocked By

- `01_init.md`

## Blocks

- `05_seed.md` - seed uses native connectors for optimized loading
- `06_run.md` - run uses native connectors for federation

---

## Implementation Checklist

- [ ] Create `core/dvt/task/native_connectors.py` with registry
- [ ] Add `sync_native_connectors()` function to sync task
- [ ] Update CLI to include `--native/--no-native` flag
- [ ] Add new event types
- [ ] Create `~/.dvt/lib/native/` directory structure
- [ ] Add unit tests for native connector registry
- [ ] Add integration tests for sync command
- [ ] Update checksums with actual values
- [ ] Document native connector requirements
