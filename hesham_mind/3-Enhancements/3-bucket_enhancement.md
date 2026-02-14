# Bucket Enhancement Plan

## Overview

This document outlines the plan for enhancing DVT's bucket configuration and staging system, including:
1. Enhanced `buckets.yml` template via `dvt init`
2. Local filesystem as default staging (zero dependencies)
3. Support for multiple bucket types (filesystem, HDFS, S3, GCS, Azure)
4. `dvt clean` command enhancements for staging cleanup
5. Cloud storage dependencies installed via `dvt sync` (like pyspark)

---

## Goals

1. **Zero-dependency default** - Local filesystem staging works out of the box
2. **Cloud-optional** - S3/GCS/Azure installed via `dvt sync` when configured
3. **HDFS native support** - Leverage Spark's built-in HDFS support for Cloudera
4. **Simple cleanup** - `dvt clean` handles both dbt artifacts and DVT staging

---

## Architecture

### Data Flow with Bucket Staging

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Source Databases                            │
│  (Postgres, MySQL, Oracle, Snowflake, etc.)                        │
└───────────────────────────────┬─────────────────────────────────────┘
                                │
                                │ DVT EL Layer
                                │ (Native bulk export via dbt adapters)
                                ▼
┌─────────────────────────────────────────────────────────────────────┐
│                         Bucket (Staging)                            │
│                                                                      │
│  Options:                                                            │
│  - filesystem: .dvt/staging/ (default, zero deps)                   │
│  - hdfs: hdfs://namenode/path/ (Spark built-in)                     │
│  - s3: s3a://bucket/prefix/ (boto3 via dvt sync)                    │
│  - gcs: gs://bucket/prefix/ (google-cloud-storage via dvt sync)    │
│  - azure: abfss://container@account/ (azure-storage-blob via sync) │
│                                                                      │
│  Format: Parquet (compressed, columnar)                             │
└───────────────────────────────┬─────────────────────────────────────┘
                                │
                                │ Spark reads Parquet
                                ▼
┌─────────────────────────────────────────────────────────────────────┐
│                         Spark Compute                               │
│                   (from computes.yml)                               │
│                                                                      │
│  - Joins across sources                                             │
│  - Transformations                                                   │
│  - Aggregations                                                      │
└───────────────────────────────┬─────────────────────────────────────┘
                                │
                                │ Write to target
                                ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      Target Database                                │
│               (Snowflake, BigQuery, Databricks, etc.)               │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Bucket Types

| Type | URI Format | Dependencies | Installed Via |
|------|-----------|--------------|---------------|
| `filesystem` | Local path | None | N/A |
| `hdfs` | `hdfs://namenode/path/` | None | Spark built-in |
| `s3` | `s3a://bucket/prefix/` | `boto3` | `dvt sync` |
| `gcs` | `gs://bucket/prefix/` | `google-cloud-storage` | `dvt sync` |
| `azure` | `abfss://container@account/path/` | `azure-storage-blob` | `dvt sync` |

---

## Path Resolution Logic

| Scenario | Behavior |
|----------|----------|
| `type: filesystem` with no `path` | Resolve to `<project_root>/.dvt/staging/` at runtime |
| `type: filesystem` with `path: /custom/path` | Use the user-specified absolute path |
| `type: hdfs/s3/gcs/azure` | Use the configured URI/bucket (must be specified) |

---

## Files to Modify/Create

| File | Action | Description |
|------|--------|-------------|
| `core/dvt/config/user_config.py` | Modify | Update `create_default_buckets_yml()` with new template |
| `core/dvt/task/clean.py` | Create | New `CleanTask` with bucket cleaning logic |
| `core/dvt/task/sync.py` | Modify | Install cloud storage deps based on buckets.yml |
| `core/dvt/cli/main.py` | Modify | Add `--bucket` and `--older-than` flags to clean command |
| `core/dvt/cli/flags.py` | Modify | Add new flags for clean command |

---

## Implementation Details

### 1. Enhanced `buckets.yml` Template

**File:** `core/dvt/config/user_config.py`

**Function:** `create_default_buckets_yml()`

**New Template:**

```yaml
# DVT Bucket Configuration
# ========================
# Buckets are staging areas for data during federation.
# DVT extracts source data to buckets, then Spark reads from buckets for compute.
#
# Structure: Each profile has a target (default bucket) and bucket definitions.
# Local filesystem is the default - no extra dependencies required.
#
# Cleanup: Staging files are kept until manual cleanup.
# Use 'dvt clean --bucket <name>' to clean specific buckets.
# Use 'dvt clean --older-than 24h' to clean old files.
#
# Cloud storage dependencies (boto3, google-cloud-storage, azure-storage-blob)
# are automatically installed by 'dvt sync' when cloud buckets are configured.
#
# See docs: https://github.com/heshamh96/dvt

default:
  target: local
  buckets:
    # === Local Filesystem (Default) ===
    # No dependencies required. Best for development.
    local:
      type: filesystem
      # Resolved at runtime to project's .dvt/staging/ directory
      # Override with absolute path if needed:
      # path: /custom/path/to/staging

    # === HDFS (Cloudera/Hadoop) ===
    # No extra dependencies - Spark has built-in HDFS support.
    # Use for on-premise Hadoop/Cloudera environments.
    # hdfs_staging:
    #   type: hdfs
    #   path: "hdfs://namenode:8020/user/dvt/staging"
    #   # For HA NameNode:
    #   # path: "hdfs://mycluster/user/dvt/staging"

    # === Amazon S3 ===
    # Dependencies installed automatically by 'dvt sync'.
    # s3_staging:
    #   type: s3
    #   bucket: my-dvt-staging-bucket
    #   prefix: staging/
    #   region: us-east-1
    #   # Authentication options (if not using IAM role / instance profile):
    #   # access_key_id: ${AWS_ACCESS_KEY_ID}
    #   # secret_access_key: ${AWS_SECRET_ACCESS_KEY}

    # === Google Cloud Storage ===
    # Dependencies installed automatically by 'dvt sync'.
    # gcs_staging:
    #   type: gcs
    #   bucket: my-dvt-staging-bucket
    #   prefix: staging/
    #   project: my-gcp-project
    #   # Authentication options (if not using default credentials):
    #   # credentials_path: /path/to/service-account.json

    # === Azure Blob Storage / ADLS Gen2 ===
    # Dependencies installed automatically by 'dvt sync'.
    # azure_staging:
    #   type: azure
    #   container: dvt-staging
    #   storage_account: mystorageaccount
    #   prefix: staging/
    #   # Authentication options:
    #   # account_key: ${AZURE_STORAGE_KEY}
    #   # Or use Azure AD / managed identity (no key needed)
```

---

### 2. `dvt sync` Enhancement for Cloud Storage Dependencies

**File:** `core/dvt/task/sync.py`

**Logic:** When running `dvt sync`, check `buckets.yml` for cloud bucket types and install the required dependencies.

```python
# Bucket type to dependency mapping
BUCKET_DEPENDENCIES = {
    "s3": "boto3",
    "gcs": "google-cloud-storage", 
    "azure": "azure-storage-blob",
    # filesystem and hdfs need no extra deps
}

def _get_bucket_dependencies(buckets_config: Dict) -> List[str]:
    """
    Determine which cloud storage dependencies are needed based on buckets.yml.
    
    Returns list of pip package names to install.
    """
    deps = set()
    
    for profile_data in buckets_config.values():
        if not isinstance(profile_data, dict):
            continue
        buckets = profile_data.get("buckets", {})
        for bucket_config in buckets.values():
            bucket_type = bucket_config.get("type", "filesystem")
            if bucket_type in BUCKET_DEPENDENCIES:
                deps.add(BUCKET_DEPENDENCIES[bucket_type])
    
    return list(deps)
```

**Integration:** Call this during `dvt sync` alongside adapter and pyspark installation.

---

### 3. `dvt clean` Command Enhancement

**File:** `core/dvt/task/clean.py` (new file)

**Behavior:**

| Command | Behavior |
|---------|----------|
| `dvt clean` | Run dbt clean + clean all DVT staging buckets + hash state (silently) |
| `dvt clean --bucket local` | Clean only the specified bucket + its hash state (skip dbt clean) |
| `dvt clean --older-than 24h` | Only clean files older than duration (skip dbt clean) |
| `dvt clean --bucket local --older-than 24h` | Clean specific bucket, only old files |

**Note:** Cleaning a bucket also cleans the associated hash state in `_state/` directory. This ensures consecutive runs don't use stale incremental state.

**CLI Flags:**

| Flag | Type | Description |
|------|------|-------------|
| `--bucket` | `str` | Clean only the specified bucket name |
| `--older-than` | `str` | Only clean files older than duration (e.g., `24h`, `7d`) |

**Duration format:** `Nh` (hours) or `Nd` (days)

**Implementation:**

```python
# core/dvt/task/clean.py

import os
import shutil
import time
from pathlib import Path
from typing import Optional, Dict, Any
from datetime import datetime, timedelta
import re

from dvt.task.base import BaseTask
from dvt.config.user_config import load_buckets_config, get_project_root


def parse_duration(duration_str: str) -> timedelta:
    """
    Parse duration string like '24h' or '7d' into timedelta.
    
    Args:
        duration_str: Duration string (e.g., '24h', '7d', '48h')
    
    Returns:
        timedelta object
    
    Raises:
        ValueError: If format is invalid
    """
    match = re.match(r'^(\d+)(h|d)$', duration_str.lower())
    if not match:
        raise ValueError(
            f"Invalid duration format: '{duration_str}'. "
            f"Expected format: Nh (hours) or Nd (days), e.g., '24h', '7d'"
        )
    
    value = int(match.group(1))
    unit = match.group(2)
    
    if unit == 'h':
        return timedelta(hours=value)
    else:  # 'd'
        return timedelta(days=value)


class CleanTask(BaseTask):
    """Clean dbt artifacts and DVT staging files."""
    
    def __init__(self, args):
        super().__init__(args)
        self.bucket_name: Optional[str] = getattr(args, 'bucket', None)
        self.older_than: Optional[str] = getattr(args, 'older_than', None)
    
    def run(self):
        # If specific bucket or older_than is specified, only clean staging
        if self.bucket_name or self.older_than:
            return self._clean_staging()
        
        # Otherwise, run dbt clean + clean all staging
        self._run_dbt_clean()
        self._clean_staging()
    
    def _run_dbt_clean(self):
        """Run dbt's clean command."""
        from dbt.task.clean import CleanTask as DbtCleanTask
        dbt_clean = DbtCleanTask(self.args)
        dbt_clean.run()
    
    def _clean_staging(self):
        """Clean DVT staging buckets."""
        buckets_config = load_buckets_config()
        if not buckets_config:
            return
        
        cutoff_time = None
        if self.older_than:
            delta = parse_duration(self.older_than)
            cutoff_time = datetime.now() - delta
        
        for bucket_name, bucket_config in buckets_config.items():
            # Skip if specific bucket requested and this isn't it
            if self.bucket_name and bucket_name != self.bucket_name:
                continue
            
            bucket_type = bucket_config.get('type', 'filesystem')
            
            if bucket_type == 'filesystem':
                self._clean_filesystem_bucket(bucket_name, bucket_config, cutoff_time)
            elif bucket_type == 'hdfs':
                self._clean_hdfs_bucket(bucket_name, bucket_config, cutoff_time)
            elif bucket_type == 's3':
                self._clean_s3_bucket(bucket_name, bucket_config, cutoff_time)
            elif bucket_type == 'gcs':
                self._clean_gcs_bucket(bucket_name, bucket_config, cutoff_time)
            elif bucket_type == 'azure':
                self._clean_azure_bucket(bucket_name, bucket_config, cutoff_time)
    
    def _clean_filesystem_bucket(
        self, 
        name: str, 
        config: Dict[str, Any], 
        cutoff_time: Optional[datetime]
    ):
        """Clean local filesystem staging including hash state."""
        path = config.get('path')
        if not path:
            # Default to project's .dvt/staging/
            project_root = get_project_root()
            path = project_root / '.dvt' / 'staging'
        else:
            path = Path(path)
        
        if not path.exists():
            return
        
        if cutoff_time:
            # Only delete files older than cutoff
            self._delete_old_files(path, cutoff_time)
        else:
            # Delete everything including _state/ directory
            shutil.rmtree(path)
            path.mkdir(parents=True, exist_ok=True)
        
        # Also clean _state/ directory (hash state for incremental extraction)
        state_path = path / '_state'
        if state_path.exists() and not cutoff_time:
            shutil.rmtree(state_path)
            state_path.mkdir(parents=True, exist_ok=True)
        
        print(f"Cleaned staging bucket: {name}")
    
    def _delete_old_files(self, path: Path, cutoff_time: datetime):
        """Delete files older than cutoff_time."""
        cutoff_timestamp = cutoff_time.timestamp()
        
        for file_path in path.rglob('*'):
            if file_path.is_file():
                if file_path.stat().st_mtime < cutoff_timestamp:
                    file_path.unlink()
        
        # Clean up empty directories
        for dir_path in sorted(path.rglob('*'), reverse=True):
            if dir_path.is_dir() and not any(dir_path.iterdir()):
                dir_path.rmdir()
    
    def _clean_hdfs_bucket(
        self, 
        name: str, 
        config: Dict[str, Any], 
        cutoff_time: Optional[datetime]
    ):
        """Clean HDFS staging including hash state using Spark's Hadoop filesystem API."""
        from pyspark.sql import SparkSession
        
        spark = SparkSession.builder.getOrCreate()
        hadoop_conf = spark._jsc.hadoopConfiguration()
        
        path = config.get('path')
        if not path:
            raise ValueError(f"HDFS bucket '{name}' requires 'path' configuration")
        
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
            spark._jvm.java.net.URI(path),
            hadoop_conf
        )
        hdfs_path = spark._jvm.org.apache.hadoop.fs.Path(path)
        
        if cutoff_time:
            cutoff_ms = int(cutoff_time.timestamp() * 1000)
            self._delete_old_hdfs_files(fs, hdfs_path, cutoff_ms)
        else:
            # Delete everything including _state/ directory
            if fs.exists(hdfs_path):
                fs.delete(hdfs_path, True)  # recursive
            fs.mkdirs(hdfs_path)
            
            # Recreate _state/ directory
            state_path = spark._jvm.org.apache.hadoop.fs.Path(path + "/_state")
            fs.mkdirs(state_path)
        
        print(f"Cleaned staging bucket: {name}")
    
    def _delete_old_hdfs_files(self, fs, path, cutoff_ms: int):
        """Delete HDFS files older than cutoff timestamp (milliseconds)."""
        if not fs.exists(path):
            return
        
        for status in fs.listStatus(path):
            file_path = status.getPath()
            if status.isDirectory():
                self._delete_old_hdfs_files(fs, file_path, cutoff_ms)
                if len(fs.listStatus(file_path)) == 0:
                    fs.delete(file_path, False)
            else:
                if status.getModificationTime() < cutoff_ms:
                    fs.delete(file_path, False)
    
    def _clean_s3_bucket(
        self, 
        name: str, 
        config: Dict[str, Any], 
        cutoff_time: Optional[datetime]
    ):
        """Clean S3 staging including hash state in _state/ prefix."""
        try:
            import boto3
        except ImportError:
            raise ImportError(
                f"S3 bucket '{name}' requires boto3. "
                f"Run 'dvt sync' to install cloud storage dependencies."
            )
        
        bucket = config.get('bucket')
        prefix = config.get('prefix', '')
        region = config.get('region')
        
        if not bucket:
            raise ValueError(f"S3 bucket '{name}' requires 'bucket' configuration")
        
        client_kwargs = {}
        if region:
            client_kwargs['region_name'] = region
        if config.get('access_key_id'):
            client_kwargs['aws_access_key_id'] = config['access_key_id']
            client_kwargs['aws_secret_access_key'] = config['secret_access_key']
        
        s3 = boto3.client('s3', **client_kwargs)
        
        # Clean both data and _state/ prefixes
        prefixes_to_clean = [prefix]
        if not cutoff_time:
            # Also clean hash state when doing full clean
            state_prefix = prefix.rstrip('/') + '/_state/' if prefix else '_state/'
            prefixes_to_clean.append(state_prefix)
        
        paginator = s3.get_paginator('list_objects_v2')
        objects_to_delete = []
        
        for clean_prefix in prefixes_to_clean:
            for page in paginator.paginate(Bucket=bucket, Prefix=clean_prefix):
                for obj in page.get('Contents', []):
                    if cutoff_time:
                        if obj['LastModified'].replace(tzinfo=None) < cutoff_time:
                            objects_to_delete.append({'Key': obj['Key']})
                    else:
                        objects_to_delete.append({'Key': obj['Key']})
        
        for i in range(0, len(objects_to_delete), 1000):
            batch = objects_to_delete[i:i+1000]
            if batch:
                s3.delete_objects(Bucket=bucket, Delete={'Objects': batch})
        
        print(f"Cleaned staging bucket: {name}")
    
    def _clean_gcs_bucket(
        self, 
        name: str, 
        config: Dict[str, Any], 
        cutoff_time: Optional[datetime]
    ):
        """Clean GCS staging including hash state in _state/ prefix."""
        try:
            from google.cloud import storage
        except ImportError:
            raise ImportError(
                f"GCS bucket '{name}' requires google-cloud-storage. "
                f"Run 'dvt sync' to install cloud storage dependencies."
            )
        
        bucket_name = config.get('bucket')
        prefix = config.get('prefix', '')
        project = config.get('project')
        credentials_path = config.get('credentials_path')
        
        if not bucket_name:
            raise ValueError(f"GCS bucket '{name}' requires 'bucket' configuration")
        
        client_kwargs = {}
        if project:
            client_kwargs['project'] = project
        if credentials_path:
            client_kwargs['credentials'] = credentials_path
        
        client = storage.Client(**client_kwargs)
        bucket = client.bucket(bucket_name)
        
        # Clean both data and _state/ prefixes
        prefixes_to_clean = [prefix]
        if not cutoff_time:
            # Also clean hash state when doing full clean
            state_prefix = prefix.rstrip('/') + '/_state/' if prefix else '_state/'
            prefixes_to_clean.append(state_prefix)
        
        for clean_prefix in prefixes_to_clean:
            blobs = bucket.list_blobs(prefix=clean_prefix)
            for blob in blobs:
                if cutoff_time:
                    if blob.updated.replace(tzinfo=None) < cutoff_time:
                        blob.delete()
                else:
                    blob.delete()
        
        print(f"Cleaned staging bucket: {name}")
    
    def _clean_azure_bucket(
        self, 
        name: str, 
        config: Dict[str, Any], 
        cutoff_time: Optional[datetime]
    ):
        """Clean Azure Blob staging including hash state in _state/ prefix."""
        try:
            from azure.storage.blob import BlobServiceClient
        except ImportError:
            raise ImportError(
                f"Azure bucket '{name}' requires azure-storage-blob. "
                f"Run 'dvt sync' to install cloud storage dependencies."
            )
        
        container_name = config.get('container')
        storage_account = config.get('storage_account')
        prefix = config.get('prefix', '')
        account_key = config.get('account_key')
        
        if not container_name or not storage_account:
            raise ValueError(
                f"Azure bucket '{name}' requires 'container' and 'storage_account'"
            )
        
        if account_key:
            conn_str = (
                f"DefaultEndpointsProtocol=https;"
                f"AccountName={storage_account};"
                f"AccountKey={account_key};"
                f"EndpointSuffix=core.windows.net"
            )
            client = BlobServiceClient.from_connection_string(conn_str)
        else:
            account_url = f"https://{storage_account}.blob.core.windows.net"
            client = BlobServiceClient(account_url=account_url)
        
        container = client.get_container_client(container_name)
        
        # Clean both data and _state/ prefixes
        prefixes_to_clean = [prefix]
        if not cutoff_time:
            # Also clean hash state when doing full clean
            state_prefix = prefix.rstrip('/') + '/_state/' if prefix else '_state/'
            prefixes_to_clean.append(state_prefix)
        
        for clean_prefix in prefixes_to_clean:
            blobs = container.list_blobs(name_starts_with=clean_prefix)
            for blob in blobs:
                if cutoff_time:
                    if blob.last_modified.replace(tzinfo=None) < cutoff_time:
                        container.delete_blob(blob.name)
                else:
                    container.delete_blob(blob.name)
        
        print(f"Cleaned staging bucket: {name}")
```

---

### 4. CLI Integration

**File:** `core/dvt/cli/flags.py`

```python
BUCKET = Argument(
    '--bucket',
    type=str,
    default=None,
    help="Clean only the specified staging bucket (skip dbt clean)"
)

OLDER_THAN = Argument(
    '--older-than',
    type=str,
    default=None,
    help="Only clean files older than duration, e.g., '24h', '7d' (skip dbt clean)"
)
```

**File:** `core/dvt/cli/main.py`

```python
@cli.command('clean')
@click.option('--bucket', type=str, default=None, 
              help="Clean only the specified staging bucket")
@click.option('--older-than', type=str, default=None,
              help="Only clean files older than duration (e.g., '24h', '7d')")
@click.pass_context
def clean(ctx, bucket, older_than):
    """Clean dbt artifacts and DVT staging files."""
    from dvt.task.clean import CleanTask
    
    args = ctx.obj['args']
    args.bucket = bucket
    args.older_than = older_than
    
    task = CleanTask(args)
    task.run()
```

---

## Dependencies

### Core DVT (No Extra Dependencies)

DVT core has minimal dependencies. The following are installed by `dvt sync`:

| Dependency | Installed Via | When Needed |
|------------|---------------|-------------|
| `pyspark` | `dvt sync` | Always (from computes.yml) |
| `sqlglot` | DVT core | Always |
| `pyarrow` | `dvt sync` | Always (Parquet handling) |
| `boto3` | `dvt sync` | S3 bucket configured |
| `google-cloud-storage` | `dvt sync` | GCS bucket configured |
| `azure-storage-blob` | `dvt sync` | Azure bucket configured |

### No Extra Dependencies Needed

| Bucket Type | Why |
|-------------|-----|
| `filesystem` | Python stdlib |
| `hdfs` | Spark built-in |

---

## `dvt sync` Dependency Detection

**Logic:** `dvt sync` reads `buckets.yml` and installs required cloud storage packages.

```python
# In sync.py

BUCKET_DEPENDENCIES = {
    "s3": "boto3",
    "gcs": "google-cloud-storage",
    "azure": "azure-storage-blob",
}

def _get_bucket_dependencies() -> List[str]:
    """Get cloud storage dependencies from buckets.yml."""
    buckets_config = load_buckets_config()
    if not buckets_config:
        return []
    
    deps = set()
    for profile_data in buckets_config.values():
        if not isinstance(profile_data, dict):
            continue
        buckets = profile_data.get("buckets", {})
        for bucket_config in buckets.values():
            bucket_type = bucket_config.get("type", "filesystem")
            if bucket_type in BUCKET_DEPENDENCIES:
                deps.add(BUCKET_DEPENDENCIES[bucket_type])
    
    return list(deps)
```

---

## Usage Examples

### Basic Development Workflow

```bash
# Initialize project with default buckets.yml
dvt init

# Sync dependencies (installs pyspark, pyarrow)
dvt sync

# Run models (staging goes to .dvt/staging/)
dvt run

# Clean everything when done
dvt clean
```

### Production with S3

```yaml
# buckets.yml
default:
  target: s3_staging
  buckets:
    s3_staging:
      type: s3
      bucket: my-company-dvt-staging
      prefix: prod/
      region: us-east-1
```

```bash
# dvt sync detects S3 bucket and installs boto3
dvt sync

# Clean old staging files
dvt clean --bucket s3_staging --older-than 7d
```

### Cloudera/Hadoop with HDFS

```yaml
# buckets.yml
default:
  target: hdfs_staging
  buckets:
    hdfs_staging:
      type: hdfs
      path: "hdfs://namenode:8020/user/dvt/staging"
```

```bash
# No extra deps needed - Spark has HDFS built-in
dvt sync
dvt run

# Clean HDFS staging
dvt clean --bucket hdfs_staging
```

---

## Testing Plan

### Unit Tests

| Test | Description |
|------|-------------|
| `test_parse_duration` | Test parsing of '24h', '7d' formats |
| `test_filesystem_clean` | Test local filesystem cleanup |
| `test_filesystem_clean_older_than` | Test age-based cleanup |
| `test_filesystem_clean_clears_state` | Test that _state/ is cleaned |
| `test_get_bucket_dependencies_s3` | Test S3 dependency detection |
| `test_get_bucket_dependencies_gcs` | Test GCS dependency detection |
| `test_get_bucket_dependencies_azure` | Test Azure dependency detection |
| `test_get_bucket_dependencies_none` | Test no deps for filesystem/hdfs |

### Integration Tests

| Test | Description |
|------|-------------|
| `test_clean_with_dbt` | Test that `dvt clean` runs dbt clean |
| `test_clean_bucket_skips_dbt` | Test that `--bucket` skips dbt clean |
| `test_clean_clears_hash_state` | Test that hash state is cleared with bucket |
| `test_sync_installs_boto3` | Test dvt sync installs boto3 for S3 |

---

## Implementation Checklist

- [ ] Update `core/dvt/config/user_config.py`
  - [ ] Update `create_default_buckets_yml()` with new template
  - [ ] Add `load_buckets_config()` function
  - [ ] Add `get_project_root()` helper

- [ ] Create `core/dvt/task/clean.py`
  - [ ] `parse_duration()` function
  - [ ] `CleanTask` class
  - [ ] Filesystem cleanup implementation (including _state/)
  - [ ] HDFS cleanup implementation (including _state/)
  - [ ] S3 cleanup implementation (including _state/ prefix)
  - [ ] GCS cleanup implementation (including _state/ prefix)
  - [ ] Azure cleanup implementation (including _state/ prefix)

- [ ] Update `core/dvt/task/sync.py`
  - [ ] Add `BUCKET_DEPENDENCIES` mapping
  - [ ] Add `_get_bucket_dependencies()` function
  - [ ] Install cloud storage deps during sync

- [ ] Update `core/dvt/cli/flags.py`
  - [ ] Add `BUCKET` flag
  - [ ] Add `OLDER_THAN` flag

- [ ] Update `core/dvt/cli/main.py`
  - [ ] Add flags to clean command
  - [ ] Wire up to CleanTask

- [ ] Add unit tests
  - [ ] `tests/unit/test_bucket_clean.py`
  - [ ] `tests/unit/test_bucket_dependencies.py`

---

## Related Plans

- **[EL Layer Plan](./el_layer_plan.md)** - Describes how the EL layer uses buckets for staging and hash state storage

---

## Future Enhancements

1. **Bucket metrics** - Track staging size per bucket (`dvt debug` shows size)
2. **Bucket health check** - `dvt debug` shows bucket connectivity status
3. **Partitioned staging** - Partition by date for efficient cleanup
4. **Cross-bucket copy** - Move staging between buckets (e.g., local → S3)
5. **Compression options** - Configure Parquet compression per bucket
