# DVT CLI Command: `dvt init`

## Phase: 1 (Foundation)
## Type: Delta (modifications to existing implementation)

---

## Current Implementation Summary

**File**: `core/dvt/task/init.py`

The current `dvt init` command:
1. Creates project directory structure
2. Generates `dbt_project.yml` template
3. Creates `~/.dvt/profiles.yml` if not exists
4. Initializes starter model files

---

## Required Changes

### Delta: Add `buckets.yml` Template Creation

When `dvt init` runs, it should also create a `buckets.yml` template in `~/.dvt/` if it doesn't exist.

### Reference Document
- `connection_optimizations.md` - Section: "buckets.yml Configuration"

---

## Implementation Details

### 1. New Template: `buckets.yml`

**Location**: `~/.dvt/buckets.yml`

**Template Content**:
```yaml
# DVT Bucket Configuration
# Configure staging buckets for native connector optimizations
# See: https://docs.dvt.dev/buckets

version: 1

# Default bucket used when no adapter-specific bucket is configured
default:
  type: s3  # s3, gcs, azure
  bucket: my-dvt-staging-bucket
  prefix: dvt-staging/
  # credentials: Use environment variables or IAM roles

# Adapter-specific bucket configurations (optional)
# These override the default for specific adapters
adapters:
  # Snowflake native connector staging
  # snowflake:
  #   type: s3
  #   bucket: snowflake-staging-bucket
  #   prefix: dvt/
  #   storage_integration: MY_S3_INTEGRATION  # Snowflake storage integration name

  # BigQuery native connector staging
  # bigquery:
  #   type: gcs
  #   bucket: bigquery-staging-bucket
  #   prefix: dvt-temp/

  # Redshift native connector staging
  # redshift:
  #   type: s3
  #   bucket: redshift-staging-bucket
  #   prefix: dvt-unload/
  #   iam_role: arn:aws:iam::123456789:role/RedshiftS3Access
```

### 2. Code Changes

**File**: `core/dvt/task/init.py`

Add function to create buckets.yml:

```python
def _create_buckets_yml_if_missing(profiles_dir: str) -> bool:
    """Create buckets.yml template if it doesn't exist.
    
    Returns True if file was created, False if it already existed.
    """
    buckets_path = os.path.join(profiles_dir, "buckets.yml")
    
    if os.path.exists(buckets_path):
        return False
    
    template = '''# DVT Bucket Configuration
# Configure staging buckets for native connector optimizations
# See: https://docs.dvt.dev/buckets

version: 1

# Default bucket used when no adapter-specific bucket is configured
default:
  type: s3  # s3, gcs, azure
  bucket: my-dvt-staging-bucket
  prefix: dvt-staging/
  # credentials: Use environment variables or IAM roles

# Adapter-specific bucket configurations (optional)
# These override the default for specific adapters
adapters:
  # Snowflake native connector staging
  # snowflake:
  #   type: s3
  #   bucket: snowflake-staging-bucket
  #   prefix: dvt/
  #   storage_integration: MY_S3_INTEGRATION

  # BigQuery native connector staging
  # bigquery:
  #   type: gcs
  #   bucket: bigquery-staging-bucket
  #   prefix: dvt-temp/

  # Redshift native connector staging
  # redshift:
  #   type: s3
  #   bucket: redshift-staging-bucket
  #   prefix: dvt-unload/
  #   iam_role: arn:aws:iam::123456789:role/RedshiftS3Access
'''
    
    with open(buckets_path, "w") as f:
        f.write(template)
    
    return True
```

**Integration Point**: Call this function in the `run()` method after creating profiles.yml:

```python
# In InitTask.run() or equivalent
def run(self):
    # ... existing profile creation ...
    
    # NEW: Create buckets.yml template
    profiles_dir = get_profiles_dir()
    if _create_buckets_yml_if_missing(profiles_dir):
        fire_event(BucketsYmlCreated(path=os.path.join(profiles_dir, "buckets.yml")))
```

### 3. New Event

**File**: `core/dvt/events/types.py` (or appropriate events file)

```python
@dataclass
class BucketsYmlCreated(InfoLevel):
    path: str
    
    def message(self) -> str:
        return f"Created buckets.yml template at {self.path}"
```

---

## CLI Output Changes

Before:
```
$ dvt init
Creating dbt_project.yml...
Creating profiles.yml...
Done!
```

After:
```
$ dvt init
Creating dbt_project.yml...
Creating profiles.yml...
Creating buckets.yml template...
Done!
```

---

## Testing Requirements

### Unit Tests

**File**: `tests/unit/test_init_task.py`

```python
def test_init_creates_buckets_yml(tmp_path):
    """Test that dvt init creates buckets.yml template."""
    profiles_dir = tmp_path / ".dvt"
    profiles_dir.mkdir()
    
    created = _create_buckets_yml_if_missing(str(profiles_dir))
    
    assert created is True
    assert (profiles_dir / "buckets.yml").exists()
    
    content = (profiles_dir / "buckets.yml").read_text()
    assert "version: 1" in content
    assert "default:" in content
    assert "adapters:" in content


def test_init_does_not_overwrite_buckets_yml(tmp_path):
    """Test that dvt init preserves existing buckets.yml."""
    profiles_dir = tmp_path / ".dvt"
    profiles_dir.mkdir()
    
    existing_content = "# My custom buckets config\nversion: 1"
    (profiles_dir / "buckets.yml").write_text(existing_content)
    
    created = _create_buckets_yml_if_missing(str(profiles_dir))
    
    assert created is False
    assert (profiles_dir / "buckets.yml").read_text() == existing_content
```

---

## Dependencies

- None (this is a foundation change)

## Blocked By

- None

## Blocks

- `02_sync.md` - sync needs buckets.yml to know where to stage files
- `03_debug.md` - debug needs to report bucket configuration status
- `05_seed.md` - seed may use bucket staging for large files

---

## Implementation Checklist

- [ ] Add `_create_buckets_yml_if_missing()` function to `core/dvt/task/init.py`
- [ ] Add `BucketsYmlCreated` event type
- [ ] Integrate into InitTask.run()
- [ ] Add unit tests
- [ ] Update CLI help text if needed
- [ ] Document in user docs
