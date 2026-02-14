# DVT CLI Command: `dvt debug`

## Phase: 1 (Foundation)
## Type: Delta (modifications to existing implementation)

---

## Current Implementation Summary

**File**: `core/dvt/task/debug.py`

The current `dvt debug` command validates:
1. Python environment and version
2. dbt/dvt installation
3. profiles.yml location and parsing
4. Profile configuration validity
5. Database connection test
6. Project configuration (dbt_project.yml)

---

## Required Changes

### Delta: Add Bucket, Compute, and Federation Status

Extend debug to report:
1. `buckets.yml` configuration status
2. `computes.yml` configuration and Spark connectivity
3. Native connector availability
4. Federation readiness summary

### Reference Documents
- `connection_optimizations.md` - Section: "buckets.yml Configuration"
- `dvt_rules.md` - Section: "Compute Resolution"

---

## Implementation Details

### 1. New Debug Sections

Add three new sections to debug output:

#### Section: Buckets Configuration

```python
def _check_buckets_config(self) -> List[DebugResult]:
    """Check buckets.yml configuration."""
    results = []
    
    buckets_path = os.path.join(self.profiles_dir, "buckets.yml")
    
    # Check file exists
    if not os.path.exists(buckets_path):
        results.append(DebugResult(
            name="buckets.yml",
            status="SKIP",
            message="Not configured (native connectors disabled)",
        ))
        return results
    
    results.append(DebugResult(
        name="buckets.yml",
        status="OK",
        message=f"Found at {buckets_path}",
    ))
    
    # Parse and validate
    try:
        config = load_buckets_config(buckets_path)
        
        # Check default bucket
        default = config.get("default", {})
        if default:
            results.append(DebugResult(
                name="  Default bucket",
                status="OK",
                message=f"{default.get('type')}://{default.get('bucket')}/{default.get('prefix', '')}",
            ))
        else:
            results.append(DebugResult(
                name="  Default bucket",
                status="WARN",
                message="Not configured",
            ))
        
        # Check adapter-specific buckets
        adapters = config.get("adapters", {})
        for adapter, bucket_config in adapters.items():
            results.append(DebugResult(
                name=f"  {adapter} bucket",
                status="OK",
                message=f"{bucket_config.get('type')}://{bucket_config.get('bucket')}",
            ))
            
    except Exception as e:
        results.append(DebugResult(
            name="  Parse",
            status="FAIL",
            message=str(e),
        ))
    
    return results
```

#### Section: Computes Configuration

```python
def _check_computes_config(self) -> List[DebugResult]:
    """Check computes.yml configuration."""
    results = []
    
    computes_path = os.path.join(self.profiles_dir, "computes.yml")
    
    if not os.path.exists(computes_path):
        results.append(DebugResult(
            name="computes.yml",
            status="SKIP",
            message="Not configured (federation disabled)",
        ))
        return results
    
    results.append(DebugResult(
        name="computes.yml",
        status="OK",
        message=f"Found at {computes_path}",
    ))
    
    try:
        config = load_computes_config(computes_path)
        
        # Check each profile's computes
        for profile_name, profile_config in config.items():
            if profile_name.startswith("_"):
                continue
                
            default_compute = profile_config.get("default")
            computes = profile_config.get("computes", {})
            
            results.append(DebugResult(
                name=f"  Profile: {profile_name}",
                status="OK",
                message=f"Default compute: {default_compute or 'None'}",
            ))
            
            for compute_name, compute_config in computes.items():
                spark_master = compute_config.get("master", "local[*]")
                results.append(DebugResult(
                    name=f"    Compute: {compute_name}",
                    status="OK",
                    message=f"Spark master: {spark_master}",
                ))
                
    except Exception as e:
        results.append(DebugResult(
            name="  Parse",
            status="FAIL",
            message=str(e),
        ))
    
    return results
```

#### Section: Native Connectors

```python
def _check_native_connectors(self) -> List[DebugResult]:
    """Check native connector JAR availability."""
    results = []
    
    native_dir = os.path.join(self.lib_dir, "native")
    
    if not os.path.exists(native_dir):
        results.append(DebugResult(
            name="Native connectors",
            status="SKIP",
            message="Not installed (run 'dvt sync' to install)",
        ))
        return results
    
    # Check for each native connector
    from dvt.task.native_connectors import NATIVE_CONNECTORS
    
    for adapter, spec in NATIVE_CONNECTORS.items():
        jar_path = os.path.join(native_dir, spec.jar_name)
        
        if os.path.exists(jar_path):
            results.append(DebugResult(
                name=f"  {adapter}",
                status="OK",
                message=spec.jar_name,
            ))
        else:
            results.append(DebugResult(
                name=f"  {adapter}",
                status="MISS",
                message=f"Not found: {spec.jar_name}",
            ))
    
    return results
```

#### Section: Federation Readiness

```python
def _check_federation_readiness(self) -> List[DebugResult]:
    """Summary of federation readiness."""
    results = []
    
    # Check all prerequisites
    has_computes = os.path.exists(os.path.join(self.profiles_dir, "computes.yml"))
    has_jdbc = os.path.exists(os.path.join(self.lib_dir, "jdbc"))
    has_pyspark = self._check_pyspark_installed()
    
    if has_computes and has_jdbc and has_pyspark:
        results.append(DebugResult(
            name="Federation",
            status="READY",
            message="All prerequisites met",
        ))
    else:
        missing = []
        if not has_computes:
            missing.append("computes.yml")
        if not has_jdbc:
            missing.append("JDBC drivers (run 'dvt sync')")
        if not has_pyspark:
            missing.append("PySpark (pip install pyspark)")
        
        results.append(DebugResult(
            name="Federation",
            status="NOT READY",
            message=f"Missing: {', '.join(missing)}",
        ))
    
    return results


def _check_pyspark_installed(self) -> bool:
    """Check if PySpark is available."""
    try:
        import pyspark
        return True
    except ImportError:
        return False
```

### 2. Integration into Debug Task

**File**: `core/dvt/task/debug.py`

```python
class DebugTask(BaseTask):
    def run(self) -> DebugRunResult:
        results = []
        
        # Existing checks
        results.extend(self._check_python_env())
        results.extend(self._check_installation())
        results.extend(self._check_profiles())
        results.extend(self._check_connection())
        results.extend(self._check_project())
        
        # NEW: DVT-specific checks
        results.append(DebugSeparator("DVT Federation Status"))
        results.extend(self._check_buckets_config())
        results.extend(self._check_computes_config())
        results.extend(self._check_native_connectors())
        results.extend(self._check_federation_readiness())
        
        return DebugRunResult(results=results)
```

---

## CLI Output Changes

Before:
```
$ dvt debug
Running with dbt=1.8.0
python version: 3.11.0
profiles.yml: OK found at ~/.dvt/profiles.yml
Connection test: OK
Project: OK
```

After:
```
$ dvt debug
Running with dvt=1.8.0
python version: 3.11.0
profiles.yml: OK found at ~/.dvt/profiles.yml
Connection test: OK
Project: OK

DVT Federation Status
---------------------
buckets.yml: OK found at ~/.dvt/buckets.yml
  Default bucket: OK s3://my-dvt-staging-bucket/dvt-staging/
  snowflake bucket: OK s3://snowflake-staging-bucket/

computes.yml: OK found at ~/.dvt/computes.yml
  Profile: my_project
    Default compute: local_spark
    Compute: local_spark - Spark master: local[*]
    Compute: databricks - Spark master: databricks://...

Native connectors:
  snowflake: OK spark-snowflake_2.12-2.12.0-spark_3.4.jar
  bigquery: MISS Not found: spark-bigquery-with-dependencies_2.12-0.32.2.jar
  redshift: OK spark-redshift_2.12-6.0.0.jar

Federation: READY All prerequisites met
```

---

## Testing Requirements

### Unit Tests

**File**: `tests/unit/test_debug_task.py`

```python
def test_debug_checks_buckets_config(tmp_path, mocker):
    """Test debug reports buckets.yml status."""
    profiles_dir = tmp_path / ".dvt"
    profiles_dir.mkdir()
    
    # No buckets.yml
    task = DebugTask(profiles_dir=str(profiles_dir))
    results = task._check_buckets_config()
    
    assert len(results) == 1
    assert results[0].status == "SKIP"
    
    # With buckets.yml
    (profiles_dir / "buckets.yml").write_text("""
version: 1
default:
  type: s3
  bucket: my-bucket
""")
    
    results = task._check_buckets_config()
    assert results[0].status == "OK"


def test_debug_checks_federation_readiness(tmp_path, mocker):
    """Test debug reports federation readiness."""
    mocker.patch.object(DebugTask, "_check_pyspark_installed", return_value=True)
    
    profiles_dir = tmp_path / ".dvt"
    profiles_dir.mkdir()
    lib_dir = tmp_path / ".dvt" / "lib"
    
    # Missing computes.yml
    task = DebugTask(profiles_dir=str(profiles_dir), lib_dir=str(lib_dir))
    results = task._check_federation_readiness()
    
    assert results[0].status == "NOT READY"
    assert "computes.yml" in results[0].message
```

---

## Dependencies

- `01_init.md` - buckets.yml template created by init
- `02_sync.md` - native connectors downloaded by sync

## Blocked By

- `01_init.md`
- `02_sync.md`

## Blocks

- None (debug is informational)

---

## Implementation Checklist

- [ ] Add `_check_buckets_config()` method
- [ ] Add `_check_computes_config()` method
- [ ] Add `_check_native_connectors()` method
- [ ] Add `_check_federation_readiness()` method
- [ ] Add `_check_pyspark_installed()` helper
- [ ] Integrate new sections into `DebugTask.run()`
- [ ] Add `DebugSeparator` for section headers
- [ ] Add unit tests for each new check
- [ ] Update CLI help text
