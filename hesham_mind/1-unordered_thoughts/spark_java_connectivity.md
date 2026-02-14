# Spark & Java Connectivity Plan

## Overview

This document outlines the plan to enhance DVT's `computes.yml` template with comprehensive Spark configurations, `java_home`/`spark_home` support, and Spark 3.x/4.x only enforcement.

---

## Goals

1. **Enhanced computes.yml template** - Include commented essential Spark configs (~15-20)
2. **java_home and spark_home** - Compute-level configuration for multi-version setups
3. **Spark 3.x/4.x only** - Block Spark 2.x at `dvt sync` time
4. **Configuration inheritance** - Specified compute → default compute → DVT hardcoded defaults
5. **YARN/Cloudera support** - Include YARN-specific configs in template examples
6. **Unix-only** - No Windows support

---

## Configuration Precedence

```
1. User's specified compute in computes.yml  (highest priority)
2. User's 'default' compute in computes.yml  (inherit missing configs)
3. DVT's internal hardcoded defaults         (fallback)
```

### Example

```yaml
my_profile:
  target: cloudera_yarn
  computes:
    default:
      type: spark
      version: "3.5.0"
      master: "local[*]"
      java_home: "/usr/java/jdk-17"
      config:
        spark.driver.memory: "4g"
        spark.sql.adaptive.enabled: "true"
    
    cloudera_yarn:
      type: spark
      version: "3.3.2"
      master: "yarn"
      # No java_home specified - inherit from 'default'
      config:
        spark.executor.memory: "8g"
        # No spark.driver.memory - inherit from 'default'
```

**Resolved `cloudera_yarn` config:**
- `version`: "3.3.2" (from cloudera_yarn)
- `master`: "yarn" (from cloudera_yarn)
- `java_home`: "/usr/java/jdk-17" (inherited from default)
- `spark.driver.memory`: "4g" (inherited from default)
- `spark.executor.memory`: "8g" (from cloudera_yarn)
- `spark.sql.adaptive.enabled`: "true" (inherited from default)

---

## Files to Modify

### 1. `core/dvt/config/user_config.py`

**Changes:**
- Update `create_default_computes_yml()` with new comprehensive template
- Add `DVT_DEFAULT_COMPUTE_CONFIG` constant
- Add `resolve_compute_config()` function for config inheritance
- Add `deep_merge()` helper function

**New Template:**

```yaml
# DVT Compute Configuration
# =========================
# Top-level keys are profile names. Each profile has:
#   - target: Active compute name  
#   - computes: Compute engine definitions
#
# DVT supports Spark 3.x and 4.x only. Spark 2.x is NOT supported.
# See docs: https://github.com/heshamh96/dvt

default:
  target: default
  computes:
    default:
      type: spark
      version: "3.5.0"
      master: "local[*]"
      
      # === Environment Settings ===
      # Uncomment to override system defaults (Unix paths only)
      # java_home: "/usr/lib/jvm/java-17-openjdk"
      # spark_home: "/opt/spark"
      
      # === Spark Configuration ===
      config:
        # --- Memory & Resources ---
        spark.driver.memory: "2g"
        # spark.driver.cores: "1"
        # spark.executor.memory: "2g"
        # spark.executor.cores: "2"
        # spark.executor.instances: "2"
        
        # --- Shuffle & Parallelism ---
        # spark.sql.shuffle.partitions: "200"
        # spark.default.parallelism: "8"
        
        # --- Adaptive Query Execution (Spark 3.2+) ---
        spark.sql.adaptive.enabled: "true"
        # spark.sql.adaptive.coalescePartitions.enabled: "true"
        # spark.sql.adaptive.skewJoin.enabled: "true"
        
        # --- JDBC & Arrow ---
        # spark.sql.execution.arrow.pyspark.enabled: "true"
        # spark.sql.inMemoryColumnarStorage.batchSize: "10000"
        
        # --- Network & Serialization ---
        # spark.serializer: "org.apache.spark.serializer.KryoSerializer"
        # spark.network.timeout: "300s"
        
        # --- SQL Behavior ---
        # spark.sql.ansi.enabled: "false"    # Note: default is TRUE in Spark 4.x
        # spark.sql.caseSensitive: "false"
        
        # --- Logging ---
        # spark.eventLog.enabled: "false"
        # spark.eventLog.dir: "/tmp/spark-events"

    # === Example: Cloudera/CDP YARN Cluster ===
    # cloudera_yarn:
    #   type: spark
    #   version: "3.3.2"
    #   master: "yarn"
    #   java_home: "/usr/java/jdk-17"
    #   spark_home: "/opt/cloudera/parcels/CDH/lib/spark"
    #   config:
    #     # --- YARN Resources ---
    #     spark.driver.memory: "4g"
    #     spark.executor.memory: "8g"
    #     spark.executor.cores: "4"
    #     spark.executor.instances: "10"
    #     spark.yarn.queue: "default"
    #     spark.yarn.submit.waitAppCompletion: "true"
    #     # spark.yarn.jars: "hdfs:///spark/jars/*"
    #     
    #     # --- Dynamic Allocation ---
    #     # spark.dynamicAllocation.enabled: "true"
    #     # spark.dynamicAllocation.minExecutors: "2"
    #     # spark.dynamicAllocation.maxExecutors: "20"
    #     # spark.shuffle.service.enabled: "true"
    #     
    #     # --- Kerberos (if enabled) ---
    #     # spark.yarn.principal: "user@REALM.COM"
    #     # spark.yarn.keytab: "/path/to/user.keytab"

    # === Example: Spark Standalone Cluster ===
    # standalone:
    #   type: spark
    #   version: "3.5.0"
    #   master: "spark://master-node:7077"
    #   java_home: "/opt/java/jdk-17"
    #   spark_home: "/opt/spark-3.5.0"
    #   config:
    #     spark.driver.memory: "4g"
    #     spark.executor.memory: "8g"
    #     spark.cores.max: "32"

    # === Example: Kubernetes Cluster ===
    # kubernetes:
    #   type: spark
    #   version: "3.5.0"
    #   master: "k8s://https://k8s-api-server:6443"
    #   config:
    #     spark.kubernetes.container.image: "spark:3.5.0"
    #     spark.kubernetes.namespace: "spark"
    #     spark.executor.instances: "5"
    #     spark.kubernetes.authenticate.driver.serviceAccountName: "spark"
```

**New Functions:**

```python
# DVT's internal hardcoded defaults
DVT_DEFAULT_COMPUTE_CONFIG = {
    "type": "spark",
    "version": "3.5.0",
    "master": "local[*]",
    "config": {
        "spark.driver.memory": "2g",
        "spark.sql.adaptive.enabled": "true",
    }
}


def deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    """
    Deep merge two dicts. Override values take precedence.
    Nested dicts (like 'config') are merged, not replaced.
    """
    result = base.copy()
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def resolve_compute_config(
    compute_name: str, 
    computes: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Resolve compute configuration with inheritance.
    
    Precedence:
    1. Specified compute's config
    2. 'default' compute's config (if exists)
    3. DVT's hardcoded defaults
    
    Returns fully resolved config dict.
    """
    import copy
    
    # Start with DVT defaults
    resolved = copy.deepcopy(DVT_DEFAULT_COMPUTE_CONFIG)
    
    # Layer 2: Apply 'default' compute if it exists and is different from target
    if "default" in computes and compute_name != "default":
        default_config = computes["default"]
        resolved = deep_merge(resolved, default_config)
    
    # Layer 1: Apply specified compute (highest priority)
    if compute_name in computes:
        target_config = computes[compute_name]
        resolved = deep_merge(resolved, target_config)
    
    return resolved
```

---

### 2. `core/dvt/task/sync.py`

**Changes:**
- Add `MIN_SPARK_VERSION = (3, 0, 0)` constant
- Add `_check_spark_version_supported()` function to block Spark 2.x
- Add `_validate_compute_paths()` function for java_home/spark_home validation
- Integrate validation into sync flow

**New Functions:**

```python
# Minimum supported Spark version
MIN_SPARK_VERSION = (3, 0, 0)


def _check_spark_version_supported(spark_version: str) -> Tuple[bool, Optional[str]]:
    """
    Check if Spark version is supported (3.x or 4.x only).
    DVT does not support Spark 2.x.
    
    Returns (is_supported, error_message).
    """
    try:
        parts = spark_version.split(".")
        major = int(parts[0]) if parts[0].isdigit() else 0
        minor = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 0
        patch = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else 0
        
        version_tuple = (major, minor, patch)
        
        if version_tuple < MIN_SPARK_VERSION:
            return (
                False,
                f"Spark {spark_version} is not supported. "
                f"DVT requires Spark 3.x or 4.x. "
                f"Please update 'version' in computes.yml to 3.0.0 or higher."
            )
        return (True, None)
    except Exception:
        return (
            False,
            f"Invalid Spark version format: '{spark_version}'. "
            f"Expected format: X.Y.Z (e.g., '3.5.0', '4.0.0')"
        )


def _validate_compute_paths(compute_config: Dict[str, Any]) -> List[str]:
    """
    Validate java_home and spark_home paths if specified.
    Returns list of warning/error messages.
    """
    errors = []
    
    java_home = compute_config.get("java_home")
    if java_home:
        java_path = Path(java_home)
        if not java_path.exists():
            errors.append(f"java_home path does not exist: {java_home}")
        elif not (java_path / "bin" / "java").exists():
            errors.append(f"java_home does not contain bin/java: {java_home}")
    
    spark_home = compute_config.get("spark_home")
    if spark_home:
        spark_path = Path(spark_home)
        if not spark_path.exists():
            errors.append(f"spark_home path does not exist: {spark_home}")
        elif not ((spark_path / "python").exists() or 
                  (spark_path / "bin" / "spark-submit").exists()):
            errors.append(
                f"spark_home does not appear to be a valid Spark installation: {spark_home}"
            )
    
    return errors
```

---

### 3. `core/dvt/task/seed.py`

**Changes:**
- Update `_load_compute_config()` to call `resolve_compute_config()` after loading raw YAML
- Return fully resolved config with inheritance applied

```python
def _load_compute_config(
    compute_name: Optional[str], 
    profiles_dir: Optional[str] = None
) -> Dict[str, Any]:
    """Load compute configuration from computes.yml with inheritance."""
    from dvt.config.user_config import load_computes_config, resolve_compute_config
    
    computes = load_computes_config(profiles_dir)
    if not computes:
        # Return DVT defaults if no computes.yml
        from dvt.config.user_config import DVT_DEFAULT_COMPUTE_CONFIG
        return DVT_DEFAULT_COMPUTE_CONFIG.copy()

    # Determine which compute to use
    target_compute = compute_name or "default"
    if target_compute not in computes and computes:
        target_compute = next(iter(computes))
    
    # Resolve with inheritance
    return resolve_compute_config(target_compute, computes)
```

---

### 4. `core/dvt/federation/spark_manager.py`

**Changes:**
- Add `_apply_environment_overrides()` method
- Call it before creating SparkSession in `get_or_create_session()`

```python
import os
from pathlib import Path


def _apply_environment_overrides(self) -> None:
    """Apply java_home and spark_home from compute config to environment."""
    java_home = self.config.get("java_home")
    spark_home = self.config.get("spark_home")
    
    if java_home:
        os.environ["JAVA_HOME"] = java_home
        # Also update PATH to include java bin
        java_bin = str(Path(java_home) / "bin")
        current_path = os.environ.get("PATH", "")
        if java_bin not in current_path:
            os.environ["PATH"] = f"{java_bin}:{current_path}"
        _log(f"  Using JAVA_HOME: {java_home}")
    
    if spark_home:
        os.environ["SPARK_HOME"] = spark_home
        _log(f"  Using SPARK_HOME: {spark_home}")


def get_or_create_session(self, app_name: str = "DVT") -> Any:
    # Apply environment overrides BEFORE creating session
    self._apply_environment_overrides()
    
    # ... rest of existing code ...
```

---

## Unit Tests

### `tests/unit/test_spark_version_validation.py`

```python
import pytest
from dvt.task.sync import _check_spark_version_supported, _validate_compute_paths


class TestSparkVersionValidation:
    """Tests for Spark version support validation."""
    
    def test_spark_2x_rejected(self):
        """Spark 2.x should be rejected."""
        for version in ["2.0.0", "2.4.0", "2.4.8"]:
            is_supported, error = _check_spark_version_supported(version)
            assert is_supported is False
            assert "not supported" in error.lower()
            assert "3.x or 4.x" in error
    
    def test_spark_3x_supported(self):
        """Spark 3.x should be supported."""
        for version in ["3.0.0", "3.2.1", "3.3.2", "3.5.0"]:
            is_supported, error = _check_spark_version_supported(version)
            assert is_supported is True
            assert error is None
    
    def test_spark_4x_supported(self):
        """Spark 4.x should be supported."""
        for version in ["4.0.0", "4.1.0", "4.2.0"]:
            is_supported, error = _check_spark_version_supported(version)
            assert is_supported is True
            assert error is None
    
    def test_invalid_version_format(self):
        """Invalid version formats should be rejected."""
        is_supported, error = _check_spark_version_supported("invalid")
        assert is_supported is False
        assert "invalid" in error.lower()


class TestComputePathValidation:
    """Tests for java_home and spark_home path validation."""
    
    def test_missing_java_home_warns(self):
        """Non-existent java_home should produce warning."""
        config = {"java_home": "/nonexistent/path/to/java"}
        errors = _validate_compute_paths(config)
        assert len(errors) == 1
        assert "does not exist" in errors[0]
    
    def test_missing_spark_home_warns(self):
        """Non-existent spark_home should produce warning."""
        config = {"spark_home": "/nonexistent/path/to/spark"}
        errors = _validate_compute_paths(config)
        assert len(errors) == 1
        assert "does not exist" in errors[0]
    
    def test_no_paths_no_errors(self):
        """No paths specified should produce no errors."""
        config = {"type": "spark", "version": "3.5.0"}
        errors = _validate_compute_paths(config)
        assert len(errors) == 0
```

### `tests/unit/test_compute_config_inheritance.py`

```python
import pytest
from dvt.config.user_config import resolve_compute_config, deep_merge, DVT_DEFAULT_COMPUTE_CONFIG


class TestDeepMerge:
    """Tests for deep_merge helper function."""
    
    def test_simple_merge(self):
        base = {"a": 1, "b": 2}
        override = {"b": 3, "c": 4}
        result = deep_merge(base, override)
        assert result == {"a": 1, "b": 3, "c": 4}
    
    def test_nested_merge(self):
        base = {"config": {"a": 1, "b": 2}}
        override = {"config": {"b": 3, "c": 4}}
        result = deep_merge(base, override)
        assert result == {"config": {"a": 1, "b": 3, "c": 4}}
    
    def test_override_replaces_non_dict(self):
        base = {"a": {"nested": 1}}
        override = {"a": "string"}
        result = deep_merge(base, override)
        assert result == {"a": "string"}


class TestResolveComputeConfig:
    """Tests for compute config inheritance resolution."""
    
    def test_uses_dvt_defaults_when_empty(self):
        result = resolve_compute_config("nonexistent", {})
        assert result["version"] == DVT_DEFAULT_COMPUTE_CONFIG["version"]
        assert result["master"] == DVT_DEFAULT_COMPUTE_CONFIG["master"]
    
    def test_inherits_from_default_compute(self):
        computes = {
            "default": {
                "java_home": "/usr/java/jdk-17",
                "config": {"spark.driver.memory": "4g"}
            },
            "yarn": {
                "master": "yarn",
                "config": {"spark.executor.memory": "8g"}
            }
        }
        result = resolve_compute_config("yarn", computes)
        assert result["java_home"] == "/usr/java/jdk-17"
        assert result["master"] == "yarn"
        assert result["config"]["spark.driver.memory"] == "4g"
        assert result["config"]["spark.executor.memory"] == "8g"
    
    def test_specified_overrides_default(self):
        computes = {
            "default": {"version": "3.5.0"},
            "yarn": {"version": "3.3.2"}
        }
        result = resolve_compute_config("yarn", computes)
        assert result["version"] == "3.3.2"
```

---

## Behavior Summary

| Scenario | Behavior |
|----------|----------|
| `dvt sync` with Spark 2.x in computes.yml | **FAIL** with error: "Spark 2.x is not supported. DVT requires Spark 3.x or 4.x." |
| `dvt sync` with Spark 3.x/4.x | **PASS** - continue normally |
| `java_home` path doesn't exist | **WARN** but continue (user might create it later) |
| `spark_home` path doesn't exist | **WARN** but continue |
| No `java_home`/`spark_home` specified | Use system defaults (current behavior) |
| `java_home` specified at runtime | Set `JAVA_HOME` and update `PATH` before SparkSession creation |
| Config missing in specified compute | Inherit from 'default' compute, then DVT hardcoded defaults |

---

## Essential Spark Configs in Template

| Category | Config | Default | Notes |
|----------|--------|---------|-------|
| **Environment** | `java_home` | System | Commented |
| | `spark_home` | System | Commented |
| **Memory** | `spark.driver.memory` | "2g" | Active |
| | `spark.driver.cores` | "1" | Commented |
| | `spark.executor.memory` | "2g" | Commented |
| | `spark.executor.cores` | "2" | Commented |
| | `spark.executor.instances` | "2" | Commented |
| **Parallelism** | `spark.sql.shuffle.partitions` | "200" | Commented |
| | `spark.default.parallelism` | "8" | Commented |
| **AQE** | `spark.sql.adaptive.enabled` | "true" | Active |
| | `spark.sql.adaptive.coalescePartitions.enabled` | "true" | Commented |
| | `spark.sql.adaptive.skewJoin.enabled` | "true" | Commented |
| **JDBC** | `spark.sql.execution.arrow.pyspark.enabled` | "true" | Commented |
| **Network** | `spark.serializer` | Kryo | Commented |
| | `spark.network.timeout` | "300s" | Commented |
| **SQL** | `spark.sql.ansi.enabled` | "false" | Commented (note: Spark 4.x default is true) |
| | `spark.sql.caseSensitive` | "false" | Commented |
| **YARN** | `spark.yarn.queue` | "default" | Commented (example) |
| | `spark.dynamicAllocation.enabled` | "true" | Commented (example) |
| | `spark.yarn.principal` | - | Commented (Kerberos) |
| | `spark.yarn.keytab` | - | Commented (Kerberos) |
| **Logging** | `spark.eventLog.enabled` | "false" | Commented |

---

## Spark Version Compatibility Reference

| Spark Version | Java Support | Python Support | Status |
|---------------|--------------|----------------|--------|
| 3.2.x | 8, 11 | 3.7-3.10 | Supported |
| 3.3.x | 8, 11, 17 | 3.7-3.10 | Supported (Cloudera CDP 7.1.9) |
| 3.4.x | 8, 11, 17 | 3.8-3.11 | Supported |
| 3.5.x | 8, 11, 17 | 3.8-3.12 | Supported (LTS until Apr 2026) |
| 4.0.x | 17, 21 | 3.10+ | Supported |
| 4.1.x | 17, 21 | 3.10+ | Supported (Current) |
| **2.x** | 8 | 2.7, 3.4-3.7 | **NOT SUPPORTED** |

---

## Design Decisions

1. **No Windows support** - Unix paths only in templates and validation
2. **Single-level inheritance** - Only default → specified (no recursive inheritance)
3. **No verbose logging for inheritance** - Keep it simple, don't log which configs were inherited
4. **Validation at sync time** - Catch issues early, before runtime
5. **Warn but continue for missing paths** - User might set up paths after running sync

---

## Implementation Checklist

- [ ] Update `core/dvt/config/user_config.py`
  - [ ] New template in `create_default_computes_yml()`
  - [ ] Add `DVT_DEFAULT_COMPUTE_CONFIG` constant
  - [ ] Add `deep_merge()` function
  - [ ] Add `resolve_compute_config()` function

- [ ] Update `core/dvt/task/sync.py`
  - [ ] Add `MIN_SPARK_VERSION` constant
  - [ ] Add `_check_spark_version_supported()` function
  - [ ] Add `_validate_compute_paths()` function
  - [ ] Integrate validation into sync flow

- [ ] Update `core/dvt/task/seed.py`
  - [ ] Update `_load_compute_config()` to use `resolve_compute_config()`

- [ ] Update `core/dvt/federation/spark_manager.py`
  - [ ] Add `_apply_environment_overrides()` method
  - [ ] Call it in `get_or_create_session()`

- [ ] Add unit tests
  - [ ] `tests/unit/test_spark_version_validation.py`
  - [ ] `tests/unit/test_compute_config_inheritance.py`
