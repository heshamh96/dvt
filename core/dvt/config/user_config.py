"""
DVT user-level configuration: ~/.dvt/profiles.yml, computes.yml, and data/mdm.duckdb.
Used by dvt init to create the DVT home directory structure.
Never overwrite existing profiles.yml or computes.yml; append or merge new profile entries only.
"""
from pathlib import Path
from typing import Any, Dict, Optional

import yaml

DVT_HOME = Path.home() / ".dvt"
PROFILES_PATH = DVT_HOME / "profiles.yml"
COMPUTES_PATH = DVT_HOME / "computes.yml"
BUCKETS_PATH = DVT_HOME / "buckets.yml"
DATA_DIR = DVT_HOME / "data"
MDM_DB_PATH = DATA_DIR / "mdm.duckdb"
JDBC_DRIVERS_DIR_NAME = ".jdbc_jars"
NATIVE_CONNECTORS_DIR_NAME = "native"


def get_dvt_home(profiles_dir: Optional[str] = None) -> Path:
    """Return DVT home directory (where profiles.yml and computes.yml live)."""
    if profiles_dir:
        return Path(profiles_dir).expanduser().resolve()
    return DVT_HOME


def get_jdbc_drivers_dir(profiles_dir: Optional[str] = None) -> Path:
    """Return the directory where JDBC driver JARs for adapters are stored (e.g. ~/.dvt/.jdbc_jars)."""
    return get_dvt_home(profiles_dir) / JDBC_DRIVERS_DIR_NAME


def get_native_connectors_dir(profiles_dir: Optional[str] = None) -> Path:
    """Return the directory where native connector JARs are stored (e.g. ~/.dvt/lib/native)."""
    return get_dvt_home(profiles_dir) / "lib" / NATIVE_CONNECTORS_DIR_NAME


def create_default_computes_yml(path: Optional[Path] = None) -> bool:
    """Create computes.yml with default local Spark config if it doesn't exist. Returns True if created."""
    path = Path(path) if path is not None else COMPUTES_PATH
    if path.exists():
        return False
    path.parent.mkdir(parents=True, exist_ok=True)
    content = """# DVT Compute Configuration
# Top-level keys are profile names. Each profile has target (active compute) and computes (definitions).
# See docs: https://github.com/heshamh96/dvt

default:
  target: default
  computes:
    default:
      type: spark
      version: "3.5.0"
      master: "local[*]"
      config:
        spark.driver.memory: "2g"
        spark.sql.adaptive.enabled: "true"
"""
    path.write_text(content)
    return True


def create_default_buckets_yml(path: Optional[Path] = None) -> bool:
    """Create buckets.yml with template for native connector staging if it doesn't exist.

    Buckets are per-profile (like profiles.yml). Each profile has:
    - target: Default bucket name to use
    - buckets: Bucket definitions (like outputs in profiles.yml)

    Returns True if created.
    """
    path = Path(path) if path is not None else BUCKETS_PATH
    if path.exists():
        return False
    path.parent.mkdir(parents=True, exist_ok=True)
    content = """# DVT Bucket Configuration
# Structure mirrors profiles.yml: each profile has target (default bucket) and buckets (definitions).
# Native connectors use cloud storage for faster data transfer instead of JDBC.
# See docs: https://github.com/heshamh96/dvt

# Example profile configuration:
# my_profile:
#   target: default  # Default bucket to use
#   buckets:
#     default:
#       type: s3  # s3, gcs, azure
#       bucket: my-dvt-staging-bucket
#       prefix: dvt-staging/
#       # region: us-east-1
#       # access_key_id: YOUR_ACCESS_KEY
#       # secret_access_key: YOUR_SECRET_KEY
#     snowflake_staging:
#       type: s3
#       bucket: snowflake-staging-bucket
#       prefix: dvt/
#       # storage_integration: MY_S3_INTEGRATION
#       # region: us-west-2
#       # access_key_id: YOUR_ACCESS_KEY
#       # secret_access_key: YOUR_SECRET_KEY
#     bigquery_staging:
#       type: gcs
#       bucket: bigquery-staging-bucket
#       prefix: dvt-temp/
#       # project: my-gcp-project
#       # credentials_path: /path/to/service-account.json
#     redshift_staging:
#       type: s3
#       bucket: redshift-staging-bucket
#       prefix: dvt-unload/
#       # iam_role: arn:aws:iam::123456789:role/RedshiftS3Access
#       # region: us-east-1
"""
    path.write_text(content)
    return True


def _default_bucket_block() -> Dict[str, Any]:
    """Default bucket block for a new profile (template with commented credentials)."""
    return {
        "target": "default",
        "buckets": {
            "default": {
                "type": "s3",
                "bucket": "my-dvt-staging-bucket",
                "prefix": "dvt-staging/",
                # Credentials are added as comments in the YAML output
            },
        },
    }


def load_buckets_config(profiles_dir: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """Load and parse buckets.yml. Returns None if not found or invalid."""
    dvt_home = get_dvt_home(profiles_dir)
    buckets_path = dvt_home / "buckets.yml"
    if not buckets_path.exists():
        return None
    try:
        with open(buckets_path, "r") as f:
            data = yaml.safe_load(f)
        return data if isinstance(data, dict) else None
    except Exception:
        return None


def load_buckets_for_profile(
    profile_name: str, profiles_dir: Optional[str] = None
) -> Optional[Dict[str, Any]]:
    """Load bucket configuration for a specific profile.

    Returns dict with 'target' and 'buckets' keys, or None if not found.
    Structure mirrors profiles.yml:
    {
        "target": "default",  # Default bucket name
        "buckets": {
            "default": {"type": "s3", "bucket": "...", ...},
            "snowflake_staging": {"type": "s3", ...},
        }
    }
    """
    all_buckets = load_buckets_config(profiles_dir)
    if not all_buckets or profile_name not in all_buckets:
        return None
    profile_buckets = all_buckets.get(profile_name)
    if not isinstance(profile_buckets, dict):
        return None
    return profile_buckets


def _bucket_profile_template(profile_name: str) -> str:
    """Generate a bucket profile template with all subconfigs commented."""
    return f"""{profile_name}:
  target: default  # Default bucket to use
  buckets:
    default:
      # type: # s3, gcs, azure
      # bucket: my-dvt-staging-bucket
      # prefix: dvt-staging/
      # region: us-east-1
      # access_key_id: YOUR_ACCESS_KEY
      # secret_access_key: YOUR_SECRET_KEY
"""


def append_profile_to_buckets_yml(
    profile_name: str,
    profiles_dir: Optional[str] = None,
) -> bool:
    """
    If the profile is not already in buckets.yml, append its block.
    Does not overwrite existing profile keys. Returns True if a new block was added.
    """
    dvt_home = get_dvt_home(profiles_dir)
    buckets_path = dvt_home / "buckets.yml"

    # Create file if it doesn't exist
    if not buckets_path.exists():
        create_default_buckets_yml(buckets_path)

    # Check if profile already exists
    with open(buckets_path, "r") as f:
        content = f.read()
        data = yaml.safe_load(content) or {}

    if not isinstance(data, dict):
        data = {}

    if profile_name in data:
        return False

    # Append the new profile template with comments
    with open(buckets_path, "a") as f:
        f.write("\n" + _bucket_profile_template(profile_name))

    return True


def load_computes_config(profiles_dir: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """Load and parse computes.yml. Returns compute definitions dict or None.

    The returned dict maps compute names to their configuration:
    {
        "default": {"type": "spark", "master": "local[*]", "config": {...}},
        "databricks": {"type": "spark", "master": "databricks://...", ...}
    }

    This extracts the 'computes' block from the active profile in computes.yml.
    """
    dvt_home = get_dvt_home(profiles_dir)
    computes_path = dvt_home / "computes.yml"
    if not computes_path.exists():
        return None
    try:
        with open(computes_path, "r") as f:
            data = yaml.safe_load(f)
        if not isinstance(data, dict):
            return None

        # computes.yml structure: {profile_name: {target: ..., computes: {...}}}
        # For now, use "default" profile or first available
        if "default" in data and isinstance(data["default"], dict):
            return data["default"].get("computes", {})

        # Try first profile
        for profile_name, profile_data in data.items():
            if isinstance(profile_data, dict) and "computes" in profile_data:
                return profile_data["computes"]

        return None
    except Exception:
        return None


def _default_compute_block() -> Dict[str, Any]:
    """Default compute block for a new profile (Spark local)."""
    return {
        "target": "default",
        "computes": {
            "default": {
                "type": "spark",
                "version": "3.5.0",
                "master": "local[*]",
                "config": {
                    "spark.driver.memory": "2g",
                    "spark.sql.adaptive.enabled": "true",
                },
            }
        },
    }


def append_profile_to_computes_yml(
    profile_name: str,
    profiles_dir: Optional[str] = None,
) -> bool:
    """
    If the profile is not already in computes.yml, append its block (merge).
    Does not overwrite existing profile keys. Returns True if a new block was added.
    """
    dvt_home = get_dvt_home(profiles_dir)
    computes_path = dvt_home / "computes.yml"
    if not computes_path.exists():
        create_default_computes_yml(computes_path)
    with open(computes_path, "r") as f:
        data = yaml.safe_load(f) or {}
    if not isinstance(data, dict):
        return False
    if profile_name in data:
        return False
    data[profile_name] = _default_compute_block()
    with open(computes_path, "w") as f:
        yaml.dump(data, f, default_flow_style=False, sort_keys=False)
    return True


def create_dvt_data_dir(profiles_dir: Optional[str] = None) -> Path:
    """Ensure ~/.dvt/data exists. Returns the data directory path."""
    dvt_home = get_dvt_home(profiles_dir)
    data_dir = dvt_home / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    return data_dir


def init_mdm_db(profiles_dir: Optional[str] = None) -> bool:
    """
    Create mdm.duckdb if it doesn't exist (minimal stub for now).
    Feature 12 (MDM) will add full schema later.
    Returns True if created.
    """
    data_dir = create_dvt_data_dir(profiles_dir)
    mdm_path = data_dir / "mdm.duckdb"
    if mdm_path.exists():
        return False
    try:
        import duckdb
        conn = duckdb.connect(str(mdm_path))
        # Minimal stub: one table so the file is a valid DuckDB DB
        conn.execute("CREATE TABLE IF NOT EXISTS _dvt_init (version INT);")
        conn.execute("INSERT INTO _dvt_init (version) VALUES (1);")
        conn.close()
        return True
    except ImportError:
        # duckdb not installed: create empty file so directory structure is there
        mdm_path.touch()
        return True
