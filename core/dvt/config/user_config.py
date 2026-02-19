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
SPARK_JARS_DIR_NAME = ".spark_jars"
NATIVE_CONNECTORS_DIR_NAME = "native"

# Backwards compatibility alias
JDBC_DRIVERS_DIR_NAME = SPARK_JARS_DIR_NAME


def get_dvt_home(profiles_dir: Optional[str] = None) -> Path:
    """Return DVT home directory (where profiles.yml and computes.yml live)."""
    if profiles_dir:
        return Path(profiles_dir).expanduser().resolve()
    return DVT_HOME


def get_spark_jars_dir(profiles_dir: Optional[str] = None) -> Path:
    """Return the directory where Spark JARs are stored (e.g. ~/.dvt/.spark_jars).

    This includes:
    - JDBC drivers (postgresql, snowflake-jdbc, etc.)
    - Native connectors (spark-snowflake, spark-bigquery, etc.)
    - Cloud connectors (hadoop-aws, gcs-connector, hadoop-azure, etc.)
    """
    return get_dvt_home(profiles_dir) / SPARK_JARS_DIR_NAME


def get_jdbc_drivers_dir(profiles_dir: Optional[str] = None) -> Path:
    """Backwards compatibility alias for get_spark_jars_dir()."""
    return get_spark_jars_dir(profiles_dir)


def get_native_connectors_dir(profiles_dir: Optional[str] = None) -> Path:
    """Return the directory where native connector JARs are stored (e.g. ~/.dvt/lib/native)."""
    return get_dvt_home(profiles_dir) / "lib" / NATIVE_CONNECTORS_DIR_NAME


def create_default_computes_yml(path: Optional[Path] = None) -> bool:
    """Create computes.yml header-only template if it doesn't exist.

    This creates only the header comments explaining the file structure.
    Actual profile entries are added by append_profile_to_computes_yml()
    when 'dvt init <project>' is run.

    Returns True if created.
    """
    path = Path(path) if path is not None else COMPUTES_PATH
    if path.exists():
        return False
    path.parent.mkdir(parents=True, exist_ok=True)
    content = """# DVT Compute Configuration
# ==========================
# This file configures Spark compute engines for federated query execution
# and JDBC-based data extraction.
#
# Java & Spark Compatibility
# --------------------------
# Each Spark version requires a specific Java version. If you have multiple
# Java installations, set 'java_home' per compute to point to the right one.
# If not set, DVT uses whatever 'java' is on your system PATH.
#
#   Spark 3.2.x  ->  Java 8 or 11
#   Spark 3.3.x  ->  Java 8, 11, or 17
#   Spark 3.4.x  ->  Java 8, 11, or 17
#   Spark 3.5.x  ->  Java 8, 11, or 17
#   Spark 4.0.x  ->  Java 17 or 21
#
# Structure:
#   <profile_name>:           # Matches profile in profiles.yml
#     target: <compute_name>  # Default compute to use
#     computes:
#       <compute_name>:
#         type: spark
#         version: "4.0.0"    # PySpark version (run 'dvt sync' after changing)
#         # java_home: /path/to/java  # Java installation for this Spark version
#         master: "local[*]"
#         config: {...}
#         delta: {...}          # Delta Lake staging settings
#         jdbc_extraction: {...}
#         jdbc_load: {...}
#
# Profiles are added automatically when you run 'dvt init <project_name>'.
# See docs: https://github.com/heshamh96/dvt
"""
    path.write_text(content)
    return True


def create_default_buckets_yml(path: Optional[Path] = None) -> bool:
    """Create buckets.yml header-only template if it doesn't exist.

    This creates only the header comments explaining the file structure.
    Actual profile entries are added by append_profile_to_buckets_yml()
    when 'dvt init <project>' is run.

    Returns True if created.
    """
    path = Path(path) if path is not None else BUCKETS_PATH
    if path.exists():
        return False
    path.parent.mkdir(parents=True, exist_ok=True)
    content = """# DVT Bucket Configuration
# ========================
# Buckets are staging areas for data during federation.
# DVT extracts source data to buckets, then Spark reads from buckets for compute.
#
# Structure:
#   <profile_name>:           # Matches profile in profiles.yml
#     target: <bucket_name>   # Default bucket to use
#     buckets:
#       <bucket_name>:
#         type: filesystem    # or s3, gcs, azure, hdfs
#         # ... type-specific settings ...
#
# Supported types: filesystem (default), s3, gcs, azure, hdfs
# Cloud storage dependencies are installed automatically by 'dvt sync'.
#
# Profiles are added automatically when you run 'dvt init <project_name>'.
# See docs: https://github.com/heshamh96/dvt
"""
    path.write_text(content)
    return True


def _default_bucket_block() -> Dict[str, Any]:
    """Default bucket block for a new profile (local filesystem by default)."""
    return {
        "target": "local",
        "buckets": {
            "local": {
                "type": "filesystem",
                # path resolved at runtime to project's .dvt/staging/
            },
        },
    }


def get_project_root() -> Optional[Path]:
    """Get the project root directory by searching for dbt_project.yml.

    Searches current directory and parents up to 10 levels.
    Returns None if not found.
    """
    cwd = Path.cwd()
    for parent in [cwd] + list(cwd.parents)[:10]:
        if (parent / "dbt_project.yml").exists():
            return parent
    return None


def get_bucket_path(
    bucket_config: Dict[str, Any],
    profile_name: str = "default",
    profiles_dir: Optional[str] = None,
) -> Optional[Path]:
    """Resolve the actual path for a bucket configuration.

    For filesystem buckets without explicit path, resolves to project's .dvt/staging/.
    For cloud buckets, returns None (handled differently).

    Args:
        bucket_config: The bucket configuration dict
        profile_name: Profile name for context
        profiles_dir: Optional profiles directory

    Returns:
        Path for filesystem/hdfs buckets, None for cloud buckets
    """
    bucket_type = bucket_config.get("type", "filesystem")

    if bucket_type == "filesystem":
        explicit_path = bucket_config.get("path")
        if explicit_path:
            return Path(explicit_path).expanduser().resolve()
        # Default to project's .dvt/staging/
        project_root = get_project_root()
        if project_root:
            return project_root / ".dvt" / "staging"
        # Fallback to DVT home staging
        return get_dvt_home(profiles_dir) / "staging"

    elif bucket_type == "hdfs":
        # HDFS paths are handled by Spark, return as Path for consistency
        hdfs_path = bucket_config.get("path")
        return Path(hdfs_path) if hdfs_path else None

    # Cloud buckets (s3, gcs, azure) don't have local paths
    return None


def get_bucket_dependencies(profiles_dir: Optional[str] = None) -> Dict[str, str]:
    """Determine which cloud storage dependencies are needed based on buckets.yml.

    Returns dict mapping bucket type to pip package name.
    """
    BUCKET_DEPENDENCIES = {
        "s3": "boto3",
        "gcs": "google-cloud-storage",
        "azure": "azure-storage-blob",
        # filesystem and hdfs need no extra deps
    }

    deps = {}
    buckets_config = load_buckets_config(profiles_dir)
    if not buckets_config:
        return deps

    for profile_name, profile_data in buckets_config.items():
        if not isinstance(profile_data, dict):
            continue
        buckets = profile_data.get("buckets", {})
        if not isinstance(buckets, dict):
            continue
        for bucket_name, bucket_config in buckets.items():
            if not isinstance(bucket_config, dict):
                continue
            bucket_type = bucket_config.get("type", "filesystem")
            if bucket_type in BUCKET_DEPENDENCIES:
                deps[bucket_type] = BUCKET_DEPENDENCIES[bucket_type]

    return deps


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
    """Generate a bucket profile template with local filesystem as default.

    Includes comprehensive examples for all cloud storage types and auth methods.
    """
    return f"""{profile_name}:
  target: local  # Default bucket to use
  buckets:
    # === Local Filesystem (Default) ===
    local:
      type: filesystem
      # Resolved at runtime to project's .dvt/staging/ directory
      # Override with absolute path if needed:
      # path: /custom/path/to/staging

    # === Amazon S3 ===
    # s3_staging:
    #   type: s3
    #   bucket: my-bucket
    #   prefix: staging/
    #   region: us-east-1
    #
    #   # Auth Option 1: Access Keys (basic)
    #   access_key_id: ${{AWS_ACCESS_KEY_ID}}
    #   secret_access_key: ${{AWS_SECRET_ACCESS_KEY}}
    #
    #   # Auth Option 2: Session Token (for temporary credentials)
    #   # access_key_id: ${{AWS_ACCESS_KEY_ID}}
    #   # secret_access_key: ${{AWS_SECRET_ACCESS_KEY}}
    #   # session_token: ${{AWS_SESSION_TOKEN}}
    #
    #   # Auth Option 3: IAM Role (for Redshift COPY/UNLOAD)
    #   # iam_role: arn:aws:iam::123456789:role/my-role
    #
    #   # Auth Option 4: Instance Profile (EC2/ECS/EKS)
    #   # use_instance_profile: true
    #
    #   # Auth Option 5: Storage Integration (Snowflake)
    #   # storage_integration: my_s3_integration
    #
    #   # S3-compatible storage (MinIO, LocalStack, etc.)
    #   # endpoint: http://localhost:9000
    #   # path_style_access: true

    # === Google Cloud Storage ===
    # gcs_staging:
    #   type: gcs
    #   bucket: my-bucket
    #   prefix: staging/
    #   project: my-gcp-project
    #
    #   # Auth Option 1: Service Account Key File
    #   keyfile: /path/to/service-account.json
    #
    #   # Auth Option 2: Inline JSON (set via environment variable)
    #   # keyfile_json: ${{GCP_SERVICE_ACCOUNT_JSON}}
    #
    #   # Auth Option 3: Application Default Credentials
    #   # use_application_default: true
    #
    #   # Auth Option 4: Storage Integration (Snowflake)
    #   # storage_integration: my_gcs_integration

    # === Azure Blob Storage / ADLS Gen2 ===
    # azure_staging:
    #   type: azure
    #   container: dvt-staging
    #   storage_account: mystorageaccount
    #   prefix: staging/
    #
    #   # Auth Option 1: Storage Account Key
    #   account_key: ${{AZURE_STORAGE_KEY}}
    #
    #   # Auth Option 2: SAS Token
    #   # sas_token: ${{AZURE_SAS_TOKEN}}
    #
    #   # Auth Option 3: Service Principal (Azure AD / Entra ID)
    #   # client_id: ${{AZURE_CLIENT_ID}}
    #   # client_secret: ${{AZURE_CLIENT_SECRET}}
    #   # tenant_id: ${{AZURE_TENANT_ID}}
    #
    #   # Auth Option 4: Managed Identity (Azure VMs/AKS)
    #   # use_managed_identity: true
    #
    #   # Auth Option 5: Storage Integration (Snowflake)
    #   # storage_integration: my_azure_integration
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


def load_computes_config(
    profiles_dir: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
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


def load_computes_for_profile(
    profile_name: str,
    profiles_dir: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """Load compute configuration for a specific profile.

    Returns dict with 'target' and 'computes' keys, or None if not found.
    Structure:
    {
        "target": "local_spark",  # Default compute name
        "computes": {
            "local_spark": {
                "type": "spark",
                "master": "local[*]",
                "config": {...},
                "jdbc_extraction": {
                    "num_partitions": 8,
                    "fetch_size": 10000,
                },
                "jdbc_load": {
                    "num_partitions": 4,
                    "batch_size": 10000,
                }
            },
        }
    }

    Args:
        profile_name: Profile name to load
        profiles_dir: Optional profiles directory

    Returns:
        Profile's compute configuration or None
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

        profile_computes = data.get(profile_name)
        if not isinstance(profile_computes, dict):
            return None

        return profile_computes

    except Exception:
        return None


def load_jdbc_load_config(
    profile_name: str,
    compute_name: Optional[str] = None,
    profiles_dir: Optional[str] = None,
) -> Dict[str, Any]:
    """Load JDBC load settings from a profile's compute configuration.

    Similar to jdbc_extraction but for writes (loading data into databases).

    Args:
        profile_name: Profile name to load
        compute_name: Specific compute to use, or None for default
        profiles_dir: Optional profiles directory

    Returns:
        Dict with 'num_partitions' and 'batch_size' keys, or empty dict
    """
    profile_computes = load_computes_for_profile(profile_name, profiles_dir)
    if not profile_computes:
        return {}

    # Get target compute
    target = compute_name or profile_computes.get("target")
    computes = profile_computes.get("computes", {})

    if target and target in computes:
        compute_config = computes[target]
        return compute_config.get("jdbc_load", {})

    return {}


def _compute_profile_template(profile_name: str) -> str:
    """Generate a compute profile template with comprehensive settings.

    Includes:
    - Commented version with tip to run 'dvt sync'
    - Commented java_home for users with multiple Java versions
    - Full Spark config including executor.memory
    - JDBC extraction settings (num_partitions, fetch_size)
    - JDBC load settings (num_partitions, batch_size)
    """
    return f"""{profile_name}:
  target: local_spark  # Default compute to use
  computes:
    local_spark:
      type: spark
      # version: "4.0.0"  # Set PySpark version (3.x or 4.x), then run 'dvt sync'
      # java_home: /path/to/java  # Java installation for this Spark version (see compat table above)
      master: "local[*]"

      # Spark configuration
      config:
        spark.driver.memory: "2g"
        spark.executor.memory: "2g"
        spark.sql.adaptive.enabled: "true"
        spark.sql.parquet.compression.codec: "zstd"

      # Delta Lake staging settings
      # DVT stores extracted source data as Delta tables in local staging.
      # These settings control how those staging files are managed.
      # delta:
      #   optimize_on_write: false          # Merge small files while writing (slower writes, faster reads next run)
      #   cleanup_retention_hours: 0        # Hours to keep old replaced files before deleting (0 = delete immediately)
      #   parallel_cleanup: false           # Delete old files in parallel during 'dvt clean --optimize'

      # JDBC extraction settings (reading from databases)
      # Higher num_partitions = more parallelism but more connections
      # Requires numeric primary key for partitioning
      jdbc_extraction:
        num_partitions: 8
        fetch_size: 10000

      # JDBC load settings (writing to databases)
      # Higher num_partitions = faster but more connections
      jdbc_load:
        num_partitions: 4
        batch_size: 10000
"""


def append_profile_to_computes_yml(
    profile_name: str,
    profiles_dir: Optional[str] = None,
) -> bool:
    """Append a new profile block to computes.yml if it doesn't already exist.

    Uses string template to preserve comments and formatting.
    Does not overwrite existing profile keys.

    Returns True if a new block was added.
    """
    dvt_home = get_dvt_home(profiles_dir)
    computes_path = dvt_home / "computes.yml"

    # Create file if it doesn't exist
    if not computes_path.exists():
        create_default_computes_yml(computes_path)

    # Check if profile already exists
    with open(computes_path, "r") as f:
        content = f.read()
        data = yaml.safe_load(content) or {}

    if not isinstance(data, dict):
        data = {}

    if profile_name in data:
        return False

    # Append the new profile template with comments
    with open(computes_path, "a") as f:
        f.write("\n" + _compute_profile_template(profile_name))

    return True


def load_profiles(profiles_dir: Optional[str] = None) -> Dict[str, Any]:
    """Load and parse profiles.yml.

    Returns dict mapping profile names to their configuration:
    {
        "profile_name": {
            "target": "dev",
            "outputs": {
                "dev": {"type": "postgres", "host": "...", ...},
                "prod": {"type": "snowflake", ...},
            }
        }
    }

    Args:
        profiles_dir: Optional profiles directory override

    Returns:
        Dict of profile configurations, empty dict if not found
    """
    dvt_home = get_dvt_home(profiles_dir)
    profiles_path = dvt_home / "profiles.yml"
    if not profiles_path.exists():
        return {}
    try:
        with open(profiles_path, "r") as f:
            data = yaml.safe_load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def create_default_profiles_yml(path: Optional[Path] = None) -> bool:
    """Create profiles.yml header-only template if it doesn't exist.

    This creates only the header comments explaining the file structure.
    Actual profile entries are added by append_profile_to_profiles_yml()
    when 'dvt init <project>' is run.

    Returns True if created.
    """
    path = Path(path) if path is not None else PROFILES_PATH
    if path.exists():
        return False
    path.parent.mkdir(parents=True, exist_ok=True)
    content = """# DVT/dbt Profiles Configuration
# ================================
# This file contains database connection profiles for your DVT/dbt projects.
#
# Structure:
#   <profile_name>:           # Matches 'profile' in dbt_project.yml
#     target: <target_name>   # Default target to use
#     outputs:
#       <target_name>:
#         type: <adapter>     # postgres, snowflake, bigquery, etc.
#         # ... adapter-specific connection settings ...
#
# Profiles are added automatically when you run 'dvt init <project_name>'.
# For available database adapters, see: https://docs.getdbt.com/docs/available-adapters
"""
    path.write_text(content)
    return True


def _profiles_profile_template(profile_name: str) -> str:
    """Generate a commented profile template with postgres as example.

    The template is fully commented out so it doesn't break anything.
    User must uncomment and fill in their actual credentials.
    """
    return f"""# {profile_name}:
#   target: dev
#   outputs:
#     dev:
#       type: postgres
#       host: localhost
#       port: 5432
#       user: your_username
#       password: your_password
#       dbname: your_database
#       schema: public
#       threads: 4
#
# For other database adapters (Snowflake, BigQuery, Databricks, etc.),
# see: https://docs.getdbt.com/docs/available-adapters
"""


def append_profile_to_profiles_yml(
    profile_name: str,
    profiles_dir: Optional[str] = None,
) -> bool:
    """Append a commented profile template to profiles.yml if profile doesn't exist.

    This is used when --skip-profile-setup is specified.
    Creates a commented-out postgres template that users can customize.

    Logic:
    - If file doesn't exist: create header + append commented template
    - If file exists but profile not in it: append commented template
    - If file exists and profile already in it (even commented): skip (return False)

    Returns True if a new block was added.
    """
    dvt_home = get_dvt_home(profiles_dir)
    profiles_path = dvt_home / "profiles.yml"

    # Create file if it doesn't exist
    if not profiles_path.exists():
        create_default_profiles_yml(profiles_path)

    # Check if profile already exists (either as actual YAML or commented template)
    with open(profiles_path, "r") as f:
        content = f.read()

    # Check in parsed YAML (for actual uncommented profiles)
    data = yaml.safe_load(content) or {}
    if isinstance(data, dict) and profile_name in data:
        return False

    # Also check raw text for commented template (e.g., "# Sprit_DB:")
    # This prevents duplicate commented templates
    if f"# {profile_name}:" in content or f"{profile_name}:" in content:
        return False

    # Append the new profile template (commented out)
    with open(profiles_path, "a") as f:
        f.write("\n" + _profiles_profile_template(profile_name))

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
