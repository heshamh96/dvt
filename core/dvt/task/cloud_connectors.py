# coding=utf-8
"""
Cloud storage connector JARs for Spark.

Maps bucket types (s3, gcs, azure) to required Hadoop connector JARs.
JAR versions are determined by detecting installed PySpark → Hadoop version.

| Bucket | JARs Required                                                    |
|--------|------------------------------------------------------------------|
| s3     | hadoop-aws-{hadoop_ver}.jar, aws-java-sdk-bundle-{aws_ver}.jar  |
| gcs    | gcs-connector-{ver}-shaded.jar                                   |
| azure  | hadoop-azure-{hadoop_ver}.jar, azure-storage-{azure_ver}.jar    |

All JARs are downloaded to ~/.dvt/.spark_jars/ alongside JDBC drivers.
"""

from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

# PySpark version → Hadoop version mapping
# Spark 4.0 uses Hadoop 3.4.1, Spark 3.5 uses Hadoop 3.3.4, etc.
PYSPARK_TO_HADOOP: Dict[str, str] = {
    "4.0": "3.4.1",
    "3.5": "3.3.4",
    "3.4": "3.3.4",
    "3.3": "3.3.2",
    "3.2": "3.3.1",
    "3.1": "3.2.0",
    "3.0": "3.2.0",
}

# AWS SDK versions compatible with Hadoop versions
# These are tested compatible combinations
HADOOP_TO_AWS_SDK: Dict[str, str] = {
    "3.4.1": "1.12.367",
    "3.3.4": "1.12.262",
    "3.3.2": "1.12.262",
    "3.3.1": "1.12.99",
    "3.2.0": "1.11.901",
}

# GCS connector version (shaded JAR, version-independent of Hadoop)
GCS_CONNECTOR_VERSION = "3.0.4"

# Azure storage SDK version
AZURE_STORAGE_VERSION = "8.6.6"

# Bucket type → list of (groupId, artifactId, version_template, classifier)
# version_template can contain {hadoop_version} or {aws_sdk_version} placeholders
BUCKET_TO_JARS: Dict[str, List[Tuple[str, str, str, Optional[str]]]] = {
    "s3": [
        ("org.apache.hadoop", "hadoop-aws", "{hadoop_version}", None),
        ("com.amazonaws", "aws-java-sdk-bundle", "{aws_sdk_version}", None),
    ],
    "gcs": [
        # GCS connector uses shaded JAR (bundles all dependencies)
        (
            "com.google.cloud.bigdataoss",
            "gcs-connector",
            GCS_CONNECTOR_VERSION,
            "shaded",
        ),
    ],
    "azure": [
        ("org.apache.hadoop", "hadoop-azure", "{hadoop_version}", None),
        ("com.microsoft.azure", "azure-storage", AZURE_STORAGE_VERSION, None),
    ],
}


def detect_pyspark_version() -> Optional[str]:
    """Detect installed PySpark version.

    Returns:
        PySpark version string (e.g., "4.0.0") or None if not installed.
    """
    try:
        import pyspark

        return pyspark.__version__
    except ImportError:
        return None


def detect_hadoop_version() -> str:
    """Detect Hadoop version from installed PySpark.

    Returns:
        Hadoop version string (e.g., "3.4.1"). Defaults to "3.4.1" if
        PySpark is not installed or version is unknown.
    """
    pyspark_version = detect_pyspark_version()
    if not pyspark_version:
        return "3.4.1"  # Default to Spark 4.0's Hadoop version

    # Extract major.minor version (e.g., "4.0" from "4.0.0")
    parts = pyspark_version.split(".")
    if len(parts) >= 2:
        major_minor = f"{parts[0]}.{parts[1]}"
        if major_minor in PYSPARK_TO_HADOOP:
            return PYSPARK_TO_HADOOP[major_minor]

    # Default to latest
    return "3.4.1"


def get_aws_sdk_version(hadoop_version: str) -> str:
    """Get compatible AWS SDK version for a Hadoop version.

    Args:
        hadoop_version: Hadoop version string (e.g., "3.4.1")

    Returns:
        AWS SDK bundle version string
    """
    return HADOOP_TO_AWS_SDK.get(hadoop_version, "1.12.367")


def get_cloud_jars_for_bucket_types(
    bucket_types: List[str],
) -> List[Tuple[str, str, str, Optional[str]]]:
    """Get JAR coordinates for given bucket types.

    Args:
        bucket_types: List of bucket types (e.g., ["s3", "gcs"])

    Returns:
        List of (groupId, artifactId, version, classifier) tuples with
        version placeholders resolved based on detected Hadoop version.
    """
    hadoop_version = detect_hadoop_version()
    aws_sdk_version = get_aws_sdk_version(hadoop_version)

    result: List[Tuple[str, str, str, Optional[str]]] = []
    seen: set = set()

    for bucket_type in bucket_types:
        if bucket_type not in BUCKET_TO_JARS:
            continue

        for group_id, artifact_id, version_template, classifier in BUCKET_TO_JARS[
            bucket_type
        ]:
            # Resolve version placeholders
            version = version_template.format(
                hadoop_version=hadoop_version,
                aws_sdk_version=aws_sdk_version,
            )

            # Deduplicate
            key = (group_id, artifact_id, version, classifier)
            if key not in seen:
                seen.add(key)
                result.append(key)

    return result


def _maven_jar_url(
    group_id: str,
    artifact_id: str,
    version: str,
    classifier: Optional[str] = None,
) -> str:
    """Build Maven Central URL for a JAR.

    Args:
        group_id: Maven group ID
        artifact_id: Maven artifact ID
        version: Version string
        classifier: Optional classifier (e.g., "shaded")

    Returns:
        Full Maven Central URL for the JAR
    """
    group_path = group_id.replace(".", "/")

    if classifier:
        jar_name = f"{artifact_id}-{version}-{classifier}.jar"
    else:
        jar_name = f"{artifact_id}-{version}.jar"

    return f"https://repo1.maven.org/maven2/{group_path}/{artifact_id}/{version}/{jar_name}"


def _jar_filename(
    artifact_id: str,
    version: str,
    classifier: Optional[str] = None,
) -> str:
    """Build JAR filename.

    Args:
        artifact_id: Maven artifact ID
        version: Version string
        classifier: Optional classifier

    Returns:
        JAR filename (e.g., "gcs-connector-3.0.4-shaded.jar")
    """
    if classifier:
        return f"{artifact_id}-{version}-{classifier}.jar"
    return f"{artifact_id}-{version}.jar"


def _download_cloud_jar(
    group_id: str,
    artifact_id: str,
    version: str,
    classifier: Optional[str],
    dest_dir: Path,
    on_event: Optional[Callable[[str], None]] = None,
) -> bool:
    """Download a single cloud connector JAR from Maven Central.

    Args:
        group_id: Maven group ID
        artifact_id: Maven artifact ID
        version: Version string
        classifier: Optional classifier
        dest_dir: Destination directory
        on_event: Optional callback for progress messages

    Returns:
        True on success, False on failure
    """
    dest_dir.mkdir(parents=True, exist_ok=True)
    jar_name = _jar_filename(artifact_id, version, classifier)
    dest_path = dest_dir / jar_name

    if dest_path.exists():
        if on_event:
            on_event(f"  Cloud: {jar_name} (cached)")
        return True

    url = _maven_jar_url(group_id, artifact_id, version, classifier)

    try:
        import urllib.request

        req = urllib.request.Request(url, headers={"User-Agent": "DVT-Sync/1.0"})
        with urllib.request.urlopen(req, timeout=120) as resp:
            if resp.status != 200:
                if on_event:
                    on_event(f"  Cloud: skipped {jar_name} (HTTP {resp.status})")
                return False
            dest_path.write_bytes(resp.read())
        if on_event:
            on_event(f"  Cloud: downloaded {jar_name}")
        return True
    except Exception as e:
        if on_event:
            on_event(f"  Cloud: failed to download {jar_name}: {e}")
        return False


def download_cloud_jars(
    bucket_types: List[str],
    dest_dir: Path,
    on_event: Optional[Callable[[str], None]] = None,
) -> int:
    """Download cloud connector JARs for given bucket types.

    Args:
        bucket_types: List of bucket types (e.g., ["s3", "gcs", "azure"])
        dest_dir: Destination directory for JARs
        on_event: Optional callback for progress messages

    Returns:
        Number of JARs successfully present after download (downloaded or cached)
    """
    jar_coords = get_cloud_jars_for_bucket_types(bucket_types)
    if not jar_coords:
        return 0

    dest_dir = Path(dest_dir)
    ok = 0

    for group_id, artifact_id, version, classifier in jar_coords:
        if _download_cloud_jar(
            group_id, artifact_id, version, classifier, dest_dir, on_event
        ):
            ok += 1

    return ok


def get_bucket_types_from_config(
    profiles_dir: Optional[str] = None,
) -> List[str]:
    """Get unique bucket types from buckets.yml (excluding 'filesystem' and 'hdfs').

    Args:
        profiles_dir: Optional profiles directory path

    Returns:
        List of unique cloud bucket types (e.g., ["s3", "gcs"])
    """
    from dvt.config.user_config import load_buckets_config

    bucket_types: set = set()
    buckets_config = load_buckets_config(profiles_dir)

    if not buckets_config:
        return []

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
            # Only include cloud bucket types that need JARs
            if bucket_type in ("s3", "gcs", "azure"):
                bucket_types.add(bucket_type)

    return list(bucket_types)


def check_cloud_jars_available(
    bucket_types: List[str],
    profiles_dir: Optional[str] = None,
) -> Dict[str, bool]:
    """Check if cloud connector JARs are available for given bucket types.

    Args:
        bucket_types: List of bucket types to check
        profiles_dir: Optional profiles directory path

    Returns:
        Dict mapping bucket type to availability (True if all JARs present)
    """
    from dvt.config.user_config import get_spark_jars_dir

    spark_jars_dir = get_spark_jars_dir(profiles_dir)
    result: Dict[str, bool] = {}

    for bucket_type in bucket_types:
        if bucket_type not in BUCKET_TO_JARS:
            result[bucket_type] = True  # No JARs needed
            continue

        jar_coords = get_cloud_jars_for_bucket_types([bucket_type])
        all_present = True

        for group_id, artifact_id, version, classifier in jar_coords:
            jar_name = _jar_filename(artifact_id, version, classifier)
            if not (spark_jars_dir / jar_name).exists():
                all_present = False
                break

        result[bucket_type] = all_present

    return result
