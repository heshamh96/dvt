import re
import shutil
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Optional

from dvt import deprecations
from dvt.cli.flags import Flags
from dvt.config.project import Project
from dvt.config.user_config import (
    get_bucket_path,
    get_dvt_home,
    load_buckets_for_profile,
)
from dvt.events.types import CheckCleanPath, ConfirmCleanPath, FinishedCleanPaths
from dvt.task.base import BaseTask, move_to_nearest_project_dir
from dbt_common.events.functions import fire_event
from dbt_common.exceptions import DbtRuntimeError


def parse_duration(duration_str: str) -> timedelta:
    """Parse duration string like '24h' or '7d' into timedelta.

    Args:
        duration_str: Duration string (e.g., '24h', '7d', '48h')

    Returns:
        timedelta object

    Raises:
        ValueError: If format is invalid
    """
    match = re.match(r"^(\d+)(h|d)$", duration_str.lower())
    if not match:
        raise ValueError(
            f"Invalid duration format: '{duration_str}'. "
            f"Expected format: Nh (hours) or Nd (days), e.g., '24h', '7d'"
        )

    value = int(match.group(1))
    unit = match.group(2)

    if unit == "h":
        return timedelta(hours=value)
    else:  # 'd'
        return timedelta(days=value)


class CleanTask(BaseTask):
    """Clean dbt artifacts and DVT staging files."""

    def __init__(self, args: Flags, config: Project):
        super().__init__(args)
        self.config = config
        self.project = config
        # DVT-specific flags for bucket cleaning
        self.bucket_name: Optional[str] = getattr(args, "BUCKET", None) or getattr(
            args, "bucket", None
        )
        self.older_than: Optional[str] = getattr(args, "OLDER_THAN", None) or getattr(
            args, "older_than", None
        )
        self.profile_name: Optional[str] = getattr(args, "PROFILE", None) or getattr(
            args, "profile", None
        )

    def run(self) -> None:
        """Clean dbt artifacts and optionally DVT staging files.

        Behavior:
        - `dvt clean`: Run dbt clean + clean all DVT staging buckets
        - `dvt clean --bucket local`: Clean only the specified bucket (skip dbt clean)
        - `dvt clean --older-than 24h`: Only clean files older than duration (skip dbt clean)
        """
        # If specific bucket or older_than is specified, only clean staging
        if self.bucket_name or self.older_than:
            self._clean_staging()
            return None

        # Otherwise, run dbt clean + clean all staging
        self._run_dbt_clean()
        self._clean_staging()
        return None

    def _run_dbt_clean(self) -> None:
        """Run dbt's standard clean logic."""
        project_dir = move_to_nearest_project_dir(self.args.project_dir)

        potential_clean_paths = set(
            Path(p).resolve() for p in self.project.clean_targets
        )
        source_paths = set(
            Path(p).resolve()
            for p in (*self.project.all_source_paths, *self.project.test_paths)
        )
        clean_paths = potential_clean_paths.difference(source_paths)

        if potential_clean_paths != clean_paths:
            raise DbtRuntimeError(
                f"dvt will not clean the following source paths: {[str(s) for s in source_paths.intersection(potential_clean_paths)]}"
            )

        paths_outside_project = set(
            path for path in clean_paths if project_dir not in path.absolute().parents
        )
        if paths_outside_project and self.args.clean_project_files_only:
            raise DbtRuntimeError(
                f"dvt will not clean the following directories outside the project: {[str(p) for p in paths_outside_project]}"
            )

        if (
            "dbt_modules" in self.project.clean_targets
            and self.config.packages_install_path not in self.config.clean_targets
        ):
            deprecations.warn("install-packages-path")

        for path in clean_paths:
            fire_event(CheckCleanPath(path=str(path)))
            shutil.rmtree(path, True)
            fire_event(ConfirmCleanPath(path=str(path)))

        fire_event(FinishedCleanPaths())

    def _clean_staging(self) -> None:
        """Clean DVT staging buckets."""
        profiles_dir = getattr(self.args, "PROFILES_DIR", None)
        profile_name = self.profile_name or "default"

        # Load bucket configuration for the profile
        profile_buckets = load_buckets_for_profile(profile_name, profiles_dir)
        if not profile_buckets:
            # No buckets configured, nothing to clean
            return

        buckets = profile_buckets.get("buckets", {})
        if not isinstance(buckets, dict):
            return

        cutoff_time: Optional[datetime] = None
        if self.older_than:
            delta = parse_duration(self.older_than)
            cutoff_time = datetime.now() - delta

        for bucket_name, bucket_config in buckets.items():
            if not isinstance(bucket_config, dict):
                continue

            # Skip if specific bucket requested and this isn't it
            if self.bucket_name and bucket_name != self.bucket_name:
                continue

            bucket_type = bucket_config.get("type", "filesystem")

            if bucket_type == "filesystem":
                self._clean_filesystem_bucket(
                    bucket_name, bucket_config, cutoff_time, profiles_dir
                )
            elif bucket_type == "hdfs":
                self._clean_hdfs_bucket(bucket_name, bucket_config, cutoff_time)
            elif bucket_type == "s3":
                self._clean_s3_bucket(bucket_name, bucket_config, cutoff_time)
            elif bucket_type == "gcs":
                self._clean_gcs_bucket(bucket_name, bucket_config, cutoff_time)
            elif bucket_type == "azure":
                self._clean_azure_bucket(bucket_name, bucket_config, cutoff_time)

    def _clean_filesystem_bucket(
        self,
        name: str,
        config: Dict[str, Any],
        cutoff_time: Optional[datetime],
        profiles_dir: Optional[str] = None,
    ) -> None:
        """Clean local filesystem staging including hash state."""
        path = get_bucket_path(config, profiles_dir=profiles_dir)
        if not path or not path.exists():
            return

        if cutoff_time:
            # Only delete files older than cutoff
            self._delete_old_files(path, cutoff_time)
        else:
            # Delete everything including _state/ directory
            shutil.rmtree(path)
            path.mkdir(parents=True, exist_ok=True)

        # Also clean _state/ directory (hash state for incremental extraction)
        state_path = path / "_state"
        if state_path.exists() and not cutoff_time:
            shutil.rmtree(state_path)
            state_path.mkdir(parents=True, exist_ok=True)

        fire_event(ConfirmCleanPath(path=f"staging bucket: {name}"))

    def _delete_old_files(self, path: Path, cutoff_time: datetime) -> None:
        """Delete files older than cutoff_time."""
        cutoff_timestamp = cutoff_time.timestamp()

        for file_path in path.rglob("*"):
            if file_path.is_file():
                if file_path.stat().st_mtime < cutoff_timestamp:
                    file_path.unlink()

        # Clean up empty directories
        for dir_path in sorted(path.rglob("*"), reverse=True):
            if dir_path.is_dir() and not any(dir_path.iterdir()):
                dir_path.rmdir()

    def _clean_hdfs_bucket(
        self,
        name: str,
        config: Dict[str, Any],
        cutoff_time: Optional[datetime],
    ) -> None:
        """Clean HDFS staging including hash state using Spark's Hadoop filesystem API."""
        try:
            from pyspark.sql import SparkSession
        except ImportError:
            fire_event(
                ConfirmCleanPath(
                    path=f"staging bucket: {name} (skipped - pyspark not installed)"
                )
            )
            return

        hdfs_path = config.get("path")
        if not hdfs_path:
            raise DbtRuntimeError(f"HDFS bucket '{name}' requires 'path' configuration")

        spark = SparkSession.builder.getOrCreate()
        hadoop_conf = spark._jsc.hadoopConfiguration()

        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
            spark._jvm.java.net.URI(hdfs_path), hadoop_conf
        )
        hadoop_path = spark._jvm.org.apache.hadoop.fs.Path(hdfs_path)

        if cutoff_time:
            cutoff_ms = int(cutoff_time.timestamp() * 1000)
            self._delete_old_hdfs_files(fs, hadoop_path, cutoff_ms, spark)
        else:
            # Delete everything including _state/ directory
            if fs.exists(hadoop_path):
                fs.delete(hadoop_path, True)  # recursive
            fs.mkdirs(hadoop_path)

            # Recreate _state/ directory
            state_path = spark._jvm.org.apache.hadoop.fs.Path(hdfs_path + "/_state")
            fs.mkdirs(state_path)

        fire_event(ConfirmCleanPath(path=f"staging bucket: {name}"))

    def _delete_old_hdfs_files(self, fs, path, cutoff_ms: int, spark) -> None:
        """Delete HDFS files older than cutoff timestamp (milliseconds)."""
        if not fs.exists(path):
            return

        for status in fs.listStatus(path):
            file_path = status.getPath()
            if status.isDirectory():
                self._delete_old_hdfs_files(fs, file_path, cutoff_ms, spark)
                if len(fs.listStatus(file_path)) == 0:
                    fs.delete(file_path, False)
            else:
                if status.getModificationTime() < cutoff_ms:
                    fs.delete(file_path, False)

    def _clean_s3_bucket(
        self,
        name: str,
        config: Dict[str, Any],
        cutoff_time: Optional[datetime],
    ) -> None:
        """Clean S3 staging including hash state in _state/ prefix."""
        try:
            import boto3
        except ImportError:
            raise DbtRuntimeError(
                f"S3 bucket '{name}' requires boto3. "
                f"Run 'dvt sync' to install cloud storage dependencies."
            )

        bucket = config.get("bucket")
        prefix = config.get("prefix", "")
        region = config.get("region")

        if not bucket:
            raise DbtRuntimeError(f"S3 bucket '{name}' requires 'bucket' configuration")

        client_kwargs: Dict[str, Any] = {}
        if region:
            client_kwargs["region_name"] = region
        if config.get("access_key_id"):
            client_kwargs["aws_access_key_id"] = config["access_key_id"]
            client_kwargs["aws_secret_access_key"] = config["secret_access_key"]

        s3 = boto3.client("s3", **client_kwargs)

        # Clean both data and _state/ prefixes
        prefixes_to_clean = [prefix]
        if not cutoff_time:
            # Also clean hash state when doing full clean
            state_prefix = prefix.rstrip("/") + "/_state/" if prefix else "_state/"
            prefixes_to_clean.append(state_prefix)

        paginator = s3.get_paginator("list_objects_v2")
        objects_to_delete = []

        for clean_prefix in prefixes_to_clean:
            for page in paginator.paginate(Bucket=bucket, Prefix=clean_prefix):
                for obj in page.get("Contents", []):
                    if cutoff_time:
                        obj_time = obj["LastModified"].replace(tzinfo=None)
                        if obj_time < cutoff_time:
                            objects_to_delete.append({"Key": obj["Key"]})
                    else:
                        objects_to_delete.append({"Key": obj["Key"]})

        for i in range(0, len(objects_to_delete), 1000):
            batch = objects_to_delete[i : i + 1000]
            if batch:
                s3.delete_objects(Bucket=bucket, Delete={"Objects": batch})

        fire_event(ConfirmCleanPath(path=f"staging bucket: {name}"))

    def _clean_gcs_bucket(
        self,
        name: str,
        config: Dict[str, Any],
        cutoff_time: Optional[datetime],
    ) -> None:
        """Clean GCS staging including hash state in _state/ prefix."""
        try:
            from google.cloud import storage
        except ImportError:
            raise DbtRuntimeError(
                f"GCS bucket '{name}' requires google-cloud-storage. "
                f"Run 'dvt sync' to install cloud storage dependencies."
            )

        bucket_name = config.get("bucket")
        prefix = config.get("prefix", "")
        project = config.get("project")

        if not bucket_name:
            raise DbtRuntimeError(
                f"GCS bucket '{name}' requires 'bucket' configuration"
            )

        client_kwargs: Dict[str, Any] = {}
        if project:
            client_kwargs["project"] = project

        client = storage.Client(**client_kwargs)
        gcs_bucket = client.bucket(bucket_name)

        # Clean both data and _state/ prefixes
        prefixes_to_clean = [prefix]
        if not cutoff_time:
            # Also clean hash state when doing full clean
            state_prefix = prefix.rstrip("/") + "/_state/" if prefix else "_state/"
            prefixes_to_clean.append(state_prefix)

        for clean_prefix in prefixes_to_clean:
            blobs = gcs_bucket.list_blobs(prefix=clean_prefix)
            for blob in blobs:
                if cutoff_time:
                    blob_time = blob.updated.replace(tzinfo=None)
                    if blob_time < cutoff_time:
                        blob.delete()
                else:
                    blob.delete()

        fire_event(ConfirmCleanPath(path=f"staging bucket: {name}"))

    def _clean_azure_bucket(
        self,
        name: str,
        config: Dict[str, Any],
        cutoff_time: Optional[datetime],
    ) -> None:
        """Clean Azure Blob staging including hash state in _state/ prefix."""
        try:
            from azure.storage.blob import BlobServiceClient
        except ImportError:
            raise DbtRuntimeError(
                f"Azure bucket '{name}' requires azure-storage-blob. "
                f"Run 'dvt sync' to install cloud storage dependencies."
            )

        container_name = config.get("container")
        storage_account = config.get("storage_account")
        prefix = config.get("prefix", "")
        account_key = config.get("account_key")

        if not container_name or not storage_account:
            raise DbtRuntimeError(
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
            state_prefix = prefix.rstrip("/") + "/_state/" if prefix else "_state/"
            prefixes_to_clean.append(state_prefix)

        for clean_prefix in prefixes_to_clean:
            blobs = container.list_blobs(name_starts_with=clean_prefix)
            for blob in blobs:
                if cutoff_time:
                    blob_time = blob.last_modified.replace(tzinfo=None)
                    if blob_time < cutoff_time:
                        container.delete_blob(blob.name)
                else:
                    container.delete_blob(blob.name)
