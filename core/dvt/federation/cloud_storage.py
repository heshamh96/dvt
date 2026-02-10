# coding=utf-8
"""
Cloud storage helper for extractors and loaders.

Provides unified interface for:
- Building Spark Hadoop configuration for cloud credentials
- Building Spark-compatible paths (s3a://, gs://, abfss://)
- Building native paths for COPY commands
- Building CREDENTIALS clauses for different database dialects

Supported cloud storage types:
- S3 (AWS S3, MinIO, LocalStack, etc.)
- GCS (Google Cloud Storage)
- Azure (Azure Blob Storage, ADLS Gen2)

Authentication methods per type:
- S3: Access keys, session token, IAM role, instance profile, custom endpoint
- GCS: Service account keyfile, inline JSON, application default, storage integration
- Azure: Account key, SAS token, service principal, managed identity
"""

import uuid
from typing import Any, Dict, Optional


class CloudStorageHelper:
    """Helper for cloud storage operations in extractors/loaders.

    This class provides a unified interface for working with cloud storage
    in both Spark operations (read/write DataFrames) and database COPY commands.

    Usage:
        helper = CloudStorageHelper(bucket_config)

        # For Spark session configuration
        spark_config = helper.get_spark_hadoop_config()

        # For Spark read/write paths
        spark_path = helper.get_spark_path("staging_12345/")

        # For database COPY commands
        native_path = helper.get_native_path("staging_12345/", dialect="snowflake")
        creds_clause = helper.get_copy_credentials_clause("snowflake")
    """

    def __init__(self, bucket_config: Dict[str, Any]):
        """Initialize with bucket configuration from buckets.yml.

        Args:
            bucket_config: Single bucket config dict with type, credentials, etc.
                Example:
                {
                    "type": "s3",
                    "bucket": "my-bucket",
                    "prefix": "staging/",
                    "access_key_id": "AKIA...",
                    "secret_access_key": "...",
                }
        """
        self.bucket_config = bucket_config
        self.bucket_type = bucket_config.get("type", "filesystem")

    # =========================================================================
    # Spark Hadoop Configuration
    # =========================================================================

    def get_spark_hadoop_config(self) -> Dict[str, str]:
        """Get Spark Hadoop config for this bucket's credentials.

        Returns dict of spark.hadoop.* keys to configure Hadoop filesystem.
        These should be passed to SparkSession.builder.config() at startup.

        Returns:
            Dict of Spark config key-value pairs
        """
        if self.bucket_type == "s3":
            return self._get_s3_hadoop_config()
        elif self.bucket_type == "gcs":
            return self._get_gcs_hadoop_config()
        elif self.bucket_type == "azure":
            return self._get_azure_hadoop_config()
        return {}

    def _get_s3_hadoop_config(self) -> Dict[str, str]:
        """Get Hadoop config for S3."""
        config: Dict[str, str] = {}
        bc = self.bucket_config

        # Use S3A filesystem implementation
        config["spark.hadoop.fs.s3a.impl"] = "org.apache.hadoop.fs.s3a.S3AFileSystem"

        # Custom endpoint (for S3-compatible storage like MinIO)
        if bc.get("endpoint"):
            config["spark.hadoop.fs.s3a.endpoint"] = bc["endpoint"]
            if bc.get("path_style_access"):
                config["spark.hadoop.fs.s3a.path.style.access"] = "true"

        # Auth: Access Keys (with optional session token)
        if bc.get("access_key_id"):
            config["spark.hadoop.fs.s3a.access.key"] = bc["access_key_id"]
            config["spark.hadoop.fs.s3a.secret.key"] = bc.get("secret_access_key", "")

            if bc.get("session_token"):
                config["spark.hadoop.fs.s3a.session.token"] = bc["session_token"]
                config["spark.hadoop.fs.s3a.aws.credentials.provider"] = (
                    "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider"
                )
            else:
                config["spark.hadoop.fs.s3a.aws.credentials.provider"] = (
                    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
                )

        # Auth: Instance Profile (EC2/ECS/EKS)
        elif bc.get("use_instance_profile"):
            config["spark.hadoop.fs.s3a.aws.credentials.provider"] = (
                "com.amazonaws.auth.InstanceProfileCredentialsProvider"
            )

        # Auth: IAM Role (via STS assume role)
        elif bc.get("iam_role"):
            # For Spark, we still need instance profile or keys to assume the role
            # The IAM role is primarily for database COPY commands
            config["spark.hadoop.fs.s3a.aws.credentials.provider"] = (
                "com.amazonaws.auth.InstanceProfileCredentialsProvider"
            )

        return config

    def _get_gcs_hadoop_config(self) -> Dict[str, str]:
        """Get Hadoop config for GCS."""
        config: Dict[str, str] = {}
        bc = self.bucket_config

        # Use GCS connector filesystem implementation
        config["spark.hadoop.fs.gs.impl"] = (
            "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem"
        )
        config["spark.hadoop.fs.AbstractFileSystem.gs.impl"] = (
            "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS"
        )

        # Auth: Service Account Key File
        if bc.get("keyfile"):
            config["spark.hadoop.google.cloud.auth.service.account.enable"] = "true"
            config["spark.hadoop.google.cloud.auth.service.account.json.keyfile"] = bc[
                "keyfile"
            ]

        # Auth: Service Account Key (inline JSON) - write to temp file
        elif bc.get("keyfile_json"):
            # Note: For inline JSON, caller should write to a temp file first
            # and set the path in bucket_config["keyfile"]
            config["spark.hadoop.google.cloud.auth.service.account.enable"] = "true"

        # Auth: Application Default Credentials
        elif bc.get("use_application_default"):
            config[
                "spark.hadoop.google.cloud.auth.application.default.credentials.enable"
            ] = "true"

        return config

    def _get_azure_hadoop_config(self) -> Dict[str, str]:
        """Get Hadoop config for Azure Blob/ADLS Gen2."""
        config: Dict[str, str] = {}
        bc = self.bucket_config
        storage_account = bc.get("storage_account", "")
        container = bc.get("container", "")

        # Auth: Storage Account Key
        if bc.get("account_key"):
            config[
                f"spark.hadoop.fs.azure.account.key.{storage_account}.blob.core.windows.net"
            ] = bc["account_key"]
            config[
                f"spark.hadoop.fs.azure.account.key.{storage_account}.dfs.core.windows.net"
            ] = bc["account_key"]

        # Auth: SAS Token
        elif bc.get("sas_token"):
            sas_token = bc["sas_token"]
            # Remove leading ? if present
            if sas_token.startswith("?"):
                sas_token = sas_token[1:]
            config[
                f"spark.hadoop.fs.azure.sas.{container}.{storage_account}.blob.core.windows.net"
            ] = sas_token
            config[
                f"spark.hadoop.fs.azure.sas.{container}.{storage_account}.dfs.core.windows.net"
            ] = sas_token

        # Auth: Service Principal (Azure AD / Entra ID)
        elif bc.get("client_id"):
            # For ABFS (Azure Blob FileSystem with OAuth)
            config["spark.hadoop.fs.azure.account.auth.type"] = "OAuth"
            config["spark.hadoop.fs.azure.account.oauth.provider.type"] = (
                "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
            )
            config[
                f"spark.hadoop.fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net"
            ] = f"https://login.microsoftonline.com/{bc.get('tenant_id', '')}/oauth2/token"
            config[
                f"spark.hadoop.fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net"
            ] = bc["client_id"]
            config[
                f"spark.hadoop.fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net"
            ] = bc.get("client_secret", "")

        # Auth: Managed Identity (Azure VMs/AKS)
        elif bc.get("use_managed_identity"):
            config["spark.hadoop.fs.azure.account.auth.type"] = "OAuth"
            config["spark.hadoop.fs.azure.account.oauth.provider.type"] = (
                "org.apache.hadoop.fs.azurebfs.oauth2.MsiTokenProvider"
            )

        return config

    # =========================================================================
    # Path Building
    # =========================================================================

    def get_spark_path(self, subpath: str = "") -> str:
        """Get Spark-compatible path for reading/writing via Hadoop.

        Args:
            subpath: Additional path suffix (e.g., "staging_abc123/")

        Returns:
            Full path with Spark-compatible scheme:
            - S3: s3a://bucket/prefix/subpath/
            - GCS: gs://bucket/prefix/subpath/
            - Azure: abfss://container@account.dfs.core.windows.net/prefix/subpath/
        """
        bc = self.bucket_config
        prefix = bc.get("prefix", "").strip("/")
        subpath = subpath.strip("/")
        path_parts = "/".join(filter(None, [prefix, subpath]))

        if self.bucket_type == "s3":
            bucket = bc.get("bucket", "")
            return f"s3a://{bucket}/{path_parts}/" if path_parts else f"s3a://{bucket}/"

        elif self.bucket_type == "gcs":
            bucket = bc.get("bucket", "")
            return f"gs://{bucket}/{path_parts}/" if path_parts else f"gs://{bucket}/"

        elif self.bucket_type == "azure":
            container = bc.get("container", "")
            storage_account = bc.get("storage_account", "")
            base = f"abfss://{container}@{storage_account}.dfs.core.windows.net"
            return f"{base}/{path_parts}/" if path_parts else f"{base}/"

        return ""

    def get_native_path(self, subpath: str = "", dialect: str = "") -> str:
        """Get cloud-native path for COPY/UNLOAD commands.

        Different databases use different path formats for the same cloud storage.

        Args:
            subpath: Additional path suffix
            dialect: Database dialect ('snowflake', 'redshift', 'bigquery', 'databricks')

        Returns:
            Native path format for the database's COPY command
        """
        bc = self.bucket_config
        prefix = bc.get("prefix", "").strip("/")
        subpath = subpath.strip("/")
        path_parts = "/".join(filter(None, [prefix, subpath]))

        if self.bucket_type == "s3":
            bucket = bc.get("bucket", "")
            # S3 uses s3:// for Snowflake/Redshift/Databricks COPY commands
            return f"s3://{bucket}/{path_parts}/" if path_parts else f"s3://{bucket}/"

        elif self.bucket_type == "gcs":
            bucket = bc.get("bucket", "")
            # Snowflake uses gcs://, BigQuery uses gs://
            scheme = "gcs" if dialect == "snowflake" else "gs"
            return (
                f"{scheme}://{bucket}/{path_parts}/"
                if path_parts
                else f"{scheme}://{bucket}/"
            )

        elif self.bucket_type == "azure":
            container = bc.get("container", "")
            storage_account = bc.get("storage_account", "")

            if dialect == "snowflake":
                # Snowflake uses azure:// format
                base = f"azure://{storage_account}.blob.core.windows.net/{container}"
                return f"{base}/{path_parts}/" if path_parts else f"{base}/"
            else:
                # Databricks uses abfss://
                base = f"abfss://{container}@{storage_account}.dfs.core.windows.net"
                return f"{base}/{path_parts}/" if path_parts else f"{base}/"

        return ""

    def generate_staging_path(self, name: str = "staging") -> str:
        """Generate a unique staging path suffix.

        Args:
            name: Base name for the staging folder

        Returns:
            Unique path like "staging_a1b2c3d4"
        """
        unique_id = str(uuid.uuid4())[:8]
        return f"{name}_{unique_id}"

    # =========================================================================
    # COPY Command Credentials
    # =========================================================================

    def get_copy_credentials_clause(self, dialect: str) -> str:
        """Get CREDENTIALS clause for COPY INTO/COPY/UNLOAD commands.

        Args:
            dialect: Database dialect ('snowflake', 'redshift', 'bigquery', 'databricks')

        Returns:
            SQL fragment for credentials (or empty string if using IAM/integration)
        """
        if self.bucket_type == "s3":
            return self._get_s3_credentials_clause(dialect)
        elif self.bucket_type == "gcs":
            return self._get_gcs_credentials_clause(dialect)
        elif self.bucket_type == "azure":
            return self._get_azure_credentials_clause(dialect)
        return ""

    def _get_s3_credentials_clause(self, dialect: str) -> str:
        """Get S3 credentials clause for COPY commands."""
        bc = self.bucket_config

        if dialect == "snowflake":
            # Snowflake S3 credentials
            if bc.get("storage_integration"):
                return f"STORAGE_INTEGRATION = {bc['storage_integration']}"
            elif bc.get("access_key_id"):
                return (
                    f"CREDENTIALS=(AWS_KEY_ID='{bc['access_key_id']}' "
                    f"AWS_SECRET_KEY='{bc.get('secret_access_key', '')}')"
                )

        elif dialect == "redshift":
            # Redshift COPY credentials
            if bc.get("iam_role"):
                return f"IAM_ROLE '{bc['iam_role']}'"
            elif bc.get("access_key_id"):
                return (
                    f"ACCESS_KEY_ID '{bc['access_key_id']}' "
                    f"SECRET_ACCESS_KEY '{bc.get('secret_access_key', '')}'"
                )

        elif dialect == "databricks":
            # Databricks COPY INTO credentials (for external locations)
            if bc.get("iam_role"):
                return f"WITH (CREDENTIAL (AWS_IAM_ROLE = '{bc['iam_role']}'))"
            # Databricks typically uses Unity Catalog or instance profile

        return ""

    def _get_gcs_credentials_clause(self, dialect: str) -> str:
        """Get GCS credentials clause for COPY commands."""
        bc = self.bucket_config

        if dialect == "snowflake":
            # Snowflake GCS credentials (requires storage integration)
            if bc.get("storage_integration"):
                return f"STORAGE_INTEGRATION = {bc['storage_integration']}"

        # BigQuery uses service account attached to the project
        # Databricks uses instance credentials or Unity Catalog

        return ""

    def _get_azure_credentials_clause(self, dialect: str) -> str:
        """Get Azure credentials clause for COPY commands."""
        bc = self.bucket_config

        if dialect == "snowflake":
            # Snowflake Azure credentials
            if bc.get("storage_integration"):
                return f"STORAGE_INTEGRATION = {bc['storage_integration']}"
            elif bc.get("sas_token"):
                sas_token = bc["sas_token"]
                # Ensure SAS token doesn't have leading ?
                if sas_token.startswith("?"):
                    sas_token = sas_token[1:]
                return f"CREDENTIALS=(AZURE_SAS_TOKEN='{sas_token}')"

        # Databricks uses Unity Catalog or instance credentials

        return ""

    # =========================================================================
    # Utility Methods
    # =========================================================================

    def get_region(self) -> Optional[str]:
        """Get the region for this bucket (if applicable).

        Returns:
            Region string or None if not applicable/configured
        """
        bc = self.bucket_config

        if self.bucket_type == "s3":
            return bc.get("region")
        # GCS and Azure don't use region in the same way

        return None

    def supports_purge(self, dialect: str) -> bool:
        """Check if this bucket/dialect combination supports PURGE option.

        PURGE automatically deletes staged files after successful load.

        Args:
            dialect: Database dialect

        Returns:
            True if PURGE is supported
        """
        if dialect == "snowflake":
            return True  # Snowflake COPY INTO supports PURGE = TRUE
        elif dialect == "databricks":
            return True  # Databricks COPY INTO supports file cleanup options
        return False

    def get_file_format_clause(self, dialect: str, format: str = "parquet") -> str:
        """Get FILE_FORMAT clause for COPY commands.

        Args:
            dialect: Database dialect
            format: File format ('parquet', 'csv', 'json')

        Returns:
            FILE_FORMAT clause for the COPY command
        """
        if dialect == "snowflake":
            if format == "parquet":
                return "FILE_FORMAT = (TYPE = PARQUET)"
            elif format == "csv":
                return "FILE_FORMAT = (TYPE = CSV)"
            elif format == "json":
                return "FILE_FORMAT = (TYPE = JSON)"

        elif dialect == "redshift":
            if format == "parquet":
                return "FORMAT AS PARQUET"
            elif format == "csv":
                return "FORMAT AS CSV"
            elif format == "json":
                return "FORMAT AS JSON"

        elif dialect == "databricks":
            if format == "parquet":
                return "FILEFORMAT = PARQUET"
            elif format == "csv":
                return "FILEFORMAT = CSV"
            elif format == "json":
                return "FILEFORMAT = JSON"

        return ""
