# coding=utf-8
"""AWS Athena authentication handler for DVT federation.

Auth Methods:
- aws_profile: AWS profile-based authentication
- access_key: AWS access key/secret key authentication
"""

from typing import Any, Dict

from .base import BaseAuthHandler


class AthenaAuthHandler(BaseAuthHandler):
    """Handles AWS Athena authentication methods."""

    SUPPORTED_AUTH_METHODS = ["aws_profile", "access_key"]
    INTERACTIVE_AUTH_METHODS = []

    def detect_auth_method(self, creds: Dict[str, Any]) -> str:
        """Detect Athena auth method from credentials."""
        if creds.get("aws_access_key_id") and creds.get("aws_secret_access_key"):
            return "access_key"
        return "aws_profile"

    def get_jdbc_properties(self, creds: Dict[str, Any]) -> Dict[str, str]:
        """Build Athena JDBC properties."""
        auth_method = self.detect_auth_method(creds)
        props: Dict[str, str] = {}

        if auth_method == "access_key":
            props["User"] = str(creds.get("aws_access_key_id", ""))
            props["Password"] = str(creds.get("aws_secret_access_key", ""))
            if creds.get("aws_session_token"):
                props["SessionToken"] = str(creds["aws_session_token"])

        elif auth_method == "aws_profile":
            if creds.get("aws_profile_name"):
                props["AwsCredentialsProviderClass"] = (
                    "com.simba.athena.amazonaws.auth.profile.ProfileCredentialsProvider"
                )
                props["AwsCredentialsProviderArguments"] = str(
                    creds["aws_profile_name"]
                )

        # Required Athena settings
        if creds.get("s3_staging_dir"):
            props["S3OutputLocation"] = str(creds["s3_staging_dir"])
        if creds.get("region_name") or creds.get("region"):
            props["AwsRegion"] = str(
                creds.get("region_name") or creds.get("region", "")
            )
        if creds.get("work_group"):
            props["Workgroup"] = str(creds["work_group"])

        return props

    def get_jdbc_url_params(self, creds: Dict[str, Any]) -> str:
        """Get Athena JDBC URL params."""
        params = []
        if creds.get("region_name") or creds.get("region"):
            region = creds.get("region_name") or creds.get("region", "")
            params.append(f"AwsRegion={region}")
        if creds.get("s3_staging_dir"):
            params.append(f"S3OutputLocation={creds['s3_staging_dir']}")
        return ";".join(params)

    def get_native_connection_kwargs(self, creds: Dict[str, Any]) -> Dict[str, Any]:
        """Get kwargs for pyathena.connect()."""
        auth_method = self.detect_auth_method(creds)
        kwargs: Dict[str, Any] = {
            "region_name": creds.get("region_name") or creds.get("region", ""),
            "schema_name": creds.get("database") or creds.get("schema", ""),
            "s3_staging_dir": creds.get("s3_staging_dir", ""),
        }

        if creds.get("work_group"):
            kwargs["work_group"] = creds["work_group"]

        if auth_method == "access_key":
            kwargs["aws_access_key_id"] = creds.get("aws_access_key_id", "")
            kwargs["aws_secret_access_key"] = creds.get("aws_secret_access_key", "")
            if creds.get("aws_session_token"):
                kwargs["aws_session_token"] = creds["aws_session_token"]

        elif auth_method == "aws_profile":
            if creds.get("aws_profile_name"):
                kwargs["profile_name"] = creds["aws_profile_name"]

        return kwargs
