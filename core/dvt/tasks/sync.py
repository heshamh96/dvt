"""
DVT Sync Task — environment bootstrap.

Reads profiles.yml and installs all necessary dependencies:
- dbt adapter packages (dbt-postgres, dbt-snowflake, etc.)
- DuckDB extensions (postgres_scanner, mysql_scanner, delta, etc.)
- Cloud SDKs (boto3, google-cloud-storage, etc.)
- Verifies Sling binary availability
"""

from typing import Any, Dict

from dvt.sync.profiles_reader import (
    default_profiles_dir,
    extract_outputs,
    get_adapter_types,
    get_bucket_outputs,
    read_profiles_yml,
)
from dvt.sync.adapter_installer import get_required_packages, install_packages
from dvt.sync.duckdb_extensions import get_required_extensions, install_extensions
from dvt.sync.cloud_deps import get_required_cloud_packages, install_cloud_packages
from dvt.sync.sling_checker import check_sling


# Status indicators
OK = "OK"
INSTALLING = "installing..."
FAILED = "FAILED"
SKIP = "skip (dry-run)"


def _status_icon(status: str) -> str:
    if status == "already_installed":
        return "[installed]"
    elif status == "installed":
        return "[installed]"
    elif status == "failed":
        return "[FAILED]"
    elif status == "dry_run":
        return "[dry-run]"
    return f"[{status}]"


class DvtSyncTask:
    """Bootstrap the DVT environment from profiles.yml."""

    def __init__(self, flags: Any, cli_kwargs: Dict[str, Any]) -> None:
        self.flags = flags
        self.skip_test = cli_kwargs.get("skip_test", False)
        self.dry_run = cli_kwargs.get("dry_run", False)
        self.profiles_dir = (
            getattr(flags, "PROFILES_DIR", None) or default_profiles_dir()
        )

    def run(self) -> Any:
        print(f"\ndvt sync — reading profiles from: {self.profiles_dir}\n")

        # 1. Read profiles.yml
        try:
            raw_profiles = read_profiles_yml(self.profiles_dir)
        except FileNotFoundError as e:
            print(f"  ERROR: {e}")
            print("  Run 'dvt init' to create a project, or use --profiles-dir.")
            return {"success": False}
        except Exception as e:
            print(f"  ERROR reading profiles.yml: {e}")
            return {"success": False}

        outputs = extract_outputs(raw_profiles)
        adapter_types = get_adapter_types(outputs)

        if not adapter_types:
            print("  No outputs found in profiles.yml. Nothing to sync.")
            return {"success": True}

        print(f"  Found {len(outputs)} outputs across {len(raw_profiles)} profile(s)")
        print(f"  Adapter types: {', '.join(sorted(adapter_types))}")
        print()

        all_ok = True

        # 2. Install dbt adapters
        print("  Adapters:")
        db_types = {
            t for t in adapter_types if t not in {"s3", "gcs", "azure", "local"}
        }
        adapter_packages = get_required_packages(db_types)
        if adapter_packages:
            results = install_packages(adapter_packages, dry_run=self.dry_run)
            for pkg, status in results:
                pad = "." * max(1, 30 - len(pkg))
                print(f"    {pkg} {pad} {_status_icon(status)}")
                if status == "failed":
                    all_ok = False
        else:
            print("    (no database adapters needed)")
        print()

        # 3. Install cloud SDKs
        bucket_types = {t for t in adapter_types if t in {"s3", "gcs", "azure"}}
        if bucket_types:
            print("  Buckets:")
            cloud_packages = get_required_cloud_packages(bucket_types)
            results = install_cloud_packages(cloud_packages, dry_run=self.dry_run)
            for pkg, status in results:
                pad = "." * max(1, 30 - len(pkg))
                print(f"    {pkg} {pad} {_status_icon(status)}")
                if status == "failed":
                    all_ok = False
            print()

        # 4. Install DuckDB extensions
        print("  DuckDB:")
        try:
            import duckdb

            pad = "." * 24
            print(f"    core {pad} duckdb {duckdb.__version__} [installed]")
        except ImportError:
            pad = "." * 24
            print(f"    core {pad} [MISSING — install duckdb]")
            all_ok = False

        extensions = get_required_extensions(adapter_types)
        results = install_extensions(extensions, dry_run=self.dry_run)
        for ext, status in results:
            pad = "." * max(1, 30 - len(ext))
            print(f"    {ext} {pad} {_status_icon(status)}")
            if status == "failed":
                all_ok = False
        print()

        # 5. Check Sling
        print("  Sling:")
        sling_available, sling_version = check_sling()
        pad = "." * 24
        if sling_available:
            print(f"    binary {pad} sling {sling_version} [installed]")
        else:
            print(f"    binary {pad} [NOT FOUND]")
            print("    Install: https://docs.slingdata.io/sling-cli/getting-started")
            print("    Sling is required for cross-engine extraction.")
            # Not a hard failure — Sling is only needed for cross-engine models
        print()

        # 6. Summary
        if all_ok:
            print("  Sync complete. Environment ready.")
        else:
            print("  Sync completed with errors. Check output above.")

        return {"success": all_ok}

    @staticmethod
    def interpret_results(results: Any) -> bool:
        if results is None:
            return False
        if isinstance(results, dict):
            return results.get("success", False)
        return True
