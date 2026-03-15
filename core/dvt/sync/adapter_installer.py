"""
Install dbt adapter packages based on adapter types found in profiles.yml.
"""

import subprocess
import sys
from typing import Dict, List, Set, Tuple

# Maps adapter type → pip package name
ADAPTER_TO_PACKAGE: Dict[str, str] = {
    "postgres": "dbt-postgres",
    "snowflake": "dbt-snowflake",
    "bigquery": "dbt-bigquery",
    "redshift": "dbt-redshift",
    "mysql": "dbt-mysql",
    "sqlserver": "dbt-sqlserver",
    "databricks": "dbt-databricks",
    "oracle": "dbt-oracle",
    "trino": "dbt-trino",
    "clickhouse": "dbt-clickhouse",
    "duckdb": "dbt-duckdb",
    "sqlite": "dbt-sqlite",
    "spark": "dbt-spark",
    "fabric": "dbt-fabric",
    "exasol": "dbt-exasol",
    "teradata": "dbt-teradata",
    "vertica": "dbt-vertica",
}


def get_required_packages(adapter_types: Set[str]) -> List[str]:
    """Map adapter types to pip package names."""
    packages = []
    for adapter_type in sorted(adapter_types):
        pkg = ADAPTER_TO_PACKAGE.get(adapter_type)
        if pkg:
            packages.append(pkg)
    return packages


def check_installed(package: str) -> bool:
    """Check if a pip package is already installed."""
    try:
        __import__(package.replace("-", "_"))
        return True
    except ImportError:
        return False


def install_packages(
    packages: List[str], dry_run: bool = False
) -> List[Tuple[str, str]]:
    """Install pip packages. Returns list of (package, status) tuples.

    Status is one of: 'installed', 'already_installed', 'failed', 'dry_run'
    """
    results = []
    for package in packages:
        if check_installed(package):
            results.append((package, "already_installed"))
            continue

        if dry_run:
            results.append((package, "dry_run"))
            continue

        try:
            subprocess.check_call(
                [sys.executable, "-m", "pip", "install", package, "--quiet"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
            )
            results.append((package, "installed"))
        except subprocess.CalledProcessError:
            results.append((package, "failed"))

    return results
