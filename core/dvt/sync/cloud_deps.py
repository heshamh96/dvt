"""
Install cloud SDK dependencies based on bucket types found in profiles.yml.
"""

import subprocess
import sys
from typing import Dict, List, Set, Tuple

# Maps bucket type → pip package(s) needed
BUCKET_TO_PACKAGES: Dict[str, List[str]] = {
    "s3": ["boto3"],
    "gcs": ["google-cloud-storage"],
    "azure": ["azure-storage-blob"],
}


def get_required_cloud_packages(adapter_types: Set[str]) -> List[str]:
    """Map bucket types to pip package names."""
    packages = []
    for adapter_type in sorted(adapter_types):
        pkgs = BUCKET_TO_PACKAGES.get(adapter_type, [])
        packages.extend(pkgs)
    return packages


def install_cloud_packages(
    packages: List[str], dry_run: bool = False
) -> List[Tuple[str, str]]:
    """Install cloud SDK packages. Returns list of (package, status) tuples."""
    results = []
    for package in packages:
        try:
            __import__(package.replace("-", "_").split("-")[0])
            results.append((package, "already_installed"))
            continue
        except ImportError:
            pass

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
