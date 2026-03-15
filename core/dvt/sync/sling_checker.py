"""
Verify Sling binary availability and version.
"""

import shutil
import subprocess
from typing import Optional, Tuple


def check_sling() -> Tuple[bool, Optional[str]]:
    """Check if Sling is installed and return (available, version_string).

    Returns:
        (True, "1.5.12") if Sling is available
        (False, None) if Sling is not found
    """
    sling_path = shutil.which("sling")
    if not sling_path:
        return False, None

    try:
        result = subprocess.run(
            ["sling", "--version"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        version = result.stdout.strip()
        if not version:
            version = result.stderr.strip()
        # Extract version number from output like "sling v1.5.12"
        for part in version.split():
            if part.startswith("v") or part[0].isdigit():
                return True, part.lstrip("v")
        return True, version
    except (subprocess.TimeoutExpired, FileNotFoundError, OSError):
        return False, None
