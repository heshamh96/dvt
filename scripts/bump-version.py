#!/usr/bin/env python3
"""Bump DVT patch version (XXX part of 1.YY.XXX)"""

import re
import sys
from pathlib import Path

VERSION_FILE = Path(__file__).parent.parent / "core/dvt/__version__.py"


def bump():
    content = VERSION_FILE.read_text()
    match = re.search(r'version = "(\d+)\.(\d+)\.(\d+)"', content)
    if not match:
        raise ValueError("Could not parse version from __version__.py")

    major, minor, patch = match.groups()
    new_patch = int(patch) + 1
    new_version = f"{major}.{minor}.{new_patch}"

    new_content = re.sub(
        r'version = "\d+\.\d+\.\d+"',
        f'version = "{new_version}"',
        content,
    )
    VERSION_FILE.write_text(new_content)
    print(f"Bumped version: {major}.{minor}.{patch} -> {new_version}")
    return new_version


def show():
    """Show current version without bumping."""
    content = VERSION_FILE.read_text()
    match = re.search(r'version = "(\d+\.\d+\.\d+)"', content)
    if not match:
        raise ValueError("Could not parse version from __version__.py")
    print(match.group(1))
    return match.group(1)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--show":
        show()
    else:
        bump()
