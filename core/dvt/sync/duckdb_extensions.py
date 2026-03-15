"""
Install DuckDB extensions based on adapter types found in profiles.yml.
"""

from typing import Dict, List, Set, Tuple

# Maps adapter type → DuckDB extension(s) needed
ADAPTER_TO_EXTENSIONS: Dict[str, List[str]] = {
    "postgres": ["postgres_scanner"],
    "mysql": ["mysql_scanner"],
    "sqlite": ["sqlite_scanner"],
    "s3": ["httpfs"],
    "gcs": ["httpfs"],
    "azure": ["azure"],
}

# Always install these extensions
ALWAYS_INSTALL = ["delta", "json"]


def get_required_extensions(adapter_types: Set[str]) -> List[str]:
    """Determine which DuckDB extensions are needed."""
    extensions = set(ALWAYS_INSTALL)
    for adapter_type in adapter_types:
        exts = ADAPTER_TO_EXTENSIONS.get(adapter_type, [])
        extensions.update(exts)
    return sorted(extensions)


def install_extensions(
    extensions: List[str], dry_run: bool = False
) -> List[Tuple[str, str]]:
    """Install DuckDB extensions. Returns list of (extension, status) tuples.

    Status is one of: 'installed', 'already_installed', 'failed', 'dry_run'
    """
    results = []

    if dry_run:
        return [(ext, "dry_run") for ext in extensions]

    try:
        import duckdb

        conn = duckdb.connect(":memory:")
    except ImportError:
        return [(ext, "failed") for ext in extensions]

    for ext in extensions:
        try:
            # Check if already installed
            installed = conn.execute(
                "SELECT installed FROM duckdb_extensions() WHERE extension_name = ?",
                [ext],
            ).fetchone()

            if installed and installed[0]:
                results.append((ext, "already_installed"))
            else:
                conn.execute(f"INSTALL {ext}")
                results.append((ext, "installed"))
        except Exception:
            results.append((ext, "failed"))

    conn.close()
    return results
