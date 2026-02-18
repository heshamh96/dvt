"""
Federation loader — single JDBC + adapter loader for all targets.

All adapter types use the same FederationLoader which loads via
Spark JDBC (parallel writes) + dbt adapter (DDL with proper quoting).
"""

from typing import Callable, Optional

from dvt.federation.loaders.base import (
    FederationLoader,
    LoadConfig,
    LoadResult,
)

# Backward compatibility aliases
BaseLoader = FederationLoader


def get_loader(
    adapter_type: str = "",
    on_progress: Optional[Callable[[str], None]] = None,
) -> FederationLoader:
    """Get the federation loader.

    Args:
        adapter_type: Ignored (kept for backward compatibility).
            All adapters use the same JDBC + adapter loader.
        on_progress: Optional callback for progress messages

    Returns:
        FederationLoader instance
    """
    return FederationLoader(on_progress)


__all__ = [
    "FederationLoader",
    "BaseLoader",
    "LoadConfig",
    "LoadResult",
    "get_loader",
]
