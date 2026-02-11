"""
Adapter manager for cross-target operations.

Provides adapter instances for any target defined in profiles.yml,
cached by (profile_name, target_name) for reuse within a task.

Usage:
    from dvt.federation.adapter_manager import AdapterManager

    # Get adapter for a specific target
    adapter = AdapterManager.get_adapter(
        profile_name="my_profile",
        target_name="databricks_prod",
        profiles_dir="~/.dvt",
    )

    # Execute DDL via adapter
    with adapter.connection_named("dvt_loader"):
        adapter.execute("TRUNCATE TABLE my_schema.my_table")

    # Cleanup at end of task
    AdapterManager.cleanup()
"""

from dataclasses import dataclass, field
from pathlib import Path
from threading import Lock
from typing import Any, Dict, Optional, Tuple, Type

from dbt_common.events.base_types import EventLevel


@dataclass
class MinimalAdapterConfig:
    """Minimal config for adapter instantiation without full RuntimeConfig.

    Implements the AdapterRequiredConfig protocol with minimal fields
    needed for adapter instantiation, SQL execution, and compilation
    (target-aware compilation passes these adapters through ProviderContext).
    """

    # HasCredentials protocol
    credentials: Any  # Credentials object
    profile_name: str
    target_name: str
    threads: int = 4

    # AdapterRequiredConfig protocol
    project_name: str = "dvt_federation"
    query_comment: Any = None
    cli_vars: Dict[str, Any] = field(default_factory=dict)
    target_path: str = "target"
    log_cache_events: bool = False

    # Quoting config — used by RelationProxy during compilation.
    # Empty dict = use adapter defaults (each adapter defines its own quote policy).
    quoting: Dict[str, Any] = field(default_factory=dict)

    # Macro dispatch — used by BaseDatabaseWrapper._get_search_packages().
    # Empty list/dict = no custom dispatch order (use adapter defaults).
    dependencies: Dict[str, Any] = field(default_factory=dict)

    def get_macro_search_order(self, namespace: str) -> Optional[list]:
        """Return macro search order for namespace (used by DatabaseWrapper)."""
        return None

    def to_target_dict(self) -> Dict[str, Any]:
        """Return target configuration dictionary."""
        result = {"type": self.credentials.type, "threads": self.threads}
        if hasattr(self.credentials, "to_dict"):
            result.update(self.credentials.to_dict())
        return result


class AdapterManager:
    """Manages adapter instances per target.

    Thread-safe singleton that caches adapters by (profile_name, target_name).
    Use cleanup() at the end of tasks to release connections.
    """

    _adapters: Dict[Tuple[str, str], Any] = {}
    _lock: Lock = Lock()

    @classmethod
    def get_adapter(
        cls,
        profile_name: str,
        target_name: str,
        profiles_dir: Optional[str] = None,
    ) -> Any:
        """Get or create adapter for a specific target.

        Thread-safe and cached by (profile_name, target_name).

        Args:
            profile_name: Profile name from profiles.yml
            target_name: Target name within the profile
            profiles_dir: Optional profiles directory path

        Returns:
            Adapter instance for the target

        Raises:
            ValueError: If target not found or credentials invalid
            RuntimeError: If adapter instantiation fails
        """
        key = (profile_name, target_name)

        with cls._lock:
            if key not in cls._adapters:
                cls._adapters[key] = cls._create_adapter(
                    profile_name, target_name, profiles_dir
                )
            return cls._adapters[key]

    @classmethod
    def _create_adapter(
        cls,
        profile_name: str,
        target_name: str,
        profiles_dir: Optional[str],
    ) -> Any:
        """Create new adapter instance for target.

        Args:
            profile_name: Profile name
            target_name: Target name
            profiles_dir: Profiles directory

        Returns:
            New adapter instance
        """
        from dvt.adapters.factory import FACTORY, load_plugin
        from dvt.config.user_config import load_profiles
        from dvt.mp_context import get_mp_context

        # Load credentials from profiles.yml
        profiles = load_profiles(profiles_dir)
        profile = profiles.get(profile_name)
        if not profile:
            raise ValueError(f"Profile '{profile_name}' not found in profiles.yml")

        outputs = profile.get("outputs", {})
        target_config = outputs.get(target_name)
        if not target_config:
            available = list(outputs.keys())
            raise ValueError(
                f"Target '{target_name}' not found in profile '{profile_name}'. "
                f"Available targets: {available}"
            )

        adapter_type = target_config.get("type")
        if not adapter_type:
            raise ValueError(
                f"No 'type' specified for target '{target_name}' "
                f"in profile '{profile_name}'"
            )

        # Load the adapter plugin (registers credentials class)
        credentials_cls = load_plugin(adapter_type)

        # Create credentials object from config dict
        # The credentials class handles validation and transformation
        try:
            credentials = credentials_cls.from_dict(target_config)
        except Exception as e:
            raise ValueError(
                f"Failed to create credentials for target '{target_name}': {e}"
            ) from e

        # Create minimal config for adapter
        config = MinimalAdapterConfig(
            credentials=credentials,
            profile_name=profile_name,
            target_name=target_name,
            threads=target_config.get("threads", 4),
        )

        # Get adapter class and instantiate
        adapter_cls = FACTORY.get_adapter_class_by_name(adapter_type)
        mp_context = get_mp_context()

        try:
            adapter = adapter_cls(config, mp_context)
        except Exception as e:
            raise RuntimeError(
                f"Failed to instantiate {adapter_type} adapter "
                f"for target '{target_name}': {e}"
            ) from e

        return adapter

    @classmethod
    def cleanup(cls) -> None:
        """Cleanup all adapter connections.

        Call this at the end of tasks to release database connections.
        """
        with cls._lock:
            for adapter in cls._adapters.values():
                try:
                    adapter.cleanup_connections()
                except Exception:
                    pass  # Ignore cleanup errors
            cls._adapters.clear()

    @classmethod
    def clear(cls) -> None:
        """Clear adapter cache without cleanup.

        Primarily for testing. Use cleanup() in production.
        """
        with cls._lock:
            cls._adapters.clear()

    @classmethod
    def get_cached_targets(cls) -> list:
        """Get list of currently cached (profile, target) pairs.

        Returns:
            List of (profile_name, target_name) tuples
        """
        with cls._lock:
            return list(cls._adapters.keys())


def parse_table_name(
    table_name: str,
    default_schema: Optional[str] = None,
    default_database: Optional[str] = None,
) -> Dict[str, Optional[str]]:
    """Parse table name into components for Relation creation.

    Handles:
    - "table" -> {database: default, schema: default, identifier: table}
    - "schema.table" -> {database: default, schema: schema, identifier: table}
    - "catalog.schema.table" -> {database: catalog, schema: schema, identifier: table}

    Args:
        table_name: Fully or partially qualified table name
        default_schema: Default schema if not specified
        default_database: Default database/catalog if not specified

    Returns:
        Dict with database, schema, identifier keys
    """
    parts = table_name.split(".")

    if len(parts) == 1:
        return {
            "database": default_database,
            "schema": default_schema,
            "identifier": parts[0],
        }
    elif len(parts) == 2:
        return {
            "database": default_database,
            "schema": parts[0],
            "identifier": parts[1],
        }
    elif len(parts) == 3:
        return {
            "database": parts[0],
            "schema": parts[1],
            "identifier": parts[2],
        }
    else:
        raise ValueError(
            f"Invalid table name '{table_name}': expected 1-3 parts, got {len(parts)}"
        )


def create_relation(
    adapter: Any,
    table_name: str,
    default_schema: Optional[str] = None,
    default_database: Optional[str] = None,
) -> Any:
    """Create a Relation object with proper quoting for the adapter's dialect.

    Args:
        adapter: Adapter instance
        table_name: Table name (1, 2, or 3 parts)
        default_schema: Default schema if not in table_name
        default_database: Default database if not in table_name

    Returns:
        Relation object that renders with proper dialect quoting

    Example:
        relation = create_relation(adapter, "my_schema.my_table")
        sql = f"TRUNCATE TABLE {relation.render()}"
        # PostgreSQL: TRUNCATE TABLE "my_schema"."my_table"
        # Databricks: TRUNCATE TABLE `my_schema`.`my_table`
    """
    from dbt.adapters.contracts.relation import RelationType

    parts = parse_table_name(table_name, default_schema, default_database)

    # Get the Relation class for this adapter
    relation_cls = adapter.Relation

    return relation_cls.create(
        database=parts["database"],
        schema=parts["schema"],
        identifier=parts["identifier"],
        type=RelationType.Table,
    )


def get_quoted_table_name(
    adapter: Any,
    table_name: str,
    default_schema: Optional[str] = None,
    default_database: Optional[str] = None,
) -> str:
    """Get properly quoted table name for SQL statements.

    Convenience wrapper around create_relation().render().

    Args:
        adapter: Adapter instance
        table_name: Table name (1, 2, or 3 parts)
        default_schema: Default schema if not in table_name
        default_database: Default database if not in table_name

    Returns:
        Quoted table name string ready for SQL

    Example:
        quoted = get_quoted_table_name(adapter, "my_schema.my_table")
        adapter.execute(f"TRUNCATE TABLE {quoted}")
    """
    relation = create_relation(adapter, table_name, default_schema, default_database)
    return relation.render()
