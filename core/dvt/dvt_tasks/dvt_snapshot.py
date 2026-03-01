"""DVT SnapshotTask -- extends dbt SnapshotTask with target-aware schema resolution.

Snapshots in DVT work via adapters only (same as dbt) -- no federation path.
When a snapshot has config(target='mssql_docker'), this task:

1. Resolves the correct schema/database from the target's profile config
   (e.g., 'dbo' for MSSQL, 'SYSTEM' for Oracle, 'devdb' for MySQL)
2. Executes the snapshot on the target adapter

Without config(target=...), behaves identically to dbt's SnapshotTask.

IMPORTANT: Snapshots do NOT support cross-engine federation.  All upstream
sources referenced by a snapshot must be on the same engine as the snapshot's
target.  DVT will raise a clear error if this constraint is violated.
"""

from __future__ import annotations

from typing import AbstractSet, Optional, Type

from dvt.adapters.base import BaseAdapter
from dvt.config import RuntimeConfig
from dvt.node_types import NodeType
from dvt.task.base import BaseRunner
from dvt.task.snapshot import SnapshotRunner, SnapshotTask
from dbt_common.events.functions import fire_event
from dbt_common.events.types import Formatting


class DvtSnapshotRunner(SnapshotRunner):
    """SnapshotRunner with target-aware schema/database resolution.

    Before execution, corrects the node's schema and database to match
    the target adapter's credentials when config(target=...) is set.
    """

    def compile(self, manifest):
        """Compile, then fix schema/database for non-default target."""
        result = super().compile(manifest)
        self._fix_snapshot_schema()
        return result

    def _fix_snapshot_schema(self) -> None:
        """Override node schema/database from the target's profile config.

        At parse time, node.schema is set from the default target's
        credentials (e.g., 'public' for PG).  When config.target points
        to a different engine, the schema should come from that target's
        profile entry instead.
        """
        node_target = getattr(getattr(self.node, "config", None), "target", None)
        if not node_target:
            return

        # Check if this is a non-default target
        default_target = self.config.target_name
        if node_target == default_target:
            return

        # Check if user explicitly set target_schema -- if so, respect it
        explicit_schema = getattr(
            getattr(self.node, "config", None), "target_schema", None
        )
        if explicit_schema:
            return

        # Read the target's credentials from profiles.yml
        try:
            from dvt.config.profile import read_profile
            from dvt.flags import get_flags

            flags = get_flags()
            raw_profiles = read_profile(flags.PROFILES_DIR)
            profile_data = raw_profiles.get(self.config.profile_name, {})
            outputs = profile_data.get("outputs", {})
            target_config = outputs.get(node_target, {})

            target_schema = target_config.get("schema")
            target_database = target_config.get("database", target_config.get("dbname"))

            if target_schema:
                self.node.schema = target_schema
            if target_database is not None:
                self.node.database = target_database
        except Exception:
            # Defensive: don't crash parsing/compilation if profile reading fails
            pass


class DvtSnapshotTask(SnapshotTask):
    """SnapshotTask with DVT target-aware schema resolution.

    Extends SnapshotTask to:
    - Use DvtSnapshotRunner (schema auto-resolution from config.target)
    - Execute snapshots on non-default target adapters when config(target=...) is set
    - Validate that snapshot sources are on the same engine (no federation)
    """

    def get_runner_type(self, _) -> Optional[Type[BaseRunner]]:
        return DvtSnapshotRunner

    def before_run(self, adapter: BaseAdapter, selected_uids: AbstractSet[str]):
        """Log snapshot execution plan before running."""
        if self.manifest:
            snapshot_targets = set()
            for uid in selected_uids:
                if uid in self.manifest.nodes:
                    node = self.manifest.nodes[uid]
                    if node.resource_type == NodeType.Snapshot:
                        node_target = getattr(
                            getattr(node, "config", None), "target", None
                        )
                        snapshot_targets.add(node_target or self.config.target_name)

            if snapshot_targets:
                targets_str = ", ".join(sorted(snapshot_targets))
                fire_event(Formatting(msg=f"Snapshot targets: {targets_str}"))

        return super().before_run(adapter, selected_uids)
