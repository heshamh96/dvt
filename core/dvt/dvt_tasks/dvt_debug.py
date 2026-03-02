# coding=utf-8
"""DVT DebugTask — extends upstream DebugTask with the DVT dashboard flow.

This is the DVT-specific extension of DebugTask that adds:
- A rich table-formatted debug dashboard (targets, computes, buckets, connectors)
- Federation readiness checks (PySpark, Java, JDBC drivers, cloud JARs)
- Per-profile filtering with project-aware profile resolution
- Support for dvt_project.yml in addition to dbt_project.yml

The base DebugTask in dvt.task.debug remains identical to upstream dbt-core.
"""

import json
import os
import re
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import dbt_common.clients.system
import dbt_common.exceptions
import dvt.exceptions
from dvt.adapters.factory import get_adapter, register_adapter, reset_adapters
from dvt.artifacts.schemas.results import RunStatus
from dvt.cli.flags import Flags
from dvt.clients.yaml_helper import load_yaml_text
from dvt.config import PartialProject, Profile
from dvt.config.project import project_yml_path_if_exists
from dvt.config.renderer import DvtProjectYamlRenderer, ProfileRenderer
from dvt.config.user_config import (
    BUCKETS_PATH,
    COMPUTES_PATH,
    DVT_HOME,
    get_native_connectors_dir,
    get_spark_jars_dir,
    load_buckets_config,
    load_buckets_for_profile,
)
from dvt.constants import DBT_PROJECT_FILE_NAME
from dvt.events.types import DebugCmdOut, DebugCmdResult, OpenCommand
from dvt.links import ProfileConfigDocs
from dvt.mp_context import get_mp_context
from dvt.task.debug import (
    COULD_NOT_CONNECT_MESSAGE,
    DebugRunStatus,
    DebugTask,
    SubtaskStatus,
)
from dvt.version import get_installed_version
from dbt_common.events.functions import fire_event
from dbt_common.ui import green, red, yellow


# ============================================================
# Table Formatting Helpers
# ============================================================

TABLE_WIDTH = 100
# ANSI escape code pattern for stripping color codes when calculating visible length
ANSI_ESCAPE = re.compile(r"\x1b\[[0-9;]*m")


def _visible_len(text: str) -> int:
    """Return visible length of string (excluding ANSI escape codes)."""
    return len(ANSI_ESCAPE.sub("", text))


def _truncate(text: str, max_len: int) -> str:
    """Truncate text with ellipsis if needed."""
    if len(text) <= max_len:
        return text
    return text[: max_len - 3] + "..."


def _table_header(title: str, subtitle: str = "", width: int = TABLE_WIDTH) -> str:
    """Format table header: ┌─ Title ─ subtitle ─────────────────────┐"""
    content = f" {title} "
    if subtitle:
        content += f"─ {subtitle} "
    padding = width - len(content) - 2  # -2 for ┌ and ┐
    return f"┌─{content}{'─' * max(0, padding)}┐"


def _table_footer(width: int = TABLE_WIDTH) -> str:
    """Format table footer: └───────────────────────────────────────┘"""
    return f"└{'─' * (width - 2)}┘"


def _pad_cell(text: str, width: int) -> str:
    """Pad cell to width, accounting for ANSI escape codes."""
    visible = _visible_len(text)
    if visible >= width:
        # Need to truncate - but be careful with ANSI codes
        stripped = ANSI_ESCAPE.sub("", text)
        if len(stripped) > width:
            return stripped[: width - 3] + "..."
        return text
    padding = width - visible
    return text + " " * padding


def _table_row(cols: List[str], widths: List[int], marker: str = "│") -> str:
    """Format a table row with fixed column widths, accounting for ANSI colors."""
    cells = [_pad_cell(col, w) for col, w in zip(cols, widths)]
    return f"{marker} {' │ '.join(cells)} {marker}"


def _table_separator(widths: List[int]) -> str:
    """Format table row separator: ├───────┼───────┼───────┤"""
    parts = ["─" * (w + 2) for w in widths]
    return f"├{'┼'.join(parts)}┤"


def _status_icon(ok: bool) -> str:
    """Return colored status icon."""
    return green("✓") if ok else red("✗")


# ============================================================
# DVT-specific message overrides
# ============================================================

ONLY_PROFILE_MESSAGE = """
A project file (dbt_project.yml or dvt_project.yml) was not found in this directory.
Using the only profile `{}`.
""".lstrip()

MULTIPLE_PROFILE_MESSAGE = """
A project file (dbt_project.yml or dvt_project.yml) was not found in this directory.
dvt found the following profiles:
{}

To debug one of these profiles, run:
dvt debug --profile [profile-name]
""".lstrip()


# ============================================================
# DvtDebugTask
# ============================================================


class DvtDebugTask(DebugTask):
    """DebugTask with DVT dashboard: targets, computes, buckets, federation readiness."""

    def __init__(self, args: Flags) -> None:
        super().__init__(args)
        # Override project_path to find both dbt_project.yml and dvt_project.yml
        path_if_exists = project_yml_path_if_exists(str(self.project_dir))
        self.project_path = path_if_exists or os.path.join(
            self.project_dir, DBT_PROJECT_FILE_NAME
        )

    # ==============================
    # Override run() — DVT dashboard
    # ==============================

    def run(self) -> bool:
        # WARN: this is a legacy workflow that is not compatible with other runtime flags
        if self.args.config_dir:
            fire_event(
                OpenCommand(
                    open_cmd=dbt_common.clients.system.open_dir_cmd(),
                    profiles_dir=str(self.profiles_dir),
                )
            )
            return DebugRunStatus.SUCCESS.value

        # Get filter flags
        show_config = getattr(self.args, "config", False)
        show_manifest = getattr(self.args, "manifest", False)
        filter_target = getattr(self.args, "debug_target", None)
        filter_compute = getattr(self.args, "debug_compute", None)
        filter_bucket = getattr(self.args, "debug_bucket", None)
        connection_target = getattr(self.args, "connection", None)
        has_filters = self._has_filter_flags()

        # Header
        version: str = get_installed_version().to_version_string(skip_matcher=True)
        timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        fire_event(DebugCmdOut(msg="=" * TABLE_WIDTH))
        fire_event(DebugCmdOut(msg=f"DVT Debug │ v{version} │ {timestamp}"))
        fire_event(DebugCmdOut(msg="=" * TABLE_WIDTH))

        # If --connection is specified, just test that connection
        if connection_target:
            return self._debug_connection_target(connection_target)

        # If specific filters are passed, show only those sections
        if has_filters:
            if show_config:
                self._debug_config()
            if show_manifest:
                self._debug_manifest()
            if filter_target:
                self._debug_single_target(filter_target)
            if filter_compute:
                self._debug_single_compute(filter_compute)
            if filter_bucket:
                self._debug_single_bucket(filter_bucket)
            return DebugRunStatus.SUCCESS.value

        # Default: show current project's targets, computes, buckets
        self._debug_config()

        # Get the current project's profile name to filter output
        current_profile = self._get_current_profile_name()

        self._debug_targets_for_profile(current_profile)
        self._debug_computes_for_profile(current_profile)
        self._debug_buckets_for_profile(current_profile)
        self._debug_native_connectors()
        self._debug_federation_readiness()

        if not os.path.exists(self.project_path):
            fire_event(
                DebugCmdOut(
                    msg=yellow(
                        "\n⚠️  No project file found. Run from the project directory or use --project-dir."
                    )
                )
            )

        return DebugRunStatus.SUCCESS.value

    # ==============================
    # Override interpret_results()
    # ==============================

    def interpret_results(self, results):
        return results

    # ==============================
    # Override _choose_profile_names()
    # ==============================

    def _choose_profile_names(self) -> Tuple[List[str], str]:
        project_profile: Optional[str] = None
        if os.path.exists(self.project_path):
            try:
                partial = PartialProject.from_project_root(
                    os.path.dirname(self.project_path),
                    verify_version=bool(self.args.VERSION_CHECK),
                )
                renderer = DvtProjectYamlRenderer(None, self.cli_vars)
                project_profile = partial.render_profile_name(renderer)
            except dvt.exceptions.DvtProjectError:
                pass

        args_profile: Optional[str] = getattr(self.args, "profile", None)

        try:
            return [Profile.pick_profile_name(args_profile, project_profile)], ""
        except dbt_common.exceptions.DbtConfigError:
            pass
        # try to guess

        profiles = []
        if self.raw_profile_data:
            profiles = [k for k in self.raw_profile_data if k != "config"]
            if project_profile is None:
                summary_message = (
                    "Could not load project file (dbt_project.yml or dvt_project.yml)\n"
                )
            elif len(profiles) == 0:
                summary_message = "The profiles.yml has no profiles\n"
            elif len(profiles) == 1:
                summary_message = ONLY_PROFILE_MESSAGE.format(profiles[0])
            else:
                summary_message = MULTIPLE_PROFILE_MESSAGE.format(
                    "\n".join(" - {}".format(o) for o in profiles)
                )
        return profiles, summary_message

    # ==============================
    # Override attempt_connection()
    # ==============================

    @staticmethod
    def attempt_connection(profile) -> Optional[str]:
        """Return a string containing the error message, or None if there was no error.

        We call reset_adapters() before register_adapter() because the dbt
        adapter factory caches adapters by *type name* (e.g. "snowflake"),
        not by target name.  Without the reset, testing a second target of
        the same adapter type (e.g. sf_dev then sf_prod) silently reuses
        the first target's cached adapter, producing a false positive.
        """
        reset_adapters()
        register_adapter(profile, get_mp_context())
        adapter = get_adapter(profile)
        try:
            with adapter.connection_named("debug"):
                # is defined in adapter class
                adapter.debug_query()
        except Exception as exc:
            return COULD_NOT_CONNECT_MESSAGE.format(
                err=str(exc),
                url=ProfileConfigDocs,
            )
        return None

    # ================
    # DVT-specific methods
    # ================

    def _has_filter_flags(self) -> bool:
        """True if any specific filter flag is set (--config, --manifest, --target, --compute, --bucket)."""
        return bool(
            getattr(self.args, "config", False)
            or getattr(self.args, "manifest", False)
            or getattr(self.args, "debug_target", None)
            or getattr(self.args, "debug_compute", None)
            or getattr(self.args, "debug_bucket", None)
        )

    def _get_current_profile_name(self) -> Optional[str]:
        """Get the profile name from the current project's dbt_project.yml."""
        if not os.path.exists(self.project_path):
            return None
        try:
            partial = PartialProject.from_project_root(
                str(self.project_dir), verify_version=False
            )
            renderer = DvtProjectYamlRenderer(None, self.cli_vars)
            return partial.render_profile_name(renderer)
        except Exception:
            return None

    def _debug_config(self) -> None:
        """Display resolved configuration (paths and project info) as compact table."""
        dvt_dir = Path(self.profiles_dir) if self.profiles_dir else DVT_HOME
        profiles_path = Path(dvt_dir) / "profiles.yml"
        computes_path = Path(dvt_dir) / "computes.yml"
        buckets_path = Path(dvt_dir) / "buckets.yml"

        # Get project info if available
        project_name = None
        profile_name = None
        if os.path.exists(self.project_path):
            try:
                partial = PartialProject.from_project_root(
                    str(self.project_dir), verify_version=False
                )
                renderer = DvtProjectYamlRenderer(None, self.cli_vars)
                project_name = partial.project_name
                profile_name = partial.render_profile_name(renderer)
            except Exception:
                pass

        # Build config rows: (label, path, exists)
        widths = [12, 40, 3]  # Label, Path, Status
        fire_event(DebugCmdOut(msg=""))
        fire_event(DebugCmdOut(msg=_table_header("Configuration")))

        rows = [
            (
                "DVT Home",
                str(dvt_dir).replace(str(Path.home()), "~"),
                Path(dvt_dir).exists(),
            ),
            (
                "profiles.yml",
                str(profiles_path).replace(str(Path.home()), "~"),
                profiles_path.exists(),
            ),
            (
                "computes.yml",
                str(computes_path).replace(str(Path.home()), "~"),
                computes_path.exists(),
            ),
            (
                "buckets.yml",
                str(buckets_path).replace(str(Path.home()), "~"),
                buckets_path.exists(),
            ),
        ]

        # Add project row
        if project_name and profile_name:
            rows.append(("Project", f"{project_name} (profile: {profile_name})", True))
        elif os.path.exists(self.project_path):
            rows.append(("Project", os.path.basename(self.project_path), True))
        else:
            rows.append(("Project", "not found", False))

        for label, value, exists in rows:
            fire_event(
                DebugCmdOut(
                    msg=_table_row(
                        [label, _truncate(value, 40), _status_icon(exists)], widths
                    )
                )
            )

        fire_event(DebugCmdOut(msg=_table_footer()))

    def _get_profile_adapter_types(self, profile_name: Optional[str] = None) -> set:
        """Extract unique adapter types from profile targets.

        Args:
            profile_name: Specific profile to check.

        Returns:
            Set of adapter type strings (e.g., {'databricks', 'snowflake'})
        """
        adapter_types: set = set()

        if not self.raw_profile_data or not profile_name:
            return adapter_types

        profile = self.raw_profile_data.get(profile_name)
        if not isinstance(profile, dict):
            return adapter_types

        outputs = profile.get("outputs") or {}
        for target_config in outputs.values():
            if isinstance(target_config, dict):
                adapter_type = target_config.get("type")
                if adapter_type:
                    adapter_types.add(adapter_type)

        return adapter_types

    def _debug_targets_for_profile(self, profile_name: Optional[str]) -> None:
        """Display targets for the current project's profile as compact table."""
        if not profile_name:
            fire_event(DebugCmdOut(msg=""))
            fire_event(DebugCmdOut(msg=_table_header("Targets")))
            fire_event(
                DebugCmdOut(msg=_table_row(["No project found"], [TABLE_WIDTH - 4]))
            )
            fire_event(DebugCmdOut(msg=_table_footer()))
            return self._debug_all_targets()

        if not self.raw_profile_data:
            load_status = self._load_profile()
            if load_status.run_status != RunStatus.Success or not self.raw_profile_data:
                fire_event(DebugCmdOut(msg=""))
                fire_event(
                    DebugCmdOut(
                        msg=_table_header("Targets", f"profile: {profile_name}")
                    )
                )
                fire_event(
                    DebugCmdOut(
                        msg=_table_row(["No profiles.yml found"], [TABLE_WIDTH - 4])
                    )
                )
                fire_event(DebugCmdOut(msg=_table_footer()))
                return

        if profile_name not in self.raw_profile_data:
            fire_event(DebugCmdOut(msg=""))
            fire_event(
                DebugCmdOut(msg=_table_header("Targets", f"profile: {profile_name}"))
            )
            fire_event(
                DebugCmdOut(
                    msg=_table_row(
                        [f"Profile '{profile_name}' not found"], [TABLE_WIDTH - 4]
                    )
                )
            )
            fire_event(DebugCmdOut(msg=_table_footer()))
            return

        renderer = ProfileRenderer(self.cli_vars)
        profile = self.raw_profile_data[profile_name]
        if not isinstance(profile, dict):
            return

        outputs = profile.get("outputs") or {}
        if not outputs:
            fire_event(DebugCmdOut(msg=""))
            fire_event(
                DebugCmdOut(msg=_table_header("Targets", f"profile: {profile_name}"))
            )
            fire_event(
                DebugCmdOut(
                    msg=_table_row(["No targets configured"], [TABLE_WIDTH - 4])
                )
            )
            fire_event(DebugCmdOut(msg=_table_footer()))
            return

        active_target = profile.get("target", "dev")
        missing_adapters: set = set()

        # Build table
        fire_event(DebugCmdOut(msg=""))
        fire_event(
            DebugCmdOut(
                msg=_table_header(
                    "Targets", f"profile: {profile_name} ─ default: {active_target}"
                )
            )
        )

        # Column widths: Name(18), Type(14), Host(42), Status(14)
        widths = [18, 14, 42, 14]
        fire_event(
            DebugCmdOut(
                msg=_table_row(["Name", "Type", "Host/Account", "Status"], widths)
            )
        )
        fire_event(DebugCmdOut(msg=_table_separator(widths)))

        for target_name, target_config in outputs.items():
            if not isinstance(target_config, dict):
                continue
            is_active = target_name == active_target
            name_display = f"{target_name} →" if is_active else target_name
            adapter_type = target_config.get("type", "N/A")

            # Get host/account
            host = (
                target_config.get("host")
                or target_config.get("account")
                or target_config.get("database")
                or "N/A"
            )

            # Test connection
            err_detail: Optional[str] = None
            try:
                prof = Profile.from_raw_profile_info(
                    profile,
                    profile_name,
                    renderer=renderer,
                    target_override=target_name,
                )
                err = self.attempt_connection(prof)
                if err is None:
                    status = green("✓ OK")
                else:
                    status = red("✗ error")
                    err_detail = err
            except Exception as e:
                err_str = str(e)
                if "No module named" in err_str or "Could not find adapter" in err_str:
                    status = red("✗ missing")
                    if adapter_type and adapter_type != "N/A":
                        missing_adapters.add(adapter_type)
                else:
                    status = red("✗ error")
                    err_detail = err_str

            fire_event(
                DebugCmdOut(
                    msg=_table_row(
                        [name_display, adapter_type, _truncate(str(host), 42), status],
                        widths,
                    )
                )
            )
            if err_detail:
                # Show first meaningful line of the error, trimmed to table width
                first_line = err_detail.strip().split("\n")[0]
                fire_event(
                    DebugCmdOut(
                        msg=red(f"  └─ {_truncate(first_line, TABLE_WIDTH - 8)}")
                    )
                )

        fire_event(DebugCmdOut(msg=_table_footer()))

        if missing_adapters:
            adapters_list = ", ".join(sorted(missing_adapters))
            fire_event(
                DebugCmdOut(
                    msg=red(f"❌ Missing adapters: {adapters_list}")
                    + yellow(" → Run 'dvt sync'")
                )
            )

    def _debug_all_targets(self) -> None:
        """Display ALL targets from ALL profiles as compact tables."""
        if not self.raw_profile_data:
            load_status = self._load_profile()
            if load_status.run_status != RunStatus.Success or not self.raw_profile_data:
                fire_event(DebugCmdOut(msg=""))
                fire_event(DebugCmdOut(msg=_table_header("Targets", "all profiles")))
                fire_event(
                    DebugCmdOut(
                        msg=_table_row(["No profiles.yml found"], [TABLE_WIDTH - 4])
                    )
                )
                fire_event(DebugCmdOut(msg=_table_footer()))
                return

        renderer = ProfileRenderer(self.cli_vars)
        missing_adapters: set = set()
        # Column widths: Name(18), Type(14), Host(42), Status(14)
        widths = [18, 14, 42, 14]

        for profile_name, profile in self.raw_profile_data.items():
            if profile_name == "config" or not isinstance(profile, dict):
                continue

            outputs = profile.get("outputs") or {}
            if not outputs:
                continue

            active_target = profile.get("target", "dev")

            fire_event(DebugCmdOut(msg=""))
            fire_event(
                DebugCmdOut(
                    msg=_table_header(
                        "Targets", f"profile: {profile_name} ─ default: {active_target}"
                    )
                )
            )
            fire_event(
                DebugCmdOut(
                    msg=_table_row(["Name", "Type", "Host/Account", "Status"], widths)
                )
            )
            fire_event(DebugCmdOut(msg=_table_separator(widths)))

            for target_name, target_config in outputs.items():
                if not isinstance(target_config, dict):
                    continue
                is_active = target_name == active_target
                name_display = f"{target_name} →" if is_active else target_name
                adapter_type = target_config.get("type", "N/A")
                host = (
                    target_config.get("host")
                    or target_config.get("account")
                    or target_config.get("database")
                    or "N/A"
                )

                err_detail: Optional[str] = None
                try:
                    prof = Profile.from_raw_profile_info(
                        profile,
                        profile_name,
                        renderer=renderer,
                        target_override=target_name,
                    )
                    err = self.attempt_connection(prof)
                    if err is None:
                        status = green("✓ OK")
                    else:
                        status = red("✗ error")
                        err_detail = err
                except Exception as e:
                    err_str = str(e)
                    if (
                        "No module named" in err_str
                        or "Could not find adapter" in err_str
                    ):
                        status = red("✗ missing")
                        if adapter_type and adapter_type != "N/A":
                            missing_adapters.add(adapter_type)
                    else:
                        status = red("✗ error")
                        err_detail = err_str

                fire_event(
                    DebugCmdOut(
                        msg=_table_row(
                            [
                                name_display,
                                adapter_type,
                                _truncate(str(host), 42),
                                status,
                            ],
                            widths,
                        )
                    )
                )
                if err_detail:
                    first_line = err_detail.strip().split("\n")[0]
                    fire_event(
                        DebugCmdOut(
                            msg=red(f"  └─ {_truncate(first_line, TABLE_WIDTH - 8)}")
                        )
                    )

            fire_event(DebugCmdOut(msg=_table_footer()))

        if missing_adapters:
            adapters_list = ", ".join(sorted(missing_adapters))
            fire_event(
                DebugCmdOut(
                    msg=red(f"❌ Missing adapters: {adapters_list}")
                    + yellow(" → Run 'dvt sync'")
                )
            )

    def _debug_single_target(self, target_name: str) -> None:
        """Debug a specific target by name (searches all profiles)."""
        fire_event(DebugCmdOut(msg=f"\n--- Target: {target_name} ---"))
        if not self.raw_profile_data:
            load_status = self._load_profile()
            if load_status.run_status != RunStatus.Success or not self.raw_profile_data:
                fire_event(DebugCmdOut(msg="  No profiles.yml found or invalid."))
                return

        renderer = ProfileRenderer(self.cli_vars)
        found = False

        for profile_name, profile in self.raw_profile_data.items():
            if profile_name == "config" or not isinstance(profile, dict):
                continue

            outputs = profile.get("outputs") or {}
            if target_name not in outputs:
                continue

            found = True
            target_config = outputs[target_name]
            if not isinstance(target_config, dict):
                continue

            active_target = profile.get("target", "dev")
            is_active = target_name == active_target

            fire_event(DebugCmdOut(msg=f"\n  Profile: {profile_name}"))
            fire_event(DebugCmdOut(msg=f"  Active: {'Yes' if is_active else 'No'}"))
            fire_event(DebugCmdOut(msg=f"  Type: {target_config.get('type', 'N/A')}"))

            for key in (
                "host",
                "port",
                "database",
                "schema",
                "account",
                "user",
                "warehouse",
                "role",
            ):
                if key in target_config and target_config[key] is not None:
                    fire_event(
                        DebugCmdOut(msg=f"  {key.capitalize()}: {target_config[key]}")
                    )

            # Test connection
            fire_event(DebugCmdOut(msg="\n  Testing connection..."))
            try:
                prof = Profile.from_raw_profile_info(
                    profile,
                    profile_name,
                    renderer=renderer,
                    target_override=target_name,
                )
                err = self.attempt_connection(prof)
                if err is None:
                    fire_event(DebugCmdOut(msg="  Connection: " + green("✓ Success")))
                else:
                    fire_event(DebugCmdOut(msg="  Connection: " + red("✗ Failed")))
                    fire_event(DebugCmdOut(msg=f"  Error: {err[:100]}"))
            except Exception as e:
                fire_event(DebugCmdOut(msg="  Connection: " + red(f"✗ {str(e)[:60]}")))
                if "No module named" in str(e) or "Could not find adapter" in str(e):
                    fire_event(
                        DebugCmdOut(
                            msg=yellow(
                                "  💡 Tip: Run 'dvt sync' to install the adapter."
                            )
                        )
                    )

        if not found:
            fire_event(
                DebugCmdOut(msg=f"  Target '{target_name}' not found in any profile.")
            )

    def _debug_computes_for_profile(self, profile_name: Optional[str]) -> None:
        """Display computes for the current project's profile as compact table."""
        if not profile_name:
            fire_event(DebugCmdOut(msg=""))
            fire_event(DebugCmdOut(msg=_table_header("Computes")))
            fire_event(
                DebugCmdOut(msg=_table_row(["No project found"], [TABLE_WIDTH - 4]))
            )
            fire_event(DebugCmdOut(msg=_table_footer()))
            return self._debug_all_computes()

        computes_path = (
            Path(self.profiles_dir) / "computes.yml"
            if self.profiles_dir
            else COMPUTES_PATH
        )

        if not computes_path.exists():
            fire_event(DebugCmdOut(msg=""))
            fire_event(
                DebugCmdOut(msg=_table_header("Computes", f"profile: {profile_name}"))
            )
            fire_event(
                DebugCmdOut(
                    msg=_table_row(
                        ["No computes.yml (using default local Spark)"],
                        [TABLE_WIDTH - 4],
                    )
                )
            )
            fire_event(DebugCmdOut(msg=_table_footer()))
            self._check_pyspark_status()
            return

        try:
            raw = load_yaml_text(
                dbt_common.clients.system.load_file_contents(str(computes_path))
            )
            raw = raw or {}
        except Exception as e:
            fire_event(DebugCmdOut(msg=""))
            fire_event(
                DebugCmdOut(msg=_table_header("Computes", f"profile: {profile_name}"))
            )
            fire_event(
                DebugCmdOut(
                    msg=_table_row([f"Error: {str(e)[:40]}"], [TABLE_WIDTH - 4])
                )
            )
            fire_event(DebugCmdOut(msg=_table_footer()))
            return

        if profile_name not in raw:
            fire_event(DebugCmdOut(msg=""))
            fire_event(
                DebugCmdOut(msg=_table_header("Computes", f"profile: {profile_name}"))
            )
            fire_event(
                DebugCmdOut(
                    msg=_table_row(
                        ["Profile not in computes.yml (using default)"],
                        [TABLE_WIDTH - 4],
                    )
                )
            )
            fire_event(DebugCmdOut(msg=_table_footer()))
            self._check_pyspark_status()
            return

        profile_block = raw[profile_name]
        if not isinstance(profile_block, dict) or "computes" not in profile_block:
            fire_event(DebugCmdOut(msg=""))
            fire_event(
                DebugCmdOut(msg=_table_header("Computes", f"profile: {profile_name}"))
            )
            fire_event(
                DebugCmdOut(
                    msg=_table_row(["No computes configured"], [TABLE_WIDTH - 4])
                )
            )
            fire_event(DebugCmdOut(msg=_table_footer()))
            self._check_pyspark_status()
            return

        active_compute = profile_block.get("target", "default")
        all_computes = profile_block.get("computes") or {}

        # Build table
        fire_event(DebugCmdOut(msg=""))
        fire_event(
            DebugCmdOut(
                msg=_table_header(
                    "Computes", f"profile: {profile_name} ─ default: {active_compute}"
                )
            )
        )

        # Column widths: Name(12), Type(8), Master(14), Version(10), Status(8)
        widths = [12, 8, 14, 10, 8]
        fire_event(
            DebugCmdOut(
                msg=_table_row(["Name", "Type", "Master", "Version", "Status"], widths)
            )
        )
        fire_event(DebugCmdOut(msg=_table_separator(widths)))

        for compute_name, cfg in all_computes.items():
            if not isinstance(cfg, dict):
                continue

            is_active = compute_name == active_compute
            name_display = f"{compute_name} →" if is_active else compute_name
            compute_type = cfg.get("type", "spark")
            master = cfg.get("master", "local[*]")
            version = cfg.get("version", "-")

            fire_event(
                DebugCmdOut(
                    msg=_table_row(
                        [
                            name_display,
                            compute_type,
                            _truncate(master, 14),
                            str(version),
                            green("✓"),
                        ],
                        widths,
                    )
                )
            )

        fire_event(DebugCmdOut(msg=_table_footer()))
        self._check_pyspark_status()

    def _debug_all_computes(self) -> None:
        """Display ALL computes from computes.yml as compact tables."""
        computes_path = (
            Path(self.profiles_dir) / "computes.yml"
            if self.profiles_dir
            else COMPUTES_PATH
        )

        if not computes_path.exists():
            fire_event(DebugCmdOut(msg=""))
            fire_event(DebugCmdOut(msg=_table_header("Computes", "all profiles")))
            fire_event(
                DebugCmdOut(
                    msg=_table_row(
                        ["No computes.yml (using default local Spark)"],
                        [TABLE_WIDTH - 4],
                    )
                )
            )
            fire_event(DebugCmdOut(msg=_table_footer()))
            self._check_pyspark_status()
            return

        try:
            raw = load_yaml_text(
                dbt_common.clients.system.load_file_contents(str(computes_path))
            )
            raw = raw or {}
        except Exception as e:
            fire_event(DebugCmdOut(msg=""))
            fire_event(DebugCmdOut(msg=_table_header("Computes", "all profiles")))
            fire_event(
                DebugCmdOut(
                    msg=_table_row([f"Error: {str(e)[:40]}"], [TABLE_WIDTH - 4])
                )
            )
            fire_event(DebugCmdOut(msg=_table_footer()))
            return

        widths = [12, 8, 14, 10, 8]

        for profile_name, profile_block in raw.items():
            if not isinstance(profile_block, dict) or "computes" not in profile_block:
                continue

            active_compute = profile_block.get("target", "default")
            all_computes = profile_block.get("computes") or {}

            fire_event(DebugCmdOut(msg=""))
            fire_event(
                DebugCmdOut(
                    msg=_table_header(
                        "Computes",
                        f"profile: {profile_name} ─ default: {active_compute}",
                    )
                )
            )
            fire_event(
                DebugCmdOut(
                    msg=_table_row(
                        ["Name", "Type", "Master", "Version", "Status"], widths
                    )
                )
            )
            fire_event(DebugCmdOut(msg=_table_separator(widths)))

            for compute_name, cfg in all_computes.items():
                if not isinstance(cfg, dict):
                    continue

                is_active = compute_name == active_compute
                name_display = f"{compute_name} →" if is_active else compute_name
                compute_type = cfg.get("type", "spark")
                master = cfg.get("master", "local[*]")
                version = cfg.get("version", "-")

                fire_event(
                    DebugCmdOut(
                        msg=_table_row(
                            [
                                name_display,
                                compute_type,
                                _truncate(master, 14),
                                str(version),
                                green("✓"),
                            ],
                            widths,
                        )
                    )
                )

            fire_event(DebugCmdOut(msg=_table_footer()))

        self._check_pyspark_status()

    def _debug_single_compute(self, compute_name: str) -> None:
        """Debug a specific compute by name."""
        fire_event(DebugCmdOut(msg=f"\n--- Compute: {compute_name} ---"))
        computes_path = (
            Path(self.profiles_dir) / "computes.yml"
            if self.profiles_dir
            else COMPUTES_PATH
        )

        if not computes_path.exists():
            fire_event(DebugCmdOut(msg="  No computes.yml found."))
            return

        try:
            raw = load_yaml_text(
                dbt_common.clients.system.load_file_contents(str(computes_path))
            )
            raw = raw or {}
        except Exception as e:
            fire_event(DebugCmdOut(msg=f"  Error reading computes.yml: {e}"))
            return

        # Search for the compute in all profiles
        found = False
        for profile_name, profile_block in raw.items():
            if not isinstance(profile_block, dict) or "computes" not in profile_block:
                continue

            all_computes = profile_block.get("computes") or {}
            if compute_name not in all_computes:
                continue

            found = True
            cfg = all_computes[compute_name]
            if not isinstance(cfg, dict):
                fire_event(
                    DebugCmdOut(msg=f"  Compute '{compute_name}' is misconfigured.")
                )
                continue

            active_compute = profile_block.get("target", "default")
            is_active = compute_name == active_compute

            fire_event(DebugCmdOut(msg=f"\n  Profile: {profile_name}"))
            fire_event(DebugCmdOut(msg=f"  Active: {'Yes' if is_active else 'No'}"))
            fire_event(DebugCmdOut(msg=f"  Type: {cfg.get('type', 'spark')}"))
            if cfg.get("version"):
                fire_event(DebugCmdOut(msg=f"  Version: {cfg.get('version')}"))
            fire_event(DebugCmdOut(msg=f"  Master: {cfg.get('master', 'local[*]')}"))

            # Show config settings
            config = cfg.get("config") or {}
            if config:
                fire_event(DebugCmdOut(msg="\n  Config:"))
                for k, v in config.items():
                    fire_event(DebugCmdOut(msg=f"    {k}: {v}"))

            # Test Spark connection
            if cfg.get("type", "spark") == "spark":
                self._test_spark_connection(cfg)

        if not found:
            fire_event(
                DebugCmdOut(
                    msg=f"  Compute '{compute_name}' not found in computes.yml."
                )
            )

    def _check_pyspark_status(self) -> None:
        """Check and display PySpark installation status inline."""
        try:
            from pyspark.sql import SparkSession  # noqa: F401

            fire_event(DebugCmdOut(msg="PySpark: " + green("✓ installed")))
        except ImportError:
            fire_event(
                DebugCmdOut(
                    msg="PySpark: "
                    + red("✗ not installed")
                    + yellow(" → Run 'dvt sync'")
                )
            )

    def _test_spark_connection(self, cfg: Dict[str, Any]) -> None:
        """Test Spark connection for a compute config."""
        fire_event(DebugCmdOut(msg="\n  Testing Spark connection..."))
        try:
            from pyspark.sql import SparkSession
        except ImportError:
            fire_event(DebugCmdOut(msg="  Spark: " + red("✗ PySpark not installed")))
            return

        master = cfg.get("master") or "local[*]"
        spark = None
        try:
            builder = SparkSession.builder.appName("dvt-debug-compute").master(master)
            for k, v in (cfg.get("config") or {}).items():
                builder = builder.config(k, v)
            spark = builder.getOrCreate()
            _ = spark.sparkContext.version
            fire_event(DebugCmdOut(msg="  Spark: " + green("✓ Connected")))
        except Exception as e:
            fire_event(DebugCmdOut(msg="  Spark: " + red(f"✗ {str(e)[:55]}")))
        finally:
            if spark is not None:
                try:
                    spark.stop()
                except Exception:
                    pass

    def _debug_manifest(self) -> None:
        """Display manifest summary from target/manifest.json."""
        fire_event(DebugCmdOut(msg="\n--- Manifest ---"))
        manifest_path = Path(self.project_dir) / "target" / "manifest.json"
        fire_event(DebugCmdOut(msg=f"\nManifest Path: {manifest_path}"))
        fire_event(
            DebugCmdOut(msg=f"  Exists: {'✓' if manifest_path.exists() else '✗'}")
        )
        if not manifest_path.exists():
            fire_event(
                DebugCmdOut(
                    msg="  Run 'dvt compile' or 'dvt parse' to generate manifest."
                )
            )
            return
        try:
            with open(manifest_path) as f:
                data = json.load(f)
            nodes = data.get("nodes") or {}
            sources = data.get("sources") or {}
            models = [n for n in nodes.values() if n.get("resource_type") == "model"]
            tests = [n for n in nodes.values() if n.get("resource_type") == "test"]
            seeds = [n for n in nodes.values() if n.get("resource_type") == "seed"]
            fire_event(DebugCmdOut(msg=f"\n  Models: {len(models)}"))
            fire_event(DebugCmdOut(msg=f"  Tests: {len(tests)}"))
            fire_event(DebugCmdOut(msg=f"  Seeds: {len(seeds)}"))
            fire_event(DebugCmdOut(msg=f"  Sources: {len(sources)}"))
            meta = data.get("metadata") or {}
            fire_event(
                DebugCmdOut(msg=f"\n  Generated: {meta.get('generated_at', 'Unknown')}")
            )
        except Exception as e:
            fire_event(DebugCmdOut(msg=f"  Error reading manifest: {e}"))

    def _debug_connection_target(self, target_name: str) -> bool:
        """Test connection to a specific target. Uses current project's profile."""
        fire_event(DebugCmdOut(msg=f"\n--- Testing Connection: {target_name} ---"))
        load_status = self._load_profile()
        if load_status.run_status != RunStatus.Success or self.raw_profile_data is None:
            fire_event(
                DebugCmdOut(msg="  Could not load profile or profiles.yml not found.")
            )
            return DebugRunStatus.FAIL.value
        try:
            partial = PartialProject.from_project_root(
                str(self.project_dir), verify_version=False
            )
            renderer = DvtProjectYamlRenderer(None, self.cli_vars)
            profile_name = partial.render_profile_name(renderer)
        except Exception as e:
            fire_event(DebugCmdOut(msg=f"  Could not get project profile name: {e}"))
            return DebugRunStatus.FAIL.value
        if profile_name not in self.raw_profile_data or profile_name == "config":
            fire_event(
                DebugCmdOut(msg=f"  Profile '{profile_name}' not in profiles.yml.")
            )
            return DebugRunStatus.FAIL.value
        if target_name not in (
            self.raw_profile_data[profile_name].get("outputs") or {}
        ):
            fire_event(
                DebugCmdOut(
                    msg=f"  Target '{target_name}' not found in profile '{profile_name}'."
                )
            )
            return DebugRunStatus.FAIL.value
        raw_profile = self.raw_profile_data[profile_name]
        renderer = ProfileRenderer(self.cli_vars)
        try:
            prof = Profile.from_raw_profile_info(
                raw_profile,
                profile_name,
                renderer=renderer,
                target_override=target_name,
            )
        except Exception as e:
            fire_event(DebugCmdOut(msg=f"  Error loading target: {e}"))
            return DebugRunStatus.FAIL.value
        for k, v in prof.credentials.connection_info():
            fire_event(DebugCmdOut(msg=f"  {k}: {v}"))
        err = self.attempt_connection(prof)
        if err is None:
            fire_event(DebugCmdOut(msg="  Connection: " + green("✓ Success")))
            return DebugRunStatus.SUCCESS.value
        fire_event(DebugCmdOut(msg="  Connection: " + red("✗ Failed")))
        fire_event(DebugCmdOut(msg=f"  {err}"))
        return DebugRunStatus.FAIL.value

    # ================
    # Federation Status (buckets, native connectors, readiness)
    # ================

    def _debug_buckets_for_profile(self, profile_name: Optional[str]) -> None:
        """Display buckets for the current project's profile as compact table."""
        if not profile_name:
            fire_event(DebugCmdOut(msg=""))
            fire_event(DebugCmdOut(msg=_table_header("Buckets")))
            fire_event(
                DebugCmdOut(msg=_table_row(["No project found"], [TABLE_WIDTH - 4]))
            )
            fire_event(DebugCmdOut(msg=_table_footer()))
            return self._debug_all_buckets()

        buckets_path = (
            Path(self.profiles_dir) / "buckets.yml"
            if self.profiles_dir
            else BUCKETS_PATH
        )

        if not buckets_path.exists():
            fire_event(DebugCmdOut(msg=""))
            fire_event(
                DebugCmdOut(msg=_table_header("Buckets", f"profile: {profile_name}"))
            )
            fire_event(
                DebugCmdOut(msg=_table_row(["Not configured"], [TABLE_WIDTH - 4]))
            )
            fire_event(DebugCmdOut(msg=_table_footer()))
            return

        profile_buckets = load_buckets_for_profile(
            profile_name, str(self.profiles_dir) if self.profiles_dir else None
        )
        if not profile_buckets:
            fire_event(DebugCmdOut(msg=""))
            fire_event(
                DebugCmdOut(msg=_table_header("Buckets", f"profile: {profile_name}"))
            )
            fire_event(
                DebugCmdOut(
                    msg=_table_row(["Profile not in buckets.yml"], [TABLE_WIDTH - 4])
                )
            )
            fire_event(DebugCmdOut(msg=_table_footer()))
            return

        default_target = profile_buckets.get("target", "default")
        buckets = profile_buckets.get("buckets", {})

        if not buckets:
            fire_event(DebugCmdOut(msg=""))
            fire_event(
                DebugCmdOut(msg=_table_header("Buckets", f"profile: {profile_name}"))
            )
            fire_event(
                DebugCmdOut(
                    msg=_table_row(["No buckets configured"], [TABLE_WIDTH - 4])
                )
            )
            fire_event(DebugCmdOut(msg=_table_footer()))
            return

        # Build table
        fire_event(DebugCmdOut(msg=""))
        fire_event(
            DebugCmdOut(
                msg=_table_header(
                    "Buckets", f"profile: {profile_name} ─ default: {default_target}"
                )
            )
        )

        # Column widths: Name(12), Type(6), Bucket(22), Prefix(12)
        widths = [12, 6, 22, 12]
        fire_event(
            DebugCmdOut(msg=_table_row(["Name", "Type", "Bucket", "Prefix"], widths))
        )
        fire_event(DebugCmdOut(msg=_table_separator(widths)))

        for bucket_name, bucket_cfg in buckets.items():
            if not isinstance(bucket_cfg, dict):
                continue
            is_default = bucket_name == default_target
            name_display = f"{bucket_name} →" if is_default else bucket_name
            bucket_type = bucket_cfg.get("type", "N/A")
            bucket_path = bucket_cfg.get("bucket", "N/A")
            prefix = bucket_cfg.get("prefix", "-")

            fire_event(
                DebugCmdOut(
                    msg=_table_row(
                        [
                            name_display,
                            bucket_type,
                            _truncate(str(bucket_path), 22),
                            _truncate(str(prefix), 12),
                        ],
                        widths,
                    )
                )
            )

        fire_event(DebugCmdOut(msg=_table_footer()))

    def _debug_all_buckets(self) -> None:
        """Display ALL buckets from buckets.yml as compact tables."""
        buckets_path = (
            Path(self.profiles_dir) / "buckets.yml"
            if self.profiles_dir
            else BUCKETS_PATH
        )

        if not buckets_path.exists():
            fire_event(DebugCmdOut(msg=""))
            fire_event(DebugCmdOut(msg=_table_header("Buckets", "all profiles")))
            fire_event(
                DebugCmdOut(msg=_table_row(["Not configured"], [TABLE_WIDTH - 4]))
            )
            fire_event(DebugCmdOut(msg=_table_footer()))
            return

        config = load_buckets_config(
            str(self.profiles_dir) if self.profiles_dir else None
        )
        if not config:
            fire_event(DebugCmdOut(msg=""))
            fire_event(DebugCmdOut(msg=_table_header("Buckets", "all profiles")))
            fire_event(DebugCmdOut(msg=_table_row(["Invalid YAML"], [TABLE_WIDTH - 4])))
            fire_event(DebugCmdOut(msg=_table_footer()))
            return

        widths = [12, 6, 22, 12]

        for profile_name, profile_buckets in config.items():
            if not isinstance(profile_buckets, dict):
                continue

            default_target = profile_buckets.get("target", "default")
            buckets = profile_buckets.get("buckets", {})

            if not buckets:
                continue

            fire_event(DebugCmdOut(msg=""))
            fire_event(
                DebugCmdOut(
                    msg=_table_header(
                        "Buckets",
                        f"profile: {profile_name} ─ default: {default_target}",
                    )
                )
            )
            fire_event(
                DebugCmdOut(
                    msg=_table_row(["Name", "Type", "Bucket", "Prefix"], widths)
                )
            )
            fire_event(DebugCmdOut(msg=_table_separator(widths)))

            for bucket_name, bucket_cfg in buckets.items():
                if not isinstance(bucket_cfg, dict):
                    continue
                is_default = bucket_name == default_target
                name_display = f"{bucket_name} →" if is_default else bucket_name
                bucket_type = bucket_cfg.get("type", "N/A")
                bucket_path = bucket_cfg.get("bucket", "N/A")
                prefix = bucket_cfg.get("prefix", "-")

                fire_event(
                    DebugCmdOut(
                        msg=_table_row(
                            [
                                name_display,
                                bucket_type,
                                _truncate(str(bucket_path), 22),
                                _truncate(str(prefix), 12),
                            ],
                            widths,
                        )
                    )
                )

            fire_event(DebugCmdOut(msg=_table_footer()))

    def _debug_single_bucket(self, bucket_name: str) -> None:
        """Debug a specific bucket by name (searches all profiles)."""
        fire_event(DebugCmdOut(msg=f"\n--- Bucket: {bucket_name} ---"))
        buckets_path = (
            Path(self.profiles_dir) / "buckets.yml"
            if self.profiles_dir
            else BUCKETS_PATH
        )

        if not buckets_path.exists():
            fire_event(DebugCmdOut(msg="  No buckets.yml found."))
            return

        config = load_buckets_config(
            str(self.profiles_dir) if self.profiles_dir else None
        )
        if not config:
            fire_event(DebugCmdOut(msg="  Error reading buckets.yml."))
            return

        found = False

        # Search for the bucket in all profiles
        for profile_name, profile_buckets in config.items():
            if not isinstance(profile_buckets, dict):
                continue

            buckets = profile_buckets.get("buckets", {})
            if not buckets or bucket_name not in buckets:
                continue

            bucket_cfg = buckets.get(bucket_name)
            if not bucket_cfg or not isinstance(bucket_cfg, dict):
                continue

            found = True
            default_target = profile_buckets.get("target", "default")
            is_default = bucket_name == default_target

            fire_event(DebugCmdOut(msg=f"\n  Profile: {profile_name}"))
            fire_event(DebugCmdOut(msg=f"  Default: {'Yes' if is_default else 'No'}"))

            bucket_type = bucket_cfg.get("type", "unknown")
            bucket_path = bucket_cfg.get("bucket", "not set")
            prefix = bucket_cfg.get("prefix", "")

            fire_event(DebugCmdOut(msg=f"  Type: {bucket_type}"))
            fire_event(DebugCmdOut(msg=f"  Bucket: {bucket_path}"))
            if prefix:
                fire_event(DebugCmdOut(msg=f"  Prefix: {prefix}"))
            fire_event(
                DebugCmdOut(msg=f"  Full Path: {bucket_type}://{bucket_path}/{prefix}")
            )

            # Show additional config (non-credential fields)
            for key in ("region", "storage_integration", "iam_role", "project"):
                if key in bucket_cfg:
                    fire_event(DebugCmdOut(msg=f"  {key}: {bucket_cfg[key]}"))

        if not found:
            fire_event(
                DebugCmdOut(msg=f"  Bucket '{bucket_name}' not found in any profile.")
            )

    def _debug_native_connectors(self) -> None:
        """Display native connector and JDBC driver availability as compact table."""
        # JDBC drivers and native connectors are always in DVT_HOME (~/.dvt/),
        # not in the project-local profiles_dir. dvt sync downloads them there.
        native_dir = get_native_connectors_dir(None)
        jdbc_dir = get_spark_jars_dir(None)

        fire_event(DebugCmdOut(msg=""))
        fire_event(DebugCmdOut(msg=_table_header("Connectors & Drivers")))

        # Column widths: Component(20), Status(70)
        widths = [20, 70]
        fire_event(DebugCmdOut(msg=_table_row(["Component", "Status"], widths)))
        fire_event(DebugCmdOut(msg=_table_separator(widths)))

        # Check JDBC drivers directory
        has_jdbc = (
            jdbc_dir.exists() and any(jdbc_dir.iterdir())
            if jdbc_dir.exists()
            else False
        )
        jdbc_status = green("✓ found") if has_jdbc else red("✗ not found")
        fire_event(DebugCmdOut(msg=_table_row(["JDBC Drivers", jdbc_status], widths)))

        # Get adapter types from current project's profile
        current_profile = self._get_current_profile_name()
        profile_adapters = self._get_profile_adapter_types(current_profile)

        # Check native connectors - only for adapters in the profile
        missing_connectors: list = []
        from dvt.dvt_tasks.lib.native_connectors import NATIVE_CONNECTORS

        if native_dir.exists():
            for adapter, spec in NATIVE_CONNECTORS.items():
                # Skip adapters not in profile
                if adapter not in profile_adapters:
                    continue

                jar_path = native_dir / spec.jar_name
                if jar_path.exists():
                    fire_event(
                        DebugCmdOut(
                            msg=_table_row(
                                [adapter, green(f"✓ {_truncate(spec.jar_name, 68)}")],
                                widths,
                            )
                        )
                    )
                else:
                    fire_event(
                        DebugCmdOut(
                            msg=_table_row([adapter, red("✗ not found")], widths)
                        )
                    )
                    missing_connectors.append(adapter)
        else:
            # Check if any profile adapter needs native connectors
            needs_native = bool(profile_adapters & set(NATIVE_CONNECTORS.keys()))
            if needs_native:
                fire_event(
                    DebugCmdOut(
                        msg=_table_row(["Native Dir", red("✗ not found")], widths)
                    )
                )

        fire_event(DebugCmdOut(msg=_table_footer()))

        # Show UX tip only if JDBC missing or relevant connectors missing
        if not has_jdbc or missing_connectors:
            fire_event(
                DebugCmdOut(msg=yellow("💡 Run 'dvt sync' to download missing drivers"))
            )

    def _debug_federation_readiness(self) -> None:
        """Display federation readiness as compact single-row summary table."""
        # Check computes.yml
        computes_path = (
            Path(self.profiles_dir) / "computes.yml"
            if self.profiles_dir
            else COMPUTES_PATH
        )
        has_computes = computes_path.exists()

        # JDBC drivers are global in DVT_HOME, not per-project profiles_dir
        jdbc_dir = get_spark_jars_dir(None)
        has_jdbc = (
            jdbc_dir.exists() and any(jdbc_dir.iterdir())
            if jdbc_dir.exists()
            else False
        )

        # Check PySpark
        has_pyspark = self._check_pyspark_installed()

        # Check Java
        has_java = self._check_java_installed()

        # Check cloud connector JARs (based on configured buckets)
        cloud_jar_status, missing_cloud_jars = self._check_cloud_jars()

        all_passed = (
            has_computes and has_jdbc and has_pyspark and has_java and cloud_jar_status
        )

        fire_event(DebugCmdOut(msg=""))
        fire_event(DebugCmdOut(msg=_table_header("Federation Readiness")))

        # Build status row
        computes_status = (
            f"computes.yml {green('✓')}" if has_computes else f"computes.yml {red('✗')}"
        )
        jdbc_status = f"JDBC {green('✓')}" if has_jdbc else f"JDBC {red('✗')}"
        pyspark_status = (
            f"PySpark {green('✓')}" if has_pyspark else f"PySpark {red('✗')}"
        )
        java_status = f"Java {green('✓')}" if has_java else f"Java {red('✗')}"
        cloud_status = (
            f"Cloud {green('✓')}" if cloud_jar_status else f"Cloud {red('✗')}"
        )
        overall = green("READY") if all_passed else red("NOT READY")

        status_line = f"│ {computes_status} │ {jdbc_status} │ {pyspark_status} │ {java_status} │ {cloud_status} │ {overall} │"
        fire_event(DebugCmdOut(msg=status_line))
        fire_event(DebugCmdOut(msg=_table_footer()))

        # Show UX tips for missing components
        tips = []
        if not has_computes:
            tips.append("computes.yml")
        if not has_jdbc:
            tips.append("JDBC drivers")
        if not has_pyspark:
            tips.append("PySpark")
        if not has_java:
            tips.append("Java")
        if missing_cloud_jars:
            tips.append(f"Cloud JARs ({', '.join(missing_cloud_jars)})")

        if tips:
            fire_event(
                DebugCmdOut(
                    msg=yellow(f"💡 Missing: {', '.join(tips)} → Run 'dvt sync'")
                )
            )

    def _check_cloud_jars(self) -> Tuple[bool, List[str]]:
        """Check if cloud connector JARs are available for configured buckets.

        Returns:
            Tuple of (all_available, list_of_missing_bucket_types)
        """
        from dvt.dvt_tasks.lib.cloud_connectors import (
            check_cloud_jars_available,
            get_bucket_types_from_config,
        )

        profiles_dir_str = str(self.profiles_dir) if self.profiles_dir else None
        bucket_types = get_bucket_types_from_config(profiles_dir_str)

        # No cloud buckets configured = passes check
        if not bucket_types:
            return True, []

        jar_status = check_cloud_jars_available(bucket_types, profiles_dir_str)
        missing = [bt for bt, available in jar_status.items() if not available]

        return len(missing) == 0, missing

    def _check_pyspark_installed(self) -> bool:
        """Check if PySpark is available."""
        try:
            import pyspark  # noqa: F401

            return True
        except ImportError:
            return False

    def _check_java_installed(self) -> bool:
        """Check if Java is available."""
        try:
            result = subprocess.run(
                ["java", "-version"], capture_output=True, text=True, timeout=5
            )
            return result.returncode == 0
        except Exception:
            return False
