# coding=utf-8
import importlib
import json
import os
import platform
import sys
from collections import namedtuple
from datetime import datetime
from enum import Flag
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import dvt.exceptions
import dbt_common.clients.system
import dbt_common.exceptions
from dvt.adapters.factory import get_adapter, register_adapter
from dvt.artifacts.schemas.results import RunStatus
from dvt.cli.flags import Flags
from dvt.clients.yaml_helper import load_yaml_text
from dvt.config import PartialProject, Profile, Project
from dvt.config.renderer import DvtProjectYamlRenderer, ProfileRenderer
from dvt.config.user_config import COMPUTES_PATH, DVT_HOME, MDM_DB_PATH
from dvt.events.types import DebugCmdOut, DebugCmdResult, OpenCommand
from dvt.links import ProfileConfigDocs
from dvt.mp_context import get_mp_context
from dvt.config.project import project_yml_path_if_exists
from dvt.constants import DBT_PROJECT_FILE_NAME
from dvt.task.base import BaseTask, get_nearest_project_dir
from dvt.version import get_installed_version
from dbt_common.events.format import pluralize
from dbt_common.events.functions import fire_event
from dbt_common.ui import green, red

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

COULD_NOT_CONNECT_MESSAGE = """
dvt was unable to connect to the specified database.
The database returned the following error:

  >{err}

Check your database credentials and try again. For more information, visit:
{url}
""".lstrip()

MISSING_PROFILE_MESSAGE = """
dvt looked for a profiles.yml file in {path}, but did
not find one. For more information on configuring your profile, consult the
documentation:

{url}
""".lstrip()

FILE_NOT_FOUND = "file not found"


SubtaskStatus = namedtuple(
    "SubtaskStatus", ["log_msg", "run_status", "details", "summary_message"]
)


class DebugRunStatus(Flag):
    SUCCESS = True
    FAIL = False


class DebugTask(BaseTask):
    def __init__(self, args: Flags) -> None:
        super().__init__(args)
        self.profiles_dir = args.PROFILES_DIR
        self.profile_path = os.path.join(self.profiles_dir, "profiles.yml")
        try:
            self.project_dir = get_nearest_project_dir(self.args.project_dir)
        except dbt_common.exceptions.DbtBaseException:
            # we probably couldn't find a project directory. Set project dir
            # to whatever was given, or default to the current directory.
            if args.project_dir:
                self.project_dir = args.project_dir
            else:
                self.project_dir = Path.cwd()
        path_if_exists = project_yml_path_if_exists(str(self.project_dir))
        self.project_path = path_if_exists or os.path.join(
            self.project_dir, DBT_PROJECT_FILE_NAME
        )
        self.cli_vars: Dict[str, Any] = args.vars

        # set by _load_*
        self.profile: Optional[Profile] = None
        self.raw_profile_data: Optional[Dict[str, Any]] = None
        self.profile_name: Optional[str] = None

    def _show_debug_sections(self) -> bool:
        """True if any of --config, --manifest, --targets, --computes are set (Feature 02)."""
        return bool(
            getattr(self.args, "config", False)
            or getattr(self.args, "manifest", False)
            or getattr(self.args, "targets", False)
            or getattr(self.args, "computes", False)
        )

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

        show_config = getattr(self.args, "config", False)
        show_manifest = getattr(self.args, "manifest", False)
        show_targets = getattr(self.args, "targets", False)
        show_computes = getattr(self.args, "computes", False)
        connection_target = getattr(self.args, "connection", None)
        show_all_sections = self._show_debug_sections()

        if show_all_sections or connection_target:
            # Feature 02: section-only or --connection <target> mode
            fire_event(
                DebugCmdOut(
                    msg="=" * 60 + "\nDVT Debug Information\nGenerated at: "
                    + datetime.now().isoformat()
                    + "\n" + "=" * 60
                )
            )
            if show_all_sections:
                if show_config:
                    self._debug_config()
                if show_targets:
                    self._debug_targets()
                if show_computes:
                    self._debug_computes()
                if show_manifest:
                    self._debug_manifest()
            if connection_target:
                return self._debug_connection_target(connection_target)
            return DebugRunStatus.SUCCESS.value

        # Original full debug flow (version, profile, project, deps, connection)
        version: str = get_installed_version().to_version_string(skip_matcher=True)
        fire_event(DebugCmdOut(msg="dvt version: {}".format(version)))
        fire_event(DebugCmdOut(msg="python version: {}".format(sys.version.split()[0])))
        fire_event(DebugCmdOut(msg="python path: {}".format(sys.executable)))
        fire_event(DebugCmdOut(msg="os info: {}".format(platform.platform())))

        load_profile_status: SubtaskStatus = self._load_profile()
        fire_event(DebugCmdOut(msg="Using profiles dir at {}".format(self.profiles_dir)))
        fire_event(DebugCmdOut(msg="Using profiles.yml file at {}".format(self.profile_path)))
        fire_event(DebugCmdOut(msg="Using project file at {}".format(self.project_path)))
        if load_profile_status.run_status == RunStatus.Success:
            if self.profile is None:
                raise dbt_common.exceptions.DbtInternalError(
                    "Profile should not be None if loading profile completed"
                )
            else:
                adapter_type: str = self.profile.credentials.type

            adapter_version: str = self._read_adapter_version(
                f"dvt.adapters.{adapter_type}.__version__"
            )
            fire_event(DebugCmdOut(msg="adapter type: {}".format(adapter_type)))
            fire_event(DebugCmdOut(msg="adapter version: {}".format(adapter_version)))

        load_project_status: SubtaskStatus = self._load_project()

        dependencies_statuses: List[SubtaskStatus] = []
        if connection_target:
            fire_event(DebugCmdOut(msg="Skipping steps before connection verification"))
        else:
            self.test_configuration(load_profile_status.log_msg, load_project_status.log_msg)
            dependencies_statuses = self.test_dependencies()

        connection_status = self.test_connection()

        all_statuses: List[SubtaskStatus] = [
            load_profile_status,
            load_project_status,
            *dependencies_statuses,
            connection_status,
        ]
        all_failing_statuses: List[SubtaskStatus] = list(
            filter(lambda status: status.run_status == RunStatus.Error, all_statuses)
        )

        failure_count: int = len(all_failing_statuses)
        if failure_count > 0:
            fire_event(DebugCmdResult(msg=red(f"{(pluralize(failure_count, 'check'))} failed:")))
            for status in all_failing_statuses:
                fire_event(DebugCmdResult(msg=f"{status.summary_message}\n"))
            return DebugRunStatus.FAIL.value
        else:
            fire_event(DebugCmdResult(msg=green("All checks passed!")))
            return DebugRunStatus.SUCCESS.value

    # ==============================
    # Override for elsewhere in core
    # ==============================

    def interpret_results(self, results):
        return results

    # ===============
    # Loading profile
    # ===============

    def _load_profile(self) -> SubtaskStatus:
        """
        Side effects: load self.profile
                      load self.target_name
                      load self.raw_profile_data
        """
        if not os.path.exists(self.profile_path):
            return SubtaskStatus(
                log_msg=red("ERROR not found"),
                run_status=RunStatus.Error,
                details=FILE_NOT_FOUND,
                summary_message=MISSING_PROFILE_MESSAGE.format(
                    path=self.profile_path, url=ProfileConfigDocs
                ),
            )

        raw_profile_data = load_yaml_text(
            dbt_common.clients.system.load_file_contents(self.profile_path)
        )
        if isinstance(raw_profile_data, dict):
            self.raw_profile_data = raw_profile_data

        profile_errors = []
        profile_names, summary_message = self._choose_profile_names()
        renderer = ProfileRenderer(self.cli_vars)
        for profile_name in profile_names:
            try:
                profile: Profile = Profile.render(
                    renderer,
                    profile_name,
                    self.args.profile,
                    self.args.target,
                    # TODO: Generalize safe access to flags.THREADS:
                    # https://github.com/dbt-labs/dbt-core/issues/6259
                    getattr(self.args, "threads", None),
                )
            except dbt_common.exceptions.DbtConfigError as exc:
                profile_errors.append(str(exc))
            else:
                if len(profile_names) == 1:
                    # if a profile was specified, set it on the task
                    self.target_name = self._choose_target_name(profile_name)
                    self.profile = profile

        if profile_errors:
            details = "\n\n".join(profile_errors)
            return SubtaskStatus(
                log_msg=red("ERROR invalid"),
                run_status=RunStatus.Error,
                details=details,
                summary_message=(
                    summary_message + f"Profile loading failed for the following reason:"
                    f"\n{details}"
                    f"\n"
                ),
            )
        else:
            return SubtaskStatus(
                log_msg=green("OK found and valid"),
                run_status=RunStatus.Success,
                details="",
                summary_message="Profile is valid",
            )

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
                summary_message = "Could not load project file (dbt_project.yml or dvt_project.yml)\n"
            elif len(profiles) == 0:
                summary_message = "The profiles.yml has no profiles\n"
            elif len(profiles) == 1:
                summary_message = ONLY_PROFILE_MESSAGE.format(profiles[0])
            else:
                summary_message = MULTIPLE_PROFILE_MESSAGE.format(
                    "\n".join(" - {}".format(o) for o in profiles)
                )
        return profiles, summary_message

    def _read_adapter_version(self, module) -> str:
        """read the version out of a standard adapter file"""
        try:
            version = importlib.import_module(module).version
        except ModuleNotFoundError:
            version = red("ERROR not found")
        except Exception as exc:
            version = red("ERROR {}".format(exc))
            raise dvt.exceptions.DbtInternalError(
                f"Error when reading adapter version from {module}: {exc}"
            )

        return version

    def _choose_target_name(self, profile_name: str):
        has_raw_profile = (
            self.raw_profile_data is not None and profile_name in self.raw_profile_data
        )

        if not has_raw_profile:
            return None

        # mypy appeasement, we checked just above
        assert self.raw_profile_data is not None
        raw_profile = self.raw_profile_data[profile_name]

        renderer = ProfileRenderer(self.cli_vars)

        target_name, _ = Profile.render_profile(
            raw_profile=raw_profile,
            profile_name=profile_name,
            target_override=getattr(self.args, "target", None),
            renderer=renderer,
        )
        return target_name

    # ===============
    # Loading project
    # ===============

    def _load_project(self) -> SubtaskStatus:
        """
        Side effect: load self.project
        """
        if not os.path.exists(self.project_path):
            return SubtaskStatus(
                log_msg=red("ERROR not found"),
                run_status=RunStatus.Error,
                details=FILE_NOT_FOUND,
                summary_message=(
                    f"Project loading failed for the following reason:"
                    f"\n project path <{self.project_path}> not found"
                ),
            )

        renderer = DvtProjectYamlRenderer(self.profile, self.cli_vars)

        try:
            self.project = Project.from_project_root(
                str(self.project_dir),
                renderer,
                verify_version=self.args.VERSION_CHECK,
            )
        except dbt_common.exceptions.DbtConfigError as exc:
            return SubtaskStatus(
                log_msg=red("ERROR invalid"),
                run_status=RunStatus.Error,
                details=str(exc),
                summary_message=(
                    f"Project loading failed for the following reason:" f"\n{str(exc)}" f"\n"
                ),
            )
        else:
            return SubtaskStatus(
                log_msg=green("OK found and valid"),
                run_status=RunStatus.Success,
                details="",
                summary_message="Project is valid",
            )

    def _profile_found(self) -> str:
        if not self.raw_profile_data:
            return red("ERROR not found")
        assert self.raw_profile_data is not None
        if self.profile_name in self.raw_profile_data:
            return green("OK found")
        else:
            return red("ERROR not found")

    def _target_found(self) -> str:
        requirements = self.raw_profile_data and self.profile_name and self.target_name
        if not requirements:
            return red("ERROR not found")
        # mypy appeasement, we checked just above
        assert self.raw_profile_data is not None
        assert self.profile_name is not None
        assert self.target_name is not None
        if self.profile_name not in self.raw_profile_data:
            return red("ERROR not found")
        profiles = self.raw_profile_data[self.profile_name]["outputs"]
        if self.target_name not in profiles:
            return red("ERROR not found")
        else:
            return green("OK found")

    # ================
    # Feature 02: debug sections (--config, --targets, --computes, --manifest, --connection)
    # ================

    def _debug_config(self) -> None:
        """Display resolved configuration (paths and project info)."""
        fire_event(DebugCmdOut(msg="\n--- Configuration ---"))
        dvt_dir = Path(self.profiles_dir) if self.profiles_dir else DVT_HOME
        fire_event(DebugCmdOut(msg=f"\nDVT Directory: {dvt_dir}"))
        fire_event(DebugCmdOut(msg=f"  Exists: {'✓' if Path(dvt_dir).exists() else '✗'}"))
        profiles_path = Path(dvt_dir) / "profiles.yml"
        fire_event(DebugCmdOut(msg=f"\nProfiles: {profiles_path}"))
        fire_event(DebugCmdOut(msg=f"  Exists: {'✓' if profiles_path.exists() else '✗'}"))
        computes_path = Path(dvt_dir) / "computes.yml"
        fire_event(DebugCmdOut(msg=f"\nComputes: {computes_path}"))
        fire_event(DebugCmdOut(msg=f"  Exists: {'✓' if computes_path.exists() else '✗'}"))
        mdm_path = Path(dvt_dir) / "data" / "mdm.duckdb"
        fire_event(DebugCmdOut(msg=f"\nMDM Database: {mdm_path}"))
        fire_event(DebugCmdOut(msg=f"  Exists: {'✓' if mdm_path.exists() else '✗'}"))
        fire_event(DebugCmdOut(msg=f"\nProject Config: {self.project_path}"))
        fire_event(DebugCmdOut(msg=f"  Exists: {'✓' if os.path.exists(self.project_path) else '✗'}"))
        if os.path.exists(self.project_path):
            try:
                partial = PartialProject.from_project_root(
                    str(self.project_dir), verify_version=False
                )
                renderer = DvtProjectYamlRenderer(None, self.cli_vars)
                profile_name = partial.render_profile_name(renderer)
                fire_event(DebugCmdOut(msg=f"\n  Project Name: {partial.project_name}"))
                fire_event(DebugCmdOut(msg=f"  Profile: {profile_name}"))
            except Exception as e:
                fire_event(DebugCmdOut(msg=f"  Error loading: {e}"))

    def _debug_targets(self) -> None:
        """Display targets for the current project's profile only, with connection status."""
        fire_event(DebugCmdOut(msg="\n--- Targets (current project profile only) ---"))
        if not self.raw_profile_data:
            load_status = self._load_profile()
            if load_status.run_status != RunStatus.Success or not self.raw_profile_data:
                fire_event(DebugCmdOut(msg="  No profiles.yml found or invalid."))
                return
        try:
            partial = PartialProject.from_project_root(str(self.project_dir), verify_version=False)
            renderer = DvtProjectYamlRenderer(None, self.cli_vars)
            profile_name = partial.render_profile_name(renderer)
        except Exception as e:
            fire_event(DebugCmdOut(msg=f"  Could not load project profile name: {e}"))
            return
        if profile_name not in self.raw_profile_data or profile_name == "config":
            fire_event(DebugCmdOut(msg=f"  Profile '{profile_name}' not found in profiles.yml."))
            return
        profile = self.raw_profile_data[profile_name]
        outputs = profile.get("outputs") or {}
        active_target = profile.get("target", "dev")
        fire_event(DebugCmdOut(msg=f"\nProfile: {profile_name}"))
        fire_event(DebugCmdOut(msg=f"  Active Target: {active_target}"))
        renderer = ProfileRenderer(self.cli_vars)
        for target_name, target_config in outputs.items():
            if not isinstance(target_config, dict):
                continue
            is_active = target_name == active_target
            marker = "→" if is_active else " "
            fire_event(DebugCmdOut(msg=f"\n  {marker} {target_name}:"))
            fire_event(DebugCmdOut(msg=f"      Type: {target_config.get('type', 'N/A')}"))
            for key in ("host", "port", "database", "schema", "account"):
                if key in target_config and target_config[key] is not None:
                    v = target_config[key]
                    if key == "host" and "port" in target_config:
                        v = f"{v}:{target_config.get('port', '')}"
                    fire_event(DebugCmdOut(msg=f"      {key.capitalize()}: {v}"))
            try:
                prof = Profile.from_raw_profile_info(
                    profile,
                    profile_name,
                    renderer=renderer,
                    target_override=target_name,
                )
                err = self.attempt_connection(prof)
                status = green("✓ Connected") if err is None else red(f"✗ {err[:60]}")
            except Exception as e:
                status = red(f"✗ Error: {str(e)[:50]}")
            fire_event(DebugCmdOut(msg=f"      Status: {status}"))

    def _debug_computes(self) -> None:
        """Display the current project's default compute only (from project vars or 'default')."""
        fire_event(DebugCmdOut(msg="\n--- Compute (current project default only) ---"))
        compute_name = "default"
        profile_name = "default"
        if os.path.exists(self.project_path):
            try:
                partial = PartialProject.from_project_root(str(self.project_dir), verify_version=False)
                vars_config = (partial.project_dict or {}).get("vars") or {}
                compute_name = vars_config.get("dvt_default_compute", "default")
                renderer = DvtProjectYamlRenderer(None, self.cli_vars)
                profile_name = partial.render_profile_name(renderer) or "default"
            except Exception:
                pass
        computes_path = Path(self.profiles_dir) / "computes.yml" if self.profiles_dir else COMPUTES_PATH
        if not computes_path.exists():
            fire_event(DebugCmdOut(msg=f"  No computes.yml found. Project default compute: {compute_name}"))
            fire_event(DebugCmdOut(msg="  (Using default local Spark when running.)"))
            try:
                from pyspark.sql import SparkSession  # noqa: F401
                fire_event(DebugCmdOut(msg="  PySpark: ✓ available"))
            except ImportError:
                fire_event(DebugCmdOut(msg="  PySpark: ✗ not installed"))
            return
        try:
            raw = load_yaml_text(dbt_common.clients.system.load_file_contents(str(computes_path)))
            raw = raw or {}
            # New structure: profile name -> { target: <name>, computes: { <name>: config } }
            profile_block = raw.get(profile_name) if isinstance(raw.get(profile_name), dict) else None
            if profile_block and "computes" in profile_block:
                compute_name = profile_block.get("target", compute_name)
                all_computes = profile_block.get("computes") or {}
            else:
                # Legacy flat: top-level "computes"
                all_computes = raw.get("computes") or {}
        except Exception as e:
            fire_event(DebugCmdOut(msg=f"  Error reading computes.yml: {e}"))
            return
        if compute_name not in all_computes:
            fire_event(DebugCmdOut(msg=f"  Compute '{compute_name}' (project default) not found in computes.yml."))
            fire_event(DebugCmdOut(msg=f"  Available: {', '.join(all_computes.keys()) or 'none'}"))
            return
        cfg = all_computes[compute_name]
        if not isinstance(cfg, dict):
            fire_event(DebugCmdOut(msg=f"  Compute '{compute_name}' is misconfigured."))
            return
        fire_event(DebugCmdOut(msg=f"\n  {compute_name} (project default):"))
        fire_event(DebugCmdOut(msg=f"    Type: {cfg.get('type', 'spark')}"))
        if cfg.get("version"):
            fire_event(DebugCmdOut(msg=f"    Version (pyspark): {cfg.get('version')}"))
        fire_event(DebugCmdOut(msg=f"    Master: {cfg.get('master', 'N/A')}"))
        for k, v in list((cfg.get("config") or {}).items())[:5]:
            fire_event(DebugCmdOut(msg=f"    {k}: {v}"))
        try:
            from pyspark.sql import SparkSession  # noqa: F401
            fire_event(DebugCmdOut(msg="\n  PySpark: ✓ available"))
        except ImportError:
            fire_event(DebugCmdOut(msg="\n  PySpark: ✗ not installed"))

    def _debug_manifest(self) -> None:
        """Display manifest summary from target/manifest.json."""
        fire_event(DebugCmdOut(msg="\n--- Manifest ---"))
        manifest_path = Path(self.project_dir) / "target" / "manifest.json"
        fire_event(DebugCmdOut(msg=f"\nManifest Path: {manifest_path}"))
        fire_event(DebugCmdOut(msg=f"  Exists: {'✓' if manifest_path.exists() else '✗'}"))
        if not manifest_path.exists():
            fire_event(DebugCmdOut(msg="  Run 'dvt compile' or 'dvt parse' to generate manifest."))
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
            fire_event(DebugCmdOut(msg=f"\n  Generated: {meta.get('generated_at', 'Unknown')}"))
        except Exception as e:
            fire_event(DebugCmdOut(msg=f"  Error reading manifest: {e}"))

    def _debug_connection_target(self, target_name: str) -> bool:
        """Test connection to a specific target. Uses current project's profile."""
        fire_event(DebugCmdOut(msg=f"\n--- Testing Connection: {target_name} ---"))
        load_status = self._load_profile()
        if load_status.run_status != RunStatus.Success or self.raw_profile_data is None:
            fire_event(DebugCmdOut(msg="  Could not load profile or profiles.yml not found."))
            return DebugRunStatus.FAIL.value
        try:
            partial = PartialProject.from_project_root(str(self.project_dir), verify_version=False)
            renderer = DvtProjectYamlRenderer(None, self.cli_vars)
            profile_name = partial.render_profile_name(renderer)
        except Exception as e:
            fire_event(DebugCmdOut(msg=f"  Could not get project profile name: {e}"))
            return DebugRunStatus.FAIL.value
        if profile_name not in self.raw_profile_data or profile_name == "config":
            fire_event(DebugCmdOut(msg=f"  Profile '{profile_name}' not in profiles.yml."))
            return DebugRunStatus.FAIL.value
        if target_name not in (self.raw_profile_data[profile_name].get("outputs") or {}):
            fire_event(DebugCmdOut(msg=f"  Target '{target_name}' not found in profile '{profile_name}'."))
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

    # ============
    # Config tests
    # ============

    def test_git(self) -> SubtaskStatus:
        try:
            dbt_common.clients.system.run_cmd(os.getcwd(), ["git", "--help"])
        except dbt_common.exceptions.ExecutableError as exc:
            return SubtaskStatus(
                log_msg=red("ERROR"),
                run_status=RunStatus.Error,
                details="git error",
                summary_message="Error from git --help: {!s}".format(exc),
            )
        else:
            return SubtaskStatus(
                log_msg=green("OK found"),
                run_status=RunStatus.Success,
                details="",
                summary_message="git is installed and on the path",
            )

    def test_dependencies(self) -> List[SubtaskStatus]:
        fire_event(DebugCmdOut(msg="Required dependencies:"))

        git_test_status = self.test_git()
        fire_event(DebugCmdResult(msg=f" - git [{git_test_status.log_msg}]\n"))

        return [git_test_status]

    def test_configuration(self, profile_status_msg, project_status_msg):
        fire_event(DebugCmdOut(msg="Configuration:"))
        fire_event(DebugCmdOut(msg=f"  profiles.yml file [{profile_status_msg}]"))
        fire_event(DebugCmdOut(msg=f"  project file (dbt_project.yml) [{project_status_msg}]"))

        # skip profile stuff if we can't find a profile name
        if self.profile_name is not None:
            fire_event(
                DebugCmdOut(
                    msg="  profile: {} [{}]\n".format(self.profile_name, self._profile_found())
                )
            )
            fire_event(
                DebugCmdOut(
                    msg="  target: {} [{}]\n".format(self.target_name, self._target_found())
                )
            )

    # ===============
    # Connection test
    # ===============

    @staticmethod
    def attempt_connection(profile) -> Optional[str]:
        """Return a string containing the error message, or None if there was no error."""
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

    def test_connection(self) -> SubtaskStatus:
        if self.profile is None:
            fire_event(DebugCmdOut(msg="Connection test skipped since no profile was found"))
            return SubtaskStatus(
                log_msg=red("SKIPPED"),
                run_status=RunStatus.Skipped,
                details="No profile found",
                summary_message="Connection test skipped since no profile was found",
            )

        fire_event(DebugCmdOut(msg="Connection:"))
        for k, v in self.profile.credentials.connection_info():
            fire_event(DebugCmdOut(msg=f"  {k}: {v}"))

        connection_result = self.attempt_connection(self.profile)
        if connection_result is None:
            status = SubtaskStatus(
                log_msg=green("OK connection ok"),
                run_status=RunStatus.Success,
                details="",
                summary_message="Connection test passed",
            )
        else:
            status = SubtaskStatus(
                log_msg=red("ERROR"),
                run_status=RunStatus.Error,
                details="Failure in connecting to db",
                summary_message=connection_result,
            )
        fire_event(DebugCmdOut(msg=f"  Connection test: [{status.log_msg}]\n"))
        return status

    @classmethod
    def validate_connection(cls, target_dict) -> None:
        """Validate a connection dictionary. On error, raises a DbtConfigError."""
        target_name = "test"
        # make a fake profile that we can parse
        profile_data = {
            "outputs": {
                target_name: target_dict,
            },
        }
        # this will raise a DbtConfigError on failure
        profile = Profile.from_raw_profile_info(
            raw_profile=profile_data,
            profile_name="",
            target_override=target_name,
            renderer=ProfileRenderer({}),
        )
        result = cls.attempt_connection(profile)
        if result is not None:
            raise dvt.exceptions.DvtProfileError(result, result_type="connection_failure")
