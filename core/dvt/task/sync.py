# coding=utf-8
"""
dvt sync: install adapters, pyspark, and JDBC drivers for the current project's profile.
- Resolves Python env: in-project (.venv, venv, env) or prompts for path.
- Reads profile from dbt_project.yml, adapter types from profiles.yml, require-adapters from project.
- Installs dbt-<adapter> per profile target type; relates each adapter type to JDBC driver(s) and downloads those JARs for Spark federation.
- Reads active target from computes.yml, installs only that pyspark version (uninstalls others).
"""
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

from dvt.cli.flags import Flags
from dvt.config.project import project_yml_path_if_exists
from dvt.config.user_config import get_dvt_home, get_jdbc_drivers_dir
from dvt.task.base import BaseTask, get_nearest_project_dir
from dvt.task.jdbc_drivers import download_jdbc_jars, get_jdbc_drivers_for_adapters
from dbt_common.clients.system import load_file_contents
from dbt_common.events.functions import fire_event
from dbt_common.exceptions import DbtRuntimeError

from dvt.events.types import DebugCmdOut

# In-project env dir names (order = preference)
IN_PROJECT_ENV_NAMES = (".venv", "venv", "env")


def _find_project_env(project_root: Path) -> Optional[Path]:
    """Return path to Python env inside project dir only (.venv, venv, env). Does not look in parent dirs."""
    project_root = Path(project_root).resolve()
    for name in IN_PROJECT_ENV_NAMES:
        candidate = project_root / name
        if candidate.is_dir():
            py = candidate / "bin" / "python"
            if py.exists():
                return candidate
            py_win = candidate / "Scripts" / "python.exe"
            if py_win.exists():
                return candidate
    return None


def _get_env_python(env_path: Path) -> Path:
    """Return path to python executable in the given env."""
    py = env_path / "bin" / "python"
    if py.exists():
        return py
    py_win = env_path / "Scripts" / "python.exe"
    if py_win.exists():
        return py_win
    raise DbtRuntimeError(f"No python found in env at {env_path}")


def _run_pip(env_python: Path, args: List[str]) -> bool:
    """Run python -m pip with args. Return True on success."""
    cmd = [str(env_python), "-m", "pip"] + args
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
        if result.returncode != 0:
            sys.stderr.write(result.stderr or "")
            return False
        return True
    except Exception as e:
        sys.stderr.write(f"pip failed: {e}\n")
        return False


def _run_uv_pip(env_path: Path, args: List[str]) -> bool:
    """Run uv pip with --python pointing to env. Return True on success."""
    env_python = _get_env_python(env_path)
    cmd = ["uv", "pip", "install", "--python", str(env_python)] + args
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
        if result.returncode != 0:
            sys.stderr.write(result.stderr or "")
            return False
        return True
    except FileNotFoundError:
        return False  # uv not available
    except Exception as e:
        sys.stderr.write(f"uv pip failed: {e}\n")
        return False


def _detect_package_manager(env_python: Path) -> str:
    """Return 'uv' if uv is available and env looks uv-managed, else 'pip'."""
    try:
        r = subprocess.run(
            ["uv", "pip", "install", "--help"],
            capture_output=True,
            timeout=5,
        )
        if r.returncode == 0:
            return "uv"
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass
    return "pip"


def _load_yaml(path: Path) -> Any:
    if not path.exists():
        return None
    try:
        raw = load_file_contents(str(path))
        return yaml.safe_load(raw) if raw else None
    except Exception:
        return None


def _get_profile_name(project_root: Path) -> Optional[str]:
    """Get profile name from project file."""
    path = project_yml_path_if_exists(str(project_root))
    if not path:
        return None
    data = _load_yaml(Path(path))
    return data.get("profile") if isinstance(data, dict) else None


def _get_require_adapters(project_root: Path) -> Dict[str, str]:
    """Get require-adapters from project file. Keys: adapter type, values: version spec."""
    path = project_yml_path_if_exists(str(project_root))
    if not path:
        return {}
    data = _load_yaml(Path(path))
    if not isinstance(data, dict):
        return {}
    raw = data.get("require-adapters")
    if isinstance(raw, dict):
        return {str(k): str(v) for k, v in raw.items()}
    return {}


def _get_adapter_types_from_profile(profiles_dir: Path, profile_name: str) -> List[str]:
    """Get list of adapter types (e.g. postgres) used by the profile's targets."""
    profiles_path = profiles_dir / "profiles.yml"
    data = _load_yaml(profiles_path)
    if not isinstance(data, dict) or profile_name not in data:
        return []
    profile = data[profile_name]
    if not isinstance(profile, dict):
        return []
    outputs = profile.get("outputs") or {}
    types = set()
    for out in outputs.values() if isinstance(outputs, dict) else []:
        if isinstance(out, dict) and "type" in out:
            types.add(str(out["type"]))
    return list(types)


def _get_computes_for_profile(computes_path: Path, profile_name: str) -> Optional[Dict[str, Any]]:
    """
    Load computes.yml. New structure: top-level keys are profile names;
    each value is { target: <name>, computes: { <name>: { type, version?, master?, config? } } }.
    Return the dict for the given profile, or None.
    """
    data = _load_yaml(computes_path)
    if not isinstance(data, dict):
        return None
    profile_block = data.get(profile_name)
    if not isinstance(profile_block, dict):
        return None
    return profile_block


def _get_active_pyspark_version(computes_path: Path, profile_name: str) -> Optional[str]:
    """
    Return the pyspark version for the active target of the given profile.
    Active target is profile_block['target']; compute config is profile_block['computes'][target].
    """
    profile_block = _get_computes_for_profile(computes_path, profile_name)
    if not profile_block:
        return None
    target_name = profile_block.get("target", "default")
    computes = profile_block.get("computes") or {}
    if not isinstance(computes, dict):
        return None
    active = computes.get(target_name) if isinstance(computes, dict) else None
    if not isinstance(active, dict):
        return None
    return active.get("version")


class SyncTask(BaseTask):
    """Install adapters and pyspark for the project. Resolves env; uses require-adapters and computes.yml."""

    def __init__(self, args: Flags) -> None:
        super().__init__(args)
        self.project_dir: Optional[Path] = None
        self.env_path: Optional[Path] = None
        self.profiles_dir = Path(args.PROFILES_DIR) if args.PROFILES_DIR else get_dvt_home()

    def run(self):
        try:
            self.project_dir = get_nearest_project_dir(self.args.project_dir)
        except DbtRuntimeError:
            fire_event(DebugCmdOut(msg="Not in a DVT project. Run from a directory with dbt_project.yml (or use --project-dir)."))
            return None, False

        project_root = self.project_dir
        assert project_root is not None

        # 1) Resolve Python environment: only --python-env or .venv/venv/env inside project dir
        explicit_env = getattr(self.args, "PYTHON_ENV", None)
        if explicit_env:
            env_path = Path(explicit_env).expanduser().resolve()
            if not env_path.is_dir():
                fire_event(DebugCmdOut(msg=f"Not a directory: {env_path}"))
                return None, False
            _get_env_python(env_path)  # validate
        else:
            env_path = _find_project_env(project_root)
        if env_path is None:
            try:
                raw = input("No virtual environment found in project. Enter absolute path to your Python env folder: ").strip()
                if raw:
                    env_path = Path(raw).resolve()
                    if not env_path.is_dir():
                        fire_event(DebugCmdOut(msg=f"Not a directory: {env_path}"))
                        return None, False
                    _get_env_python(env_path)  # validate
                else:
                    fire_event(DebugCmdOut(msg="No path provided. Sync skipped."))
                    return None, False
            except EOFError:
                fire_event(DebugCmdOut(msg="No path provided (non-interactive). Sync skipped."))
                return None, False
        self.env_path = env_path
        env_python = _get_env_python(env_path)
        fire_event(DebugCmdOut(msg=f"Using environment: {env_path}"))

        # 2) Profile and adapter types
        profile_name = _get_profile_name(project_root)
        if not profile_name:
            fire_event(DebugCmdOut(msg="No profile in project file. Sync skipped."))
            return None, False
        adapter_types = _get_adapter_types_from_profile(self.profiles_dir, profile_name)
        require_adapters = _get_require_adapters(project_root)

        # 3) Install adapters
        for adapter_type in adapter_types:
            spec = require_adapters.get(adapter_type)
            pkg = f"dbt-{adapter_type}"
            if spec:
                # spec may be ">=1.0.0" or "1.2.0"; pip needs "dbt-postgres>=1.0.0" or "dbt-postgres==1.2.0"
                pkg_spec = f"{pkg}{spec}" if any(spec.startswith(c) for c in ("=", ">", "<", "~", "!")) else f"{pkg}=={spec}"
            else:
                pkg_spec = pkg
            fire_event(DebugCmdOut(msg=f"Installing {pkg_spec} ..."))
            if _detect_package_manager(env_python) == "uv":
                ok = _run_uv_pip(env_path, [pkg_spec])
            else:
                ok = _run_pip(env_python, ["install", pkg_spec])
            if not ok:
                fire_event(DebugCmdOut(msg=f"Failed to install {pkg_spec}"))

        # 4) Pyspark: single version from active target; use canonical ~/.dvt/computes.yml
        # so the project's profile block is used (not a local computes.yml from another profile).
        computes_path = get_dvt_home(None) / "computes.yml"
        pyspark_version = _get_active_pyspark_version(computes_path, profile_name) if computes_path.exists() else None
        if pyspark_version:
            fire_event(DebugCmdOut(msg=f"Uninstalling other pyspark versions ..."))
            _run_pip(env_python, ["uninstall", "pyspark", "-y"])
            fire_event(DebugCmdOut(msg=f"Installing pyspark=={pyspark_version} ..."))
            if _detect_package_manager(env_python) == "uv":
                ok = _run_uv_pip(env_path, [f"pyspark=={pyspark_version}"])
            else:
                ok = _run_pip(env_python, ["install", f"pyspark=={pyspark_version}"])
            if not ok:
                fire_event(DebugCmdOut(msg=f"Failed to install pyspark=={pyspark_version}"))
        else:
            fire_event(DebugCmdOut(msg="No pyspark version in computes.yml for this profile; skipping pyspark install."))

        # 5) JDBC drivers: relate profile adapters to JDBC jars and download for federation.
        # Always use canonical DVT home (~/.dvt/.jdbc_jars) so jars are in one place (e.g. trial
        # folders with local profiles.yml would otherwise put jars in project dir).
        jdbc_dir = get_jdbc_drivers_dir(None)
        drivers = get_jdbc_drivers_for_adapters(adapter_types)
        if drivers:
            fire_event(DebugCmdOut(msg=f"Syncing JDBC drivers for adapters: {', '.join(adapter_types)}"))
            download_jdbc_jars(
                drivers,
                jdbc_dir,
                on_event=lambda msg: fire_event(DebugCmdOut(msg=msg)),
            )
        else:
            fire_event(DebugCmdOut(msg="No JDBC drivers required for these adapters (or adapters not in registry)."))

        fire_event(DebugCmdOut(msg="Sync complete."))
        return None, True
