# coding=utf-8
"""
dvt sync: install adapters, pyspark, and JDBC drivers for the current project's profile.
- Resolves Python env: in-project (.venv, venv, env) or prompts for path.
- Reads profile from dbt_project.yml, adapter types from profiles.yml, require-adapters from project.
- Installs dbt-<adapter> per profile target type; relates each adapter type to JDBC driver(s) and downloads those JARs for Spark federation.
- Reads active target from computes.yml, installs only that pyspark version (uninstalls others).
"""

import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml

from dvt.cli.flags import Flags
from dvt.config.project import project_yml_path_if_exists
from dvt.config.user_config import (
    get_bucket_dependencies,
    get_dvt_home,
    get_spark_jars_dir,
)
from dvt.task.base import BaseTask, get_nearest_project_dir
from dvt.task.jdbc_drivers import download_jdbc_jars, get_jdbc_drivers_for_adapters
from dvt.task.native_connectors import (
    get_native_connectors_for_adapters,
    sync_native_connectors,
)
from dbt_common.clients.system import load_file_contents
from dbt_common.events.functions import fire_event
from dbt_common.exceptions import DbtRuntimeError

from dvt.events.types import DebugCmdOut
from dbt_common.ui import yellow, green, red

# In-project env dir names (order = preference)
IN_PROJECT_ENV_NAMES = (".venv", "venv", "env")


def _sync_log(msg: str) -> None:
    """Write sync progress/errors to stderr so they are always visible (event system may not show them)."""
    sys.stderr.write(msg + "\n")
    sys.stderr.flush()
    fire_event(DebugCmdOut(msg=msg))


def _is_valid_env(candidate: Path) -> bool:
    """Check if a directory is a valid Python virtual environment.

    Args:
        candidate: Path to check

    Returns:
        True if the directory contains a valid Python interpreter
    """
    if not candidate.is_dir():
        return False
    # Check for Unix python
    if (candidate / "bin" / "python").exists():
        return True
    # Check for Windows python
    if (candidate / "Scripts" / "python.exe").exists():
        return True
    return False


def _find_project_env(project_root: Path, cwd: Optional[Path] = None) -> Optional[Path]:
    """Return path to Python env by searching multiple locations.

    Search order:
    1. Inside project_root (.venv, venv, env)
    2. Inside cwd if different from project_root (for --project-dir usage)
    3. Parent directory of project_root (common pattern: trial_root/.venv + trial_root/project/)

    Args:
        project_root: The project directory containing dbt_project.yml
        cwd: Current working directory (optional, for when running with --project-dir)

    Returns:
        Path to valid Python environment, or None if not found
    """
    project_root = Path(project_root).resolve()

    # 1. Check inside project directory first
    for name in IN_PROJECT_ENV_NAMES:
        candidate = project_root / name
        if _is_valid_env(candidate):
            return candidate

    # 2. Check current working directory (if different from project)
    if cwd:
        cwd = Path(cwd).resolve()
        if cwd != project_root:
            for name in IN_PROJECT_ENV_NAMES:
                candidate = cwd / name
                if _is_valid_env(candidate):
                    return candidate

    # 3. Check parent of project directory (one level up)
    parent = project_root.parent
    if parent != project_root:  # Avoid infinite loop at filesystem root
        for name in IN_PROJECT_ENV_NAMES:
            candidate = parent / name
            if _is_valid_env(candidate):
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
    """Run python -m pip with args. Streams output to terminal. Return True on success."""
    cmd = [str(env_python), "-m", "pip"] + args
    try:
        # Stream stdout to terminal for progress visibility
        # Capture stderr separately to check for errors
        result = subprocess.run(
            cmd,
            stdout=None,  # Inherit from parent (shows in terminal)
            stderr=subprocess.PIPE,
            text=True,
            timeout=300,
        )
        if result.returncode != 0:
            if result.stderr:
                sys.stderr.write(result.stderr)
            return False
        return True
    except subprocess.TimeoutExpired:
        sys.stderr.write("pip failed: timed out after 300 seconds\n")
        return False
    except Exception as e:
        sys.stderr.write(f"pip failed: {e}\n")
        return False


def _run_uv_pip(env_path: Path, args: List[str], timeout: int = 300) -> bool:
    """Run uv pip with --python pointing to env. Streams output to terminal. Return True on success."""
    env_python = _get_env_python(env_path)
    cmd = ["uv", "pip", "install", "--python", str(env_python)] + args
    try:
        # Stream stdout to terminal for progress visibility
        result = subprocess.run(
            cmd,
            stdout=None,  # Inherit from parent (shows in terminal)
            stderr=subprocess.PIPE,
            text=True,
            timeout=timeout,
        )
        if result.returncode != 0:
            if result.stderr:
                sys.stderr.write(result.stderr)
            return False
        return True
    except FileNotFoundError:
        return False  # uv not available
    except subprocess.TimeoutExpired:
        sys.stderr.write(f"uv pip failed: timed out after {timeout} seconds\n")
        return False
    except Exception as e:
        sys.stderr.write(f"uv pip failed: {e}\n")
        return False


def _run_uv_pip_uninstall(env_path: Path, packages: List[str]) -> bool:
    """Run uv pip uninstall with --python pointing to env. Streams output to terminal. Return True on success."""
    env_python = _get_env_python(env_path)
    cmd = ["uv", "pip", "uninstall", "--python", str(env_python)] + packages
    try:
        # Stream stdout to terminal for progress visibility
        result = subprocess.run(
            cmd,
            stdout=None,  # Inherit from parent (shows in terminal)
            stderr=subprocess.PIPE,
            text=True,
            timeout=60,
        )
        if result.returncode != 0:
            if result.stderr:
                sys.stderr.write(result.stderr)
            return False
        return True
    except FileNotFoundError:
        return False
    except subprocess.TimeoutExpired:
        sys.stderr.write("uv pip uninstall failed: timed out after 60 seconds\n")
        return False
    except Exception as e:
        sys.stderr.write(f"uv pip uninstall failed: {e}\n")
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


def _get_computes_for_profile(
    computes_path: Path, profile_name: str
) -> Optional[Dict[str, Any]]:
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


def _get_active_pyspark_version(
    computes_path: Path, profile_name: str
) -> Optional[str]:
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


def _get_delta_spark_version(spark_version: str) -> Optional[str]:
    """Return the compatible delta-spark version for a given Spark/PySpark version.

    Delta Lake versions are NOT 1:1 with PySpark versions.
    Mapping from https://docs.delta.io/latest/releases.html:
        Spark 4.x   -> delta-spark 4.0.x
        Spark 3.5.x -> delta-spark 3.2.x
        Spark 3.4.x -> delta-spark 2.4.x
        Spark 3.3.x -> delta-spark 2.3.x
        Spark 3.2.x -> delta-spark 2.0.x

    Returns the latest compatible delta-spark version spec, or None if unknown.
    """
    parts = spark_version.split(".")
    major = int(parts[0]) if parts[0].isdigit() else 0
    minor = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 0

    if major >= 4:
        if minor == 0:
            return "4.0.1"
        # Spark 4.1+ not yet supported by delta-spark (4.0.1 targets Spark 4.0.x)
        return None
    elif major == 3:
        if minor >= 5:
            return "3.2.1"
        elif minor == 4:
            return "2.4.0"
        elif minor == 3:
            return "2.3.0"
        elif minor == 2:
            return "2.0.2"
    return None


def _get_required_java_versions(spark_version: str) -> List[str]:
    """
    Return list of required Java major versions for the given Spark version.
    Spark 3.2.x → Java 8, 11
    Spark 3.3.x–3.5.x → Java 8, 11, 17
    Spark 4.x → Java 17, 21
    """
    # Parse Spark version: "3.2.0" -> "3.2", "4.1.0" -> "4", "3.5" -> "3.5"
    parts = spark_version.split(".")
    major = int(parts[0]) if parts[0].isdigit() else 0
    minor = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 0

    if major == 3:
        if minor == 2:
            return ["8", "11"]
        elif minor in (3, 4, 5):
            return ["8", "11", "17"]
        else:
            # Default for Spark 3.x: Java 8, 11, 17
            return ["8", "11", "17"]
    elif major == 4:
        return ["17", "21"]
    else:
        # Unknown version: default to Java 8, 11, 17 (most common)
        return ["8", "11", "17"]


def _detect_java_version() -> Optional[str]:
    """
    Detect installed Java version by running 'java -version'.
    Returns major version as string (e.g., "8", "11", "17", "21") or None if not found.
    """
    try:
        result = subprocess.run(
            ["java", "-version"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        # Java prints version to stderr
        output = result.stderr or result.stdout

        # Patterns to match:
        # - "openjdk version \"17.0.2\"" -> "17"
        # - "java version \"1.8.0_291\"" -> "8"
        # - "java version \"21.0.1\"" -> "21"
        # - "openjdk version \"11.0.19\"" -> "11"

        # Try to match "version \"X.Y.Z\"" or "version \"X\"" where X is major version
        patterns = [
            r'version\s+"(\d+)\.',  # Matches "version "17.0.2" -> "17"
            r'version\s+"1\.(\d+)\.',  # Matches "version "1.8.0_291" -> "8"
        ]

        for pattern in patterns:
            match = re.search(pattern, output)
            if match:
                version_str = match.group(1)
                # If we matched "1.8" pattern, version_str is "8"; otherwise it's the major version
                return version_str

        # Fallback: try to extract any number that looks like a major version
        numbers = re.findall(r"\b(\d+)\.\d+", output)
        if numbers:
            major = numbers[0]
            # If it's "1", check for Java 8 pattern
            if major == "1" and "1.8" in output:
                return "8"
            return major

    except FileNotFoundError:
        return None
    except Exception:
        return None
    return None


def _check_java_compatibility(
    spark_version: str, java_version: Optional[str]
) -> Tuple[bool, Optional[str]]:
    """
    Check if Java version is compatible with Spark version.
    Returns (is_compatible, warning_message).
    """
    required_versions = _get_required_java_versions(spark_version)
    required_str = ", ".join(f"Java {v}" for v in required_versions)

    if java_version is None:
        return (
            False,
            f"Java not found. Spark {spark_version} requires one of: {required_str}.",
        )

    if java_version not in required_versions:
        return (
            False,
            f"Java {java_version} is incompatible with Spark {spark_version}. Required: {required_str}.",
        )

    return (True, None)


def _print_java_installation_instructions(required_versions: List[str]) -> None:
    """Print Java installation instructions for Mac and Linux."""
    _sync_log("")
    _sync_log(yellow("=" * 70))
    _sync_log(yellow("☕ Java Installation Instructions"))
    _sync_log(yellow("=" * 70))
    _sync_log("")
    _sync_log(
        f"Required Java versions: {yellow(', '.join(f'Java {v}' for v in required_versions))}"
    )
    _sync_log("")

    # Mac instructions
    _sync_log(green("🍎 macOS:"))
    _sync_log("  Option 1: Homebrew (recommended)")
    for v in required_versions:
        _sync_log(f"    brew install openjdk@{v}")
    _sync_log("    Then link: brew link --overwrite openjdk@<version>")
    _sync_log("")
    _sync_log("  Option 2: SDKMAN")
    _sync_log('    curl -s "https://get.sdkman.io" | bash')
    _sync_log("    sdk install java <version>")
    _sync_log("    Example: sdk install java 17.0.2-tem")
    _sync_log("")
    _sync_log("  Option 3: Manual download")
    _sync_log("    Download from: https://adoptium.net/")
    _sync_log("    Extract and set JAVA_HOME:")
    _sync_log(
        '    export JAVA_HOME="/Library/Java/JavaVirtualMachines/jdk-<version>.jdk/Contents/Home"'
    )
    _sync_log("")

    # Linux instructions
    _sync_log(green("🐧 Linux:"))
    _sync_log("  Option 1: apt (Debian/Ubuntu)")
    for v in required_versions:
        _sync_log(f"    sudo apt update && sudo apt install openjdk-{v}-jdk")
    _sync_log("")
    _sync_log("  Option 2: yum/dnf (RHEL/CentOS/Fedora)")
    for v in required_versions:
        _sync_log(f"    sudo yum install java-{v}-openjdk-devel")
    _sync_log("    # or for newer Fedora:")
    _sync_log(f"    sudo dnf install java-{v}-openjdk-devel")
    _sync_log("")
    _sync_log("  Option 3: SDKMAN")
    _sync_log('    curl -s "https://get.sdkman.io" | bash')
    _sync_log("    sdk install java <version>")
    _sync_log("")
    _sync_log("  Option 4: Manual download")
    _sync_log("    Download from: https://adoptium.net/")
    _sync_log("    Extract and set JAVA_HOME:")
    _sync_log('    export JAVA_HOME="/usr/lib/jvm/java-<version>-openjdk"')
    _sync_log("")

    _sync_log(green("✅ After installation, verify:"))
    _sync_log("  java -version")
    _sync_log("  echo $JAVA_HOME  # Should point to the Java installation")
    _sync_log("")
    _sync_log(yellow("=" * 70))


class SyncTask(BaseTask):
    """Install adapters and pyspark for the project. Resolves env; uses require-adapters and computes.yml."""

    def __init__(self, args: Flags) -> None:
        super().__init__(args)
        self.project_dir: Optional[Path] = None
        self.env_path: Optional[Path] = None
        self.profiles_dir = (
            Path(args.PROFILES_DIR) if args.PROFILES_DIR else get_dvt_home()
        )

    def run(self):
        _sync_log("dvt sync: starting...")
        java_warnings: List[str] = []  # Collect Java compatibility warnings
        # Use explicit project_dir from flags, or default (cwd/parents) so CLI and programmatic use both work
        project_dir_arg = getattr(self.args, "project_dir", None) or getattr(
            self.args, "PROJECT_DIR", None
        )
        try:
            self.project_dir = get_nearest_project_dir(project_dir_arg)
        except DbtRuntimeError:
            msg = red(
                "❌ Not in a DVT project. Run from a directory with dbt_project.yml (or use --project-dir)."
            )
            _sync_log(msg)
            return None, False

        project_root = Path(self.project_dir).resolve()
        assert project_root is not None

        # 1) Resolve Python environment: only --python-env or .venv/venv/env inside project dir
        explicit_env = getattr(self.args, "PYTHON_ENV", None)
        if explicit_env:
            env_path = Path(explicit_env).expanduser().resolve()
            if not env_path.is_dir():
                _sync_log(f"Not a directory: {env_path}")
                return None, False
            _get_env_python(env_path)  # validate
        else:
            env_path = _find_project_env(project_root, cwd=Path.cwd())
        if env_path is None:
            try:
                prompt_msg = (
                    "\n⚠️  No Python virtual environment detected.\n\n"
                    "DVT searched for common environment folders (.venv, venv, env) in:\n"
                    "  • Project directory\n"
                    "  • Current working directory\n"
                    "  • Parent directory of project\n\n"
                    "If your environment has a different name or location, enter the full path below.\n"
                    "Examples:\n"
                    "  • /path/to/myproject/.venv\n"
                    "  • /home/user/.virtualenvs/myproject\n"
                    "  • /path/to/conda/envs/myenv\n\n"
                    "Path to environment (or press Ctrl+C to cancel): "
                )
                sys.stdout.write(prompt_msg)
                sys.stdout.flush()
                raw = input().strip()
                if raw:
                    env_path = Path(raw).resolve()
                    if not env_path.is_dir():
                        _sync_log(f"Not a directory: {env_path}")
                        return None, False
                    _get_env_python(env_path)  # validate
                else:
                    _sync_log(
                        'No path provided. Sync skipped. Use --python-env "/path/to/env" to specify your environment.'
                    )
                    return None, False
            except EOFError:
                _sync_log(
                    "No path provided (non-interactive). Sync skipped. "
                    'Use --python-env "/path/to/env" to specify your environment.'
                )
                return None, False
        self.env_path = env_path
        env_python = _get_env_python(env_path)
        _sync_log(green(f"📦 Using environment: {env_path}"))

        # 2) Profile and adapter types
        profile_name = _get_profile_name(project_root)
        if not profile_name:
            _sync_log(
                "No profile in project file. Sync skipped. Ensure dbt_project.yml (or dvt_project.yml) exists and has a 'profile' key."
            )
            return None, False
        adapter_types = _get_adapter_types_from_profile(self.profiles_dir, profile_name)
        require_adapters = _get_require_adapters(project_root)

        # 3) Install adapters
        pkg_manager = _detect_package_manager(env_python)
        for adapter_type in adapter_types:
            spec = require_adapters.get(adapter_type)
            pkg = f"dbt-{adapter_type}"
            if spec:
                # spec may be ">=1.0.0" or "1.2.0"; pip needs "dbt-postgres>=1.0.0" or "dbt-postgres==1.2.0"
                pkg_spec = (
                    f"{pkg}{spec}"
                    if any(spec.startswith(c) for c in ("=", ">", "<", "~", "!"))
                    else f"{pkg}=={spec}"
                )
            else:
                pkg_spec = pkg
            _sync_log(f"📥 Installing {pkg_spec} ...")
            if pkg_manager == "uv":
                ok = _run_uv_pip(env_path, [pkg_spec])
                if not ok:
                    _sync_log(yellow("  ⚠️  uv failed, falling back to pip..."))
                    ok = _run_pip(env_python, ["install", pkg_spec])
            else:
                ok = _run_pip(env_python, ["install", pkg_spec])
            if not ok:
                _sync_log(red(f"❌ Failed to install {pkg_spec}"))

        # 4) Pyspark: single version from active target; use canonical ~/.dvt/computes.yml
        # so the project's profile block is used (not a local computes.yml from another profile).
        computes_path = get_dvt_home(None) / "computes.yml"
        pyspark_version = (
            _get_active_pyspark_version(computes_path, profile_name)
            if computes_path.exists()
            else None
        )
        if pyspark_version:
            # Check Java compatibility before installing pyspark
            java_version = _detect_java_version()
            is_compatible, warning = _check_java_compatibility(
                pyspark_version, java_version
            )
            if not is_compatible and warning:
                java_warnings.append(warning)
            _sync_log("🔄 Uninstalling other pyspark versions ...")
            if pkg_manager == "uv":
                ok = _run_uv_pip_uninstall(env_path, ["pyspark"])
                if not ok:
                    _sync_log(yellow("  ⚠️  uv failed, falling back to pip..."))
                    _run_pip(env_python, ["uninstall", "pyspark", "-y"])
            else:
                _run_pip(env_python, ["uninstall", "pyspark", "-y"])
            _sync_log(f"📥 Installing pyspark=={pyspark_version} ...")
            # Pyspark download can be slow; use 10 min timeout for uv pip install
            pyspark_pkg = f"pyspark=={pyspark_version}"
            if pkg_manager == "uv":
                ok = _run_uv_pip(env_path, [pyspark_pkg], timeout=600)
                if not ok:
                    _sync_log(yellow("  ⚠️  uv failed, falling back to pip..."))
                    ok = _run_pip(env_python, ["install", pyspark_pkg])
            else:
                ok = _run_pip(env_python, ["install", pyspark_pkg])
            if not ok:
                _sync_log(red(f"❌ Failed to install pyspark=={pyspark_version}"))
            # Install delta-spark (always coupled with pyspark)
            delta_version = _get_delta_spark_version(pyspark_version)
            if delta_version:
                delta_pkg = f"delta-spark=={delta_version}"
                _sync_log(f"📥 Installing {delta_pkg} (for Delta Lake staging) ...")
                if pkg_manager == "uv":
                    ok = _run_uv_pip(env_path, [delta_pkg])
                    if not ok:
                        _sync_log(yellow("  ⚠️  uv failed, falling back to pip..."))
                        ok = _run_pip(env_python, ["install", delta_pkg])
                else:
                    ok = _run_pip(env_python, ["install", delta_pkg])
                if not ok:
                    _sync_log(
                        red(
                            f"❌ Failed to install {delta_pkg}. "
                            f"Delta Lake staging will not be available."
                        )
                    )
            else:
                _sync_log(
                    yellow(
                        f"  ⚠️  No known delta-spark version for pyspark {pyspark_version}. "
                        f"Delta Lake staging will not be available."
                    )
                )
        else:
            _sync_log(
                "No pyspark version in computes.yml for this profile; skipping pyspark install."
            )

        # 5) JDBC drivers: relate profile adapters to JDBC jars and download for federation.
        # Always use canonical DVT home (~/.dvt/.spark_jars) so jars are in one place (e.g. trial
        # folders with local profiles.yml would otherwise put jars in project dir).
        jdbc_dir = get_spark_jars_dir(None)
        drivers = get_jdbc_drivers_for_adapters(adapter_types)
        if drivers:
            _sync_log(
                f"🔌 Syncing JDBC drivers for adapters: {', '.join(adapter_types)}"
            )
            download_jdbc_jars(
                drivers,
                jdbc_dir,
                on_event=lambda msg: _sync_log(msg),
            )
        else:
            _sync_log(
                "ℹ️  No JDBC drivers required for these adapters (or adapters not in registry)."
            )

        # 6) Native connectors: download Spark native connectors for Snowflake, BigQuery, Redshift
        # These enable optimized data transfer using cloud storage staging.
        native_connectors = get_native_connectors_for_adapters(adapter_types)
        if native_connectors:
            _sync_log(
                f"🚀 Syncing native connectors for: {', '.join([c.adapter for c in native_connectors])}"
            )
            native_results = sync_native_connectors(
                adapter_types,
                profiles_dir=None,  # Use canonical ~/.dvt
                on_event=lambda msg: _sync_log(msg),
            )
            for adapter, success in native_results.items():
                if not success:
                    _sync_log(
                        yellow(
                            f"  ⚠️  Native connector for {adapter} failed to download"
                        )
                    )
        else:
            _sync_log(
                "ℹ️  No native connectors available for these adapters (JDBC will be used)."
            )

        # 7) Cloud storage dependencies: install based on bucket types in buckets.yml
        bucket_deps = get_bucket_dependencies(None)  # Use canonical ~/.dvt
        if bucket_deps:
            _sync_log(
                f"☁️  Installing cloud storage dependencies for bucket types: {', '.join(bucket_deps.keys())}"
            )
            for bucket_type, package_name in bucket_deps.items():
                _sync_log(
                    f"📥 Installing {package_name} (for {bucket_type} buckets)..."
                )
                if pkg_manager == "uv":
                    ok = _run_uv_pip(env_path, [package_name])
                    if not ok:
                        _sync_log(yellow("  ⚠️  uv failed, falling back to pip..."))
                        ok = _run_pip(env_python, ["install", package_name])
                else:
                    ok = _run_pip(env_python, ["install", package_name])
                if not ok:
                    _sync_log(red(f"❌ Failed to install {package_name}"))
        else:
            _sync_log(
                "ℹ️  No cloud storage dependencies needed (using local filesystem or HDFS buckets)."
            )

        # 8) Cloud storage connector JARs: download Hadoop connectors for Spark
        # These enable Spark to read/write directly to S3, GCS, Azure
        from dvt.task.cloud_connectors import (
            get_bucket_types_from_config,
            download_cloud_jars,
            detect_hadoop_version,
        )

        cloud_bucket_types = get_bucket_types_from_config(None)  # Use canonical ~/.dvt
        if cloud_bucket_types:
            hadoop_ver = detect_hadoop_version()
            _sync_log(
                f"☁️  Syncing cloud connector JARs for: {', '.join(cloud_bucket_types)} (Hadoop {hadoop_ver})"
            )
            spark_jars_dir = get_spark_jars_dir(None)
            downloaded = download_cloud_jars(
                cloud_bucket_types,
                spark_jars_dir,
                on_event=lambda msg: _sync_log(msg),
            )
            if downloaded > 0:
                _sync_log(f"  {downloaded} cloud connector JAR(s) ready")
        else:
            _sync_log(
                "ℹ️  No cloud connector JARs needed (no S3/GCS/Azure buckets configured)."
            )

        # 9) CLI tool detection: check for pipe-optimized data transfer tools
        from dvt.task.cli_tools import detect_cli_tools, format_cli_tool_report

        _sync_log("🔧 Detecting CLI tools for pipe-optimized data transfer...")
        cli_results = detect_cli_tools(adapter_types)
        cli_report = format_cli_tool_report(cli_results)
        if cli_report:
            for line in cli_report.splitlines():
                _sync_log(line)

        # Show Java warnings and installation instructions if any
        if java_warnings:
            _sync_log("")
            _sync_log(yellow("⚠️  Java Compatibility Warnings:"))
            for warning in java_warnings:
                _sync_log(red(f"  - {warning}"))
            required_versions = []
            if pyspark_version:
                required_versions = _get_required_java_versions(pyspark_version)
            if required_versions:
                _print_java_installation_instructions(required_versions)

        _sync_log(green("✅ Sync complete."))
        return None, True
