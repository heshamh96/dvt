from pathlib import Path

from dvt.config.project import PartialProject, project_yml_path_if_exists
from dvt.constants import DBT_PROJECT_FILE_NAME, DVT_PROJECT_FILE_NAME
from dvt.exceptions import DvtProjectError


def default_project_dir() -> Path:
    # 1) Check current directory and parents (so "dvt parse" works from a subdir of the project)
    paths = list(Path.cwd().parents)
    paths.insert(0, Path.cwd())
    for x in paths:
        if project_yml_path_if_exists(str(x)) is not None:
            return x
    # 2) If no project in cwd or parents, check direct children (so "dvt parse" works from a parent of the project)
    cwd = Path.cwd()
    try:
        subdirs = sorted(p for p in cwd.iterdir() if p.is_dir())
    except OSError:
        subdirs = []
    for subdir in subdirs:
        for name in (DBT_PROJECT_FILE_NAME, DVT_PROJECT_FILE_NAME):
            if (subdir / name).is_file():
                return subdir
    return Path.cwd()


def default_profiles_dir() -> Path:
    return Path.cwd() if (Path.cwd() / "profiles.yml").exists() else Path.home() / ".dvt"


def default_log_path(project_dir: Path, verify_version: bool = False) -> Path:
    """If available, derive a default log path from dbt_project.yml. Otherwise, default to "logs".
    Known limitations:
    1. Using PartialProject here, so no jinja rendering of log-path.
    2. Programmatic invocations of the cli via dvtRunner may pass a Project object directly,
       which is not being taken into consideration here to extract a log-path.
    """
    default_log_path = Path("logs")
    try:
        partial = PartialProject.from_project_root(str(project_dir), verify_version=verify_version)
        partial_log_path = partial.project_dict.get("log-path") or default_log_path
        default_log_path = Path(project_dir) / partial_log_path
    except DvtProjectError:
        pass

    return default_log_path
