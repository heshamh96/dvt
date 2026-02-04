from pathlib import Path

from dvt.config.project import PartialProject, project_yml_path_if_exists
from dvt.exceptions import DvtProjectError


def default_project_dir() -> Path:
    paths = list(Path.cwd().parents)
    paths.insert(0, Path.cwd())
    return next(
        (x for x in paths if project_yml_path_if_exists(str(x)) is not None),
        Path.cwd(),
    )


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
