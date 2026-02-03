#!/usr/bin/env python3
"""
Fallback/debug only: run init-like file creation without the dvt CLI (no dvt imports).
Canonical test flow: build and install dvt-core in dev mode with uv, then run the
installed `dvt` CLI (see .cursor/rules/test-team-*.mdc and docs/RUNNING_DVT.md).
Usage: from Testing_Playground, run:
  python /path/to/dvt-core/scripts/run_init_standalone.py Coke_DB
"""
import os
import shutil
import sys
from pathlib import Path

CORE = Path(__file__).resolve().parent.parent / "core"
STARTER = CORE / "dvt" / "include" / "starter_project"
IGNORE = {"__init__.py", "__pycache__", ".git"}

COMPUTES_YML = """# DVT Compute Configuration
# Defines Spark compute environments for federated queries.

computes:
  default:
    type: spark
    master: "local[*]"
    config:
      spark.driver.memory: "2g"
      spark.sql.adaptive.enabled: "true"
"""


def create_user_config(profiles_dir: Path) -> None:
    profiles_dir.mkdir(parents=True, exist_ok=True)
    (profiles_dir / "computes.yml").write_text(COMPUTES_YML)
    data_dir = profiles_dir / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    mdm = data_dir / "mdm.duckdb"
    if not mdm.exists():
        try:
            import duckdb
            conn = duckdb.connect(str(mdm))
            conn.execute("CREATE TABLE IF NOT EXISTS _dvt_init (version INT);")
            conn.execute("INSERT INTO _dvt_init (version) VALUES (1);")
            conn.close()
        except ImportError:
            mdm.touch()


def main():
    project_name = sys.argv[1] if len(sys.argv) > 1 else "Coke_DB"
    cwd = Path.cwd()
    profiles_dir = Path(os.environ.get("DBT_PROFILES_DIR", str(Path.home() / ".dvt"))).resolve()

    create_user_config(profiles_dir)

    project_path = cwd / project_name
    if project_path.exists():
        print(f"Project {project_name} already exists.", file=sys.stderr)
        sys.exit(1)
    shutil.copytree(STARTER, project_path, ignore=shutil.ignore_patterns(*IGNORE))

    project_yml = project_path / "dvt_project.yml"
    content = project_yml.read_text().replace("{project_name}", project_name).replace("{profile_name}", project_name)
    project_yml.write_text(content)

    print(f"Created project {project_name} at {project_path}")
    print(f"User config at {profiles_dir}: computes.yml, data/mdm.duckdb")


if __name__ == "__main__":
    main()
