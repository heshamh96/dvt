"""
DVT user-level configuration: ~/.dvt/profiles.yml, computes.yml, and data/mdm.duckdb.
Used by dvt init to create the DVT home directory structure.
Never overwrite existing profiles.yml or computes.yml; append or merge new profile entries only.
"""
from pathlib import Path
from typing import Any, Dict, Optional

import yaml

DVT_HOME = Path.home() / ".dvt"
PROFILES_PATH = DVT_HOME / "profiles.yml"
COMPUTES_PATH = DVT_HOME / "computes.yml"
DATA_DIR = DVT_HOME / "data"
MDM_DB_PATH = DATA_DIR / "mdm.duckdb"
JDBC_DRIVERS_DIR_NAME = ".jdbc_jars"


def get_dvt_home(profiles_dir: Optional[str] = None) -> Path:
    """Return DVT home directory (where profiles.yml and computes.yml live)."""
    if profiles_dir:
        return Path(profiles_dir).expanduser().resolve()
    return DVT_HOME


def get_jdbc_drivers_dir(profiles_dir: Optional[str] = None) -> Path:
    """Return the directory where JDBC driver JARs for adapters are stored (e.g. ~/.dvt/.jdbc_jars)."""
    return get_dvt_home(profiles_dir) / JDBC_DRIVERS_DIR_NAME


def create_default_computes_yml(path: Optional[Path] = None) -> bool:
    """Create computes.yml with default local Spark config if it doesn't exist. Returns True if created."""
    path = Path(path) if path is not None else COMPUTES_PATH
    if path.exists():
        return False
    path.parent.mkdir(parents=True, exist_ok=True)
    content = """# DVT Compute Configuration
# Top-level keys are profile names. Each profile has target (active compute) and computes (definitions).
# See docs: https://github.com/heshamh96/dvt

default:
  target: default
  computes:
    default:
      type: spark
      version: "3.5.0"
      master: "local[*]"
      config:
        spark.driver.memory: "2g"
        spark.sql.adaptive.enabled: "true"
"""
    path.write_text(content)
    return True


def _default_compute_block() -> Dict[str, Any]:
    """Default compute block for a new profile (Spark local)."""
    return {
        "target": "default",
        "computes": {
            "default": {
                "type": "spark",
                "version": "3.5.0",
                "master": "local[*]",
                "config": {
                    "spark.driver.memory": "2g",
                    "spark.sql.adaptive.enabled": "true",
                },
            }
        },
    }


def append_profile_to_computes_yml(
    profile_name: str,
    profiles_dir: Optional[str] = None,
) -> bool:
    """
    If the profile is not already in computes.yml, append its block (merge).
    Does not overwrite existing profile keys. Returns True if a new block was added.
    """
    dvt_home = get_dvt_home(profiles_dir)
    computes_path = dvt_home / "computes.yml"
    if not computes_path.exists():
        create_default_computes_yml(computes_path)
    with open(computes_path, "r") as f:
        data = yaml.safe_load(f) or {}
    if not isinstance(data, dict):
        return False
    if profile_name in data:
        return False
    data[profile_name] = _default_compute_block()
    with open(computes_path, "w") as f:
        yaml.dump(data, f, default_flow_style=False, sort_keys=False)
    return True


def create_dvt_data_dir(profiles_dir: Optional[str] = None) -> Path:
    """Ensure ~/.dvt/data exists. Returns the data directory path."""
    dvt_home = get_dvt_home(profiles_dir)
    data_dir = dvt_home / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    return data_dir


def init_mdm_db(profiles_dir: Optional[str] = None) -> bool:
    """
    Create mdm.duckdb if it doesn't exist (minimal stub for now).
    Feature 12 (MDM) will add full schema later.
    Returns True if created.
    """
    data_dir = create_dvt_data_dir(profiles_dir)
    mdm_path = data_dir / "mdm.duckdb"
    if mdm_path.exists():
        return False
    try:
        import duckdb
        conn = duckdb.connect(str(mdm_path))
        # Minimal stub: one table so the file is a valid DuckDB DB
        conn.execute("CREATE TABLE IF NOT EXISTS _dvt_init (version INT);")
        conn.execute("INSERT INTO _dvt_init (version) VALUES (1);")
        conn.close()
        return True
    except ImportError:
        # duckdb not installed: create empty file so directory structure is there
        mdm_path.touch()
        return True
