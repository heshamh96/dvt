"""
DVT user-level configuration: ~/.dvt/profiles.yml, computes.yml, and data/mdm.duckdb.
Used by dvt init to create the DVT home directory structure.
"""
from pathlib import Path
from typing import Optional

DVT_HOME = Path.home() / ".dvt"
PROFILES_PATH = DVT_HOME / "profiles.yml"
COMPUTES_PATH = DVT_HOME / "computes.yml"
DATA_DIR = DVT_HOME / "data"
MDM_DB_PATH = DATA_DIR / "mdm.duckdb"


def get_dvt_home(profiles_dir: Optional[str] = None) -> Path:
    """Return DVT home directory (where profiles.yml and computes.yml live)."""
    if profiles_dir:
        return Path(profiles_dir).resolve()
    return DVT_HOME


def create_default_computes_yml(path: Optional[Path] = None) -> bool:
    """Create computes.yml with default local Spark config if it doesn't exist. Returns True if created."""
    path = Path(path) if path is not None else COMPUTES_PATH
    if path.exists():
        return False
    path.parent.mkdir(parents=True, exist_ok=True)
    content = """# DVT Compute Configuration
# Defines Spark compute environments for federated queries.
# See docs: https://github.com/heshamh96/dvt

computes:
  default:
    type: spark
    master: "local[*]"
    config:
      spark.driver.memory: "2g"
      spark.sql.adaptive.enabled: "true"
"""
    path.write_text(content)
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
