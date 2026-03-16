"""
DVT Show Task — run queries locally via DuckDB without hitting the warehouse.

DuckDB ATTACHes to source databases (Postgres, MySQL, etc.) and runs
the query locally. No data movement, no materialization.
"""

import logging
from typing import Any, Dict, List, Optional, Tuple

from dvt.config.source_connections import load_source_connections
from dvt.extraction.connection_mapper import map_to_sling_url
from dvt.sync.profiles_reader import default_profiles_dir, read_profiles_yml

logger = logging.getLogger(__name__)

# Maps adapter type → DuckDB ATTACH type
ADAPTER_TO_DUCKDB_TYPE = {
    "postgres": "POSTGRES",
    "mysql": "MYSQL",
    "sqlite": "SQLITE",
}


class DvtShowTask:
    """Run a query locally via DuckDB without hitting the target warehouse."""

    def __init__(self, flags: Any, config: Any, manifest: Any = None) -> None:
        self.flags = flags
        self.config = config
        self.manifest = manifest

    def run(self) -> Any:
        import duckdb

        inline = getattr(self.flags, "INLINE", None)
        select = getattr(self.flags, "SELECT", None)
        limit = getattr(self.flags, "SHOW_LIMIT", 100)

        if not inline and not select:
            print(
                "Usage: dvt show --inline 'SELECT ...' or dvt show --select model_name"
            )
            return None

        # Read profiles + source connections
        profiles_dir = (
            getattr(self.flags, "PROFILES_DIR", None) or default_profiles_dir()
        )
        try:
            raw_profiles = read_profiles_yml(profiles_dir)
        except Exception as e:
            print(f"Error reading profiles.yml: {e}")
            return None

        project_dir = getattr(self.flags, "PROJECT_DIR", None) or "."
        source_connections = load_source_connections(project_dir)

        # Start DuckDB
        conn = duckdb.connect(":memory:")
        conn.execute("SET memory_limit = '2GB'")

        try:
            # ATTACH all source databases that DuckDB supports
            self._attach_sources(conn, source_connections, raw_profiles)

            # Get the SQL to run
            if inline:
                sql = inline
            elif select and self.manifest:
                sql = self._get_model_sql(select)
            else:
                sql = inline or "SELECT 1"

            # Apply limit
            if limit and "LIMIT" not in sql.upper():
                sql = f"SELECT * FROM ({sql}) AS _dvt_show LIMIT {limit}"

            # Execute
            result = conn.execute(sql)
            columns = [desc[0] for desc in result.description]
            rows = result.fetchall()

            # Print results
            self._print_table(columns, rows)

            return {"columns": columns, "rows": rows}

        except Exception as e:
            print(f"\nError: {e}")
            return None
        finally:
            conn.close()

    def _attach_sources(
        self,
        conn: Any,
        source_connections: Dict[str, str],
        raw_profiles: Dict[str, Any],
    ) -> None:
        """ATTACH source databases to DuckDB for local querying."""
        attached = set()

        for source_name, connection_name in source_connections.items():
            if connection_name in attached:
                continue

            # Get the output config
            output_config = self._get_output_config(connection_name, raw_profiles)
            if not output_config:
                continue

            adapter_type = output_config.get("type", "")
            duckdb_type = ADAPTER_TO_DUCKDB_TYPE.get(adapter_type)

            if not duckdb_type:
                logger.debug(
                    f"dvt show: skipping {connection_name} ({adapter_type}) — not ATTACHable"
                )
                continue

            try:
                attach_str = self._build_attach_string(output_config, adapter_type)
                conn.execute(
                    f"ATTACH '{attach_str}' AS {connection_name} (TYPE {duckdb_type}, READ_ONLY)"
                )
                attached.add(connection_name)
                logger.info(f"dvt show: ATTACHed {connection_name} ({adapter_type})")
            except Exception as e:
                logger.warning(f"dvt show: failed to ATTACH {connection_name}: {e}")

    def _build_attach_string(self, config: Dict[str, Any], adapter_type: str) -> str:
        """Build the ATTACH connection string for DuckDB."""
        if adapter_type == "postgres":
            host = config.get("host", "localhost")
            port = config.get("port", 5432)
            user = config.get("user", "")
            password = config.get("password", config.get("pass", ""))
            dbname = config.get("dbname", config.get("database", ""))
            return f"dbname={dbname} user={user} password={password} host={host} port={port}"

        elif adapter_type == "mysql":
            host = config.get("host", config.get("server", "localhost"))
            port = config.get("port", 3306)
            user = config.get("user", config.get("username", ""))
            password = config.get("password", config.get("pass", ""))
            database = config.get("database", config.get("schema", ""))
            return f"host={host} port={port} user={user} password={password} database={database}"

        elif adapter_type == "sqlite":
            return config.get("path", config.get("database", ""))

        return ""

    def _get_model_sql(self, select: Tuple) -> str:
        """Get compiled SQL for a selected model."""
        if not self.manifest:
            raise RuntimeError("No manifest available. Run dvt parse first.")

        # Find the model by name
        select_name = select[0] if isinstance(select, (list, tuple)) else select
        for uid, node in self.manifest.nodes.items():
            if getattr(node, "name", "") == select_name:
                compiled = getattr(node, "compiled_code", None)
                if compiled:
                    return compiled
                raw = getattr(node, "raw_code", None) or getattr(node, "raw_sql", "")
                return raw

        raise RuntimeError(f"Model '{select_name}' not found in manifest")

    def _get_output_config(
        self, output_name: str, raw_profiles: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """Get a profiles.yml output config by name."""
        for profile_name, profile_data in raw_profiles.items():
            if not isinstance(profile_data, dict):
                continue
            outputs = profile_data.get("outputs", {})
            if output_name in outputs:
                return outputs[output_name]
        return None

    def _print_table(self, columns: List[str], rows: List[Tuple]) -> None:
        """Print results as a formatted table."""
        if not rows:
            print("(0 rows)")
            return

        # Calculate column widths
        widths = [len(str(c)) for c in columns]
        for row in rows:
            for i, val in enumerate(row):
                widths[i] = max(widths[i], len(str(val)))

        # Header
        header = " | ".join(str(c).ljust(widths[i]) for i, c in enumerate(columns))
        separator = "-+-".join("-" * w for w in widths)
        print(f"\n {header}")
        print(f" {separator}")

        # Rows
        for row in rows:
            line = " | ".join(str(v).ljust(widths[i]) for i, v in enumerate(row))
            print(f" {line}")

        print(f"\n({len(rows)} rows)")

    @staticmethod
    def interpret_results(results: Any) -> bool:
        return results is not None
